package e2e

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/httperr"
)

// auPEM returns pub as a PKIX "PUBLIC KEY" PEM document.
func auPEM(t *testing.T, pub any) string {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

// auSigner returns the signing key for alg: the shared secret for HS*, otherwise the private key of the family.
func auSigner(k *auKeys, alg string) any {
	switch alg {
	case "HS256", "HS384", "HS512":
		return []byte(auHMAC(alg))
	case "RS256", "RS384", "RS512":
		return k.rsa1
	case "ES256":
		return k.ec256
	case "ES384":
		return k.ec384
	case "ES512":
		return k.ec521
	case "EdDSA":
		return k.edPriv
	}
	panic("auSigner: unsupported algorithm " + alg)
}

// auPublic returns the verification key for alg: the shared secret for HS*, otherwise the public key of the family.
func auPublic(k *auKeys, alg string) any {
	switch alg {
	case "HS256", "HS384", "HS512":
		return []byte(auHMAC(alg))
	case "RS256", "RS384", "RS512":
		return &k.rsa1.PublicKey
	case "ES256":
		return &k.ec256.PublicKey
	case "ES384":
		return &k.ec384.PublicKey
	case "ES512":
		return &k.ec521.PublicKey
	case "EdDSA":
		return k.edPriv.Public().(ed25519.PublicKey)
	}
	panic("auPublic: unsupported algorithm " + alg)
}

// auFetcher is a KeySetFetcher that records its calls and returns a configurable body or error.
type auFetcher struct {
	mu        sync.Mutex
	calls     int
	providers []string
	urls      []string
	body      []byte
	err       error
	panics    bool
	block     chan struct{}
}

func (f *auFetcher) FetchKeySet(ctx context.Context, provider, url string) ([]byte, error) {
	f.mu.Lock()
	f.calls++
	f.providers = append(f.providers, provider)
	f.urls = append(f.urls, url)
	body, err, panics, block := f.body, f.err, f.panics, f.block
	f.mu.Unlock()

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if panics {
		panic("auFetcher: failure")
	}
	return body, err
}

func (f *auFetcher) set(body []byte, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.body, f.err = body, err
}

func (f *auFetcher) setPanic(on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.panics = on
}

func (f *auFetcher) setBlock(ch chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.block = ch
}

func (f *auFetcher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type auFailingTransport struct{ t *testing.T }

func (f auFailingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	f.t.Errorf("unexpected HTTP request")
	return nil, errors.New("unexpected HTTP request")
}

// auFailingClient returns a client that fails the test on any request.
func auFailingClient(t *testing.T) *http.Client {
	return &http.Client{Transport: auFailingTransport{t: t}}
}

// auAssert403 checks a 403 response with the forbidden error body.
func auAssert403(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	e := auErrorBody(t, rec)
	if e.Code != httperr.CodeForbidden {
		t.Errorf("code = %q, want %q", e.Code, httperr.CodeForbidden)
	}
	if e.Message != httperr.Message(httperr.CodeForbidden) {
		t.Errorf("message = %q, want %q", e.Message, httperr.Message(httperr.CodeForbidden))
	}
	if e.RequestID == "" {
		t.Error("request_id is empty")
	}
}

// auFailures returns the value of microgateway_auth_failures_total for route and reason, or 0 when not yet recorded.
func auFailures(t *testing.T, g *microgateway.Gateway, route, reason string) float64 {
	t.Helper()
	return rsMetricOr0(t, g, "microgateway_auth_failures_total", "route", route, "reason", reason)
}

const apkRoute = "  - name: r\n    match:\n      path: /x\n    upstream: be\n    auth:\n      mode: apikey\n      providers: [keys1]\n"

func apkAuth(hash string, forward bool) string {
	return fmt.Sprintf("auth:\n  api_keys:\n    - name: keys1\n      query: api_key\n      forward_credentials: %t\n      keys:\n        - id: k1\n          hash: %q\n          consumer: alice\n", forward, hash)
}

func apkKeyFileAuth() string {
	return "auth:\n  api_keys:\n    - name: keys1\n      key_file: keys.yaml\n"
}

func apkKeysFile(hash string) string {
	return fmt.Sprintf("keys:\n  - id: file-1\n    hash: %q\n    consumer: bob\n", hash)
}

func apkHdr(v string) http.Header {
	return http.Header{"X-API-Key": {v}}
}

func apkWrite(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}

func apkFromFile(t *testing.T, e *auEnv, path string, opts ...microgateway.Option) *microgateway.Gateway {
	t.Helper()
	cfg, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	g, err := microgateway.New(cfg, append(opts, microgateway.WithLogger(e.logger()), withDialer(e.n.DialContext))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func TestAuthAPIKeyLocations(t *testing.T) {
	e := newAuEnv(t)
	be, _ := auBackend(e)
	key := e.secret("apk-alpha-7c1d0e9f2b3a4857")
	other := e.secret("apk-omega-0a9b8c7d6e5f4321")
	hash := e.secret(auHash(key))
	g := e.gateway(auDoc(be.URL, apkAuth(hash, false), apkRoute))

	if rec := e.get(g, "/x", apkHdr(key)); rec.Code != http.StatusOK {
		t.Fatalf("key in header: status = %d, want 200", rec.Code)
	}
	if rec := e.get(g, "/x?api_key="+key, nil); rec.Code != http.StatusOK {
		t.Fatalf("key in query: status = %d, want 200", rec.Code)
	}
	if rec := e.get(g, "/x?api_key="+other, apkHdr(key)); rec.Code != http.StatusOK {
		t.Fatalf("valid header with invalid query: status = %d, want 200", rec.Code)
	}
	if rec := e.get(g, "/x?api_key="+key, apkHdr("")); rec.Code != http.StatusOK {
		t.Fatalf("empty header with valid query: status = %d, want 200", rec.Code)
	}
	rec := e.get(g, "/x?api_key="+key, apkHdr(other))
	auAssert401(t, rec, "ApiKey")
	rec = e.get(g, "/x", apkHdr(other))
	auAssert401(t, rec, "ApiKey")
	if got := auFailures(t, g, "r", "invalid_key"); got != 2 {
		t.Fatalf("invalid_key failures = %v, want 2", got)
	}
}

func TestAuthAPIKeyStripping(t *testing.T) {
	e := newAuEnv(t)
	be, rec := auBackend(e)
	key := e.secret("apk-strip-3f2e1d0c9b8a7654")
	hash := e.secret(auHash(key))
	target := "/x?b=2&api_key=" + key + "&a=%2F"

	g := e.gateway(auDoc(be.URL, apkAuth(hash, false), apkRoute))
	if res := e.get(g, target, apkHdr(key)); res.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.Code)
	}
	h, q := rec.Last()
	if h == nil {
		t.Fatal("backend received no request")
	}
	if _, ok := h["X-Api-Key"]; ok {
		t.Error("X-Api-Key reached the backend")
	}
	if q != "b=2&a=%2F" {
		t.Errorf("RawQuery = %q, want %q", q, "b=2&a=%2F")
	}

	kept := e.gateway(auDoc(be.URL, apkAuth(hash, true), apkRoute))
	if res := e.get(kept, target, apkHdr(key)); res.Code != http.StatusOK {
		t.Fatalf("forwarding: status = %d, want 200", res.Code)
	}
	h, q = rec.Last()
	if h == nil {
		t.Fatal("backend received no request")
	}
	if got := h.Get("X-Api-Key"); got != key {
		t.Errorf("forwarded X-Api-Key = %q, want the presented key", got)
	}
	if want := "b=2&api_key=" + key + "&a=%2F"; q != want {
		t.Errorf("forwarding RawQuery = %q, want %q", q, want)
	}
}

func TestAuthAPIKeyFile(t *testing.T) {
	e := newAuEnv(t)
	be, _ := auBackend(e)
	dir := t.TempDir()
	keyA := e.secret("apk-file-a-6d5c4b3a2f1e0d9c")
	keyB := e.secret("apk-file-b-0f1e2d3c4b5a6978")
	hashA := e.secret(auHash(keyA))
	hashB := e.secret(auHash(keyB))
	keysPath := filepath.Join(dir, "keys.yaml")
	apkWrite(t, keysPath, apkKeysFile(hashA))
	cfgPath := writeConfig(t, dir, auDoc(be.URL, apkKeyFileAuth(), apkRoute))

	g := apkFromFile(t, e, cfgPath, withClock(e.clk))
	if res := e.get(g, "/x", apkHdr(keyA)); res.Code != http.StatusOK {
		t.Fatalf("key A before reload: status = %d, want 200", res.Code)
	}

	apkWrite(t, keysPath, apkKeysFile(hashB))
	if err := g.ReloadFile(cfgPath); err != nil {
		t.Fatalf("ReloadFile = %v, want nil", err)
	}
	if res := e.get(g, "/x", apkHdr(keyB)); res.Code != http.StatusOK {
		t.Fatalf("key B after reload: status = %d, want 200", res.Code)
	}
	auAssert401(t, e.get(g, "/x", apkHdr(keyA)), "ApiKey")

	apkWrite(t, keysPath, "keys:\n  - id: file-1\n    hash: \"sha256:not-a-hash\"\n    consumer: bob\n")
	err := g.ReloadFile(cfgPath)
	if err == nil {
		t.Fatal("ReloadFile with a bad key file = nil, want error")
	}
	if !strings.Contains(err.Error(), "keys.yaml (line 3)") {
		t.Fatalf("ReloadFile error = %q, want it to name keys.yaml (line 3)", err.Error())
	}
	if res := e.get(g, "/x", apkHdr(keyB)); res.Code != http.StatusOK {
		t.Fatalf("key B after failed reload: status = %d, want 200", res.Code)
	}
}

func TestWatchFileReloadsKeyFile(t *testing.T) {
	e := newAuEnv(t)
	be, _ := auBackend(e)
	dir := t.TempDir()
	keyA := e.secret("apk-watch-a-2b3c4d5e6f708192")
	keyB := e.secret("apk-watch-b-9a8b7c6d5e4f3021")
	hashA := e.secret(auHash(keyA))
	hashB := e.secret(auHash(keyB))
	keysPath := filepath.Join(dir, "keys.yaml")
	apkWrite(t, keysPath, apkKeysFile(hashA))
	cfgPath := writeConfig(t, dir, auDoc(be.URL, apkKeyFileAuth(), apkRoute))
	g := apkFromFile(t, e, cfgPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- g.WatchFile(ctx, cfgPath) }()
	eventually(t, func() bool {
		_, _, u := counters(g)
		return u == 1
	})

	tmp := filepath.Join(dir, "keys.yaml.tmp")
	apkWrite(t, tmp, apkKeysFile(hashB))
	if err := os.Rename(tmp, keysPath); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return e.get(g, "/x", apkHdr(keyB)).Code == http.StatusOK })
	auAssert401(t, e.get(g, "/x", apkHdr(keyA)), "ApiKey")

	cancel()
	if err := awaitWatch(t, done); err != nil {
		t.Fatalf("WatchFile = %v, want nil", err)
	}
}

func TestAuthSeededErrorsNew(t *testing.T) {
	data, err := os.ReadFile("../../config/testdata/auth_errors.yaml")
	if err != nil {
		t.Fatal(err)
	}
	_, err = microgateway.NewFromYAML(data)
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("NewFromYAML error = %v, want *config.ValidationError", err)
	}
	if len(ve.Errors) < 12 {
		t.Fatalf("got %d errors, want at least 12:\n%v", len(ve.Errors), err)
	}
	paths := []string{
		"auth.jwt[0].issuer",
		"auth.jwt[1].audiences",
		"auth.jwt[2].keys[0].secret",
		"auth.jwt[2].max_token_bytes",
		"auth.jwt[3].keys[0].public_key",
		"auth.jwt[4].keys[0].algorithm",
		"auth.jwt[5].jwks.ttl",
		"auth.api_keys[0].keys[0].hash",
		"routes[0].auth.providers[0]",
		"routes[1].auth.providers[0]",
		"routes[2].auth.allow_anonymous",
		"routes[3].pipeline",
	}
	for _, p := range paths {
		found := false
		for _, fe := range ve.Errors {
			if fe.Path == p {
				found = true
				if fe.Line <= 0 {
					t.Errorf("%s: line = %d, want > 0", p, fe.Line)
				}
			}
		}
		if !found {
			t.Errorf("no error at %s:\n%v", p, err)
		}
	}
}

func TestAuthExplicitZeroAndEmpty(t *testing.T) {
	t.Run("leeway zero is honored", func(t *testing.T) {
		e := newAuEnv(t)
		be, rec := auBackend(e)
		secret := e.secret(auHMAC("HS256"))
		provider := func(name, leeway string) string {
			extra := ""
			if leeway != "" {
				extra = "      leeway: " + leeway + "\n"
			}
			return fmt.Sprintf(`    - name: %s
      issuer: https://idp.example
      audiences: [api]
%s      keys:
        - kid: h1
          algorithm: HS256
          secret: %q
`, name, extra, secret)
		}
		authYAML := "auth:\n  jwt:\n" + provider("zero", "0s") + provider("default", "")
		routes := aidRoute("zero", "/zero", aidAuthBlock("jwt", "zero", "")) +
			aidRoute("default", "/default", aidAuthBlock("jwt", "default", ""))
		g := e.gateway(auDoc(be.URL, authYAML, routes))

		tok := e.mint("HS256", "h1", []byte(secret), e.claims(jwt.MapClaims{"exp": e.clk.Now().Add(-time.Second).Unix()}))
		hdr := http.Header{"Authorization": {"Bearer " + tok}}
		auAssert401(t, e.get(g, "/zero", hdr), "Bearer")
		if got := auFailures(t, g, "zero", "expired"); got != 1 {
			t.Fatalf("expired failures on zero = %v, want 1", got)
		}
		if res := e.get(g, "/default", hdr); res.Code != http.StatusOK {
			t.Fatalf("default leeway: status = %d, want 200", res.Code)
		}
		if rec.Count() != 1 {
			t.Fatalf("backend requests = %d, want 1", rec.Count())
		}
	})

	t.Run("empty strip list keeps identity headers", func(t *testing.T) {
		e := newAuEnv(t)
		be, rec := auBackend(e)
		routes := aidRoute("none", "/none", "")
		doc := auDoc(be.URL, "", routes)
		empty := strings.Replace(doc, "version: 1\n", "version: 1\nglobal:\n  strip_identity_headers: []\n", 1)

		kept := e.gateway(empty)
		if res := e.get(kept, "/none", http.Header{"X-User-Id": {"spoofed"}}); res.Code != http.StatusOK {
			t.Fatalf("empty strip list: status = %d, want 200", res.Code)
		}
		h, _ := rec.Last()
		if got := h.Get("X-User-Id"); got != "spoofed" {
			t.Errorf("empty strip list: X-User-Id = %q, want the forwarded value", got)
		}

		stripped := e.gateway(doc)
		if res := e.get(stripped, "/none", http.Header{"X-User-Id": {"spoofed"}}); res.Code != http.StatusOK {
			t.Fatalf("default strip list: status = %d, want 200", res.Code)
		}
		h, _ = rec.Last()
		if _, ok := h["X-User-Id"]; ok {
			t.Error("default strip list: X-User-Id reached the backend")
		}
	})
}

func TestAuthAdminConfigRedacted(t *testing.T) {
	e := newAuEnv(t)
	be, _ := auBackend(e)
	dir := t.TempDir()
	secret := e.secret(auHMAC("HS256"))
	inline := e.secret(auHash("act-inline-key-4f5e6d7c8b9a0f1e"))
	fileHash := e.secret(auHash("act-file-key-1e2d3c4b5a697881"))
	query := e.secret("q-s3cret-5b")
	keysPath := filepath.Join(dir, "keys.yaml")
	apkWrite(t, keysPath, apkKeysFile(fileHash))
	doc := auDoc(be.URL, fmt.Sprintf(`auth:
  jwt:
    - name: jwt1
      issuer: https://idp.example
      audiences: [api]
      keys:
        - kid: h1
          algorithm: HS256
          secret: %q
    - name: jwks1
      issuer: https://idp.example
      audiences: [api]
      jwks:
        url: "https://idp.example/jwks?token=%s"
        algorithms: [RS256]
  api_keys:
    - name: keys1
      keys:
        - id: k1
          hash: %q
          consumer: alice
    - name: keys2
      key_file: %q
`, secret, query, inline, keysPath), aidRoute("jwt", "/jwt", aidAuthBlock("jwt", "jwt1", "")))
	cfgPath := writeConfig(t, dir, doc)
	f := &auFetcher{}
	g := apkFromFile(t, e, cfgPath, withClock(e.clk), microgateway.WithKeySetFetcher(f))

	body := adminBody(t, g, "/config")
	if !strings.Contains(body, "REDACTED") {
		t.Error("/config contains no REDACTED marker")
	}
	for _, s := range []string{secret, inline, fileHash, query, "file_keys"} {
		if strings.Contains(body, s) {
			t.Error("/config exposes a registered value or the file_keys field")
		}
	}
	if f.count() != 0 {
		t.Errorf("the JWKS fetcher was called %d times by the admin handler", f.count())
	}
}

func aidAuthYAML(hash, forwardClaims string) string {
	return fmt.Sprintf(`auth:
  jwt:
    - name: jwt1
      issuer: https://idp.example
      audiences: [api]
      keys:
        - kid: h1
          algorithm: HS256
          secret: %q
      forward_claims:
%s  api_keys:
    - name: keys1
      keys:
        - id: k1
          hash: %q
          consumer: alice
          metadata:
            tier: gold
      metadata_headers:
        tier: X-Tier
`, auHMAC("HS256"), forwardClaims, hash)
}

func aidRoute(name, path, auth string) string {
	return fmt.Sprintf("  - name: %s\n    match:\n      path: %s\n    upstream: be\n", name, path) + auth
}

func aidAuthBlock(mode string, providers string, extra string) string {
	return fmt.Sprintf("    auth:\n      mode: %s\n      providers: [%s]\n%s", mode, providers, extra)
}

func aidSpoof(extra map[string]string) http.Header {
	h := http.Header{
		"X-User-Id":     {"spoofed-user"},
		"X-Consumer-Id": {"spoofed-consumer"},
		"X-Auth-Claims": {`{"sub":"spoofed"}`},
		"X-Tier":        {"spoofed-tier"},
	}
	for k, v := range extra {
		h.Set(k, v)
	}
	return h
}

func aidAssertIdentityHeaders(t *testing.T, h http.Header, want map[string]string) {
	t.Helper()
	for _, name := range []string{"X-User-Id", "X-Consumer-Id", "X-Auth-Claims", "X-Tier"} {
		vals, present := h[name]
		v, ok := want[name]
		switch {
		case ok && (!present || len(vals) != 1 || vals[0] != v):
			t.Errorf("backend %s = %q, want %q", name, h.Get(name), v)
		case !ok && present:
			t.Errorf("backend %s = %q, want absent", name, h.Get(name))
		}
	}
}

func TestAuthIdentityHeadersStripped(t *testing.T) {
	e := newAuEnv(t)
	be, rec := auBackend(e)
	key := e.secret("aid-key-5a4b3c2d1e0f9a8b")
	hash := e.secret(auHash(key))
	secret := e.secret(auHMAC("HS256"))
	tok := e.mint("HS256", "h1", []byte(secret), e.claims(nil))

	routes := aidRoute("none", "/none", "") +
		aidRoute("none-pipe", "/none-pipe", "    pipeline: [timeout]\n") +
		aidRoute("jwt", "/jwt", aidAuthBlock("jwt", "jwt1", "")) +
		aidRoute("key", "/key", aidAuthBlock("apikey", "keys1", "")) +
		aidRoute("any", "/any", aidAuthBlock("any", "jwt1, keys1", "")) +
		aidRoute("anon", "/anon", aidAuthBlock("any", "jwt1, keys1", "      allow_anonymous: true\n")) +
		aidRoute("failed", "/failed", aidAuthBlock("jwt", "jwt1", ""))
	g := e.gateway(auDoc(be.URL, aidAuthYAML(hash, "        sub: X-User-ID\n"), routes))

	cases := []struct {
		name   string
		path   string
		hdr    http.Header
		status int
		want   map[string]string
	}{
		{"none", "/none", aidSpoof(nil), http.StatusOK, nil},
		{"none with timeout pipeline", "/none-pipe", aidSpoof(nil), http.StatusOK, nil},
		{"valid jwt", "/jwt", aidSpoof(map[string]string{"Authorization": "Bearer " + tok}), http.StatusOK, map[string]string{"X-User-Id": "user-1"}},
		{"valid api key", "/key", aidSpoof(map[string]string{"X-API-Key": key}), http.StatusOK, map[string]string{"X-Consumer-Id": "alice", "X-Tier": "gold"}},
		{"any with jwt", "/any", aidSpoof(map[string]string{"Authorization": "Bearer " + tok}), http.StatusOK, map[string]string{"X-User-Id": "user-1"}},
		{"any with api key", "/any", aidSpoof(map[string]string{"X-API-Key": key}), http.StatusOK, map[string]string{"X-Consumer-Id": "alice", "X-Tier": "gold"}},
		{"anonymous", "/anon", aidSpoof(nil), http.StatusOK, nil},
		{"failed authentication", "/failed", aidSpoof(map[string]string{"Authorization": "Bearer x.y.z"}), http.StatusUnauthorized, nil},
	}
	for _, c := range cases {
		before := rec.Count()
		res := e.get(g, c.path, c.hdr)
		if res.Code != c.status {
			t.Fatalf("%s: status = %d, want %d", c.name, res.Code, c.status)
		}
		if c.status != http.StatusOK {
			if rec.Count() != before {
				t.Errorf("%s: backend was called after a failed authentication", c.name)
			}
			continue
		}
		if rec.Count() != before+1 {
			t.Fatalf("%s: backend was not reached", c.name)
		}
		h, _ := rec.Last()
		aidAssertIdentityHeaders(t, h, c.want)
	}
}

func TestAuthForwardClaims(t *testing.T) {
	e := newAuEnv(t)
	be, rec := auBackend(e)
	key := e.secret("aid-claims-key-0c1d2e3f4a5b6c7d")
	hash := e.secret(auHash(key))
	secret := e.secret(auHMAC("HS256"))
	forward := "        sub: X-User-ID\n        level: X-Level\n        groups: X-Groups\n        missing: X-Missing\n        evil: X-Evil\n"
	routes := aidRoute("jwt", "/jwt", aidAuthBlock("jwt", "jwt1", "")) +
		aidRoute("key", "/key", aidAuthBlock("apikey", "keys1", ""))
	g := e.gateway(auDoc(be.URL, aidAuthYAML(hash, forward), routes))

	tok := e.mint("HS256", "h1", []byte(secret), e.claims(jwt.MapClaims{
		"level":  3,
		"groups": []string{"a", "b"},
		"evil":   "x\r\ny",
	}))
	if res := e.get(g, "/jwt", aidSpoof(map[string]string{"Authorization": "Bearer " + tok})); res.Code != http.StatusOK {
		t.Fatalf("jwt: status = %d, want 200", res.Code)
	}
	h, _ := rec.Last()
	if got := h.Get("X-User-Id"); got != "user-1" {
		t.Errorf("X-User-Id = %q, want user-1", got)
	}
	if got := h.Get("X-Level"); got != "3" {
		t.Errorf("X-Level = %q, want 3", got)
	}
	if got := h.Get("X-Groups"); got != `["a","b"]` {
		t.Errorf("X-Groups = %q, want [\"a\",\"b\"]", got)
	}
	for _, name := range []string{"X-Missing", "X-Evil"} {
		if _, ok := h[name]; ok {
			t.Errorf("%s reached the backend, want absent", name)
		}
	}

	if res := e.get(g, "/key", aidSpoof(map[string]string{"X-API-Key": key})); res.Code != http.StatusOK {
		t.Fatalf("api key: status = %d, want 200", res.Code)
	}
	h, _ = rec.Last()
	if got := h.Get("X-Consumer-Id"); got != "alice" {
		t.Errorf("X-Consumer-Id = %q, want alice", got)
	}
	if got := h.Get("X-Tier"); got != "gold" {
		t.Errorf("X-Tier = %q, want gold", got)
	}
}

type aidProbe struct {
	mu sync.Mutex
	id microgateway.Identity
	ok bool
}

func (p *aidProbe) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := microgateway.IdentityFromContext(r.Context())
		p.mu.Lock()
		p.id, p.ok = id, ok
		p.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func (p *aidProbe) seen() (microgateway.Identity, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.id, p.ok
}

func TestAuthIdentityFromContext(t *testing.T) {
	e := newAuEnv(t)
	be, _ := auBackend(e)
	key := e.secret("aid-ctx-key-8d7c6b5a4f3e2d1c")
	hash := e.secret(auHash(key))
	secret := e.secret(auHMAC("HS256"))
	tok := e.mint("HS256", "h1", []byte(secret), e.claims(nil))

	probe := &aidProbe{}
	const probeRoute = "    middlewares: [probe]\n    pipeline: [auth, probe]\n"
	routes := aidRoute("jwt", "/jwt", probeRoute+aidAuthBlock("jwt", "jwt1", "")) +
		aidRoute("key", "/key", probeRoute+aidAuthBlock("apikey", "keys1", "")) +
		aidRoute("anon", "/anon", probeRoute+aidAuthBlock("any", "jwt1, keys1", "      allow_anonymous: true\n"))
	auth := aidAuthYAML(hash, "        sub: X-User-ID\n") + "middlewares:\n  - name: probe\n    type: probe\n"
	g := e.gateway(auDoc(be.URL, auth, routes), microgateway.WithMiddleware("probe", func(microgateway.MiddlewareParams) (microgateway.Middleware, error) {
		return probe.middleware, nil
	}))

	if res := e.get(g, "/jwt", http.Header{"Authorization": {"Bearer " + tok}}); res.Code != http.StatusOK {
		t.Fatalf("jwt: status = %d, want 200", res.Code)
	}
	id, ok := probe.seen()
	if !ok {
		t.Fatal("jwt: no identity in context")
	}
	if id.Provider != "jwt1" || id.Subject != "user-1" || id.Consumer != "user-1" {
		t.Errorf("jwt identity = %+v, want provider jwt1, subject and consumer user-1", id)
	}
	if id.Claims == nil || id.Claims["sub"] != "user-1" {
		t.Errorf("jwt identity claims = %v, want sub user-1", id.Claims)
	}

	if res := e.get(g, "/key", apkHdr(key)); res.Code != http.StatusOK {
		t.Fatalf("api key: status = %d, want 200", res.Code)
	}
	id, ok = probe.seen()
	if !ok {
		t.Fatal("api key: no identity in context")
	}
	if id.Provider != "keys1" || id.Consumer != "alice" {
		t.Errorf("api key identity = %+v, want provider keys1 and consumer alice", id)
	}
	if id.Metadata["tier"] != "gold" {
		t.Errorf("api key identity metadata = %v, want tier gold", id.Metadata)
	}
	if id.Claims != nil {
		t.Errorf("api key identity claims = %v, want nil", id.Claims)
	}

	if res := e.get(g, "/anon", nil); res.Code != http.StatusOK {
		t.Fatalf("anonymous: status = %d, want 200", res.Code)
	}
	if _, ok := probe.seen(); ok {
		t.Error("anonymous request carries an identity")
	}
}

const jwksTestURL = "https://idp.example/jwks?client=s3cret"

// auJWKSDoc returns a configuration with the JWKS provider idp, whose key set is fetched from jwksURL. jwksExtra is
// indented under jwks. The route jwt authenticates through idp when withJWT is set. The route other sends the header
// X-Rev with the value rev, so that changing rev changes only that route.
func auJWKSDoc(backendURL, jwksURL, jwksExtra, rev string, withJWT bool) string {
	authYAML := ""
	jwtRoute := ""
	if withJWT {
		authYAML = "auth:\n  jwt:\n    - name: idp\n      issuer: " + auIssuer + "\n      audiences:\n        - " + auAudience + "\n" +
			"      jwks:\n        url: " + strconv.Quote(jwksURL) + "\n        algorithms:\n          - RS256\n          - ES256\n" +
			jwksExtra
		jwtRoute = "  - name: jwt\n    match:\n      path: /jwt\n    upstream: be\n    auth:\n      mode: jwt\n      providers:\n        - idp\n"
	}
	routes := jwtRoute + "  - name: other\n    match:\n      path: /other\n    upstream: be\n    request_headers:\n      set:\n        X-Rev: " + strconv.Quote(rev) + "\n"
	return auDoc(backendURL, authYAML, routes)
}

// auHungFetcher blocks until the context of the fetch is done and records that the cancellation was seen.
type auHungFetcher struct {
	cancelled chan struct{}
	once      sync.Once
}

func (h *auHungFetcher) FetchKeySet(ctx context.Context, provider, url string) ([]byte, error) {
	<-ctx.Done()
	h.once.Do(func() { close(h.cancelled) })
	return nil, ctx.Err()
}

func TestAuthJWKSRotation(t *testing.T) {
	e := newAuEnv(t)
	k := auTestKeys(t)
	be, _ := auBackend(e)
	f := &auFetcher{}
	f.set(auJWKS(auJWK("k1", "RS256", &k.rsa1.PublicKey)), nil)
	g := e.gateway(auJWKSDoc(be.URL, jwksTestURL, "", "1", true),
		microgateway.WithKeySetFetcher(f), microgateway.WithHTTPClient(auFailingClient(t)))
	get := func(tok string) *httptest.ResponseRecorder {
		return e.get(g, "/jwt", http.Header{"Authorization": []string{"Bearer " + tok}})
	}
	tok1 := e.mint("RS256", "k1", k.rsa1, e.claims(nil))
	tok2 := e.mint("RS256", "k2", k.rsa2, e.claims(nil))

	if rr := get(tok1); rr.Code != http.StatusOK {
		t.Fatalf("k1 status = %d, want 200", rr.Code)
	}
	if n := f.count(); n != 1 {
		t.Fatalf("fetches = %d, want 1", n)
	}

	f.set(auJWKS(auJWK("k2", "RS256", &k.rsa2.PublicKey)), nil)
	rr := get(tok2)
	auAssert401(t, rr, "Bearer")
	if n := f.count(); n != 1 {
		t.Fatalf("fetches after cooldown request = %d, want 1", n)
	}
	if got := auFailures(t, g, "jwt", "unknown_kid"); got != 1 {
		t.Errorf("unknown_kid failures = %v, want 1", got)
	}

	e.clk.Advance(30 * time.Second)
	if rr := get(tok2); rr.Code != http.StatusOK {
		t.Fatalf("k2 after refresh status = %d, want 200", rr.Code)
	}
	if n := f.count(); n != 2 {
		t.Fatalf("fetches after refresh = %d, want 2", n)
	}

	rr = get(tok1)
	auAssert401(t, rr, "Bearer")
	if n := f.count(); n != 2 {
		t.Fatalf("fetches for removed k1 = %d, want 2 (cooldown)", n)
	}
	if got := auFailures(t, g, "jwt", "unknown_kid"); got != 2 {
		t.Errorf("unknown_kid failures = %v, want 2", got)
	}
}

func TestAuthJWKSConcurrentUnknownKID(t *testing.T) {
	e := newAuEnv(t)
	k := auTestKeys(t)
	be, _ := auBackend(e)
	f := &auFetcher{}
	f.set(auJWKS(auJWK("k1", "RS256", &k.rsa1.PublicKey)), nil)
	g := e.gateway(auJWKSDoc(be.URL, jwksTestURL, "", "1", true),
		microgateway.WithKeySetFetcher(f), microgateway.WithHTTPClient(auFailingClient(t)))
	hdr := func(tok string) http.Header {
		return http.Header{"Authorization": []string{"Bearer " + tok}}
	}
	tok1 := e.mint("RS256", "k1", k.rsa1, e.claims(nil))
	if rr := e.get(g, "/jwt", hdr(tok1)); rr.Code != http.StatusOK {
		t.Fatalf("k1 status = %d, want 200", rr.Code)
	}
	if n := f.count(); n != 1 {
		t.Fatalf("fetches = %d, want 1", n)
	}

	e.clk.Advance(30 * time.Second)
	unknown := e.mint("RS256", "missing", k.rsa1, e.claims(nil))
	const workers = 100
	results := make([]*httptest.ResponseRecorder, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = e.get(g, "/jwt", hdr(unknown))
		}(i)
	}
	wg.Wait()
	for _, rr := range results {
		auAssert401(t, rr, "Bearer")
	}
	if n := f.count(); n != 2 {
		t.Fatalf("fetches = %d, want 2 (one refresh for all unknown-kid requests)", n)
	}
}

func TestAuthJWKSLastGood(t *testing.T) {
	cases := []struct {
		name string
		fail func(f *auFetcher)
	}{
		{"error", func(f *auFetcher) { f.set(nil, errors.New("idp unavailable")) }},
		{"invalid json", func(f *auFetcher) { f.set([]byte("{not json"), nil) }},
		{"empty set", func(f *auFetcher) { f.set(auJWKS(), nil) }},
		{"panic", func(f *auFetcher) { f.setPanic(true) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newAuEnv(t)
			k := auTestKeys(t)
			be, _ := auBackend(e)
			f := &auFetcher{}
			f.set(auJWKS(auJWK("k1", "RS256", &k.rsa1.PublicKey)), nil)
			g := e.gateway(auJWKSDoc(be.URL, jwksTestURL, "", "1", true),
				microgateway.WithKeySetFetcher(f), microgateway.WithHTTPClient(auFailingClient(t)))
			hdr := http.Header{"Authorization": []string{"Bearer " + e.mint("RS256", "k1", k.rsa1, e.claims(nil))}}
			if rr := e.get(g, "/jwt", hdr); rr.Code != http.StatusOK {
				t.Fatalf("initial status = %d, want 200", rr.Code)
			}

			tc.fail(f)
			e.clk.Advance(10 * time.Minute)
			if rr := e.get(g, "/jwt", hdr); rr.Code != http.StatusOK {
				t.Fatalf("status after failed refresh = %d, want 200 (last-good keys)", rr.Code)
			}
			if n := f.count(); n != 2 {
				t.Errorf("fetches = %d, want 2", n)
			}
			if got := rsMetricOr0(t, g, "microgateway_auth_jwks_refreshes_total", "provider", "idp", "result", "failure"); got != 1 {
				t.Errorf("failed refreshes = %v, want 1", got)
			}
			e.mu.Lock()
			logs := e.logs.String()
			e.mu.Unlock()
			if !strings.Contains(logs, "jwks refresh failed") {
				t.Error("log does not contain \"jwks refresh failed\"")
			}
		})
	}
}

func TestAuthJWKSTTLLazy(t *testing.T) {
	e := newAuEnv(t)
	k := auTestKeys(t)
	be, _ := auBackend(e)
	f := &auFetcher{}
	f.set(auJWKS(auJWK("k1", "RS256", &k.rsa1.PublicKey)), nil)
	g := e.gateway(auJWKSDoc(be.URL, jwksTestURL, "", "1", true),
		microgateway.WithKeySetFetcher(f), microgateway.WithHTTPClient(auFailingClient(t)))
	hdr := http.Header{"Authorization": []string{"Bearer " + e.mint("RS256", "k1", k.rsa1, e.claims(nil))}}

	if n := f.count(); n != 0 {
		t.Fatalf("fetches before first request = %d, want 0", n)
	}
	if rr := e.get(g, "/jwt", hdr); rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if n := f.count(); n != 1 {
		t.Fatalf("fetches after first request = %d, want 1", n)
	}

	e.clk.Advance(10*time.Minute - time.Second)
	if rr := e.get(g, "/jwt", hdr); rr.Code != http.StatusOK {
		t.Fatalf("status before ttl = %d, want 200", rr.Code)
	}
	if n := f.count(); n != 1 {
		t.Fatalf("fetches before ttl = %d, want 1", n)
	}

	e.clk.Advance(time.Second)
	if rr := e.get(g, "/jwt", hdr); rr.Code != http.StatusOK {
		t.Fatalf("status after ttl = %d, want 200", rr.Code)
	}
	if n := f.count(); n != 2 {
		t.Fatalf("fetches after ttl = %d, want 2", n)
	}
	if rr := e.get(g, "/jwt", hdr); rr.Code != http.StatusOK {
		t.Fatalf("status on refreshed keys = %d, want 200", rr.Code)
	}
	if n := f.count(); n != 2 {
		t.Fatalf("fetches on refreshed keys = %d, want 2", n)
	}
}

func TestAuthJWKSHungFetcher(t *testing.T) {
	e := newAuEnv(t)
	k := auTestKeys(t)
	be, _ := auBackend(e)
	h := &auHungFetcher{cancelled: make(chan struct{})}
	g := e.gateway(auJWKSDoc(be.URL, jwksTestURL, "", "1", true),
		microgateway.WithKeySetFetcher(h), microgateway.WithHTTPClient(auFailingClient(t)))
	hdr := http.Header{"Authorization": []string{"Bearer " + e.mint("RS256", "k1", k.rsa1, e.claims(nil))}}

	baseline := e.clk.Waiters()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- e.get(g, "/jwt", hdr)
	}()
	e.clk.BlockUntil(baseline + 1)
	e.clk.Advance(5 * time.Second)
	rr := <-done

	auAssert401(t, rr, "Bearer")
	if got := auFailures(t, g, "jwt", "keys_unavailable"); got != 1 {
		t.Errorf("keys_unavailable failures = %v, want 1", got)
	}
	eventually(t, func() bool {
		select {
		case <-h.cancelled:
			return true
		default:
			return false
		}
	})
}

func TestAuthJWKSDownAtStartAndReload(t *testing.T) {
	e := newAuEnv(t)
	k := auTestKeys(t)
	be, _ := auBackend(e)
	f := &auFetcher{}
	f.set(nil, errors.New("idp down"))
	g := e.gateway(auJWKSDoc(be.URL, jwksTestURL, "", "1", true),
		microgateway.WithKeySetFetcher(f), microgateway.WithHTTPClient(auFailingClient(t)))
	hdr := http.Header{"Authorization": []string{"Bearer " + e.mint("RS256", "k1", k.rsa1, e.claims(nil))}}

	rr := e.get(g, "/jwt", hdr)
	auAssert401(t, rr, "Bearer")
	if got := auFailures(t, g, "jwt", "keys_unavailable"); got != 1 {
		t.Errorf("keys_unavailable failures = %v, want 1", got)
	}

	if err := g.ReloadYAML([]byte(auJWKSDoc(be.URL, jwksTestURL, "", "2", true))); err != nil {
		t.Fatal(err)
	}
	rr = e.get(g, "/jwt", hdr)
	auAssert401(t, rr, "Bearer")

	f.set(auJWKS(auJWK("k1", "RS256", &k.rsa1.PublicKey)), nil)
	e.clk.Advance(30 * time.Second)
	if rr := e.get(g, "/jwt", hdr); rr.Code != http.StatusOK {
		t.Fatalf("status after recovery = %d, want 200", rr.Code)
	}
}

func TestAuthJWKSCustomFetcherArgs(t *testing.T) {
	e := newAuEnv(t)
	k := auTestKeys(t)
	be, _ := auBackend(e)
	e.secret("s3cret")
	f := &auFetcher{}
	f.set(nil, fmt.Errorf("dial %s: refused", jwksTestURL))
	g := e.gateway(auJWKSDoc(be.URL, jwksTestURL, "", "1", true),
		microgateway.WithKeySetFetcher(f), microgateway.WithHTTPClient(auFailingClient(t)))
	hdr := http.Header{"Authorization": []string{"Bearer " + e.mint("RS256", "k1", k.rsa1, e.claims(nil))}}

	rr := e.get(g, "/jwt", hdr)
	auAssert401(t, rr, "Bearer")

	f.mu.Lock()
	providers := append([]string(nil), f.providers...)
	urls := append([]string(nil), f.urls...)
	f.mu.Unlock()
	if len(providers) != 1 || providers[0] != "idp" {
		t.Errorf("fetch providers = %v, want [idp]", providers)
	}
	if len(urls) != 1 || urls[0] != jwksTestURL {
		t.Errorf("fetch urls = %v, want [%s]", urls, jwksTestURL)
	}

	e.mu.Lock()
	logs := e.logs.String()
	e.mu.Unlock()
	if !strings.Contains(logs, "jwks refresh failed") {
		t.Error("log does not contain \"jwks refresh failed\"")
	}
	if strings.Contains(logs, "s3cret") || strings.Contains(logs, "client=s3cret") {
		t.Error("logs contain the JWKS query")
	}
}

func TestAuthJWKSHTTPClient(t *testing.T) {
	k := auTestKeys(t)
	cases := []struct {
		name    string
		handler http.HandlerFunc
		status  int
		result  string
	}{
		{"ok", func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(auJWKS(auJWK("k1", "RS256", &k.rsa1.PublicKey)))
		}, http.StatusOK, "success"},
		{"status 500", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		}, http.StatusUnauthorized, "failure"},
		{"oversize body", func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(bytes.Repeat([]byte(" "), 1<<20+1))
		}, http.StatusUnauthorized, "failure"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newAuEnv(t)
			be, _ := auBackend(e)
			e.secret("s3cret")
			idp := e.n.serve(t, tc.handler)
			g := e.gateway(auJWKSDoc(be.URL, idp.URL+"/jwks?client=s3cret", "", "1", true),
				microgateway.WithHTTPClient(e.n.client(t, nil)))
			hdr := http.Header{"Authorization": []string{"Bearer " + e.mint("RS256", "k1", k.rsa1, e.claims(nil))}}

			rr := e.get(g, "/jwt", hdr)
			if tc.status == http.StatusOK {
				if rr.Code != http.StatusOK {
					t.Fatalf("status = %d, want 200", rr.Code)
				}
			} else {
				auAssert401(t, rr, "Bearer")
			}
			if got := rsMetricOr0(t, g, "microgateway_auth_jwks_refreshes_total", "provider", "idp", "result", tc.result); got != 1 {
				t.Errorf("refreshes{result=%s} = %v, want 1", tc.result, got)
			}
		})
	}
}

func TestAuthJWKSPerInstance(t *testing.T) {
	e := newAuEnv(t)
	k := auTestKeys(t)
	be, _ := auBackend(e)
	f := &auFetcher{}
	f.set(auJWKS(auJWK("k1", "RS256", &k.rsa1.PublicKey)), nil)
	doc := auJWKSDoc(be.URL, jwksTestURL, "", "1", true)
	g1 := e.gateway(doc, microgateway.WithKeySetFetcher(f), microgateway.WithHTTPClient(auFailingClient(t)))
	g2 := e.gateway(doc, microgateway.WithKeySetFetcher(f), microgateway.WithHTTPClient(auFailingClient(t)))
	hdr := http.Header{"Authorization": []string{"Bearer " + e.mint("RS256", "k1", k.rsa1, e.claims(nil))}}

	if rr := e.get(g1, "/jwt", hdr); rr.Code != http.StatusOK {
		t.Fatalf("instance 1 status = %d, want 200", rr.Code)
	}
	if n := f.count(); n != 1 {
		t.Fatalf("fetches after instance 1 = %d, want 1", n)
	}
	if rr := e.get(g2, "/jwt", hdr); rr.Code != http.StatusOK {
		t.Fatalf("instance 2 status = %d, want 200", rr.Code)
	}
	if n := f.count(); n != 2 {
		t.Fatalf("fetches after instance 2 = %d, want 2", n)
	}
}

func TestAuthJWKSReloadState(t *testing.T) {
	e := newAuEnv(t)
	k := auTestKeys(t)
	be, _ := auBackend(e)
	e.secret("s3cret")
	f := &auFetcher{}
	f.set(auJWKS(auJWK("k1", "RS256", &k.rsa1.PublicKey)), nil)
	g := e.gateway(auJWKSDoc(be.URL, jwksTestURL, "", "1", true),
		microgateway.WithKeySetFetcher(f), microgateway.WithHTTPClient(auFailingClient(t)))
	hdr := http.Header{"Authorization": []string{"Bearer " + e.mint("RS256", "k1", k.rsa1, e.claims(nil))}}

	if rr := e.get(g, "/jwt", hdr); rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if n := f.count(); n != 1 {
		t.Fatalf("fetches = %d, want 1", n)
	}

	if err := g.ReloadYAML([]byte(auJWKSDoc(be.URL, jwksTestURL, "", "2", true))); err != nil {
		t.Fatal(err)
	}
	if rr := e.get(g, "/jwt", hdr); rr.Code != http.StatusOK {
		t.Fatalf("status after unrelated reload = %d, want 200", rr.Code)
	}
	if n := f.count(); n != 1 {
		t.Fatalf("fetches after unrelated reload = %d, want 1", n)
	}

	newURL := "https://idp.example/jwks2?client=s3cret"
	if err := g.ReloadYAML([]byte(auJWKSDoc(be.URL, newURL, "", "2", true))); err != nil {
		t.Fatal(err)
	}
	if rr := e.get(g, "/jwt", hdr); rr.Code != http.StatusOK {
		t.Fatalf("status after url change = %d, want 200", rr.Code)
	}
	if n := f.count(); n != 2 {
		t.Fatalf("fetches after url change = %d, want 2", n)
	}
	f.mu.Lock()
	lastURL := f.urls[len(f.urls)-1]
	f.mu.Unlock()
	if lastURL != newURL {
		t.Errorf("fetched url = %q, want %q", lastURL, newURL)
	}

	if err := g.ReloadYAML([]byte(auJWKSDoc(be.URL, newURL, "        ttl: 5m\n", "2", true))); err != nil {
		t.Fatal(err)
	}
	if rr := e.get(g, "/jwt", hdr); rr.Code != http.StatusOK {
		t.Fatalf("status after ttl change = %d, want 200", rr.Code)
	}
	if n := f.count(); n != 3 {
		t.Fatalf("fetches after ttl change = %d, want 3", n)
	}

	if err := g.ReloadYAML([]byte("version: [")); err == nil {
		t.Fatal("invalid YAML reload succeeded")
	}
	if rr := e.get(g, "/jwt", hdr); rr.Code != http.StatusOK {
		t.Fatalf("status after failed reload = %d, want 200", rr.Code)
	}
	if n := f.count(); n != 3 {
		t.Fatalf("fetches after failed reload = %d, want 3", n)
	}

	before := inspect(g).StateEntries
	if err := g.ReloadYAML([]byte(auJWKSDoc(be.URL, jwksTestURL, "", "3", false))); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return inspect(g).StateEntries == before-1 })
}

// auJWTDoc returns a configuration with one JWT provider p. settings is indented under the provider, keys is the
// indented keys list. The route jwt runs the pipeline timeout, auth, idprobe against the upstream be.
func auJWTDoc(backendURL, settings, keys string) string {
	authYAML := "auth:\n  jwt:\n    - name: p\n      issuer: " + auIssuer + "\n      audiences:\n        - " + auAudience + "\n" +
		settings + "      keys:\n" + keys +
		"middlewares:\n  - name: idprobe\n    type: idprobe\n"
	routes := "  - name: jwt\n    upstream: be\n    auth:\n      mode: jwt\n      providers:\n        - p\n" +
		"    middlewares:\n      - idprobe\n    pipeline:\n      - timeout\n      - auth\n      - idprobe\n"
	return auDoc(backendURL, authYAML, routes)
}

// auJWTSecretKey returns a list entry for an HS key with the deterministic test secret of alg.
func auJWTSecretKey(kid, alg string) string {
	return fmt.Sprintf("        - kid: %s\n          algorithm: %s\n          secret: %q\n", kid, alg, auHMAC(alg))
}

// auJWTPublicKey returns a list entry for an asymmetric key of alg whose PEM is pub.
func auJWTPublicKey(t *testing.T, kid, alg string, pub any) string {
	return fmt.Sprintf("        - kid: %s\n          algorithm: %s\n          public_key: %s\n",
		kid, alg, strconv.Quote(auPEM(t, pub)))
}

// auJWTProbe copies the subject of the authenticated identity to the X-Probe-Sub request header.
func auJWTProbe(microgateway.MiddlewareParams) (microgateway.Middleware, error) {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if id, ok := microgateway.IdentityFromContext(r.Context()); ok {
				r.Header.Set("X-Probe-Sub", id.Subject)
			}
			next.ServeHTTP(w, r)
		})
	}, nil
}

func TestAuthJWTTable(t *testing.T) {
	e := newAuEnv(t)
	k := auTestKeys(t)
	be, rec := auBackend(e)
	bearer := func(tok string) http.Header {
		return http.Header{"Authorization": []string{"Bearer " + tok}}
	}
	keysFor := func(algs ...string) string {
		var b strings.Builder
		for _, alg := range algs {
			kid := strings.ToLower(alg)
			if strings.HasPrefix(alg, "HS") {
				b.WriteString(auJWTSecretKey(kid, alg))
				e.secret(auHMAC(alg))
				continue
			}
			b.WriteString(auJWTPublicKey(t, kid, alg, auPublic(k, alg)))
		}
		return b.String()
	}

	hs := e.gateway(auJWTDoc(be.URL, "      max_token_bytes: 512\n", keysFor("HS256", "HS384", "HS512")),
		microgateway.WithMiddleware("idprobe", auJWTProbe))
	rs := e.gateway(auJWTDoc(be.URL, "", keysFor("RS256", "RS384", "RS512")),
		microgateway.WithMiddleware("idprobe", auJWTProbe))
	es := e.gateway(auJWTDoc(be.URL, "", keysFor("ES256", "ES384", "ES512")),
		microgateway.WithMiddleware("idprobe", auJWTProbe))
	ed := e.gateway(auJWTDoc(be.URL, "", keysFor("EdDSA")),
		microgateway.WithMiddleware("idprobe", auJWTProbe))

	valid := []struct {
		alg string
		g   *microgateway.Gateway
	}{
		{"HS256", hs}, {"HS384", hs}, {"HS512", hs},
		{"RS256", rs}, {"RS384", rs}, {"RS512", rs},
		{"ES256", es}, {"ES384", es}, {"ES512", es},
		{"EdDSA", ed},
	}
	for _, tc := range valid {
		t.Run("valid "+tc.alg, func(t *testing.T) {
			tok := e.mint(tc.alg, strings.ToLower(tc.alg), auSigner(k, tc.alg), e.claims(nil))
			before := rec.Count()
			rr := e.get(tc.g, "/jwt", bearer(tok))
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rr.Code)
			}
			if rec.Count() != before+1 {
				t.Fatalf("backend requests = %d, want %d", rec.Count(), before+1)
			}
			h, _ := rec.Last()
			if got := h.Get("X-Probe-Sub"); got != "user-1" {
				t.Errorf("X-Probe-Sub = %q, want %q", got, "user-1")
			}
		})
	}

	rejects := []struct {
		name   string
		g      *microgateway.Gateway
		reason string
		token  func() string
	}{
		{"expired", hs, "expired", func() string {
			return e.mint("HS256", "hs256", auSigner(k, "HS256"),
				e.claims(jwt.MapClaims{"exp": e.clk.Now().Add(-time.Hour).Unix()}))
		}},
		{"nbf in the future", hs, "expired", func() string {
			return e.mint("HS256", "hs256", auSigner(k, "HS256"),
				e.claims(jwt.MapClaims{"nbf": e.clk.Now().Add(time.Hour).Unix()}))
		}},
		{"wrong issuer", hs, "claims", func() string {
			return e.mint("HS256", "hs256", auSigner(k, "HS256"),
				e.claims(jwt.MapClaims{"iss": "https://evil.example"}))
		}},
		{"wrong audience", hs, "claims", func() string {
			return e.mint("HS256", "hs256", auSigner(k, "HS256"),
				e.claims(jwt.MapClaims{"aud": "other"}))
		}},
		{"missing exp", hs, "claims", func() string {
			return e.mint("HS256", "hs256", auSigner(k, "HS256"),
				e.claims(jwt.MapClaims{"exp": nil}))
		}},
		{"bad signature", hs, "signature", func() string {
			return e.mint("HS256", "hs256", []byte(strings.Repeat("x", 32)), e.claims(nil))
		}},
		{"alg none", hs, "algorithm", func() string {
			return e.mint("none", "hs256", nil, e.claims(nil))
		}},
		{"unlisted algorithm", ed, "algorithm", func() string {
			return e.mint("RS256", "", auSigner(k, "RS256"), e.claims(nil))
		}},
		{"per-key algorithm mismatch", rs, "algorithm", func() string {
			return e.mint("RS384", "rs256", auSigner(k, "RS384"), e.claims(nil))
		}},
		{"RSA public key as HS256 secret", rs, "algorithm", func() string {
			return e.mint("HS256", "rs256", []byte(auPEM(t, &k.rsa1.PublicKey)), e.claims(nil))
		}},
		{"over max_token_bytes", hs, "too_large", func() string {
			return e.mint("HS256", "hs256", auSigner(k, "HS256"),
				e.claims(jwt.MapClaims{"pad": strings.Repeat("a", 600)}))
		}},
	}
	for _, tc := range rejects {
		t.Run("reject "+tc.name, func(t *testing.T) {
			tok := tc.token()
			before := rec.Count()
			failures := auFailures(t, tc.g, "jwt", tc.reason)
			rr := e.get(tc.g, "/jwt", bearer(tok))
			auAssert401(t, rr, "Bearer")
			if rec.Count() != before {
				t.Errorf("backend requests changed from %d to %d", before, rec.Count())
			}
			if got := auFailures(t, tc.g, "jwt", tc.reason); got != failures+1 {
				t.Errorf("auth failures {reason=%s} = %v, want %v", tc.reason, got, failures+1)
			}
		})
	}
}

func TestAuthJWTLeeway(t *testing.T) {
	e := newAuEnv(t)
	k := auTestKeys(t)
	be, rec := auBackend(e)
	bearer := func(tok string) http.Header {
		return http.Header{"Authorization": []string{"Bearer " + tok}}
	}
	keys := auJWTSecretKey("hs256", "HS256")
	e.secret(auHMAC("HS256"))
	withDefault := e.gateway(auJWTDoc(be.URL, "", keys), microgateway.WithMiddleware("idprobe", auJWTProbe))
	noLeeway := e.gateway(auJWTDoc(be.URL, "      leeway: 0s\n", keys), microgateway.WithMiddleware("idprobe", auJWTProbe))

	tok := e.mint("HS256", "hs256", auSigner(k, "HS256"),
		e.claims(jwt.MapClaims{"exp": e.clk.Now().Add(-20 * time.Second).Unix()}))

	rr := e.get(withDefault, "/jwt", bearer(tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("default leeway: status = %d, want 200", rr.Code)
	}
	if rec.Count() != 1 {
		t.Fatalf("default leeway: backend requests = %d, want 1", rec.Count())
	}

	before := rec.Count()
	failures := auFailures(t, noLeeway, "jwt", "expired")
	rr = e.get(noLeeway, "/jwt", bearer(tok))
	auAssert401(t, rr, "Bearer")
	if rec.Count() != before {
		t.Errorf("leeway 0s: backend requests changed from %d to %d", before, rec.Count())
	}
	if got := auFailures(t, noLeeway, "jwt", "expired"); got != failures+1 {
		t.Errorf("leeway 0s: auth failures {reason=expired} = %v, want %v", got, failures+1)
	}
}

func amdMerge(a, b http.Header) http.Header {
	out := http.Header{}
	for k, v := range a {
		out[k] = append([]string(nil), v...)
	}
	for k, v := range b {
		out[k] = append([]string(nil), v...)
	}
	return out
}

func TestAuthModes(t *testing.T) {
	e := newAuEnv(t)
	be, _ := auBackend(e)
	secret := e.secret(auHMAC("HS256"))
	key := e.secret("amd-key-1a2b3c4d5e6f7081")
	wrong := e.secret("amd-wrong-9f8e7d6c5b4a3921")
	hash := e.secret(auHash(key))
	valid := e.mint("HS256", "h1", []byte(secret), e.claims(nil))
	expired := e.mint("HS256", "h1", []byte(secret), e.claims(jwt.MapClaims{"exp": e.clk.Now().Add(-time.Hour).Unix()}))

	routes := amdRoute("any", "/any", "    auth:\n      mode: any\n      providers: [jwt1, keys1]\n") +
		amdRoute("anon", "/anon", "    auth:\n      mode: any\n      providers: [jwt1, keys1]\n      allow_anonymous: true\n") +
		amdRoute("none", "/none", "") +
		amdRoute("piped", "/piped", "    pipeline: [auth, timeout]\n    auth:\n      mode: apikey\n      providers: [keys1]\n")
	g := e.gateway(auDoc(be.URL, amdAuthYAML(secret, hash), routes))

	bearer := func(tok string) http.Header { return http.Header{"Authorization": {"Bearer " + tok}} }
	apiKey := func(k string) http.Header { return http.Header{"X-API-Key": {k}} }

	if res := e.get(g, "/any", bearer(valid)); res.Code != http.StatusOK {
		t.Fatalf("any with a valid jwt: status = %d, want 200", res.Code)
	}
	if res := e.get(g, "/any", apiKey(key)); res.Code != http.StatusOK {
		t.Fatalf("any with a valid key: status = %d, want 200", res.Code)
	}
	if res := e.get(g, "/any", amdMerge(bearer(expired), apiKey(key))); res.Code != http.StatusOK {
		t.Fatalf("any with an invalid jwt and a valid key: status = %d, want 200", res.Code)
	}

	auAssert401(t, e.get(g, "/any", amdMerge(bearer(expired), apiKey(wrong))), "Bearer")
	if got := auFailures(t, g, "any", "expired"); got != 1 {
		t.Fatalf("expired failures on any = %v, want 1", got)
	}
	if got := auFailures(t, g, "any", "invalid_key"); got != 0 {
		t.Fatalf("invalid_key failures on any = %v, want 0", got)
	}

	auAssert401(t, e.get(g, "/any", nil), "Bearer")
	if got := auFailures(t, g, "any", "missing"); got != 1 {
		t.Fatalf("missing failures on any = %v, want 1", got)
	}

	if res := e.get(g, "/anon", nil); res.Code != http.StatusOK {
		t.Fatalf("anonymous with nothing presented: status = %d, want 200", res.Code)
	}
	auAssert401(t, e.get(g, "/anon", apiKey(wrong)), "Bearer")

	if res := e.get(g, "/none", nil); res.Code != http.StatusOK {
		t.Fatalf("none route without credentials: status = %d, want 200", res.Code)
	}

	auAssert401(t, e.get(g, "/piped", nil), "ApiKey")
	if res := e.get(g, "/piped", apiKey(key)); res.Code != http.StatusOK {
		t.Fatalf("explicit pipeline with a valid key: status = %d, want 200", res.Code)
	}
}

func TestAuthAuthorization(t *testing.T) {
	e := newAuEnv(t)
	be, rec := auBackend(e)
	secret := e.secret(auHMAC("HS256"))
	key := e.secret("amd-authz-key-7e6d5c4b3a291807")
	hash := e.secret(auHash(key))
	jwtRoute := func(name, path, requires string) string {
		return amdRoute(name, path, "    auth:\n      mode: jwt\n      providers: [jwt1]\n"+requires)
	}
	routes := jwtRoute("scope", "/scope", "      require:\n        scopes: [write]\n") +
		jwtRoute("role", "/role", "      require:\n        roles: [admin]\n") +
		jwtRoute("claim", "/claim", "      require:\n        claims:\n          tier: gold\n") +
		jwtRoute("all", "/all", "      require:\n        scopes: [read]\n        roles: [user]\n        claims:\n          tier: gold\n") +
		amdRoute("key", "/key", "    auth:\n      mode: apikey\n      providers: [keys1]\n      require:\n        scopes: [read]\n        roles: [user]\n        claims:\n          tier: gold\n")
	g := e.gateway(auDoc(be.URL, amdAuthYAML(secret, hash), routes))

	token := func(c jwt.MapClaims) http.Header {
		return http.Header{"Authorization": {"Bearer " + e.mint("HS256", "h1", []byte(secret), e.claims(c))}}
	}
	expectDenied := func(path string, hdr http.Header, route string) {
		t.Helper()
		before := rec.Count()
		auAssert403(t, e.get(g, path, hdr))
		if rec.Count() != before {
			t.Errorf("%s: backend was called for a forbidden request", path)
		}
		if got := auFailures(t, g, route, "forbidden"); got != 1 {
			t.Errorf("forbidden failures on %s = %v, want 1", route, got)
		}
	}
	expectAllowed := func(path string, hdr http.Header) {
		t.Helper()
		if res := e.get(g, path, hdr); res.Code != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200", path, res.Code)
		}
	}

	expectDenied("/scope", token(jwt.MapClaims{"scope": "read"}), "scope")
	expectAllowed("/scope", token(jwt.MapClaims{"scope": "read write"}))
	expectAllowed("/scope", token(jwt.MapClaims{"scope": []string{"read", "write"}}))

	expectDenied("/role", token(jwt.MapClaims{"roles": []string{"user"}}), "role")
	expectAllowed("/role", token(jwt.MapClaims{"roles": []string{"admin", "user"}}))

	expectDenied("/claim", token(jwt.MapClaims{"tier": "silver"}), "claim")
	expectAllowed("/claim", token(jwt.MapClaims{"tier": "gold"}))

	expectAllowed("/key", apkHdr(key))

	expectAllowed("/all", token(jwt.MapClaims{"scope": "read", "roles": []string{"user"}, "tier": "gold"}))
}

func aobDoc(be, keysHash string) string {
	return auDoc(be, fmt.Sprintf(`auth:
  jwt:
    - name: jwt1
      issuer: https://idp.example
      audiences: [api]
      keys:
        - kid: h1
          algorithm: HS256
          secret: %q
    - name: jwks1
      issuer: https://idp.example
      audiences: [api]
      jwks:
        url: https://idp.example/jwks
        algorithms: [RS256]
  api_keys:
    - name: keys1
      keys:
        - id: k1
          hash: %q
          consumer: alice
`, auHMAC("HS256"), keysHash), aobRoutes())
}

func aobRoutes() string {
	return aidRoute("missing", "/missing", aidAuthBlock("jwt", "jwt1", "")) +
		aidRoute("malformed", "/malformed", aidAuthBlock("jwt", "jwt1", "")) +
		aidRoute("too-large", "/too-large", aidAuthBlock("jwt", "jwt1", "")) +
		aidRoute("expired", "/expired", aidAuthBlock("jwt", "jwt1", "")) +
		aidRoute("signature", "/signature", aidAuthBlock("jwt", "jwt1", "")) +
		aidRoute("algorithm", "/algorithm", aidAuthBlock("jwt", "jwt1", "")) +
		aidRoute("claims", "/claims", aidAuthBlock("jwt", "jwt1", "")) +
		aidRoute("unknown-kid", "/unknown-kid", aidAuthBlock("jwt", "jwt1", "")) +
		aidRoute("invalid-key", "/invalid-key", aidAuthBlock("apikey", "keys1", "")) +
		aidRoute("forbidden", "/forbidden", aidAuthBlock("jwt", "jwt1", "      require:\n        scopes: [admin]\n")) +
		aidRoute("keys-unavailable", "/keys-unavailable", aidAuthBlock("jwt", "jwks1", ""))
}

func TestAuthMetricsExposition(t *testing.T) {
	e := newAuEnv(t)
	be, _ := auBackend(e)
	secret := e.secret(auHMAC("HS256"))
	key := e.secret("aob-key-6b5a4f3e2d1c0b9a")
	hash := e.secret(auHash(key))
	keys := auTestKeys(t)
	f := &auFetcher{}
	f.set(nil, errors.New("connection refused"))
	g := e.gateway(aobDoc(be.URL, hash), microgateway.WithKeySetFetcher(f))

	claims := e.claims(nil)
	expiredClaims := e.claims(jwt.MapClaims{"exp": e.clk.Now().Add(-time.Hour).Unix()})
	type probe struct {
		route, path, reason, challenge string
		hdr                            http.Header
		status                         int
	}
	probes := []probe{
		{"missing", "/missing", "missing", "Bearer", nil, http.StatusUnauthorized},
		{"malformed", "/malformed", "malformed", "Bearer", http.Header{"Authorization": {"Bearer"}}, http.StatusUnauthorized},
		{"too-large", "/too-large", "too_large", "Bearer", http.Header{"Authorization": {"Bearer " + strings.Repeat("a", 4097)}}, http.StatusUnauthorized},
		{"expired", "/expired", "expired", "Bearer", http.Header{"Authorization": {"Bearer " + e.mint("HS256", "h1", []byte(secret), expiredClaims)}}, http.StatusUnauthorized},
		{"signature", "/signature", "signature", "Bearer", http.Header{"Authorization": {"Bearer " + e.mint("HS256", "h1", []byte(strings.Repeat("x", 32)), claims)}}, http.StatusUnauthorized},
		{"algorithm", "/algorithm", "algorithm", "Bearer", http.Header{"Authorization": {"Bearer " + e.mint("none", "", nil, claims)}}, http.StatusUnauthorized},
		{"claims", "/claims", "claims", "Bearer", http.Header{"Authorization": {"Bearer " + e.mint("HS256", "h1", []byte(secret), e.claims(jwt.MapClaims{"iss": "https://evil.example"}))}}, http.StatusUnauthorized},
		{"unknown-kid", "/unknown-kid", "unknown_kid", "Bearer", http.Header{"Authorization": {"Bearer " + e.mint("HS256", "nope", []byte(secret), claims)}}, http.StatusUnauthorized},
		{"invalid-key", "/invalid-key", "invalid_key", "ApiKey", http.Header{"X-API-Key": {"aob-wrong-key-0a1b2c3d"}}, http.StatusUnauthorized},
		{"forbidden", "/forbidden", "forbidden", "", http.Header{"Authorization": {"Bearer " + e.mint("HS256", "h1", []byte(secret), claims)}}, http.StatusForbidden},
		{"keys-unavailable", "/keys-unavailable", "keys_unavailable", "Bearer", http.Header{"Authorization": {"Bearer " + e.mint("RS256", "r1", auSigner(keys, "RS256"), claims)}}, http.StatusUnauthorized},
	}
	for _, p := range probes {
		res := e.get(g, p.path, p.hdr)
		if res.Code != p.status {
			t.Fatalf("%s: status = %d, want %d", p.route, res.Code, p.status)
		}
		if p.status == http.StatusUnauthorized {
			auAssert401(t, res, p.challenge)
		} else {
			auAssert403(t, res)
		}
	}

	text := metricsText(t, g)
	rsCheckExposition(t, text)
	for _, p := range probes {
		if got := auFailures(t, g, p.route, p.reason); got != 1 {
			t.Errorf("%s: %s failures = %v, want 1", p.route, p.reason, got)
		}
		if line := series("microgateway_auth_failures_total", "route", p.route, "reason", p.reason); !strings.Contains(text, line) {
			t.Errorf("metrics lack %s", line)
		}
	}
}

func TestAuthNoSecretsInLogs(t *testing.T) {
	e := newAuEnv(t)
	be, _ := auBackend(e)
	dir := t.TempDir()
	secret := e.secret(auHMAC("HS256"))
	key := e.secret("aob-file-key-3c2b1a0f9e8d7c6b")
	hash := e.secret(auHash(key))
	pw := e.secret("pw-s3cret-7f")
	token := e.secret("q-s3cret-9e")
	keysPath := filepath.Join(dir, "keys.yaml")
	apkWrite(t, keysPath, apkKeysFile(hash))
	cfgPath := writeConfig(t, dir, auDoc(be.URL, fmt.Sprintf(`auth:
  jwt:
    - name: jwt1
      issuer: https://idp.example
      audiences: [api]
      keys:
        - kid: h1
          algorithm: HS256
          secret: %q
    - name: jwks1
      issuer: https://idp.example
      audiences: [api]
      jwks:
        url: "https://microgateway-user:%s@idp.example/jwks?token=%s"
        algorithms: [RS256]
  api_keys:
    - name: keys1
      key_file: keys.yaml
`, secret, pw, token), aidRoute("jwt", "/jwt", aidAuthBlock("jwt", "jwt1", ""))+
		aidRoute("keys", "/keys", aidAuthBlock("apikey", "keys1", ""))+
		aidRoute("jwks", "/jwks", aidAuthBlock("jwt", "jwks1", ""))))

	f := &auFetcher{}
	f.set(nil, errors.New("connection refused"))
	g := apkFromFile(t, e, cfgPath, withClock(e.clk), microgateway.WithKeySetFetcher(f))

	bad := e.mint("HS256", "h1", []byte(strings.Repeat("z", 32)), e.claims(nil))
	auAssert401(t, e.get(g, "/jwt", http.Header{"Authorization": {"Bearer " + bad}}), "Bearer")
	wrongKey := e.secret("aob-wrong-key-8a7b6c5d4e3f2a1b")
	auAssert401(t, e.get(g, "/keys", apkHdr(wrongKey)), "ApiKey")
	jwksTok := e.mint("RS256", "r1", auSigner(auTestKeys(t), "RS256"), e.claims(nil))
	auAssert401(t, e.get(g, "/jwks", http.Header{"Authorization": {"Bearer " + jwksTok}}), "Bearer")
	if f.count() == 0 {
		t.Fatal("the JWKS fetcher was never called")
	}

	apkWrite(t, keysPath, "keys:\n  - id: file-1\n    hash: \"sha256:not-a-hash\"\n    consumer: bob\n")
	err := g.ReloadFile(cfgPath)
	if err == nil {
		t.Fatal("ReloadFile with a bad key file = nil, want error")
	}
	for _, s := range []string{secret, key, hash, pw, token, bad, wrongKey, jwksTok} {
		if strings.Contains(err.Error(), s) {
			t.Error("ReloadFile error exposes a registered secret")
		}
	}

	body := adminBody(t, g, "/config")
	for _, s := range []string{secret, key, hash, pw, token, bad, wrongKey, jwksTok} {
		if strings.Contains(body, s) {
			t.Error("/config exposes a registered secret")
		}
	}
	if !strings.Contains(body, "REDACTED") {
		t.Error("/config contains no REDACTED marker")
	}
}

func TestWithKeySetFetcherNil(t *testing.T) {
	g, err := microgateway.NewFromYAML([]byte("version: 1\n"), microgateway.WithKeySetFetcher(nil))
	if err == nil || err.Error() != "microgateway: WithKeySetFetcher: nil fetcher" {
		t.Fatalf("NewFromYAML error = %v, want nil fetcher error", err)
	}
	if g != nil {
		t.Fatal("NewFromYAML returned a gateway with an error")
	}
	g, err = microgateway.New(&config.Config{}, microgateway.WithKeySetFetcher(nil))
	if err == nil || err.Error() != "microgateway: WithKeySetFetcher: nil fetcher" {
		t.Fatalf("New error = %v, want nil fetcher error", err)
	}
	if g != nil {
		t.Fatal("New returned a gateway with an error")
	}
}

func TestWithHTTPClientNil(t *testing.T) {
	g, err := microgateway.NewFromYAML([]byte("version: 1\n"), microgateway.WithHTTPClient(nil))
	if err == nil || err.Error() != "microgateway: WithHTTPClient: nil client" {
		t.Fatalf("NewFromYAML error = %v, want nil client error", err)
	}
	if g != nil {
		t.Fatal("NewFromYAML returned a gateway with an error")
	}
	g, err = microgateway.New(&config.Config{}, microgateway.WithHTTPClient(nil))
	if err == nil || err.Error() != "microgateway: WithHTTPClient: nil client" {
		t.Fatalf("New error = %v, want nil client error", err)
	}
	if g != nil {
		t.Fatal("New returned a gateway with an error")
	}
}
