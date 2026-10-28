package microgateway

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ok0/go-microgateway/internal/authkey"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httperr"
)

const (
	auIssuer   = "https://idp.example"
	auAudience = "api"
)

var auStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type auKeys struct {
	rsa1, rsa2 *rsa.PrivateKey
	ec256      *ecdsa.PrivateKey
	ec384      *ecdsa.PrivateKey
	ec521      *ecdsa.PrivateKey
	edPriv     ed25519.PrivateKey
}

var (
	auKeysOnce sync.Once
	auKeysVal  *auKeys
)

// auTestKeys returns the shared test key set, generated once per test binary.
func auTestKeys(t *testing.T) *auKeys {
	t.Helper()
	auKeysOnce.Do(func() {
		k := &auKeys{}
		var err error
		if k.rsa1, err = rsa.GenerateKey(rand.Reader, 2048); err != nil {
			panic(err)
		}
		if k.rsa2, err = rsa.GenerateKey(rand.Reader, 2048); err != nil {
			panic(err)
		}
		if k.ec256, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader); err != nil {
			panic(err)
		}
		if k.ec384, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader); err != nil {
			panic(err)
		}
		if k.ec521, err = ecdsa.GenerateKey(elliptic.P521(), rand.Reader); err != nil {
			panic(err)
		}
		if _, k.edPriv, err = ed25519.GenerateKey(rand.Reader); err != nil {
			panic(err)
		}
		auKeysVal = k
	})
	return auKeysVal
}

// auPEM returns pub as a PKIX "PUBLIC KEY" PEM document.
func auPEM(t *testing.T, pub any) string {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

// auHMAC returns a deterministic shared secret for alg, exactly the minimum length for that algorithm.
func auHMAC(alg string) string {
	base := alg + ":microgateway-auth-test-secret:0123456789abcdef0123456789abcdef"
	return strings.Repeat(base, 2)[:authkey.MinSecretBytes(alg)]
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

// auEnv is the per-test environment of the root auth tests: an in-memory network, a fake clock and a log sink whose
// contents are checked against the registered secrets when the test ends.
type auEnv struct {
	t       *testing.T
	n       *memNetwork
	clk     *clock.Fake
	mu      sync.Mutex
	logs    bytes.Buffer
	secrets []string
}

func newAuEnv(t *testing.T) *auEnv {
	t.Helper()
	e := &auEnv{t: t, n: newMemNetwork(), clk: clock.NewFake(auStart)}
	t.Cleanup(e.checkLogs)
	return e
}

// checkLogs fails the test when a registered secret appears in the captured logs. The secret value is not printed.
func (e *auEnv) checkLogs() {
	e.mu.Lock()
	defer e.mu.Unlock()
	logs := e.logs.String()
	for _, s := range e.secrets {
		if s != "" && strings.Contains(logs, s) {
			e.t.Errorf("a registered secret appears in the logs")
		}
	}
}

func (e *auEnv) Write(p []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.logs.Write(p)
}

func (e *auEnv) logger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(e, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func (e *auEnv) gateway(doc string, opts ...Option) *Gateway {
	e.t.Helper()
	all := append([]Option{withClock(e.clk), WithLogger(e.logger())}, opts...)
	return e.n.gateway(e.t, doc, all...)
}

// secret registers s so that the log check fails when it is logged, and returns it.
func (e *auEnv) secret(s string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.secrets = append(e.secrets, s)
	return s
}

// claims returns standard claims relative to the fake clock; a nil value in extra removes the claim.
func (e *auEnv) claims(extra jwt.MapClaims) jwt.MapClaims {
	c := jwt.MapClaims{
		"iss": auIssuer,
		"aud": auAudience,
		"sub": "user-1",
		"exp": e.clk.Now().Add(time.Hour).Unix(),
	}
	for k, v := range extra {
		if v == nil {
			delete(c, k)
			continue
		}
		c[k] = v
	}
	return c
}

// mint signs c with alg and key, sets kid when non-empty, and registers the token as a secret.
func (e *auEnv) mint(alg, kid string, key any, c jwt.MapClaims) string {
	e.t.Helper()
	method := jwt.GetSigningMethod(alg)
	if method == nil {
		e.t.Fatalf("mint: unknown signing method %q", alg)
	}
	tok := jwt.NewWithClaims(method, c)
	if kid != "" {
		tok.Header["kid"] = kid
	}
	if alg == "none" {
		key = jwt.UnsafeAllowNoneSignatureType
	}
	s, err := tok.SignedString(key)
	if err != nil {
		e.t.Fatal(err)
	}
	return e.secret(s)
}

// get serves a GET request for target with hdr through g and returns the recorded response.
func (e *auEnv) get(g *Gateway, target string, hdr http.Header) *httptest.ResponseRecorder {
	e.t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	for name, vals := range hdr {
		for _, v := range vals {
			req.Header.Add(name, v)
		}
	}
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	return rec
}

// auRecorder is an upstream that records the headers and raw query of every request it receives.
type auRecorder struct {
	mu      sync.Mutex
	headers []http.Header
	queries []string
}

func (r *auRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	r.headers = append(r.headers, req.Header.Clone())
	r.queries = append(r.queries, req.URL.RawQuery)
	r.mu.Unlock()
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

// Count returns the number of requests received.
func (r *auRecorder) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.headers)
}

// Last returns the headers and raw query of the most recent request, or nil and "" when none was received.
func (r *auRecorder) Last() (http.Header, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.headers) == 0 {
		return nil, ""
	}
	return r.headers[len(r.headers)-1], r.queries[len(r.queries)-1]
}

// auBackend runs a recording upstream on the environment's network.
func auBackend(e *auEnv) (*testServer, *auRecorder) {
	rec := &auRecorder{}
	return e.n.serve(e.t, rec), rec
}

// auJWK returns the JWK of pub (RSA, ECDSA or Ed25519) with the given kid and alg; an empty alg is omitted.
func auJWK(kid, alg string, pub any) map[string]any {
	b64 := func(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
	m := map[string]any{"kid": kid}
	if alg != "" {
		m["alg"] = alg
	}
	switch k := pub.(type) {
	case *rsa.PublicKey:
		m["kty"] = "RSA"
		m["n"] = b64(k.N.Bytes())
		m["e"] = b64(big.NewInt(int64(k.E)).Bytes())
	case *ecdsa.PublicKey:
		size := (k.Curve.Params().BitSize + 7) / 8
		m["kty"] = "EC"
		m["crv"] = k.Curve.Params().Name
		m["x"] = b64(k.X.FillBytes(make([]byte, size)))
		m["y"] = b64(k.Y.FillBytes(make([]byte, size)))
	case ed25519.PublicKey:
		m["kty"] = "OKP"
		m["crv"] = "Ed25519"
		m["x"] = b64(k)
	}
	return m
}

// auJWKS returns a JWKS document containing keys.
func auJWKS(keys ...map[string]any) []byte {
	b, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		panic(err)
	}
	return b
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

// auHash returns the configuration form of an API key: "sha256:" followed by the hex digest of key.
func auHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// auDoc returns a configuration with one listener, one upstream named be at backendURL, the top-level auth block
// authYAML (which must end with a newline or be empty) and the routes list body routesYAML.
func auDoc(backendURL, authYAML, routesYAML string) string {
	return "version: 1\n" + listenersYAML +
		fmt.Sprintf("upstreams:\n  - name: be\n    hosts:\n      - url: %q\n", backendURL) +
		authYAML + "routes:\n" + routesYAML
}

func auErrorBody(t *testing.T, rec *httptest.ResponseRecorder) apiError {
	t.Helper()
	var body struct {
		Error apiError `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body.Error
}

// auAssert401 checks a 401 response with the unauthorized error body and the given WWW-Authenticate challenge.
func auAssert401(t *testing.T, rec *httptest.ResponseRecorder, challenge string) {
	t.Helper()
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	e := auErrorBody(t, rec)
	if e.Code != httperr.CodeUnauthorized {
		t.Errorf("code = %q, want %q", e.Code, httperr.CodeUnauthorized)
	}
	if e.Message != httperr.Message(httperr.CodeUnauthorized) {
		t.Errorf("message = %q, want %q", e.Message, httperr.Message(httperr.CodeUnauthorized))
	}
	if e.RequestID == "" {
		t.Error("request_id is empty")
	}
	if got := rec.Header().Get("WWW-Authenticate"); got != challenge {
		t.Errorf("WWW-Authenticate = %q, want %q", got, challenge)
	}
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
func auFailures(t *testing.T, g *Gateway, route, reason string) float64 {
	t.Helper()
	return rsMetricOr0(t, g, "microgateway_auth_failures_total", "route", route, "reason", reason)
}
