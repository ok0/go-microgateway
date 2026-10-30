package runtime

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/middleware"
	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/internal/state"
)

const (
	auSecret = "runtime-test-hs256-secret-0123456789"
	auIssuer = "https://idp.example"
)

var auStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type auFetcher struct {
	mu    sync.Mutex
	body  []byte
	calls int
}

func (f *auFetcher) FetchKeySet(context.Context, string, string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.body, nil
}

func (f *auFetcher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type auSeen struct {
	mu   sync.Mutex
	last http.Header
}

func (s *auSeen) add(h http.Header) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = h
}

func (s *auSeen) get() http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

func auBackend(t *testing.T, seen *auSeen) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.add(r.Header.Clone())
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func auECKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func auKeySet(t *testing.T, key *ecdsa.PrivateKey, kid string) []byte {
	t.Helper()
	pub := key.PublicKey
	doc, err := json.Marshal(map[string]any{"keys": []map[string]string{{
		"kty": "EC",
		"crv": "P-256",
		"kid": kid,
		"alg": "ES256",
		"use": "sig",
		"x":   base64.RawURLEncoding.EncodeToString(pub.X.FillBytes(make([]byte, 32))),
		"y":   base64.RawURLEncoding.EncodeToString(pub.Y.FillBytes(make([]byte, 32))),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func auClaims() jwt.MapClaims {
	return jwt.MapClaims{
		"iss": auIssuer,
		"aud": "api",
		"sub": "user-1",
		"exp": auStart.Add(time.Hour).Unix(),
	}
}

func auSignES256(t *testing.T, key *ecdsa.PrivateKey, kid string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, auClaims())
	tok.Header["kid"] = kid
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func auSignHS256(t *testing.T) string {
	t.Helper()
	s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, auClaims()).SignedString([]byte(auSecret))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func auBuild(t *testing.T, reg *state.Registry, doc string, d Deps) (*Snapshot, *state.Lease) {
	t.Helper()
	// a snapshot's limits and timers run on a fake clock unless the test passes its own
	if d.Clock == nil {
		d.Clock = clock.NewFake(auStart)
	}
	tx := reg.Begin()
	s, err := Build(mustParse(t, doc), d, tx)
	if err != nil {
		tx.Abort()
		t.Fatal(err)
	}
	lease := tx.Commit()
	t.Cleanup(func() { _ = lease.Release() })
	s.Activate()
	return s, lease
}

func auGet(t *testing.T, s *Snapshot, path string, hdr http.Header) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

const auJWKSHead = `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: be
    hosts:
      - url: %q
`

const auJWKSProvider = `auth:
  jwt:
    - name: idp
      issuer: https://idp.example
      audiences: [api]
      jwks:
        url: https://idp.example/jwks
        algorithms: [ES256]
        ttl: %s
`

const auJWKSRoutes = `routes:
  - name: extra
    match:
      path: %s
    upstream: be
  - name: api
    match:
      path: /api
    upstream: be
    auth:
      mode: jwt
      providers: [idp]
`

const auJWKSRoutesNoAuth = `routes:
  - name: extra
    match:
      path: %s
    upstream: be
`

func auJWKSDoc(backend, ttl, extraPath string, provider bool) string {
	if !provider {
		return fmt.Sprintf(auJWKSHead, backend) + fmt.Sprintf(auJWKSRoutesNoAuth, extraPath)
	}
	return fmt.Sprintf(auJWKSHead, backend) + fmt.Sprintf(auJWKSProvider, ttl) + fmt.Sprintf(auJWKSRoutes, extraPath)
}

func TestBuildAuthJWKSStateCarryOver(t *testing.T) {
	backend := auBackend(t, &auSeen{})
	key := auECKey(t)
	fetch := &auFetcher{body: auKeySet(t, key, "k1")}
	clk := clock.NewFake(auStart)
	deps := Deps{Clock: clk, KeySetFetcher: fetch}
	token := auSignES256(t, key, "k1")
	hdr := http.Header{"Authorization": {"Bearer " + token}}
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)

	s1, l1 := auBuild(t, reg, auJWKSDoc(backend, "10m", "/extra", true), deps)
	if resp := auGet(t, s1, "/api/x", hdr); resp.Code != http.StatusOK {
		t.Fatalf("build 1: status = %d, want 200", resp.Code)
	}
	if n := fetch.count(); n != 1 {
		t.Fatalf("build 1: fetches = %d, want 1", n)
	}

	s2, l2 := auBuild(t, reg, auJWKSDoc(backend, "10m", "/other", true), deps)
	if resp := auGet(t, s2, "/api/x", hdr); resp.Code != http.StatusOK {
		t.Fatalf("build 2: status = %d, want 200", resp.Code)
	}
	if n := fetch.count(); n != 1 {
		t.Fatalf("build 2: fetches = %d, want 1 (key set carried over)", n)
	}
	if err := l1.Release(); err != nil {
		t.Fatal(err)
	}

	s3, l3 := auBuild(t, reg, auJWKSDoc(backend, "5m", "/other", true), deps)
	if resp := auGet(t, s3, "/api/x", hdr); resp.Code != http.StatusOK {
		t.Fatalf("build 3: status = %d, want 200", resp.Code)
	}
	if n := fetch.count(); n != 2 {
		t.Fatalf("build 3: fetches = %d, want 2 (ttl changed)", n)
	}
	if err := l2.Release(); err != nil {
		t.Fatal(err)
	}
	lenWithProvider := reg.Len()

	abortTx := reg.Begin()
	badDoc := auJWKSDoc(backend, "5m", "/other", true) + "middlewares:\n  - name: bad\n    type: nope\n"
	if _, err := Build(mustParse(t, badDoc), deps, abortTx); err == nil {
		abortTx.Abort()
		t.Fatal("aborted build: error = nil, want unknown middleware type")
	}
	abortTx.Abort()
	if n := reg.Len(); n != lenWithProvider {
		t.Errorf("registry Len = %d after aborted build, want %d", n, lenWithProvider)
	}
	if n := fetch.count(); n != 2 {
		t.Errorf("fetches = %d after aborted build, want 2", n)
	}

	_, l4 := auBuild(t, reg, auJWKSDoc(backend, "", "/other", false), deps)
	if err := l3.Release(); err != nil {
		t.Fatal(err)
	}
	if n := reg.Len(); n != lenWithProvider-1 {
		t.Errorf("registry Len = %d after provider removed, want %d", n, lenWithProvider-1)
	}
	if err := l4.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestBuildAuthStripOnNoneRoute(t *testing.T) {
	seen := &auSeen{}
	backend := auBackend(t, seen)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)

	doc := fmt.Sprintf(`version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: be
    hosts:
      - url: %q
auth:
  jwt:
    - name: idp
      issuer: https://idp.example
      audiences: [api]
      keys:
        - algorithm: HS256
          secret: %q
      forward_claims:
        sub: X-Tier
routes:
  - name: open
    match:
      path: /open
    upstream: be
    timeout: 5s
    pipeline: [timeout]
`, backend, auSecret)
	s, _ := auBuild(t, reg, doc, Deps{})
	hdr := http.Header{"X-User-ID": {"spoof"}, "X-Tier": {"gold"}}
	if resp := auGet(t, s, "/open/x", hdr); resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.Code)
	}
	got := seen.get()
	if v := got.Get("X-User-ID"); v != "" {
		t.Errorf("backend saw X-User-ID = %q, want it stripped", v)
	}
	if v := got.Get("X-Tier"); v != "" {
		t.Errorf("backend saw X-Tier = %q, want it stripped", v)
	}

	open := `version: 1
listeners:
  - name: public
    address: ":8080"
global:
  strip_identity_headers: []
upstreams:
  - name: be
    hosts:
      - url: %q
routes:
  - name: open
    match:
      path: /open
    upstream: be
    timeout: 5s
    pipeline: [timeout]
`
	s2, _ := auBuild(t, reg, fmt.Sprintf(open, backend), Deps{})
	if resp := auGet(t, s2, "/open/x", http.Header{"X-User-ID": {"spoof"}}); resp.Code != http.StatusOK {
		t.Fatalf("empty strip list: status = %d, want 200", resp.Code)
	}
	if v := seen.get().Get("X-User-ID"); v != "spoof" {
		t.Errorf("backend saw X-User-ID = %q with an empty strip list, want %q", v, "spoof")
	}
}

func TestBuildAuthStageOrder(t *testing.T) {
	backend := auBackend(t, &auSeen{})
	var got struct{ identity, deadline bool }
	mreg := middleware.NewRegistry()
	if err := mreg.Register("recorder", func(config.Middleware) (middleware.Middleware, error) {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, got.identity = reqctx.IdentityFrom(r.Context())
				_, got.deadline = r.Context().Deadline()
				next.ServeHTTP(w, r)
			})
		}, nil
	}); err != nil {
		t.Fatal(err)
	}
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)

	doc := fmt.Sprintf(`version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: be
    hosts:
      - url: %q
middlewares:
  - name: rec
    type: recorder
auth:
  jwt:
    - name: idp
      issuer: https://idp.example
      audiences: [api]
      keys:
        - algorithm: HS256
          secret: %q
routes:
  - name: ordered
    match:
      path: /ordered
    upstream: be
    timeout: 5s
    auth:
      mode: jwt
      providers: [idp]
    pipeline: [timeout, auth, rec]
  - name: reversed
    match:
      path: /reversed
    upstream: be
    timeout: 5s
    auth:
      mode: jwt
      providers: [idp]
    pipeline: [rec, auth]
`, backend, auSecret)
	s, _ := auBuild(t, reg, doc, Deps{Middlewares: mreg, Clock: clock.NewFake(auStart)})
	hdr := http.Header{"Authorization": {"Bearer " + auSignHS256(t)}}

	if resp := auGet(t, s, "/ordered/x", hdr); resp.Code != http.StatusOK {
		t.Fatalf("ordered: status = %d, want 200", resp.Code)
	}
	if !got.identity {
		t.Error("ordered: recorder saw no identity, want the identity set by auth")
	}
	if !got.deadline {
		t.Error("ordered: recorder saw no deadline, want the timeout stage before it")
	}

	if resp := auGet(t, s, "/reversed/x", hdr); resp.Code != http.StatusOK {
		t.Fatalf("reversed: status = %d, want 200", resp.Code)
	}
	if got.identity {
		t.Error("reversed: recorder saw an identity, want none because rec runs before auth")
	}
}
