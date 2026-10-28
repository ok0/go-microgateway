package microgateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

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
		WithKeySetFetcher(f), WithHTTPClient(auFailingClient(t)))
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
		WithKeySetFetcher(f), WithHTTPClient(auFailingClient(t)))
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
				WithKeySetFetcher(f), WithHTTPClient(auFailingClient(t)))
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
		WithKeySetFetcher(f), WithHTTPClient(auFailingClient(t)))
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
		WithKeySetFetcher(h), WithHTTPClient(auFailingClient(t)))
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
		WithKeySetFetcher(f), WithHTTPClient(auFailingClient(t)))
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
		WithKeySetFetcher(f), WithHTTPClient(auFailingClient(t)))
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
				WithHTTPClient(e.n.client(t, nil)))
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
	g1 := e.gateway(doc, WithKeySetFetcher(f), WithHTTPClient(auFailingClient(t)))
	g2 := e.gateway(doc, WithKeySetFetcher(f), WithHTTPClient(auFailingClient(t)))
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
		WithKeySetFetcher(f), WithHTTPClient(auFailingClient(t)))
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

	before := g.state.Len()
	if err := g.ReloadYAML([]byte(auJWKSDoc(be.URL, jwksTestURL, "", "3", false))); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return g.state.Len() == before-1 })
}
