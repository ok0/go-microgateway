package microgateway

import (
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

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
	id Identity
	ok bool
}

func (p *aidProbe) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := IdentityFromContext(r.Context())
		p.mu.Lock()
		p.id, p.ok = id, ok
		p.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func (p *aidProbe) seen() (Identity, bool) {
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
	g := e.gateway(auDoc(be.URL, auth, routes), WithMiddleware("probe", func(MiddlewareParams) (Middleware, error) {
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
