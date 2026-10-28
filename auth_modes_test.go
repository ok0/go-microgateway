package microgateway

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func amdRoute(name, path, body string) string {
	return fmt.Sprintf("  - name: %s\n    match:\n      path: %s\n    upstream: be\n", name, path) + body
}

func amdAuthYAML(secret, hash string) string {
	return fmt.Sprintf(`auth:
  jwt:
    - name: jwt1
      issuer: https://idp.example
      audiences: [api]
      keys:
        - kid: h1
          algorithm: HS256
          secret: %q
  api_keys:
    - name: keys1
      keys:
        - id: k1
          hash: %q
          consumer: alice
          scopes: [read]
          roles: [user]
          metadata:
            tier: gold
`, secret, hash)
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
