package microgateway

import (
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

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
	g := e.gateway(aobDoc(be.URL, hash), WithKeySetFetcher(f))

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
	g := apkFromFile(t, e, cfgPath, withClock(e.clk), WithKeySetFetcher(f))

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
