package microgateway

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

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
func auJWTProbe(MiddlewareParams) (Middleware, error) {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if id, ok := IdentityFromContext(r.Context()); ok {
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
		WithMiddleware("idprobe", auJWTProbe))
	rs := e.gateway(auJWTDoc(be.URL, "", keysFor("RS256", "RS384", "RS512")),
		WithMiddleware("idprobe", auJWTProbe))
	es := e.gateway(auJWTDoc(be.URL, "", keysFor("ES256", "ES384", "ES512")),
		WithMiddleware("idprobe", auJWTProbe))
	ed := e.gateway(auJWTDoc(be.URL, "", keysFor("EdDSA")),
		WithMiddleware("idprobe", auJWTProbe))

	valid := []struct {
		alg string
		g   *Gateway
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
		g      *Gateway
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
	withDefault := e.gateway(auJWTDoc(be.URL, "", keys), WithMiddleware("idprobe", auJWTProbe))
	noLeeway := e.gateway(auJWTDoc(be.URL, "      leeway: 0s\n", keys), WithMiddleware("idprobe", auJWTProbe))

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
