//go:build realnet

package lychgate

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestRealnetAuthJWKS(t *testing.T) {
	k := auTestKeys(t)
	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(auJWKS(auJWK("k1", "RS256", &k.rsa1.PublicKey)))
	}))
	t.Cleanup(idp.Close)
	be := backend(t, "be")

	authYAML := "auth:\n  jwt:\n    - name: idp\n      issuer: " + auIssuer + "\n      audiences:\n        - " + auAudience +
		"\n      jwks:\n        url: " + strconv.Quote(idp.URL+"/jwks") + "\n        algorithms:\n          - RS256\n"
	routes := "  - name: jwt\n    upstream: be\n    auth:\n      mode: jwt\n      providers:\n        - idp\n"
	g := gwWith(t, auDoc(be.URL, authYAML, routes))

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": auIssuer,
		"aud": auAudience,
		"sub": "user-1",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	tok.Header["kid"] = "k1"
	signed, err := tok.SignedString(k.rsa1)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/jwt", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}
