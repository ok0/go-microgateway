//go:build realnet

// The tests in this file repeat the reload scenarios over loopback TCP. They open hundreds of loopback connections and are run explicitly with -tags realnet.
package e2e

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestFileEditUnderLoadTCP(t *testing.T) {
	runFileEditUnderLoad(t, tcpNetwork{})
}

func TestReloadStressConsistentTCP(t *testing.T) {
	runReloadStressConsistent(t, tcpNetwork{})
}

func TestInFlightSurvivesRemovalTCP(t *testing.T) {
	runInFlightSurvivesRemoval(t, tcpNetwork{})
}

func TestNoLeakAfterManyReloadsTCP(t *testing.T) {
	runNoLeakAfterManyReloads(t, tcpNetwork{})
}

func TestKeepAliveAcrossReloadTCP(t *testing.T) {
	runKeepAliveAcrossReload(t, tcpNetwork{})
}

func TestCarryOverPooledConnectionTCP(t *testing.T) {
	runCarryOverPooledConnection(t, tcpNetwork{})
}

func TestCarryOverInFlightTCP(t *testing.T) {
	runCarryOverInFlight(t, tcpNetwork{})
}

func TestTransportRecreatedOnTimeoutChangeTCP(t *testing.T) {
	runTransportRecreatedOnTimeoutChange(t, tcpNetwork{})
}

func TestRenameResetsStateTCP(t *testing.T) {
	runRenameResetsState(t, tcpNetwork{})
}

func TestPassiveEjectionTCP(t *testing.T) {
	runPassiveEjection(t, tcpNetwork{})
}

func TestActiveHealthTCP(t *testing.T) {
	runActiveHealth(t, tcpNetwork{})
}

func TestTransportLimitsTCP(t *testing.T) {
	runTransportLimits(t, tcpNetwork{})
}

func TestBreakerOpenCloseTCP(t *testing.T) {
	runBreakerOpenClose(t, tcpNetwork{})
}

func TestBreakerSelectionTCP(t *testing.T) {
	runBreakerSelection(t, tcpNetwork{})
}

func TestRetryBasicsTCP(t *testing.T) {
	runRetryBasics(t, tcpNetwork{})
}

func TestRetryIdempotencyTCP(t *testing.T) {
	runRetryIdempotency(t, tcpNetwork{})
}

func TestRetryBodyReplayTCP(t *testing.T) {
	runRetryBodyReplay(t, tcpNetwork{})
}

func TestRetryStreamingTCP(t *testing.T) {
	runRetryStreaming(t, tcpNetwork{})
}

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
