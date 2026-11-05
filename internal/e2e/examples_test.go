package e2e

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/doccheck"
)

func TestMemoryExampleRunThrough(t *testing.T) {
	root, err := doccheck.Root()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, "examples", "memory.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	n := newMemNetwork()
	var catalogCalls atomic.Int64
	catalog := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		catalogCalls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		_, _ = fmt.Fprintf(w, "catalog %s", r.URL.Path)
	}))
	orders := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "orders %s", r.URL.Path)
	}))
	fake := clock.NewFake(auStart)
	g := n.gateway(t, string(data),
		microgateway.WithEnvLookup(doccheck.ExampleLookup(map[string]string{"CATALOG_URL": catalog.URL, "ORDERS_URL": orders.URL})),
		withClock(fake))

	const client = "192.0.2.10:4000"
	const catalogItems = "http://example.com/catalog/items"
	const ordersOne = "http://example.com/orders/1"

	first := rlGet(t, g, catalogItems, client, nil)
	if first.Code != http.StatusOK || first.Body.String() != "catalog /catalog/items" {
		t.Fatalf("first catalog GET = %d %q, want 200 %q", first.Code, first.Body.String(), "catalog /catalog/items")
	}
	if cacheLast(t, first).Hit {
		t.Fatal("first catalog GET is a cache hit, want miss")
	}
	second := rlGet(t, g, catalogItems, client, nil)
	if !cacheLast(t, second).Hit {
		t.Fatal("second catalog GET is a miss, want hit")
	}
	if got := catalogCalls.Load(); got != 1 {
		t.Fatalf("catalog calls after two GETs = %d, want 1", got)
	}

	anonymous := rlGet(t, g, ordersOne, client, nil)
	if anonymous.Code != http.StatusUnauthorized {
		t.Fatalf("orders GET without credentials = %d, want 401", anonymous.Code)
	}
	if e := decodeErr(t, anonymous.Result()); e.Code != "unauthorized" {
		t.Fatalf("orders GET without credentials code = %q, want unauthorized", e.Code)
	}

	apiKey := http.Header{}
	apiKey.Set("X-API-Key", "example-api-key-alice")
	if rec := rlGet(t, g, ordersOne, client, apiKey); rec.Code != http.StatusOK || rec.Body.String() != "orders /orders/1" {
		t.Fatalf("orders GET with API key = %d %q, want 200 %q", rec.Code, rec.Body.String(), "orders /orders/1")
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": "https://auth.example.com",
		"aud": "example-api",
		"sub": "alice",
		"exp": fake.Now().Add(time.Hour).Unix(),
	})
	tok.Header["kid"] = "example-hs256"
	signed, err := tok.SignedString([]byte("example-jwt-secret-for-hs256-signing-only"))
	if err != nil {
		t.Fatal(err)
	}
	bearer := http.Header{}
	bearer.Set("Authorization", "Bearer "+signed)
	if rec := rlGet(t, g, ordersOne, client, bearer); rec.Code != http.StatusOK || rec.Body.String() != "orders /orders/1" {
		t.Fatalf("orders GET with JWT = %d %q, want 200 %q", rec.Code, rec.Body.String(), "orders /orders/1")
	}

	purge := func(authorization string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/cache/purge", strings.NewReader(`{"urls":["`+catalogItems+`"]}`))
		req.Header.Set("Content-Type", "application/json")
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		rec := httptest.NewRecorder()
		g.AdminHandler().ServeHTTP(rec, req)
		return rec
	}
	if rec := purge(""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("purge without Authorization = %d, want 401", rec.Code)
	}
	if rec := purge("Bearer wrong-token"); rec.Code != http.StatusForbidden {
		t.Fatalf("purge with wrong token = %d, want 403", rec.Code)
	}
	rec := purge("Bearer example-admin-token")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"scope":"instance"`) {
		t.Fatalf("purge with admin token = %d %q, want 200 with scope instance", rec.Code, rec.Body.String())
	}

	if after := rlGet(t, g, catalogItems, client, nil); cacheLast(t, after).Hit {
		t.Fatal("catalog GET after purge is a hit, want miss")
	}
	if got := catalogCalls.Load(); got != 2 {
		t.Fatalf("catalog calls after purge = %d, want 2", got)
	}

	const burst = "192.0.2.50:1000"
	for i := range 10 {
		if rec := rlGet(t, g, "http://example.com/catalog/other", burst, nil); rec.Code == http.StatusTooManyRequests {
			t.Fatalf("burst request %d limited, want allowed", i+1)
		}
	}
	limited := rlGet(t, g, "http://example.com/catalog/other", burst, nil)
	if limited.Code != http.StatusTooManyRequests {
		t.Fatalf("burst request 11 = %d, want 429", limited.Code)
	}
	retry, err := strconv.Atoi(limited.Header().Get("Retry-After"))
	if err != nil || retry < 1 {
		t.Fatalf("Retry-After = %q, want an integer of at least 1", limited.Header().Get("Retry-After"))
	}
	if e := decodeErr(t, limited.Result()); e.Code != "rate_limited" {
		t.Fatalf("burst request 11 code = %q, want rate_limited", e.Code)
	}

	if body := adminBody(t, g, "/metrics"); !strings.Contains(body, "microgateway_requests_total") {
		t.Fatal("metrics do not contain microgateway_requests_total")
	}
}
