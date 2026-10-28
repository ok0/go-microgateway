package microgateway

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
)

const rlStart = 1_700_000_040

func rlFake() *clock.Fake {
	return clock.NewFake(time.Unix(rlStart, 0))
}

func rlAuEnv(t *testing.T) *auEnv {
	t.Helper()
	e := newAuEnv(t)
	e.clk = rlFake()
	return e
}

func rlDoc(backendURL, global, routesYAML string) string {
	return "version: 1\n" + global + listenersYAML +
		fmt.Sprintf("upstreams:\n  - name: up\n    hosts:\n      - url: %q\n", backendURL) +
		"routes:\n" + routesYAML
}

func rlRoute(name, body string) string {
	return fmt.Sprintf("  - name: %s\n    match:\n      path: /%s\n    upstream: up\n", name, name) + body
}

func rlBackend(t *testing.T, n *memNetwork) (*testServer, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	h := echoHandler("rl")
	s := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		h.ServeHTTP(w, r)
	}))
	return s, &hits
}

func rlGet(t *testing.T, g *Gateway, target, remote string, hdr http.Header) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.RemoteAddr = remote
	for name, vals := range hdr {
		for _, v := range vals {
			req.Header.Add(name, v)
		}
	}
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	return rec
}

func rlExpectAllowed(t *testing.T, rec *httptest.ResponseRecorder, limit, remaining, reset string) {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	for name, want := range map[string]string{"RateLimit-Limit": limit, "RateLimit-Remaining": remaining, "RateLimit-Reset": reset} {
		if got := rec.Header().Get(name); got != want {
			t.Fatalf("%s = %q, want %q", name, got, want)
		}
	}
}

func rlExpectRemaining(t *testing.T, rec *httptest.ResponseRecorder, remaining string) {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("RateLimit-Remaining"); got != remaining {
		t.Fatalf("RateLimit-Remaining = %q, want %q", got, remaining)
	}
}

func rlExpectLimited(t *testing.T, rec *httptest.ResponseRecorder, retryAfter string) {
	t.Helper()
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != retryAfter {
		t.Fatalf("Retry-After = %q, want %q", got, retryAfter)
	}
	e := decodeErr(t, rec.Result())
	if e.Code != "rate_limited" {
		t.Fatalf("code = %q, want rate_limited", e.Code)
	}
	if e.RequestID == "" {
		t.Fatal("request_id is empty")
	}
}

func rlExhaust(t *testing.T, g *Gateway, path, remote string, hdr http.Header) {
	t.Helper()
	for i := range 3 {
		rlExpectRemaining(t, rlGet(t, g, path, remote, hdr), strconv.Itoa(2-i))
	}
	rlExpectLimited(t, rlGet(t, g, path, remote, hdr), "1200")
}

func TestRateLimitTokenBucket(t *testing.T) {
	n := newMemNetwork()
	fake := rlFake()
	be, hits := rlBackend(t, n)
	g := n.gateway(t, rlDoc(be.URL, "", rlRoute("tb", "    rate_limit:\n      rate: 5\n      per: 1s\n      burst: 3\n")), withClock(fake))
	const client = "192.0.2.10:4000"

	rlExpectAllowed(t, rlGet(t, g, "/tb", client, nil), "3", "2", "1")
	rlExpectAllowed(t, rlGet(t, g, "/tb", client, nil), "3", "1", "1")
	rlExpectAllowed(t, rlGet(t, g, "/tb", client, nil), "3", "0", "1")
	rlExpectLimited(t, rlGet(t, g, "/tb", client, nil), "1")
	if got := hits.Load(); got != 3 {
		t.Fatalf("backend requests = %d, want 3", got)
	}

	fake.Advance(199 * time.Millisecond)
	rlExpectLimited(t, rlGet(t, g, "/tb", client, nil), "1")

	fake.Advance(time.Millisecond)
	if rec := rlGet(t, g, "/tb", client, nil); rec.Code != http.StatusOK {
		t.Fatalf("status after 200ms = %d, want 200", rec.Code)
	}
}

func TestRateLimitSlidingWindow(t *testing.T) {
	n := newMemNetwork()
	fake := rlFake()
	be, _ := rlBackend(t, n)
	g := n.gateway(t, rlDoc(be.URL, "", rlRoute("sw", "    rate_limit:\n      algorithm: sliding_window\n      limit: 10\n      window: 1m\n")), withClock(fake))
	const client = "192.0.2.20:4000"

	for i := range 10 {
		rlExpectAllowed(t, rlGet(t, g, "/sw", client, nil), "10", strconv.Itoa(9-i), "120")
	}
	rlExpectLimited(t, rlGet(t, g, "/sw", client, nil), "66")

	fake.Advance(65999 * time.Millisecond)
	rlExpectLimited(t, rlGet(t, g, "/sw", client, nil), "1")

	fake.Advance(time.Millisecond)
	rlExpectAllowed(t, rlGet(t, g, "/sw", client, nil), "10", "0", "114")
}

func TestRateLimitKeySources(t *testing.T) {
	const hourly3 = "    rate_limit:\n      rate: 3\n      per: 1h\n"

	t.Run("ip", func(t *testing.T) {
		e := rlAuEnv(t)
		be, _ := auBackend(e)
		g := e.gateway(auDoc(be.URL, "", amdRoute("ip", "/ip", hourly3)))
		rlExhaust(t, g, "/ip", "192.0.2.1:1000", nil)
		rlExhaust(t, g, "/ip", "192.0.2.2:1000", nil)
	})

	t.Run("trusted proxy", func(t *testing.T) {
		e := rlAuEnv(t)
		be, _ := auBackend(e)
		g := e.gateway(rlDoc(be.URL, trustedGlobal, rlRoute("tp", hourly3)))
		xff := func(v string) http.Header { return http.Header{"X-Forwarded-For": {v}} }

		rlExhaust(t, g, "/tp", "203.0.113.9:4000", xff("1.2.3.4"))
		rlExpectRemaining(t, rlGet(t, g, "/tp", "203.0.113.9:4000", xff("1.2.3.5")), "2")

		for i, v := range []string{"1.2.3.4", "1.2.3.10", "1.2.3.11"} {
			rlExpectRemaining(t, rlGet(t, g, "/tp", "198.51.100.7:4000", xff(v)), strconv.Itoa(2-i))
		}
		rlExpectLimited(t, rlGet(t, g, "/tp", "198.51.100.7:4000", xff("1.2.3.12")), "1200")
	})

	t.Run("consumer", func(t *testing.T) {
		e := rlAuEnv(t)
		be, _ := auBackend(e)
		secret := e.secret(auHMAC("HS256"))
		keyA := e.secret("rl-consumer-alice-5a4b3c2d1e0f9a8b")
		keyB := e.secret("rl-consumer-bob-1f2e3d4c5b6a7980")
		authYAML := fmt.Sprintf(`auth:
  jwt:
    - name: jwt1
      issuer: %s
      audiences: [%s]
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
        - id: k2
          hash: %q
          consumer: bob
`, auIssuer, auAudience, secret, e.secret(auHash(keyA)), e.secret(auHash(keyB)))
		route := amdRoute("cons", "/cons", "    auth:\n      mode: any\n      providers: [jwt1, keys1]\n      allow_anonymous: true\n"+
			"    rate_limit:\n      rate: 3\n      per: 1h\n      key:\n        source: consumer\n")
		g := e.gateway(auDoc(be.URL, authYAML, route))
		keyHdr := func(k string) http.Header { return http.Header{"X-API-Key": {k}} }
		token := e.mint("HS256", "h1", []byte(secret), e.claims(nil))
		jwtHdr := http.Header{"Authorization": {"Bearer " + token}}

		rlExhaust(t, g, "/cons", "192.0.2.30:1", keyHdr(keyA))
		rlExhaust(t, g, "/cons", "192.0.2.30:1", keyHdr(keyB))
		rlExhaust(t, g, "/cons", "192.0.2.30:1", jwtHdr)
		rlExhaust(t, g, "/cons", "192.0.2.50:1", nil)
	})

	t.Run("header", func(t *testing.T) {
		e := rlAuEnv(t)
		be, _ := auBackend(e)
		g := e.gateway(auDoc(be.URL, "", amdRoute("hdr", "/hdr",
			"    rate_limit:\n      rate: 3\n      per: 1h\n      key:\n        source: header\n        name: X-Tenant\n")))
		tenant := func(v string) http.Header { return http.Header{"X-Tenant": {v}} }

		rlExhaust(t, g, "/hdr", "192.0.2.1:1", tenant("t1"))
		rlExpectLimited(t, rlGet(t, g, "/hdr", "192.0.2.2:1", tenant("t1")), "1200")
		rlExpectRemaining(t, rlGet(t, g, "/hdr", "192.0.2.1:1", tenant("t2")), "2")
		rlExhaust(t, g, "/hdr", "192.0.2.1:1", nil)
		rlExhaust(t, g, "/hdr", "192.0.2.3:1", tenant(strings.Repeat("v", 200)))
	})

	t.Run("route", func(t *testing.T) {
		e := rlAuEnv(t)
		be, _ := auBackend(e)
		g := e.gateway(auDoc(be.URL, "", amdRoute("all", "/all",
			"    rate_limit:\n      rate: 3\n      per: 1h\n      key:\n        source: route\n")))
		for i, ip := range []string{"192.0.2.1:1", "192.0.2.2:1", "192.0.2.3:1"} {
			rlExpectRemaining(t, rlGet(t, g, "/all", ip, nil), strconv.Itoa(2-i))
		}
		rlExpectLimited(t, rlGet(t, g, "/all", "192.0.2.4:1", nil), "1200")
	})

	t.Run("failed authentication is not counted", func(t *testing.T) {
		e := rlAuEnv(t)
		be, _ := auBackend(e)
		key := e.secret("rl-guard-key-0a1b2c3d4e5f6071")
		wrong := e.secret("rl-guard-wrong-9f8e7d6c5b4a3921")
		authYAML := amdAuthYAML(e.secret(auHMAC("HS256")), e.secret(auHash(key)))
		g := e.gateway(auDoc(be.URL, authYAML, amdRoute("guard", "/guard",
			"    auth:\n      mode: apikey\n      providers: [keys1]\n"+
				"    rate_limit:\n      rate: 3\n      per: 1h\n      key:\n        source: ip\n")))
		for range 10 {
			auAssert401(t, rlGet(t, g, "/guard", "192.0.2.77:1", http.Header{"X-API-Key": {wrong}}), "ApiKey")
		}
		rlExpectRemaining(t, rlGet(t, g, "/guard", "192.0.2.77:1", http.Header{"X-API-Key": {key}}), "2")
	})
}

func TestRateLimitPerInstance(t *testing.T) {
	n := newMemNetwork()
	fake := rlFake()
	be, _ := rlBackend(t, n)
	doc := rlDoc(be.URL, "", rlRoute("pi", "    rate_limit:\n      rate: 3\n      per: 1h\n"))
	for _, g := range []*Gateway{n.gateway(t, doc, withClock(fake)), n.gateway(t, doc, withClock(fake))} {
		rlExhaust(t, g, "/pi", "192.0.2.1:1", nil)
	}
}

func TestRateLimitMetrics(t *testing.T) {
	n := newMemNetwork()
	fake := rlFake()
	be, _ := rlBackend(t, n)
	g := n.gateway(t, rlDoc(be.URL, "", rlRoute("rl", "    rate_limit:\n      rate: 2\n      per: 1h\n")), withClock(fake))

	if got := rsMetric(t, g, "microgateway_rate_limited_total", "route", "rl"); got != 0 {
		t.Fatalf("rate_limited_total before requests = %v, want 0", got)
	}
	if got := rsMetric(t, g, "microgateway_ratelimit_keys", "route", "rl"); got != 0 {
		t.Fatalf("ratelimit_keys before requests = %v, want 0", got)
	}

	limited := 0
	for _, step := range []struct {
		remote string
		want   int
	}{
		{"192.0.2.1:1", http.StatusOK},
		{"192.0.2.1:1", http.StatusOK},
		{"192.0.2.1:1", http.StatusTooManyRequests},
		{"192.0.2.1:1", http.StatusTooManyRequests},
		{"192.0.2.2:1", http.StatusOK},
	} {
		rec := rlGet(t, g, "/rl", step.remote, nil)
		if rec.Code != step.want {
			t.Fatalf("status for %s = %d, want %d", step.remote, rec.Code, step.want)
		}
		if rec.Code == http.StatusTooManyRequests {
			limited++
		}
	}

	if got := rsMetric(t, g, "microgateway_rate_limited_total", "route", "rl"); got != float64(limited) {
		t.Fatalf("rate_limited_total = %v, want %d", got, limited)
	}
	if got := rsMetric(t, g, "microgateway_ratelimit_keys", "route", "rl"); got != 2 {
		t.Fatalf("ratelimit_keys = %v, want 2", got)
	}
	rsCheckExposition(t, metricsText(t, g))
}
