package redis

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
)

const outageClientAddr = "192.0.2.20:4000"

func outageRouteBody(policy string) string {
	return "rate: 1\nper: 1h\nburst: 2\nstore: shared\non_store_error: " + policy + "\nstore_timeout: 200ms\n"
}

// outageStatuses lists the status of each request sent while the store is down.
// The local limiter admits two requests and then denies.
func outageStatuses(policy string) []int {
	switch policy {
	case "allow":
		return []int{200, 200, 200, 200, 200}
	case "deny":
		return []int{503, 503, 503, 503, 503}
	default:
		return []int{200, 200, 429, 429, 429}
	}
}

// openGuardStatuses lists the status of each request sent while the guard is
// open. The local limiter is exhausted by then.
func openGuardStatuses(policy string) []int {
	switch policy {
	case "allow":
		return []int{200, 200, 200}
	case "deny":
		return []int{503, 503, 503}
	default:
		return []int{429, 429, 429}
	}
}

// checkOutageReply checks one reply sent while the store is unavailable or
// after it has recovered from an outage. Allowed replies under the allow policy
// carry no rate limit headers.
func checkOutageReply(t *testing.T, policy string, label string, rec *httptest.ResponseRecorder, status int) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("%s: status %d, want %d", label, rec.Code, status)
	}
	switch status {
	case http.StatusOK:
		if policy == "allow" {
			for _, h := range []string{"RateLimit-Limit", "RateLimit-Remaining", "RateLimit-Reset"} {
				if v := rec.Header().Get(h); v != "" {
					t.Errorf("%s: header %s = %q, want none under allow", label, h, v)
				}
			}
		}
	case http.StatusServiceUnavailable:
		if code := responseErrorCode(t, rec); code != "rate_limit_unavailable" {
			t.Errorf("%s: error code %q, want rate_limit_unavailable", label, code)
		}
	case http.StatusTooManyRequests:
		if code := responseErrorCode(t, rec); code != "rate_limited" {
			t.Errorf("%s: error code %q, want rate_limited", label, code)
		}
	}
}

func checkSetupOnlyHello(t *testing.T, p *respProxy, label string) {
	t.Helper()
	for _, c := range p.Setup() {
		if c != "HELLO" {
			t.Errorf("%s: setup command %q, want only HELLO", label, c)
		}
	}
}

func requireSeries(t *testing.T, body, name string, labels map[string]string, want float64) {
	t.Helper()
	got, ok := seriesValue(body, name, labels)
	if !ok {
		t.Errorf("series %s %v missing from exposition", name, labels)
		return
	}
	if got != want {
		t.Errorf("series %s %v = %v, want %v", name, labels, got, want)
	}
}

func TestOutagePolicies(t *testing.T) {
	t.Parallel()
	for _, policy := range []string{"allow", "deny", "local"} {
		t.Run(policy, func(t *testing.T) {
			runOutage(t, policy)
		})
	}
}

func runOutage(t *testing.T, policy string) {
	m := newMini(t)
	p := newRespProxy(t, m.Addr())
	fake := clock.NewFake(miniStart)
	stores := redisStoreYAML("shared", []string{p.Addr()}, "pool_size: 10\n", "consecutive_failures: 3\nopen_timeout: 2s\n")
	doc := gwDoc(stores, routeYAML("rl", outageRouteBody(policy)), upstream(t))
	g := mustGateway(t, doc, fake, nil)

	for i, want := range []string{"1", "0"} {
		rec := doGet(g, "/rl", outageClientAddr)
		if rec.Code != http.StatusOK {
			t.Fatalf("healthy request %d: status %d, want 200", i+1, rec.Code)
		}
		if got := rec.Header().Get("RateLimit-Remaining"); got != want {
			t.Errorf("healthy request %d: RateLimit-Remaining %q, want %q", i+1, got, want)
		}
	}

	// The proxy goes down with the server, so a connection a dial opens around this moment is refused or closed here,
	// never left open to a proxy that has not yet noticed the server is gone.
	p.Down()
	m.Close()
	for i, status := range outageStatuses(policy) {
		rec, d := getAdvancing(t, g, fake, 200*time.Millisecond, "/rl", outageClientAddr)
		if d > 300*time.Millisecond {
			t.Errorf("outage request %d took %s, want at most 300ms", i+1, d)
		}
		checkOutageReply(t, policy, "outage request "+strconv.Itoa(i+1), rec, status)
	}
	body := metricsText(t, g)
	checkExposition(t, body)
	requireSeries(t, body, "microgateway_store_errors_total", map[string]string{"store": "shared", "op": "take"}, 3)
	requireSeries(t, body, "microgateway_store_available", map[string]string{"store": "shared"}, 0)
	requireSeries(t, body, "microgateway_ratelimit_store_fallback_total", map[string]string{"route": "rl", "policy": policy}, 5)

	if err := m.Restart(); err != nil {
		t.Fatal(err)
	}
	if err := p.Up(); err != nil {
		t.Fatal(err)
	}
	p.Reset()
	for i, status := range openGuardStatuses(policy) {
		rec := doGet(g, "/rl", outageClientAddr)
		checkOutageReply(t, policy, "open-guard request "+strconv.Itoa(i+1), rec, status)
	}
	if got := p.Commands(); len(got) != 0 {
		t.Errorf("open guard reached the server: data-plane commands %v, want none", got)
	}
	checkSetupOnlyHello(t, p, "open guard")

	fake.Advance(2 * time.Second)
	rec := doGet(g, "/rl", outageClientAddr)
	checkOutageReply(t, policy, "recovery request", rec, http.StatusTooManyRequests)
	if v, err := strconv.Atoi(rec.Header().Get("Retry-After")); err != nil || v < 1 {
		t.Errorf("recovery request: Retry-After %q, want integer >= 1", rec.Header().Get("Retry-After"))
	}
	if got := rec.Header().Get("RateLimit-Remaining"); got != "0" {
		t.Errorf("recovery request: RateLimit-Remaining %q, want 0", got)
	}
	if got := p.Commands(); !slices.Equal(got, []string{"EVALSHA"}) {
		t.Errorf("recovery request: data-plane commands %v, want [EVALSHA] (one store call)", got)
	}
	checkSetupOnlyHello(t, p, "recovery request")
	body = metricsText(t, g)
	checkExposition(t, body)
	requireSeries(t, body, "microgateway_store_available", map[string]string{"store": "shared"}, 1)
}

func gcCheckBypass(t *testing.T, label string, rec *httptest.ResponseRecorder) {
	t.Helper()
	st := gcStatus(t, rec)
	if rec.Code != http.StatusOK || rec.Body.String() != "v1|" || st.Fwd != httpcache.FwdBypass || st.Detail != "store-error" {
		t.Fatalf("%s: status %d %q %+v, want 200 from the origin with fwd=bypass detail=store-error", label, rec.Code, rec.Body.String(), st)
	}
}

func TestCacheOutage(t *testing.T) {
	t.Parallel()
	m := newMini(t)
	p := newRespProxy(t, m.Addr())
	fake := clock.NewFake(miniStart)
	o := newGCOrigin(t)
	o.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "v1")
	stores := redisStoreYAML("shared", []string{p.Addr()}, "db: 0\nkey_prefix: "+uniquePrefix(t)+"\n", "consecutive_failures: 3\nopen_timeout: 2s\n")
	doc := gcDoc(stores, "  store: shared\n  store_timeout: 200ms\n", gcAdminYAML(gcToken), gcRoute("r"), o.url)
	g := mustGateway(t, doc, fake, nil)
	t.Cleanup(func() {
		if v := p.Violations(); len(v) > 0 {
			t.Errorf("slot co-location violations: %q", v)
		}
	})

	cached := gcEntry{method: http.MethodGet, path: "/c"}
	if rec, st := gcFetch(t, g, cached); rec.Code != http.StatusOK || !st.Stored {
		t.Fatalf("healthy fill: status %d %+v, want 200 stored", rec.Code, st)
	}
	if _, st := gcFetch(t, g, cached); !st.Hit {
		t.Fatalf("healthy repeat: status %+v, want hit", st)
	}

	p.Down()
	m.Close()
	for i := 1; i <= 3; i++ {
		rec, d := getAdvancing(t, g, fake, 50*time.Millisecond, cached.url(), gcClientAddr)
		if d > 300*time.Millisecond {
			t.Errorf("outage request %d took %s, want at most 300ms", i, d)
		}
		gcCheckBypass(t, "outage request "+strconv.Itoa(i), rec)
	}
	body := metricsText(t, g)
	checkExposition(t, body)
	requireSeries(t, body, "microgateway_cache_store_errors_total", map[string]string{"op": "get"}, 3)
	requireSeries(t, body, "microgateway_store_errors_total", map[string]string{"store": "shared", "op": "get"}, 3)
	requireSeries(t, body, "microgateway_store_available", map[string]string{"store": "shared"}, 0)

	rec := gcPurge(t, g, gcToken, `{"urls":["http://example.com/c"]}`)
	if rec.Code != http.StatusServiceUnavailable || responseErrorCode(t, rec) != "store_unavailable" {
		t.Fatalf("purge during the outage: status %d %s, want 503 store_unavailable", rec.Code, rec.Body.String())
	}
	requireSeries(t, metricsText(t, g), "microgateway_cache_purges_total", map[string]string{"type": "url", "mode": "hard"}, 0)

	if err := m.Restart(); err != nil {
		t.Fatal(err)
	}
	if err := p.Up(); err != nil {
		t.Fatal(err)
	}
	p.Reset()
	for i := 1; i <= 2; i++ {
		gcCheckBypass(t, "open-guard request "+strconv.Itoa(i), gcRequest(g, cached.method, cached.url(), nil))
	}
	if got := p.Commands(); len(got) != 0 {
		t.Errorf("open guard reached the server: data-plane commands %v, want none", got)
	}
	checkSetupOnlyHello(t, p, "open guard")

	fake.Advance(2 * time.Second)
	rec = gcRequest(g, cached.method, cached.url(), nil)
	if st := gcStatus(t, rec); rec.Code != http.StatusOK || !st.Hit || rec.Body.String() != "v1|" {
		t.Fatalf("probe request: status %d %+v body %q, want the cached entry as a hit", rec.Code, st, rec.Body.String())
	}
	if len(p.Commands()) == 0 {
		t.Error("probe request did not reach the server")
	}
	requireSeries(t, metricsText(t, g), "microgateway_store_available", map[string]string{"store": "shared"}, 1)
}

func TestCacheOutageBlackholeRealTimers(t *testing.T) {
	addr, _ := blackhole(t)
	o := newGCOrigin(t)
	o.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "v1")
	stores := redisStoreYAML("shared", []string{addr}, "db: 0\nkey_prefix: "+uniquePrefix(t)+"\n", "consecutive_failures: 3\nopen_timeout: 2s\n")
	doc := gcDoc(stores, "  store: shared\n  store_timeout: 50ms\n", "", gcRoute("r"), o.url)
	g := mustGateway(t, doc, clock.Real(), nil)
	e := gcEntry{method: http.MethodGet, path: "/b"}
	for i := 1; i <= 6; i++ {
		rec := getGuarded(t, g, e.url(), gcClientAddr)
		gcCheckBypass(t, "blackhole request "+strconv.Itoa(i), rec)
		if i == 3 {
			requireSeries(t, metricsText(t, g), "microgateway_store_available", map[string]string{"store": "shared"}, 0)
		}
	}
}

func TestOutageBlackholeRealTimers(t *testing.T) {
	route := "rate: 1\nper: 1h\nburst: 2\nstore: shared\non_store_error: deny\nstore_timeout: 50ms\n"
	t.Run("real clock", func(t *testing.T) {
		addr, _ := blackhole(t)
		stores := redisStoreYAML("shared", []string{addr}, "", "consecutive_failures: 3\nopen_timeout: 2s\n")
		doc := gwDoc(stores, routeYAML("rl", route), upstream(t))
		// Real clock: the black-holed store's timeout has to fire on the production clock.
		g := mustGateway(t, doc, clock.Real(), nil)

		for i := 1; i <= 6; i++ {
			rec := getGuarded(t, g, "/rl", outageClientAddr)
			checkOutageReply(t, "deny", "blackhole request "+strconv.Itoa(i), rec, http.StatusServiceUnavailable)
			if i == 3 {
				requireSeries(t, metricsText(t, g), "microgateway_store_available", map[string]string{"store": "shared"}, 0)
			}
		}
	})

	t.Run("fake clock", func(t *testing.T) {
		addr, _ := blackhole(t)
		stores := redisStoreYAML("shared", []string{addr}, "", "consecutive_failures: 3\nopen_timeout: 2s\n")
		doc := gwDoc(stores, routeYAML("rl", route), upstream(t))
		fake := clock.NewFake(miniStart)
		g := mustGateway(t, doc, fake, nil)

		for i := 1; i <= 6; i++ {
			rec, d := getAdvancing(t, g, fake, 50*time.Millisecond, "/rl", outageClientAddr)
			if d > 150*time.Millisecond {
				t.Errorf("request %d took %s, want at most 150ms", i, d)
			}
			checkOutageReply(t, "deny", "blackhole request "+strconv.Itoa(i), rec, http.StatusServiceUnavailable)
			if i == 3 {
				requireSeries(t, metricsText(t, g), "microgateway_store_available", map[string]string{"store": "shared"}, 0)
			}
		}
	})
}
