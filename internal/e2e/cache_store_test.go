package e2e

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/teststore"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

// cacheSharedOption registers a cache-capable store type typ whose handles share shared.
func cacheSharedOption(typ string, shared *teststore.SharedCache) microgateway.Option {
	return microgateway.WithStoreType(typ, store.CapCache, func(microgateway.StoreParams) (store.Backend, error) {
		return shared.Handle(), nil
	})
}

// cacheSharedDoc returns a configuration whose cache uses the store "sc" of type "shared", with the cache block top
// lines added to the top-level cache block and the routes.
func cacheSharedDoc(backendURL, cacheExtra, routes string) string {
	return cacheGatewayDoc(backendURL, "stores:\n  - name: sc\n    type: shared\ncache:\n  store: sc\n"+cacheExtra, routes)
}

func cacheHasSeries(body, name string) bool {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+" ") || strings.HasPrefix(line, name+"{") {
			return true
		}
	}
	return false
}

func TestCacheStoreFailureGateway(t *testing.T) {
	t.Parallel()
	n := newMemNetwork()
	scr := &cacheScript{}
	scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
	be := n.serve(t, scr)
	fake := clock.NewFake(cacheStart)
	shared := teststore.NewSharedCache(fake, 1<<20)
	t.Cleanup(func() { _ = shared.Close() })
	logs := &logBuf{}
	doc := cacheSharedDoc(be.URL, "  store_timeout: 100ms\n", cacheRouteYAML("r"))
	g := n.gateway(t, doc, withClock(fake), cacheSharedOption("shared", shared), microgateway.WithLogger(newJSONLogger(logs)))

	get := func(path string) *httptest.ResponseRecorder {
		return cacheDo(g, http.MethodGet, "http://example.com"+path, nil)
	}
	storeErrs := func(op string) float64 {
		return rsMetricOr0(t, g, "microgateway_cache_store_errors_total", "op", op)
	}
	storeErrsByStore := func(op string) float64 {
		return rsMetricOr0(t, g, "microgateway_store_errors_total", "store", "sc", "op", op)
	}
	requireBypass := func(rec *httptest.ResponseRecorder) {
		t.Helper()
		st := cacheLast(t, rec)
		if rec.Code != http.StatusOK || st.Hit || st.Fwd != httpcache.FwdBypass || st.Detail != "store-error" {
			t.Fatalf("status %d %+v, want 200 bypassed with store-error", rec.Code, st)
		}
	}

	if st := cacheLast(t, get("/a")); !st.Stored {
		t.Fatalf("prime %+v, want stored", st)
	}

	shared.SetMode(teststore.Fail, fmt.Errorf("backend down: %w", store.ErrUnavailable))
	for range 3 {
		requireBypass(get("/a"))
	}
	if v := storeErrs("get"); v != 3 {
		t.Fatalf("cache get errors = %v, want 3", v)
	}
	if v := storeErrsByStore("get"); v != 3 {
		t.Fatalf("store get errors = %v, want 3", v)
	}
	requireBypass(get("/a"))
	if v := storeErrs("get"); v != 3 {
		t.Fatalf("cache get errors after breaker open = %v, want 3 (short-circuits are not counted)", v)
	}
	if v := storeErrsByStore("get"); v != 3 {
		t.Fatalf("store get errors after breaker open = %v, want 3", v)
	}

	fake.Advance(2 * time.Second)
	shared.SetMode(teststore.Hang, nil)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- get("/a") }()
	awaitDue(t, fake, 1, 100*time.Millisecond, "store timeout timer", nil)
	fake.Advance(100 * time.Millisecond)
	rec, _ := testwait.Recv(t, "request after store timeout", done, nil)
	requireBypass(rec)
	if v := storeErrs("get"); v != 4 {
		t.Fatalf("cache get errors after timeout = %v, want 4", v)
	}
	if v := storeErrsByStore("get"); v != 4 {
		t.Fatalf("store get errors after timeout = %v, want 4", v)
	}

	fake.Advance(2 * time.Second)
	shared.SetMode(teststore.Healthy, nil)
	if st := cacheLast(t, get("/a")); !st.Hit {
		t.Fatalf("first probe after recovery %+v, want hit", st)
	}
	if st := cacheLast(t, get("/a")); !st.Hit {
		t.Fatalf("second probe after recovery %+v, want hit", st)
	}

	shared.SetOpMode("put", teststore.Fail, fmt.Errorf("put down: %w", store.ErrUnavailable))
	if rec := get("/p"); rec.Code != http.StatusOK || rec.Body.String() != "x" {
		t.Fatalf("response with failing put: code %d body %q", rec.Code, rec.Body.String())
	}
	if v := storeErrs("put"); v != 1 {
		t.Fatalf("cache put errors = %v, want 1", v)
	}
	if v := storeErrsByStore("put"); v != 1 {
		t.Fatalf("store put errors = %v, want 1", v)
	}

	shared.SetOpMode("put", teststore.Inherit, nil)
	scr.set(http.StatusCreated, nil, "")
	if rec := cacheDo(g, http.MethodPost, "http://example.com/a", nil); rec.Code != http.StatusCreated {
		t.Fatalf("POST status %d, want 201", rec.Code)
	}
	scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
	shared.SetOpMode("purge", teststore.Fail, fmt.Errorf("purge down: %w", store.ErrUnavailable))
	scr.set(http.StatusCreated, nil, "")
	if rec := cacheDo(g, http.MethodPost, "http://example.com/a", nil); rec.Code != http.StatusCreated {
		t.Fatalf("POST with failing purge status %d, want 201", rec.Code)
	}
	if v := storeErrs("purge"); v != 1 {
		t.Fatalf("cache purge errors = %v, want 1", v)
	}
	if v := storeErrsByStore("purge"); v != 1 {
		t.Fatalf("store purge errors = %v, want 1", v)
	}

	shared.SetOpMode("purge", teststore.Inherit, nil)
	scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
	if st := cacheLast(t, get("/r")); !st.Stored {
		t.Fatalf("recovery fill %+v, want stored", st)
	}
	if st := cacheLast(t, get("/r")); !st.Hit {
		t.Fatalf("recovery read %+v, want hit", st)
	}

	warns := logs.records(t, "cache store call failed; request served without the cache")
	if len(warns) == 0 {
		t.Fatal("no store failure log records")
	}
	if len(warns) != 3 {
		t.Fatalf("store failure records = %d, want one per second of failures", len(warns))
	}
	for i, w := range warns {
		if w["store"] != "sc" {
			t.Fatalf("store failure record %v does not name the store", w)
		}
		if _, ok := w["suppressed"]; !ok {
			t.Fatalf("store failure record %v has no suppressed count", w)
		}
		if want := []float64{0, 3, 0}[i]; w["suppressed"] != want {
			t.Fatalf("record %d suppressed = %v, want %v", i, w["suppressed"], want)
		}
	}
	if got := len(logs.records(t, "cache store unavailable; requests bypass the cache")); got != 1 {
		t.Fatalf("degraded records = %d, want 1", got)
	}
	recovered := logs.records(t, "cache store recovered; cache resumed")
	if len(recovered) != 1 || recovered[0]["store"] != "sc" {
		t.Fatalf("recovered records = %v, want one naming sc", recovered)
	}
}

func TestCacheMetricsGateway(t *testing.T) {
	t.Parallel()
	n := newMemNetwork()
	fake := clock.NewFake(cacheStart)
	shared := teststore.NewSharedCache(fake, 1<<20)
	t.Cleanup(func() { _ = shared.Close() })
	raceKeys, err := httpcache.URLKeys("http://example.com/race")
	if err != nil {
		t.Fatal(err)
	}
	be := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ns":
			w.Header().Set("Cache-Control", "no-store")
		case "/big":
			w.Header().Set("Cache-Control", "max-age=60")
			_, _ = io.WriteString(w, strings.Repeat("b", 3000))
			return
		case "/ab":
			w.Header().Set("Cache-Control", "max-age=60")
			w.WriteHeader(http.StatusOK)
			_ = http.NewResponseController(w).Flush()
			_, _ = io.WriteString(w, "partial")
			panic(http.ErrAbortHandler)
		case "/race":
			w.Header().Set("Cache-Control", "max-age=60")
			_, _ = shared.Purge(r.Context(), store.Purge{Kind: store.PurgeKeys, Keys: raceKeys, Retain: time.Hour})
		default:
			w.Header().Set("Cache-Control", "max-age=60")
		}
		_, _ = io.WriteString(w, strings.Repeat("a", 1000))
	}))

	memDoc := cacheGatewayDoc(be.URL, "cache:\n  max_body_bytes: 2048\n  memory:\n    max_bytes: 4096\n    shards: 1\n", cacheRouteYAML("r"))
	mem := n.gateway(t, memDoc, withClock(fake))
	get := func(g *microgateway.Gateway, path string) *httptest.ResponseRecorder {
		return cacheDo(g, http.MethodGet, "http://example.com"+path, nil)
	}
	for i := 1; i <= 5; i++ {
		if st := cacheLast(t, get(mem, fmt.Sprintf("/s%d", i))); !st.Stored {
			t.Fatalf("fill %d %+v, want stored", i, st)
		}
	}
	get(mem, "/ns")
	get(mem, "/big")
	get(mem, "/ab")
	if st := cacheLast(t, get(mem, "/s5")); !st.Hit {
		t.Fatalf("hit on /s5 %+v", st)
	}

	m := metricsText(t, mem)
	want := map[string]float64{"stored": 5, "not_storable": 1, "too_large": 1, "aborted": 1, "error": 0, "refused": 0}
	for result, v := range want {
		if got := metricValue(t, m, series("microgateway_cache_stores_total", "route", "r", "result", result)); got != v {
			t.Fatalf("stores_total result=%s = %v, want %v", result, got, v)
		}
	}
	if got := metricValue(t, m, series("microgateway_cache_requests_total", "route", "r", "result", "hit")); got != 1 {
		t.Fatalf("requests_total hit = %v, want 1", got)
	}
	evictions := metricValue(t, m, "microgateway_cache_evictions_total")
	entries := metricValue(t, m, "microgateway_cache_entries")
	bytes := metricValue(t, m, "microgateway_cache_bytes")
	if evictions <= 0 {
		t.Fatalf("evictions = %v, want more than 0 with max_bytes below the workload", evictions)
	}
	if entries+evictions != 5 {
		t.Fatalf("entries %v + evictions %v, want 5 stored entries", entries, evictions)
	}
	if bytes <= 0 || bytes > 4096 {
		t.Fatalf("bytes = %v, want within (0, 4096]", bytes)
	}

	shParams := cacheSharedDoc(be.URL, "", cacheRouteYAML("r"))
	sh := n.gateway(t, shParams, withClock(fake), cacheSharedOption("shared", shared))
	if st := cacheLast(t, get(sh, "/ok")); !st.Stored {
		t.Fatalf("shared fill %+v, want stored", st)
	}
	get(sh, "/race")
	shared.SetOpMode("put", teststore.Fail, fmt.Errorf("put down: %w", store.ErrUnavailable))
	get(sh, "/err")
	shared.SetOpMode("put", teststore.Inherit, nil)
	ms := metricsText(t, sh)
	for result, v := range map[string]float64{"stored": 1, "refused": 1, "error": 1} {
		if got := metricValue(t, ms, series("microgateway_cache_stores_total", "route", "r", "result", result)); got != v {
			t.Fatalf("shared stores_total result=%s = %v, want %v", result, got, v)
		}
	}
	if cacheHasSeries(ms, "microgateway_cache_bytes") {
		t.Fatal("shared-store gateway exposes the memory cache gauge")
	}

	noCache := cacheGatewayDoc(be.URL, "", "  - name: r\n    match:\n      path: /\n    upstream: up\n")
	if err := mem.ReloadYAML([]byte(noCache)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "one live generation after removing every cache route", func() bool { return inspect(mem).LiveGenerations == 1 }, func() string {
		return fmt.Sprintf("LiveGenerations=%d", inspect(mem).LiveGenerations)
	})
	after := metricsText(t, mem)
	if cacheHasSeries(after, "microgateway_cache_bytes") || cacheHasSeries(after, "microgateway_cache_entries") {
		t.Fatal("cache gauges still exposed after every cache route was removed")
	}
}

func TestCacheReloadGateway(t *testing.T) {
	t.Parallel()
	t.Run("entry survives an unrelated reload and a route cache change", func(t *testing.T) {
		n := newMemNetwork()
		fake := clock.NewFake(cacheStart)
		scr := &cacheScript{}
		scr.set(http.StatusOK, nil, "x")
		be := n.serve(t, scr)
		other := func(upstream string) string {
			return fmt.Sprintf("  - name: other\n    match:\n      path: /other\n    upstream: up\n    request_headers:\n      set:\n        X-Rev: %s\n", upstream)
		}
		doc := func(rev, ttl string) string {
			return cacheGatewayDoc(be.URL, "", cacheRouteYAML("r", ttl)+other(rev))
		}
		g := n.gateway(t, doc("1", "default_ttl: 30s"), withClock(fake))
		cacheDo(g, http.MethodGet, "http://example.com/keep", nil)
		if err := g.ReloadYAML([]byte(doc("2", "default_ttl: 30s"))); err != nil {
			t.Fatal(err)
		}
		if st := cacheLast(t, cacheDo(g, http.MethodGet, "http://example.com/keep", nil)); !st.Hit {
			t.Fatalf("entry after unrelated reload %+v, want hit", st)
		}
		if scr.count() != 1 {
			t.Fatalf("origin requests = %d, want 1", scr.count())
		}
		fake.Advance(10 * time.Second)
		if err := g.ReloadYAML([]byte(doc("2", "default_ttl: 5s"))); err != nil {
			t.Fatal(err)
		}
		if st := cacheLast(t, cacheDo(g, http.MethodGet, "http://example.com/keep", nil)); st.Hit || st.Fwd != httpcache.FwdStale {
			t.Fatalf("entry after default_ttl 30s→5s at age 10s %+v, want stale", st)
		}
	})

	t.Run("changed memory settings start empty and close the old cache after drain", func(t *testing.T) {
		n := newMemNetwork()
		fake := clock.NewFake(cacheStart)
		scr := &cacheScript{}
		scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		be := n.serve(t, scr)
		doc := func(maxBytes string) string {
			return cacheGatewayDoc(be.URL, "cache:\n  memory:\n    max_bytes: "+maxBytes+"\n    shards: 1\n", cacheRouteYAML("r"))
		}
		pre := snapGoroutines()
		g := n.gateway(t, doc("2097152"), withClock(fake))
		cacheDo(g, http.MethodGet, "http://example.com/mem", nil)
		base := snapGoroutines()
		if err := g.ReloadYAML([]byte(doc("4194304"))); err != nil {
			t.Fatal(err)
		}
		if st := cacheLast(t, cacheDo(g, http.MethodGet, "http://example.com/mem", nil)); st.Hit {
			t.Fatalf("entry after memory max_bytes change %+v, want miss", st)
		}
		awaitGoroutinesSettled(t, "janitor of the old cache to exit after drain", pre, base)
	})

	t.Run("fill in flight across a reload is stored in the old generation", func(t *testing.T) {
		n := newMemNetwork()
		fake := clock.NewFake(cacheStart)
		arrived := make(chan struct{}, 1)
		release := make(chan struct{})
		scr := &cacheScript{}
		scr.setHandle(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "max-age=60")
			select {
			case arrived <- struct{}{}:
			default:
			}
			<-release
			_, _ = io.WriteString(w, "x")
		})
		be := n.serve(t, scr)
		doc := func(maxBytes string) string {
			return cacheGatewayDoc(be.URL, "cache:\n  memory:\n    max_bytes: "+maxBytes+"\n    shards: 1\n", cacheRouteYAML("r"))
		}
		g := n.gateway(t, doc("2097152"), withClock(fake))
		done := make(chan *httptest.ResponseRecorder, 1)
		go func() { done <- cacheDo(g, http.MethodGet, "http://example.com/flight", nil) }()
		testwait.Done(t, "fill to reach the origin", arrived, nil)
		if err := g.ReloadYAML([]byte(doc("4194304"))); err != nil {
			t.Fatal(err)
		}
		close(release)
		rec, _ := testwait.Recv(t, "fill response", done, nil)
		if st := cacheLast(t, rec); !st.Stored {
			t.Fatalf("in-flight fill %+v, want stored", st)
		}
		if v := metricValue(t, metricsText(t, g), series("microgateway_cache_stores_total", "route", "r", "result", "stored")); v != 1 {
			t.Fatalf("stored = %v, want 1", v)
		}
		if st := cacheLast(t, cacheDo(g, http.MethodGet, "http://example.com/flight", nil)); st.Hit {
			t.Fatal("entry stored in the old generation is visible in the new cache")
		}
	})
}

func TestCacheMultiInstance(t *testing.T) {
	t.Parallel()
	n := newMemNetwork()
	scr := &cacheScript{}
	scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
	be := n.serve(t, scr)

	t.Run("shared store is shared by instances", func(t *testing.T) {
		shared := teststore.NewSharedCache(clock.NewFake(cacheStart), 1<<20)
		t.Cleanup(func() { _ = shared.Close() })
		doc := cacheSharedDoc(be.URL, "", cacheRouteYAML("r"))
		a := n.gateway(t, doc, withClock(clock.NewFake(cacheStart)), cacheSharedOption("shared", shared))
		b := n.gateway(t, doc, withClock(clock.NewFake(cacheStart)), cacheSharedOption("shared", shared))
		if st := cacheLast(t, cacheDo(a, http.MethodGet, "http://example.com/mi", nil)); !st.Stored {
			t.Fatalf("instance A fill %+v, want stored", st)
		}
		if st := cacheLast(t, cacheDo(b, http.MethodGet, "http://example.com/mi", nil)); !st.Hit {
			t.Fatalf("instance B %+v, want hit on the entry A stored", st)
		}
	})

	t.Run("built-in memory caches are per instance", func(t *testing.T) {
		doc := cacheGatewayDoc(be.URL, "", cacheRouteYAML("r"))
		a := n.gateway(t, doc, withClock(clock.NewFake(cacheStart)))
		b := n.gateway(t, doc, withClock(clock.NewFake(cacheStart)))
		before := scr.count()
		if st := cacheLast(t, cacheDo(a, http.MethodGet, "http://example.com/mm", nil)); !st.Stored {
			t.Fatalf("instance A fill %+v, want stored", st)
		}
		if st := cacheLast(t, cacheDo(b, http.MethodGet, "http://example.com/mm", nil)); st.Hit {
			t.Fatalf("instance B %+v, want miss", st)
		}
		if d := scr.count() - before; d != 2 {
			t.Fatalf("origin requests = %d, want 2", d)
		}
	})
}

func TestCacheNoGoroutineLeak(t *testing.T) {
	t.Parallel()
	pre := snapGoroutines()
	n := newMemNetwork()
	scr := &cacheScript{}
	scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
	be := n.serve(t, scr)
	fake := clock.NewFake(cacheStart)
	shared := teststore.NewSharedCache(fake, 1<<20)
	t.Cleanup(func() { _ = shared.Close() })
	g := n.gateway(t, cacheSharedDoc(be.URL, "  store_timeout: 100ms\n", cacheRouteYAML("r")), withClock(fake), cacheSharedOption("shared", shared))
	t.Cleanup(func() { _ = g.Close() })
	cacheDo(g, http.MethodGet, "http://example.com/leak", nil)
	base := snapGoroutines()

	for range 20 {
		cacheDo(g, http.MethodGet, "http://example.com/leak", nil)
		cacheDo(g, http.MethodHead, "http://example.com/leak", nil)
		cacheDo(g, http.MethodPost, "http://example.com/leak-post", nil)
		cacheDo(g, http.MethodGet, "http://example.com/leak", http.Header{"Range": {"bytes=0-1"}})
	}

	shared.SetMode(teststore.Hang, nil)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- cacheDo(g, http.MethodGet, "http://example.com/leak-timeout", nil) }()
	awaitDue(t, fake, 1, 100*time.Millisecond, "store timeout timer", nil)
	fake.Advance(100 * time.Millisecond)
	testwait.Recv(t, "request after store timeout", done, nil)
	shared.SetMode(teststore.Inherit, nil)

	awaitGoroutinesSettled(t, "goroutines started by cache requests to exit", pre, base)
}
