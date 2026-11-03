package runtime

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/middleware"
	"github.com/ok0/go-microgateway/internal/state"
	"github.com/ok0/go-microgateway/store"
)

const cacheHead = `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
`

const cacheRouteBlock = `routes:
  - name: api
    match:
      path: /api
    upstream: users
    cache:
      enabled: true
`

const plainRouteBlock = `  - name: plain
    match:
      path: /plain
    upstream: users
`

func cacheMetricFamilies(t *testing.T, reg *metrics.Registry) []string {
	t.Helper()
	var b strings.Builder
	if err := reg.WriteText(&b); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, line := range strings.Split(b.String(), "\n") {
		if name, ok := strings.CutPrefix(line, "# TYPE "); ok {
			names = append(names, strings.Fields(name)[0])
		}
	}
	slices.Sort(names)
	return names
}

func hasCacheFamilies(names []string) bool {
	for _, n := range names {
		if strings.HasPrefix(n, "microgateway_cache_") {
			return true
		}
	}
	return false
}

func TestCacheSeriesOnlyWhenEnabled(t *testing.T) {
	plain := mustParse(t, cacheHead+"routes:\n"+plainRouteBlock)
	cached := mustParse(t, cacheHead+cacheRouteBlock)

	base := metrics.NewRegistry()
	if _, err := RegisterSeries(base); err != nil {
		t.Fatal(err)
	}
	reg := metrics.NewRegistry()
	if _, _, err := build(t, plain, Deps{Metrics: reg}); err != nil {
		t.Fatal(err)
	}
	got := cacheMetricFamilies(t, reg)
	if hasCacheFamilies(got) {
		t.Fatalf("families without cache routes = %v, want no microgateway_cache_ family", got)
	}
	if want := cacheMetricFamilies(t, base); !slices.Equal(got, want) {
		t.Errorf("families = %v, want the families of a plain RegisterSeries registry %v", got, want)
	}

	if _, _, err := build(t, cached, Deps{Metrics: reg}); err != nil {
		t.Fatal(err)
	}
	if got := cacheMetricFamilies(t, reg); !hasCacheFamilies(got) {
		t.Errorf("families with a cache route = %v, want the cache families", got)
	}

	if _, _, err := build(t, plain, Deps{Metrics: reg}); err != nil {
		t.Fatal(err)
	}
	if got := cacheMetricFamilies(t, reg); !hasCacheFamilies(got) {
		t.Errorf("families after a later build without cache = %v, want the cache families kept", got)
	}
}

func TestCacheMemoryStateEntry(t *testing.T) {
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	mk := func(maxBytes string) (*Snapshot, *state.Lease) {
		t.Helper()
		doc := cacheHead + "cache:\n  memory:\n    max_bytes: " + maxBytes + "\n" + cacheRouteBlock
		cfg := mustParse(t, doc)
		tx := reg.Begin()
		s, err := Build(cfg, Deps{Metrics: metrics.NewRegistry(), Clock: clock.NewFake(buildStart)}, tx)
		if err != nil {
			tx.Abort()
			t.Fatal(err)
		}
		return s, tx.Commit()
	}

	s1, l1 := mk("67108864")
	s2, l2 := mk("67108864")
	if s1.memCache == nil || s1.memCache != s2.memCache {
		t.Fatalf("memory cache across builds = %p and %p, want the same instance", s1.memCache, s2.memCache)
	}

	s3, l3 := mk("134217728")
	if s3.memCache == nil || s3.memCache == s1.memCache {
		t.Fatal("changed max_bytes reused the old memory cache, want a new instance")
	}
	ctx := context.Background()
	noSel := func([]string) (string, bool) { return "", false }
	if _, err := s1.memCache.Get(ctx, "k", noSel); err != nil {
		t.Fatalf("old cache closed while its lease is held: %v", err)
	}
	if err := l1.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := s1.memCache.Get(ctx, "k", noSel); err != nil {
		t.Fatalf("old cache closed while another lease holds it: %v", err)
	}
	if err := l2.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := s1.memCache.Get(ctx, "k", noSel); !errors.Is(err, store.ErrUnavailable) {
		t.Errorf("old cache Get after its leases were released = %v, want ErrUnavailable", err)
	}
	if _, err := s3.memCache.Get(ctx, "k", noSel); err != nil {
		t.Errorf("new cache Get = %v, want nil", err)
	}
	if err := l3.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestCacheStageOrder(t *testing.T) {
	order := middleware.Order(nil, nil)
	at := func(name string) int { return slices.Index(order, name) }
	if !(at(middleware.StageRateLimit) < at(middleware.StageCache) && at(middleware.StageCache) < at(middleware.StageCircuitBreaker)) {
		t.Errorf("default order = %v, want cache after rate_limit and before circuit_breaker", order)
	}

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(origin.Close)
	doc := `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "` + origin.URL + `"
routes:
  - name: api
    match:
      path: /api
    upstream: users
    cache:
      enabled: true
  - name: piped
    match:
      path: /piped
    upstream: users
    pipeline: [timeout]
    cache:
      enabled: true
  - name: plain
    match:
      path: /plain
    upstream: users
`
	s, _, err := build(t, mustParse(t, doc), Deps{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"api", "piped"}; !slices.Equal(s.cacheRoutes, want) {
		t.Fatalf("cache routes = %v, want %v", s.cacheRoutes, want)
	}
	for _, p := range []string{"/api", "/piped", "/plain"} {
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200", p, rec.Code)
		}
	}
	if n := s.cacheSeries.Requests.With("api", "miss").Value(); n != 1 {
		t.Errorf("api miss requests = %v, want 1 (default order runs the stage)", n)
	}
	if n := s.cacheSeries.Requests.With("piped", "miss").Value(); n != 0 {
		t.Errorf("piped miss requests = %v, want 0 (explicit pipeline without cache)", n)
	}
	if n := s.cacheSeries.Requests.With("plain", "miss").Value(); n != 0 {
		t.Errorf("plain miss requests = %v, want 0 (no cache.enabled)", n)
	}
}
