package e2e

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/teststore"
	"github.com/ok0/go-microgateway/store"
)

const cacheTTLLifetime = 60 * time.Second

// cacheTTLOrigin returns the origin headers of the TTL tests: a validator and a length, plus extra.
func cacheTTLOrigin(extra http.Header) http.Header {
	h := http.Header{"Etag": {`"v1"`}, "Content-Length": {"1"}}
	for k, v := range extra {
		h[k] = v
	}
	return h
}

// cacheFixture returns a gateway on the fake clock whose single route has the cache block lines in route (one per
// line) and whose origin answers 200 with origin headers and body "x".
func cacheFixture(t *testing.T, route string, origin http.Header, opts ...microgateway.Option) (*microgateway.Gateway, *clock.Fake, *cacheScript) {
	t.Helper()
	n := newMemNetwork()
	scr := &cacheScript{}
	scr.set(http.StatusOK, origin, "x")
	be := n.serve(t, scr)
	fake := clock.NewFake(cacheStart)
	var lines []string
	if route != "" {
		lines = strings.Split(route, "\n")
	}
	all := append([]microgateway.Option{withClock(fake)}, opts...)
	g := n.gateway(t, cacheGatewayDoc(be.URL, "", cacheRouteYAML("r", lines...)), all...)
	return g, fake, scr
}

func TestCacheTTLGateway(t *testing.T) {
	t.Parallel()
	sources := []struct {
		name     string
		route    string
		origin   http.Header
		maxStale bool
	}{
		{name: "max-age", route: "stale_if_error: 1h",
			origin: cacheTTLOrigin(http.Header{"Cache-Control": {"max-age=60"}}), maxStale: true},
		{name: "s-maxage over larger max-age", route: "stale_if_error: 1h",
			origin: cacheTTLOrigin(http.Header{"Cache-Control": {"max-age=3600, s-maxage=60"}})},
		{name: "expires", route: "stale_if_error: 1h",
			origin: cacheTTLOrigin(http.Header{"Date": {cacheStart.Format(http.TimeFormat)}, "Expires": {cacheStart.Add(cacheTTLLifetime).Format(http.TimeFormat)}}), maxStale: true},
		{name: "default_ttl", route: "default_ttl: 60s\nstale_if_error: 1h",
			origin: cacheTTLOrigin(nil), maxStale: true},
		{name: "max-age clamped to max_ttl", route: "max_ttl: 60s\nstale_if_error: 1h",
			origin: cacheTTLOrigin(http.Header{"Cache-Control": {"max-age=3600"}}), maxStale: true},
	}
	const target = "http://example.com/ttl"
	for _, src := range sources {
		t.Run(src.name, func(t *testing.T) {
			g, fake, scr := cacheFixture(t, src.route, src.origin)
			cacheDo(g, http.MethodGet, target, nil)
			fake.Advance(cacheTTLLifetime - time.Nanosecond)
			rec := cacheDo(g, http.MethodGet, target, nil)
			st := cacheLast(t, rec)
			if !st.Hit || !st.HasTTL || st.TTL != 1 {
				t.Fatalf("at L-1ns: %+v, want hit with ttl=1", st)
			}
			if got := rec.Header().Get("Age"); got != strconv.Itoa(int(cacheTTLLifetime/time.Second)-1) {
				t.Fatalf("at L-1ns: Age %q, want %d", got, int(cacheTTLLifetime/time.Second)-1)
			}
			fake.Advance(time.Nanosecond)
			rec = cacheDo(g, http.MethodGet, target, nil)
			if st := cacheLast(t, rec); st.Hit || st.Fwd != httpcache.FwdStale {
				t.Fatalf("at L: %+v, want stale with a conditional forward", st)
			}
			if got := scr.lastHeader().Get("If-None-Match"); got != `"v1"` {
				t.Fatalf("at L: origin If-None-Match %q, want \"v1\"", got)
			}
			if !src.maxStale {
				return
			}
			ms, msFake, msScr := cacheFixture(t, src.route, src.origin)
			cacheDo(ms, http.MethodGet, target, nil)
			msFake.Advance(cacheTTLLifetime)
			maxStale := http.Header{"Cache-Control": {"max-stale=3600"}}
			rec = cacheDo(ms, http.MethodGet, target, maxStale)
			if st := cacheLast(t, rec); !st.Hit || !st.HasTTL || st.TTL != 0 {
				t.Fatalf("max-stale at L: %+v, want hit with ttl=0", st)
			}
			msFake.Advance(time.Nanosecond)
			rec = cacheDo(ms, http.MethodGet, target, maxStale)
			if st := cacheLast(t, rec); !st.Hit || !st.HasTTL || st.TTL != -1 {
				t.Fatalf("max-stale at L+1ns: %+v, want hit with ttl=-1", st)
			}
			if msScr.count() != 1 {
				t.Fatalf("origin calls %d, want 1: serving stale must not refill", msScr.count())
			}
		})
	}

	t.Run("storage boundary", func(t *testing.T) {
		cases := []struct {
			name       string
			route      string
			keep       time.Duration
			background bool
		}{
			{name: "keep", route: "keep: 30s", keep: 30 * time.Second},
			{name: "stale_while_revalidate", route: "stale_while_revalidate: 40s", keep: 40 * time.Second, background: true},
			{name: "stale_if_error", route: "stale_if_error: 50s", keep: 50 * time.Second},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				s := cacheTTLLifetime + c.keep
				origin := cacheTTLOrigin(http.Header{"Cache-Control": {"max-age=60"}})
				probe := func(offset time.Duration) (*microgateway.Gateway, *cacheScript) {
					g, fake, scr := cacheFixture(t, c.route, origin)
					cacheDo(g, http.MethodGet, "http://example.com/s", nil)
					fake.Advance(offset)
					return g, scr
				}
				g, scr := probe(s - time.Nanosecond)
				rec := cacheDo(g, http.MethodGet, "http://example.com/s", nil)
				st := cacheLast(t, rec)
				if c.background {
					if !st.Hit || st.Fwd != httpcache.FwdStale || st.Detail != "stale-while-revalidate" || !st.HasTTL || st.TTL != -40 {
						t.Fatalf("at S-1ns: %+v, want stale-while-revalidate hit with ttl=-40", st)
					}
					if age := rec.Header().Get("Age"); age != "99" {
						t.Fatalf("at S-1ns: Age %q, want 99", age)
					}
					eventually(t, "background refresh at S-1ns", func() bool {
						return strings.Contains(metricsText(t, g), series("microgateway_cache_background_revalidations_total", "route", "r", "result", "replaced")+" 1\n")
					}, nil)
				} else if st.Fwd != httpcache.FwdStale {
					t.Fatalf("at S-1ns: %+v, want conditional forward", st)
				}
				if got := scr.lastHeader().Get("If-None-Match"); got != `"v1"` {
					t.Fatalf("at S-1ns: origin If-None-Match %q, want \"v1\"", got)
				}
				if scr.count() != 2 {
					t.Fatalf("at S-1ns: origin calls %d, want 2", scr.count())
				}
				for _, at := range []time.Duration{s, s + time.Nanosecond} {
					g, scr := probe(at)
					rec := cacheDo(g, http.MethodGet, "http://example.com/s", nil)
					st := cacheLast(t, rec)
					if st.Fwd != httpcache.FwdURIMiss {
						t.Fatalf("at %v: fwd %q, want uri-miss", at, st.Fwd)
					}
					if got := scr.lastHeader().Get("If-None-Match"); got != "" {
						t.Fatalf("at %v: origin request still conditional (%q)", at, got)
					}
				}
			})
		}
		t.Run("keep adds nothing without a validator", func(t *testing.T) {
			origin := http.Header{"Cache-Control": {"max-age=60"}}
			probe := func(offset time.Duration) *httptest.ResponseRecorder {
				g, fake, _ := cacheFixture(t, "keep: 30s", origin)
				cacheDo(g, http.MethodGet, "http://example.com/nv", nil)
				fake.Advance(offset)
				return cacheDo(g, http.MethodGet, "http://example.com/nv", nil)
			}
			if !cacheLast(t, probe(cacheTTLLifetime-time.Nanosecond)).Hit {
				t.Fatal("at L-1ns: want hit")
			}
			if st := cacheLast(t, probe(cacheTTLLifetime)); st.Fwd != httpcache.FwdURIMiss {
				t.Fatalf("at L: fwd %q, want uri-miss", st.Fwd)
			}
		})
	})
	t.Run("ttl inputs", func(t *testing.T) {
		date := cacheStart.Format(http.TimeFormat)
		cases := []struct {
			name      string
			route     string
			origin    http.Header
			stored    bool
			ttl       *int64
			secondHit bool
			secondFwd string
		}{
			{name: "max-age=0 is not stored", origin: cacheTTLOrigin(http.Header{"Cache-Control": {"max-age=0"}}), stored: false},
			{name: "max-age=-1 is zero lifetime and default_ttl is not used", route: "default_ttl: 30s",
				origin: cacheTTLOrigin(http.Header{"Cache-Control": {"max-age=-1"}}), stored: false},
			{name: "40-digit max-age saturates and clamps to max_ttl",
				origin: cacheTTLOrigin(http.Header{"Cache-Control": {"max-age=1234567890123456789012345678901234567890"}}),
				stored: true, ttl: ptrInt64(86400), secondHit: true},
			{name: "Expires 0 gives zero lifetime", origin: cacheTTLOrigin(http.Header{"Expires": {"0"}}), stored: false},
			{name: "Expires before Date gives zero lifetime", origin: cacheTTLOrigin(http.Header{
				"Date":    {cacheStart.Add(10 * time.Second).Format(http.TimeFormat)},
				"Expires": {cacheStart.Add(5 * time.Second).Format(http.TimeFormat)},
			}), stored: false},
			{name: "invalid Date is replaced by the response time", origin: cacheTTLOrigin(http.Header{
				"Date":    {"not a date"},
				"Expires": {cacheStart.Add(cacheTTLLifetime).Format(http.TimeFormat)},
			}), stored: true, ttl: ptrInt64(60), secondHit: true},
			{name: "negative Age is ignored", origin: cacheTTLOrigin(http.Header{"Cache-Control": {"max-age=60"}, "Age": {"-1"}, "Date": {date}}),
				stored: true, ttl: ptrInt64(60), secondHit: true},
			{name: "Age above 2^31 seconds is stored and stale on first read", origin: cacheTTLOrigin(http.Header{
				"Cache-Control": {"max-age=60"}, "Age": {"4294967296"}, "Date": {date},
			}), stored: true, secondFwd: httpcache.FwdStale},
			{name: "no freshness and default_ttl 0 is not stored", origin: cacheTTLOrigin(nil), stored: false},
			{name: "no freshness and default_ttl 30s is stored for 30s", route: "default_ttl: 30s",
				origin: cacheTTLOrigin(nil), stored: true, ttl: ptrInt64(30), secondHit: true},
			{name: "keep with validator and max-age=0 revalidates every request", route: "keep: 1h",
				origin: cacheTTLOrigin(http.Header{"Cache-Control": {"max-age=0"}}), stored: true, secondFwd: httpcache.FwdStale},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				g, _, _ := cacheFixture(t, c.route, c.origin)
				const target = "http://example.com/in"
				st := cacheLast(t, cacheDo(g, http.MethodGet, target, nil))
				if st.Stored != c.stored {
					t.Fatalf("stored=%v, want %v (%+v)", st.Stored, c.stored, st)
				}
				if c.ttl != nil && (!st.HasTTL || st.TTL != *c.ttl) {
					t.Fatalf("ttl=%d (has=%v), want %d", st.TTL, st.HasTTL, *c.ttl)
				}
				if !c.stored && c.secondFwd == "" {
					return
				}
				second := cacheLast(t, cacheDo(g, http.MethodGet, target, nil))
				if second.Hit != c.secondHit {
					t.Fatalf("second hit=%v, want %v", second.Hit, c.secondHit)
				}
				if c.secondFwd != "" && second.Fwd != c.secondFwd {
					t.Fatalf("second fwd %q, want %q", second.Fwd, c.secondFwd)
				}
			})
		}
	})
}

func ptrInt64(v int64) *int64 { return &v }

func TestCacheApparentAgeGateway(t *testing.T) {
	t.Parallel()
	origin := http.Header{
		"Cache-Control":  {"max-age=60"},
		"Age":            {"30"},
		"Date":           {cacheStart.Add(-5 * time.Second).Format(http.TimeFormat)},
		"Content-Length": {"1"},
	}
	g, fake, _ := cacheFixture(t, "", origin)
	const target = "http://example.com/age"
	fill := cacheLast(t, cacheDo(g, http.MethodGet, target, nil))
	if !fill.Stored || !fill.HasTTL || fill.TTL != 30 {
		t.Fatalf("fill %+v, want stored with ttl=30", fill)
	}
	rec := cacheDo(g, http.MethodGet, target, nil)
	if got := rec.Header().Get("Age"); got != "30" {
		t.Fatalf("Age %q, want 30", got)
	}
	fake.Advance(10 * time.Second)
	rec = cacheDo(g, http.MethodGet, target, nil)
	st := cacheLast(t, rec)
	if rec.Header().Get("Age") != "40" || !st.HasTTL || st.TTL != 20 {
		t.Fatalf("after 10s Age %q status %+v, want Age 40 ttl 20", rec.Header().Get("Age"), st)
	}
}

func TestCacheSkewSharedStore(t *testing.T) {
	t.Parallel()
	type decision struct {
		hit  bool
		fwd  string
		age  string
		ttl  int64
		hasT bool
	}
	decide := func(t *testing.T, fillerIsA bool, offset time.Duration) decision {
		storeClk := clock.NewFake(cacheStart)
		shared := teststore.NewSharedCache(storeClk, 1<<20)
		t.Cleanup(func() { _ = shared.Close() })
		n := newMemNetwork()
		scr := &cacheScript{}
		scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}, "Content-Length": {"1"}}, "x")
		be := n.serve(t, scr)
		doc := cacheGatewayDoc(be.URL, "stores:\n  - name: sc\n    type: shared\ncache:\n  store: sc\n", cacheRouteYAML("r", "stale_if_error: 1h"))
		opt := microgateway.WithStoreType("shared", store.CapCache, func(microgateway.StoreParams) (store.Backend, error) {
			return shared.Handle(), nil
		})
		a := n.gateway(t, doc, withClock(clock.NewFake(cacheStart.Add(time.Hour))), opt)
		b := n.gateway(t, doc, withClock(clock.NewFake(cacheStart.Add(-time.Hour))), opt)
		filler, other := a, b
		if !fillerIsA {
			filler, other = b, a
		}
		const target = "http://example.com/skew"
		cacheDo(filler, http.MethodGet, target, nil)
		storeClk.Advance(offset)
		rec := cacheDo(other, http.MethodGet, target, nil)
		st := cacheLast(t, rec)
		return decision{hit: st.Hit, fwd: st.Fwd, age: rec.Header().Get("Age"), ttl: st.TTL, hasT: st.HasTTL}
	}
	for _, fillerIsA := range []bool{true, false} {
		fresh := decide(t, fillerIsA, cacheTTLLifetime-time.Nanosecond)
		if !fresh.hit || fresh.age != "59" || !fresh.hasT || fresh.ttl != 1 {
			t.Fatalf("filler A=%v at L-1ns: %+v", fillerIsA, fresh)
		}
		for _, offset := range []time.Duration{cacheTTLLifetime, cacheTTLLifetime + time.Nanosecond} {
			d := decide(t, fillerIsA, offset)
			if d.hit || d.fwd != httpcache.FwdStale {
				t.Fatalf("filler A=%v at %v: %+v, want stale", fillerIsA, offset, d)
			}
		}
	}
}
