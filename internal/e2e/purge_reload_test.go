package e2e

import (
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/teststore"
)

// ttlRoute returns one caching route on path / to upstream up, with the route-level timeout when set and the cache
// block lines cacheLines.
func ttlRoute(timeout string, cacheLines ...string) string {
	s := "  - name: r\n    match:\n      path: /\n    upstream: up\n"
	if timeout != "" {
		s += "    timeout: " + timeout + "\n"
	}
	s += "    cache:\n      enabled: true\n"
	for _, l := range cacheLines {
		s += "      " + l + "\n"
	}
	return s
}

func TestPurgeAcrossReload(t *testing.T) {
	const purged = "http://h/purged"
	const kept = "http://h/kept"
	admin := purgeAdminYAML(purgeTokA)
	pre := snapGoroutines()
	g, _, origin, be := purgeFixtureOn(t, admin, ttlRoute(""))
	origin.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "body")
	for _, u := range []string{purged, kept} {
		if st := purgeLast(t, purgeGet(t, g, u, nil)); !st.Stored {
			t.Fatalf("fill %s %+v, want stored", u, st)
		}
	}
	purgeOK(t, purgeDo(t, g, purgeTokA, `{"urls":["`+purged+`"]}`))
	base := snapGoroutines()

	onlyCached := http.Header{"Cache-Control": {"only-if-cached"}}
	wantPurgedGone := func(t *testing.T, step string) {
		t.Helper()
		calls, _, _ := origin.counts()
		resp := purgeReq(t, g, http.MethodGet, purged, onlyCached)
		if resp.StatusCode != http.StatusGatewayTimeout {
			t.Fatalf("%s: purged URL status %d, want 504", step, resp.StatusCode)
		}
		if e := decodeErr(t, resp); e.Code != httperr.CodeNotCached {
			t.Fatalf("%s: purged URL error code %q, want %q", step, e.Code, httperr.CodeNotCached)
		}
		if after, _, _ := origin.counts(); after != calls {
			t.Fatalf("%s: origin calls %d, want %d", step, after, calls)
		}
	}
	wantKeptHit := func(t *testing.T, step string) {
		t.Helper()
		if st := purgeLast(t, purgeGet(t, g, kept, nil)); !st.Hit {
			t.Fatalf("%s: unrelated URL %+v, want hit", step, st)
		}
	}

	if err := g.ReloadYAML([]byte(cacheGatewayDoc(be, admin, ttlRoute("5s")))); err != nil {
		t.Fatalf("reload with a route timeout change: %v", err)
	}
	wantPurgedGone(t, "after timeout change")
	wantKeptHit(t, "after timeout change")

	if err := g.ReloadYAML([]byte(cacheGatewayDoc(be, admin, ttlRoute("5s", "default_ttl: 30s")))); err != nil {
		t.Fatalf("reload with a cache block change: %v", err)
	}
	wantPurgedGone(t, "after cache block change")
	wantKeptHit(t, "after cache block change")

	if err := g.ReloadYAML([]byte(cacheGatewayDoc(be, admin+"cache:\n  memory:\n    max_bytes: 33554432\n", ttlRoute("5s", "default_ttl: 30s")))); err != nil {
		t.Fatalf("reload with a memory max_bytes change: %v", err)
	}
	mc := inspect(g).MemCache
	if mc == nil {
		t.Fatal("built-in memory cache state is not exposed")
	}
	if mc.Entries != 0 {
		t.Fatalf("entries after memory max_bytes change = %d, want 0", mc.Entries)
	}
	wantPurgedGone(t, "after memory change")
	eventually(t, "one live generation after the memory change", func() bool { return inspect(g).LiveGenerations == 1 }, func() string {
		return fmt.Sprintf("LiveGenerations=%d", inspect(g).LiveGenerations)
	})
	awaitGoroutinesSettled(t, "janitor of the old memory cache to exit after drain", pre, base)
}

func TestPurgeMarkerCapDefault(t *testing.T) {
	t.Parallel()
	if testing.Short() || os.Getenv("MICROGATEWAY_LOAD") == "" {
		t.Skip("set MICROGATEWAY_LOAD=1")
	}
	g, _, origin := purgeFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	origin.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "body")
	if st := purgeLast(t, purgeGet(t, g, "http://h/unrelated", nil)); !st.Stored {
		t.Fatalf("fill unrelated %+v, want stored", st)
	}
	const calls = 656
	for i := range calls {
		purgeOK(t, purgeDo(t, g, purgeTokA, `{"urls":`+jsonURLs(numbered(fmt.Sprintf("http://h/c%d-u", i), 1000)...)+`}`))
	}
	if m := inspect(g).MemCache; m == nil || m.Markers > 65536 {
		t.Fatalf("markers after %d distinct purges = %v, want at most 65536", calls*1000, m)
	}
	if st := purgeLast(t, purgeGet(t, g, "http://h/unrelated", nil)); !st.Hit {
		t.Fatalf("unrelated entry %+v, want hit", st)
	}
}

func TestPurgeMultiInstance(t *testing.T) {
	t.Parallel()
	const target = "http://h/m"
	t.Run("built-in memory caches are per instance", func(t *testing.T) {
		n := newMemNetwork()
		origin := newFlightOrigin()
		origin.set(http.StatusOK, ttlOriginHeader("max-age=60"), "body")
		be := n.serve(t, origin)
		doc := cacheGatewayDoc(be.URL, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
		a := n.gateway(t, doc, withClock(clock.NewFake(cacheStart)))
		b := n.gateway(t, doc, withClock(clock.NewFake(cacheStart)))
		for _, g := range []*microgateway.Gateway{a, b} {
			if st := purgeLast(t, purgeGet(t, g, target, nil)); !st.Stored {
				t.Fatalf("fill %+v, want stored", st)
			}
		}
		if got := purgeOK(t, purgeDo(t, a, purgeTokA, `{"urls":["`+target+`"]}`)); got.Scope != "instance" {
			t.Fatalf("purge scope %q, want instance", got.Scope)
		}
		_, _, u0 := origin.counts()
		if st := purgeLast(t, purgeGet(t, a, target, nil)); st.Hit || st.Fwd != httpcache.FwdURIMiss {
			t.Fatalf("A after purge %+v, want uri-miss", st)
		}
		if _, _, u1 := origin.counts(); u1-u0 != 1 {
			t.Fatalf("A after purge reached origin %d times unconditionally, want 1", u1-u0)
		}
		if st := purgeLast(t, purgeGet(t, b, target, nil)); !st.Hit {
			t.Fatalf("B after purge to A %+v, want hit", st)
		}
	})

	for _, soft := range []bool{false, true} {
		t.Run(ttlMode(soft)+" shared store is store wide", func(t *testing.T) {
			n := newMemNetwork()
			origin := newFlightOrigin()
			origin.set(http.StatusOK, ttlOriginHeader("max-age=60"), "body")
			be := n.serve(t, origin)
			storeClk := clock.NewFake(cacheStart)
			shared := teststore.NewSharedCache(storeClk, 1<<20)
			t.Cleanup(func() { _ = shared.Close() })
			doc := cacheSharedDoc(be.URL, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
			opt := cacheSharedOption("shared", shared)
			a := n.gateway(t, doc, withClock(clock.NewFake(cacheStart)), opt)
			b := n.gateway(t, doc, withClock(clock.NewFake(cacheStart)), opt)
			if st := purgeLast(t, purgeGet(t, a, target, nil)); !st.Stored {
				t.Fatalf("fill via A %+v, want stored", st)
			}
			if got := purgeOK(t, purgeDo(t, a, purgeTokA, fmt.Sprintf(`{"urls":["%s"],"soft":%t}`, target, soft))); got.Scope != "store" {
				t.Fatalf("purge scope %q, want store", got.Scope)
			}
			_, c0, u0 := origin.counts()
			st := purgeLast(t, purgeGet(t, b, target, nil))
			_, c1, u1 := origin.counts()
			if st.Hit {
				t.Fatalf("B after purge to A %+v, want no hit", st)
			}
			if soft {
				if st.Fwd != httpcache.FwdStale || c1-c0 != 1 || u1-u0 != 0 {
					t.Fatalf("B after soft purge %+v, conditional=%d unconditional=%d, want stale conditional", st, c1-c0, u1-u0)
				}
			} else if st.Fwd != httpcache.FwdURIMiss || c1-c0 != 0 || u1-u0 != 1 {
				t.Fatalf("B after hard purge %+v, conditional=%d unconditional=%d, want uri-miss unconditional", st, c1-c0, u1-u0)
			}
		})
	}
}
