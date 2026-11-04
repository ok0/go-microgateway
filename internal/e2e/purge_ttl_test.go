package e2e

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/teststore"
	"github.com/ok0/go-microgateway/store"
)

const ttlRetention = 110 * time.Minute

var ttlRoutes = cacheRouteYAML("r", "max_ttl: 1h", "stale_while_revalidate: 10m", "stale_if_error: 20m", "keep: 30m")

func ttlOriginHeader(cc string) http.Header {
	return http.Header{"Cache-Control": {cc}, "Etag": {`"v1"`}, "Surrogate-Key": {"tg"}}
}

func ttlMode(soft bool) string {
	if soft {
		return "soft"
	}
	return "hard"
}

// ttlSharedFixture returns a gateway whose cache is the shared store "sc" on the fake clock returned, with its origin
// and the backend URL.
func ttlSharedFixture(t *testing.T, admin, routes string) (*microgateway.Gateway, *clock.Fake, *flightOrigin, *teststore.SharedCache, string) {
	t.Helper()
	n := newMemNetwork()
	origin := newFlightOrigin()
	be := n.serve(t, origin)
	fake := clock.NewFake(cacheStart)
	shared := teststore.NewSharedCache(fake, 1<<20)
	t.Cleanup(func() { _ = shared.Close() })
	g := n.gateway(t, cacheSharedDoc(be.URL, admin, routes), withClock(fake), cacheSharedOption("shared", shared))
	return g, fake, origin, shared, be.URL
}

func ttlPurge(t *testing.T, g *microgateway.Gateway, body string) purgeReply {
	t.Helper()
	return purgeOK(t, purgeDo(t, g, purgeTokA, body))
}

// ttlPutTTLs returns the TTL of every recorded put, in call order.
func ttlPutTTLs(shared *teststore.SharedCache) []time.Duration {
	var out []time.Duration
	for _, c := range shared.Calls() {
		if c.Op == "put" {
			out = append(out, c.TTL)
		}
	}
	return out
}

// ttlWantRetain asserts that the store received exactly n purges and that each carries marker lifetime want.
func ttlWantRetain(t *testing.T, shared *teststore.SharedCache, want time.Duration, n int) {
	t.Helper()
	calls := purgeCallsOf(shared)
	if len(calls) != n {
		t.Fatalf("store purge calls = %d, want %d", len(calls), n)
	}
	for i, c := range calls {
		if c.Purge.Retain != want {
			t.Fatalf("purge %d Retain = %v, want %v", i, c.Purge.Retain, want)
		}
	}
}

// ttlPutDirect stores an entry for rawURL with storage lifetime ttl straight into the shared store, with a fill token
// issued by a miss on the same key.
func ttlPutDirect(t *testing.T, h *teststore.CacheHandle, rawURL string, ttl time.Duration) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, rawURL, nil)
	req.Host = req.URL.Host
	primary := httpcache.PrimaryKey(req)
	ctx := context.Background()
	lk, err := h.Get(ctx, primary, func([]string) (string, bool) { return "", false })
	if err != nil {
		t.Fatalf("get %s: %v", rawURL, err)
	}
	secondary, ok := httpcache.SecondaryKey(nil, req)
	if !ok {
		t.Fatalf("secondary key of %s", rawURL)
	}
	e := &store.Entry{Status: http.StatusOK, Header: ttlOriginHeader("max-age=3600"), Body: []byte("stored")}
	stored, err := h.Put(ctx, lk.Token, primary, secondary, nil, e, ttl)
	if err != nil || !stored {
		t.Fatalf("put %s with ttl %v: stored=%v err=%v", rawURL, ttl, stored, err)
	}
}

func TestPurgeMarkerLifetime(t *testing.T) {
	t.Parallel()
	const target = "http://h/p"
	admin := purgeAdminYAML(purgeTokA)
	header := ttlOriginHeader("max-age=3600, stale-if-error=1200")
	instants := []struct {
		name string
		at   time.Duration
	}{{"R-1ns", ttlRetention - time.Nanosecond}, {"R", ttlRetention}, {"R+1ns", ttlRetention + time.Nanosecond}}
	sels := []struct {
		name  string
		field string
	}{{"url", `"urls":["http://h/p"]`}, {"tag", `"tags":["tg"]`}, {"all", `"all":true`}}

	for _, sel := range sels {
		for _, soft := range []bool{false, true} {
			for _, in := range instants {
				t.Run(fmt.Sprintf("%s/%s/%s", sel.name, ttlMode(soft), in.name), func(t *testing.T) {
					g, fake, origin, shared, _ := ttlSharedFixture(t, admin, ttlRoutes)
					origin.set(http.StatusOK, header, "body")
					if st := purgeLast(t, purgeGet(t, g, target, nil)); !st.Stored {
						t.Fatalf("fill %+v, want stored", st)
					}
					if got := ttlPutTTLs(shared); !slices.Equal(got, []time.Duration{ttlRetention}) {
						t.Fatalf("put TTLs %v, want [%v]", got, ttlRetention)
					}
					ttlPurge(t, g, fmt.Sprintf(`{%s,"soft":%t}`, sel.field, soft))
					fake.Advance(in.at)

					_, c0, u0 := origin.counts()
					st := purgeLast(t, purgeGet(t, g, target, nil))
					_, c1, u1 := origin.counts()
					if st.Hit {
						t.Fatalf("at %s: %+v, want no hit", in.name, st)
					}
					if soft && in.at < ttlRetention {
						if st.Fwd != httpcache.FwdStale || c1-c0 != 1 || u1-u0 != 0 || origin.lastHeader().Get("If-None-Match") != `"v1"` {
							t.Fatalf("at %s: %+v, conditional=%d unconditional=%d, want stale conditional revalidation", in.name, st, c1-c0, u1-u0)
						}
					} else if st.Fwd != httpcache.FwdURIMiss || c1-c0 != 0 || u1-u0 != 1 {
						t.Fatalf("at %s: %+v, conditional=%d unconditional=%d, want uri-miss unconditional", in.name, st, c1-c0, u1-u0)
					}

					if rec := purgeReq(t, g, http.MethodPost, "http://h/other", nil); rec.StatusCode != http.StatusOK {
						t.Fatalf("unsafe POST status %d, want 200", rec.StatusCode)
					}
					ttlWantRetain(t, shared, ttlRetention, 2)
				})
			}
		}
	}

	t.Run("stored deadlines R-1ns and R+1ns", func(t *testing.T) {
		for _, soft := range []bool{false, true} {
			for _, in := range instants {
				t.Run(ttlMode(soft)+"/"+in.name, func(t *testing.T) {
					g, fake, origin, shared, _ := ttlSharedFixture(t, admin, ttlRoutes)
					origin.set(http.StatusOK, header, "body")
					h := shared.Handle()
					ttlPutDirect(t, h, "http://h/short", ttlRetention-time.Nanosecond)
					ttlPutDirect(t, h, "http://h/long", ttlRetention+time.Nanosecond)
					ttlPurge(t, g, fmt.Sprintf(`{"all":true,"soft":%t}`, soft))
					fake.Advance(in.at)

					for _, u := range []struct {
						url      string
						deadline time.Duration
					}{{"http://h/short", ttlRetention - time.Nanosecond}, {"http://h/long", ttlRetention + time.Nanosecond}} {
						_, c0, u0 := origin.counts()
						st := purgeLast(t, purgeGet(t, g, u.url, nil))
						_, c1, u1 := origin.counts()
						if st.Hit {
							t.Fatalf("%s at %s: %+v, want no hit", u.url, in.name, st)
						}
						if soft && in.at < u.deadline {
							if st.Fwd != httpcache.FwdStale || c1-c0 != 1 || u1-u0 != 0 {
								t.Fatalf("%s at %s: %+v, conditional=%d unconditional=%d, want stale conditional revalidation", u.url, in.name, st, c1-c0, u1-u0)
							}
						} else if st.Fwd != httpcache.FwdURIMiss || c1-c0 != 0 || u1-u0 != 1 {
							t.Fatalf("%s at %s: %+v, conditional=%d unconditional=%d, want uri-miss unconditional", u.url, in.name, st, c1-c0, u1-u0)
						}
					}
					ttlWantRetain(t, shared, ttlRetention, 1)
				})
			}
		}
	})
}

func TestPurgeRetainCeiling(t *testing.T) {
	t.Parallel()
	routes := cacheRouteYAML("r", "max_ttl: 8760h", "stale_while_revalidate: 8760h", "stale_if_error: 8760h", "keep: 8760h")
	g, _, origin, shared, _ := ttlSharedFixture(t, purgeAdminYAML(purgeTokA), routes)
	origin.set(http.StatusOK, ttlOriginHeader("max-age=31536000"), "body")
	if st := purgeLast(t, purgeGet(t, g, "http://h/p", nil)); !st.Stored {
		t.Fatalf("fill %+v, want stored", st)
	}
	if got := ttlPutTTLs(shared); !slices.Equal(got, []time.Duration{store.MaxTTL}) {
		t.Fatalf("put TTLs %v, want [%v]", got, store.MaxTTL)
	}
	ttlPurge(t, g, `{"all":true}`)
	ttlPurge(t, g, `{"urls":["http://h/p"],"soft":true}`)
	ttlWantRetain(t, shared, store.MaxTTL, 2)
}

func TestPurgeRetainHighWater(t *testing.T) {
	t.Parallel()
	const windows = time.Hour + 10*time.Minute
	const before = 48*time.Hour + windows
	admin := purgeAdminYAML(purgeTokA)
	high := cacheRouteYAML("r", "max_ttl: 48h", "stale_if_error: 1h", "keep: 10m")
	low := cacheRouteYAML("r", "max_ttl: 1h", "stale_if_error: 1h", "keep: 10m")
	for _, in := range []struct {
		name string
		at   time.Duration
	}{{"deadline-1ns", before - time.Nanosecond}, {"deadline", before}} {
		t.Run(in.name, func(t *testing.T) {
			g, fake, origin, shared, be := ttlSharedFixture(t, admin, high)
			origin.set(http.StatusOK, ttlOriginHeader("max-age=172800"), "body")
			if st := purgeLast(t, purgeGet(t, g, "http://h/hw", nil)); !st.Stored {
				t.Fatalf("fill %+v, want stored", st)
			}
			if got := ttlPutTTLs(shared); !slices.Equal(got, []time.Duration{before}) {
				t.Fatalf("put TTLs %v, want [%v]", got, before)
			}
			if err := g.ReloadYAML([]byte(cacheSharedDoc(be, admin, low))); err != nil {
				t.Fatalf("reload to max_ttl 1h: %v", err)
			}
			ttlPurge(t, g, `{"all":true,"soft":true}`)
			ttlWantRetain(t, shared, before, 1)

			fake.Advance(in.at)
			_, c0, u0 := origin.counts()
			st := purgeLast(t, purgeGet(t, g, "http://h/hw", nil))
			_, c1, u1 := origin.counts()
			if st.Hit {
				t.Fatalf("at %s: %+v, want no hit", in.name, st)
			}
			if in.at < before {
				if st.Fwd != httpcache.FwdStale || c1-c0 != 1 || u1-u0 != 0 {
					t.Fatalf("at %s: %+v, conditional=%d unconditional=%d, want stale conditional revalidation", in.name, st, c1-c0, u1-u0)
				}
			} else if st.Fwd != httpcache.FwdURIMiss || c1-c0 != 0 || u1-u0 != 1 {
				t.Fatalf("at %s: %+v, conditional=%d unconditional=%d, want uri-miss unconditional", in.name, st, c1-c0, u1-u0)
			}
		})
	}
}

type ttlDecision struct {
	hit         bool
	fwd         string
	ifNoneMatch string
}

func TestPurgeSkewSharedStore(t *testing.T) {
	t.Parallel()
	const target = "http://h/p"
	admin := purgeAdminYAML(purgeTokA)
	type scenario struct {
		name       string
		offA, offB time.Duration
		advA, advB time.Duration
	}
	scenarios := []scenario{
		{name: "unskewed"},
		{name: "gateway clocks at +1h and -1h", offA: time.Hour, offB: -time.Hour},
		{name: "gateway A advanced by 1h after the purge", advA: time.Hour},
		{name: "gateway B advanced by 1h after the purge", advB: time.Hour},
	}
	run := func(t *testing.T, sc scenario, soft bool, at time.Duration) ttlDecision {
		t.Helper()
		storeClk := clock.NewFake(cacheStart)
		shared := teststore.NewSharedCache(storeClk, 1<<20)
		t.Cleanup(func() { _ = shared.Close() })
		n := newMemNetwork()
		origin := newFlightOrigin()
		origin.set(http.StatusOK, ttlOriginHeader("max-age=3600, stale-if-error=1200"), "body")
		be := n.serve(t, origin)
		fakeA := clock.NewFake(cacheStart.Add(sc.offA))
		fakeB := clock.NewFake(cacheStart.Add(sc.offB))
		doc := cacheSharedDoc(be.URL, admin, ttlRoutes)
		opt := cacheSharedOption("shared", shared)
		a := n.gateway(t, doc, withClock(fakeA), opt)
		b := n.gateway(t, doc, withClock(fakeB), opt)
		if st := purgeLast(t, purgeGet(t, a, target, nil)); !st.Stored {
			t.Fatalf("fill via A %+v, want stored", st)
		}
		if got := purgeOK(t, purgeDo(t, a, purgeTokA, fmt.Sprintf(`{"urls":["%s"],"soft":%t}`, target, soft))); got.Scope != "store" {
			t.Fatalf("purge scope %q, want store", got.Scope)
		}
		if sc.advA > 0 {
			fakeA.Advance(sc.advA)
		}
		if sc.advB > 0 {
			fakeB.Advance(sc.advB)
		}
		storeClk.Advance(at)
		st := purgeLast(t, purgeGet(t, b, target, nil))
		return ttlDecision{hit: st.Hit, fwd: st.Fwd, ifNoneMatch: origin.lastHeader().Get("If-None-Match")}
	}

	for _, soft := range []bool{false, true} {
		for _, in := range []struct {
			name string
			at   time.Duration
		}{{"R-1ns", ttlRetention - time.Nanosecond}, {"R", ttlRetention}, {"R+1ns", ttlRetention + time.Nanosecond}} {
			t.Run(ttlMode(soft)+"/"+in.name, func(t *testing.T) {
				want := ttlDecision{hit: false, fwd: httpcache.FwdURIMiss}
				if soft && in.at < ttlRetention {
					want = ttlDecision{hit: false, fwd: httpcache.FwdStale, ifNoneMatch: `"v1"`}
				}
				ref := run(t, scenarios[0], soft, in.at)
				if ref != want {
					t.Fatalf("unskewed decision %+v, want %+v", ref, want)
				}
				for _, sc := range scenarios[1:] {
					if got := run(t, sc, soft, in.at); got != ref {
						t.Fatalf("%s: decision %+v, want the unskewed %+v", sc.name, got, ref)
					}
				}
			})
		}
	}
}
