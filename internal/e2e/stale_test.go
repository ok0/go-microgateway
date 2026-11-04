package e2e

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/teststore"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

const (
	staleLifetime    = 60 * time.Second
	staleSkewStorage = staleLifetime + time.Hour + 2*time.Hour
	staleSharedTop   = "stores:\n  - name: sc\n    type: shared\ncache:\n  store: sc\n"
)

var staleBackgroundResults = []string{"revalidated", "replaced", "not_stored", "error", "skipped"}

// staleSWRHeader is the origin header of stale-while-revalidate entries: a 60s lifetime, a 30s window and a validator.
func staleSWRHeader() http.Header {
	return http.Header{"Cache-Control": {"max-age=60, stale-while-revalidate=30"}, "Etag": {`"v1"`}}
}

// staleOrigin returns the origin header with the Cache-Control value cc and a validator.
func staleOrigin(cc string) http.Header {
	return http.Header{"Cache-Control": {cc}, "Etag": {`"v1"`}}
}

// staleRoute returns the route r with the cache block lines of route, one per line.
func staleRoute(route string) string {
	var lines []string
	if route != "" {
		lines = strings.Split(route, "\n")
	}
	return cacheRouteYAML("r", lines...)
}

// staleStoreTop returns the top-level block of a gateway whose cache uses the shared store sc, whose backend is
// identified by version: a change of version makes a new backend.
func staleStoreTop(version string) string {
	return fmt.Sprintf("stores:\n  - name: sc\n    type: shared\n    params:\n      version: %q\ncache:\n  store: sc\n", version)
}

// staleEnv is a gateway on a fake clock whose origin is o, with the backend be of network n.
type staleEnv struct {
	n    *memNetwork
	be   *testServer
	o    *flightOrigin
	fake *clock.Fake
	g    *microgateway.Gateway
}

// newStaleEnv returns a gateway built from the top-level block top and the routes, whose origin answers 200 with the
// staleSWRHeader and body "old" until the test changes it.
func newStaleEnv(t *testing.T, fake *clock.Fake, top, routes string, opts ...microgateway.Option) *staleEnv {
	t.Helper()
	n := newMemNetwork()
	o := newFlightOrigin()
	o.set(http.StatusOK, staleSWRHeader(), "old")
	be := n.serve(t, o)
	all := append([]microgateway.Option{withClock(fake)}, opts...)
	return &staleEnv{n: n, be: be, o: o, fake: fake, g: n.gateway(t, cacheGatewayDoc(be.URL, top, routes), all...)}
}

// staleSharedEnv returns an environment whose cache is the shared store sc, on the clock fake, and the store.
func staleSharedEnv(t *testing.T, fake *clock.Fake, routes string) (*staleEnv, *teststore.SharedCache) {
	t.Helper()
	shared := teststore.NewSharedCache(fake, 1<<20)
	t.Cleanup(func() { _ = shared.Close() })
	return newStaleEnv(t, fake, staleSharedTop, routes, cacheSharedOption("shared", shared)), shared
}

func (e *staleEnv) get(target string, hdr http.Header) *httptest.ResponseRecorder {
	return cacheDo(e.g, http.MethodGet, target, hdr)
}

// fill sends a GET of target and fails t unless the response is stored.
func (e *staleEnv) fill(t *testing.T, target string) {
	t.Helper()
	st := cacheLast(t, e.get(target, nil))
	testwait.Recv(t, "fill at origin", e.o.arrived, nil)
	if !st.Stored {
		t.Fatalf("fill %+v, want stored", st)
	}
}

// reload installs the configuration with the top-level block top and the routes on the same backend.
func (e *staleEnv) reload(t *testing.T, top, routes string) {
	t.Helper()
	if err := e.g.ReloadYAML([]byte(cacheGatewayDoc(e.be.URL, top, routes))); err != nil {
		t.Fatal(err)
	}
}

// settle waits until no background refresh of e runs.
func (e *staleEnv) settle(t *testing.T) {
	t.Helper()
	eventually(t, "background refreshes finished", func() bool {
		return inspect(e.g).Background == 0
	}, func() string {
		return fmt.Sprintf("Background=%d", inspect(e.g).Background)
	})
}

// staleWarmConns makes two concurrent misses at the origin, so that the transport keeps two idle connections before a
// goroutine baseline is taken.
func staleWarmConns(t *testing.T, e *staleEnv) {
	t.Helper()
	e.o.hold()
	a := e.async("http://example.com/warm-a", nil)
	b := e.async("http://example.com/warm-b", nil)
	testwait.Recv(t, "warm-up arrival", e.o.arrived, nil)
	testwait.Recv(t, "warm-up arrival", e.o.arrived, nil)
	e.o.release()
	coalesceRecv(t, a)
	coalesceRecv(t, b)
}

func (e *staleEnv) async(target string, hdr http.Header) <-chan *httptest.ResponseRecorder {
	return coalesceAsync(context.Background(), e.g, target, hdr)
}

// staleReleaseOrigin makes the origin answer conditional requests with 304 and every other request with 200 "old".
func staleReleaseOrigin(o *flightOrigin) {
	o.setBegin(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != "" {
			writeOrigin(w, http.StatusNotModified, staleSWRHeader(), "")
			return
		}
		writeOrigin(w, http.StatusOK, staleSWRHeader(), "old")
	})
}

func staleBackground(t *testing.T, g *microgateway.Gateway, result string) float64 {
	t.Helper()
	return rsMetricOr0(t, g, "microgateway_cache_background_revalidations_total", "route", "r", "result", result)
}

// staleExpectBackground fails t unless the background result want has count 1 and every other result has count 0. An
// empty want expects no background refresh at all.
func staleExpectBackground(t *testing.T, g *microgateway.Gateway, want string) {
	t.Helper()
	for _, res := range staleBackgroundResults {
		w := 0.0
		if res == want {
			w = 1
		}
		if got := staleBackground(t, g, res); got != w {
			t.Fatalf("background %s = %v, want %v", res, got, w)
		}
	}
}

func staleRequests(t *testing.T, g *microgateway.Gateway, result string) float64 {
	t.Helper()
	return rsMetricOr0(t, g, "microgateway_cache_requests_total", "route", "r", "result", result)
}

func staleStores(t *testing.T, g *microgateway.Gateway, result string) float64 {
	t.Helper()
	return rsMetricOr0(t, g, "microgateway_cache_stores_total", "route", "r", "result", result)
}

// staleNoStoreErrors fails t when a store error counter of g is non-zero or the route counted a store error outcome.
func staleNoStoreErrors(t *testing.T, g *microgateway.Gateway) {
	t.Helper()
	for _, line := range strings.Split(metricsText(t, g), "\n") {
		if !strings.HasPrefix(line, "microgateway_cache_store_errors_total{") {
			continue
		}
		v, err := strconv.ParseFloat(line[strings.LastIndexByte(line, ' ')+1:], 64)
		if err != nil {
			t.Fatal(err)
		}
		if v != 0 {
			t.Fatalf("store error series %q, want zero", line)
		}
	}
	if v := staleStores(t, g, "error"); v != 0 {
		t.Fatalf("stores error = %v, want 0", v)
	}
}

// staleExpectOrigin fails t unless rec carries the origin response with status code and body "fail", and no stored
// entry was served in its place.
func staleExpectOrigin(t *testing.T, rec *httptest.ResponseRecorder, code int) {
	t.Helper()
	if rec.Code != code || rec.Body.String() != "fail" {
		t.Fatalf("client response %d %q, want %d fail", rec.Code, rec.Body.String(), code)
	}
	if st := cacheLast(t, rec); st.Detail == "stale-if-error" {
		t.Fatalf("status %+v, want no stale-if-error", st)
	}
}

// stalePutTTLs returns the storage TTLs of the puts recorded by shared, in call order.
func stalePutTTLs(shared *teststore.SharedCache) []time.Duration {
	var out []time.Duration
	for _, c := range shared.Calls() {
		if c.Op == "put" {
			out = append(out, c.TTL)
		}
	}
	return out
}

func TestStaleWhileRevalidateGateway(t *testing.T) {
	t.Parallel()
	t.Run("windows", func(t *testing.T) {
		t.Parallel()
		const swr = 30 * time.Second
		cases := []struct {
			name  string
			at    time.Duration
			fresh bool
			sync  bool
			ttl   int64
		}{
			{name: "L-1ns is fresh", at: staleLifetime - time.Nanosecond, fresh: true},
			{name: "L is served stale", at: staleLifetime, ttl: 0},
			{name: "L+1ns is served stale", at: staleLifetime + time.Nanosecond, ttl: -1},
			{name: "L+swr-1ns is served stale", at: staleLifetime + swr - time.Nanosecond, ttl: -30},
			{name: "L+swr is revalidated synchronously", at: staleLifetime + swr, sync: true},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				t.Parallel()
				fake := clock.NewFake(cacheStart)
				e := newStaleEnv(t, fake, "", cacheRouteYAML("r", "keep: 1h"))
				e.fill(t, "/swr")
				e.o.set(http.StatusNotModified, staleSWRHeader(), "")
				fake.Advance(c.at)
				rec := e.get("/swr", nil)
				st := cacheLast(t, rec)
				if rec.Code != http.StatusOK || rec.Body.String() != "old" {
					t.Fatalf("client response %d %q, want 200 old", rec.Code, rec.Body.String())
				}
				calls, _, _ := e.o.counts()
				switch {
				case c.fresh:
					if !st.Hit || st.Fwd != "" || calls != 1 {
						t.Fatalf("status %+v with %d origin calls, want a fresh hit without forward", st, calls)
					}
					staleExpectBackground(t, e.g, "")
				case c.sync:
					if st.Fwd != httpcache.FwdStale || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified {
						t.Fatalf("status %+v, want revalidated with fwd-status 304", st)
					}
					if got := e.o.lastHeader().Get("If-None-Match"); got != `"v1"` {
						t.Fatalf("origin If-None-Match %q, want \"v1\"", got)
					}
					if v := staleRequests(t, e.g, "revalidated"); v != 1 {
						t.Fatalf("revalidated count %v, want 1", v)
					}
					staleExpectBackground(t, e.g, "")
				default:
					if !st.Hit || st.Fwd != httpcache.FwdStale || st.Detail != "stale-while-revalidate" || !st.HasTTL || st.TTL != c.ttl {
						t.Fatalf("status %+v, want stale hit under stale-while-revalidate with ttl %d", st, c.ttl)
					}
					if got, want := rec.Header().Get("Age"), strconv.FormatInt(int64(c.at/time.Second), 10); got != want {
						t.Fatalf("Age %q, want %s", got, want)
					}
					e.settle(t)
					if calls, _, _ := e.o.counts(); calls != 2 {
						t.Fatalf("origin calls %d, want fill and one background refresh", calls)
					}
					staleExpectBackground(t, e.g, "revalidated")
				}
			})
		}
	})

	t.Run("storage ends at lifetime plus window", func(t *testing.T) {
		t.Parallel()
		const storage = staleLifetime + 30*time.Second
		t.Run("one nanosecond before serves stale and refreshes", func(t *testing.T) {
			t.Parallel()
			fake := clock.NewFake(cacheStart)
			e := newStaleEnv(t, fake, "", cacheRouteYAML("r"))
			e.fill(t, "/swr")
			e.o.set(http.StatusNotModified, staleSWRHeader(), "")
			fake.Advance(storage - time.Nanosecond)
			st := cacheLast(t, e.get("/swr", nil))
			if !st.Hit || st.Fwd != httpcache.FwdStale || st.Detail != "stale-while-revalidate" {
				t.Fatalf("status %+v, want stale-while-revalidate hit", st)
			}
			e.settle(t)
			if got := e.o.lastHeader().Get("If-None-Match"); got != `"v1"` {
				t.Fatalf("origin If-None-Match %q, want \"v1\"", got)
			}
			staleExpectBackground(t, e.g, "revalidated")
		})
		for _, c := range []struct {
			name string
			at   time.Duration
		}{
			{name: "at the boundary is a miss", at: storage},
			{name: "one nanosecond after is a miss", at: storage + time.Nanosecond},
		} {
			t.Run(c.name, func(t *testing.T) {
				t.Parallel()
				fake := clock.NewFake(cacheStart)
				e := newStaleEnv(t, fake, "", cacheRouteYAML("r"))
				e.fill(t, "/swr")
				fake.Advance(c.at)
				if st := cacheLast(t, e.get("/swr", nil)); st.Fwd != httpcache.FwdURIMiss {
					t.Fatalf("fwd %q, want uri-miss", st.Fwd)
				}
				if got := e.o.lastHeader().Get("If-None-Match"); got != "" {
					t.Fatalf("origin request still conditional (%q)", got)
				}
			})
		}
	})

	t.Run("one refresh per key under concurrent stale requests", func(t *testing.T) {
		t.Parallel()
		fake := clock.NewFake(cacheStart)
		e := newStaleEnv(t, fake, "", cacheRouteYAML("r"))
		e.fill(t, "/one")
		fake.Advance(61 * time.Second)
		e.o.set(http.StatusNotModified, staleSWRHeader(), "")
		e.o.hold()
		const n = 20
		recs := make(chan *httptest.ResponseRecorder, n)
		for range n {
			go func() { recs <- e.get("/one", nil) }()
		}
		for range n {
			rec, _ := testwait.Recv(t, "stale response", recs, nil)
			if rec.Code != http.StatusOK || rec.Body.String() != "old" {
				t.Fatalf("response %d %q, want 200 old", rec.Code, rec.Body.String())
			}
			if st := cacheLast(t, rec); st.Detail != "stale-while-revalidate" {
				t.Fatalf("status %+v, want stale-while-revalidate", st)
			}
		}
		testwait.Recv(t, "refresh at origin", e.o.arrived, nil)
		if v := staleRequests(t, e.g, "stale"); v != n {
			t.Fatalf("stale count %v, want %d", v, n)
		}
		if v := staleBackground(t, e.g, "skipped"); v != n-1 {
			t.Fatalf("skipped refreshes %v, want %d", v, n-1)
		}
		e.o.release()
		e.settle(t)
		if v := staleBackground(t, e.g, "revalidated"); v != 1 {
			t.Fatalf("revalidated refreshes %v, want 1", v)
		}
		if calls, _, _ := e.o.counts(); calls != 2 {
			t.Fatalf("origin calls %d, want fill and one refresh", calls)
		}
	})

	t.Run("refresh outcomes", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name    string
			arrange func(t *testing.T, e *staleEnv)
			result  string
		}{
			{name: "304 revalidates", result: "revalidated", arrange: func(t *testing.T, e *staleEnv) {
				e.o.set(http.StatusNotModified, staleSWRHeader(), "")
			}},
			{name: "200 replaces", result: "replaced", arrange: func(t *testing.T, e *staleEnv) {
				e.o.set(http.StatusOK, coalesceHot(), "new")
			}},
			{name: "500 is not stored", result: "not_stored", arrange: func(t *testing.T, e *staleEnv) {
				e.o.set(http.StatusInternalServerError, nil, "err")
			}},
			{name: "connection refused is an error", result: "error", arrange: func(t *testing.T, e *staleEnv) {
				e.n.refuse(t, e.be)
				e.o.setBegin(func(w http.ResponseWriter, r *http.Request) {
					conn, _, err := http.NewResponseController(w).Hijack()
					if err != nil {
						panic(err)
					}
					_ = conn.Close()
				})
			}},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				t.Parallel()
				fake := clock.NewFake(cacheStart)
				e := newStaleEnv(t, fake, "", cacheRouteYAML("r"))
				e.fill(t, "/rf")
				fake.Advance(61 * time.Second)
				c.arrange(t, e)
				rec := e.get("/rf", nil)
				if rec.Code != http.StatusOK || rec.Body.String() != "old" {
					t.Fatalf("triggering response %d %q, want 200 old", rec.Code, rec.Body.String())
				}
				e.settle(t)
				staleExpectBackground(t, e.g, c.result)
			})
		}
		t.Run("open breaker is an error", func(t *testing.T) {
			t.Parallel()
			n := newMemNetwork()
			o := newFlightOrigin()
			o.set(http.StatusOK, staleSWRHeader(), "old")
			be := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/bad" {
					panic(http.ErrAbortHandler)
				}
				o.ServeHTTP(w, r)
			}))
			fake := clock.NewFake(cacheStart)
			doc := rsDoc{hosts: []string{be.URL}, breaker: rsBreaker, routes: []rsRoute{
				{name: "r", path: "/", extra: "cache:\n      enabled: true\n      stale_while_revalidate: 30s\n"},
			}}.yaml()
			g := n.gateway(t, doc, withClock(fake))
			const target = "http://example.com/rf"
			cacheDo(g, http.MethodGet, target, nil)
			fake.Advance(61 * time.Second)
			for range 4 {
				cacheDo(g, http.MethodGet, "http://example.com/bad", nil)
			}
			o.set(http.StatusNotModified, staleSWRHeader(), "")
			rec := cacheDo(g, http.MethodGet, target, nil)
			if rec.Code != http.StatusOK || rec.Body.String() != "old" {
				t.Fatalf("triggering response %d %q, want 200 old", rec.Code, rec.Body.String())
			}
			eventually(t, "background refresh finished", func() bool { return inspect(g).Background == 0 }, nil)
			staleExpectBackground(t, g, "error")
			if calls, _, _ := o.counts(); calls != 1 {
				t.Fatalf("origin calls %d, want only the fill", calls)
			}
		})
	})

	t.Run("request rules", func(t *testing.T) {
		t.Parallel()
		newStale := func(t *testing.T) *staleEnv {
			fake := clock.NewFake(cacheStart)
			e := newStaleEnv(t, fake, "", cacheRouteYAML("r"))
			e.fill(t, "/rr")
			e.o.set(http.StatusNotModified, staleSWRHeader(), "")
			return e
		}
		t.Run("only-if-cached serves the stale entry", func(t *testing.T) {
			t.Parallel()
			e := newStale(t)
			e.fake.Advance(61 * time.Second)
			rec := e.get("/rr", http.Header{"Cache-Control": {"only-if-cached"}})
			if rec.Code != http.StatusOK || rec.Body.String() != "old" {
				t.Fatalf("response %d %q, want 200 old", rec.Code, rec.Body.String())
			}
			e.settle(t)
			if calls, _, _ := e.o.counts(); calls != 1 {
				t.Fatalf("origin calls %d, want 1", calls)
			}
			staleExpectBackground(t, e.g, "")
		})
		t.Run("max-age=0 revalidates synchronously", func(t *testing.T) {
			t.Parallel()
			e := newStale(t)
			e.fake.Advance(61 * time.Second)
			rec := e.get("/rr", http.Header{"Cache-Control": {"max-age=0"}})
			calls, _, _ := e.o.counts()
			if rec.Body.String() != "old" || calls != 2 || e.o.lastHeader().Get("If-None-Match") != `"v1"` {
				t.Fatalf("response %q with %d origin calls, want a synchronous conditional revalidation", rec.Body.String(), calls)
			}
			if v := staleRequests(t, e.g, "revalidated"); v != 1 {
				t.Fatalf("revalidated count %v, want 1", v)
			}
			staleExpectBackground(t, e.g, "")
		})
		t.Run("min-fresh revalidates a fresh entry synchronously", func(t *testing.T) {
			t.Parallel()
			e := newStale(t)
			e.fake.Advance(55 * time.Second)
			rec := e.get("/rr", http.Header{"Cache-Control": {"min-fresh=10"}})
			calls, _, _ := e.o.counts()
			if rec.Body.String() != "old" || calls != 2 || e.o.lastHeader().Get("If-None-Match") != `"v1"` {
				t.Fatalf("response %q with %d origin calls, want a synchronous conditional revalidation", rec.Body.String(), calls)
			}
			staleExpectBackground(t, e.g, "")
		})
		t.Run("max-stale serves without a refresh", func(t *testing.T) {
			t.Parallel()
			e := newStale(t)
			e.fake.Advance(61 * time.Second)
			rec := e.get("/rr", http.Header{"Cache-Control": {"max-stale=100"}})
			if st := cacheLast(t, rec); !st.Hit || st.Fwd != "" || st.Detail != "" || rec.Body.String() != "old" {
				t.Fatalf("status %+v body %q, want a stale hit with no forward", st, rec.Body.String())
			}
			e.settle(t)
			if calls, _, _ := e.o.counts(); calls != 1 {
				t.Fatalf("origin calls %d, want 1", calls)
			}
			staleExpectBackground(t, e.g, "")
		})
		t.Run("no-cache revalidates synchronously", func(t *testing.T) {
			t.Parallel()
			e := newStale(t)
			e.fake.Advance(61 * time.Second)
			rec := e.get("/rr", http.Header{"Cache-Control": {"no-cache"}})
			calls, _, _ := e.o.counts()
			if rec.Body.String() != "old" || calls != 2 || e.o.lastHeader().Get("If-None-Match") != `"v1"` {
				t.Fatalf("response %q with %d origin calls, want a synchronous conditional revalidation", rec.Body.String(), calls)
			}
			staleExpectBackground(t, e.g, "")
		})
	})
}

func TestStaleBackgroundRunnerGateway(t *testing.T) {
	t.Parallel()
	pre := snapGoroutines()
	fake := clock.NewFake(cacheStart)
	e := newStaleEnv(t, fake, "cache:\n  max_background: 1\n", cacheRouteYAML("r"))
	e.fill(t, "/a")
	e.fill(t, "/b")
	base := snapGoroutines()

	fake.Advance(61 * time.Second)
	e.o.set(http.StatusNotModified, staleSWRHeader(), "")
	e.o.hold()
	if st := cacheLast(t, e.get("/a", nil)); st.Detail != "stale-while-revalidate" {
		t.Fatalf("status %+v for a, want stale-while-revalidate", st)
	}
	testwait.Recv(t, "refresh of a at origin", e.o.arrived, nil)
	if st := cacheLast(t, e.get("/b", nil)); st.Detail != "stale-while-revalidate" {
		t.Fatalf("status %+v for b, want stale-while-revalidate", st)
	}
	if v := staleBackground(t, e.g, "skipped"); v != 1 {
		t.Fatalf("skipped refreshes %v, want 1", v)
	}
	e.o.release()
	eventually(t, "refresh of a revalidated", func() bool {
		return staleBackground(t, e.g, "revalidated") == 1
	}, nil)
	e.settle(t)
	if st := cacheLast(t, e.get("/b", nil)); st.Detail != "stale-while-revalidate" {
		t.Fatalf("status %+v for b after release, want stale-while-revalidate", st)
	}
	testwait.Recv(t, "refresh of b at origin", e.o.arrived, nil)
	e.settle(t)
	if v := staleBackground(t, e.g, "revalidated"); v != 2 {
		t.Fatalf("revalidated refreshes %v, want 2", v)
	}
	if v := staleBackground(t, e.g, "skipped"); v != 1 {
		t.Fatalf("skipped refreshes %v, want 1", v)
	}

	fake.Advance(61 * time.Second)
	e.o.hold()
	if st := cacheLast(t, e.get("/a", nil)); st.Detail != "stale-while-revalidate" {
		t.Fatalf("status %+v for a, want stale-while-revalidate", st)
	}
	testwait.Recv(t, "second refresh of a at origin", e.o.arrived, nil)
	if st := cacheLast(t, e.get("/b", nil)); st.Detail != "stale-while-revalidate" {
		t.Fatalf("status %+v for b, want stale-while-revalidate", st)
	}
	if v := staleBackground(t, e.g, "skipped"); v != 2 {
		t.Fatalf("skipped refreshes %v, want 2", v)
	}
	if err := e.g.Close(); err != nil {
		t.Fatal(err)
	}
	testwait.Recv(t, "origin observed the refresh context end", e.o.gone, nil)
	eventually(t, "no live generation and no background refresh after close", func() bool {
		return inspect(e.g).LiveGenerations == 0 && inspect(e.g).Background == 0
	}, func() string {
		return fmt.Sprintf("LiveGenerations=%d Background=%d", inspect(e.g).LiveGenerations, inspect(e.g).Background)
	})
	if v := staleBackground(t, e.g, "not_stored"); v != 1 {
		t.Fatalf("not_stored refreshes %v, want 1", v)
	}
	if v := staleBackground(t, e.g, "revalidated"); v != 2 {
		t.Fatalf("revalidated refreshes %v, want 2", v)
	}
	if calls, _, _ := e.o.counts(); calls != 5 {
		t.Fatalf("origin calls %d, want two fills, two refreshes and the refresh of a after the second release", calls)
	}
	awaitGoroutinesSettled(t, "background refresh goroutines after close", pre, base)
}

func TestStaleBackgroundReleaseGateway(t *testing.T) {
	t.Parallel()
	const target = "http://example.com/rel"
	const other = "  - name: other\n    match:\n      path: /other\n    upstream: up\n"

	t.Run("reload with a new memory cache drains the refresh", func(t *testing.T) {
		t.Parallel()
		pre := snapGoroutines()
		fake := clock.NewFake(cacheStart)
		e := newStaleEnv(t, fake, "", cacheRouteYAML("r"))
		staleReleaseOrigin(e.o)
		e.fill(t, target)
		staleWarmConns(t, e)
		base := snapGoroutines()

		fake.Advance(61 * time.Second)
		e.o.hold()
		if st := cacheLast(t, e.get(target, nil)); st.Detail != "stale-while-revalidate" {
			t.Fatalf("status %+v, want stale-while-revalidate", st)
		}
		testwait.Recv(t, "refresh at origin", e.o.arrived, nil)
		storedBefore := staleStores(t, e.g, "stored")
		miss := e.async("http://example.com/miss", nil)
		testwait.Recv(t, "leader at origin", e.o.arrived, nil)
		e.reload(t, "cache:\n  memory:\n    max_bytes: 33554432\n", cacheRouteYAML("r"))
		if live := inspect(e.g).LiveGenerations; live != 2 {
			t.Fatalf("live generations %d, want 2 while the refresh and the leader run", live)
		}
		e.o.release()
		coalesceOK(t, coalesceRecv(t, miss), "old")
		eventually(t, "old generation released after its refresh", func() bool {
			return inspect(e.g).LiveGenerations == 1
		}, func() string {
			return fmt.Sprintf("LiveGenerations=%d", inspect(e.g).LiveGenerations)
		})
		if v := staleBackground(t, e.g, "revalidated"); v != 1 {
			t.Fatalf("revalidated = %v, want 1", v)
		}
		if v := staleStores(t, e.g, "stored") - storedBefore; v != 2 {
			t.Fatalf("stores stored incremented by %v, want 2 (refresh merge and leader fill)", v)
		}
		staleNoStoreErrors(t, e.g)
		awaitGoroutinesSettled(t, "goroutines after reload with a new memory cache", pre, base)
	})

	t.Run("reload with an unchanged cache drains the refresh", func(t *testing.T) {
		t.Parallel()
		pre := snapGoroutines()
		fake := clock.NewFake(cacheStart)
		e := newStaleEnv(t, fake, "", cacheRouteYAML("r"))
		staleReleaseOrigin(e.o)
		e.fill(t, target)
		staleWarmConns(t, e)
		base := snapGoroutines()

		fake.Advance(61 * time.Second)
		e.o.hold()
		if st := cacheLast(t, e.get(target, nil)); st.Detail != "stale-while-revalidate" {
			t.Fatalf("status %+v, want stale-while-revalidate", st)
		}
		testwait.Recv(t, "refresh at origin", e.o.arrived, nil)
		e.reload(t, "", cacheRouteYAML("r")+other)
		if live := inspect(e.g).LiveGenerations; live != 2 {
			t.Fatalf("live generations %d, want 2 while the refresh runs", live)
		}
		e.o.release()
		eventually(t, "old generation released after its refresh", func() bool {
			return inspect(e.g).LiveGenerations == 1
		}, func() string {
			return fmt.Sprintf("LiveGenerations=%d", inspect(e.g).LiveGenerations)
		})
		if v := staleBackground(t, e.g, "revalidated"); v != 1 {
			t.Fatalf("revalidated = %v, want 1", v)
		}
		if st := cacheLast(t, e.get(target, nil)); !st.Hit || !st.HasTTL || st.TTL != 60 {
			t.Fatalf("status %+v, want a fresh hit with ttl 60", st)
		}
		staleNoStoreErrors(t, e.g)
		awaitGoroutinesSettled(t, "goroutines after reload with an unchanged cache", pre, base)
	})

	t.Run("close cancels the refresh", func(t *testing.T) {
		t.Parallel()
		pre := snapGoroutines()
		fake := clock.NewFake(cacheStart)
		e := newStaleEnv(t, fake, "", cacheRouteYAML("r"))
		staleReleaseOrigin(e.o)
		e.fill(t, target)
		base := snapGoroutines()

		fake.Advance(61 * time.Second)
		e.o.hold()
		if st := cacheLast(t, e.get(target, nil)); st.Detail != "stale-while-revalidate" {
			t.Fatalf("status %+v, want stale-while-revalidate", st)
		}
		testwait.Recv(t, "refresh at origin", e.o.arrived, nil)
		if err := e.g.Close(); err != nil {
			t.Fatal(err)
		}
		testwait.Recv(t, "origin observed the refresh context end", e.o.gone, nil)
		eventually(t, "no live generation and no background refresh after close", func() bool {
			return inspect(e.g).LiveGenerations == 0 && inspect(e.g).Background == 0
		}, func() string {
			return fmt.Sprintf("LiveGenerations=%d Background=%d", inspect(e.g).LiveGenerations, inspect(e.g).Background)
		})
		staleExpectBackground(t, e.g, "not_stored")
		staleNoStoreErrors(t, e.g)
		awaitGoroutinesSettled(t, "goroutines after close with a refresh in flight", pre, base)
	})

	t.Run("named store reload", func(t *testing.T) {
		t.Parallel()
		pre := snapGoroutines()
		fake := clock.NewFake(cacheStart)
		shared := teststore.NewSharedCache(fake, 1<<20)
		t.Cleanup(func() { _ = shared.Close() })
		e := newStaleEnv(t, fake, staleStoreTop("1"), cacheRouteYAML("r"), cacheSharedOption("shared", shared))
		staleReleaseOrigin(e.o)
		e.fill(t, target)
		staleWarmConns(t, e)
		base := snapGoroutines()
		handles := shared.Handles()
		old := handles[len(handles)-1]

		fake.Advance(61 * time.Second)
		e.o.hold()
		if st := cacheLast(t, e.get(target, nil)); st.Detail != "stale-while-revalidate" {
			t.Fatalf("status %+v, want stale-while-revalidate", st)
		}
		testwait.Recv(t, "refresh at origin", e.o.arrived, nil)
		e.reload(t, staleStoreTop("2"), cacheRouteYAML("r"))
		if n := old.Closes(); n != 0 {
			t.Fatalf("old backend closed %d times while its refresh runs, want 0", n)
		}
		if live := inspect(e.g).LiveGenerations; live != 2 {
			t.Fatalf("live generations %d, want 2 while the refresh runs", live)
		}
		e.o.release()
		eventually(t, "old generation released after its refresh", func() bool {
			return inspect(e.g).LiveGenerations == 1
		}, func() string {
			return fmt.Sprintf("LiveGenerations=%d", inspect(e.g).LiveGenerations)
		})
		if n := old.Closes(); n != 1 {
			t.Fatalf("old backend closed %d times after drain, want 1", n)
		}
		if v := staleBackground(t, e.g, "revalidated"); v != 1 {
			t.Fatalf("revalidated = %v, want 1", v)
		}
		staleNoStoreErrors(t, e.g)
		awaitGoroutinesSettled(t, "goroutines after named store reload", pre, base)
	})
}

func TestStaleIfErrorGateway(t *testing.T) {
	t.Parallel()
	const target = "http://example.com/sie"
	sieFill := func(t *testing.T, route string, fill http.Header) *staleEnv {
		t.Helper()
		e := newStaleEnv(t, clock.NewFake(cacheStart), "", staleRoute(route))
		e.o.set(http.StatusOK, fill, "old")
		e.fill(t, target)
		return e
	}

	t.Run("failure statuses serve the stale entry", func(t *testing.T) {
		t.Parallel()
		for _, code := range []int{500, 502, 503, 504} {
			t.Run(strconv.Itoa(code), func(t *testing.T) {
				t.Parallel()
				e := sieFill(t, "stale_if_error: 1h", staleOrigin("max-age=60"))
				e.fake.Advance(61 * time.Second)
				e.o.set(code, http.Header{"Retry-After": {"5"}}, "fail")
				rec := e.get(target, nil)
				if rec.Code != http.StatusOK || rec.Body.String() != "old" {
					t.Fatalf("client response %d %q, want 200 old", rec.Code, rec.Body.String())
				}
				if v := rec.Header().Get("Retry-After"); v != "" {
					t.Fatalf("Retry-After %q reached the client", v)
				}
				st := cacheLast(t, rec)
				if !st.Hit || st.Fwd != httpcache.FwdStale || !st.HasFwdStatus || st.FwdStatus != code || st.Detail != "stale-if-error" || !st.HasTTL || st.TTL != -1 {
					t.Fatalf("status %+v, want hit fwd=stale fwd-status=%d detail=stale-if-error ttl -1", st, code)
				}
				if got := rec.Header().Get("Age"); got != "61" {
					t.Fatalf("Age %q, want 61", got)
				}
				if v := staleRequests(t, e.g, "stale"); v != 1 {
					t.Fatalf("stale count %v, want 1", v)
				}
				if v := staleStores(t, e.g, "stored"); v != 1 {
					t.Fatalf("stored count %v, want only the fill", v)
				}
				if v := staleStores(t, e.g, "not_storable"); v != 1 {
					t.Fatalf("not_storable count %v, want 1", v)
				}
			})
		}
	})

	t.Run("statuses outside the failure set reach the client", func(t *testing.T) {
		t.Parallel()
		for _, code := range []int{501, 404, 302, 400} {
			t.Run(strconv.Itoa(code), func(t *testing.T) {
				t.Parallel()
				e := sieFill(t, "stale_if_error: 1h", staleOrigin("max-age=60"))
				e.fake.Advance(61 * time.Second)
				e.o.set(code, http.Header{"Cache-Control": {"no-store"}}, "fail")
				rec := e.get(target, nil)
				if rec.Code != code || rec.Body.String() != "fail" {
					t.Fatalf("client response %d %q, want %d fail", rec.Code, rec.Body.String(), code)
				}
				if st := cacheLast(t, rec); st.Detail == "stale-if-error" {
					t.Fatalf("status %+v, want no stale-if-error", st)
				}
			})
		}
	})

	t.Run("304 revalidates the entry", func(t *testing.T) {
		t.Parallel()
		e := sieFill(t, "stale_if_error: 1h", staleOrigin("max-age=60"))
		e.fake.Advance(61 * time.Second)
		e.o.set(http.StatusNotModified, staleOrigin("max-age=60"), "")
		rec := e.get(target, nil)
		st := cacheLast(t, rec)
		if rec.Code != http.StatusOK || rec.Body.String() != "old" || !st.Hit || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified {
			t.Fatalf("response %d %q status %+v, want 200 old revalidated with fwd-status 304", rec.Code, rec.Body.String(), st)
		}
		if v := staleRequests(t, e.g, "revalidated"); v != 1 {
			t.Fatalf("revalidated count %v, want 1", v)
		}
	})

	t.Run("window boundaries", func(t *testing.T) {
		t.Parallel()
		const window = 30 * time.Second
		cases := []struct {
			name  string
			route string
			cc    string
			at    time.Duration
			stale bool
			age   int64
			ttl   int64
			put   time.Duration
		}{
			{name: "directive one nanosecond inside the window", cc: "max-age=60, stale-if-error=30", at: staleLifetime + window - time.Nanosecond, stale: true, age: 89, ttl: -30, put: staleLifetime + window},
			{name: "directive at the window end", cc: "max-age=60, stale-if-error=30", at: staleLifetime + window, put: staleLifetime + window},
			{name: "directive one nanosecond after the window end", cc: "max-age=60, stale-if-error=30", at: staleLifetime + window + time.Nanosecond, put: staleLifetime + window},
			{name: "route window one nanosecond inside", route: "stale_if_error: 30s", cc: "max-age=60", at: staleLifetime + window - time.Nanosecond, stale: true, age: 89, ttl: -30, put: staleLifetime + window},
			{name: "route window at the window end", route: "stale_if_error: 30s", cc: "max-age=60", at: staleLifetime + window, put: staleLifetime + window},
			{name: "route window one nanosecond after the window end", route: "stale_if_error: 30s", cc: "max-age=60", at: staleLifetime + window + time.Nanosecond, put: staleLifetime + window},
			{name: "directive 0 overrides the route window", route: "stale_if_error: 1h", cc: "max-age=60, stale-if-error=0", at: staleLifetime + time.Nanosecond, put: staleLifetime},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				t.Parallel()
				fake := clock.NewFake(cacheStart)
				e, shared := staleSharedEnv(t, fake, staleRoute(c.route))
				e.o.set(http.StatusOK, staleOrigin(c.cc), "old")
				e.fill(t, target)
				if puts := stalePutTTLs(shared); len(puts) != 1 || puts[0] != c.put {
					t.Fatalf("puts %v, want only the fill with storage ttl %v", puts, c.put)
				}
				fake.Advance(c.at)
				e.o.set(http.StatusInternalServerError, http.Header{"Retry-After": {"5"}}, "fail")
				rec := e.get(target, nil)
				if !c.stale {
					staleExpectOrigin(t, rec, http.StatusInternalServerError)
					return
				}
				st := cacheLast(t, rec)
				if rec.Code != http.StatusOK || rec.Body.String() != "old" || st.Fwd != httpcache.FwdStale || st.FwdStatus != http.StatusInternalServerError || st.Detail != "stale-if-error" || !st.HasTTL || st.TTL != c.ttl {
					t.Fatalf("response %d %q status %+v, want 200 old stale-if-error with ttl %d", rec.Code, rec.Body.String(), st, c.ttl)
				}
				if got := rec.Header().Get("Age"); got != strconv.FormatInt(c.age, 10) {
					t.Fatalf("Age %q, want %d", got, c.age)
				}
			})
		}
	})

	t.Run("prohibitions", func(t *testing.T) {
		t.Parallel()
		t.Run("must-revalidate", func(t *testing.T) {
			t.Parallel()
			e := sieFill(t, "stale_if_error: 1h", staleOrigin("max-age=60, must-revalidate"))
			e.fake.Advance(61 * time.Second)
			e.o.set(http.StatusInternalServerError, nil, "fail")
			staleExpectOrigin(t, e.get(target, nil), http.StatusInternalServerError)
		})
		t.Run("no-cache with keep", func(t *testing.T) {
			t.Parallel()
			e := sieFill(t, "keep: 1h", staleOrigin("max-age=60, no-cache"))
			e.fake.Advance(61 * time.Second)
			e.o.set(http.StatusInternalServerError, nil, "fail")
			staleExpectOrigin(t, e.get(target, nil), http.StatusInternalServerError)
		})
		t.Run("soft-purged entry is revalidated", func(t *testing.T) {
			t.Parallel()
			fake := clock.NewFake(cacheStart)
			e, shared := staleSharedEnv(t, fake, staleRoute("stale_if_error: 1h"))
			e.o.set(http.StatusOK, staleOrigin("max-age=60"), "old")
			e.fill(t, target)
			fake.Advance(61 * time.Second)
			primary := httpcache.PrimaryKey(httptest.NewRequest(http.MethodGet, target, nil))
			if _, err := shared.Purge(context.Background(), store.Purge{Kind: store.PurgeKeys, Keys: []string{primary}, Soft: true, Retain: time.Hour}); err != nil {
				t.Fatal(err)
			}
			e.o.set(http.StatusInternalServerError, nil, "fail")
			staleExpectOrigin(t, e.get(target, nil), http.StatusInternalServerError)
			if got := e.o.lastHeader().Get("If-None-Match"); got != `"v1"` {
				t.Fatalf("origin If-None-Match %q, want \"v1\"", got)
			}
		})
		t.Run("fresh entry revalidated by max-age=0", func(t *testing.T) {
			t.Parallel()
			e := sieFill(t, "stale_if_error: 1h", staleOrigin("max-age=60"))
			e.fake.Advance(30 * time.Second)
			e.o.set(http.StatusInternalServerError, nil, "fail")
			staleExpectOrigin(t, e.get(target, http.Header{"Cache-Control": {"max-age=0"}}), http.StatusInternalServerError)
		})
		t.Run("no entry", func(t *testing.T) {
			t.Parallel()
			e := newStaleEnv(t, clock.NewFake(cacheStart), "", staleRoute("stale_if_error: 1h"))
			e.o.set(http.StatusInternalServerError, nil, "fail")
			staleExpectOrigin(t, e.get(target, nil), http.StatusInternalServerError)
		})
		t.Run("only-if-cached", func(t *testing.T) {
			t.Parallel()
			e := sieFill(t, "stale_if_error: 1h", staleOrigin("max-age=60"))
			e.fake.Advance(61 * time.Second)
			e.o.set(http.StatusInternalServerError, nil, "fail")
			before, _, _ := e.o.counts()
			rec := e.get(target, http.Header{"Cache-Control": {"only-if-cached"}})
			if st := cacheLast(t, rec); rec.Code != http.StatusGatewayTimeout || st.Detail != "only-if-cached" || st.Hit {
				t.Fatalf("response %d status %+v, want 504 with detail only-if-cached", rec.Code, st)
			}
			if calls, _, _ := e.o.counts(); calls != before {
				t.Fatalf("origin calls %d, want %d", calls, before)
			}
		})
		t.Run("retry route answers a plain miss", func(t *testing.T) {
			t.Parallel()
			fake := clock.NewFake(cacheStart)
			var attempts atomic.Int32
			n := newMemNetwork()
			o := newFlightOrigin()
			o.setBegin(func(w http.ResponseWriter, r *http.Request) {
				if attempts.Add(1) == 1 {
					writeOrigin(w, http.StatusServiceUnavailable, nil, "fail")
					return
				}
				writeOrigin(w, http.StatusOK, staleOrigin("max-age=60"), "x")
			})
			be := n.serve(t, o)
			g := n.gateway(t, cacheGatewayDoc(be.URL, "", staleRoute("stale_if_error: 1h")+"    retry: {}\n"), withClock(fake))
			d := reDrive(t, fake, 250*time.Millisecond)
			rec := cacheDo(g, http.MethodGet, target, nil)
			d.stop()
			st := cacheLast(t, rec)
			if rec.Code != http.StatusOK || rec.Body.String() != "x" || st.Hit || st.Fwd != httpcache.FwdURIMiss || st.Detail != "" {
				t.Fatalf("response %d %q status %+v, want a plain 200 miss", rec.Code, rec.Body.String(), st)
			}
			if calls, _, _ := o.counts(); calls != 2 {
				t.Fatalf("origin calls %d, want the failed attempt and the retry", calls)
			}
		})
	})
}

func TestStaleIfErrorWaitersGateway(t *testing.T) {
	t.Parallel()
	const target = "http://example.com/sieq"

	t.Run("stale entry answers the leader and every waiter", func(t *testing.T) {
		t.Parallel()
		fake := clock.NewFake(cacheStart)
		e := newStaleEnv(t, fake, "", staleRoute("stale_if_error: 1h"))
		e.o.set(http.StatusOK, staleOrigin("max-age=60"), "old")
		e.fill(t, target)
		fake.Advance(61 * time.Second)
		e.o.set(http.StatusServiceUnavailable, http.Header{"Retry-After": {"5"}}, "fail")
		e.o.hold()
		chans := make([]<-chan *httptest.ResponseRecorder, 5)
		for i := range chans {
			chans[i] = e.async(target, nil)
		}
		testwait.Recv(t, "leader at origin", e.o.arrived, nil)
		awaitWaiters(t, e.g, 4)
		e.o.release()
		for _, ch := range chans {
			rec := coalesceRecv(t, ch)
			st := cacheLast(t, rec)
			if rec.Code != http.StatusOK || rec.Body.String() != "old" || st.FwdStatus != http.StatusServiceUnavailable || st.Detail != "stale-if-error" {
				t.Fatalf("response %d %q status %+v, want 200 old under stale-if-error 503", rec.Code, rec.Body.String(), st)
			}
		}
		if calls, _, _ := e.o.counts(); calls != 2 {
			t.Fatalf("origin calls %d, want fill and one forward", calls)
		}
		if v := staleRequests(t, e.g, "stale"); v != 5 {
			t.Fatalf("stale count %v, want 5", v)
		}
		if v := rsMetricOr0(t, e.g, "microgateway_cache_coalesced_total", "route", "r"); v != 0 {
			t.Fatalf("coalesced count %v, want 0", v)
		}
	})

	t.Run("no entry forwards every waiter on its own", func(t *testing.T) {
		t.Parallel()
		e := newStaleEnv(t, clock.NewFake(cacheStart), "", staleRoute("stale_if_error: 1h"))
		e.o.set(http.StatusInternalServerError, nil, "fail")
		e.o.hold()
		chans := make([]<-chan *httptest.ResponseRecorder, 4)
		for i := range chans {
			chans[i] = e.async(target, nil)
		}
		testwait.Recv(t, "leader at origin", e.o.arrived, nil)
		awaitWaiters(t, e.g, 3)
		e.o.release()
		for _, ch := range chans {
			staleExpectOrigin(t, coalesceRecv(t, ch), http.StatusInternalServerError)
		}
		if calls, _, _ := e.o.counts(); calls != 4 {
			t.Fatalf("origin calls %d, want the leader and three waiters", calls)
		}
	})

	t.Run("fresh entry revalidated by max-age=0 forwards every request", func(t *testing.T) {
		t.Parallel()
		fake := clock.NewFake(cacheStart)
		e := newStaleEnv(t, fake, "", staleRoute("stale_if_error: 1h"))
		e.o.set(http.StatusOK, staleOrigin("max-age=60"), "old")
		e.fill(t, target)
		fake.Advance(30 * time.Second)
		e.o.set(http.StatusInternalServerError, nil, "fail")
		e.o.hold()
		hdr := http.Header{"Cache-Control": {"max-age=0"}}
		chans := make([]<-chan *httptest.ResponseRecorder, 2)
		for i := range chans {
			chans[i] = e.async(target, hdr)
		}
		testwait.Recv(t, "leader at origin", e.o.arrived, nil)
		awaitWaiters(t, e.g, 1)
		e.o.release()
		for _, ch := range chans {
			staleExpectOrigin(t, coalesceRecv(t, ch), http.StatusInternalServerError)
		}
		if calls, _, _ := e.o.counts(); calls != 3 {
			t.Fatalf("origin calls %d, want fill, leader and waiter", calls)
		}
	})
}

func TestStaleSoftPurgeGateway(t *testing.T) {
	t.Parallel()
	const target = "http://example.com/sp"
	purge := func(t *testing.T, shared *teststore.SharedCache) {
		t.Helper()
		primary := httpcache.PrimaryKey(httptest.NewRequest(http.MethodGet, target, nil))
		if _, err := shared.Purge(context.Background(), store.Purge{Kind: store.PurgeKeys, Keys: []string{primary}, Soft: true, Retain: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("held revalidation is shared by twenty requests", func(t *testing.T) {
		t.Parallel()
		variants := []struct {
			name   string
			status int
			body   string
			want   string
		}{
			{name: "304", status: http.StatusNotModified, body: "", want: "old"},
			{name: "200", status: http.StatusOK, body: "new", want: "new"},
		}
		for _, v := range variants {
			t.Run(v.name, func(t *testing.T) {
				t.Parallel()
				fake := clock.NewFake(cacheStart)
				e, shared := staleSharedEnv(t, fake, staleRoute(""))
				e.o.set(http.StatusOK, staleOrigin("max-age=60"), "old")
				e.fill(t, target)
				purge(t, shared)
				e.o.set(v.status, staleOrigin("max-age=60"), v.body)
				e.o.hold()
				const n = 20
				chans := make([]<-chan *httptest.ResponseRecorder, n)
				for i := range chans {
					chans[i] = e.async(target, nil)
				}
				testwait.Recv(t, "leader at origin", e.o.arrived, nil)
				awaitWaiters(t, e.g, n-1)
				e.o.release()
				for _, ch := range chans {
					rec := coalesceRecv(t, ch)
					if rec.Code != http.StatusOK || rec.Body.String() != v.want {
						t.Fatalf("response %d %q, want 200 %q", rec.Code, rec.Body.String(), v.want)
					}
				}
				if _, cond, uncond := e.o.counts(); cond != 1 || uncond != 1 {
					t.Fatalf("origin requests conditional %d unconditional %d, want 1 and 1", cond, uncond)
				}
				if st := cacheLast(t, e.get(target, nil)); !st.Hit {
					t.Fatalf("later status %+v, want hit", st)
				}
			})
		}
	})

	t.Run("soft purged entry is never served stale", func(t *testing.T) {
		t.Parallel()
		newPurged := func(t *testing.T) *staleEnv {
			t.Helper()
			fake := clock.NewFake(cacheStart)
			e, shared := staleSharedEnv(t, fake, staleRoute("stale_while_revalidate: 1h\nstale_if_error: 1h"))
			e.o.set(http.StatusOK, staleOrigin("max-age=60"), "old")
			e.fill(t, target)
			fake.Advance(61 * time.Second)
			purge(t, shared)
			return e
		}
		t.Run("max-stale is not unbounded", func(t *testing.T) {
			t.Parallel()
			e := newPurged(t)
			e.o.set(http.StatusOK, staleOrigin("max-age=60"), "new")
			rec := e.get(target, http.Header{"Cache-Control": {"max-stale"}})
			if st := cacheLast(t, rec); st.Hit || rec.Body.String() != "new" {
				t.Fatalf("status %+v body %q, want a forwarded 200 new", st, rec.Body.String())
			}
			if _, cond, _ := e.o.counts(); cond != 1 {
				t.Fatalf("conditional origin requests %d, want 1", cond)
			}
		})
		t.Run("stale-while-revalidate window is not served", func(t *testing.T) {
			t.Parallel()
			e := newPurged(t)
			e.o.set(http.StatusNotModified, staleOrigin("max-age=60"), "")
			rec := e.get(target, nil)
			st := cacheLast(t, rec)
			if rec.Code != http.StatusOK || rec.Body.String() != "old" || st.Detail == "stale-while-revalidate" || st.FwdStatus != http.StatusNotModified {
				t.Fatalf("response %d %q status %+v, want a synchronous 304 revalidation", rec.Code, rec.Body.String(), st)
			}
			if got := e.o.lastHeader().Get("If-None-Match"); got != `"v1"` {
				t.Fatalf("origin If-None-Match %q, want \"v1\"", got)
			}
			staleExpectBackground(t, e.g, "")
		})
		t.Run("erroring origin is not served stale", func(t *testing.T) {
			t.Parallel()
			e := newPurged(t)
			e.o.set(http.StatusInternalServerError, nil, "fail")
			staleExpectOrigin(t, e.get(target, nil), http.StatusInternalServerError)
		})
	})
}

func TestStaleWindowInputsGateway(t *testing.T) {
	t.Parallel()
	const target = "http://example.com/win"
	long := "max-age=60, stale-while-revalidate=" + strings.Repeat("9", 40)
	cases := []struct {
		name   string
		route  string
		cc     string
		etag   bool
		probe  time.Duration
		served bool
		detail string
		ttl    int64
		put    time.Duration
	}{
		{name: "stale-while-revalidate=0 has no window", cc: "max-age=60, stale-while-revalidate=0", etag: true, probe: 61 * time.Second, put: staleLifetime},
		{name: "stale-while-revalidate=-1 is read as 0", cc: "max-age=60, stale-while-revalidate=-1", etag: true, probe: 61 * time.Second, put: staleLifetime},
		{name: "stale-while-revalidate=abc is read as 0", cc: "max-age=60, stale-while-revalidate=abc", etag: true, probe: 61 * time.Second, put: staleLifetime},
		{name: "40-digit window is clamped to 365d", cc: long, etag: true, probe: 61 * time.Second, served: true, detail: "stale-while-revalidate", ttl: -1, put: staleLifetime + httpcache.MaxWindow},
		{name: "window above 365d is clamped to 365d", cc: "max-age=60, stale-while-revalidate=31622400", etag: true, probe: 61 * time.Second, served: true, detail: "stale-while-revalidate", ttl: -1, put: staleLifetime + httpcache.MaxWindow},
		{name: "route window without directive", route: "stale_while_revalidate: 10s", cc: "max-age=60", etag: true, probe: 61 * time.Second, served: true, detail: "stale-while-revalidate", ttl: -1, put: staleLifetime + 10*time.Second},
		{name: "directive 0 overrides route window", route: "stale_while_revalidate: 10s", cc: "max-age=60, stale-while-revalidate=0", etag: true, probe: 61 * time.Second, put: staleLifetime},
		{name: "max-age clamped to max_ttl", route: "max_ttl: 1h", cc: "max-age=999999999", etag: true, probe: time.Hour + time.Nanosecond, put: time.Hour},
		{name: "max-age=0 is stale at once", cc: "max-age=0, stale-while-revalidate=10", etag: true, probe: 0, served: true, detail: "stale-while-revalidate", ttl: 0, put: 10 * time.Second},
		{name: "keep with a validator", route: "keep: 1h", cc: "max-age=60", etag: true, probe: 61 * time.Second, put: staleLifetime + time.Hour},
		{name: "keep without a validator adds nothing", route: "keep: 1h", cc: "max-age=60", probe: 61 * time.Second, put: staleLifetime},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			fake := clock.NewFake(cacheStart)
			e, shared := staleSharedEnv(t, fake, staleRoute(c.route))
			fill := http.Header{"Cache-Control": {c.cc}}
			if c.etag {
				fill.Set("Etag", `"v1"`)
			}
			e.o.set(http.StatusOK, fill, "old")
			e.fill(t, target)
			if puts := stalePutTTLs(shared); len(puts) != 1 || puts[0] != c.put {
				t.Fatalf("puts %v, want only the fill with storage ttl %v", puts, c.put)
			}
			if c.put > store.MaxTTL {
				t.Fatalf("storage ttl %v exceeds store.MaxTTL", c.put)
			}
			fake.Advance(c.probe)
			e.o.set(http.StatusInternalServerError, nil, "fail")
			rec := e.get(target, nil)
			if !c.served {
				staleExpectOrigin(t, rec, http.StatusInternalServerError)
				return
			}
			st := cacheLast(t, rec)
			age := strconv.FormatInt(int64(c.probe/time.Second), 10)
			if rec.Code != http.StatusOK || rec.Body.String() != "old" || st.Fwd != httpcache.FwdStale || st.Detail != c.detail || !st.HasTTL || st.TTL != c.ttl || rec.Header().Get("Age") != age {
				t.Fatalf("response %d %q status %+v Age %q, want 200 old stale with detail %s ttl %d Age %s", rec.Code, rec.Body.String(), st, rec.Header().Get("Age"), c.detail, c.ttl, age)
			}
			e.settle(t)
		})
	}

	t.Run("stale-while-revalidate and stale-if-error together", func(t *testing.T) {
		t.Parallel()
		fake := clock.NewFake(cacheStart)
		e, shared := staleSharedEnv(t, fake, staleRoute(""))
		e.o.set(http.StatusOK, staleOrigin("max-age=60, stale-while-revalidate=10, stale-if-error=20"), "old")
		e.fill(t, target)
		if puts := stalePutTTLs(shared); len(puts) != 1 || puts[0] != staleLifetime+20*time.Second {
			t.Fatalf("puts %v, want the fill with storage ttl 80s", puts)
		}
		fake.Advance(65 * time.Second)
		e.o.set(http.StatusInternalServerError, nil, "fail")
		st := cacheLast(t, e.get(target, nil))
		if st.Detail != "stale-while-revalidate" || st.TTL != -5 {
			t.Fatalf("status %+v at 65s, want stale-while-revalidate with ttl -5", st)
		}
		e.settle(t)
		fake.Advance(10 * time.Second)
		rec := e.get(target, nil)
		st = cacheLast(t, rec)
		if rec.Code != http.StatusOK || rec.Body.String() != "old" || st.Detail != "stale-if-error" || st.FwdStatus != http.StatusInternalServerError || st.TTL != -15 || rec.Header().Get("Age") != "75" {
			t.Fatalf("response %d %q status %+v at 75s, want stale-if-error 500 with ttl -15", rec.Code, rec.Body.String(), st)
		}
		fake.Advance(5 * time.Second)
		staleExpectOrigin(t, e.get(target, nil), http.StatusInternalServerError)
	})
}

// staleProbe is the response of one probe of a skew run: its Age header, its status, its code and its body.
type staleProbe struct {
	age  string
	st   httpcache.Status
	code int
	body string
}

// staleSkewProbe fills a shared store through gateway fillOn and probes it through gateway probeOn, after offset on
// the store clock and on both gateway clocks. The gateways run at one hour ahead and one hour behind the store.
func staleSkewProbe(t *testing.T, fillOn, probeOn int, offset time.Duration) staleProbe {
	t.Helper()
	const target = "http://example.com/skew"
	storeClock := clock.NewFake(cacheStart)
	shared := teststore.NewSharedCache(storeClock, 1<<20)
	t.Cleanup(func() { _ = shared.Close() })
	n := newMemNetwork()
	o := newFlightOrigin()
	o.setBegin(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != "" {
			writeOrigin(w, http.StatusNotModified, staleOrigin("max-age=60"), "")
			return
		}
		writeOrigin(w, http.StatusOK, staleOrigin("max-age=60"), "old")
	})
	be := n.serve(t, o)
	clocks := []*clock.Fake{clock.NewFake(cacheStart.Add(time.Hour)), clock.NewFake(cacheStart.Add(-time.Hour))}
	doc := cacheGatewayDoc(be.URL, staleSharedTop, cacheRouteYAML("r", "stale_while_revalidate: 30s", "stale_if_error: 1h", "keep: 2h"))
	gws := make([]*microgateway.Gateway, 2)
	for i := range gws {
		gws[i] = n.gateway(t, doc, withClock(clocks[i]), cacheSharedOption("shared", shared))
	}
	cacheDo(gws[fillOn], http.MethodGet, target, nil)
	storeClock.Advance(offset)
	for _, c := range clocks {
		c.Advance(offset)
	}
	rec := cacheDo(gws[probeOn], http.MethodGet, target, nil)
	eventually(t, "background refresh finished", func() bool {
		return inspect(gws[probeOn]).Background == 0
	}, nil)
	return staleProbe{age: rec.Header().Get("Age"), st: cacheLast(t, rec), code: rec.Code, body: rec.Body.String()}
}

func TestStaleSkewSharedStore(t *testing.T) {
	t.Parallel()
	offsets := []struct {
		name string
		at   time.Duration
	}{
		{name: "L-1ns", at: staleLifetime - time.Nanosecond},
		{name: "L", at: staleLifetime},
		{name: "L+1ns", at: staleLifetime + time.Nanosecond},
		{name: "L+swr-1ns", at: staleLifetime + 30*time.Second - time.Nanosecond},
		{name: "L+swr", at: staleLifetime + 30*time.Second},
		{name: "L+sie-1ns", at: staleLifetime + time.Hour - time.Nanosecond},
		{name: "L+sie", at: staleLifetime + time.Hour},
		{name: "S-1ns", at: staleSkewStorage - time.Nanosecond},
		{name: "S", at: staleSkewStorage},
	}
	for _, off := range offsets {
		t.Run(off.name, func(t *testing.T) {
			t.Parallel()
			var first *staleProbe
			for fillOn := range 2 {
				for probeOn := range 2 {
					p := staleSkewProbe(t, fillOn, probeOn, off.at)
					if p.st.Hit && p.st.FwdStatus == 0 {
						if want := strconv.FormatInt(int64(off.at/time.Second), 10); p.age != want {
							t.Fatalf("fill by %d probe by %d: Age %q, want %s", fillOn, probeOn, p.age, want)
						}
					}
					if first == nil {
						first = &p
						continue
					}
					if !reflect.DeepEqual(p.st, first.st) || p.age != first.age || p.code != first.code || p.body != first.body {
						t.Fatalf("fill by %d probe by %d: %d %q %+v Age %q, want %d %q %+v Age %q", fillOn, probeOn, p.code, p.body, p.st, p.age, first.code, first.body, first.st, first.age)
					}
				}
			}
		})
	}

	for _, variant := range []struct {
		name        string
		dateOnStore bool
		want        time.Duration
	}{
		{name: "Date from the refreshing gateway", want: 2 * time.Second},
		{name: "Date from the store", dateOnStore: true, want: time.Hour + 2*time.Second},
	} {
		t.Run("background initial age, "+variant.name, func(t *testing.T) {
			t.Parallel()
			storeClock := clock.NewFake(cacheStart)
			shared := teststore.NewSharedCache(storeClock, 1<<20)
			t.Cleanup(func() { _ = shared.Close() })
			n := newMemNetwork()
			o := newFlightOrigin()
			ahead := clock.NewFake(cacheStart.Add(time.Hour))
			o.setBegin(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("If-None-Match") == "" {
					writeOrigin(w, http.StatusOK, staleSWRHeader(), "old")
					return
				}
				date := ahead.Now()
				if variant.dateOnStore {
					date = storeClock.Now()
				}
				ahead.Advance(2 * time.Second)
				writeOrigin(w, http.StatusNotModified, http.Header{
					"Cache-Control": {"max-age=60, stale-while-revalidate=30"},
					"Etag":          {`"v1"`},
					"Date":          {date.Format(http.TimeFormat)},
				}, "")
			})
			be := n.serve(t, o)
			g := n.gateway(t, cacheGatewayDoc(be.URL, staleSharedTop, cacheRouteYAML("r")), withClock(ahead), cacheSharedOption("shared", shared))
			const target = "http://example.com/initial"
			cacheDo(g, http.MethodGet, target, nil)
			storeClock.Advance(61 * time.Second)
			ahead.Advance(61 * time.Second)
			if st := cacheLast(t, cacheDo(g, http.MethodGet, target, nil)); st.Detail != "stale-while-revalidate" {
				t.Fatalf("status %+v, want stale-while-revalidate", st)
			}
			eventually(t, "background refresh finished", func() bool {
				return inspect(g).Background == 0
			}, nil)
			if v := metricValue(t, metricsText(t, g), series("microgateway_cache_background_revalidations_total", "route", "r", "result", "revalidated")); v != 1 {
				t.Fatalf("revalidated = %v, want 1", v)
			}
			rec := cacheDo(g, http.MethodGet, target, http.Header{"Cache-Control": {"max-stale"}})
			if st := cacheLast(t, rec); !st.Hit {
				t.Fatalf("read-back status %+v, want the stored entry served", st)
			}
			if got, want := rec.Header().Get("Age"), strconv.FormatInt(int64(variant.want/time.Second), 10); got != want {
				t.Fatalf("Age of the stored entry %q, want %s (its initial age %v)", got, want, variant.want)
			}
		})
	}
}

func TestStaleMetricsGateway(t *testing.T) {
	t.Parallel()
	t.Run("counts of every outcome", func(t *testing.T) {
		t.Parallel()
		n := newMemNetwork()
		o := newFlightOrigin()
		fake := clock.NewFake(cacheStart)
		gate1 := make(chan struct{})
		gate2 := make(chan struct{})
		swr := staleSWRHeader()
		hot := coalesceHot()
		eh := http.Header{"Cache-Control": {"max-age=60, stale-while-revalidate=0"}, "Etag": {`"v1"`}}
		o.setBegin(func(w http.ResponseWriter, r *http.Request) {
			cond := r.Header.Get("If-None-Match") != ""
			switch r.URL.Path {
			case "/c1":
				select {
				case <-gate1:
				case <-r.Context().Done():
					return
				}
				writeOrigin(w, http.StatusOK, hot, "x")
			case "/c2":
				select {
				case <-gate2:
				case <-r.Context().Done():
					return
				}
				writeOrigin(w, http.StatusOK, hot, "x")
			case "/e":
				if cond {
					writeOrigin(w, http.StatusServiceUnavailable, http.Header{"Retry-After": {"5"}}, "fail")
					return
				}
				writeOrigin(w, http.StatusOK, eh, "old")
			default:
				if !cond {
					writeOrigin(w, http.StatusOK, swr, "old")
					return
				}
				switch r.URL.Path {
				case "/s1":
					writeOrigin(w, http.StatusNotModified, swr, "")
				case "/s2":
					writeOrigin(w, http.StatusOK, hot, "new")
				case "/s3":
					writeOrigin(w, http.StatusInternalServerError, nil, "err")
				default:
					panic(http.ErrAbortHandler)
				}
			}
		})
		be := n.serve(t, o)
		g := n.gateway(t, cacheGatewayDoc(be.URL, "cache:\n  coalescing:\n    timeout: 5s\n", cacheRouteYAML("r", "stale_while_revalidate: 30s", "stale_if_error: 1h")), withClock(fake))
		const base = "http://example.com/"

		lead1 := coalesceAsync(context.Background(), g, base+"c1", nil)
		testwait.Recv(t, "c1 leader at origin", o.arrived, nil)
		var waiters []<-chan *httptest.ResponseRecorder
		for range 3 {
			waiters = append(waiters, coalesceAsync(context.Background(), g, base+"c1", nil))
		}
		cctx, ccancel := context.WithCancel(context.Background())
		released := coalesceAsync(cctx, g, base+"c1", nil)
		awaitWaiters(t, g, 4)
		ccancel()
		coalesceRecv(t, released)
		awaitWaiters(t, g, 3)
		close(gate1)
		coalesceOK(t, coalesceRecv(t, lead1), "x")
		for _, ch := range waiters {
			coalesceOK(t, coalesceRecv(t, ch), "x")
		}

		lead2 := coalesceAsync(context.Background(), g, base+"c2", nil)
		testwait.Recv(t, "c2 leader at origin", o.arrived, nil)
		timed := coalesceAsync(context.Background(), g, base+"c2", nil)
		awaitWaiters(t, g, 1)
		fake.Advance(5 * time.Second)
		testwait.Recv(t, "timed-out waiter at origin", o.arrived, nil)
		awaitWaiters(t, g, 0)
		close(gate2)
		coalesceOK(t, coalesceRecv(t, lead2), "x")
		timedRec := coalesceRecv(t, timed)
		coalesceOK(t, timedRec, "x")
		if st := cacheLast(t, timedRec); st.Detail != "coalesce-timeout" {
			t.Fatalf("timed-out waiter status %+v, want detail coalesce-timeout", st)
		}

		for _, p := range []string{"s1", "s2", "s3", "s4"} {
			if st := cacheLast(t, cacheDo(g, http.MethodGet, base+p, nil)); st.Hit {
				t.Fatalf("fill of %s status %+v, want a miss", p, st)
			}
		}
		if st := cacheLast(t, cacheDo(g, http.MethodGet, base+"e", nil)); !st.Stored {
			t.Fatalf("fill of e status %+v, want stored", st)
		}
		fake.Advance(61 * time.Second)
		for _, p := range []string{"s1", "s2", "s3", "s4"} {
			if st := cacheLast(t, cacheDo(g, http.MethodGet, base+p, nil)); st.Detail != "stale-while-revalidate" {
				t.Fatalf("%s status %+v, want stale-while-revalidate", p, st)
			}
		}
		eventually(t, "background refreshes finished", func() bool {
			return inspect(g).Background == 0
		}, nil)
		if st := cacheLast(t, cacheDo(g, http.MethodGet, base+"e", nil)); st.Detail != "stale-if-error" || st.FwdStatus != http.StatusServiceUnavailable {
			t.Fatalf("e status %+v, want stale-if-error under 503", st)
		}

		m := metricsText(t, g)
		if v := metricValue(t, m, series("microgateway_cache_coalesced_total", "route", "r")); v != 3 {
			t.Fatalf("coalesced_total = %v, want 3", v)
		}
		for res, want := range map[string]float64{"revalidated": 1, "replaced": 1, "not_stored": 1, "error": 1, "skipped": 0} {
			if v := metricValue(t, m, series("microgateway_cache_background_revalidations_total", "route", "r", "result", res)); v != want {
				t.Fatalf("background %s = %v, want %v", res, v, want)
			}
		}
		if v := metricValue(t, m, series("microgateway_cache_requests_total", "route", "r", "result", "miss")); v != 12 {
			t.Fatalf("requests miss = %v, want 12", v)
		}
		if v := metricValue(t, m, series("microgateway_cache_requests_total", "route", "r", "result", "stale")); v != 5 {
			t.Fatalf("requests stale = %v, want 5", v)
		}
		if sum := staleSumSeries(m, "microgateway_cache_requests_total"); sum != 17 {
			t.Fatalf("requests_total sum = %v, want 17 client requests", sum)
		}
	})

	t.Run("gateway without cache exposes no cache series", func(t *testing.T) {
		t.Parallel()
		n := newMemNetwork()
		o := newFlightOrigin()
		o.set(http.StatusOK, coalesceHot(), "ok")
		be := n.serve(t, o)
		doc := cacheGatewayDoc(be.URL, "", "  - name: r\n    match:\n      path: /\n    upstream: up\n")
		fake := clock.NewFake(cacheStart)
		a := n.gateway(t, doc, withClock(fake))
		b := n.gateway(t, doc, withClock(fake))
		for _, g := range []*microgateway.Gateway{a, b} {
			for range 3 {
				cacheDo(g, http.MethodGet, "http://example.com/plain", nil)
			}
		}
		ma, mb := metricsText(t, a), metricsText(t, b)
		if ma != mb {
			t.Fatalf("metrics differ between two gateways without cache:\n%s\n---\n%s", ma, mb)
		}
		if strings.Contains(ma, "microgateway_cache_") {
			t.Fatal("gateway without cache exposes cache series")
		}
	})
}

// staleSumSeries returns the sum of the values of the series of name in body.
func staleSumSeries(body, name string) float64 {
	var sum float64
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, name+"{") {
			continue
		}
		v, _ := strconv.ParseFloat(line[strings.LastIndexByte(line, ' ')+1:], 64)
		sum += v
	}
	return sum
}

func TestStaleStoreFailureGateway(t *testing.T) {
	t.Parallel()
	const target = "http://example.com/sf"
	fake := clock.NewFake(cacheStart)
	e, shared := staleSharedEnv(t, fake, staleRoute(""))
	e.o.set(http.StatusOK, staleSWRHeader(), "old")
	e.fill(t, target)
	fake.Advance(61 * time.Second)

	expectBypass := func(t *testing.T, rec *httptest.ResponseRecorder) {
		t.Helper()
		st := cacheLast(t, rec)
		if rec.Code != http.StatusOK || rec.Body.String() != "old" || st.Hit || st.Fwd != httpcache.FwdBypass || st.Detail != "store-error" {
			t.Fatalf("response %d %q status %+v, want 200 old bypassed with store-error", rec.Code, rec.Body.String(), st)
		}
	}

	shared.SetMode(teststore.Fail, fmt.Errorf("backend down: %w", store.ErrUnavailable))
	e.o.hold()
	ch := e.async(target, nil)
	testwait.Recv(t, "bypassed request at origin", e.o.arrived, nil)
	if k := coalesceStats(e.g).Keys; k != 0 {
		t.Fatalf("coalesce keys %d while the bypassed request runs, want 0", k)
	}
	e.o.release()
	expectBypass(t, coalesceRecv(t, ch))
	staleExpectBackground(t, e.g, "")

	shared.SetMode(teststore.Hang, nil)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- e.get(target, nil) }()
	awaitDue(t, fake, 1, 100*time.Millisecond, "store timeout timer", nil)
	fake.Advance(100 * time.Millisecond)
	rec, _ := testwait.Recv(t, "request after store timeout", done, nil)
	expectBypass(t, rec)
	staleExpectBackground(t, e.g, "")

	shared.SetMode(teststore.Healthy, nil)
	shared.SetOpMode("put", teststore.Fail, fmt.Errorf("put down: %w", store.ErrUnavailable))
	e.o.set(http.StatusNotModified, staleSWRHeader(), "")
	rec = e.get(target, nil)
	if rec.Code != http.StatusOK || rec.Body.String() != "old" || cacheLast(t, rec).Detail != "stale-while-revalidate" {
		t.Fatalf("triggering response %d %q, want 200 old stale-while-revalidate", rec.Code, rec.Body.String())
	}
	e.settle(t)
	staleExpectBackground(t, e.g, "not_stored")
	if v := rsMetricOr0(t, e.g, "microgateway_cache_store_errors_total", "op", "put"); v != 1 {
		t.Fatalf("store put errors = %v, want 1", v)
	}
}
