package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/testwait"
)

// coalesceGateway returns a gateway on a fake clock at cacheStart, with the top-level block top, the routes and the
// origin o, and the fake clock.
func coalesceGateway(t *testing.T, n *memNetwork, o *flightOrigin, top, routes string, opts ...microgateway.Option) (*microgateway.Gateway, *clock.Fake) {
	t.Helper()
	fake := clock.NewFake(cacheStart)
	be := n.serve(t, o)
	all := append([]microgateway.Option{withClock(fake)}, opts...)
	return n.gateway(t, cacheGatewayDoc(be.URL, top, routes), all...), fake
}

// coalesceAsync sends a GET of target with the header hdr through g on its own goroutine, under ctx, and returns the
// channel that receives its recorder.
func coalesceAsync(ctx context.Context, g *microgateway.Gateway, target string, hdr http.Header) <-chan *httptest.ResponseRecorder {
	ch := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest(http.MethodGet, target, nil).WithContext(ctx)
		for k, v := range hdr {
			req.Header[k] = v
		}
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, req)
		ch <- rec
	}()
	return ch
}

// coalesceRecv waits for the recorder on ch.
func coalesceRecv(t *testing.T, ch <-chan *httptest.ResponseRecorder) *httptest.ResponseRecorder {
	t.Helper()
	rec, _ := testwait.Recv(t, "response", ch, nil)
	return rec
}

// awaitWaiters waits until n requests are parked on the calls of g.
func awaitWaiters(t *testing.T, g *microgateway.Gateway, n int) {
	t.Helper()
	eventually(t, fmt.Sprintf("%d coalesced waiters parked", n), func() bool {
		return coalesceStats(g).Waiters == n
	}, func() string {
		return fmt.Sprintf("coalesce waiters %d", coalesceStats(g).Waiters)
	})
}

// coalesceOK fails t unless rec is a 200 response with the body body.
func coalesceOK(t *testing.T, rec *httptest.ResponseRecorder, body string) {
	t.Helper()
	if rec.Code != http.StatusOK || rec.Body.String() != body {
		t.Fatalf("response %d %q, want 200 %q", rec.Code, rec.Body.String(), body)
	}
}

// coalesceHot is the origin header of the cacheable responses of the coalescing tests.
func coalesceHot() http.Header {
	return http.Header{"Cache-Control": {"max-age=60"}}
}

func TestCoalesceGateway(t *testing.T) {
	t.Run("hundred concurrent misses share one fetch", func(t *testing.T) {
		t.Parallel()
		n := newMemNetwork()
		o := newFlightOrigin()
		o.set(http.StatusOK, coalesceHot(), "x")
		o.hold()
		g, _ := coalesceGateway(t, n, o, "", cacheRouteYAML("r"))
		const target = "http://example.com/hot"
		recs := make(chan *httptest.ResponseRecorder, 100)
		for range 100 {
			go func() { recs <- cacheDo(g, http.MethodGet, target, nil) }()
		}
		testwait.Recv(t, "leader at origin", o.arrived, nil)
		awaitWaiters(t, g, 99)
		o.release()
		var first http.Header
		for range 100 {
			rec, _ := testwait.Recv(t, "shared response", recs, nil)
			coalesceOK(t, rec, "x")
			h := rec.Header().Clone()
			h.Del("Cache-Status")
			h.Del("X-Request-ID")
			if first == nil {
				first = h
			} else if !reflect.DeepEqual(first, h) {
				t.Fatalf("header %v, want %v", h, first)
			}
		}
		m := metricsText(t, g)
		if v := metricValue(t, m, series("microgateway_cache_coalesced_total", "route", "r")); v != 99 {
			t.Fatalf("coalesced_total = %v, want 99", v)
		}
		if v := metricValue(t, m, series("microgateway_cache_requests_total", "route", "r", "result", "miss")); v != 100 {
			t.Fatalf("requests miss = %v, want 100", v)
		}
		if calls, _, _ := o.counts(); calls != 1 {
			t.Fatalf("origin calls %d, want 1", calls)
		}
		if st := cacheLast(t, cacheDo(g, http.MethodGet, target, nil)); !st.Hit {
			t.Fatalf("next status %+v, want hit", st)
		}
	})

	t.Run("disabled forwards each", func(t *testing.T) {
		t.Parallel()
		n := newMemNetwork()
		o := newFlightOrigin()
		o.set(http.StatusOK, coalesceHot(), "x")
		o.hold()
		g, _ := coalesceGateway(t, n, o, "cache:\n  coalescing:\n    enabled: false\n", cacheRouteYAML("r"))
		const target = "http://example.com/off"
		recs := make(chan *httptest.ResponseRecorder, 100)
		for range 100 {
			go func() { recs <- cacheDo(g, http.MethodGet, target, nil) }()
		}
		for range 100 {
			testwait.Recv(t, "origin arrival", o.arrived, nil)
		}
		o.release()
		for range 100 {
			rec, _ := testwait.Recv(t, "response", recs, nil)
			coalesceOK(t, rec, "x")
		}
		if calls, _, _ := o.counts(); calls != 100 {
			t.Fatalf("origin calls %d, want 100", calls)
		}
	})

	t.Run("leader cancel", func(t *testing.T) {
		pre := snapGoroutines()
		n := newMemNetwork()
		o := newFlightOrigin()
		o.set(http.StatusOK, coalesceHot(), "x")
		g, _ := coalesceGateway(t, n, o, "", cacheRouteYAML("r"))
		const target = "http://example.com/lc"
		cacheDo(g, http.MethodGet, "http://example.com/lc-warm", nil)
		testwait.Recv(t, "warm-up at origin", o.arrived, nil)
		base := snapGoroutines()
		before, _, _ := o.counts()

		o.hold()
		lctx, lcancel := context.WithCancel(context.Background())
		defer lcancel()
		leader := coalesceAsync(lctx, g, target, nil)
		testwait.Recv(t, "leader at origin", o.arrived, nil)
		waiter := coalesceAsync(context.Background(), g, target, nil)
		awaitWaiters(t, g, 1)
		lcancel()
		o.release()
		coalesceOK(t, coalesceRecv(t, waiter), "x")
		coalesceRecv(t, leader)

		if v := metricValue(t, metricsText(t, g), series("microgateway_cache_coalesced_total", "route", "r")); v != 1 {
			t.Fatalf("coalesced_total = %v, want 1", v)
		}
		if calls, _, _ := o.counts(); calls != before+1 {
			t.Fatalf("origin calls %d, want %d", calls, before+1)
		}
		if st := cacheLast(t, cacheDo(g, http.MethodGet, target, nil)); !st.Hit {
			t.Fatalf("next status %+v, want hit", st)
		}
		awaitGoroutinesSettled(t, "goroutines after leader cancel", pre, base)
	})

	t.Run("waiter cancel", func(t *testing.T) {
		pre := snapGoroutines()
		n := newMemNetwork()
		o := newFlightOrigin()
		o.set(http.StatusOK, coalesceHot(), "x")
		g, _ := coalesceGateway(t, n, o, "", cacheRouteYAML("r"))
		const target = "http://example.com/wc"
		cacheDo(g, http.MethodGet, "http://example.com/wc-warm", nil)
		testwait.Recv(t, "warm-up at origin", o.arrived, nil)
		base := snapGoroutines()

		o.hold()
		leader := coalesceAsync(context.Background(), g, target, nil)
		testwait.Recv(t, "leader at origin", o.arrived, nil)
		wctx, wcancel := context.WithCancel(context.Background())
		waiter := coalesceAsync(wctx, g, target, nil)
		awaitWaiters(t, g, 1)
		wcancel()
		released := coalesceRecv(t, waiter)
		if released.Body.Len() != 0 || len(released.Header().Values("Cache-Status")) != 0 {
			t.Fatalf("cancelled waiter wrote body %q and Cache-Status %q, want neither", released.Body.String(), released.Header().Values("Cache-Status"))
		}
		awaitWaiters(t, g, 0)
		o.release()
		coalesceOK(t, coalesceRecv(t, leader), "x")

		m := metricsText(t, g)
		if v := rsMetricOr0(t, g, "microgateway_cache_coalesced_total", "route", "r"); v != 0 {
			t.Fatalf("coalesced_total = %v, want 0", v)
		}
		if v := metricValue(t, m, series("microgateway_cache_requests_total", "route", "r", "result", "miss")); v != 3 {
			t.Fatalf("requests miss = %v, want 3 (warm-up, leader and cancelled waiter)", v)
		}
		if calls, _, _ := o.counts(); calls != 2 {
			t.Fatalf("origin calls %d, want 2", calls)
		}
		awaitGoroutinesSettled(t, "goroutines after waiter cancel", pre, base)
	})

	t.Run("all clients gone", func(t *testing.T) {
		pre := snapGoroutines()
		n := newMemNetwork()
		o := newFlightOrigin()
		o.set(http.StatusOK, coalesceHot(), "x")
		g, _ := coalesceGateway(t, n, o, "", cacheRouteYAML("r"))
		const target = "http://example.com/gone"
		cacheDo(g, http.MethodGet, "http://example.com/gone-warm", nil)
		testwait.Recv(t, "warm-up at origin", o.arrived, nil)
		base := snapGoroutines()
		before, _, _ := o.counts()

		o.hold()
		lctx, lcancel := context.WithCancel(context.Background())
		wctx, wcancel := context.WithCancel(context.Background())
		leader := coalesceAsync(lctx, g, target, nil)
		testwait.Recv(t, "leader at origin", o.arrived, nil)
		waiter := coalesceAsync(wctx, g, target, nil)
		awaitWaiters(t, g, 1)
		lcancel()
		wcancel()
		coalesceRecv(t, waiter)
		awaitWaiters(t, g, 0)
		o.release()
		coalesceRecv(t, leader)

		if calls, _, _ := o.counts(); calls != before+1 {
			t.Fatalf("origin calls %d, want %d", calls, before+1)
		}
		if st := cacheLast(t, cacheDo(g, http.MethodGet, target, nil)); !st.Hit {
			t.Fatalf("next status %+v, want hit", st)
		}
		awaitGoroutinesSettled(t, "goroutines after all clients left", pre, base)
	})

	t.Run("not shareable releases waiters", func(t *testing.T) {
		t.Parallel()
		abort := func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "max-age=60")
			w.Header().Set("Content-Length", "100")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "partial")
			_ = http.NewResponseController(w).Flush()
			conn, _, err := http.NewResponseController(w).Hijack()
			if err != nil {
				panic(err)
			}
			_ = conn.Close()
		}
		cases := []struct {
			name   string
			top    string
			header http.Header
			body   string
			begin  http.HandlerFunc
		}{
			{name: "vary star", header: http.Header{"Cache-Control": {"max-age=60"}, "Vary": {"*"}}, body: "x"},
			{name: "no-store", header: http.Header{"Cache-Control": {"no-store"}}, body: "x"},
			{name: "body above max_body_bytes", top: "cache:\n  max_body_bytes: 16\n", header: coalesceHot(), body: strings.Repeat("x", 32)},
			{name: "origin aborts mid-body", header: coalesceHot(), begin: abort},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				t.Parallel()
				n := newMemNetwork()
				o := newFlightOrigin()
				o.set(http.StatusOK, c.header, c.body)
				o.setBegin(c.begin)
				o.hold()
				g, _ := coalesceGateway(t, n, o, c.top, cacheRouteYAML("r"))
				const target = "http://example.com/ns"
				leader := coalesceAsync(context.Background(), g, target, nil)
				testwait.Recv(t, "leader at origin", o.arrived, nil)
				w1 := coalesceAsync(context.Background(), g, target, nil)
				w2 := coalesceAsync(context.Background(), g, target, nil)
				awaitWaiters(t, g, 2)
				o.release()
				for _, ch := range []<-chan *httptest.ResponseRecorder{leader, w1, w2} {
					if rec := coalesceRecv(t, ch); rec.Code != http.StatusOK {
						t.Fatalf("response code %d, want 200", rec.Code)
					}
				}
				if calls, _, _ := o.counts(); calls != 3 {
					t.Fatalf("origin calls %d, want the leader and two forwards", calls)
				}
				if v := rsMetricOr0(t, g, "microgateway_cache_coalesced_total", "route", "r"); v != 0 {
					t.Fatalf("coalesced_total = %v, want 0", v)
				}
			})
		}
	})

	t.Run("vary mismatch", func(t *testing.T) {
		t.Parallel()
		n := newMemNetwork()
		o := newFlightOrigin()
		o.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Vary": {"Accept-Language"}}, "x")
		o.hold()
		g, _ := coalesceGateway(t, n, o, "", cacheRouteYAML("r"))
		const target = "http://example.com/vm"
		leader := coalesceAsync(context.Background(), g, target, http.Header{"Accept-Language": {"en"}})
		testwait.Recv(t, "leader at origin", o.arrived, nil)
		waiter := coalesceAsync(context.Background(), g, target, http.Header{"Accept-Language": {"fr"}})
		awaitWaiters(t, g, 1)
		o.release()
		coalesceOK(t, coalesceRecv(t, leader), "x")
		coalesceOK(t, coalesceRecv(t, waiter), "x")
		if calls, _, _ := o.counts(); calls != 2 {
			t.Fatalf("origin calls %d, want the leader and the waiter on its own", calls)
		}
		if v := rsMetricOr0(t, g, "microgateway_cache_coalesced_total", "route", "r"); v != 0 {
			t.Fatalf("coalesced_total = %v, want 0", v)
		}
	})

	t.Run("bounds", func(t *testing.T) {
		t.Parallel()
		t.Run("max_waiters", func(t *testing.T) {
			t.Parallel()
			n := newMemNetwork()
			o := newFlightOrigin()
			o.set(http.StatusOK, coalesceHot(), "x")
			o.hold()
			g, _ := coalesceGateway(t, n, o, "cache:\n  coalescing:\n    max_waiters: 1\n", cacheRouteYAML("r"))
			const target = "http://example.com/mw"
			leader := coalesceAsync(context.Background(), g, target, nil)
			testwait.Recv(t, "leader at origin", o.arrived, nil)
			w1 := coalesceAsync(context.Background(), g, target, nil)
			awaitWaiters(t, g, 1)
			w2 := coalesceAsync(context.Background(), g, target, nil)
			testwait.Recv(t, "second waiter forwarded to origin", o.arrived, nil)
			o.release()
			for _, ch := range []<-chan *httptest.ResponseRecorder{leader, w1, w2} {
				coalesceOK(t, coalesceRecv(t, ch), "x")
			}
			if calls, _, _ := o.counts(); calls != 2 {
				t.Fatalf("origin calls %d, want 2", calls)
			}
			if v := metricValue(t, metricsText(t, g), series("microgateway_cache_coalesced_total", "route", "r")); v != 1 {
				t.Fatalf("coalesced_total = %v, want 1", v)
			}
		})
		t.Run("max_keys", func(t *testing.T) {
			t.Parallel()
			n := newMemNetwork()
			o := newFlightOrigin()
			o.set(http.StatusOK, coalesceHot(), "x")
			o.hold()
			g, _ := coalesceGateway(t, n, o, "cache:\n  coalescing:\n    max_keys: 1\n", cacheRouteYAML("r"))
			leader := coalesceAsync(context.Background(), g, "http://example.com/mk-a", nil)
			testwait.Recv(t, "leader at origin", o.arrived, nil)
			other := coalesceAsync(context.Background(), g, "http://example.com/mk-b", nil)
			testwait.Recv(t, "second key forwarded to origin", o.arrived, nil)
			o.release()
			coalesceOK(t, coalesceRecv(t, leader), "x")
			coalesceOK(t, coalesceRecv(t, other), "x")
			if calls, _, _ := o.counts(); calls != 2 {
				t.Fatalf("origin calls %d, want 2", calls)
			}
			if v := rsMetricOr0(t, g, "microgateway_cache_coalesced_total", "route", "r"); v != 0 {
				t.Fatalf("coalesced_total = %v, want 0", v)
			}
		})
	})

	t.Run("waiter timeout boundary", func(t *testing.T) {
		t.Parallel()
		n := newMemNetwork()
		o := newFlightOrigin()
		o.set(http.StatusOK, coalesceHot(), "x")
		o.hold()
		g, fake := coalesceGateway(t, n, o, "cache:\n  coalescing:\n    timeout: 5s\n", cacheRouteYAML("r"))
		const target = "http://example.com/tb"
		leader := coalesceAsync(context.Background(), g, target, nil)
		testwait.Recv(t, "leader at origin", o.arrived, nil)
		waiter := coalesceAsync(context.Background(), g, target, nil)
		awaitWaiters(t, g, 1)
		fake.Advance(5*time.Second - time.Nanosecond)
		if w := coalesceStats(g).Waiters; w != 1 {
			t.Fatalf("waiters at 5s-1ns = %d, want 1", w)
		}
		fake.Advance(time.Nanosecond)
		testwait.Recv(t, "timed-out waiter forwarded to origin", o.arrived, nil)
		awaitWaiters(t, g, 0)
		o.release()
		rec := coalesceRecv(t, waiter)
		coalesceOK(t, rec, "x")
		if st := cacheLast(t, rec); st.Detail != "coalesce-timeout" {
			t.Fatalf("timed-out waiter status %+v, want detail coalesce-timeout", st)
		}
		coalesceOK(t, coalesceRecv(t, leader), "x")
		if calls, _, _ := o.counts(); calls != 2 {
			t.Fatalf("origin calls %d, want the leader and the timed-out waiter", calls)
		}
	})

	t.Run("panic in the fetch", func(t *testing.T) {
		pre := snapGoroutines()
		n := newMemNetwork()
		o := newFlightOrigin()
		o.set(http.StatusOK, coalesceHot(), "x")
		b := &boomOnce{arrived: make(chan struct{}, 1), gate: make(chan struct{})}
		routes := cacheRouteYAML("r") + "    middlewares: [boom]\n    pipeline: [cache, boom]\n"
		top := "middlewares:\n  - name: boom\n    type: boom\n"
		g, _ := coalesceGateway(t, n, o, top, routes, microgateway.WithMiddleware("boom", func(microgateway.MiddlewareParams) (microgateway.Middleware, error) {
			return b.middleware, nil
		}))
		cacheDo(g, http.MethodGet, "http://example.com/pn-warm", nil)
		base := snapGoroutines()
		const target = "http://example.com/pn"
		leader := coalesceAsync(context.Background(), g, target, nil)
		testwait.Recv(t, "leader reached the fetch", b.arrived, nil)
		w1 := coalesceAsync(context.Background(), g, target, nil)
		w2 := coalesceAsync(context.Background(), g, target, nil)
		awaitWaiters(t, g, 2)
		close(b.gate)
		for _, ch := range []<-chan *httptest.ResponseRecorder{leader, w1, w2} {
			rec := coalesceRecv(t, ch)
			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("response code %d, want 500", rec.Code)
			}
			if strings.Contains(rec.Body.String(), "secret panic text") {
				t.Fatalf("response body %q exposes the panic", rec.Body.String())
			}
			if code := decodeErr(t, rec.Result()).Code; code != httperr.CodeInternal {
				t.Fatalf("error code %q, want %q", code, httperr.CodeInternal)
			}
		}
		next := cacheDo(g, http.MethodGet, target, nil)
		if st := cacheLast(t, next); !st.Stored {
			t.Fatalf("next status %+v, want stored", st)
		}
		coalesceOK(t, next, "x")
		if v := metricValue(t, metricsText(t, g), series("microgateway_cache_requests_total", "route", "r", "result", "miss")); v != 5 {
			t.Fatalf("requests miss = %v, want 5 (warm-up, three panicking requests and the next fill)", v)
		}
		awaitGoroutinesSettled(t, "goroutines after a fetch panic", pre, base)
	})
}

// boomOnce is a middleware that panics on its first request with a message that must not reach a client. It reports
// the first request on arrived and waits for gate before panicking.
type boomOnce struct {
	calls   atomic.Int32
	arrived chan struct{}
	gate    chan struct{}
}

func (b *boomOnce) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/pn" && b.calls.Add(1) == 1 {
			b.arrived <- struct{}{}
			<-b.gate
			panic("secret panic text")
		}
		next.ServeHTTP(w, r)
	})
}

func TestCoalesceReloadGateway(t *testing.T) {
	otherRoute := "  - name: other\n    match:\n      path: /other\n    upstream: up\n"

	t.Run("reloads keep the parked waiters on the call in flight", func(t *testing.T) {
		t.Parallel()
		n := newMemNetwork()
		o := newFlightOrigin()
		o.set(http.StatusOK, coalesceHot(), "x")
		be := n.serve(t, o)
		doc := func(top, routes string) []byte {
			return []byte(cacheGatewayDoc(be.URL, top, routes))
		}
		g := n.gateway(t, string(doc("", cacheRouteYAML("r"))), withClock(clock.NewFake(cacheStart)))
		o.hold()
		const target = "http://example.com/rl"
		leader := coalesceAsync(context.Background(), g, target, nil)
		testwait.Recv(t, "leader at origin", o.arrived, nil)
		w1 := coalesceAsync(context.Background(), g, target, nil)
		w2 := coalesceAsync(context.Background(), g, target, nil)
		awaitWaiters(t, g, 2)
		if err := g.ReloadYAML(doc("", cacheRouteYAML("r")+otherRoute)); err != nil {
			t.Fatal(err)
		}
		if err := g.ReloadYAML(doc("", cacheRouteYAML("r", "default_ttl: 30s")+otherRoute)); err != nil {
			t.Fatal(err)
		}
		w3 := coalesceAsync(context.Background(), g, target, nil)
		awaitWaiters(t, g, 3)
		o.release()
		for _, ch := range []<-chan *httptest.ResponseRecorder{leader, w1, w2, w3} {
			coalesceOK(t, coalesceRecv(t, ch), "x")
		}
		if calls, _, _ := o.counts(); calls != 1 {
			t.Fatalf("origin calls %d, want 1", calls)
		}
		if v := metricValue(t, metricsText(t, g), series("microgateway_cache_coalesced_total", "route", "r")); v != 3 {
			t.Fatalf("coalesced_total = %v, want 3", v)
		}
	})

	t.Run("bound reloads drain a held fetch to the base state", func(t *testing.T) {
		pre := snapGoroutines()
		n := newMemNetwork()
		o := newFlightOrigin()
		o.set(http.StatusOK, coalesceHot(), "x")
		be := n.serve(t, o)
		doc := func(top string) []byte {
			return []byte(cacheGatewayDoc(be.URL, top, cacheRouteYAML("r")))
		}
		g := n.gateway(t, string(doc("")), withClock(clock.NewFake(cacheStart)))
		cacheDo(g, http.MethodGet, "http://example.com/bd-warm", nil)
		testwait.Recv(t, "warm-up at origin", o.arrived, nil)
		base := snapGoroutines()
		baseEntries := inspect(g).StateEntries

		o.hold()
		const target = "http://example.com/bd"
		leader := coalesceAsync(context.Background(), g, target, nil)
		testwait.Recv(t, "leader at origin", o.arrived, nil)
		waiter := coalesceAsync(context.Background(), g, target, nil)
		awaitWaiters(t, g, 1)
		if err := g.ReloadYAML(doc("cache:\n  coalescing:\n    max_waiters: 2\n")); err != nil {
			t.Fatal(err)
		}
		if err := g.ReloadYAML(doc("cache:\n  coalescing:\n    max_waiters: 2\n  max_background: 2\n")); err != nil {
			t.Fatal(err)
		}
		o.release()
		coalesceOK(t, coalesceRecv(t, leader), "x")
		coalesceOK(t, coalesceRecv(t, waiter), "x")
		eventually(t, "state entries back to the base count", func() bool {
			return inspect(g).StateEntries == baseEntries
		}, func() string {
			return fmt.Sprintf("StateEntries=%d, want %d", inspect(g).StateEntries, baseEntries)
		})
		awaitGoroutinesSettled(t, "goroutines after bound reloads", pre, base)
	})

	t.Run("bound reloads drain a held refresh to the base state", func(t *testing.T) {
		pre := snapGoroutines()
		n := newMemNetwork()
		o := newFlightOrigin()
		swr := http.Header{"Cache-Control": {"max-age=60, stale-while-revalidate=30"}, "Etag": {`"v1"`}}
		o.set(http.StatusOK, swr, "old")
		be := n.serve(t, o)
		fake := clock.NewFake(cacheStart)
		doc := func(top string) []byte {
			return []byte(cacheGatewayDoc(be.URL, top, cacheRouteYAML("r")))
		}
		g := n.gateway(t, string(doc("")), withClock(fake))
		const target = "http://example.com/brf"
		cacheDo(g, http.MethodGet, target, nil)
		testwait.Recv(t, "fill at origin", o.arrived, nil)
		base := snapGoroutines()
		baseEntries := inspect(g).StateEntries

		fake.Advance(61 * time.Second)
		o.set(http.StatusNotModified, swr, "")
		o.hold()
		rec := cacheDo(g, http.MethodGet, target, nil)
		if st := cacheLast(t, rec); st.Detail != "stale-while-revalidate" {
			t.Fatalf("status %+v, want stale-while-revalidate", st)
		}
		testwait.Recv(t, "refresh at origin", o.arrived, nil)
		if err := g.ReloadYAML(doc("cache:\n  coalescing:\n    max_waiters: 2\n")); err != nil {
			t.Fatal(err)
		}
		if err := g.ReloadYAML(doc("cache:\n  coalescing:\n    max_waiters: 2\n  max_background: 2\n")); err != nil {
			t.Fatal(err)
		}
		o.release()
		eventually(t, "old generation released after its refresh", func() bool {
			return inspect(g).LiveGenerations == 1
		}, func() string {
			return fmt.Sprintf("LiveGenerations=%d", inspect(g).LiveGenerations)
		})
		if v := metricValue(t, metricsText(t, g), series("microgateway_cache_background_revalidations_total", "route", "r", "result", "revalidated")); v != 1 {
			t.Fatalf("revalidated = %v, want 1", v)
		}
		eventually(t, "state entries back to the base count", func() bool {
			return inspect(g).StateEntries == baseEntries
		}, func() string {
			return fmt.Sprintf("StateEntries=%d, want %d", inspect(g).StateEntries, baseEntries)
		})
		awaitGoroutinesSettled(t, "goroutines after refresh bound reloads", pre, base)
	})
}
