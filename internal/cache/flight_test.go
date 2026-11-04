package cache

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/coalesce"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/internal/testwait"
)

// served is the outcome of a request started by serveAsync: the recorder, or the value of a panic that escaped it.
type served struct {
	rec      *httptest.ResponseRecorder
	panicked any
}

// serveAsync serves req through the rig in a goroutine. The channel receives the outcome once the request returned.
func serveAsync(rg *rig, req *http.Request) <-chan served {
	ch := make(chan served, 1)
	go func() {
		var s served
		func() {
			defer func() { s.panicked = recover() }()
			s.rec = do(rg.h, req)
		}()
		ch <- s
	}()
	return ch
}

func awaitServed(t *testing.T, what string, ch <-chan served) served {
	t.Helper()
	s, _ := testwait.Recv(t, what, ch, nil)
	return s
}

// newGet returns a GET request for path with the header hdr, whose context is ctx.
func newGet(ctx context.Context, path string, hdr http.Header) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "http://example.com"+path, nil).WithContext(ctx)
	for k, v := range hdr {
		req.Header[k] = slices.Clone(v)
	}
	return req
}

// awaitWaiters waits until n requests are parked in the coalescing group of rg.
func awaitWaiters(t *testing.T, rg *rig, n int) {
	t.Helper()
	testwait.Until(t, fmt.Sprintf("%d waiters parked", n), func() bool {
		return rg.group.Stats().Waiters == n
	}, func() string { return fmt.Sprint(rg.group.Stats()) })
}

// writeFull answers a request with the header h and the body b as a complete upstream exchange.
func writeFull(w http.ResponseWriter, r *http.Request, h http.Header, b string) {
	ex := reqctx.ExchangeFrom(r.Context())
	ex.Begin()
	for k, v := range h {
		w.Header()[k] = slices.Clone(v)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(b))
	ex.Finish()
}

func TestCoalesceShare(t *testing.T) {
	rg := newRig(t, rigOpts{})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"s"`}, "Content-Length": {"6"}}, "shared")
	arrived, release := rg.orig.hold()
	t.Cleanup(release)
	recs := make([]*httptest.ResponseRecorder, 100)
	done := parallel(100, func(i int) { recs[i] = rg.get("/c", nil) })
	testwait.Recv(t, "leader at origin", arrived, nil)
	awaitWaiters(t, rg, 99)
	release()
	testwait.Done(t, "all requests returned", done, nil)
	if n := rg.orig.count(); n != 1 {
		t.Fatalf("origin calls %d, want 1", n)
	}
	want := recs[0].Header().Clone()
	want.Del("Cache-Status")
	var leaders, waiters int
	for i, rec := range recs {
		if rec.Code != http.StatusOK || rec.Body.String() != "shared" {
			t.Fatalf("request %d answered %d %q", i, rec.Code, rec.Body.String())
		}
		h := rec.Header().Clone()
		h.Del("Cache-Status")
		if !reflect.DeepEqual(h, want) {
			t.Fatalf("request %d header %v, want %v", i, h, want)
		}
		st := lastStatus(t, rec)
		switch {
		case st.Stored && !st.Collapsed:
			leaders++
		case st.Collapsed && !st.Stored:
			waiters++
		default:
			t.Fatalf("request %d last member %+v", i, st)
		}
	}
	if leaders != 1 || waiters != 99 {
		t.Fatalf("leaders %d waiters %d, want 1 and 99", leaders, waiters)
	}
	if n := rg.coalesced(); n != 99 {
		t.Fatalf("coalesced %v, want 99", n)
	}
	if n := rg.requests(resultMiss); n != 100 {
		t.Fatalf("miss requests %v, want 100", n)
	}
	if !hit(t, rg.get("/c", nil)) {
		t.Fatal("next GET was not a hit")
	}
	if st := rg.group.Stats(); st != (coalesce.Stats{}) {
		t.Fatalf("group stats %+v, want empty", st)
	}
}

func TestCoalesceDisabled(t *testing.T) {
	rg := newRig(t, rigOpts{global: "coalescing:\n  enabled: false"})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
	_, release := rg.orig.hold()
	t.Cleanup(release)
	done := parallel(10, func(int) { rg.get("/d", nil) })
	testwait.Until(t, "10 origin calls held", func() bool { return rg.orig.count() == 10 }, nil)
	release()
	testwait.Done(t, "requests returned", done, nil)
	if n := rg.orig.count(); n != 10 {
		t.Fatalf("origin calls %d, want 10", n)
	}
}

func TestCoalesceLeaderDetached(t *testing.T) {
	t.Run("leader cancelled", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"4"}}, "full")
		arrived, release := rg.orig.hold()
		t.Cleanup(release)
		leaderCtx, cancelLeader := context.WithCancel(context.Background())
		defer cancelLeader()
		leader := serveAsync(rg, newGet(leaderCtx, "/d", nil))
		testwait.Recv(t, "leader at origin", arrived, nil)
		waiters := make([]*httptest.ResponseRecorder, 3)
		done := parallel(3, func(i int) { waiters[i] = rg.get("/d", nil) })
		awaitWaiters(t, rg, 3)
		cancelLeader()
		release()
		awaitServed(t, "leader returned", leader)
		testwait.Done(t, "waiters returned", done, nil)
		for i, rec := range waiters {
			if rec.Code != http.StatusOK || rec.Body.String() != "full" {
				t.Fatalf("waiter %d answered %d %q", i, rec.Code, rec.Body.String())
			}
		}
		if n := len(rg.spy.callsOf(opPut)); n != 1 {
			t.Fatalf("put calls %d, want 1", n)
		}
		if !hit(t, rg.get("/d", nil)) {
			t.Fatal("entry was not served")
		}
	})
	t.Run("all clients gone", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"4"}}, "full")
		rg.get("/warm", nil)
		puts := len(rg.spy.callsOf(opPut))
		base := runtime.NumGoroutine()
		arrived, release := rg.orig.hold()
		t.Cleanup(release)
		leaderCtx, cancelLeader := context.WithCancel(context.Background())
		defer cancelLeader()
		leader := serveAsync(rg, newGet(leaderCtx, "/d", nil))
		testwait.Recv(t, "leader at origin", arrived, nil)
		waiterCtx, cancelWaiters := context.WithCancel(context.Background())
		defer cancelWaiters()
		done := parallel(2, func(int) { do(rg.h, newGet(waiterCtx, "/d", nil)) })
		awaitWaiters(t, rg, 2)
		cancelLeader()
		cancelWaiters()
		testwait.Done(t, "cancelled waiters returned", done, nil)
		release()
		awaitServed(t, "leader returned", leader)
		testwait.Until(t, "one put", func() bool { return len(rg.spy.callsOf(opPut)) == puts+1 }, nil)
		if !hit(t, rg.get("/d", nil)) {
			t.Fatal("entry was not served")
		}
		testwait.Until(t, "goroutines settled", func() bool { return runtime.NumGoroutine() <= base }, nil)
	})
}

func TestCoalesceWaiterCancel(t *testing.T) {
	rg := newRig(t, rigOpts{})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"4"}}, "full")
	arrived, release := rg.orig.hold()
	t.Cleanup(release)
	leader := serveAsync(rg, newGet(context.Background(), "/w", nil))
	testwait.Recv(t, "leader at origin", arrived, nil)
	cancelled, cancelWaiter := context.WithCancel(context.Background())
	defer cancelWaiter()
	gone := serveAsync(rg, newGet(cancelled, "/w", nil))
	other := serveAsync(rg, newGet(context.Background(), "/w", nil))
	awaitWaiters(t, rg, 2)
	cancelWaiter()
	testwait.Until(t, "cancelled waiter left", func() bool { return rg.group.Stats().Waiters == 1 }, nil)
	release()
	g := awaitServed(t, "cancelled waiter returned", gone).rec
	if g.Code != http.StatusOK || g.Body.Len() != 0 || g.Header().Get("Cache-Status") != "" {
		t.Fatalf("cancelled waiter wrote %d %q, header %v", g.Code, g.Body.String(), g.Header())
	}
	for _, ch := range []<-chan served{other, leader} {
		if rec := awaitServed(t, "request returned", ch).rec; rec.Code != http.StatusOK || rec.Body.String() != "full" {
			t.Fatalf("request answered %d %q", rec.Code, rec.Body.String())
		}
	}
	if n := rg.coalesced(); n != 1 {
		t.Fatalf("coalesced %v, want 1", n)
	}
}

func TestCoalesceRelease(t *testing.T) {
	const full = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEF"
	gated := func(h http.Header, b string) func(w http.ResponseWriter, r *http.Request, gate, finish <-chan struct{}) {
		return func(w http.ResponseWriter, r *http.Request, gate, _ <-chan struct{}) {
			<-gate
			writeFull(w, r, h, b)
		}
	}
	chunked := func(w http.ResponseWriter, r *http.Request, gate, finish <-chan struct{}) {
		ex := reqctx.ExchangeFrom(r.Context())
		ex.Begin()
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(full[:8]))
		http.NewResponseController(w).Flush()
		<-gate
		_, _ = w.Write([]byte(full[8:]))
		<-finish
		ex.Finish()
	}
	aborted := func(w http.ResponseWriter, r *http.Request, gate, _ <-chan struct{}) {
		ex := reqctx.ExchangeFrom(r.Context())
		ex.Begin()
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(full[:4]))
		<-gate
	}
	cases := []struct {
		name   string
		global string
		header http.Header
		mid    bool
		lead   func(w http.ResponseWriter, r *http.Request, gate, finish <-chan struct{})
	}{
		{
			name:   "no-store",
			header: http.Header{"Cache-Control": {"no-store"}},
			lead:   gated(http.Header{"Cache-Control": {"no-store"}}, full),
		},
		{
			name:   "set-cookie",
			header: http.Header{"Cache-Control": {"max-age=60"}, "Set-Cookie": {"a=b"}},
			lead:   gated(http.Header{"Cache-Control": {"max-age=60"}, "Set-Cookie": {"a=b"}}, full),
		},
		{
			name:   "trailer",
			header: http.Header{"Cache-Control": {"max-age=60"}, "Trailer": {"X"}},
			lead:   gated(http.Header{"Cache-Control": {"max-age=60"}, "Trailer": {"X"}}, full),
		},
		{
			name:   "declared length over max_body_bytes",
			global: "max_body_bytes: 16",
			header: http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"40"}},
			lead:   gated(http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"40"}}, full),
		},
		{
			name:   "chunked body over max_body_bytes",
			global: "max_body_bytes: 16",
			header: http.Header{"Cache-Control": {"max-age=60"}},
			mid:    true,
			lead:   chunked,
		},
		{
			name:   "origin aborts mid-body",
			header: http.Header{"Cache-Control": {"max-age=60"}},
			lead:   aborted,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t, rigOpts{global: tc.global})
			arrived := make(chan struct{}, 1)
			gate := make(chan struct{})
			finish := make(chan struct{})
			waiterHdr := tc.header.Clone()
			waiterHdr.Set("Cache-Control", "no-store")
			var leaders atomic.Int32
			reached := make(chan struct{}, 8)
			rg.orig.setHandle(func(w http.ResponseWriter, r *http.Request) {
				if leaders.Add(1) == 1 {
					arrived <- struct{}{}
					tc.lead(w, r, gate, finish)
					return
				}
				reached <- struct{}{}
				writeFull(w, r, waiterHdr, full)
			})
			leader := serveAsync(rg, newGet(context.Background(), "/x", nil))
			testwait.Recv(t, "leader at origin", arrived, nil)
			waiters := make([]*httptest.ResponseRecorder, 3)
			done := parallel(3, func(i int) { waiters[i] = rg.get("/x", nil) })
			awaitWaiters(t, rg, 3)
			close(gate)
			if tc.mid {
				for range 3 {
					testwait.Recv(t, "waiter reached the origin while the leader is mid-body", reached, nil)
				}
				close(finish)
			}
			awaitServed(t, "leader returned", leader)
			testwait.Done(t, "waiters returned", done, nil)
			for i, rec := range waiters {
				if rec.Code != http.StatusOK || rec.Body.String() != full {
					t.Fatalf("waiter %d answered %d %q", i, rec.Code, rec.Body.String())
				}
			}
			if n := rg.orig.count(); n != 4 {
				t.Fatalf("origin calls %d, want 4", n)
			}
			if n := rg.stores(storeStored); n != 0 {
				t.Fatalf("stored %v, want 0", n)
			}
			if n := rg.coalesced(); n != 0 {
				t.Fatalf("coalesced %v, want 0", n)
			}
		})
	}
}

func TestCoalesceVaryMismatch(t *testing.T) {
	rg := newRig(t, rigOpts{})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Vary": {"Accept-Language"}, "Content-Length": {"1"}}, "v")
	arrived, release := rg.orig.hold()
	t.Cleanup(release)
	leader := serveAsync(rg, newGet(context.Background(), "/v", http.Header{"Accept-Language": {"en"}}))
	testwait.Recv(t, "leader at origin", arrived, nil)
	same := serveAsync(rg, newGet(context.Background(), "/v", http.Header{"Accept-Language": {"en"}}))
	other := serveAsync(rg, newGet(context.Background(), "/v", http.Header{"Accept-Language": {"fr"}}))
	awaitWaiters(t, rg, 2)
	release()
	awaitServed(t, "leader returned", leader)
	s := awaitServed(t, "same variant returned", same).rec
	o := awaitServed(t, "other variant returned", other).rec
	if s.Code != http.StatusOK || s.Body.String() != "v" || !lastStatus(t, s).Collapsed {
		t.Fatalf("same variant answered %d %q, member %+v", s.Code, s.Body.String(), lastStatus(t, s))
	}
	if o.Code != http.StatusOK || o.Body.String() != "v" || lastStatus(t, o).Collapsed {
		t.Fatalf("other variant answered %d %q, member %+v", o.Code, o.Body.String(), lastStatus(t, o))
	}
	if n := rg.orig.count(); n != 2 {
		t.Fatalf("origin calls %d, want 2", n)
	}
	if n := rg.coalesced(); n != 1 {
		t.Fatalf("coalesced %v, want 1", n)
	}
}

func TestCoalesceBounds(t *testing.T) {
	t.Run("max waiters", func(t *testing.T) {
		rg := newRig(t, rigOpts{global: "coalescing:\n  max_waiters: 1"})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"1"}}, "b")
		arrived, release := rg.orig.hold()
		t.Cleanup(release)
		leader := serveAsync(rg, newGet(context.Background(), "/b", nil))
		testwait.Recv(t, "leader at origin", arrived, nil)
		first := serveAsync(rg, newGet(context.Background(), "/b", nil))
		awaitWaiters(t, rg, 1)
		second := serveAsync(rg, newGet(context.Background(), "/b", nil))
		testwait.Until(t, "second request forwarded alone", func() bool { return rg.orig.count() == 2 }, nil)
		if n := rg.group.Stats().Waiters; n != 1 {
			t.Fatalf("waiters %d, want 1", n)
		}
		release()
		awaitServed(t, "leader returned", leader)
		f := awaitServed(t, "first waiter returned", first).rec
		s := awaitServed(t, "second request returned", second).rec
		if f.Code != http.StatusOK || f.Body.String() != "b" || !lastStatus(t, f).Collapsed {
			t.Fatalf("first waiter answered %d %q, member %+v", f.Code, f.Body.String(), lastStatus(t, f))
		}
		if s.Code != http.StatusOK || s.Body.String() != "b" || lastStatus(t, s).Collapsed {
			t.Fatalf("second request answered %d %q, member %+v", s.Code, s.Body.String(), lastStatus(t, s))
		}
		if n := rg.orig.count(); n != 2 {
			t.Fatalf("origin calls %d, want 2", n)
		}
		if n := rg.coalesced(); n != 1 {
			t.Fatalf("coalesced %v, want 1", n)
		}
	})
	t.Run("max keys", func(t *testing.T) {
		rg := newRig(t, rigOpts{global: "coalescing:\n  max_keys: 1"})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"1"}}, "k")
		arrived, release := rg.orig.hold()
		t.Cleanup(release)
		leader := serveAsync(rg, newGet(context.Background(), "/one", nil))
		testwait.Recv(t, "leader at origin", arrived, nil)
		other := serveAsync(rg, newGet(context.Background(), "/two", nil))
		testwait.Until(t, "other key forwarded alone", func() bool { return rg.orig.count() == 2 }, nil)
		release()
		awaitServed(t, "leader returned", leader)
		awaitServed(t, "other key returned", other)
		if !hit(t, rg.get("/two", nil)) {
			t.Fatal("other key was not stored")
		}
	})
}

func TestCoalesceTimeout(t *testing.T) {
	t.Run("fresh miss", func(t *testing.T) {
		rg := newRig(t, rigOpts{global: "coalescing:\n  timeout: 5s"})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"4"}}, "full")
		arrived, release := rg.orig.hold()
		t.Cleanup(release)
		leader := serveAsync(rg, newGet(context.Background(), "/t", nil))
		testwait.Recv(t, "leader at origin", arrived, nil)
		waiter := serveAsync(rg, newGet(context.Background(), "/t", nil))
		awaitWaiters(t, rg, 1)
		rg.clk.Advance(5*time.Second - time.Nanosecond)
		if n := rg.group.Stats().Waiters; n != 1 {
			t.Fatalf("waiters %d before the timeout, want 1", n)
		}
		if n := rg.orig.count(); n != 1 {
			t.Fatalf("origin calls %d before the timeout, want 1", n)
		}
		rg.clk.Advance(time.Nanosecond)
		testwait.Until(t, "timed-out waiter forwarded alone", func() bool { return rg.orig.count() == 2 }, nil)
		release()
		w := awaitServed(t, "waiter returned", waiter).rec
		awaitServed(t, "leader returned", leader)
		st := lastStatus(t, w)
		if st.Detail != detailCoalesceTimeout || st.Stored || st.Fwd != httpcache.FwdURIMiss || st.Collapsed {
			t.Fatalf("timed-out waiter member %+v", st)
		}
		if n := len(rg.spy.callsOf(opPut)); n != 1 {
			t.Fatalf("put calls %d, want 1", n)
		}
	})
	t.Run("stale entry keeps its stale status", func(t *testing.T) {
		rg := newRig(t, rigOpts{global: "coalescing:\n  timeout: 5s", route: "enabled: true\nkeep: 1h"})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=1"}, "Etag": {`"v"`}}, "old")
		rg.get("/s", nil)
		rg.clk.Advance(2 * time.Second)
		arrived, release := rg.orig.hold()
		t.Cleanup(release)
		leader := serveAsync(rg, newGet(context.Background(), "/s", nil))
		testwait.Recv(t, "leader at origin", arrived, nil)
		waiter := serveAsync(rg, newGet(context.Background(), "/s", nil))
		awaitWaiters(t, rg, 1)
		rg.clk.Advance(5*time.Second - time.Nanosecond)
		rg.clk.Advance(time.Nanosecond)
		testwait.Until(t, "timed-out waiter forwarded alone", func() bool { return rg.orig.count() == 3 }, nil)
		release()
		w := awaitServed(t, "waiter returned", waiter).rec
		awaitServed(t, "leader returned", leader)
		st := lastStatus(t, w)
		if st.Fwd != httpcache.FwdStale || st.Detail != detailCoalesceTimeout {
			t.Fatalf("timed-out waiter member %+v", st)
		}
	})
}

func TestCoalescePanic(t *testing.T) {
	const secret = "secret panic text"
	expectInternal := func(t *testing.T, what string, rec *httptest.ResponseRecorder) {
		t.Helper()
		if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), httperr.CodeInternal) {
			t.Fatalf("%s answered %d %q", what, rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "secret") {
			t.Fatalf("%s body leaks the panic value: %q", what, rec.Body.String())
		}
	}
	t.Run("origin panics", func(t *testing.T) {
		var logs bytes.Buffer
		rg := newRig(t, rigOpts{logger: slog.New(slog.NewTextHandler(&logs, nil))})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		arrived, release := rg.orig.hold()
		t.Cleanup(release)
		rg.orig.setHandle(func(http.ResponseWriter, *http.Request) { panic(secret) })
		leader := serveAsync(rg, newGet(context.Background(), "/p", nil))
		testwait.Recv(t, "leader at origin", arrived, nil)
		waiters := []<-chan served{
			serveAsync(rg, newGet(context.Background(), "/p", nil)),
			serveAsync(rg, newGet(context.Background(), "/p", nil)),
		}
		awaitWaiters(t, rg, 2)
		release()
		l := awaitServed(t, "leader returned", leader)
		if l.panicked != nil {
			t.Fatalf("leader panicked with %v", l.panicked)
		}
		expectInternal(t, "leader", l.rec)
		for i, ch := range waiters {
			w := awaitServed(t, "waiter returned", ch)
			if w.panicked != nil {
				t.Fatalf("waiter %d panicked with %v", i, w.panicked)
			}
			expectInternal(t, "waiter", w.rec)
		}
		if n := len(rg.spy.callsOf(opPut)); n != 0 {
			t.Fatalf("put calls %d, want 0", n)
		}
		if st := rg.group.Stats(); st.Keys != 0 {
			t.Fatalf("group keys %d, want 0", st.Keys)
		}
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"2"}}, "ok")
		if rec := rg.get("/p", nil); rec.Code != http.StatusOK || rec.Body.String() != "ok" {
			t.Fatalf("following GET answered %d %q", rec.Code, rec.Body.String())
		}
		if n := len(rg.spy.callsOf(opPut)); n != 1 {
			t.Fatalf("put calls after the following GET %d, want 1", n)
		}
		if out := logs.String(); !strings.Contains(out, "panic recovered") || !strings.Contains(out, secret) {
			t.Fatalf("log %q lacks the recovered panic", out)
		}
	})
	t.Run("store panics after the response was sent", func(t *testing.T) {
		var logs bytes.Buffer
		rg := newRig(t, rigOpts{logger: slog.New(slog.NewTextHandler(&logs, nil))})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"1"}}, "x")
		arrived, release := rg.orig.hold()
		t.Cleanup(release)
		rg.spy.panicOn(opPut)
		leader := serveAsync(rg, newGet(context.Background(), "/p", nil))
		testwait.Recv(t, "leader at origin", arrived, nil)
		waiters := []<-chan served{
			serveAsync(rg, newGet(context.Background(), "/p", nil)),
			serveAsync(rg, newGet(context.Background(), "/p", nil)),
		}
		awaitWaiters(t, rg, 2)
		release()
		l := awaitServed(t, "leader returned", leader)
		if l.panicked != http.ErrAbortHandler {
			t.Fatalf("leader panicked with %v, want ErrAbortHandler", l.panicked)
		}
		for i, ch := range waiters {
			w := awaitServed(t, "waiter returned", ch)
			if w.panicked != nil {
				t.Fatalf("waiter %d panicked with %v", i, w.panicked)
			}
			expectInternal(t, "waiter", w.rec)
		}
		if st := rg.group.Stats(); st.Keys != 0 {
			t.Fatalf("group keys %d, want 0", st.Keys)
		}
		if out := logs.String(); !strings.Contains(out, "panic recovered") || !strings.Contains(out, secret) {
			t.Fatalf("log %q lacks the recovered panic", out)
		}
	})
	t.Run("origin aborts the response", func(t *testing.T) {
		var logs bytes.Buffer
		rg := newRig(t, rigOpts{logger: slog.New(slog.NewTextHandler(&logs, nil))})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"1"}}, "x")
		arrived, release := rg.orig.hold()
		t.Cleanup(release)
		var leaders atomic.Int32
		rg.orig.setHandle(func(w http.ResponseWriter, r *http.Request) {
			if leaders.Add(1) == 1 {
				panic(http.ErrAbortHandler)
			}
			writeFull(w, r, http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"1"}}, "x")
		})
		leader := serveAsync(rg, newGet(context.Background(), "/p", nil))
		testwait.Recv(t, "leader at origin", arrived, nil)
		waiters := []<-chan served{
			serveAsync(rg, newGet(context.Background(), "/p", nil)),
			serveAsync(rg, newGet(context.Background(), "/p", nil)),
		}
		awaitWaiters(t, rg, 2)
		release()
		l := awaitServed(t, "leader returned", leader)
		if l.panicked != http.ErrAbortHandler {
			t.Fatalf("leader panicked with %v, want ErrAbortHandler", l.panicked)
		}
		for i, ch := range waiters {
			w := awaitServed(t, "waiter returned", ch)
			if w.panicked != nil || w.rec.Code != http.StatusOK || w.rec.Body.String() != "x" {
				t.Fatalf("waiter %d answered %v %d %q", i, w.panicked, w.rec.Code, w.rec.Body.String())
			}
		}
		if n := rg.orig.count(); n != 3 {
			t.Fatalf("origin calls %d, want 3", n)
		}
		if out := logs.String(); strings.Contains(out, "panic recovered") {
			t.Fatalf("abort logged a panic: %q", out)
		}
	})
}

func TestCoalesceRevalidated(t *testing.T) {
	rg := newRig(t, rigOpts{route: "enabled: true\nkeep: 1h"})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=1"}, "Etag": {`"v1"`}}, "old")
	rg.get("/r", nil)
	rg.clk.Advance(2 * time.Second)
	rg.orig.setHandle(func(w http.ResponseWriter, r *http.Request) {
		ex := reqctx.ExchangeFrom(r.Context())
		ex.Begin()
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("Etag", `"v1"`)
		w.WriteHeader(http.StatusNotModified)
		ex.Finish()
	})
	arrived, release := rg.orig.hold()
	t.Cleanup(release)
	leader := serveAsync(rg, newGet(context.Background(), "/r", nil))
	testwait.Recv(t, "leader at origin", arrived, nil)
	first := serveAsync(rg, newGet(context.Background(), "/r", nil))
	second := serveAsync(rg, newGet(context.Background(), "/r", nil))
	awaitWaiters(t, rg, 2)
	release()
	awaitServed(t, "leader returned", leader)
	for i, ch := range []<-chan served{first, second} {
		rec := awaitServed(t, "waiter returned", ch).rec
		st := lastStatus(t, rec)
		if rec.Code != http.StatusOK || rec.Body.String() != "old" {
			t.Fatalf("waiter %d answered %d %q", i, rec.Code, rec.Body.String())
		}
		if !st.Hit || st.Fwd != httpcache.FwdStale || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified || !st.Collapsed {
			t.Fatalf("waiter %d member %+v", i, st)
		}
	}
	if n := rg.requests(resultRevalidated); n != 3 {
		t.Fatalf("revalidated requests %v, want 3", n)
	}
	if n := rg.coalesced(); n != 2 {
		t.Fatalf("coalesced %v, want 2", n)
	}
	if n := rg.orig.conditional(); n != 1 {
		t.Fatalf("conditional origin requests %d, want 1", n)
	}
}

func TestCoalesceWaiterPreconditions(t *testing.T) {
	rg := newRig(t, rigOpts{})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"s"`}, "Content-Length": {"6"}}, "shared")
	arrived, release := rg.orig.hold()
	t.Cleanup(release)
	leader := serveAsync(rg, newGet(context.Background(), "/p", nil))
	testwait.Recv(t, "leader at origin", arrived, nil)
	waiter := serveAsync(rg, newGet(context.Background(), "/p", http.Header{"If-None-Match": {`"s"`}}))
	awaitWaiters(t, rg, 1)
	release()
	l := awaitServed(t, "leader returned", leader).rec
	w := awaitServed(t, "waiter returned", waiter).rec
	if l.Code != http.StatusOK || l.Body.String() != "shared" {
		t.Fatalf("leader answered %d %q", l.Code, l.Body.String())
	}
	if w.Code != http.StatusNotModified || w.Body.Len() != 0 {
		t.Fatalf("waiter answered %d %q, want 304 without body", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Etag"); got != `"s"` {
		t.Fatalf("waiter Etag %q", got)
	}
}

func TestCoalesceWaiterDeadline(t *testing.T) {
	rg := newRig(t, rigOpts{})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"4"}}, "full")
	arrived, release := rg.orig.hold()
	t.Cleanup(release)
	leader := serveAsync(rg, newGet(context.Background(), "/g", nil))
	testwait.Recv(t, "leader at origin", arrived, nil)
	ctx, cancel := clock.WithTimeout(context.Background(), rg.clk, 2*time.Second)
	defer cancel()
	waiter := serveAsync(rg, newGet(ctx, "/g", nil))
	awaitWaiters(t, rg, 1)
	rg.clk.Advance(2 * time.Second)
	w := awaitServed(t, "waiter answered", waiter).rec
	if w.Code != http.StatusGatewayTimeout || !strings.Contains(w.Body.String(), httperr.CodeGatewayTimeout) {
		t.Fatalf("waiter answered %d %q", w.Code, w.Body.String())
	}
	release()
	if l := awaitServed(t, "leader returned", leader).rec; l.Code != http.StatusOK || l.Body.String() != "full" {
		t.Fatalf("leader answered %d %q", l.Code, l.Body.String())
	}
}

func TestOuterHeadersNotStored(t *testing.T) {
	wrap := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Request-ID", r.Header.Get("X-Test-ID"))
			next.ServeHTTP(w, r)
		})
	}
	rg := newRig(t, rigOpts{wrap: wrap})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"1"}}, "x")
	id := func(v string) http.Header {
		h := http.Header{}
		h.Set("X-Test-ID", v)
		return h
	}
	if rec := rg.get("/o", id("a")); rec.Header().Get("X-Request-ID") != "a" {
		t.Fatalf("fill X-Request-ID %q", rec.Header().Get("X-Request-ID"))
	}
	puts := rg.spy.callsOf(opPut)
	if len(puts) != 1 || puts[0].Header.Get("X-Request-ID") != "" {
		t.Fatalf("stored header carries the request ID: %v", puts)
	}
	h := rg.get("/o", id("b"))
	if !hit(t, h) || h.Header().Get("X-Request-ID") != "b" {
		t.Fatalf("hit X-Request-ID %q", h.Header().Get("X-Request-ID"))
	}
	arrived, release := rg.orig.hold()
	t.Cleanup(release)
	leader := serveAsync(rg, newGet(context.Background(), "/s", id("c")))
	testwait.Recv(t, "leader at origin", arrived, nil)
	waiter := serveAsync(rg, newGet(context.Background(), "/s", id("d")))
	awaitWaiters(t, rg, 1)
	release()
	l := awaitServed(t, "leader returned", leader).rec
	w := awaitServed(t, "waiter returned", waiter).rec
	if l.Header().Get("X-Request-ID") != "c" {
		t.Fatalf("leader X-Request-ID %q", l.Header().Get("X-Request-ID"))
	}
	if !lastStatus(t, w).Collapsed || w.Header().Get("X-Request-ID") != "d" {
		t.Fatalf("waiter X-Request-ID %q, member %+v", w.Header().Get("X-Request-ID"), lastStatus(t, w))
	}
}
