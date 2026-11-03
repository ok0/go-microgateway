package cache

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

var errStoreDown = fmt.Errorf("%w: connection refused", store.ErrUnavailable)

func TestStoreFailureBypass(t *testing.T) {
	t.Run("get error bypasses the store and counts once", func(t *testing.T) {
		var mu sync.Mutex
		var hooked []string
		rg := newRig(t, rigOpts{guard: true, onStoreError: func(op string) {
			mu.Lock()
			hooked = append(hooked, op)
			mu.Unlock()
		}})
		rg.spy.setErr(opGet, errStoreDown)
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		rec := rg.get("/f", nil)
		st := lastStatus(t, rec)
		if st.Fwd != httpcache.FwdBypass || st.Detail != "store-error" || rec.Code != http.StatusOK || rec.Body.String() != "x" {
			t.Fatalf("status %+v code %d body %q", st, rec.Code, rec.Body.String())
		}
		if n := len(rg.spy.callsOf(opPut)); n != 0 {
			t.Fatalf("put called %d times after a get failure", n)
		}
		if rg.storeErrs(opGet) != 1 {
			t.Fatalf("get store errors %v, want 1", rg.storeErrs(opGet))
		}
		mu.Lock()
		defer mu.Unlock()
		if strings.Join(hooked, ",") != opGet {
			t.Fatalf("OnStoreError calls %v, want [get]", hooked)
		}
	})
	t.Run("hung get completes after exactly store_timeout", func(t *testing.T) {
		rg := newRig(t, rigOpts{guard: true})
		release := rg.spy.hang(opGet)
		defer release()
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		done := make(chan *httptest.ResponseRecorder, 1)
		go func() { done <- rg.get("/hang", nil) }()
		testwait.Until(t, "guard timer armed", func() bool { return rg.clk.Waiters() >= 1 }, nil)
		rg.clk.Advance(100 * time.Millisecond)
		rec, ok := testwait.Recv(t, "hung request", done, nil)
		if !ok {
			t.Fatal("no response")
		}
		if st := lastStatus(t, rec); st.Fwd != httpcache.FwdBypass || st.Detail != "store-error" {
			t.Fatalf("status %+v", st)
		}
		if elapsed := rg.clk.Now().Sub(start); elapsed != 100*time.Millisecond {
			t.Fatalf("elapsed fake time %v, want exactly the store timeout", elapsed)
		}
		if rg.storeErrs(opGet) != 1 {
			t.Fatalf("get store errors %v, want 1", rg.storeErrs(opGet))
		}
	})
	t.Run("open guard short-circuits without counting", func(t *testing.T) {
		rg := newRig(t, rigOpts{guard: true})
		rg.spy.setErr(opGet, errStoreDown)
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		rg.get("/o", nil)
		rg.get("/o", nil)
		if rg.storeErrs(opGet) != 2 {
			t.Fatalf("get store errors %v before the breaker opens, want 2", rg.storeErrs(opGet))
		}
		rec := rg.get("/o", nil)
		if st := lastStatus(t, rec); st.Fwd != httpcache.FwdBypass || rec.Body.String() != "x" {
			t.Fatalf("open guard status %+v body %q", st, rec.Body.String())
		}
		if rg.storeErrs(opGet) != 2 {
			t.Fatalf("short-circuit counted: get store errors %v, want 2", rg.storeErrs(opGet))
		}
		if rg.orig.count() != 3 {
			t.Fatalf("origin calls %d, want 3", rg.orig.count())
		}
	})
	t.Run("put and purge failures leave responses unchanged", func(t *testing.T) {
		rg := newRig(t, rigOpts{guard: true})
		rg.spy.setErr(opPut, errStoreDown)
		rg.spy.setErr(opPurge, errStoreDown)
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		if rec := rg.get("/p", nil); rec.Code != http.StatusOK || rec.Body.String() != "x" {
			t.Fatalf("put failure changed the response: %d %q", rec.Code, rec.Body.String())
		}
		rg.orig.script(http.StatusCreated, nil, "")
		if rec := rg.serve(http.MethodPost, "/p", nil); rec.Code != http.StatusCreated {
			t.Fatalf("purge failure changed the response: %d", rec.Code)
		}
		if rg.storeErrs(opPut) != 1 || rg.storeErrs(opPurge) != 1 {
			t.Fatalf("put=%v purge=%v, want 1 each", rg.storeErrs(opPut), rg.storeErrs(opPurge))
		}
	})
	t.Run("recovery resumes hits", func(t *testing.T) {
		rg := newRig(t, rigOpts{guard: true})
		rg.spy.setErr(opGet, errStoreDown)
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		rg.get("/rec", nil)
		rg.get("/rec", nil)
		rg.spy.setErr(opGet, nil)
		rg.clk.Advance(time.Second)
		rg.get("/rec", nil)
		if !hit(t, rg.get("/rec", nil)) {
			t.Fatal("hit not resumed after recovery")
		}
	})
}

func TestErrorLogRateLimit(t *testing.T) {
	clk := clock.NewFake(start)
	errs, buf := logBuffer(clk)
	for i := 0; i < 5; i++ {
		errs.Log("", opGet, errStoreDown)
	}
	if n := strings.Count(buf.String(), "cache store call failed"); n != 1 {
		t.Fatalf("records within one second: %d, want 1", n)
	}
	clk.Advance(time.Second)
	errs.Log("shared", opPut, errStoreDown)
	out := buf.String()
	if n := strings.Count(out, "cache store call failed"); n != 2 {
		t.Fatalf("records after one second: %d, want 2", n)
	}
	if !strings.Contains(out, "suppressed=4") {
		t.Fatalf("second record lacks suppressed=4: %s", out)
	}
	if !strings.Contains(out, "store=memory") || !strings.Contains(out, "store=shared") {
		t.Fatalf("store names missing: %s", out)
	}
}

func TestNoGoroutineAfterRequest(t *testing.T) {
	rg := newRig(t, rigOpts{guard: true})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
	baseline := runtime.NumGoroutine()
	rg.get("/n", nil)
	if !hit(t, rg.get("/n", nil)) {
		t.Fatal("second request not a hit")
	}
	rg.get("/n", http.Header{"Range": {"bytes=0-1"}})
	release := rg.spy.hang(opGet)
	defer release()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- rg.get("/n2", nil) }()
	testwait.Until(t, "guard timer armed", func() bool { return rg.clk.Waiters() >= 1 }, nil)
	rg.clk.Advance(100 * time.Millisecond)
	if _, ok := testwait.Recv(t, "hung request", done, nil); !ok {
		t.Fatal("no response")
	}
	testwait.Until(t, "goroutines back to baseline", func() bool {
		return runtime.NumGoroutine() <= baseline
	}, func() string { return fmt.Sprintf("goroutines %d, baseline %d", runtime.NumGoroutine(), baseline) })
}
