package cache

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/coalesce"
	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

// loadGate skips a load test unless MICROGATEWAY_LOAD is set.
func loadGate(t *testing.T) {
	t.Helper()
	if testing.Short() || os.Getenv("MICROGATEWAY_LOAD") == "" {
		t.Skip("set MICROGATEWAY_LOAD=1")
	}
}

// TestCoalesceScale shares one upstream fetch among 1,000 concurrent requests for the same entry.
func TestCoalesceScale(t *testing.T) {
	loadGate(t)
	rg := newRig(t, rigOpts{})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"s"`}}, "shared")
	arrived, release := rg.orig.hold()
	t.Cleanup(release)
	const n = 1000
	recs := make([]*httptest.ResponseRecorder, n)
	done := parallel(n, func(i int) { recs[i] = rg.get("/scale", nil) })
	testwait.Recv(t, "leader at origin", arrived, nil)
	awaitWaiters(t, rg, n-1)
	release()
	testwait.Done(t, "all requests returned", done, nil)
	for i, rec := range recs {
		if rec.Code != http.StatusOK || rec.Body.String() != "shared" {
			t.Fatalf("request %d answered %d %q", i, rec.Code, rec.Body.String())
		}
	}
	if got := rg.orig.count(); got != 1 {
		t.Fatalf("origin calls %d, want 1", got)
	}
	if st := rg.group.Stats(); st != (coalesce.Stats{}) {
		t.Fatalf("group stats %+v, want empty", st)
	}
}

// TestCacheMixedLoad drives hits, misses, stale-while-revalidate and stale-if-error requests from many goroutines, with
// a store failure in the middle, and checks the bounds and the settled state afterwards.
func TestCacheMixedLoad(t *testing.T) {
	loadGate(t)
	const workers, iters, maxBg = 64, 1000, 8
	const maxBytes = 1 << 22
	rg := newRig(t, rigOpts{
		route:  "stale_if_error: 1h\nstale_while_revalidate: 30s",
		global: fmt.Sprintf("max_background: %d", maxBg),
	})
	rg.orig.setHandle(func(w http.ResponseWriter, r *http.Request) {
		ex := reqctx.ExchangeFrom(r.Context())
		if ex != nil {
			ex.Begin()
		}
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("Etag", `"l"`)
		status := http.StatusOK
		if strings.HasPrefix(r.URL.Path, "/sie") {
			status = http.StatusInternalServerError
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte("body"))
		if ex != nil {
			ex.Finish()
		}
	})
	rg.get("/hot", nil)
	rg.clk.Advance(61 * time.Second)
	base := runtime.NumGoroutine()
	var bgOver, bytesOver atomic.Int64
	errc := make(chan any, workers)
	done := parallel(workers, func(w int) {
		defer func() {
			if p := recover(); p != nil {
				errc <- p
			}
		}()
		for i := range iters {
			if w == 0 {
				switch i {
				case iters / 4:
					rg.spy.setErr(opGet, errors.New("load: store down"))
				case iters / 2:
					rg.spy.setErr(opGet, nil)
				}
			}
			switch i % 5 {
			case 0, 4:
				rg.get("/hot", nil)
			case 1:
				rg.get(fmt.Sprintf("/miss/%d/%d", w, i), nil)
			case 2:
				rg.get("/swr", nil)
			default:
				rg.get("/sie", nil)
			}
			if rg.runner.Running() > maxBg {
				bgOver.Add(1)
			}
			if rg.spy.inner.Bytes() > maxBytes {
				bytesOver.Add(1)
			}
		}
	})
	testwait.Done(t, "workers finished", done, nil)
	if n := len(errc); n != 0 {
		t.Fatalf("%d workers panicked, first: %v", n, <-errc)
	}
	if n := bgOver.Load(); n != 0 {
		t.Fatalf("background refreshes above max_background in %d samples", n)
	}
	if n := bytesOver.Load(); n != 0 {
		t.Fatalf("memory cache above max_bytes in %d samples", n)
	}
	rg.settle()
	if st := rg.group.Stats(); st != (coalesce.Stats{}) {
		t.Fatalf("group stats %+v, want empty", st)
	}
	testwait.Until(t, "goroutines settled", func() bool { return runtime.NumGoroutine() <= base }, func() string {
		return fmt.Sprintf("%d goroutines, base %d", runtime.NumGoroutine(), base)
	})
}

// TestPurgeLoad shares one revalidation among 1,000 requests after a soft purge, then runs 64 workers of mixed
// purges and requests, with a store failure in the middle, and checks the settled state afterwards.
func TestPurgeLoad(t *testing.T) {
	loadGate(t)
	t.Run("soft purge scale", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"p"`}}, "shared")
		rg.get("/soft", nil)
		p := newPurgeRig(t, purgeOpts{sp: rg.spy})
		if _, err := p.p.Purge(context.Background(), PurgeRequest{URLs: []string{"http://example.com/soft"}, Soft: true}, ""); err != nil {
			t.Fatal(err)
		}
		before := rg.orig.count()
		arrived, release := rg.orig.hold()
		t.Cleanup(release)
		const n = 1000
		recs := make([]*httptest.ResponseRecorder, n)
		done := parallel(n, func(i int) { recs[i] = rg.get("/soft", nil) })
		testwait.Recv(t, "revalidation at origin", arrived, nil)
		awaitWaiters(t, rg, n-1)
		release()
		testwait.Done(t, "all requests returned", done, nil)
		for i, rec := range recs {
			if rec.Code != http.StatusOK || rec.Body.String() != "shared" {
				t.Fatalf("request %d answered %d %q", i, rec.Code, rec.Body.String())
			}
		}
		if got := rg.orig.count() - before; got != 1 {
			t.Fatalf("origin requests after the soft purge %d, want 1", got)
		}
	})
	t.Run("mixed operations", func(t *testing.T) {
		rg := newRig(t, rigOpts{route: "stale_if_error: 1h"})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"m"`}}, "mixed")
		p := newPurgeRig(t, purgeOpts{sp: rg.spy})
		const workers, iters = 64, 1000
		base := runtime.NumGoroutine()
		errc := make(chan error, workers*iters)
		done := parallel(workers, func(w int) {
			ctx := context.Background()
			for i := range iters {
				if w == 0 {
					switch i {
					case iters / 4:
						rg.spy.setErr(opPurge, errors.New("load: store down"))
					case iters / 2:
						rg.spy.setErr(opPurge, nil)
					}
				}
				u := fmt.Sprintf("http://example.com/mix/%d", (w+i)%50)
				var err error
				switch i % 10 {
				case 0:
					_, err = p.p.Purge(ctx, PurgeRequest{URLs: []string{u}}, "")
				case 1:
					_, err = p.p.Purge(ctx, PurgeRequest{URLs: []string{u}, Soft: true}, "")
				case 2:
					_, err = p.p.Purge(ctx, PurgeRequest{Tags: []string{"load"}}, "")
				case 3:
					if i%100 == 3 {
						_, err = p.p.Purge(ctx, PurgeRequest{All: true}, "")
					}
				default:
					rg.get(fmt.Sprintf("/mix/%d", (w+i)%50), nil)
				}
				if err != nil && !errors.Is(err, store.ErrUnavailable) {
					errc <- err
				}
			}
		})
		testwait.Done(t, "workers finished", done, nil)
		if n := len(errc); n != 0 {
			t.Fatalf("%d unexpected purge errors, first: %v", n, <-errc)
		}
		rg.settle()
		testwait.Until(t, "goroutines settled", func() bool { return runtime.NumGoroutine() <= base }, func() string {
			return fmt.Sprintf("%d goroutines, base %d", runtime.NumGoroutine(), base)
		})
	})
}
