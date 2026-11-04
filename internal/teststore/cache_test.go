package teststore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/store"
	"github.com/ok0/go-microgateway/store/storetest"
)

var cacheEpoch = time.Unix(1_700_000_000, 0)

type quietClock struct {
	*clock.Fake
	idle *clock.Fake
}

func (q quietClock) NewTicker(d time.Duration) clock.Ticker {
	return q.idle.NewTicker(d)
}

func onlySecondary(sec string) func([]string) (string, bool) {
	return func([]string) (string, bool) { return sec, true }
}

func newSharedCache(t *testing.T) (*SharedCache, *clock.Fake) {
	t.Helper()
	clk := clock.NewFake(cacheEpoch)
	s := NewSharedCache(quietClock{Fake: clk, idle: clock.NewFake(cacheEpoch)}, 1<<20)
	t.Cleanup(func() { _ = s.Close() })
	return s, clk
}

func fillEntry(t *testing.T, h *CacheHandle, primary, sec, body string) {
	t.Helper()
	l, err := h.Get(context.Background(), primary, onlySecondary(sec))
	if err != nil {
		t.Fatalf("Get(%q): %v", primary, err)
	}
	ok, err := h.Put(context.Background(), l.Token, primary, sec, nil, &store.Entry{Status: 200, Body: []byte(body)}, time.Hour)
	if err != nil || !ok {
		t.Fatalf("Put(%q) = (%v, %v), want (true, nil)", primary, ok, err)
	}
}

func TestSharedCacheConformance(t *testing.T) {
	var clk *clock.Fake
	var s *SharedCache
	newAt := func(t *testing.T, start time.Time) (store.Cache, store.Cache) {
		clk = clock.NewFake(start)
		s = NewSharedCache(quietClock{Fake: clk, idle: clock.NewFake(start)}, 1<<27)
		t.Cleanup(func() { _ = s.Close() })
		return s.Handle(), s.Handle()
	}
	storetest.Cache(t, storetest.Harness[store.Cache]{
		New: func(t *testing.T) (store.Cache, store.Cache) {
			return newAt(t, cacheEpoch)
		},
		NewAt: newAt,
		Advance: func(t *testing.T, d time.Duration) {
			clk.Advance(d)
			s.mem.Sweep(clk.Now())
		},
		Resolution: time.Nanosecond,
		Horizon:    time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
	})
}

func TestSharedCacheModes(t *testing.T) {
	ctx := context.Background()

	t.Run("FailPutOnly", func(t *testing.T) {
		s, _ := newSharedCache(t)
		h := s.Handle()
		s.SetOpMode("put", Fail, errBoom)
		l, err := h.Get(ctx, "p", onlySecondary("s"))
		if err != nil || l.Entry != nil {
			t.Fatalf("Get = (%+v, %v), want miss with nil error", l.Entry, err)
		}
		ok, err := h.Put(ctx, l.Token, "p", "s", nil, &store.Entry{Status: 200}, time.Hour)
		if !errors.Is(err, errBoom) || ok {
			t.Fatalf("Put = (%v, %v), want (false, %v)", ok, err, errBoom)
		}
		if _, err := h.Get(ctx, "p", onlySecondary("s")); err != nil {
			t.Fatalf("Get after put failure: %v", err)
		}
	})

	t.Run("OperationModeOrder", func(t *testing.T) {
		s, _ := newSharedCache(t)
		h := s.Handle()
		s.SetMode(Fail, errBoom)
		if _, err := h.Get(ctx, "p", onlySecondary("s")); !errors.Is(err, errBoom) {
			t.Fatalf("inherited Get error = %v, want %v", err, errBoom)
		}
		s.SetOpMode("get", Healthy, nil)
		if _, err := h.Get(ctx, "p", onlySecondary("s")); err != nil {
			t.Fatalf("per-op Healthy Get error = %v, want nil", err)
		}
		h.SetMode(Fail, errBoom)
		s.SetOpMode("get", Healthy, nil)
		if _, err := h.Get(ctx, "p", onlySecondary("s")); !errors.Is(err, errBoom) {
			t.Fatalf("handle Fail Get error = %v, want %v", err, errBoom)
		}
		h.SetMode(Inherit, nil)
		s.SetOpMode("get", Inherit, nil)
		if _, err := h.Get(ctx, "p", onlySecondary("s")); !errors.Is(err, errBoom) {
			t.Fatalf("reverted Get error = %v, want %v", err, errBoom)
		}
	})

	t.Run("Hang", func(t *testing.T) {
		s, _ := newSharedCache(t)
		h := s.Handle()
		h.SetMode(Hang, nil)
		done := make(chan error, 1)
		go func() {
			_, err := h.Get(ctx, "p", onlySecondary("s"))
			done <- err
		}()
		h.WaitBlocked(1)
		select {
		case err := <-done:
			t.Fatalf("parked call returned before Release: %v", err)
		default:
		}
		h.Release()
		if err := <-done; err != nil {
			t.Fatalf("released call error = %v, want nil", err)
		}
	})

	t.Run("HangIgnoreContext", func(t *testing.T) {
		s, _ := newSharedCache(t)
		h := s.Handle()
		h.SetMode(HangIgnoreContext, nil)
		cancelCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			_, err := h.Get(cancelCtx, "p", onlySecondary("s"))
			done <- err
		}()
		h.WaitBlocked(1)
		cancel()
		select {
		case err := <-done:
			t.Fatalf("call returned while parked after cancel: %v", err)
		default:
		}
		h.Release()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("released call error = %v, want context.Canceled", err)
		}
	})

	t.Run("Panic", func(t *testing.T) {
		s, _ := newSharedCache(t)
		h := s.Handle()
		h.SetMode(Panic, errBoom)
		var got any
		func() {
			defer func() { got = recover() }()
			_, _ = h.Get(ctx, "p", onlySecondary("s"))
		}()
		if got != errBoom {
			t.Fatalf("recovered %v, want %v", got, errBoom)
		}
	})

	t.Run("ClosedHandle", func(t *testing.T) {
		s, _ := newSharedCache(t)
		h := s.Handle()
		if err := h.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, err := h.Get(ctx, "p", onlySecondary("s")); !errors.Is(err, store.ErrUnavailable) {
			t.Fatalf("closed Get error = %v, want wrapping store.ErrUnavailable", err)
		}
		if h.Closes() != 1 {
			t.Fatalf("Closes = %d, want 1", h.Closes())
		}
	})
}

func TestSharedCacheCallsAndEntries(t *testing.T) {
	ctx := context.Background()
	s, _ := newSharedCache(t)
	a := s.Handle()
	b := s.Handle()
	if a.ID() != 0 || b.ID() != 1 {
		t.Fatalf("IDs = %d, %d, want 0, 1", a.ID(), b.ID())
	}
	if hs := s.Handles(); len(hs) != 2 || hs[0] != a || hs[1] != b {
		t.Fatalf("Handles() = %v, want [a b]", hs)
	}

	l, err := a.Get(ctx, "p", onlySecondary("s"))
	if err != nil {
		t.Fatalf("a.Get: %v", err)
	}
	e := &store.Entry{Status: 200, Body: []byte("body"), Tags: []string{"t"}}
	if ok, err := a.Put(ctx, l.Token, "p", "s", nil, e, time.Hour); !ok || err != nil {
		t.Fatalf("a.Put = (%v, %v), want (true, nil)", ok, err)
	}
	hit, err := b.Get(ctx, "p", onlySecondary("s"))
	if err != nil || hit.Entry == nil || string(hit.Entry.Body) != "body" {
		t.Fatalf("b.Get = (%+v, %v), want the entry stored through a", hit.Entry, err)
	}
	if n := s.Stats().Entries; n != 1 {
		t.Fatalf("Stats().Entries = %d, want 1", n)
	}
	if _, err := b.Purge(ctx, store.Purge{Kind: store.PurgeKeys, Keys: []string{"p"}, Retain: time.Hour}); err != nil {
		t.Fatalf("b.Purge: %v", err)
	}

	calls := s.Calls()
	wantOps := []string{"get", "put", "get", "purge"}
	wantHandles := []int{0, 0, 1, 1}
	if len(calls) != len(wantOps) {
		t.Fatalf("Calls() len = %d, want %d", len(calls), len(wantOps))
	}
	for i := range wantOps {
		if calls[i].Op != wantOps[i] || calls[i].Handle != wantHandles[i] {
			t.Fatalf("Calls()[%d] = {Op:%q Handle:%d}, want {Op:%q Handle:%d}", i, calls[i].Op, calls[i].Handle, wantOps[i], wantHandles[i])
		}
	}
	if put := calls[1]; !put.Stored || put.TTL != time.Hour || len(put.Tags) != 1 || put.Tags[0] != "t" || put.Primary != "p" || put.Secondary != "s" {
		t.Fatalf("put call = %+v, want stored with ttl 1h and tag t", put)
	}
	if get := calls[0]; get.Primary != "p" || get.Secondary != "" {
		t.Fatalf("miss get call = %+v, want primary p and no secondary", get)
	}
	if get := calls[2]; get.Primary != "p" || get.Secondary != "s" {
		t.Fatalf("hit get call = %+v, want primary p and secondary s", get)
	}
	if purge := calls[3]; purge.Purge.Kind != store.PurgeKeys || len(purge.Purge.Keys) != 1 {
		t.Fatalf("purge call = %+v, want a keys purge", purge)
	}
}

func TestSharedCachePurgeSoftMarksStale(t *testing.T) {
	ctx := context.Background()
	s, _ := newSharedCache(t)
	a := s.Handle()
	b := s.Handle()
	fillEntry(t, a, "p", "s", "body")

	s.SetMode(Fail, errBoom)
	res, err := s.Purge(ctx, store.Purge{Kind: store.PurgeKeys, Keys: []string{"p"}, Soft: true, Retain: time.Hour})
	if err != nil || res.Affected != 1 {
		t.Fatalf("soft Purge = (%+v, %v), want one affected entry and nil error", res, err)
	}
	s.SetMode(Healthy, nil)

	l, err := b.Get(ctx, "p", onlySecondary("s"))
	if err != nil || l.Entry == nil || !l.Entry.Stale {
		t.Fatalf("b.Get after soft purge = (%+v, %v), want a stale entry", l.Entry, err)
	}
	for _, c := range s.Calls() {
		if c.Op == "purge" {
			t.Fatalf("direct SharedCache.Purge was recorded as a handle call: %+v", c)
		}
	}
}

func TestSharedCachePurgeFailAfter(t *testing.T) {
	ctx := context.Background()
	s, _ := newSharedCache(t)
	a := s.Handle()
	b := s.Handle()
	fillEntry(t, a, "p", "s", "body")
	fillEntry(t, a, "q", "s", "body")

	s.SetPurgeFailAfter(1, errBoom)
	if res, err := a.Purge(ctx, store.Purge{Kind: store.PurgeKeys, Keys: []string{"p"}, Retain: time.Hour}); err != nil || res.Affected != 1 {
		t.Fatalf("first handle Purge = (%+v, %v), want one affected entry and nil error", res, err)
	}
	if res, err := a.Purge(ctx, store.Purge{Kind: store.PurgeKeys, Keys: []string{"q"}, Retain: time.Hour}); !errors.Is(err, errBoom) || res != (store.PurgeResult{}) {
		t.Fatalf("second handle Purge = (%+v, %v), want zero result and errBoom", res, err)
	}
	l, err := b.Get(ctx, "q", onlySecondary("s"))
	if err != nil || l.Entry == nil {
		t.Fatalf("Get(q) after failed purge = (%+v, %v), want the entry still present", l.Entry, err)
	}

	s.SetPurgeFailAfter(-1, nil)
	if res, err := a.Purge(ctx, store.Purge{Kind: store.PurgeKeys, Keys: []string{"q"}, Retain: time.Hour}); err != nil || res.Affected != 1 {
		t.Fatalf("handle Purge after turning mode off = (%+v, %v), want one affected entry and nil error", res, err)
	}
}

func TestSharedCacheAffectedUnknown(t *testing.T) {
	ctx := context.Background()
	s, _ := newSharedCache(t)
	a := s.Handle()
	b := s.Handle()
	fillEntry(t, a, "p", "s", "body")

	s.SetAffectedUnknown(true)
	res, err := a.Purge(ctx, store.Purge{Kind: store.PurgeKeys, Keys: []string{"p"}, Retain: time.Hour})
	if err != nil || res.AffectedKnown || res.Affected != 0 {
		t.Fatalf("handle Purge = (%+v, %v), want AffectedKnown false, Affected 0 and nil error", res, err)
	}
	if l, err := b.Get(ctx, "p", onlySecondary("s")); err != nil || l.Entry != nil {
		t.Fatalf("Get(p) after unknown-affected purge = (%+v, %v), want the entry removed", l.Entry, err)
	}
}
