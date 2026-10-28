package teststore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/store"
	"github.com/ok0/lychgate/store/storetest"
)

var errBoom = errors.New("boom")

func slidingLimit(count int64) store.Limit {
	return store.Limit{Algorithm: store.SlidingWindow, Count: count, Period: time.Second}
}

func TestConformance(t *testing.T) {
	var fake *clock.Fake
	storetest.RateLimiter(t, storetest.Harness[store.RateLimiter]{
		New: func(t *testing.T) (store.RateLimiter, store.RateLimiter) {
			fake = clock.NewFake(time.Unix(1_700_000_000, 0))
			s := NewShared(fake)
			t.Cleanup(func() { _ = s.Close() })
			return s.Handle(), s.Handle()
		},
		Advance: func(t *testing.T, d time.Duration) {
			fake.Advance(d)
		},
	})
}

func TestModes(t *testing.T) {
	ctx := context.Background()

	t.Run("Fail", func(t *testing.T) {
		fake := clock.NewFake(time.Unix(1_700_000_000, 0))
		s := NewShared(fake)
		t.Cleanup(func() { _ = s.Close() })
		h := s.Handle()
		h.SetMode(Fail, errBoom)
		d, err := h.Take(ctx, "k", slidingLimit(5))
		if !errors.Is(err, errBoom) {
			t.Fatalf("Take error = %v, want %v", err, errBoom)
		}
		if d != (store.Decision{}) {
			t.Fatalf("Take decision = %+v, want zero", d)
		}
	})

	t.Run("Panic", func(t *testing.T) {
		fake := clock.NewFake(time.Unix(1_700_000_000, 0))
		s := NewShared(fake)
		t.Cleanup(func() { _ = s.Close() })
		h := s.Handle()
		h.SetMode(Panic, errBoom)
		var got any
		func() {
			defer func() { got = recover() }()
			_, _ = h.Take(ctx, "k", slidingLimit(5))
		}()
		if got != errBoom {
			t.Fatalf("recovered %v, want %v", got, errBoom)
		}
	})

	t.Run("Hang", func(t *testing.T) {
		fake := clock.NewFake(time.Unix(1_700_000_000, 0))
		s := NewShared(fake)
		t.Cleanup(func() { _ = s.Close() })
		h := s.Handle()
		h.SetMode(Hang, nil)

		doneA := make(chan error, 1)
		go func() {
			_, err := h.Take(ctx, "a", slidingLimit(5))
			doneA <- err
		}()
		h.WaitBlocked(1)
		select {
		case err := <-doneA:
			t.Fatalf("parked call returned before Release: %v", err)
		default:
		}

		cancelCtx, cancel := context.WithCancel(ctx)
		doneB := make(chan error, 1)
		go func() {
			_, err := h.Take(cancelCtx, "b", slidingLimit(5))
			doneB <- err
		}()
		h.WaitBlocked(2)
		cancel()
		if err := <-doneB; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled parked call error = %v, want context.Canceled", err)
		}

		h.Release()
		if err := <-doneA; err != nil {
			t.Fatalf("released call error = %v, want nil", err)
		}
	})

	t.Run("HangIgnoreContext", func(t *testing.T) {
		fake := clock.NewFake(time.Unix(1_700_000_000, 0))
		s := NewShared(fake)
		t.Cleanup(func() { _ = s.Close() })
		h := s.Handle()
		h.SetMode(HangIgnoreContext, nil)

		cancelCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			_, err := h.Take(cancelCtx, "k", slidingLimit(5))
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

	t.Run("PerHandleOverridesShared", func(t *testing.T) {
		fake := clock.NewFake(time.Unix(1_700_000_000, 0))
		s := NewShared(fake)
		t.Cleanup(func() { _ = s.Close() })
		s.SetMode(Fail, errBoom)
		h1 := s.Handle()
		h2 := s.Handle()

		h1.SetMode(Healthy, nil)
		if _, err := h1.Take(ctx, "k", slidingLimit(5)); err != nil {
			t.Fatalf("healthy override Take error = %v, want nil", err)
		}
		if _, err := h2.Take(ctx, "k", slidingLimit(5)); !errors.Is(err, errBoom) {
			t.Fatalf("inherited Take error = %v, want %v", err, errBoom)
		}

		h1.SetMode(Inherit, nil)
		if _, err := h1.Take(ctx, "k", slidingLimit(5)); !errors.Is(err, errBoom) {
			t.Fatalf("reverted Take error = %v, want %v", err, errBoom)
		}
	})
}

func TestCallsAndCloses(t *testing.T) {
	ctx := context.Background()
	fake := clock.NewFake(time.Unix(1_700_000_000, 0))
	s := NewShared(fake)
	t.Cleanup(func() { _ = s.Close() })

	a := s.Handle()
	b := s.Handle()
	if a.ID() != 0 || b.ID() != 1 {
		t.Fatalf("IDs = %d, %d, want 0, 1", a.ID(), b.ID())
	}
	if hs := s.Handles(); len(hs) != 2 || hs[0] != a || hs[1] != b {
		t.Fatalf("Handles() = %v, want [a b]", hs)
	}

	l1 := slidingLimit(5)
	l2 := slidingLimit(7)
	if _, err := a.Take(ctx, "k1", l1); err != nil {
		t.Fatalf("a.Take: %v", err)
	}
	if _, err := b.Take(ctx, "k2", l2); err != nil {
		t.Fatalf("b.Take: %v", err)
	}

	calls := s.Calls()
	want := []Call{{Handle: 0, Key: "k1", Limit: l1}, {Handle: 1, Key: "k2", Limit: l2}}
	if len(calls) != len(want) {
		t.Fatalf("Calls() len = %d, want %d", len(calls), len(want))
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("Calls()[%d] = %+v, want %+v", i, calls[i], want[i])
		}
	}

	if a.Takes() != 1 || b.Takes() != 1 {
		t.Fatalf("Takes = %d, %d, want 1, 1", a.Takes(), b.Takes())
	}

	if err := a.Close(); err != nil {
		t.Fatalf("a.Close: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("a.Close second: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("b.Close: %v", err)
	}
	if a.Closes() != 2 || b.Closes() != 1 {
		t.Fatalf("Closes = %d, %d, want 2, 1", a.Closes(), b.Closes())
	}
}
