// Package storetest provides an acceptance suite that every store backend must
// pass unchanged.
//
// The suite models two gateway instances, a and b, that share one state. A
// backend passes when both handles observe the same decisions, a denied
// request consumes nothing, and invalid limits and canceled contexts leave the
// state untouched. The suite never reads the real clock: time moves only
// through the Advance function supplied by the backend's Harness.
package storetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/store"
)

// Harness adapts a backend to the suite.
type Harness[S any] struct {
	// New returns two handles, a and b, over one fresh shared state. The
	// harness registers its own cleanup for the handles.
	New func(t *testing.T) (a, b S)
	// Advance moves the backend clock forward by exactly d.
	Advance func(t *testing.T, d time.Duration)
}

// RateLimiter runs the conformance subtests against the backend described by h.
func RateLimiter(t *testing.T, h Harness[store.RateLimiter]) {
	t.Run("TokenBucketBurstThenRefill", func(t *testing.T) {
		a, _ := h.New(t)
		tb := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
		want(t, "first take", take(t, a, "tb", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 2, ResetAfter: 200 * time.Millisecond})
		want(t, "second take", take(t, a, "tb", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 1, ResetAfter: 400 * time.Millisecond})
		want(t, "third take", take(t, a, "tb", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 0, ResetAfter: 600 * time.Millisecond})
		want(t, "fourth take", take(t, a, "tb", tb), store.Decision{Allowed: false, Limit: 3, Remaining: 0, RetryAfter: 200 * time.Millisecond, ResetAfter: 600 * time.Millisecond})
		for round := 0; round < 5; round++ {
			h.Advance(t, 200*time.Millisecond)
			want(t, "refilled take", take(t, a, "tb", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 0, ResetAfter: 600 * time.Millisecond})
			want(t, "denied take after refill", take(t, a, "tb", tb), store.Decision{Allowed: false, Limit: 3, Remaining: 0, RetryAfter: 200 * time.Millisecond, ResetAfter: 600 * time.Millisecond})
		}
	})

	t.Run("TokenBucketScriptedDecisions", func(t *testing.T) {
		a, _ := h.New(t)
		tb := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
		want(t, "t+0 take 1", take(t, a, "tb", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 2, ResetAfter: 200 * time.Millisecond})
		want(t, "t+0 take 2", take(t, a, "tb", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 1, ResetAfter: 400 * time.Millisecond})
		want(t, "t+0 take 3", take(t, a, "tb", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 0, ResetAfter: 600 * time.Millisecond})
		want(t, "t+0 take 4", take(t, a, "tb", tb), store.Decision{Allowed: false, Limit: 3, Remaining: 0, RetryAfter: 200 * time.Millisecond, ResetAfter: 600 * time.Millisecond})
		h.Advance(t, 199*time.Millisecond)
		want(t, "t+199ms", take(t, a, "tb", tb), store.Decision{Allowed: false, Limit: 3, Remaining: 0, RetryAfter: time.Millisecond, ResetAfter: 401 * time.Millisecond})
		h.Advance(t, time.Millisecond)
		want(t, "t+200ms", take(t, a, "tb", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 0, ResetAfter: 600 * time.Millisecond})
		h.Advance(t, 600*time.Millisecond)
		want(t, "t+800ms", take(t, a, "tb", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 2, ResetAfter: 200 * time.Millisecond})
	})

	t.Run("DeniedRequestsConsumeNothing", func(t *testing.T) {
		a, _ := h.New(t)
		tb := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
		for i := 0; i < 3; i++ {
			take(t, a, "tb", tb)
		}
		denied := store.Decision{Allowed: false, Limit: 3, Remaining: 0, RetryAfter: 200 * time.Millisecond, ResetAfter: 600 * time.Millisecond}
		for i := 0; i < 100; i++ {
			want(t, "denied take", take(t, a, "tb", tb), denied)
		}
		h.Advance(t, 200*time.Millisecond)
		if n := countAllowed(t, a, "tb", tb, 10); n != 1 {
			t.Errorf("after refill admitted %d of 10 takes, want 1", n)
		}

		align(t, h, a, 8*time.Second)
		sw := store.Limit{Algorithm: store.SlidingWindow, Count: 4, Period: 8 * time.Second}
		for i := 0; i < 4; i++ {
			take(t, a, "sw", sw)
		}
		swDenied := store.Decision{Allowed: false, Limit: 4, Remaining: 0, RetryAfter: 10 * time.Second, ResetAfter: 16 * time.Second}
		for i := 0; i < 100; i++ {
			want(t, "sliding denied take", take(t, a, "sw", sw), swDenied)
		}
		h.Advance(t, 10*time.Second)
		if n := countAllowed(t, a, "sw", sw, 10); n != 1 {
			t.Errorf("after window advance admitted %d of 10 takes, want 1", n)
		}
	})

	t.Run("TokenBucketFullRefillAfterIdle", func(t *testing.T) {
		a, _ := h.New(t)
		tb := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
		for i := 0; i < 3; i++ {
			take(t, a, "tb", tb)
		}
		h.Advance(t, 600*time.Millisecond)
		want(t, "refill after 600ms", take(t, a, "tb", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 2, ResetAfter: 200 * time.Millisecond})
		for i := 0; i < 2; i++ {
			take(t, a, "tb", tb)
		}
		h.Advance(t, time.Hour)
		want(t, "refill after idle hour", take(t, a, "tb", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 2, ResetAfter: 200 * time.Millisecond})
	})

	t.Run("SlidingWindowScripted", func(t *testing.T) {
		a, _ := h.New(t)
		sw := store.Limit{Algorithm: store.SlidingWindow, Count: 4, Period: 8 * time.Second}
		align(t, h, a, 8*time.Second)
		want(t, "window take 1", take(t, a, "sw", sw), store.Decision{Allowed: true, Limit: 4, Remaining: 3, ResetAfter: 16 * time.Second})
		want(t, "window take 2", take(t, a, "sw", sw), store.Decision{Allowed: true, Limit: 4, Remaining: 2, ResetAfter: 16 * time.Second})
		want(t, "window take 3", take(t, a, "sw", sw), store.Decision{Allowed: true, Limit: 4, Remaining: 1, ResetAfter: 16 * time.Second})
		want(t, "window take 4", take(t, a, "sw", sw), store.Decision{Allowed: true, Limit: 4, Remaining: 0, ResetAfter: 16 * time.Second})
		want(t, "window denied", take(t, a, "sw", sw), store.Decision{Allowed: false, Limit: 4, Remaining: 0, RetryAfter: 10 * time.Second, ResetAfter: 16 * time.Second})
		h.Advance(t, 9999*time.Millisecond)
		want(t, "t+9999ms", take(t, a, "sw", sw), store.Decision{Allowed: false, Limit: 4, Remaining: 0, RetryAfter: time.Millisecond, ResetAfter: 6001 * time.Millisecond})
		h.Advance(t, time.Millisecond)
		want(t, "t+10s", take(t, a, "sw", sw), store.Decision{Allowed: true, Limit: 4, Remaining: 0, ResetAfter: 14 * time.Second})
		h.Advance(t, 4*time.Second)
		want(t, "t+14s", take(t, a, "sw", sw), store.Decision{Allowed: true, Limit: 4, Remaining: 1, ResetAfter: 10 * time.Second})
		h.Advance(t, 18*time.Second)
		want(t, "after two-window gap", take(t, a, "sw", sw), store.Decision{Allowed: true, Limit: 4, Remaining: 3, ResetAfter: 16 * time.Second})
	})

	t.Run("KeyIsolation", func(t *testing.T) {
		a, _ := h.New(t)
		tb := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
		for i := 0; i < 3; i++ {
			take(t, a, "k1", tb)
		}
		want(t, "exhausted key", take(t, a, "k1", tb), store.Decision{Allowed: false, Limit: 3, Remaining: 0, RetryAfter: 200 * time.Millisecond, ResetAfter: 600 * time.Millisecond})
		want(t, "other key", take(t, a, "k2", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 2, ResetAfter: 200 * time.Millisecond})

		align(t, h, a, 8*time.Second)
		sw := store.Limit{Algorithm: store.SlidingWindow, Count: 4, Period: 8 * time.Second}
		for i := 0; i < 4; i++ {
			take(t, a, "k1-sw", sw)
		}
		want(t, "exhausted sliding key", take(t, a, "k1-sw", sw), store.Decision{Allowed: false, Limit: 4, Remaining: 0, RetryAfter: 10 * time.Second, ResetAfter: 16 * time.Second})
		want(t, "other sliding key", take(t, a, "k2-sw", sw), store.Decision{Allowed: true, Limit: 4, Remaining: 3, ResetAfter: 16 * time.Second})
	})

	t.Run("InvalidLimits", func(t *testing.T) {
		a, _ := h.New(t)
		tb := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
		invalid := []store.Limit{
			{Algorithm: 0, Count: 1, Period: time.Second, Burst: 1},
			{Algorithm: 3, Count: 1, Period: time.Second, Burst: 1},
			{Algorithm: store.TokenBucket, Count: 0, Period: time.Second, Burst: 1},
			{Algorithm: store.TokenBucket, Count: 1, Period: 0, Burst: 1},
			{Algorithm: store.TokenBucket, Count: 1, Period: time.Second, Burst: 0},
			{Algorithm: store.TokenBucket, Count: 1001, Period: time.Millisecond, Burst: 1},
			{Algorithm: store.TokenBucket, Count: 1, Period: time.Hour, Burst: 25},
			{Algorithm: store.SlidingWindow, Count: 1, Period: 24*time.Hour + time.Nanosecond},
		}
		for _, l := range invalid {
			_, err := a.Take(context.Background(), "tb", l)
			if !errors.Is(err, store.ErrInvalidLimit) || errors.Is(err, store.ErrUnavailable) {
				t.Errorf("Take with limit %+v: error %v, want ErrInvalidLimit and not ErrUnavailable", l, err)
			}
		}
		want(t, "take after invalid limits", take(t, a, "tb", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 2, ResetAfter: 200 * time.Millisecond})
	})

	t.Run("CanceledContext", func(t *testing.T) {
		a, b := h.New(t)
		tb := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for _, s := range []store.RateLimiter{a, b} {
			_, err := s.Take(ctx, "cc", tb)
			if !errors.Is(err, context.Canceled) || errors.Is(err, store.ErrUnavailable) {
				t.Errorf("canceled Take: error %v, want context.Canceled and not ErrUnavailable", err)
			}
		}
		want(t, "take after canceled", take(t, a, "cc", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 2, ResetAfter: 200 * time.Millisecond})
	})

	t.Run("ConsistentAcrossHandles", func(t *testing.T) {
		a, b := h.New(t)
		tb := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
		want(t, "handle a", take(t, a, "shared", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 2, ResetAfter: 200 * time.Millisecond})
		want(t, "handle b", take(t, b, "shared", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 1, ResetAfter: 400 * time.Millisecond})
		alternating := []store.RateLimiter{a, b}
		allowed := 0
		for i := 0; i < 10; i++ {
			if take(t, alternating[i%2], "alt", tb).Allowed {
				allowed++
			}
		}
		if allowed != 3 {
			t.Errorf("alternating takes admitted %d, want 3", allowed)
		}

		align(t, h, a, 8*time.Second)
		sw := store.Limit{Algorithm: store.SlidingWindow, Count: 4, Period: 8 * time.Second}
		want(t, "sliding handle a", take(t, a, "sw-alt", sw), store.Decision{Allowed: true, Limit: 4, Remaining: 3, ResetAfter: 16 * time.Second})
		want(t, "sliding handle b", take(t, b, "sw-alt", sw), store.Decision{Allowed: true, Limit: 4, Remaining: 2, ResetAfter: 16 * time.Second})
		allowed = 2
		for i := 0; i < 8; i++ {
			if take(t, alternating[i%2], "sw-alt", sw).Allowed {
				allowed++
			}
		}
		if allowed != 4 {
			t.Errorf("sliding window alternating takes admitted %d, want 4", allowed)
		}
	})

	t.Run("ConcurrentTakes", func(t *testing.T) {
		a, b := h.New(t)
		handles := []store.RateLimiter{a, b}
		tbBig := store.Limit{Algorithm: store.TokenBucket, Count: 50, Period: time.Second, Burst: 50}
		swBig := store.Limit{Algorithm: store.SlidingWindow, Count: 50, Period: 8 * time.Second}
		burst := func(key string, l store.Limit) int {
			const goroutines = 16
			const takesEach = 25
			start := make(chan struct{})
			var wg sync.WaitGroup
			counts := make([]int, goroutines)
			errs := make([]error, goroutines)
			for g := 0; g < goroutines; g++ {
				wg.Add(1)
				go func(g int, s store.RateLimiter) {
					defer wg.Done()
					<-start
					for i := 0; i < takesEach; i++ {
						d, err := s.Take(context.Background(), key, l)
						if err != nil {
							errs[g] = err
							return
						}
						if d.Allowed {
							counts[g]++
						}
					}
				}(g, handles[g%2])
			}
			close(start)
			wg.Wait()
			total := 0
			for g := 0; g < goroutines; g++ {
				if errs[g] != nil {
					t.Errorf("concurrent take failed: %v", errs[g])
				}
				total += counts[g]
			}
			return total
		}

		if got := burst("conc-tb", tbBig); got != 50 {
			t.Errorf("first token bucket burst admitted %d, want 50", got)
		}
		h.Advance(t, 140*time.Millisecond)
		if got := burst("conc-tb", tbBig); got != 7 {
			t.Errorf("token bucket after 140ms admitted %d, want 7", got)
		}

		align(t, h, a, 8*time.Second)
		if got := burst("conc-sw", swBig); got != 50 {
			t.Errorf("first sliding window burst admitted %d, want 50", got)
		}
		h.Advance(t, 8*time.Second)
		if got := burst("conc-sw", swBig); got != 0 {
			t.Errorf("sliding window after one window admitted %d, want 0", got)
		}
		h.Advance(t, 4*time.Second)
		if got := burst("conc-sw", swBig); got != 25 {
			t.Errorf("sliding window at half window admitted %d, want 25", got)
		}
	})

	t.Run("CloseOneHandle", func(t *testing.T) {
		a, b := h.New(t)
		tb := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
		if err := a.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, err := a.Take(context.Background(), "close-tb", tb); !errors.Is(err, store.ErrUnavailable) {
			t.Errorf("Take on closed handle: error %v, want ErrUnavailable", err)
		}
		want(t, "take on open handle", take(t, b, "close-tb", tb), store.Decision{Allowed: true, Limit: 3, Remaining: 2, ResetAfter: 200 * time.Millisecond})
	})
}

func take(t *testing.T, s store.RateLimiter, key string, l store.Limit) store.Decision {
	t.Helper()
	d, err := s.Take(context.Background(), key, l)
	if err != nil {
		t.Fatalf("Take(%q): %v", key, err)
	}
	return d
}

func want(t *testing.T, step string, got, exp store.Decision) {
	t.Helper()
	if got != exp {
		t.Errorf("%s: got %+v, want %+v", step, got, exp)
	}
}

func countAllowed(t *testing.T, s store.RateLimiter, key string, l store.Limit, n int) int {
	t.Helper()
	allowed := 0
	for i := 0; i < n; i++ {
		if take(t, s, key, l).Allowed {
			allowed++
		}
	}
	return allowed
}

func align(t *testing.T, h Harness[store.RateLimiter], s store.RateLimiter, period time.Duration) {
	t.Helper()
	d := take(t, s, "storetest:align:"+period.String(), store.Limit{Algorithm: store.SlidingWindow, Count: 1, Period: period})
	h.Advance(t, d.ResetAfter-period)
}
