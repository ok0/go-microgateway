package redis

import (
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/store"
	"github.com/ok0/go-microgateway/store/storetest"
)

func miniHarness() storetest.Harness[store.RateLimiter] {
	var mc *miniClock
	return storetest.Harness[store.RateLimiter]{
		New: func(t *testing.T) (store.RateLimiter, store.RateLimiter) {
			t.Helper()
			m := newMini(t)
			mc = &miniClock{m: m, now: miniStart}
			return newTestStore(t, m.Addr(), 0, "mgtest"), newTestStore(t, m.Addr(), 0, "mgtest")
		},
		Advance: func(t *testing.T, d time.Duration) {
			t.Helper()
			mc.Advance(d)
		},
	}
}

func injectedHarness(addr func(t *testing.T) (string, int)) storetest.Harness[store.RateLimiter] {
	var fake *clock.Fake
	return storetest.Harness[store.RateLimiter]{
		New: func(t *testing.T) (store.RateLimiter, store.RateLimiter) {
			t.Helper()
			a, db := addr(t)
			prefix := uniquePrefix(t)
			raw := newTestClient(t, a, db)
			fake = clock.NewFake(injectedStart())
			sa := withInjectedClock(newTestStore(t, a, db, prefix), fake.Now)
			sb := withInjectedClock(newTestStore(t, a, db, prefix), fake.Now)
			t.Cleanup(func() { deletePrefix(t, raw, prefix) })
			return sa, sb
		},
		Advance: func(t *testing.T, d time.Duration) {
			t.Helper()
			fake.Advance(d)
		},
	}
}

func TestConformance(t *testing.T) {
	storetest.RateLimiter(t, miniHarness())
}

func TestConformanceInjected(t *testing.T) {
	storetest.RateLimiter(t, injectedHarness(func(t *testing.T) (string, int) {
		m := newMini(t)
		return m.Addr(), 0
	}))
}

func TestCacheConformance(t *testing.T) {
	storetest.Cache(t, miniCacheHarness())
}

func TestCacheConformanceInjected(t *testing.T) {
	storetest.Cache(t, injectedCacheHarness(func(t *testing.T) (string, int) {
		m := newMini(t)
		return m.Addr(), 0
	}))
}
