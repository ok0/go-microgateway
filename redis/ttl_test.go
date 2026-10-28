package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/ok0/go-microgateway/store"
)

func pexpiretime(t *testing.T, c goredis.UniversalClient, key string) int64 {
	t.Helper()
	ms, err := c.Do(context.Background(), "PEXPIRETIME", key).Int64()
	if err != nil {
		t.Fatalf("PEXPIRETIME %q: %v", key, err)
	}
	if ms < 0 {
		t.Fatalf("PEXPIRETIME %q = %d, want an expiry", key, ms)
	}
	return ms
}

func ceilMs(t time.Time) int64 {
	return (t.UnixNano() + 999_999) / 1_000_000
}

func runTTLContract(t *testing.T, s *Store, now func() time.Time, advance func(time.Duration), expiryMs func(t *testing.T, key string) int64, keys func() []string) {
	t.Helper()
	ctx := context.Background()

	tb := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
	tbKey := s.keyFor("ttl-tb", store.TokenBucket)
	var last int64
	for i := 0; i < 4; i++ {
		if i > 0 {
			advance(time.Millisecond)
		}
		d, err := s.Take(ctx, "ttl-tb", tb)
		if err != nil {
			t.Fatalf("token bucket take %d: %v", i, err)
		}
		got := expiryMs(t, tbKey)
		if i < 3 {
			if !d.Allowed {
				t.Fatalf("token bucket take %d: %+v, want allowed", i, d)
			}
			if want := ceilMs(now().Add(d.ResetAfter)); got != want {
				t.Errorf("token bucket take %d: expiry %d, want %d", i, got, want)
			}
		} else {
			if d.Allowed {
				t.Fatalf("token bucket take %d: %+v, want denied", i, d)
			}
			if got != last {
				t.Errorf("denied token bucket take changed expiry from %d to %d", last, got)
			}
		}
		last = got
	}

	sw := store.Limit{Algorithm: store.SlidingWindow, Count: 4, Period: 8 * time.Second}
	swKey := s.keyFor("ttl-sw", store.SlidingWindow)
	for i := 0; i < 5; i++ {
		if i > 0 {
			advance(time.Millisecond)
		}
		d, err := s.Take(ctx, "ttl-sw", sw)
		if err != nil {
			t.Fatalf("sliding window take %d: %v", i, err)
		}
		got := expiryMs(t, swKey)
		if i < 4 {
			if !d.Allowed {
				t.Fatalf("sliding window take %d: %+v, want allowed", i, d)
			}
			if want := ceilMs(now().Add(d.ResetAfter)); got != want {
				t.Errorf("sliding window take %d: expiry %d, want %d", i, got, want)
			}
		} else {
			if d.Allowed {
				t.Fatalf("sliding window take %d: %+v, want denied", i, d)
			}
			if got != last {
				t.Errorf("denied sliding window take changed expiry from %d to %d", last, got)
			}
		}
		last = got
	}

	nowMs := now().UnixMilli()
	for _, k := range keys() {
		if e := expiryMs(t, k); e <= nowMs {
			t.Errorf("key %q expires at %d, not after now %d", k, e, nowMs)
		}
	}
}

func TestTTLMatchesContract(t *testing.T) {
	start := miniStart.Add(300 * time.Microsecond)
	m := miniredis.RunT(t)
	m.SetTime(start)
	mc := &miniClock{m: m, now: start}
	s := newTestStore(t, m.Addr(), 0, "mgtest")
	c := newTestClient(t, m.Addr(), 0)
	runTTLContract(t, s, func() time.Time { return mc.now }, mc.Advance,
		func(t *testing.T, key string) int64 { return pexpiretime(t, c, key) },
		m.Keys)
	for _, k := range m.Keys() {
		if m.TTL(k) <= 0 {
			t.Errorf("key %q has no positive TTL", k)
		}
	}
}

func TestFloodExpires(t *testing.T) {
	ctx := context.Background()
	m := newMini(t)
	mc := &miniClock{m: m, now: miniStart}
	s := newTestStore(t, m.Addr(), 0, "mgtest")
	c := newTestClient(t, m.Addr(), 0)
	tb := store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: time.Second, Burst: 1}
	sw := store.Limit{Algorithm: store.SlidingWindow, Count: 1, Period: time.Second}
	for i := 0; i < 5000; i++ {
		mustTake(t, s, fmt.Sprintf("flood-tb:%d", i), tb)
		mustTake(t, s, fmt.Sprintf("flood-sw:%d", i), sw)
	}
	mc.Advance(2*time.Second + time.Millisecond)
	if n := len(m.Keys()); n != 0 {
		t.Errorf("%d keys remain after expiry, want 0", n)
	}
	if n, err := c.DBSize(ctx).Result(); err != nil || n != 0 {
		t.Errorf("DBSIZE = %d, %v; want 0", n, err)
	}
}

func TestExpiryIsLossless(t *testing.T) {
	mA := newMini(t)
	mB := newMini(t)
	clkA := &miniClock{m: mA, now: miniStart}
	now := miniStart
	advB := func(d time.Duration) {
		now = now.Add(d)
		mB.SetTime(now)
	}
	sa := newTestStore(t, mA.Addr(), 0, "mgtest")
	sb := newTestStore(t, mB.Addr(), 0, "mgtest")

	tb := store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: 500 * time.Millisecond, Burst: 2}
	sw := store.Limit{Algorithm: store.SlidingWindow, Count: 3, Period: time.Second}
	const gap = 1001 * time.Millisecond
	steps := []step{
		{0, "lossless:tb", tb},
		{0, "lossless:tb", tb},
		{gap, "lossless:tb", tb},
		{gap, "lossless:tb", tb},
		{gap, "lossless:tb", tb},
		{0, "lossless:sw", sw},
		{0, "lossless:sw", sw},
		{0, "lossless:sw", sw},
		{2 * time.Second, "lossless:sw", sw},
		{0, "lossless:sw", sw},
		{2*time.Second - time.Microsecond, "lossless:sw", sw},
		{0, "lossless:sw", sw},
		{2 * time.Second, "lossless:sw", sw},
		{0, "lossless:sw", sw},
	}
	compareSteps(t, "lossless", steps,
		timedStore{rl: sa, advance: clkA.Advance},
		timedStore{rl: sb, advance: advB})
}
