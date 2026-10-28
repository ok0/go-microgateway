package memstore

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/store"
)

type refKey struct {
	started   bool
	first     int64
	tat       int64
	win       int64
	prev      int64
	curr      int64
	admitted  int64
	perWindow map[int64]int64
}

func refView(k *refKey, idx int64) (prev, curr int64) {
	if !k.started {
		return 0, 0
	}
	if k.win == idx {
		return k.prev, k.curr
	}
	if k.win == idx-1 {
		return k.curr, 0
	}
	return 0, 0
}

func refEstimate(prev, curr, elapsed, period int64) float64 {
	frac := float64(elapsed) / float64(period)
	weight := 1 - frac
	weighted := float64(float64(prev) * weight)
	return weighted + float64(curr)
}

func refTokenBucket(k *refKey, l store.Limit, now int64) store.Decision {
	t := int64(l.Period) / l.Count
	tau := t * l.Burst
	tat := now
	if k.started {
		tat = k.tat
	}
	base := tat
	if now > base {
		base = now
	}
	newTAT := base + t
	allowed := newTAT-now <= tau
	stored := tat
	if allowed {
		stored = newTAT
		if !k.started {
			k.first = now
		}
		k.started = true
		k.tat = newTAT
		k.admitted++
	}
	m := stored
	if m < now {
		m = now
	}
	left := tau - (m - now)
	remaining := int64(0)
	if left >= 0 {
		remaining = left / t
	}
	reset := stored - now
	if reset < 0 {
		reset = 0
	}
	var retry time.Duration
	if !allowed {
		retry = time.Duration(newTAT - now - tau)
	}
	return store.Decision{
		Allowed:    allowed,
		Limit:      l.Burst,
		Remaining:  remaining,
		RetryAfter: retry,
		ResetAfter: time.Duration(reset),
	}
}

func refSlidingAllowed(k *refKey, l store.Limit, idx, now int64) bool {
	period := int64(l.Period)
	prev, curr := refView(k, idx)
	elapsed := now - idx*period
	return refEstimate(prev, curr, elapsed, period)+1 <= float64(l.Count)
}

func refSlidingWindow(k *refKey, l store.Limit, now int64) store.Decision {
	period := int64(l.Period)
	idx := now / period
	prev, curr := refView(k, idx)
	elapsed := now - idx*period
	est := refEstimate(prev, curr, elapsed, period)
	allowed := est+1 <= float64(l.Count)
	if allowed {
		if !k.started {
			k.first = now
		}
		curr++
		est = refEstimate(prev, curr, elapsed, period)
		k.started = true
		k.win = idx
		k.prev = prev
		k.curr = curr
		k.admitted++
		k.perWindow[idx]++
	}
	remaining := int64(math.Floor(float64(l.Count) - est))
	if remaining < 0 {
		remaining = 0
	}
	var reset time.Duration
	switch {
	case curr > 0:
		reset = time.Duration((idx+2)*period - now)
	case prev > 0:
		reset = time.Duration((idx+1)*period - now)
	}
	var retry time.Duration
	if !allowed {
		const step = int64(time.Millisecond)
		bound := ((idx+2)*period - now + step - 1) / step
		for ms := int64(1); ms <= bound; ms++ {
			at := now + ms*step
			if refSlidingAllowed(k, l, at/period, at) {
				retry = time.Duration(ms) * time.Millisecond
				break
			}
		}
	}
	return store.Decision{
		Allowed:    allowed,
		Limit:      l.Count,
		Remaining:  remaining,
		RetryAfter: retry,
		ResetAfter: reset,
	}
}

func TestModel(t *testing.T) {
	limits := []store.Limit{
		{Algorithm: store.TokenBucket, Count: 3, Period: time.Second, Burst: 3},
		{Algorithm: store.TokenBucket, Count: 10, Period: time.Second, Burst: 1},
		{Algorithm: store.TokenBucket, Count: 7, Period: 3 * time.Second, Burst: 5},
		{Algorithm: store.TokenBucket, Count: 1, Period: 250 * time.Millisecond, Burst: 4},
		{Algorithm: store.SlidingWindow, Count: 5, Period: 2 * time.Second},
		{Algorithm: store.SlidingWindow, Count: 3, Period: 1500 * time.Millisecond},
		{Algorithm: store.SlidingWindow, Count: 10, Period: 7 * time.Second},
		{Algorithm: store.SlidingWindow, Count: 2, Period: 333 * time.Millisecond},
	}
	const keyCount = 12
	for seed := uint64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			clk := clock.NewFake(epoch)
			r := newRL(t, RateLimiterOptions{MaxKeys: 1000, Clock: quiet(clk)})
			rnd := clock.NewRand(seed)
			refs := make([]*refKey, keyCount)
			for i := range refs {
				refs[i] = &refKey{perWindow: map[int64]int64{}}
			}
			for op := 0; op < 3000; op++ {
				ki := int(rnd.Int64N(keyCount))
				l := limits[ki%len(limits)]
				key := fmt.Sprintf("model-%d", ki)
				now := clk.Now().UnixNano()

				got := take(t, r, key, l)
				var exp store.Decision
				if l.Algorithm == store.TokenBucket {
					exp = refTokenBucket(refs[ki], l, now)
				} else {
					exp = refSlidingWindow(refs[ki], l, now)
				}
				if got != exp {
					t.Fatalf("op %d key %s (%v): memstore %+v, reference %+v", op, key, l.Algorithm, got, exp)
				}

				ref := refs[ki]
				if l.Algorithm == store.TokenBucket {
					interval := int64(l.Period) / l.Count
					if limit := l.Burst + (now-ref.first)/interval; ref.admitted > limit {
						t.Fatalf("op %d key %s: admitted %d exceeds bound %d", op, key, ref.admitted, limit)
					}
				} else if ref.perWindow[now/int64(l.Period)] > l.Count {
					t.Fatalf("op %d key %s: window admitted %d exceeds count %d", op, key, ref.perWindow[now/int64(l.Period)], l.Count)
				}

				clk.Advance(time.Duration(rnd.Int64N(int64(1500 * time.Millisecond))))
			}
		})
	}
}
