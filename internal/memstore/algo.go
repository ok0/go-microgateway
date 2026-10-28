package memstore

import (
	"math"
	"time"

	"github.com/ok0/lychgate/store"
)

type window struct {
	win, prev, curr int64
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && a < 0 {
		q--
	}
	return q
}

func ceilDiv(a, b int64) int64 {
	return floorDiv(a+b-1, b)
}

func gcra(tat, now int64, l store.Limit) (store.Decision, int64, bool) {
	t := int64(l.Period) / l.Count
	tau := t * l.Burst
	newTAT := max(tat, now) + t
	allowed := newTAT-now <= tau
	stored := tat
	if allowed {
		stored = newTAT
	}
	var remaining int64
	if x := tau - (max(stored, now) - now); x >= 0 {
		remaining = x / t
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
		ResetAfter: time.Duration(max(stored-now, 0)),
	}, stored, allowed
}

func view(st window, idx int64) (prev, curr int64) {
	switch st.win {
	case idx:
		return st.prev, st.curr
	case idx - 1:
		return st.curr, 0
	}
	return 0, 0
}

func estimate(prev, curr, elapsed, p int64) float64 {
	e := float64(elapsed) / float64(p)
	w := 1 - e
	pw := float64(float64(prev) * w)
	return pw + float64(curr)
}

func sliding(st window, fresh bool, now int64, l store.Limit) (store.Decision, window, bool) {
	if fresh {
		st = window{}
	}
	p := int64(l.Period)
	idx := floorDiv(now, p)
	elapsed := now - idx*p
	prev, curr := view(st, idx)
	est := estimate(prev, curr, elapsed, p)
	allowed := est+1 <= float64(l.Count)
	next := st
	if allowed {
		curr++
		est = estimate(prev, curr, elapsed, p)
		next = window{win: idx, prev: prev, curr: curr}
	}
	remaining := int64(math.Floor(float64(l.Count) - est))
	if remaining < 0 {
		remaining = 0
	}
	var resetAfter time.Duration
	switch {
	case curr > 0:
		resetAfter = time.Duration((idx+2)*p - now)
	case prev > 0:
		resetAfter = time.Duration((idx+1)*p - now)
	}
	var retry time.Duration
	if !allowed {
		retry = slidingRetry(idx, prev, curr, now, p, l.Count)
	}
	return store.Decision{
		Allowed:    allowed,
		Limit:      l.Count,
		Remaining:  remaining,
		RetryAfter: retry,
		ResetAfter: resetAfter,
	}, next, allowed
}

func slidingRetry(idx, prev, curr, now, p, count int64) time.Duration {
	st := window{win: idx, prev: prev, curr: curr}
	lo := int64(1)
	hi := ceilDiv((idx+2)*p-now, int64(time.Millisecond))
	for lo < hi {
		mid := lo + (hi-lo)/2
		if slidingAllowedAt(st, now+mid*int64(time.Millisecond), p, count) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return time.Duration(lo) * time.Millisecond
}

func slidingAllowedAt(st window, t, p, count int64) bool {
	idx := floorDiv(t, p)
	prev, curr := view(st, idx)
	return estimate(prev, curr, t-idx*p, p)+1 <= float64(count)
}

func expired(e *entry, now int64) bool {
	if e.alg == store.TokenBucket {
		return e.a <= now
	}
	return floorDiv(now, e.period) >= e.a+2
}
