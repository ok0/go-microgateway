package main

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	microgateway "github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/store"
)

const defaultMaxKeys = 10000

type memoryLimiter struct {
	mu      sync.Mutex
	now     func() time.Time
	maxKeys int
	keys    map[string]*limiterState
	lru     list.List
}

type limiterState struct {
	alg           store.Algorithm
	a, prev, curr int64
	period        int64
	elem          *list.Element
}

type limiterHandle struct {
	lim    *memoryLimiter
	closed atomic.Bool
}

var _ store.RateLimiter = (*limiterHandle)(nil)
var _ store.Backend = (*limiterHandle)(nil)

func newMemoryLimiter(now func() time.Time, maxKeys int) *memoryLimiter {
	return &memoryLimiter{now: now, maxKeys: maxKeys, keys: make(map[string]*limiterState)}
}

func (m *memoryLimiter) handle() *limiterHandle {
	return &limiterHandle{lim: m}
}

func (h *limiterHandle) Take(ctx context.Context, key string, l store.Limit) (store.Decision, error) {
	if err := ctx.Err(); err != nil {
		return store.Decision{}, err
	}
	if err := l.Validate(); err != nil {
		return store.Decision{}, err
	}
	if h.closed.Load() {
		return store.Decision{}, fmt.Errorf("example-memory: handle closed: %w", store.ErrUnavailable)
	}
	return h.lim.take(key, l), nil
}

func (h *limiterHandle) Close() error {
	h.closed.Store(true)
	return nil
}

func (m *memoryLimiter) take(key string, l store.Limit) store.Decision {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now().UnixNano()
	s := m.keys[key]
	fresh := s == nil || s.alg != l.Algorithm ||
		(l.Algorithm == store.SlidingWindow && s.period != int64(l.Period))

	var (
		d                  store.Decision
		allowed            bool
		a, prev, curr, per int64
	)
	if l.Algorithm == store.TokenBucket {
		tat := now
		if !fresh {
			tat = s.a
		}
		d, a, allowed = gcra(tat, now, l)
	} else {
		var st window
		if !fresh {
			st = window{win: s.a, prev: s.prev, curr: s.curr}
		}
		var next window
		d, next, allowed = sliding(st, fresh, now, l)
		a, prev, curr = next.win, next.prev, next.curr
		per = int64(l.Period)
	}

	if s != nil {
		m.lru.MoveToFront(s.elem)
	}
	if !allowed {
		return d
	}
	if s == nil {
		if len(m.keys) == m.maxKeys {
			m.evictLRU()
		}
		k := strings.Clone(key)
		s = &limiterState{elem: m.lru.PushFront(k)}
		m.keys[k] = s
	}
	s.alg = l.Algorithm
	s.a, s.prev, s.curr, s.period = a, prev, curr, per
	return d
}

func (m *memoryLimiter) evictLRU() {
	back := m.lru.Back()
	k := back.Value.(string)
	m.lru.Remove(back)
	delete(m.keys, k)
}

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

func limiterFactory(now func() time.Time) microgateway.StoreFactory {
	return func(p microgateway.StoreParams) (store.Backend, error) {
		params := struct {
			MaxKeys int `yaml:"max_keys"`
		}{MaxKeys: defaultMaxKeys}
		if err := p.Decode(&params); err != nil {
			return nil, err
		}
		if params.MaxKeys < 1 {
			return nil, errors.New("max_keys must be at least 1")
		}
		return newMemoryLimiter(now, params.MaxKeys).handle(), nil
	}
}
