// Package memstore implements an in-memory store.RateLimiter.
//
// Each RateLimiter is independent of every other one. Two instances share no
// state, so N gateway instances that each hold a RateLimiter enforce N times
// the configured limit. Time is read only from the injected clock.
//
// The number of keys held by an instance is capped. Each shard keeps its keys
// in least-recently-used order and evicts the least recently used key when it
// is full; an evicted key restarts from the initial state. A janitor goroutine
// removes expired keys every JanitorInterval of clock time.
package memstore

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/store"
)

const (
	// DefaultShards is the number of shards used when RateLimiterOptions.Shards
	// is not positive.
	DefaultShards = 64
	// JanitorInterval is the clock interval between expired-key sweeps.
	JanitorInterval = 30 * time.Second
)

// RateLimiterOptions configures a RateLimiter.
type RateLimiterOptions struct {
	// MaxKeys is the maximum number of keys held at once. It must be at least 1.
	MaxKeys int
	// Shards is the number of shards. Values <= 0 select DefaultShards, and
	// values above MaxKeys are reduced to MaxKeys.
	Shards int
	// Clock supplies the time. Nil selects clock.Real().
	Clock clock.Clock
	// OnSweep, if set, is called by the janitor after each sweep with the
	// number of keys removed.
	OnSweep func(removed int)
}

// RateLimiter is an in-memory store.RateLimiter.
type RateLimiter struct {
	shards  []shard
	clk     clock.Clock
	closed  atomic.Bool
	done    chan struct{}
	once    sync.Once
	wg      sync.WaitGroup
	onSweep func(int)
}

type shard struct {
	mu   sync.Mutex
	m    map[string]*entry
	head entry
	cap  int
}

type entry struct {
	key           string
	lruPrev       *entry
	lruNext       *entry
	alg           store.Algorithm
	a, prev, curr int64
	period        int64
}

var _ store.RateLimiter = (*RateLimiter)(nil)

// NewRateLimiter returns a RateLimiter configured by o and starts its janitor.
// It panics if o.MaxKeys is less than 1. Call Close to stop the janitor.
func NewRateLimiter(o RateLimiterOptions) *RateLimiter {
	if o.MaxKeys < 1 {
		panic("memstore: MaxKeys must be >= 1")
	}
	n := o.Shards
	if n <= 0 {
		n = DefaultShards
	}
	if n > o.MaxKeys {
		n = o.MaxKeys
	}
	clk := o.Clock
	if clk == nil {
		clk = clock.Real()
	}
	r := &RateLimiter{
		shards:  make([]shard, n),
		clk:     clk,
		done:    make(chan struct{}),
		onSweep: o.OnSweep,
	}
	base, extra := o.MaxKeys/n, o.MaxKeys%n
	for i := range r.shards {
		s := &r.shards[i]
		s.cap = base
		if i < extra {
			s.cap++
		}
		s.m = make(map[string]*entry, s.cap)
		s.head.lruPrev = &s.head
		s.head.lruNext = &s.head
	}
	t := clk.NewTicker(JanitorInterval)
	r.wg.Add(1)
	go r.janitor(t)
	return r
}

// Take consumes one request for key under l if it is admitted and reports the
// decision. It follows the take order described in the store package.
func (r *RateLimiter) Take(ctx context.Context, key string, l store.Limit) (store.Decision, error) {
	if err := ctx.Err(); err != nil {
		return store.Decision{}, err
	}
	if err := l.Validate(); err != nil {
		return store.Decision{}, err
	}
	if r.closed.Load() {
		return store.Decision{}, fmt.Errorf("memstore: rate limiter closed: %w", store.ErrUnavailable)
	}
	now := r.clk.Now().UnixNano()
	s := r.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.m[key]
	fresh := e == nil || e.alg != l.Algorithm ||
		(l.Algorithm == store.SlidingWindow && e.period != int64(l.Period))

	var (
		d                  store.Decision
		allowed            bool
		a, prev, curr, per int64
	)
	if l.Algorithm == store.TokenBucket {
		tat := now
		if !fresh {
			tat = e.a
		}
		d, a, allowed = gcra(tat, now, l)
	} else {
		var st window
		if !fresh {
			st = window{win: e.a, prev: e.prev, curr: e.curr}
		}
		var next window
		d, next, allowed = sliding(st, fresh, now, l)
		a, prev, curr = next.win, next.prev, next.curr
		per = int64(l.Period)
	}

	if !allowed {
		if e != nil {
			s.moveToFront(e)
		}
		return d, nil
	}
	if e == nil {
		if len(s.m) == s.cap {
			s.evictLRU()
		}
		e = &entry{key: strings.Clone(key)}
		s.m[e.key] = e
		s.pushFront(e)
	} else {
		s.moveToFront(e)
	}
	e.alg = l.Algorithm
	e.a, e.prev, e.curr, e.period = a, prev, curr, per
	return d, nil
}

// Sweep removes every key whose state has expired at now and returns the
// number of keys removed.
func (r *RateLimiter) Sweep(now time.Time) (removed int) {
	n := now.UnixNano()
	for i := range r.shards {
		removed += r.shards[i].sweep(n)
	}
	return removed
}

// Len returns the number of keys currently held.
func (r *RateLimiter) Len() int {
	total := 0
	for i := range r.shards {
		s := &r.shards[i]
		s.mu.Lock()
		total += len(s.m)
		s.mu.Unlock()
	}
	return total
}

// Close stops the janitor and marks the limiter closed. Subsequent calls to
// Take fail with an error wrapping store.ErrUnavailable. Close is idempotent
// and always returns nil.
func (r *RateLimiter) Close() error {
	r.once.Do(func() {
		r.closed.Store(true)
		close(r.done)
		r.wg.Wait()
	})
	return nil
}

func (r *RateLimiter) janitor(t clock.Ticker) {
	defer r.wg.Done()
	defer t.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-t.C():
			n := r.Sweep(r.clk.Now())
			if r.onSweep != nil {
				r.onSweep(n)
			}
		}
	}
}

func (r *RateLimiter) shardFor(key string) *shard {
	h := uint64(14695981039346656037)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	return &r.shards[h%uint64(len(r.shards))]
}

func (s *shard) sweep(now int64) (removed int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for e := s.head.lruNext; e != &s.head; {
		next := e.lruNext
		if expired(e, now) {
			s.unlink(e)
			delete(s.m, e.key)
			removed++
		}
		e = next
	}
	return removed
}

func (s *shard) unlink(e *entry) {
	e.lruPrev.lruNext = e.lruNext
	e.lruNext.lruPrev = e.lruPrev
}

func (s *shard) pushFront(e *entry) {
	e.lruPrev = &s.head
	e.lruNext = s.head.lruNext
	s.head.lruNext.lruPrev = e
	s.head.lruNext = e
}

func (s *shard) moveToFront(e *entry) {
	s.unlink(e)
	s.pushFront(e)
}

func (s *shard) evictLRU() {
	victim := s.head.lruPrev
	s.unlink(victim)
	delete(s.m, victim.key)
}
