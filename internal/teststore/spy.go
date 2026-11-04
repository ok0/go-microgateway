package teststore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ok0/go-microgateway/store"
)

var spyOps = []string{"take", "get", "put", "purge", "close"}

type SpyCall struct {
	Handle int
	Op     string
	Key    string
}

type Spy struct {
	mu      sync.Mutex
	cond    *sync.Cond
	calls   []SpyCall
	handles []*SpyHandle
	ops     map[string]opMode
	gate    chan struct{}
	parked  int
}

func NewSpy() *Spy {
	s := &Spy{ops: make(map[string]opMode), gate: make(chan struct{})}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *Spy) Wrap(rl store.RateLimiter, c store.Cache) *SpyHandle {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := &SpyHandle{spy: s, id: len(s.handles) + 1, rl: rl, c: c}
	s.handles = append(s.handles, h)
	return h
}

func (s *Spy) Calls() []SpyCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]SpyCall(nil), s.calls...)
}

func (s *Spy) Count(op string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if op == "" || c.Op == op {
			n++
		}
	}
	return n
}

func (s *Spy) CountHandle(id int, op string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if c.Handle == id && (op == "" || c.Op == op) {
			n++
		}
	}
	return n
}

func (s *Spy) Handles() []*SpyHandle {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*SpyHandle(nil), s.handles...)
}

func (s *Spy) SetMode(op string, m Mode, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if op == "" {
		for _, o := range spyOps {
			s.setOpLocked(o, m, err)
		}
		return
	}
	s.setOpLocked(op, m, err)
}

func (s *Spy) setOpLocked(op string, m Mode, err error) {
	if m == Inherit {
		delete(s.ops, op)
		return
	}
	s.ops[op] = opMode{mode: m, err: err}
}

func (s *Spy) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.gate)
	s.gate = make(chan struct{})
}

func (s *Spy) WaitBlocked(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.parked < n {
		s.cond.Wait()
	}
}

type spyPlan struct {
	mode Mode
	err  error
	gate chan struct{}
}

func (s *Spy) begin(id int, op, key string) spyPlan {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, SpyCall{Handle: id, Op: op, Key: key})
	p := spyPlan{mode: Healthy, gate: s.gate}
	if om, ok := s.ops[op]; ok {
		p.mode, p.err = om.mode, om.err
	}
	return p
}

func (s *Spy) park(ctx context.Context, op string, gate <-chan struct{}, honorContext bool) error {
	s.mu.Lock()
	s.parked++
	s.cond.Broadcast()
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.parked--
		s.cond.Broadcast()
		s.mu.Unlock()
	}()

	if !honorContext {
		<-gate
		return nil
	}
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("teststore: spy %s: %w", op, ctx.Err())
	}
}

type SpyHandle struct {
	spy    *Spy
	id     int
	rl     store.RateLimiter
	c      store.Cache
	closes atomic.Int64
}

var (
	_ store.RateLimiter = (*SpyHandle)(nil)
	_ store.Cache       = (*SpyHandle)(nil)
)

func (h *SpyHandle) ID() int {
	return h.id
}

func (h *SpyHandle) Closes() int64 {
	return h.closes.Load()
}

func spyMissing(kind string) error {
	return fmt.Errorf("teststore: spy handle has no %s: %w", kind, store.ErrUnavailable)
}

func spyInvoke[T any](ctx context.Context, h *SpyHandle, op, key string, healthy func() (T, error)) (T, error) {
	var zero T
	p := h.spy.begin(h.id, op, key)
	switch p.mode {
	case Fail:
		return zero, p.err
	case Panic:
		panic(p.err)
	case Hang:
		if err := h.spy.park(ctx, op, p.gate, true); err != nil {
			return zero, err
		}
	case HangIgnoreContext:
		_ = h.spy.park(ctx, op, p.gate, false)
	}
	return healthy()
}

func (h *SpyHandle) Take(ctx context.Context, key string, l store.Limit) (store.Decision, error) {
	return spyInvoke(ctx, h, "take", key, func() (store.Decision, error) {
		if h.rl == nil {
			return store.Decision{}, spyMissing("rate limiter")
		}
		return h.rl.Take(ctx, key, l)
	})
}

func (h *SpyHandle) Get(ctx context.Context, primary string, sel func(varyNames []string) (string, bool)) (store.Lookup, error) {
	return spyInvoke(ctx, h, "get", primary, func() (store.Lookup, error) {
		if h.c == nil {
			return store.Lookup{}, spyMissing("cache")
		}
		return h.c.Get(ctx, primary, sel)
	})
}

func (h *SpyHandle) Put(ctx context.Context, tok store.FillToken, primary, secondary string, varyNames []string, e *store.Entry, ttl time.Duration) (bool, error) {
	return spyInvoke(ctx, h, "put", primary, func() (bool, error) {
		if h.c == nil {
			return false, spyMissing("cache")
		}
		return h.c.Put(ctx, tok, primary, secondary, varyNames, e, ttl)
	})
}

func (h *SpyHandle) Purge(ctx context.Context, p store.Purge) (store.PurgeResult, error) {
	return spyInvoke(ctx, h, "purge", "", func() (store.PurgeResult, error) {
		if h.c == nil {
			return store.PurgeResult{}, spyMissing("cache")
		}
		return h.c.Purge(ctx, p)
	})
}

func (h *SpyHandle) Close() error {
	s := h.spy
	s.mu.Lock()
	s.calls = append(s.calls, SpyCall{Handle: h.id, Op: "close"})
	s.mu.Unlock()
	h.closes.Add(1)

	var errs []error
	if h.rl != nil {
		errs = append(errs, h.rl.Close())
	}
	if h.c != nil {
		errs = append(errs, h.c.Close())
	}
	return errors.Join(errs...)
}
