// Package teststore provides shared in-memory stores for tests: a
// store.RateLimiter and a store.Cache.
//
// A Shared value holds one rate-limit state and hands out distinct Handles
// over it, so several handles observe the same decisions the way several
// gateway instances sharing one backend do. Each handle supports failure,
// hang and panic injection, records its calls in the shared call log, and
// counts how often it was closed. Time is read only from the injected clock,
// so the store is hermetic.
//
// A SharedCache value does the same for a store.Cache: its handles share one
// set of entries, purges and call log, and support the same injection. Its
// entry deadlines are read from the clock given to NewSharedCache, which is
// independent of any gateway clock.
package teststore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/memstore"
	"github.com/ok0/go-microgateway/store"
)

// Mode selects how a handle behaves when Take is called.
type Mode int

const (
	// Inherit uses the mode set on the Shared value.
	Inherit Mode = iota
	// Healthy delegates to the shared memory store.
	Healthy
	// Fail returns the injected error without consuming anything.
	Fail
	// Hang parks the call until Release is called or the context is done.
	Hang
	// HangIgnoreContext parks the call until Release is called, whatever the
	// context does while it is parked.
	HangIgnoreContext
	// Panic panics with the injected error.
	Panic
)

// Call records one invocation of Take.
type Call struct {
	Handle int
	Key    string
	Limit  store.Limit
}

// Shared is one rate-limit state with any number of handles.
type Shared struct {
	mu      sync.Mutex
	cond    *sync.Cond
	rl      *memstore.RateLimiter
	calls   []Call
	handles []*Handle
	mode    Mode
	err     error
}

// NewShared returns a Shared whose state is driven by clk. Its default mode is
// Healthy.
func NewShared(clk clock.Clock) *Shared {
	s := &Shared{
		rl:   memstore.NewRateLimiter(memstore.RateLimiterOptions{MaxKeys: 1 << 20, Clock: clk}),
		mode: Healthy,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Handle returns a new handle distinct from all earlier ones. Its ID is its
// index in Handles.
func (s *Shared) Handle() *Handle {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := &Handle{s: s, id: len(s.handles), gate: make(chan struct{})}
	s.handles = append(s.handles, h)
	return h
}

// Handles returns the handles created so far, in creation order.
func (s *Shared) Handles() []*Handle {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*Handle(nil), s.handles...)
}

// Calls returns a copy of every recorded call, in call order.
func (s *Shared) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Call(nil), s.calls...)
}

// SetMode sets the mode and error used by handles whose mode is Inherit.
func (s *Shared) SetMode(m Mode, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mode = m
	s.err = err
}

// Close closes the shared memory store.
func (s *Shared) Close() error {
	return s.rl.Close()
}

// Handle is one view of a Shared state.
type Handle struct {
	s      *Shared
	id     int
	mode   Mode
	err    error
	gate   chan struct{}
	parked int
	takes  atomic.Int64
	closes atomic.Int64
	closed atomic.Bool
}

// ID returns the index of the handle within its Shared value.
func (h *Handle) ID() int {
	return h.id
}

// SetMode sets the mode and error of this handle. Inherit makes the handle
// follow the mode set on the Shared value.
func (h *Handle) SetMode(m Mode, err error) {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	h.mode = m
	h.err = err
}

// Release unblocks every call currently parked on this handle and installs a
// new gate for later calls.
func (h *Handle) Release() {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	close(h.gate)
	h.gate = make(chan struct{})
}

// WaitBlocked blocks until at least n calls are parked on this handle.
func (h *Handle) WaitBlocked(n int) {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	for h.parked < n {
		h.s.cond.Wait()
	}
}

// Takes returns the number of calls made to Take on this handle.
func (h *Handle) Takes() int64 {
	return h.takes.Load()
}

// Closes returns the number of calls made to Close on this handle.
func (h *Handle) Closes() int64 {
	return h.closes.Load()
}

// Take records the call and applies this handle's mode. Healthy calls are
// delegated to the shared memory store after the context check.
func (h *Handle) Take(ctx context.Context, key string, l store.Limit) (store.Decision, error) {
	s := h.s
	s.mu.Lock()
	s.calls = append(s.calls, Call{Handle: h.id, Key: key, Limit: l})
	h.takes.Add(1)
	if h.closed.Load() {
		s.mu.Unlock()
		return store.Decision{}, fmt.Errorf("teststore handle closed: %w", store.ErrUnavailable)
	}
	mode, err := h.mode, h.err
	if mode == Inherit {
		mode, err = s.mode, s.err
	}
	if mode == Inherit {
		mode = Healthy
	}
	gate := h.gate
	s.mu.Unlock()

	switch mode {
	case Fail:
		return store.Decision{}, err
	case Panic:
		panic(err)
	case Hang:
		if perr := h.park(ctx, gate, true); perr != nil {
			return store.Decision{}, perr
		}
	case HangIgnoreContext:
		if perr := h.park(ctx, gate, false); perr != nil {
			return store.Decision{}, perr
		}
	}

	if err := ctx.Err(); err != nil {
		return store.Decision{}, err
	}
	return s.rl.Take(ctx, key, l)
}

// Close counts the call and marks this handle closed, so later calls to Take
// fail with an error wrapping store.ErrUnavailable. The shared state is not
// closed.
func (h *Handle) Close() error {
	h.closes.Add(1)
	h.closed.Store(true)
	return nil
}

func (h *Handle) park(ctx context.Context, gate <-chan struct{}, honorContext bool) error {
	s := h.s
	s.mu.Lock()
	h.parked++
	s.cond.Broadcast()
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		h.parked--
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
		return ctx.Err()
	}
}
