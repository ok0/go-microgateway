package teststore

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/memstore"
	"github.com/ok0/go-microgateway/store"
)

// CacheCall records one invocation of a CacheHandle method. Op is get, put or
// purge. A get records the Vary names and secondary key reported to its
// selector, a put records its Vary names, Tags, TTL and whether the entry was
// stored, and a purge records the purge.
type CacheCall struct {
	Handle             int
	Op                 string
	Primary, Secondary string
	VaryNames, Tags    []string
	TTL                time.Duration
	Stored             bool
	Purge              store.Purge
}

type opMode struct {
	mode Mode
	err  error
}

// SharedCache is one response cache with any number of handles. Its entries
// live in one memstore.Cache driven by the clock given to NewSharedCache, which
// is independent of any gateway clock.
type SharedCache struct {
	mu      sync.Mutex
	cond    *sync.Cond
	mem     *memstore.Cache
	calls   []CacheCall
	handles []*CacheHandle
	mode    Mode
	err     error
	ops     map[string]opMode
}

// NewSharedCache returns a SharedCache whose entry deadlines are read from clk
// and whose byte budget is maxBytes. Its default mode is Healthy.
func NewSharedCache(clk clock.Clock, maxBytes int64) *SharedCache {
	s := &SharedCache{
		mem:  memstore.NewCache(memstore.CacheOptions{MaxBytes: maxBytes, Clock: clk}),
		mode: Healthy,
		ops:  make(map[string]opMode),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Handle returns a new handle distinct from all earlier ones. Its ID is its
// index in Handles.
func (s *SharedCache) Handle() *CacheHandle {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := &CacheHandle{s: s, id: len(s.handles), gate: make(chan struct{})}
	s.handles = append(s.handles, h)
	return h
}

// Handles returns the handles created so far, in creation order.
func (s *SharedCache) Handles() []*CacheHandle {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*CacheHandle(nil), s.handles...)
}

// Calls returns a copy of every recorded handle call, in call order. Direct
// calls to SharedCache.Purge are not recorded.
func (s *SharedCache) Calls() []CacheCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]CacheCall(nil), s.calls...)
}

// SetMode sets the mode and error used by handles whose mode is Inherit for
// every operation without a per-operation mode.
func (s *SharedCache) SetMode(m Mode, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mode = m
	s.err = err
}

// SetOpMode sets the mode and error for one operation (get, put or purge) of
// every handle whose mode is Inherit. Inherit falls back to the shared mode.
func (s *SharedCache) SetOpMode(op string, m Mode, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops[op] = opMode{mode: m, err: err}
}

// Purge applies p directly to the shared cache. It ignores the modes and is
// not recorded as a handle call.
func (s *SharedCache) Purge(ctx context.Context, p store.Purge) (store.PurgeResult, error) {
	return s.mem.Purge(ctx, p)
}

// Stats returns the state of the shared memory cache.
func (s *SharedCache) Stats() memstore.CacheStats {
	return s.mem.Stats()
}

// Close closes the shared memory cache.
func (s *SharedCache) Close() error {
	return s.mem.Close()
}

// resolve returns the mode for op on handle h: the handle mode, then the
// per-operation mode, then the shared mode, then Healthy. It must be called
// with s.mu held.
func (s *SharedCache) resolve(h *CacheHandle, op string) (Mode, error) {
	if h.mode != Inherit {
		return h.mode, h.err
	}
	if om, ok := s.ops[op]; ok && om.mode != Inherit {
		return om.mode, om.err
	}
	if s.mode != Inherit {
		return s.mode, s.err
	}
	return Healthy, nil
}

func (s *SharedCache) noteGet(idx int, names []string, secondary string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[idx].VaryNames = slices.Clone(names)
	s.calls[idx].Secondary = secondary
}

func (s *SharedCache) noteStored(idx int, stored bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[idx].Stored = stored
}

// CacheHandle is one view of a SharedCache. It implements store.Cache.
type CacheHandle struct {
	s      *SharedCache
	id     int
	mode   Mode
	err    error
	gate   chan struct{}
	parked int
	closes atomic.Int64
	closed atomic.Bool
}

// ID returns the index of the handle within its SharedCache.
func (h *CacheHandle) ID() int {
	return h.id
}

// SetMode sets the mode and error of this handle. Inherit makes the handle
// follow the per-operation and shared modes.
func (h *CacheHandle) SetMode(m Mode, err error) {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	h.mode = m
	h.err = err
}

// Release unblocks every call currently parked on this handle and installs a
// new gate for later calls.
func (h *CacheHandle) Release() {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	close(h.gate)
	h.gate = make(chan struct{})
}

// WaitBlocked blocks until at least n calls are parked on this handle.
func (h *CacheHandle) WaitBlocked(n int) {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	for h.parked < n {
		h.s.cond.Wait()
	}
}

// Closes returns the number of calls made to Close on this handle.
func (h *CacheHandle) Closes() int64 {
	return h.closes.Load()
}

// Close counts the call and marks this handle closed, so later calls fail with
// an error wrapping store.ErrUnavailable. The shared cache is not closed.
func (h *CacheHandle) Close() error {
	h.closes.Add(1)
	h.closed.Store(true)
	return nil
}

// Get looks up the variant of primary selected by sel through this handle.
func (h *CacheHandle) Get(ctx context.Context, primary string, sel func(varyNames []string) (string, bool)) (store.Lookup, error) {
	return invoke(ctx, h, CacheCall{Op: "get", Primary: primary}, func(idx int) (store.Lookup, error) {
		next := sel
		if sel != nil {
			next = func(names []string) (string, bool) {
				secondary, ok := sel(names)
				h.s.noteGet(idx, names, secondary)
				return secondary, ok
			}
		}
		return h.s.mem.Get(ctx, primary, next)
	})
}

// Put stores e through this handle. The result reports whether the entry was
// stored.
func (h *CacheHandle) Put(ctx context.Context, tok store.FillToken, primary, secondary string, varyNames []string, e *store.Entry, ttl time.Duration) (bool, error) {
	c := CacheCall{Op: "put", Primary: primary, Secondary: secondary, VaryNames: slices.Clone(varyNames), TTL: ttl}
	if e != nil {
		c.Tags = slices.Clone(e.Tags)
	}
	return invoke(ctx, h, c, func(idx int) (bool, error) {
		stored, err := h.s.mem.Put(ctx, tok, primary, secondary, varyNames, e, ttl)
		h.s.noteStored(idx, stored)
		return stored, err
	})
}

// Purge applies p through this handle.
func (h *CacheHandle) Purge(ctx context.Context, p store.Purge) (store.PurgeResult, error) {
	return invoke(ctx, h, CacheCall{Op: "purge", Purge: p}, func(int) (store.PurgeResult, error) {
		return h.s.mem.Purge(ctx, p)
	})
}

type plan struct {
	idx    int
	mode   Mode
	err    error
	gate   chan struct{}
	closed bool
}

// begin records c, then reports whether the handle is closed and which mode
// applies to the operation.
func (h *CacheHandle) begin(c CacheCall) plan {
	s := h.s
	s.mu.Lock()
	defer s.mu.Unlock()
	c.Handle = h.id
	p := plan{idx: len(s.calls), gate: h.gate}
	s.calls = append(s.calls, c)
	if h.closed.Load() {
		p.closed = true
		return p
	}
	p.mode, p.err = s.resolve(h, c.Op)
	return p
}

// invoke applies the mode of the operation described by c and, when the call
// is not failed or panicked, runs healthy with the index of the recorded call.
func invoke[T any](ctx context.Context, h *CacheHandle, c CacheCall, healthy func(idx int) (T, error)) (T, error) {
	var zero T
	p := h.begin(c)
	if p.closed {
		return zero, fmt.Errorf("teststore cache handle closed: %w", store.ErrUnavailable)
	}
	switch p.mode {
	case Fail:
		return zero, p.err
	case Panic:
		panic(p.err)
	case Hang:
		if err := h.park(ctx, p.gate, true); err != nil {
			return zero, err
		}
	case HangIgnoreContext:
		_ = h.park(ctx, p.gate, false)
	}
	return healthy(p.idx)
}

func (h *CacheHandle) park(ctx context.Context, gate <-chan struct{}, honorContext bool) error {
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
