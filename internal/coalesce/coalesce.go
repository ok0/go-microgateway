package coalesce

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
)

// ErrTimeout is returned by Wait when the timeout passed before the call completed.
var ErrTimeout = errors.New("coalesce: wait timed out")

// PanicError is a panic recovered from a leader. Value is the panic value and Stack the goroutine stack at recovery.
type PanicError struct {
	Value any
	Stack []byte
}

// Error returns fixed text; it never includes Value.
func (e *PanicError) Error() string {
	return "coalesce: leader panicked"
}

// Protect runs fn and returns the recovered panic, or nil.
func Protect(fn func()) (pe *PanicError) {
	defer func() {
		if v := recover(); v != nil {
			pe = &PanicError{Value: v, Stack: debug.Stack()}
		}
	}()
	fn()
	return nil
}

// Stats describes a Group. Keys is the number of calls in flight; Waiters is the number of callers inside Wait with
// their timer armed, over all keys.
type Stats struct {
	Keys    int
	Waiters int
}

// Group shares calls among callers of the same key. It is safe for concurrent use.
type Group struct {
	mu         sync.Mutex
	clk        clock.Clock
	maxKeys    int
	maxWaiters int
	calls      map[string]*Call
	parked     int
}

// New returns a Group that holds at most maxKeys calls in flight and at most maxWaiters waiters per call. Values
// below 1 are treated as 1. clk times Wait; a nil clk is clock.Real().
func New(maxKeys, maxWaiters int, clk clock.Clock) *Group {
	if maxKeys < 1 {
		maxKeys = 1
	}
	if maxWaiters < 1 {
		maxWaiters = 1
	}
	if clk == nil {
		clk = clock.Real()
	}
	return &Group{clk: clk, maxKeys: maxKeys, maxWaiters: maxWaiters, calls: make(map[string]*Call)}
}

// Acquire joins the call in flight for key or starts one. A caller that gets leader=true must complete the call with
// Lead or Complete. A caller that gets leader=false must call Wait exactly once. ok=false means a bound is reached
// and the caller must not coalesce.
func (g *Group) Acquire(key string) (c *Call, leader, ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if existing, exists := g.calls[key]; exists {
		if existing.slots >= g.maxWaiters {
			return nil, false, false
		}
		existing.slots++
		return existing, false, true
	}
	if len(g.calls) >= g.maxKeys {
		return nil, false, false
	}
	c = &Call{g: g, key: key, done: make(chan struct{})}
	g.calls[key] = c
	return c, true, true
}

// Begin starts a call for key and makes the caller its leader only when no call for key is in flight and a key slot
// is free. It never waits.
func (g *Group) Begin(key string) (*Call, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.calls[key]; exists || len(g.calls) >= g.maxKeys {
		return nil, false
	}
	c := &Call{g: g, key: key, done: make(chan struct{})}
	g.calls[key] = c
	return c, true
}

// Stats reports the calls in flight and the parked waiters.
func (g *Group) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return Stats{Keys: len(g.calls), Waiters: g.parked}
}

// Waiting reports the parked waiters of the call in flight for key, or 0 when there is none.
func (g *Group) Waiting(key string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c, exists := g.calls[key]; exists {
		return c.parked
	}
	return 0
}

// Call is one in-flight call shared by a leader and its waiters. The mutable fields are guarded by the group's
// mutex; v and err are read only after done is closed.
type Call struct {
	g         *Group
	key       string
	done      chan struct{}
	v         any
	err       error
	completed bool
	slots     int
	parked    int
}

// Complete publishes v and err to the leader and every waiter. Only the first call has an effect; later calls report
// false.
func (c *Call) Complete(v any, err error) bool {
	g := c.g
	g.mu.Lock()
	if c.completed {
		g.mu.Unlock()
		return false
	}
	c.completed = true
	c.v, c.err = v, err
	if g.calls[c.key] == c {
		delete(g.calls, c.key)
	}
	g.mu.Unlock()
	close(c.done)
	return true
}

// Lead runs fn and completes c with its results unless c was completed earlier (for example by fn itself). A panic
// completes c with the *PanicError, which is returned; it is never re-panicked.
func (c *Call) Lead(fn func() (any, error)) *PanicError {
	var v any
	var err error
	pe := Protect(func() { v, err = fn() })
	if pe != nil {
		c.Complete(nil, pe)
		return pe
	}
	c.Complete(v, err)
	return nil
}

// Wait blocks until the call completes and returns its results. It returns ctx.Err() when ctx ends first, which
// affects only this waiter. It returns ErrTimeout once timeout has passed on the group's clock: the deadline is the
// arm time plus timeout, so a waiter is still waiting at timeout-1ns and released at timeout.
func (c *Call) Wait(ctx context.Context, timeout time.Duration) (any, error) {
	fired := make(chan struct{})
	t := c.g.clk.AfterFunc(timeout, func() { close(fired) })
	c.g.mu.Lock()
	c.parked++
	c.g.parked++
	c.g.mu.Unlock()
	defer func() {
		t.Stop()
		c.g.mu.Lock()
		c.parked--
		c.g.parked--
		c.slots--
		c.g.mu.Unlock()
	}()
	select {
	case <-c.done:
		return c.v, c.err
	default:
	}
	select {
	case <-c.done:
		return c.v, c.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-fired:
		select {
		case <-c.done:
			return c.v, c.err
		default:
			return nil, ErrTimeout
		}
	}
}
