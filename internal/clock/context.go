package clock

import (
	"context"
	"sync"
	"time"
)

// IsReal reports whether c is the real clock. A nil clock counts as real.
func IsReal(c Clock) bool {
	if c == nil {
		return true
	}
	_, ok := c.(realClock)
	return ok
}

// WithTimeout is WithTimeoutCause with a nil cause.
func WithTimeout(parent context.Context, c Clock, d time.Duration) (context.Context, context.CancelFunc) {
	return WithTimeoutCause(parent, c, d, nil)
}

// WithTimeoutCause returns a context that ends d after the current time of c, with cause as its cause
// (nil: context.DeadlineExceeded). The real clock uses context.WithTimeoutCause. With a non-real clock the
// context ends when d of c's time has passed; the deadline it reports is in c's time; expiry happens inside
// Advance, so the context is done when Advance returns. A parent already done at construction yields a context
// that is done on return, with the parent's error and cause and no timer. Done and Err follow the parent's
// cancellation synchronously; ending by the parent releases the timer.
func WithTimeoutCause(parent context.Context, c Clock, d time.Duration, cause error) (context.Context, context.CancelFunc) {
	if IsReal(c) {
		return context.WithTimeoutCause(parent, d, cause)
	}
	if cause == nil {
		cause = context.DeadlineExceeded
	}
	deadline := c.Now().Add(d)
	if pd, ok := parent.Deadline(); ok && pd.Before(deadline) {
		deadline = pd
	}
	inner, cancel := context.WithCancelCause(parent)
	tc := &timeoutCtx{Context: inner, deadline: deadline, done: make(chan struct{})}
	if inner.Err() != nil {
		// The parent was already done, so WithCancelCause canceled inner synchronously: the context is done on return
		// and no timer is armed.
		tc.closeDone()
		return tc, func() { cancel(nil) }
	}
	expire := func() {
		tc.mu.Lock()
		if inner.Err() == nil {
			tc.expired = true
			cancel(cause)
		}
		tc.mu.Unlock()
		tc.closeDone()
	}
	if d <= 0 {
		expire()
		return tc, func() {
			cancel(nil)
			tc.closeDone()
		}
	}
	t := c.AfterFunc(d, expire)
	// When inner ends by its parent or by cancel, close done and release the timer, as context.WithTimeout does.
	context.AfterFunc(inner, func() {
		t.Stop()
		tc.closeDone()
	})
	return tc, func() {
		t.Stop()
		cancel(nil)
		tc.closeDone()
	}
}

type timeoutCtx struct {
	context.Context
	deadline time.Time
	mu       sync.Mutex
	expired  bool
	once     sync.Once
	done     chan struct{}
}

func (tc *timeoutCtx) Deadline() (time.Time, bool) {
	return tc.deadline, true
}

// Done closes as soon as inner is seen canceled, so Done, Err and context.Cause agree without waiting
// for the AfterFunc goroutine.
func (tc *timeoutCtx) Done() <-chan struct{} {
	if tc.Context.Err() != nil {
		tc.closeDone()
	}
	return tc.done
}

func (tc *timeoutCtx) Err() error {
	if tc.Context.Err() == nil {
		return nil
	}
	tc.closeDone()
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.expired {
		return context.DeadlineExceeded
	}
	return tc.Context.Err()
}

func (tc *timeoutCtx) closeDone() {
	tc.once.Do(func() {
		close(tc.done)
	})
}

// WithoutDeadline returns a context that reports no deadline and otherwise behaves like ctx.
// It is for handing a context whose deadline is in a non-real clock's time to standard-library code
// that reads deadlines as wall time.
func WithoutDeadline(ctx context.Context) context.Context {
	return noDeadline{Context: ctx}
}

type noDeadline struct {
	context.Context
}

func (noDeadline) Deadline() (time.Time, bool) {
	return time.Time{}, false
}
