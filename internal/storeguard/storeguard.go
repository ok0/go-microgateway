// Package storeguard wraps calls to a store with a circuit breaker, a bound on
// calls in flight and a per-call timeout. One guard is built per configured
// store and shared by all of its users. Time comes only from the injected clock.
package storeguard

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/ok0/go-microgateway/internal/breaker"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/store"
)

// Settings configures a Guard. ConsecutiveFailures is the number of consecutive failed calls that opens
// the breaker, OpenTimeout is how long it stays open before a probe call is allowed, and MaxInflight
// bounds the calls running at once. Every field must be positive.
type Settings struct {
	ConsecutiveFailures int
	OpenTimeout         time.Duration
	MaxInflight         int
}

// ErrInvalidSettings is wrapped by every error that Settings.Validate reports.
var ErrInvalidSettings = errors.New("storeguard: invalid settings")

// Validate reports every field of s that is out of range. It returns nil when s is valid, and otherwise
// an error joining one ErrInvalidSettings-wrapped error per invalid field.
func (s Settings) Validate() error {
	var errs []error
	if s.ConsecutiveFailures < 1 {
		errs = append(errs, fmt.Errorf("%w: ConsecutiveFailures must be >= 1, got %d", ErrInvalidSettings, s.ConsecutiveFailures))
	}
	if s.OpenTimeout <= 0 {
		errs = append(errs, fmt.Errorf("%w: OpenTimeout must be > 0, got %v", ErrInvalidSettings, s.OpenTimeout))
	}
	if s.MaxInflight < 1 {
		errs = append(errs, fmt.Errorf("%w: MaxInflight must be >= 1, got %d", ErrInvalidSettings, s.MaxInflight))
	}
	return errors.Join(errs...)
}

// Reason classifies why a guarded call did not reach the store.
type Reason string

// Reason values reported by Error.Reason.
const (
	ReasonOpen       Reason = "open"
	ReasonOverloaded Reason = "overloaded"
	ReasonTimeout    Reason = "timeout"
	ReasonPanic      Reason = "panic"
)

// Error is returned by Guard.Do when a call is refused, times out or panics before the store answers.
// It unwraps to store.ErrUnavailable.
type Error struct {
	Store  string
	Reason Reason
}

// Error returns a message that names the store and describes the Reason.
func (e *Error) Error() string {
	switch e.Reason {
	case ReasonOpen:
		return fmt.Sprintf("store %q unavailable: guard open", e.Store)
	case ReasonOverloaded:
		return fmt.Sprintf("store %q unavailable: too many calls in flight", e.Store)
	case ReasonTimeout:
		return fmt.Sprintf("store %q unavailable: call timed out", e.Store)
	case ReasonPanic:
		return fmt.Sprintf("store %q unavailable: call panicked", e.Store)
	}
	return fmt.Sprintf("store %q unavailable", e.Store)
}

// Unwrap returns store.ErrUnavailable, so errors.Is reports true for every Error.
func (e *Error) Unwrap() error { return store.ErrUnavailable }

// ShortCircuit reports whether err is an Error for a call refused without reaching the store, because
// the breaker is open or too many calls are in flight.
func ShortCircuit(err error) bool {
	var e *Error
	return errors.As(err, &e) && (e.Reason == ReasonOpen || e.Reason == ReasonOverloaded)
}

// Guard wraps calls to one store with a circuit breaker, a bound on calls in flight and a per-call timeout.
type Guard struct {
	name     string
	s        Settings
	clk      clock.Clock
	b        *breaker.Breaker
	inflight atomic.Int64
	down     atomic.Bool
	onChange func(bool)
}

// New returns a Guard for the store called name, configured by s. It returns the error from Settings.Validate
// when s is invalid. The clock clk drives the breaker and the timeouts; nil selects the real clock. onChange,
// when non-nil, is called with false when the guard becomes unavailable and with true when it recovers.
func New(name string, s Settings, clk clock.Clock, onChange func(available bool)) (*Guard, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if clk == nil {
		clk = clock.Real()
	}
	g := &Guard{name: name, s: s, clk: clk, onChange: onChange}
	b, err := breaker.New(breaker.Settings{
		Window:              time.Second,
		Buckets:             1,
		MinRequests:         math.MaxInt32,
		FailureRatio:        1,
		ConsecutiveFailures: s.ConsecutiveFailures,
		OpenTimeout:         s.OpenTimeout,
		HalfOpenProbes:      1,
		SuccessThreshold:    1,
		OnStateChange:       g.transition,
	}, clk)
	if err != nil {
		return nil, err
	}
	g.b = b
	return g, nil
}

func (g *Guard) transition(from, to breaker.State, _ breaker.Reason) {
	switch {
	case to == breaker.Open && from == breaker.Closed:
		g.down.Store(true)
		if g.onChange != nil {
			g.onChange(false)
		}
	case to == breaker.Closed:
		g.down.Store(false)
		if g.onChange != nil {
			g.onChange(true)
		}
	}
}

// Do runs fn with a context bounded by timeout and returns its result. A non-positive timeout is an error.
// Do returns an *Error without running fn when the guard is open or overloaded, when the call times out,
// and when fn panics. A timeout, a panic, or a failure with store.ErrUnavailable counts as a breaker failure;
// other errors do not. When ctx ends first, Do returns ctx's error and the call is not counted.
func (g *Guard) Do(ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if timeout <= 0 {
		return fmt.Errorf("storeguard: non-positive timeout")
	}
	if g.inflight.Add(1) > int64(g.s.MaxInflight) {
		g.inflight.Add(-1)
		return &Error{Store: g.name, Reason: ReasonOverloaded}
	}
	tok, err := g.b.Allow()
	if err != nil {
		g.inflight.Add(-1)
		return &Error{Store: g.name, Reason: ReasonOpen}
	}
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	fired := make(chan struct{})
	t := g.clk.AfterFunc(timeout, func() { close(fired) })
	go func() {
		var err error
		defer func() {
			if p := recover(); p != nil {
				err = &Error{Store: g.name, Reason: ReasonPanic}
			}
			g.inflight.Add(-1)
			done <- err
		}()
		err = fn(cctx)
	}()
	select {
	case err := <-done:
		t.Stop()
		cancel()
		var se *Error
		isPanic := errors.As(err, &se) && se.Reason == ReasonPanic
		switch {
		case err == nil:
			tok.Done(breaker.Success)
			return nil
		case ctx.Err() != nil:
			tok.Done(breaker.Ignored)
			return ctx.Err()
		case isPanic || errors.Is(err, store.ErrUnavailable):
			tok.Done(breaker.Failure)
			return err
		default:
			tok.Done(breaker.Ignored)
			return err
		}
	case <-fired:
		cancel()
		tok.Done(breaker.Failure)
		return &Error{Store: g.name, Reason: ReasonTimeout}
	case <-ctx.Done():
		t.Stop()
		cancel()
		tok.Done(breaker.Ignored)
		return ctx.Err()
	}
}

// Available reports whether the guard is not open. It reflects the last breaker transition.
func (g *Guard) Available() bool {
	return !g.down.Load()
}

// Settings returns the settings the guard was built with.
func (g *Guard) Settings() Settings {
	return g.s
}
