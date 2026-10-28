// Package breaker implements a rolling-window circuit breaker.
//
// State is per gateway instance and is never shared: N instances open
// independently and probe independently, so up to N × HalfOpenProbes probes
// may be in flight across the fleet. The breaker has no timers and no
// goroutines. The open-to-half-open transition is evaluated lazily on the next
// call to Allow, Ready, State or Snapshot.
package breaker

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ok0/lychgate/internal/clock"
)

// State is the position of a breaker in its state machine.
type State int32

const (
	Closed   State = 0
	HalfOpen State = 1
	Open     State = 2
)

// String returns the lowercase name of the state.
func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case HalfOpen:
		return "half_open"
	case Open:
		return "open"
	default:
		return fmt.Sprintf("State(%d)", int32(s))
	}
}

// Outcome classifies a completed request for the breaker.
type Outcome uint8

const (
	Success Outcome = iota
	Failure
	Ignored
)

// Reason explains why a transition happened.
type Reason string

const (
	ReasonRatio           Reason = "ratio"
	ReasonConsecutive     Reason = "consecutive"
	ReasonProbeFailed     Reason = "probe_failed"
	ReasonProbesSucceeded Reason = "probes_succeeded"
	ReasonTimeoutElapsed  Reason = "timeout_elapsed"
)

// Settings configures a breaker.
type Settings struct {
	Window              time.Duration
	Buckets             int
	MinRequests         int
	FailureRatio        float64
	ConsecutiveFailures int // 0 disables the consecutive rule
	OpenTimeout         time.Duration
	HalfOpenProbes      int
	SuccessThreshold    int
	// OnStateChange may be nil. It runs with the breaker lock held and must not call the same breaker.
	OnStateChange func(from, to State, reason Reason)
}

// ErrOpen is returned by Allow when a request is not admitted.
var ErrOpen = errors.New("breaker: open")

// ErrInvalidSettings wraps every settings validation failure.
var ErrInvalidSettings = errors.New("breaker: invalid settings")

// Validate reports every violated constraint, joined into one error.
func (s Settings) Validate() error {
	var errs []error
	if s.Window <= 0 {
		errs = append(errs, fmt.Errorf("%w: Window must be > 0, got %v", ErrInvalidSettings, s.Window))
	}
	if s.Buckets < 1 {
		errs = append(errs, fmt.Errorf("%w: Buckets must be >= 1, got %d", ErrInvalidSettings, s.Buckets))
	}
	if s.Window > 0 && s.Buckets >= 1 && s.Window/time.Duration(s.Buckets) < time.Millisecond {
		errs = append(errs, fmt.Errorf("%w: Window/Buckets must be >= 1ms, got %v", ErrInvalidSettings, s.Window/time.Duration(s.Buckets)))
	}
	if s.MinRequests < 1 {
		errs = append(errs, fmt.Errorf("%w: MinRequests must be >= 1, got %d", ErrInvalidSettings, s.MinRequests))
	}
	if !(s.FailureRatio > 0 && s.FailureRatio <= 1) {
		errs = append(errs, fmt.Errorf("%w: FailureRatio must be in (0, 1], got %v", ErrInvalidSettings, s.FailureRatio))
	}
	if s.ConsecutiveFailures < 0 {
		errs = append(errs, fmt.Errorf("%w: ConsecutiveFailures must be >= 0, got %d", ErrInvalidSettings, s.ConsecutiveFailures))
	}
	if s.OpenTimeout <= 0 {
		errs = append(errs, fmt.Errorf("%w: OpenTimeout must be > 0, got %v", ErrInvalidSettings, s.OpenTimeout))
	}
	if s.HalfOpenProbes < 1 {
		errs = append(errs, fmt.Errorf("%w: HalfOpenProbes must be >= 1, got %d", ErrInvalidSettings, s.HalfOpenProbes))
	}
	if s.SuccessThreshold < 1 || s.SuccessThreshold > s.HalfOpenProbes {
		errs = append(errs, fmt.Errorf("%w: SuccessThreshold must be between 1 and HalfOpenProbes (%d), got %d", ErrInvalidSettings, s.HalfOpenProbes, s.SuccessThreshold))
	}
	return errors.Join(errs...)
}

// Breaker is a circuit breaker. It is safe for concurrent use.
type Breaker struct {
	settings Settings
	clk      clock.Clock

	mu          sync.Mutex
	state       State
	epoch       uint64
	openedAt    time.Time
	consecutive int
	probes      int
	successes   int
	origin      time.Time
	width       time.Duration
	buckets     []bucket
	inflight    atomic.Int64
}

type bucket struct {
	idx      int64
	total    int64
	failures int64
}

// New returns a closed breaker. A nil clk selects the real clock.
func New(s Settings, clk clock.Clock) (*Breaker, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if clk == nil {
		clk = clock.Real()
	}
	return &Breaker{
		settings: s,
		clk:      clk,
		state:    Closed,
		origin:   clk.Now(),
		width:    s.Window / time.Duration(s.Buckets),
		buckets:  make([]bucket, s.Buckets),
	}, nil
}

// Allow admits a request or returns ErrOpen. The returned token must be completed with Done.
func (b *Breaker) Allow() (Token, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clk.Now()
	b.advance(now)
	switch b.state {
	case Closed:
		return b.issue(false), nil
	case Open:
		return Token{}, ErrOpen
	default:
		if b.probes >= b.settings.HalfOpenProbes {
			return Token{}, ErrOpen
		}
		b.probes++
		return b.issue(true), nil
	}
}

// Ready reports whether Allow would admit a request. It never transitions the breaker or takes a probe slot.
func (b *Breaker) Ready() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clk.Now()
	switch b.state {
	case Closed:
		return true
	case Open:
		return now.Sub(b.openedAt) >= b.settings.OpenTimeout
	default:
		return b.probes < b.settings.HalfOpenProbes
	}
}

// State returns the current state after applying any due transition.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advance(b.clk.Now())
	return b.state
}

// Snapshot is a point-in-time view of a breaker.
type Snapshot struct {
	State               State
	Requests            int64
	Failures            int64
	ConsecutiveFailures int
	UntilHalfOpen       time.Duration
	Probes              int
	InFlight            int64
}

// Snapshot returns the current counters after applying any due transition.
func (b *Breaker) Snapshot() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clk.Now()
	b.advance(now)
	total, failures := b.sums(now)
	snap := Snapshot{
		State:               b.state,
		Requests:            total,
		Failures:            failures,
		ConsecutiveFailures: b.consecutive,
		Probes:              b.probes,
		InFlight:            b.inflight.Load(),
	}
	if b.state == Open {
		snap.UntilHalfOpen = max(0, b.settings.OpenTimeout-now.Sub(b.openedAt))
	}
	return snap
}

// Token represents one admitted request. The zero value is valid and does nothing.
type Token struct{ t *token }

type token struct {
	b     *Breaker
	epoch uint64
	probe bool
	done  atomic.Bool
}

// Valid reports whether the token was issued by a breaker.
func (t Token) Valid() bool {
	return t.t != nil
}

// Done completes the token with the given outcome. Calls after the first are no-ops.
func (t Token) Done(o Outcome) {
	if t.t == nil || !t.t.done.CompareAndSwap(false, true) {
		return
	}
	b := t.t.b
	b.inflight.Add(-1)

	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clk.Now()
	b.advance(now)
	if t.t.epoch != b.epoch {
		return
	}

	if t.t.probe {
		b.probes--
		switch o {
		case Success:
			b.successes++
			if b.successes >= b.settings.SuccessThreshold {
				b.transition(Closed, now, ReasonProbesSucceeded)
			}
		case Failure:
			b.transition(Open, now, ReasonProbeFailed)
		}
		return
	}

	switch o {
	case Success:
		b.cur(now).total++
		b.consecutive = 0
	case Failure:
		bk := b.cur(now)
		bk.total++
		bk.failures++
		b.consecutive++
		if b.settings.ConsecutiveFailures > 0 && b.consecutive >= b.settings.ConsecutiveFailures {
			b.transition(Open, now, ReasonConsecutive)
			return
		}
		total, failures := b.sums(now)
		if total >= int64(b.settings.MinRequests) && float64(failures)/float64(total) >= b.settings.FailureRatio {
			b.transition(Open, now, ReasonRatio)
		}
	}
}

func (b *Breaker) issue(probe bool) Token {
	b.inflight.Add(1)
	return Token{t: &token{b: b, epoch: b.epoch, probe: probe}}
}

func (b *Breaker) advance(now time.Time) {
	if b.state == Open && now.Sub(b.openedAt) >= b.settings.OpenTimeout {
		b.transition(HalfOpen, now, ReasonTimeoutElapsed)
	}
}

func (b *Breaker) transition(to State, now time.Time, reason Reason) {
	from := b.state
	b.state = to
	b.epoch++
	b.probes = 0
	b.successes = 0
	b.consecutive = 0
	clear(b.buckets)
	if to == Open {
		b.openedAt = now
	}
	if b.settings.OnStateChange != nil {
		b.settings.OnStateChange(from, to, reason)
	}
}

func (b *Breaker) indexAt(now time.Time) int64 {
	return max(0, int64(now.Sub(b.origin)/b.width))
}

func (b *Breaker) cur(now time.Time) *bucket {
	idx := b.indexAt(now)
	bk := &b.buckets[idx%int64(len(b.buckets))]
	if bk.idx != idx {
		*bk = bucket{idx: idx}
	}
	return bk
}

func (b *Breaker) sums(now time.Time) (total, failures int64) {
	idx := b.indexAt(now)
	n := int64(len(b.buckets))
	for i := range b.buckets {
		bk := &b.buckets[i]
		if bk.idx > idx-n && bk.idx <= idx {
			total += bk.total
			failures += bk.failures
		}
	}
	return total, failures
}
