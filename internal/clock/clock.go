// Package clock abstracts time and randomness so that code depending on them
// can be driven deterministically in tests.
package clock

import "time"

// Clock provides the time operations used by the rest of the program.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// Since returns the time elapsed since t.
	Since(t time.Time) time.Duration
	// After returns a channel that receives the time once d has elapsed.
	After(d time.Duration) <-chan time.Time
	// NewTicker returns a Ticker that fires every d.
	NewTicker(d time.Duration) Ticker
	// AfterFunc calls f in its own goroutine once d has elapsed and returns a Timer for it.
	AfterFunc(d time.Duration, f func()) Timer
}

// Ticker delivers ticks at a fixed interval.
type Ticker interface {
	// C returns the channel on which ticks are delivered.
	C() <-chan time.Time
	// Stop turns the ticker off.
	Stop()
	// Reset stops the ticker and restarts it with interval d.
	Reset(d time.Duration)
}

// Timer runs a function once after a delay.
type Timer interface {
	// Stop prevents the timer from firing. It reports whether the timer was pending.
	Stop() bool
	// Reset changes the timer to fire after d. It reports whether the timer was pending.
	Reset(d time.Duration) bool
}

// Real returns a Clock backed by the time package.
func Real() Clock {
	return realClock{}
}

// Housekeeping returns a view of c for timers that maintain resources rather than bound an operation
// (for example idle-connection expiry). On a *Fake, the view's timers, tickers and After channels fire on
// Advance and Set like any other, but Waiters, Due, BlockUntil, BlockUntilDue and WaitDue do not count them;
// HousekeepingWaiters does. Any other clock (including nil) is returned unchanged.
func Housekeeping(c Clock) Clock {
	if f, ok := c.(*Fake); ok {
		return fakeHousekeeping{f}
	}
	return c
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) Since(t time.Time) time.Duration {
	return time.Since(t)
}

func (realClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func (realClock) NewTicker(d time.Duration) Ticker {
	return realTicker{t: time.NewTicker(d)}
}

func (realClock) AfterFunc(d time.Duration, f func()) Timer {
	return realTimer{t: time.AfterFunc(d, f)}
}

type realTicker struct {
	t *time.Ticker
}

func (r realTicker) C() <-chan time.Time {
	return r.t.C
}

func (r realTicker) Stop() {
	r.t.Stop()
}

func (r realTicker) Reset(d time.Duration) {
	r.t.Reset(d)
}

type realTimer struct {
	t *time.Timer
}

func (r realTimer) Stop() bool {
	return r.t.Stop()
}

func (r realTimer) Reset(d time.Duration) bool {
	return r.t.Reset(d)
}
