package clock

import (
	"context"
	"sync"
	"time"
)

// Fake is a Clock whose time only moves when Advance or Set is called.
// Timers, tickers and After channels fire during those calls, never on their own.
type Fake struct {
	mu      sync.Mutex
	cond    *sync.Cond
	now     time.Time
	seq     uint64
	waiters []*waiter
}

type waiter struct {
	deadline time.Time
	seq      uint64
	ch       chan time.Time
	fn       func()
	period   time.Duration
}

// NewFake returns a Fake clock whose current time is start.
func NewFake(start time.Time) *Fake {
	f := &Fake{now: start}
	f.cond = sync.NewCond(&f.mu)
	return f
}

// Now returns the fake current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Since returns the fake time elapsed since t.
func (f *Fake) Since(t time.Time) time.Duration {
	return f.Now().Sub(t)
}

// After returns a channel that receives the fake time once d has been advanced past.
// A non-positive d yields a channel that already holds the current time.
func (f *Fake) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- f.now
		return ch
	}
	f.addLocked(&waiter{deadline: f.now.Add(d), ch: ch})
	return ch
}

// NewTicker returns a Ticker that delivers ticks every d of fake time.
// It panics if d is not positive.
func (f *Fake) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("non-positive interval for NewTicker")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	w := &waiter{ch: make(chan time.Time, 1), period: d, deadline: f.now.Add(d)}
	f.addLocked(w)
	return &fakeTicker{f: f, w: w}
}

// AfterFunc schedules fn to run once d of fake time has been advanced past.
// The function runs synchronously inside Advance. A non-positive d runs fn in a new goroutine
// and returns a timer that is already stopped.
func (f *Fake) AfterFunc(d time.Duration, fn func()) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{f: f, w: &waiter{fn: fn}}
	if d <= 0 {
		go fn()
		return t
	}
	t.w.deadline = f.now.Add(d)
	f.addLocked(t.w)
	return t
}

// Advance moves the fake time forward by d and fires every timer, ticker and After channel
// whose deadline is reached, in deadline order. AfterFunc callbacks run synchronously in the
// calling goroutine. A ticker delivers at most one buffered tick; further ticks are dropped
// while its channel is full.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	target := f.now.Add(d)
	for {
		w := f.nextDueLocked(target)
		if w == nil {
			break
		}
		deadline := w.deadline
		f.now = deadline
		switch {
		case w.fn != nil:
			f.removeLocked(w)
			f.mu.Unlock()
			w.fn()
			f.mu.Lock()
		case w.period > 0:
			w.deadline = deadline.Add(w.period)
			select {
			case w.ch <- deadline:
			default:
			}
		default:
			f.removeLocked(w)
			select {
			case w.ch <- deadline:
			default:
			}
		}
	}
	f.now = target
	f.cond.Broadcast()
	f.mu.Unlock()
}

// Set moves the fake time to t. Moving forward is equivalent to Advance by the difference.
// Moving backward changes the time without firing anything.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	d := t.Sub(f.now)
	f.mu.Unlock()
	if d > 0 {
		f.Advance(d)
		return
	}
	f.mu.Lock()
	f.now = t
	f.cond.Broadcast()
	f.mu.Unlock()
}

// BlockUntil blocks until at least n timers, tickers or After channels are pending.
func (f *Fake) BlockUntil(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for len(f.waiters) < n {
		f.cond.Wait()
	}
}

// Due returns the number of pending waiters whose deadline is no later than now+within.
func (f *Fake) Due(within time.Duration) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dueLocked(within)
}

// BlockUntilDue blocks until at least n pending waiters are due within the given duration of fake time.
func (f *Fake) BlockUntilDue(n int, within time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for f.dueLocked(within) < n {
		f.cond.Wait()
	}
}

// WaitDue blocks until at least n pending waiters are due within the given duration of fake time or ctx ends.
// It returns nil in the first case and ctx.Err() in the second.
func (f *Fake) WaitDue(ctx context.Context, n int, within time.Duration) error {
	stop := context.AfterFunc(ctx, func() {
		f.mu.Lock()
		f.cond.Broadcast()
		f.mu.Unlock()
	})
	defer stop()
	f.mu.Lock()
	defer f.mu.Unlock()
	for f.dueLocked(within) < n {
		if err := ctx.Err(); err != nil {
			return err
		}
		f.cond.Wait()
	}
	return nil
}

// Waiters returns the number of pending After channels, active tickers and pending AfterFunc timers.
func (f *Fake) Waiters() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.waiters)
}

func (f *Fake) addLocked(w *waiter) {
	f.seq++
	w.seq = f.seq
	f.waiters = append(f.waiters, w)
	f.cond.Broadcast()
}

func (f *Fake) removeLocked(w *waiter) bool {
	for i, x := range f.waiters {
		if x == w {
			f.waiters = append(f.waiters[:i], f.waiters[i+1:]...)
			f.cond.Broadcast()
			return true
		}
	}
	return false
}

func (f *Fake) dueLocked(within time.Duration) int {
	limit := f.now.Add(within)
	count := 0
	for _, w := range f.waiters {
		if !w.deadline.After(limit) {
			count++
		}
	}
	return count
}

func (f *Fake) nextDueLocked(target time.Time) *waiter {
	var best *waiter
	for _, w := range f.waiters {
		if w.deadline.After(target) {
			continue
		}
		if best == nil || w.deadline.Before(best.deadline) ||
			(w.deadline.Equal(best.deadline) && w.seq < best.seq) {
			best = w
		}
	}
	return best
}

type fakeTicker struct {
	f *Fake
	w *waiter
}

// C returns the channel on which fake ticks are delivered.
func (t *fakeTicker) C() <-chan time.Time {
	return t.w.ch
}

// Stop removes the ticker from the fake clock.
func (t *fakeTicker) Stop() {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	t.f.removeLocked(t.w)
}

// Reset restarts the ticker with interval d, measured from the current fake time.
// It panics if d is not positive.
func (t *fakeTicker) Reset(d time.Duration) {
	if d <= 0 {
		panic("non-positive interval for Ticker.Reset")
	}
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	t.f.removeLocked(t.w)
	t.w.period = d
	t.w.deadline = t.f.now.Add(d)
	t.f.addLocked(t.w)
}

type fakeTimer struct {
	f *Fake
	w *waiter
}

// Stop removes the timer from the fake clock. It reports whether the timer was pending.
func (t *fakeTimer) Stop() bool {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	return t.f.removeLocked(t.w)
}

// Reset reschedules the timer to fire d after the current fake time. It reports whether the timer was pending.
func (t *fakeTimer) Reset(d time.Duration) bool {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	pending := t.f.removeLocked(t.w)
	t.w.deadline = t.f.now.Add(d)
	t.f.addLocked(t.w)
	return pending
}
