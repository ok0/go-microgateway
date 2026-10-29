// Package testwait is the one way a test waits on real time. A wait ends on an observed event or condition; its wall-clock limit is a hang guard that fails the test with a diagnostic, never a budget that decides the outcome.
package testwait

import (
	"runtime"
	"testing"
	"time"
)

const (
	noDeadlineGuard = 10 * time.Minute
	minGrace        = 5 * time.Second
	minPoll         = time.Millisecond
	maxPoll         = 50 * time.Millisecond
)

// Guard returns the hang-guard duration for a wait under t. It is the time left before the test deadline, less a grace
// period (a tenth of the remaining time, at least minGrace) so that the diagnostic is printed before the
// `go test -timeout` panic. Without a deadline, or when t has none, it returns noDeadlineGuard.
func Guard(t testing.TB) time.Duration {
	d, ok := t.(interface{ Deadline() (time.Time, bool) })
	if !ok {
		return noDeadlineGuard
	}
	deadline, has := d.Deadline()
	if !has {
		return noDeadlineGuard
	}
	left := time.Until(deadline)
	grace := max(left/10, minGrace)
	return max(left-grace, 0)
}

// Until waits until cond returns true. It polls cond with a backoff from minPoll to maxPoll. If the hang guard expires
// first, it fails t with what, the output of state (nil prints "(none)") and a goroutine dump.
func Until(t testing.TB, what string, cond func() bool, state func() string) {
	t.Helper()
	start := time.Now()
	if cond() {
		return
	}
	expired := time.NewTimer(Guard(t))
	defer expired.Stop()
	delay := minPoll
	poll := time.NewTimer(delay)
	defer poll.Stop()
	for {
		select {
		case <-expired.C:
			if cond() {
				return
			}
			fail(t, what, start, state)
			return
		case <-poll.C:
			if cond() {
				return
			}
			delay = min(2*delay, maxPoll)
			poll.Reset(delay)
		}
	}
}

// Recv waits for a value on ch. It reports false when ch is closed. If the hang guard expires first, it fails t with
// what, the output of state (nil prints "(none)") and a goroutine dump, and returns the zero value and false.
func Recv[T any](t testing.TB, what string, ch <-chan T, state func() string) (T, bool) {
	t.Helper()
	start := time.Now()
	select {
	case v, ok := <-ch:
		return v, ok
	default:
	}
	timer := time.NewTimer(Guard(t))
	defer timer.Stop()
	select {
	case v, ok := <-ch:
		return v, ok
	case <-timer.C:
	}
	fail(t, what, start, state)
	var zero T
	return zero, false
}

// Done waits until ch is closed or receives a value. If the hang guard expires first, it fails t as Recv does.
func Done(t testing.TB, what string, ch <-chan struct{}, state func() string) {
	t.Helper()
	Recv(t, what, ch, state)
}

func fail(t testing.TB, what string, start time.Time, state func() string) {
	t.Helper()
	s := "(none)"
	if state != nil {
		s = state()
	}
	t.Fatalf("hang guard: %s not observed after %v\nstate: %s\ngoroutines:\n%s", what, time.Since(start).Round(time.Millisecond), s, goroutines())
}

func goroutines() string {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}
