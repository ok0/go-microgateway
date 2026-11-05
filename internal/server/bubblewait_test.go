package server

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// bubbleGuard bounds a wait inside a synctest bubble in bubble time. Bubble time moves only when every goroutine in the
// bubble is durably blocked, so the guard fires only when the awaited event did not happen and only more time could
// bring it. It is longer than every limit a bubble test sets (at most one hour), so it never decides an outcome; it
// turns a stuck wait into a failure of that test instead of synctest's deadlock panic or an endless run of server
// timers.
const bubbleGuard = 2 * time.Hour

// bubbleRecv waits for a value from ch inside a synctest bubble. It reports false when ch is closed, and fails the test
// when the hang guard fires first.
func bubbleRecv[T any](t testing.TB, what string, ch <-chan T, state func() string) (T, bool) {
	t.Helper()
	start := time.Now()
	timer := time.NewTimer(bubbleGuard)
	defer timer.Stop()
	select {
	case v, ok := <-ch:
		return v, ok
	case <-timer.C:
	}
	s := "(none)"
	if state != nil {
		s = state()
	}
	t.Fatalf("hang guard: %s not observed after %v of bubble time\nstate: %s\ngoroutines:\n%s", what, time.Since(start), s, bubbleStacks())
	var zero T
	return zero, false
}

// bubbleDone waits inside a synctest bubble until ch is closed, under the same hang guard as bubbleRecv.
func bubbleDone(t testing.TB, what string, ch <-chan struct{}, state func() string) {
	t.Helper()
	bubbleRecv(t, what, ch, state)
}

// bubbleStacks returns the stacks of every goroutine.
func bubbleStacks() string {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}

type guardTB struct {
	testing.TB
	msg string
}

func (g *guardTB) Fatalf(format string, args ...any) {
	g.msg = fmt.Sprintf(format, args...)
}

func TestBubbleRecvGuard(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		idle := &guardTB{TB: t}
		ch := make(chan int)
		start := time.Now()
		_, ok := bubbleRecv(idle, "value", ch, func() string { return "idle" })
		if ok {
			t.Errorf("guarded receive reported a value")
		}
		if got := time.Since(start); got != bubbleGuard {
			t.Errorf("guard fired after %v, want %v", got, bubbleGuard)
		}
		const prefix = "hang guard: value not observed after 2h0m0s of bubble time\nstate: idle\ngoroutines:\n"
		if !strings.HasPrefix(idle.msg, prefix) {
			t.Errorf("guard message = %q, want prefix %q", idle.msg, prefix)
		}

		lateTB := &guardTB{TB: t}
		lateCh := make(chan int)
		go func() {
			time.Sleep(time.Hour)
			lateCh <- 7
		}()
		start = time.Now()
		v, ok := bubbleRecv(lateTB, "value", lateCh, nil)
		if v != 7 || !ok {
			t.Errorf("receive = %d, %v; want 7, true", v, ok)
		}
		if got := time.Since(start); got != time.Hour {
			t.Errorf("receive took %v, want %v", got, time.Hour)
		}
		if lateTB.msg != "" {
			t.Errorf("unexpected guard message %q", lateTB.msg)
		}

		closedTB := &guardTB{TB: t}
		closedCh := make(chan int)
		close(closedCh)
		_, ok = bubbleRecv(closedTB, "value", closedCh, nil)
		if ok {
			t.Errorf("receive from closed channel reported ok")
		}
		if closedTB.msg != "" {
			t.Errorf("unexpected guard message %q", closedTB.msg)
		}
	})
}
