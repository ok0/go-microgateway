package testwait

import (
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeTB struct {
	testing.TB
	deadline    time.Time
	hasDeadline bool
	mu          sync.Mutex
	msgs        []string
	at          []time.Time
}

func (f *fakeTB) Helper() {}

func (f *fakeTB) Deadline() (time.Time, bool) {
	return f.deadline, f.hasDeadline
}

func (f *fakeTB) Fatalf(format string, args ...any) {
	f.mu.Lock()
	f.msgs = append(f.msgs, fmt.Sprintf(format, args...))
	f.at = append(f.at, time.Now())
	f.mu.Unlock()
	runtime.Goexit()
}

func (f *fakeTB) failures() ([]string, []time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.msgs...), append([]time.Time(nil), f.at...)
}

type plainTB struct{ testing.TB }

func (plainTB) Helper() {}

func runWait(t *testing.T, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	Done(t, "wait under the fake TB to end", done, nil)
}

func dumpMarker(release <-chan struct{}) { <-release }

func recvParked(f testing.TB, ch <-chan int, got chan<- int) {
	f.Helper()
	if v, ok := Recv(f, "value", ch, nil); ok {
		got <- v
	}
}

func doneParked(f testing.TB, ch <-chan struct{}, got chan<- struct{}) {
	f.Helper()
	Done(f, "signal", ch, nil)
	close(got)
}

func parked(fn string) bool {
	buf := make([]byte, 1<<16)
	var n int
	for {
		n = runtime.Stack(buf, true)
		if n < len(buf) {
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	needle := "testwait." + fn + "("
	for _, block := range strings.Split(string(buf[:n]), "\n\n") {
		if !strings.Contains(block, needle) || strings.Contains(block, "time.Sleep") {
			continue
		}
		first := block
		if i := strings.IndexByte(block, '\n'); i >= 0 {
			first = block[:i]
		}
		if strings.Contains(first, "[chan receive") || strings.Contains(first, "[select") {
			return true
		}
	}
	return false
}

func expectFailures(t *testing.T, f *fakeTB, want int) ([]string, []time.Time) {
	t.Helper()
	msgs, at := f.failures()
	if len(msgs) != want {
		t.Fatalf("failures = %d, want %d: %q", len(msgs), want, msgs)
	}
	return msgs, at
}

func TestGuard(t *testing.T) {
	t.Run("ten percent grace", func(t *testing.T) {
		deadline := time.Now().Add(100 * time.Second)
		g := Guard(&fakeTB{deadline: deadline, hasDeadline: true})
		after := time.Until(deadline)
		if g > 90*time.Second || g < after-after/10 {
			t.Fatalf("Guard = %v, want between %v and 90s", g, after-after/10)
		}
	})
	t.Run("minimum grace", func(t *testing.T) {
		deadline := time.Now().Add(20 * time.Second)
		g := Guard(&fakeTB{deadline: deadline, hasDeadline: true})
		after := time.Until(deadline)
		if g > 15*time.Second || g < after-5*time.Second {
			t.Fatalf("Guard = %v, want between %v and 15s", g, after-5*time.Second)
		}
	})
	t.Run("inside the grace", func(t *testing.T) {
		if g := Guard(&fakeTB{deadline: time.Now().Add(3 * time.Second), hasDeadline: true}); g != 0 {
			t.Fatalf("Guard 3s before deadline = %v, want 0", g)
		}
		if g := Guard(&fakeTB{deadline: time.Now().Add(-1 * time.Second), hasDeadline: true}); g != 0 {
			t.Fatalf("Guard 1s after deadline = %v, want 0", g)
		}
	})
	t.Run("no deadline", func(t *testing.T) {
		if g := Guard(&fakeTB{}); g != 10*time.Minute {
			t.Fatalf("Guard without deadline = %v, want 10m", g)
		}
		if g := Guard(plainTB{}); g != 10*time.Minute {
			t.Fatalf("Guard on plain TB = %v, want 10m", g)
		}
	})
}

func TestUntilFailsOnceAfterGuard(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	go dumpMarker(release)
	deadline := time.Now().Add(5*time.Second + 300*time.Millisecond)
	f := &fakeTB{deadline: deadline, hasDeadline: true}
	var calls atomic.Int64
	runWait(t, func() {
		Until(f, "the never condition", func() bool {
			calls.Add(1)
			return false
		}, func() string { return "state-7f3a" })
	})
	msgs, at := expectFailures(t, f, 1)
	if at[0].Before(deadline.Add(-5 * time.Second)) {
		t.Fatalf("failure at %v, before the guard bound %v", at[0], deadline.Add(-5*time.Second))
	}
	for _, want := range []string{"the never condition", "state: state-7f3a", "testwait.dumpMarker("} {
		if !strings.Contains(msgs[0], want) {
			t.Fatalf("message lacks %q:\n%s", want, msgs[0])
		}
	}
	if ok, _ := regexp.MatchString("not observed after [0-9]", msgs[0]); !ok {
		t.Fatalf("message lacks elapsed time:\n%s", msgs[0])
	}
	if calls.Load() < 2 {
		t.Fatalf("cond called %d times, want at least 2", calls.Load())
	}
}

func TestUntilNilState(t *testing.T) {
	f := &fakeTB{deadline: time.Now().Add(time.Second), hasDeadline: true}
	runWait(t, func() {
		Until(f, "nil state wait", func() bool { return false }, nil)
	})
	msgs, _ := expectFailures(t, f, 1)
	for _, want := range []string{"state: (none)", "goroutine "} {
		if !strings.Contains(msgs[0], want) {
			t.Fatalf("message lacks %q:\n%s", want, msgs[0])
		}
	}
}

func TestUntilReturnsWhenTrue(t *testing.T) {
	f := &fakeTB{}
	calls := 0
	Until(f, "third call", func() bool {
		calls++
		return calls == 3
	}, nil)
	expectFailures(t, f, 0)
	if calls != 3 {
		t.Fatalf("cond called %d times, want 3", calls)
	}
}

func TestUntilAlreadyTrue(t *testing.T) {
	f := &fakeTB{deadline: time.Now().Add(time.Second), hasDeadline: true}
	calls := 0
	Until(f, "already true", func() bool {
		calls++
		return true
	}, nil)
	expectFailures(t, f, 0)
	if calls != 1 {
		t.Fatalf("cond called %d times, want 1", calls)
	}
}

func TestRecvReturnsSentValueWithoutPolling(t *testing.T) {
	f := &fakeTB{}
	ch := make(chan int)
	got := make(chan int, 1)
	go recvParked(f, ch, got)
	Until(t, "receiver to park on the channel", func() bool { return parked("recvParked") }, nil)
	ch <- 42
	v, _ := Recv(t, "received value", got, nil)
	if v != 42 {
		t.Fatalf("received %d, want 42", v)
	}
	expectFailures(t, f, 0)

	c := make(chan int)
	close(c)
	cv, ok := Recv(f, "closed", c, nil)
	if cv != 0 || ok {
		t.Fatalf("Recv on closed channel = (%d, %v), want (0, false)", cv, ok)
	}
	expectFailures(t, f, 0)
}

func TestDoneReturnsOnCloseWithoutPolling(t *testing.T) {
	f := &fakeTB{}
	ch := make(chan struct{})
	got := make(chan struct{})
	go doneParked(f, ch, got)
	Until(t, "waiter to park on the channel", func() bool { return parked("doneParked") }, nil)
	close(ch)
	Done(t, "done returned", got, nil)
	expectFailures(t, f, 0)
}

func TestRecvFailsAtGuard(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	go dumpMarker(release)
	deadline := time.Now().Add(5*time.Second + 300*time.Millisecond)
	f := &fakeTB{deadline: deadline, hasDeadline: true}
	runWait(t, func() {
		Recv(f, "a value that never comes", make(chan int), func() string { return "state-recv" })
	})
	msgs, at := expectFailures(t, f, 1)
	if at[0].Before(deadline.Add(-5 * time.Second)) {
		t.Fatalf("failure at %v, before the guard bound %v", at[0], deadline.Add(-5*time.Second))
	}
	for _, want := range []string{"a value that never comes", "state: state-recv", "testwait.dumpMarker("} {
		if !strings.Contains(msgs[0], want) {
			t.Fatalf("message lacks %q:\n%s", want, msgs[0])
		}
	}
	if ok, _ := regexp.MatchString("not observed after [0-9]", msgs[0]); !ok {
		t.Fatalf("message lacks elapsed time:\n%s", msgs[0])
	}
}

func TestDoneFailsAtGuard(t *testing.T) {
	deadline := time.Now().Add(5*time.Second + 300*time.Millisecond)
	f := &fakeTB{deadline: deadline, hasDeadline: true}
	runWait(t, func() {
		Done(f, "a signal that never comes", make(chan struct{}), nil)
	})
	msgs, at := expectFailures(t, f, 1)
	if at[0].Before(deadline.Add(-5 * time.Second)) {
		t.Fatalf("failure at %v, before the guard bound %v", at[0], deadline.Add(-5*time.Second))
	}
	for _, want := range []string{"a signal that never comes", "state: (none)", "goroutine "} {
		if !strings.Contains(msgs[0], want) {
			t.Fatalf("message lacks %q:\n%s", want, msgs[0])
		}
	}
}
