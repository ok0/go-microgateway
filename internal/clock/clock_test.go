package clock

import (
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/testwait"
)

var testStart = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func tryRecv(ch <-chan time.Time) (time.Time, bool) {
	select {
	case v := <-ch:
		return v, true
	default:
		return time.Time{}, false
	}
}

func TestFakeAfterFiresOnAdvance(t *testing.T) {
	f := NewFake(testStart)
	ch := f.After(2 * time.Second)

	f.Advance(time.Second)
	if _, ok := tryRecv(ch); ok {
		t.Fatal("After fired before its deadline")
	}

	f.Advance(time.Second)
	v, ok := tryRecv(ch)
	if !ok {
		t.Fatal("After did not fire at its deadline")
	}
	if want := testStart.Add(2 * time.Second); !v.Equal(want) {
		t.Fatalf("After value = %v, want %v", v, want)
	}
}

func TestFakeAdvanceOrder(t *testing.T) {
	f := NewFake(testStart)
	var order []int
	f.AfterFunc(3*time.Second, func() { order = append(order, 3) })
	f.AfterFunc(time.Second, func() { order = append(order, 1) })
	f.AfterFunc(2*time.Second, func() { order = append(order, 2) })
	ch := f.After(1500 * time.Millisecond)

	f.Advance(5 * time.Second)

	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Fatalf("callback order = %v, want [1 2 3]", order)
	}
	v, ok := tryRecv(ch)
	if !ok {
		t.Fatal("After(1500ms) did not fire")
	}
	if want := testStart.Add(1500 * time.Millisecond); !v.Equal(want) {
		t.Fatalf("After value = %v, want %v", v, want)
	}
	if want := testStart.Add(5 * time.Second); !f.Now().Equal(want) {
		t.Fatalf("Now() = %v, want %v", f.Now(), want)
	}
}

func TestFakeTicker(t *testing.T) {
	f := NewFake(testStart)
	tk := f.NewTicker(time.Second)

	for k := 1; k <= 3; k++ {
		f.Advance(time.Second)
		v, ok := tryRecv(tk.C())
		if !ok {
			t.Fatalf("tick %d not delivered", k)
		}
		if want := testStart.Add(time.Duration(k) * time.Second); !v.Equal(want) {
			t.Fatalf("tick %d value = %v, want %v", k, v, want)
		}
	}

	f.Advance(3 * time.Second)
	if _, ok := tryRecv(tk.C()); !ok {
		t.Fatal("expected one buffered tick after Advance(3s)")
	}
	if _, ok := tryRecv(tk.C()); ok {
		t.Fatal("expected at most one buffered tick after Advance(3s)")
	}

	tk.Stop()
	f.Advance(5 * time.Second)
	if _, ok := tryRecv(tk.C()); ok {
		t.Fatal("stopped ticker delivered a tick")
	}
	if n := f.Waiters(); n != 0 {
		t.Fatalf("Waiters() = %d, want 0", n)
	}
}

func TestFakeTimerStopReset(t *testing.T) {
	f := NewFake(testStart)
	fired := 0
	tm := f.AfterFunc(time.Second, func() { fired++ })

	if !tm.Stop() {
		t.Fatal("Stop() on pending timer = false, want true")
	}
	f.Advance(2 * time.Second)
	if fired != 0 {
		t.Fatal("stopped timer fired")
	}

	if tm.Reset(time.Second) {
		t.Fatal("Reset() on stopped timer = true, want false")
	}
	f.Advance(time.Second)
	if fired != 1 {
		t.Fatalf("reset timer fired %d times, want 1", fired)
	}
}

func TestFakeBlockUntil(t *testing.T) {
	f := NewFake(testStart)
	done := make(chan struct{})
	go func() {
		<-f.After(time.Second)
		close(done)
	}()

	blocked := make(chan struct{})
	go func() {
		f.BlockUntil(1)
		close(blocked)
	}()
	testwait.Done(t, "BlockUntil(1) to return", blocked, nil)
	f.Advance(time.Second)
	testwait.Done(t, "After(1s) to fire", done, nil)
}

func TestBlockUntilDue(t *testing.T) {
	f := NewFake(testStart)
	f.AfterFunc(time.Hour, func() {})
	if n := f.Due(250 * time.Millisecond); n != 0 {
		t.Fatalf("Due(250ms) with only a 1h timer = %d, want 0", n)
	}

	go f.AfterFunc(100*time.Millisecond, func() {})
	blocked := make(chan struct{})
	go func() {
		f.BlockUntilDue(1, 250*time.Millisecond)
		close(blocked)
	}()
	testwait.Done(t, "BlockUntilDue to return", blocked, nil)
	if n := f.Due(250 * time.Millisecond); n != 1 {
		t.Fatalf("Due(250ms) after BlockUntilDue = %d, want 1", n)
	}

	f.Advance(100 * time.Millisecond)
	if n := f.Due(250 * time.Millisecond); n != 0 {
		t.Fatalf("Due(250ms) after Advance(100ms) = %d, want 0", n)
	}
}

func TestFakeSet(t *testing.T) {
	f := NewFake(testStart)
	ch := f.After(5 * time.Second)

	f.Set(testStart.Add(10 * time.Second))
	v, ok := tryRecv(ch)
	if !ok {
		t.Fatal("Set forward did not fire After(5s)")
	}
	if want := testStart.Add(5 * time.Second); !v.Equal(want) {
		t.Fatalf("After value = %v, want %v", v, want)
	}

	later := f.After(time.Second)
	f.Set(testStart)
	if !f.Now().Equal(testStart) {
		t.Fatalf("Now() after Set backward = %v, want %v", f.Now(), testStart)
	}
	if _, ok := tryRecv(later); ok {
		t.Fatal("Set backward fired a pending After")
	}
}

func TestFakeSince(t *testing.T) {
	f := NewFake(testStart)
	f.Advance(3 * time.Second)
	if got := f.Since(testStart); got != 3*time.Second {
		t.Fatalf("Since(start) = %v, want 3s", got)
	}
}

func TestRandDeterministic(t *testing.T) {
	a := NewRand(42)
	b := NewRand(42)
	for i := 0; i < 100; i++ {
		x, y := a.Int64N(1000), b.Int64N(1000)
		if x != y {
			t.Fatalf("Int64N sequence diverged at %d: %d != %d", i, x, y)
		}
		if x < 0 || x >= 1000 {
			t.Fatalf("Int64N value %d out of range", x)
		}
	}
	for i := 0; i < 100; i++ {
		x, y := a.Float64(), b.Float64()
		if x != y {
			t.Fatalf("Float64 sequence diverged at %d: %v != %v", i, x, y)
		}
		if x < 0 || x >= 1 {
			t.Fatalf("Float64 value %v out of range", x)
		}
	}

	c := NewRand(43)
	d := NewRand(42)
	differs := false
	for i := 0; i < 100; i++ {
		if c.Int64N(1000) != d.Int64N(1000) {
			differs = true
			break
		}
	}
	if !differs {
		t.Fatal("NewRand(43) produced the same sequence as NewRand(42)")
	}
}

func TestRandConcurrent(t *testing.T) {
	r := NewRand(7)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				if i%2 == 0 {
					_ = r.Int64N(100)
				} else {
					_ = r.Float64()
				}
			}
		}()
	}
	wg.Wait()
}

func TestRealClock(t *testing.T) {
	// Real clock: the real implementation is under test; only firing is awaited, never its timing.
	c := Real()
	if c.Now().IsZero() {
		t.Fatal("Real().Now() is zero")
	}
	testwait.Recv(t, "Real().After to fire", c.After(time.Millisecond), nil)

	done := make(chan struct{})
	c.AfterFunc(time.Millisecond, func() { close(done) })
	testwait.Done(t, "Real().AfterFunc to run", done, nil)
}
