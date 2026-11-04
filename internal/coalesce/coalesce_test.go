package coalesce

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testwait"
)

var testStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type result struct {
	v   any
	err error
}

func TestLeaderAndWaiters(t *testing.T) {
	fc := clock.NewFake(testStart)
	g := New(10, 10, fc)
	c, leader, ok := g.Acquire("k")
	if !leader || !ok {
		t.Fatalf("first Acquire: leader=%v ok=%v, want leader", leader, ok)
	}
	out := make(chan result, 3)
	for range 3 {
		go func() {
			w, leader, ok := g.Acquire("k")
			if leader || !ok {
				out <- result{err: errors.New("waiter Acquire: unexpected leader or bound")}
				return
			}
			v, err := w.Wait(context.Background(), time.Hour)
			out <- result{v, err}
		}()
	}
	testwait.Until(t, "waiters parked", func() bool { return g.Waiting("k") == 3 }, nil)
	if !c.Complete("v", nil) {
		t.Fatal("Complete reported false for the first call")
	}
	for range 3 {
		r, ok := testwait.Recv(t, "waiter result", out, nil)
		if !ok {
			t.Fatal("waiter result channel closed")
		}
		if r.err != nil || r.v != "v" {
			t.Fatalf("waiter got (%v, %v), want (v, nil)", r.v, r.err)
		}
	}
	testwait.Until(t, "stats drained", func() bool { return g.Stats() == Stats{} }, nil)
}

func TestCompleteOnce(t *testing.T) {
	fc := clock.NewFake(testStart)
	g := New(10, 10, fc)
	c, _, _ := g.Acquire("k")
	w, _, _ := g.Acquire("k")
	out := make(chan result, 1)
	go func() {
		v, err := w.Wait(context.Background(), time.Hour)
		out <- result{v, err}
	}()
	testwait.Until(t, "waiter parked", func() bool { return g.Waiting("k") == 1 }, nil)
	errX := errors.New("x")
	if !c.Complete("first", nil) {
		t.Fatal("first Complete reported false")
	}
	if c.Complete("x", errX) {
		t.Fatal("second Complete reported true")
	}
	r, _ := testwait.Recv(t, "waiter result", out, nil)
	if r.err != nil || r.v != "first" {
		t.Fatalf("waiter got (%v, %v), want (first, nil)", r.v, r.err)
	}
}

func TestBounds(t *testing.T) {
	fc := clock.NewFake(testStart)
	g := New(1, 1, fc)
	a, leader, ok := g.Acquire("a")
	if !leader || !ok {
		t.Fatal("Acquire(a): want leader")
	}
	if _, _, ok := g.Acquire("b"); ok {
		t.Fatal("Acquire(b) past maxKeys granted")
	}
	w, leader, ok := g.Acquire("a")
	if leader || !ok {
		t.Fatalf("Acquire(a) waiter: leader=%v ok=%v", leader, ok)
	}
	if _, _, ok := g.Acquire("a"); ok {
		t.Fatal("second waiter past maxWaiters granted")
	}
	out := make(chan result, 1)
	go func() {
		v, err := w.Wait(context.Background(), time.Hour)
		out <- result{v, err}
	}()
	testwait.Until(t, "waiter parked", func() bool { return g.Waiting("a") == 1 }, nil)
	a.Complete("done", nil)
	testwait.Recv(t, "waiter result", out, nil)
	if _, leader, ok := g.Acquire("b"); !leader || !ok {
		t.Fatal("Acquire(b) after Complete: want leader")
	}
}

func TestBegin(t *testing.T) {
	fc := clock.NewFake(testStart)
	g := New(1, 1, fc)
	c, ok := g.Begin("k")
	if !ok {
		t.Fatal("Begin(k): want true")
	}
	if _, ok := g.Begin("k"); ok {
		t.Fatal("second Begin(k): want false")
	}
	w, leader, ok := g.Acquire("k")
	if leader || !ok || w != c {
		t.Fatalf("Acquire(k) after Begin: leader=%v ok=%v sameCall=%v", leader, ok, w == c)
	}
	if _, ok := g.Begin("other"); ok {
		t.Fatal("Begin over maxKeys: want false")
	}
	c.Complete("v", nil)
	if v, err := w.Wait(context.Background(), time.Hour); v != "v" || err != nil {
		t.Fatalf("waiter got (%v, %v), want (v, nil)", v, err)
	}
}

func TestLeadPanic(t *testing.T) {
	fc := clock.NewFake(testStart)
	g := New(10, 10, fc)
	c, _, _ := g.Acquire("k")
	w, _, _ := g.Acquire("k")
	out := make(chan result, 1)
	go func() {
		v, err := w.Wait(context.Background(), time.Hour)
		out <- result{v, err}
	}()
	testwait.Until(t, "waiter parked", func() bool { return g.Waiting("k") == 1 }, nil)
	pe := c.Lead(func() (any, error) { panic("boom") })
	if pe == nil {
		t.Fatal("Lead returned nil for a panicking fn")
	}
	if pe.Value != "boom" {
		t.Fatalf("PanicError.Value = %v, want boom", pe.Value)
	}
	if len(pe.Stack) == 0 {
		t.Fatal("PanicError.Stack is empty")
	}
	if strings.Contains(pe.Error(), "boom") {
		t.Fatalf("PanicError.Error() = %q includes the panic value", pe.Error())
	}
	r, _ := testwait.Recv(t, "waiter result", out, nil)
	var got *PanicError
	if !errors.As(r.err, &got) {
		t.Fatalf("waiter error = %v, want *PanicError", r.err)
	}
	if st := g.Stats(); st.Keys != 0 {
		t.Fatalf("Stats().Keys = %d, want 0", st.Keys)
	}
	if Protect(func() {}) != nil {
		t.Fatal("Protect of a normal return is not nil")
	}
}

func TestLeadCompletedByFn(t *testing.T) {
	fc := clock.NewFake(testStart)
	g := New(10, 10, fc)
	c, _, _ := g.Acquire("k")
	w, _, _ := g.Acquire("k")
	if pe := c.Lead(func() (any, error) {
		c.Complete("early", nil)
		return "late", nil
	}); pe != nil {
		t.Fatalf("Lead returned %v, want nil", pe)
	}
	v, err := w.Wait(context.Background(), time.Hour)
	if v != "early" || err != nil {
		t.Fatalf("waiter got (%v, %v), want (early, nil)", v, err)
	}
}

func TestWaiterContext(t *testing.T) {
	fc := clock.NewFake(testStart)
	g := New(10, 2, fc)
	c, _, _ := g.Acquire("k")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w1, _, _ := g.Acquire("k")
	w2, _, _ := g.Acquire("k")
	out1 := make(chan result, 1)
	out2 := make(chan result, 1)
	go func() {
		v, err := w1.Wait(ctx, time.Hour)
		out1 <- result{v, err}
	}()
	go func() {
		v, err := w2.Wait(context.Background(), time.Hour)
		out2 <- result{v, err}
	}()
	testwait.Until(t, "waiters parked", func() bool { return g.Waiting("k") == 2 }, nil)
	cancel()
	r1, _ := testwait.Recv(t, "cancelled waiter result", out1, nil)
	if !errors.Is(r1.err, context.Canceled) {
		t.Fatalf("cancelled waiter error = %v, want context.Canceled", r1.err)
	}
	testwait.Until(t, "cancelled waiter left", func() bool { return g.Waiting("k") == 1 }, nil)
	w3, leader, ok := g.Acquire("k")
	if leader || !ok {
		t.Fatalf("Acquire after cancelled waiter left: leader=%v ok=%v, want waiter", leader, ok)
	}
	if _, _, ok := g.Acquire("k"); ok {
		t.Fatal("Acquire past maxWaiters granted")
	}
	c.Complete("v", nil)
	r2, _ := testwait.Recv(t, "remaining waiter result", out2, nil)
	if r2.err != nil || r2.v != "v" {
		t.Fatalf("remaining waiter got (%v, %v), want (v, nil)", r2.v, r2.err)
	}
	if v, err := w3.Wait(context.Background(), time.Hour); v != "v" || err != nil {
		t.Fatalf("late waiter got (%v, %v), want (v, nil)", v, err)
	}
}

func TestWaitTimeoutBoundary(t *testing.T) {
	fc := clock.NewFake(testStart)
	g := New(10, 10, fc)
	c, _, _ := g.Acquire("k")
	w, _, _ := g.Acquire("k")
	out := make(chan result, 1)
	go func() {
		v, err := w.Wait(context.Background(), 5*time.Second)
		out <- result{v, err}
	}()
	testwait.Until(t, "waiter parked with timer armed", func() bool {
		return g.Waiting("k") == 1 && fc.Waiters() == 1
	}, nil)
	fc.Advance(5*time.Second - time.Nanosecond)
	if g.Waiting("k") != 1 {
		t.Fatalf("Waiting = %d at timeout-1ns, want 1", g.Waiting("k"))
	}
	select {
	case r := <-out:
		t.Fatalf("waiter returned at timeout-1ns: (%v, %v)", r.v, r.err)
	default:
	}
	fc.Advance(time.Nanosecond)
	r, _ := testwait.Recv(t, "timed-out waiter result", out, nil)
	if !errors.Is(r.err, ErrTimeout) {
		t.Fatalf("waiter error = %v, want ErrTimeout", r.err)
	}
	if !c.Complete("late", nil) {
		t.Fatal("leader Complete after timeout reported false")
	}
	if n := fc.Waiters(); n != 0 {
		t.Fatalf("fake clock waiters = %d after Wait returned, want 0", n)
	}
}

func TestResultBeforeTimer(t *testing.T) {
	fc := clock.NewFake(testStart)
	g := New(10, 10, fc)
	c, _, _ := g.Acquire("k")
	w, _, _ := g.Acquire("k")
	out := make(chan result, 1)
	go func() {
		v, err := w.Wait(context.Background(), 5*time.Second)
		out <- result{v, err}
	}()
	testwait.Until(t, "waiter parked with timer armed", func() bool {
		return g.Waiting("k") == 1 && fc.Waiters() == 1
	}, nil)
	c.Complete("v", nil)
	fc.Advance(5 * time.Second)
	r, _ := testwait.Recv(t, "waiter result", out, nil)
	if r.err != nil || r.v != "v" {
		t.Fatalf("waiter got (%v, %v), want (v, nil)", r.v, r.err)
	}
}
