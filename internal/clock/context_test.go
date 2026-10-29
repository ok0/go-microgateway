package clock

import (
	"context"
	"errors"
	"testing"
	"time"
)

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestIsReal(t *testing.T) {
	if !IsReal(nil) {
		t.Error("IsReal(nil) = false, want true")
	}
	if !IsReal(Real()) {
		t.Error("IsReal(Real()) = false, want true")
	}
	if IsReal(NewFake(testStart)) {
		t.Error("IsReal(fake) = true, want false")
	}
}

func TestWithTimeoutRealClock(t *testing.T) {
	before := time.Now()
	ctx, cancel := WithTimeout(context.Background(), Real(), time.Hour)
	after := time.Now()
	defer cancel()
	dl, ok := ctx.Deadline()
	if !ok {
		t.Fatal("real clock: no deadline")
	}
	if dl.Before(before.Add(time.Hour)) || dl.After(after.Add(time.Hour)) {
		t.Errorf("real clock deadline %v outside [%v, %v]", dl, before.Add(time.Hour), after.Add(time.Hour))
	}

	errX := errors.New("x")
	ctx2, cancel2 := WithTimeoutCause(context.Background(), nil, time.Nanosecond, errX)
	defer cancel2()
	select {
	case <-ctx2.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("nil clock: context did not expire")
	}
	if err := ctx2.Err(); err != context.DeadlineExceeded {
		t.Errorf("Err = %v, want DeadlineExceeded", err)
	}
	if cause := context.Cause(ctx2); cause != errX {
		t.Errorf("Cause = %v, want %v", cause, errX)
	}
}

func TestWithTimeoutFakeExpires(t *testing.T) {
	t0 := testStart
	fk := NewFake(t0)
	errX := errors.New("x")
	ctx, cancel := WithTimeoutCause(context.Background(), fk, time.Minute, errX)
	defer cancel()

	if dl, ok := ctx.Deadline(); !ok || !dl.Equal(t0.Add(time.Minute)) {
		t.Fatalf("Deadline = %v, %v; want %v, true", dl, ok, t0.Add(time.Minute))
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("Err before expiry = %v, want nil", err)
	}
	if n := fk.Waiters(); n != 1 {
		t.Fatalf("Waiters = %d, want 1", n)
	}

	child, cc := context.WithCancel(ctx)
	defer cc()

	fk.Advance(time.Minute - 1)
	if isClosed(ctx.Done()) {
		t.Fatal("context done before its deadline")
	}

	fk.Advance(1)
	if !isClosed(ctx.Done()) {
		t.Fatal("context not done right after Advance reached the deadline")
	}
	if err := ctx.Err(); err != context.DeadlineExceeded {
		t.Errorf("Err = %v, want DeadlineExceeded", err)
	}
	if cause := context.Cause(ctx); cause != errX {
		t.Errorf("Cause = %v, want %v", cause, errX)
	}
	select {
	case <-child.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("child context did not end")
	}
	if err := child.Err(); err != context.DeadlineExceeded {
		t.Errorf("child Err = %v, want DeadlineExceeded", err)
	}

	cancel()
	if err := ctx.Err(); err != context.DeadlineExceeded {
		t.Errorf("Err after cancel = %v, want DeadlineExceeded", err)
	}
}

func TestWithTimeoutFakeCancel(t *testing.T) {
	fk := NewFake(testStart)
	ctx, cancel := WithTimeout(context.Background(), fk, time.Hour)
	cancel()
	if !isClosed(ctx.Done()) {
		t.Fatal("Done not closed right after cancel")
	}
	if err := ctx.Err(); err != context.Canceled {
		t.Fatalf("Err = %v, want Canceled", err)
	}
	if n := fk.Waiters(); n != 0 {
		t.Fatalf("Waiters = %d, want 0", n)
	}
	fk.Advance(time.Hour)
	if err := ctx.Err(); err != context.Canceled {
		t.Errorf("Err after Advance = %v, want Canceled", err)
	}
}

func TestWithTimeoutFakeParent(t *testing.T) {
	t0 := testStart
	fk := NewFake(t0)
	parent, pc := context.WithCancel(context.Background())
	ctx, cancel := WithTimeout(parent, fk, time.Minute)
	defer cancel()
	pc()
	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("context not done after parent canceled")
	}
	if err := ctx.Err(); err != context.Canceled {
		t.Fatalf("Err = %v, want Canceled", err)
	}
	fk.Advance(time.Minute)
	if err := ctx.Err(); err != context.Canceled {
		t.Errorf("Err after Advance past deadline = %v, want Canceled", err)
	}

	outer, oc := WithTimeout(context.Background(), fk, time.Minute)
	defer oc()
	nested, nc := WithTimeout(outer, fk, time.Hour)
	defer nc()
	if dl, ok := nested.Deadline(); !ok || !dl.Equal(fk.Now().Add(time.Minute)) {
		t.Errorf("nested Deadline = %v, %v; want %v, true", dl, ok, fk.Now().Add(time.Minute))
	}
}

func TestWithTimeoutFakeNonPositive(t *testing.T) {
	fk := NewFake(testStart)
	ctx, cancel := WithTimeout(context.Background(), fk, 0)
	defer cancel()
	if !isClosed(ctx.Done()) {
		t.Fatal("non-positive timeout: Done not closed")
	}
	if err := ctx.Err(); err != context.DeadlineExceeded {
		t.Errorf("Err = %v, want DeadlineExceeded", err)
	}
	if n := fk.Waiters(); n != 0 {
		t.Errorf("Waiters = %d, want 0", n)
	}
}

func TestWithoutDeadline(t *testing.T) {
	fk := NewFake(testStart)
	ctx, cancel := WithTimeout(context.Background(), fk, time.Minute)
	defer cancel()
	w := WithoutDeadline(ctx)
	if dl, ok := w.Deadline(); ok || !dl.IsZero() {
		t.Fatalf("Deadline = %v, %v; want zero, false", dl, ok)
	}
	cancel()
	select {
	case <-w.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("wrapped context not done after cancel")
	}
	if err := w.Err(); err != context.Canceled {
		t.Errorf("Err = %v, want Canceled", err)
	}
}

func TestWaitDue(t *testing.T) {
	t.Run("already due", func(t *testing.T) {
		fk := NewFake(testStart)
		fk.AfterFunc(50*time.Millisecond, func() {})
		if err := fk.WaitDue(context.Background(), 1, 100*time.Millisecond); err != nil {
			t.Errorf("WaitDue = %v, want nil", err)
		}
	})

	t.Run("becomes due", func(t *testing.T) {
		fk := NewFake(testStart)
		errc := make(chan error, 1)
		go func() {
			errc <- fk.WaitDue(context.Background(), 1, 100*time.Millisecond)
		}()
		fk.AfterFunc(50*time.Millisecond, func() {})
		select {
		case err := <-errc:
			if err != nil {
				t.Errorf("WaitDue = %v, want nil", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("WaitDue did not return after a due waiter was added")
		}
	})

	t.Run("context ends", func(t *testing.T) {
		fk := NewFake(testStart)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errc := make(chan error, 1)
		go func() {
			errc <- fk.WaitDue(ctx, 1, time.Hour)
		}()
		cancel()
		select {
		case err := <-errc:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("WaitDue = %v, want context.Canceled", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("WaitDue did not return after its context was canceled")
		}
	})
}
