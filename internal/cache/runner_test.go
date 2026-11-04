package cache

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/ok0/go-microgateway/internal/testwait"
)

// held returns a refresh that returns once gate is closed or its context ends.
func held(gate <-chan struct{}) func(ctx context.Context) {
	return func(ctx context.Context) {
		select {
		case <-gate:
		case <-ctx.Done():
		}
	}
}

func noop(context.Context) {}

func TestRunnerBounds(t *testing.T) {
	t.Run("a full runner starts nothing", func(t *testing.T) {
		r := NewRunner(1)
		t.Cleanup(func() { _ = r.Close() })
		gate := make(chan struct{})
		if !r.Go("a", held(gate)) {
			t.Fatal("first Go refused")
		}
		if r.Go("b", noop) {
			t.Fatal("Go accepted a second refresh on a full runner")
		}
		close(gate)
		testwait.Until(t, "runner idle", func() bool { return r.Running() == 0 }, nil)
		if !r.Go("b", noop) {
			t.Fatal("Go refused after the runner went idle")
		}
	})
	t.Run("one refresh per key", func(t *testing.T) {
		r := NewRunner(2)
		t.Cleanup(func() { _ = r.Close() })
		gate := make(chan struct{})
		t.Cleanup(func() { close(gate) })
		if !r.Go("a", held(gate)) {
			t.Fatal("first Go refused")
		}
		if r.Go("a", noop) {
			t.Fatal("Go accepted a second refresh for a key in flight")
		}
	})
}

func TestRunnerClose(t *testing.T) {
	base := runtime.NumGoroutine()
	r := NewRunner(2)
	var returned atomic.Bool
	if !r.Go("a", func(ctx context.Context) {
		<-ctx.Done()
		returned.Store(true)
	}) {
		t.Fatal("Go refused")
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !returned.Load() {
		t.Fatal("Close returned before the refresh returned")
	}
	if r.Go("b", noop) {
		t.Fatal("Go started a refresh after Close")
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	testwait.Until(t, "goroutines settled", func() bool { return runtime.NumGoroutine() <= base }, nil)
}

func TestScopeSharesRunnerBounds(t *testing.T) {
	t.Run("the runner bound is shared", func(t *testing.T) {
		r := NewRunner(1)
		t.Cleanup(func() { _ = r.Close() })
		a, b := r.Scope(), r.Scope()
		gate := make(chan struct{})
		if !a.Go("k", held(gate)) {
			t.Fatal("scope a refused its first refresh")
		}
		if b.Go("x", noop) {
			t.Fatal("scope b started a refresh on a full runner")
		}
		if n := b.Running(); n != 0 {
			t.Fatalf("scope b running %d, want 0", n)
		}
		close(gate)
		testwait.Until(t, "runner idle", func() bool { return r.Running() == 0 }, nil)
	})
	t.Run("the key bound is shared", func(t *testing.T) {
		r := NewRunner(2)
		t.Cleanup(func() { _ = r.Close() })
		a, b := r.Scope(), r.Scope()
		gate := make(chan struct{})
		if !a.Go("k", held(gate)) {
			t.Fatal("scope a refused its first refresh")
		}
		if b.Go("k", noop) {
			t.Fatal("scope b started a second refresh for a key in flight in scope a")
		}
		close(gate)
		testwait.Until(t, "runner idle", func() bool { return r.Running() == 0 }, nil)
	})
}

func TestScopeRetireIdle(t *testing.T) {
	r := NewRunner(1)
	t.Cleanup(func() { _ = r.Close() })
	s := r.Scope()
	var called atomic.Bool
	f := func() { called.Store(true) }
	if s.Retire(f) {
		t.Fatal("Retire of an idle scope reported running refreshes")
	}
	if s.Go("k", noop) {
		t.Fatal("Go started a refresh on a retired scope")
	}
	if s.Retire(f) {
		t.Fatal("second Retire reported running refreshes")
	}
	if called.Load() {
		t.Fatal("onIdle ran for a scope with no running refresh")
	}
}

func TestScopeRetireWaits(t *testing.T) {
	r := NewRunner(2)
	t.Cleanup(func() { _ = r.Close() })
	s := r.Scope()
	gate := make(chan struct{})
	var returned atomic.Bool
	errc := make(chan error, 1)
	if !s.Go("k", func(ctx context.Context) {
		select {
		case <-gate:
		case <-ctx.Done():
		}
		errc <- ctx.Err()
		returned.Store(true)
	}) {
		t.Fatal("Go refused")
	}
	testwait.Until(t, "refresh reserved", func() bool { return s.Running() == 1 }, nil)
	var idleCalls atomic.Int32
	idle := make(chan struct{})
	if !s.Retire(func() {
		idleCalls.Add(1)
		close(idle)
	}) {
		t.Fatal("Retire reported no running refresh")
	}
	if s.Go("j", noop) {
		t.Fatal("Go started a refresh on a retired scope")
	}
	select {
	case <-idle:
		t.Fatal("onIdle ran while a refresh was running")
	default:
	}
	close(gate)
	testwait.Done(t, "onIdle called", idle, nil)
	if !returned.Load() {
		t.Fatal("onIdle ran before the refresh returned")
	}
	err := <-errc
	if err != nil {
		t.Fatalf("refresh context ended with %v before the scope was idle", err)
	}
	if n := idleCalls.Load(); n != 1 {
		t.Fatalf("onIdle called %d times, want 1", n)
	}
	if n := s.Running(); n != 0 {
		t.Fatalf("scope running %d, want 0", n)
	}
}

func TestScopeCancel(t *testing.T) {
	r := NewRunner(4)
	t.Cleanup(func() { _ = r.Close() })
	a, b := r.Scope(), r.Scope()
	aerr, berr := make(chan error, 1), make(chan error, 1)
	if !a.Go("ka", func(ctx context.Context) {
		<-ctx.Done()
		aerr <- ctx.Err()
	}) {
		t.Fatal("scope a refused its refresh")
	}
	if !b.Go("kb", func(ctx context.Context) {
		<-ctx.Done()
		berr <- ctx.Err()
	}) {
		t.Fatal("scope b refused its refresh")
	}
	a.Cancel()
	err, _ := testwait.Recv(t, "cancelled refresh of scope a returned", aerr, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("scope a refresh context ended with %v, want canceled", err)
	}
	if n := b.Running(); n != 1 {
		t.Fatalf("scope b running %d, want 1", n)
	}
	if a.Go("x", noop) {
		t.Fatal("Go started a refresh on a cancelled scope")
	}
	if !b.Go("other", noop) {
		t.Fatal("scope b refused a refresh while not cancelled")
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	err, _ = testwait.Recv(t, "refresh of scope b ended by runner close", berr, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("scope b refresh context ended with %v, want canceled", err)
	}
	if a.Retire(func() { t.Error("onIdle ran for a cancelled idle scope") }) {
		t.Fatal("Retire of a cancelled idle scope reported running refreshes")
	}
}

func TestScopeIdleClosesRunner(t *testing.T) {
	settled := func(t *testing.T, r *Runner, s *Scope, base int) {
		t.Helper()
		if n := r.Running(); n != 0 {
			t.Fatalf("runner running %d, want 0", n)
		}
		if r.Go("x", noop) {
			t.Fatal("Go started a refresh on a closed runner")
		}
		if s.Go("y", noop) {
			t.Fatal("Go started a refresh on a retired scope")
		}
		testwait.Until(t, "goroutines settled", func() bool { return runtime.NumGoroutine() <= base }, nil)
	}
	t.Run("retire then release", func(t *testing.T) {
		base := runtime.NumGoroutine()
		r := NewRunner(1)
		s := r.Scope()
		started := make(chan struct{})
		gate := make(chan struct{})
		closed := make(chan error, 1)
		if !s.Go("k", func(context.Context) {
			close(started)
			<-gate
		}) {
			t.Fatal("Go refused")
		}
		testwait.Done(t, "refresh started", started, nil)
		if !s.Retire(func() { closed <- r.Close() }) {
			t.Fatal("Retire reported no running refresh")
		}
		close(gate)
		err, _ := testwait.Recv(t, "idle callback closed the runner", closed, nil)
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
		settled(t, r, s, base)
	})
	t.Run("cancel then retire", func(t *testing.T) {
		base := runtime.NumGoroutine()
		r := NewRunner(1)
		s := r.Scope()
		started := make(chan struct{})
		gate := make(chan struct{})
		cancelled := make(chan error, 1)
		closed := make(chan error, 1)
		if !s.Go("k", func(ctx context.Context) {
			close(started)
			<-ctx.Done()
			cancelled <- ctx.Err()
			<-gate
		}) {
			t.Fatal("Go refused")
		}
		testwait.Done(t, "refresh started", started, nil)
		s.Cancel()
		err, _ := testwait.Recv(t, "refresh context cancelled", cancelled, nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("refresh context ended with %v, want canceled", err)
		}
		if !s.Retire(func() { closed <- r.Close() }) {
			t.Fatal("Retire reported no running refresh")
		}
		close(gate)
		err, _ = testwait.Recv(t, "idle callback closed the runner", closed, nil)
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
		settled(t, r, s, base)
	})
	t.Run("runner closed from outside", func(t *testing.T) {
		base := runtime.NumGoroutine()
		r := NewRunner(1)
		s := r.Scope()
		started := make(chan struct{})
		closed := make(chan error, 1)
		if !s.Go("k", func(ctx context.Context) {
			close(started)
			<-ctx.Done()
		}) {
			t.Fatal("Go refused")
		}
		testwait.Done(t, "refresh started", started, nil)
		if !s.Retire(func() { closed <- r.Close() }) {
			t.Fatal("Retire reported no running refresh")
		}
		ext := make(chan error, 1)
		go func() { ext <- r.Close() }()
		err, _ := testwait.Recv(t, "external Close returned", ext, nil)
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
		err, _ = testwait.Recv(t, "idle callback closed the runner", closed, nil)
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
		settled(t, r, s, base)
	})
}
