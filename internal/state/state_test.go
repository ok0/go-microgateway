package state

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

type res struct {
	closed atomic.Int32
	err    error
}

func (r *res) Close() error {
	r.closed.Add(1)
	return r.err
}

type other struct{}

func counting(n *int, err error) func() (*res, error) {
	return func() (*res, error) {
		*n++
		return &res{err: err}, nil
	}
}

func TestReuseUnchanged(t *testing.T) {
	r := NewRegistry(nil)
	var creates int
	tx1 := r.Begin()
	v1, err := Get(tx1, "k", "a", "f1", counting(&creates, nil))
	if err != nil {
		t.Fatal(err)
	}
	l1 := tx1.Commit()

	tx2 := r.Begin()
	v2, err := Get(tx2, "k", "a", "f1", counting(&creates, nil))
	if err != nil {
		t.Fatal(err)
	}
	if v2 != v1 {
		t.Fatal("unchanged component was not reused")
	}
	if creates != 1 {
		t.Fatalf("create called %d times, want 1", creates)
	}
	l2 := tx2.Commit()

	if err := l1.Release(); err != nil {
		t.Fatal(err)
	}
	if v1.closed.Load() != 0 {
		t.Fatalf("reused component closed %d times, want 0", v1.closed.Load())
	}
	if r.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", r.Len())
	}
	if err := l2.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestRecreateChanged(t *testing.T) {
	r := NewRegistry(nil)
	var creates int
	tx1 := r.Begin()
	old, err := Get(tx1, "k", "a", "f1", counting(&creates, nil))
	if err != nil {
		t.Fatal(err)
	}
	l1 := tx1.Commit()

	tx2 := r.Begin()
	fresh, err := Get(tx2, "k", "a", "f2", counting(&creates, nil))
	if err != nil {
		t.Fatal(err)
	}
	if fresh == old {
		t.Fatal("changed component was reused")
	}
	l2 := tx2.Commit()
	if old.closed.Load() != 0 {
		t.Fatalf("superseded component closed during Commit while leased: %d", old.closed.Load())
	}

	if err := l1.Release(); err != nil {
		t.Fatal(err)
	}
	if old.closed.Load() != 1 {
		t.Fatalf("old closed %d times, want 1", old.closed.Load())
	}
	if fresh.closed.Load() != 0 {
		t.Fatalf("new closed %d times, want 0", fresh.closed.Load())
	}
	if err := l2.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestUnreferencedClosedAfterDrain(t *testing.T) {
	r := NewRegistry(nil)
	var creates int
	tx1 := r.Begin()
	a, err := Get(tx1, "k", "a", "f1", counting(&creates, nil))
	if err != nil {
		t.Fatal(err)
	}
	l1 := tx1.Commit()

	tx2 := r.Begin()
	l2 := tx2.Commit()
	if r.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", r.Len())
	}
	if a.closed.Load() != 0 {
		t.Fatalf("leased component closed during Commit: %d", a.closed.Load())
	}

	if err := l1.Release(); err != nil {
		t.Fatal(err)
	}
	if a.closed.Load() != 1 {
		t.Fatalf("closed %d times, want 1", a.closed.Load())
	}
	if err := l1.Release(); err != nil {
		t.Fatal(err)
	}
	if a.closed.Load() != 1 {
		t.Fatalf("closed %d times after second Release, want 1", a.closed.Load())
	}
	if err := l2.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestOutOfOrderRelease(t *testing.T) {
	r := NewRegistry(nil)
	var creates int
	tx1 := r.Begin()
	a, err := Get(tx1, "k", "a", "f1", counting(&creates, nil))
	if err != nil {
		t.Fatal(err)
	}
	l1 := tx1.Commit()

	tx2 := r.Begin()
	if _, err := Get(tx2, "k", "a", "f1", counting(&creates, nil)); err != nil {
		t.Fatal(err)
	}
	l2 := tx2.Commit()

	tx3 := r.Begin()
	l3 := tx3.Commit()

	if err := l2.Release(); err != nil {
		t.Fatal(err)
	}
	if a.closed.Load() != 0 {
		t.Fatalf("closed %d times after first release, want 0", a.closed.Load())
	}
	if err := l1.Release(); err != nil {
		t.Fatal(err)
	}
	if a.closed.Load() != 1 {
		t.Fatalf("closed %d times, want 1", a.closed.Load())
	}
	if err := l3.Release(); err != nil {
		t.Fatal(err)
	}
	if a.closed.Load() != 1 {
		t.Fatalf("closed %d times, want 1", a.closed.Load())
	}
}

func TestAbortClosesOnlyCreated(t *testing.T) {
	r := NewRegistry(nil)
	var creates int
	tx1 := r.Begin()
	a, err := Get(tx1, "k", "a", "f1", counting(&creates, nil))
	if err != nil {
		t.Fatal(err)
	}
	_ = tx1.Commit()

	tx2 := r.Begin()
	reused, err := Get(tx2, "k", "a", "f1", counting(&creates, nil))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Get(tx2, "k", "b", "f1", counting(&creates, nil))
	if err != nil {
		t.Fatal(err)
	}
	if reused != a {
		t.Fatal("registered component was not reused")
	}
	tx2.Abort()

	if b.closed.Load() != 1 {
		t.Fatalf("created component closed %d times, want 1", b.closed.Load())
	}
	if a.closed.Load() != 0 {
		t.Fatalf("registered component closed %d times, want 0", a.closed.Load())
	}
	if r.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", r.Len())
	}

	tx3 := r.Begin()
	again, err := Get(tx3, "k", "a", "f1", counting(&creates, nil))
	if err != nil {
		t.Fatal(err)
	}
	if again != a {
		t.Fatal("registered component changed after Abort")
	}
	tx3.Abort()
}

func TestCloseErrorDoesNotFailCommit(t *testing.T) {
	t.Run("release reports", func(t *testing.T) {
		r := NewRegistry(nil)
		tx1 := r.Begin()
		if _, err := Get(tx1, "k", "a", "f1", func() (*res, error) {
			return &res{err: errors.New("boom")}, nil
		}); err != nil {
			t.Fatal(err)
		}
		l1 := tx1.Commit()

		tx2 := r.Begin()
		l2 := tx2.Commit()
		if l2 == nil {
			t.Fatal("Commit returned nil lease")
		}

		err := l1.Release()
		if err == nil {
			t.Fatal("Release returned nil, want close error")
		}
		if !strings.Contains(err.Error(), "boom") || !strings.Contains(err.Error(), `"a"`) {
			t.Fatalf("Release error = %q, want it to name a and boom", err)
		}
		if err := l2.Release(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("commit logs", func(t *testing.T) {
		var buf bytes.Buffer
		r := NewRegistry(slog.New(slog.NewJSONHandler(&buf, nil)))
		tx1 := r.Begin()
		if _, err := Get(tx1, "k", "a", "f1", func() (*res, error) {
			return &res{err: errors.New("boom")}, nil
		}); err != nil {
			t.Fatal(err)
		}
		l1 := tx1.Commit()
		if err := l1.Release(); err != nil {
			t.Fatal(err)
		}

		tx2 := r.Begin()
		l2 := tx2.Commit()
		if l2 == nil {
			t.Fatal("Commit returned nil lease")
		}
		out := buf.String()
		if !strings.Contains(out, "state component close failed") || !strings.Contains(out, "boom") {
			t.Fatalf("log = %q, want close failure with boom", out)
		}
	})
}

func TestBeginWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRegistry(nil)
		tx1 := r.Begin()
		var acquired atomic.Bool
		go func() {
			tx := r.Begin()
			acquired.Store(true)
			tx.Abort()
		}()
		synctest.Wait()
		if acquired.Load() {
			t.Fatal("second Begin did not wait for the open transaction")
		}
		_ = tx1.Commit()
		synctest.Wait()
		if !acquired.Load() {
			t.Fatal("second Begin did not proceed after Commit")
		}
	})
}

func TestTypeMismatch(t *testing.T) {
	r := NewRegistry(nil)
	var creates int
	tx1 := r.Begin()
	if _, err := Get(tx1, "k", "a", "f1", counting(&creates, nil)); err != nil {
		t.Fatal(err)
	}
	_ = tx1.Commit()

	tx2 := r.Begin()
	got, err := Get(tx2, "k", "a", "f1", func() (*other, error) {
		return &other{}, nil
	})
	if !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("err = %v, want ErrTypeMismatch", err)
	}
	if got != nil {
		t.Fatal("mismatch returned a non-zero value")
	}

	if _, err := Get(tx2, "k", "b", "f1", counting(&creates, nil)); err != nil {
		t.Fatal(err)
	}
	_, err = Get(tx2, "k", "b", "f2", counting(&creates, nil))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	tx2.Abort()
}

func TestGetAfterDone(t *testing.T) {
	r := NewRegistry(nil)
	var creates int

	tx1 := r.Begin()
	_ = tx1.Commit()
	if _, err := Get(tx1, "k", "a", "f1", counting(&creates, nil)); !errors.Is(err, ErrDone) {
		t.Fatalf("after Commit: err = %v, want ErrDone", err)
	}

	tx2 := r.Begin()
	tx2.Abort()
	if _, err := Get(tx2, "k", "a", "f1", counting(&creates, nil)); !errors.Is(err, ErrDone) {
		t.Fatalf("after Abort: err = %v, want ErrDone", err)
	}
}

func TestCreateError(t *testing.T) {
	r := NewRegistry(nil)
	want := errors.New("create failed")
	tx := r.Begin()
	_, err := Get(tx, "k", "a", "f1", func() (*res, error) {
		return nil, want
	})
	if err != want {
		t.Fatalf("err = %v, want the create error verbatim", err)
	}
	_ = tx.Commit()
	if r.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", r.Len())
	}
}

func TestRegistryClose(t *testing.T) {
	r := NewRegistry(nil)
	var creates int

	tx1 := r.Begin()
	a, err := Get(tx1, "k", "a", "f1", counting(&creates, nil))
	if err != nil {
		t.Fatal(err)
	}
	l1 := tx1.Commit()

	tx2 := r.Begin()
	if _, err := Get(tx2, "k", "a", "f1", counting(&creates, nil)); err != nil {
		t.Fatal(err)
	}
	b, err := Get(tx2, "k", "b", "f1", counting(&creates, nil))
	if err != nil {
		t.Fatal(err)
	}
	l2 := tx2.Commit()

	if err := l2.Release(); err != nil {
		t.Fatal(err)
	}
	if b.closed.Load() != 0 {
		t.Fatalf("b closed %d times before Close, want 0", b.closed.Load())
	}

	r.Close()
	if b.closed.Load() != 1 {
		t.Fatalf("b closed %d times after Close, want 1", b.closed.Load())
	}
	if a.closed.Load() != 0 {
		t.Fatalf("a closed %d times while leased, want 0", a.closed.Load())
	}
	if r.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", r.Len())
	}

	if err := l1.Release(); err != nil {
		t.Fatal(err)
	}
	if a.closed.Load() != 1 {
		t.Fatalf("a closed %d times after its lease released, want 1", a.closed.Load())
	}
}
