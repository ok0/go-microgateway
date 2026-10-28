package storeguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/store"
)

var testSettings = Settings{ConsecutiveFailures: 3, OpenTimeout: 2 * time.Second, MaxInflight: 4}

const callTimeout = time.Minute

func newFake() *clock.Fake {
	return clock.NewFake(time.Unix(1_700_000_000, 0))
}

func newGuard(t *testing.T, s Settings, f *clock.Fake, onChange func(bool)) *Guard {
	t.Helper()
	g, err := New("db", s, f, onChange)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func failWith(g *Guard, err error) error {
	return g.Do(context.Background(), callTimeout, func(context.Context) error { return err })
}

func tripGuard(t *testing.T, g *Guard) {
	t.Helper()
	for range g.s.ConsecutiveFailures {
		if err := failWith(g, store.ErrUnavailable); !errors.Is(err, store.ErrUnavailable) {
			t.Fatalf("failure returned %v", err)
		}
	}
	if g.Available() {
		t.Fatal("guard still available after tripping")
	}
}

func expectOpenReject(t *testing.T, g *Guard) {
	t.Helper()
	var calls atomic.Int64
	err := g.Do(context.Background(), callTimeout, func(context.Context) error {
		calls.Add(1)
		return nil
	})
	var se *Error
	if !errors.As(err, &se) || se.Reason != ReasonOpen {
		t.Fatalf("Do() = %v, want ReasonOpen", err)
	}
	if calls.Load() != 0 {
		t.Fatal("fn was called while the guard rejected")
	}
}

func blockOn(gate <-chan error) func(context.Context) error {
	return func(context.Context) error { return <-gate }
}

func waitInflight(t *testing.T, g *Guard, want int64) {
	t.Helper()
	for range 10000 {
		if g.inflight.Load() == want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("inflight = %d, want %d", g.inflight.Load(), want)
}

func TestSettingsValidate(t *testing.T) {
	bad := []Settings{
		{ConsecutiveFailures: 0, OpenTimeout: time.Second, MaxInflight: 1},
		{ConsecutiveFailures: -1, OpenTimeout: time.Second, MaxInflight: 1},
		{ConsecutiveFailures: 1, OpenTimeout: 0, MaxInflight: 1},
		{ConsecutiveFailures: 1, OpenTimeout: -time.Second, MaxInflight: 1},
		{ConsecutiveFailures: 1, OpenTimeout: time.Second, MaxInflight: 0},
		{ConsecutiveFailures: 1, OpenTimeout: time.Second, MaxInflight: -1},
	}
	for _, s := range bad {
		if err := s.Validate(); !errors.Is(err, ErrInvalidSettings) {
			t.Errorf("Validate(%+v) = %v, want ErrInvalidSettings", s, err)
		}
		if _, err := New("db", s, newFake(), nil); !errors.Is(err, ErrInvalidSettings) {
			t.Errorf("New(%+v) error = %v, want ErrInvalidSettings", s, err)
		}
	}
	if err := (Settings{ConsecutiveFailures: 1, OpenTimeout: time.Nanosecond, MaxInflight: 1}).Validate(); err != nil {
		t.Fatalf("Validate() of minimal valid settings = %v", err)
	}
}

func TestNewNilClockAndAccessors(t *testing.T) {
	g, err := New("db", testSettings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !g.Available() {
		t.Fatal("new guard is not available")
	}
	if g.Settings() != testSettings {
		t.Fatalf("Settings() = %+v, want %+v", g.Settings(), testSettings)
	}
}

func TestTripAtConsecutiveFailures(t *testing.T) {
	f := newFake()
	var changes []bool
	g := newGuard(t, testSettings, f, func(a bool) { changes = append(changes, a) })
	for range 2 {
		if err := failWith(g, store.ErrUnavailable); !errors.Is(err, store.ErrUnavailable) {
			t.Fatalf("failWith() = %v", err)
		}
	}
	if !g.Available() || len(changes) != 0 {
		t.Fatalf("after two failures: available=%v changes=%v", g.Available(), changes)
	}
	if err := failWith(g, store.ErrUnavailable); !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("failWith() = %v", err)
	}
	if g.Available() {
		t.Fatal("guard available after third failure")
	}
	if !slices.Equal(changes, []bool{false}) {
		t.Fatalf("changes = %v, want [false]", changes)
	}
}

func TestSuccessResetsCount(t *testing.T) {
	g := newGuard(t, testSettings, newFake(), nil)
	seq := []error{store.ErrUnavailable, store.ErrUnavailable, nil, store.ErrUnavailable, store.ErrUnavailable}
	for _, e := range seq {
		if got := failWith(g, e); got != e {
			t.Fatalf("failWith(%v) = %v", e, got)
		}
	}
	if !g.Available() {
		t.Fatal("guard tripped although success reset the count")
	}
}

func TestOpenRejectsWithoutCalling(t *testing.T) {
	g := newGuard(t, testSettings, newFake(), nil)
	tripGuard(t, g)
	var calls atomic.Int64
	err := g.Do(context.Background(), callTimeout, func(context.Context) error {
		calls.Add(1)
		return nil
	})
	var se *Error
	if !errors.As(err, &se) || se.Reason != ReasonOpen {
		t.Fatalf("Do() = %v, want ReasonOpen", err)
	}
	if !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("Do() = %v does not wrap ErrUnavailable", err)
	}
	if calls.Load() != 0 {
		t.Fatal("fn was called on an open guard")
	}
	if !ShortCircuit(err) {
		t.Fatal("ShortCircuit() = false for open rejection")
	}
}

func TestHalfOpenSingleProbe(t *testing.T) {
	f := newFake()
	var changes []bool
	g := newGuard(t, testSettings, f, func(a bool) { changes = append(changes, a) })
	tripGuard(t, g)
	f.Advance(testSettings.OpenTimeout)

	gate := make(chan error)
	started := make(chan struct{})
	probeDone := make(chan error, 1)
	go func() {
		probeDone <- g.Do(context.Background(), callTimeout, func(context.Context) error {
			close(started)
			return <-gate
		})
	}()
	<-started

	expectOpenReject(t, g)

	gate <- nil
	if err := <-probeDone; err != nil {
		t.Fatalf("probe Do() = %v", err)
	}
	if !g.Available() {
		t.Fatal("guard not available after successful probe")
	}
	if !slices.Equal(changes, []bool{false, true}) {
		t.Fatalf("changes = %v, want [false true]", changes)
	}
}

func TestProbeFailureReopens(t *testing.T) {
	f := newFake()
	var changes []bool
	g := newGuard(t, testSettings, f, func(a bool) { changes = append(changes, a) })
	tripGuard(t, g)
	f.Advance(testSettings.OpenTimeout)

	if err := failWith(g, store.ErrUnavailable); !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("probe failWith() = %v", err)
	}
	if g.Available() {
		t.Fatal("guard available after failed probe")
	}
	if !slices.Equal(changes, []bool{false}) {
		t.Fatalf("changes = %v, want [false]", changes)
	}

	f.Advance(1999 * time.Millisecond)
	expectOpenReject(t, g)

	f.Advance(time.Millisecond)
	admitted := false
	if err := g.Do(context.Background(), callTimeout, func(context.Context) error {
		admitted = true
		return nil
	}); err != nil {
		t.Fatalf("probe after timeout Do() = %v", err)
	}
	if !admitted {
		t.Fatal("probe was not admitted after open timeout")
	}
	if !g.Available() || !slices.Equal(changes, []bool{false, true}) {
		t.Fatalf("available=%v changes=%v", g.Available(), changes)
	}
}

func TestTimeoutIgnoringCancellation(t *testing.T) {
	f := newFake()
	s := Settings{ConsecutiveFailures: 3, OpenTimeout: 2 * time.Second, MaxInflight: 1}
	g := newGuard(t, s, f, nil)
	gate := make(chan error)
	returned := make(chan struct{})
	fn := func(context.Context) error {
		defer close(returned)
		return <-gate
	}
	first := make(chan error, 1)
	go func() {
		first <- g.Do(context.Background(), 50*time.Millisecond, fn)
	}()
	f.BlockUntil(1)
	f.Advance(50 * time.Millisecond)
	err := <-first
	var se *Error
	if !errors.As(err, &se) || se.Reason != ReasonTimeout {
		t.Fatalf("Do() = %v, want ReasonTimeout", err)
	}
	if !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("Do() = %v does not wrap ErrUnavailable", err)
	}

	second := g.Do(context.Background(), callTimeout, func(context.Context) error { return nil })
	if !errors.As(second, &se) || se.Reason != ReasonOverloaded {
		t.Fatalf("second Do() = %v, want ReasonOverloaded", second)
	}

	gate <- nil
	<-returned
	waitInflight(t, g, 0)
	if err := g.Do(context.Background(), callTimeout, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Do() after release = %v", err)
	}
}

func TestInflightCapDoesNotTrip(t *testing.T) {
	f := newFake()
	s := Settings{ConsecutiveFailures: 3, OpenTimeout: 2 * time.Second, MaxInflight: 2}
	g := newGuard(t, s, f, nil)
	gate := make(chan error)
	started := make(chan struct{}, 2)
	results := make(chan error, 2)
	for range 2 {
		go func() {
			results <- g.Do(context.Background(), callTimeout, func(context.Context) error {
				started <- struct{}{}
				return <-gate
			})
		}()
	}
	<-started
	<-started

	for range 5 {
		err := g.Do(context.Background(), callTimeout, func(context.Context) error { return nil })
		var se *Error
		if !errors.As(err, &se) || se.Reason != ReasonOverloaded {
			t.Fatalf("Do() = %v, want ReasonOverloaded", err)
		}
		if !ShortCircuit(err) {
			t.Fatal("ShortCircuit() = false for overload")
		}
	}
	if !g.Available() {
		t.Fatal("overload made the guard unavailable")
	}

	gate <- nil
	gate <- nil
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("blocked Do() = %v", err)
		}
	}
	if err := failWith(g, store.ErrUnavailable); !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("failWith() = %v", err)
	}
	if !g.Available() {
		t.Fatal("one failure after overload opened the breaker")
	}
}

func TestInvalidLimitNotCounted(t *testing.T) {
	g := newGuard(t, testSettings, newFake(), nil)
	for range 3 {
		want := fmt.Errorf("x: %w", store.ErrInvalidLimit)
		if got := failWith(g, want); got != want {
			t.Fatalf("failWith() = %v, want %v", got, want)
		}
	}
	if !g.Available() {
		t.Fatal("invalid limit errors made the guard unavailable")
	}
}

func TestCallerCancellation(t *testing.T) {
	g := newGuard(t, testSettings, newFake(), nil)
	for range 3 {
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan struct{})
		res := make(chan error, 1)
		go func() {
			res <- g.Do(ctx, callTimeout, func(c context.Context) error {
				close(started)
				<-c.Done()
				return c.Err()
			})
		}()
		<-started
		cancel()
		if err := <-res; !errors.Is(err, context.Canceled) {
			t.Fatalf("Do() = %v, want context.Canceled", err)
		}
	}
	if !g.Available() {
		t.Fatal("caller cancellations made the guard unavailable")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var calls atomic.Int64
	err := g.Do(ctx, callTimeout, func(context.Context) error {
		calls.Add(1)
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Do() on canceled ctx = %v, want context.Canceled", err)
	}
	if calls.Load() != 0 {
		t.Fatal("fn was called with a canceled ctx")
	}
}

func TestCancelDuringFnIgnored(t *testing.T) {
	g := newGuard(t, testSettings, newFake(), nil)
	for range 3 {
		ctx, cancel := context.WithCancel(context.Background())
		err := g.Do(ctx, callTimeout, func(context.Context) error {
			cancel()
			return store.ErrUnavailable
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Do() = %v, want context.Canceled", err)
		}
	}
	if !g.Available() {
		t.Fatal("canceled calls were counted as failures")
	}
}

func TestPanicIsFailure(t *testing.T) {
	g := newGuard(t, testSettings, newFake(), nil)
	for range 3 {
		err := g.Do(context.Background(), callTimeout, func(context.Context) error {
			panic("secret-xyz")
		})
		var se *Error
		if !errors.As(err, &se) || se.Reason != ReasonPanic {
			t.Fatalf("Do() = %v, want ReasonPanic", err)
		}
		if strings.Contains(err.Error(), "secret-xyz") {
			t.Fatalf("error text leaks panic value: %q", err.Error())
		}
	}
	if g.Available() {
		t.Fatal("three panics did not trip the guard")
	}
}

func TestNonPositiveTimeout(t *testing.T) {
	g := newGuard(t, testSettings, newFake(), nil)
	var calls atomic.Int64
	fn := func(context.Context) error {
		calls.Add(1)
		return nil
	}
	for _, d := range []time.Duration{0, -time.Second} {
		err := g.Do(context.Background(), d, fn)
		if err == nil || errors.Is(err, store.ErrUnavailable) {
			t.Fatalf("Do(timeout=%v) = %v, want non-store error", d, err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("fn was called with a non-positive timeout")
	}
}

func TestCallbackPerChange(t *testing.T) {
	f := newFake()
	var changes []bool
	g := newGuard(t, testSettings, f, func(a bool) { changes = append(changes, a) })
	tripGuard(t, g)
	f.Advance(testSettings.OpenTimeout)
	if err := failWith(g, store.ErrUnavailable); !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("probe failWith() = %v", err)
	}
	f.Advance(testSettings.OpenTimeout)
	if err := failWith(g, nil); err != nil {
		t.Fatalf("probe success Do() = %v", err)
	}
	if !slices.Equal(changes, []bool{false, true}) {
		t.Fatalf("changes = %v, want [false true]", changes)
	}
}

func TestErrorStrings(t *testing.T) {
	cases := []struct {
		reason Reason
		want   string
		short  bool
	}{
		{ReasonOpen, `store "db" unavailable: guard open`, true},
		{ReasonOverloaded, `store "db" unavailable: too many calls in flight`, true},
		{ReasonTimeout, `store "db" unavailable: call timed out`, false},
		{ReasonPanic, `store "db" unavailable: call panicked`, false},
		{Reason("other"), `store "db" unavailable`, false},
	}
	for _, c := range cases {
		e := &Error{Store: "db", Reason: c.reason}
		if got := e.Error(); got != c.want {
			t.Errorf("Error() = %q, want %q", got, c.want)
		}
		if !errors.Is(e, store.ErrUnavailable) {
			t.Errorf("%v does not wrap ErrUnavailable", c.reason)
		}
		if got := ShortCircuit(e); got != c.short {
			t.Errorf("ShortCircuit(%v) = %v, want %v", c.reason, got, c.short)
		}
	}
	if ShortCircuit(nil) || ShortCircuit(store.ErrUnavailable) {
		t.Error("ShortCircuit() true for a non-Error value")
	}
}

func TestGoroutinesReturnToBaseline(t *testing.T) {
	f := newFake()
	g := newGuard(t, testSettings, f, nil)
	baseline := runtime.NumGoroutine()

	gate := make(chan error)
	exited := make(chan struct{}, 4)
	results := make(chan error, 4)
	fn := func(context.Context) error {
		defer func() { exited <- struct{}{} }()
		return <-gate
	}
	for range 4 {
		go func() {
			results <- g.Do(context.Background(), 50*time.Millisecond, fn)
		}()
	}
	f.BlockUntil(4)
	f.Advance(50 * time.Millisecond)
	for range 4 {
		err := <-results
		var se *Error
		if !errors.As(err, &se) || se.Reason != ReasonTimeout {
			t.Fatalf("Do() = %v, want ReasonTimeout", err)
		}
	}
	for range 4 {
		gate <- nil
	}
	for range 4 {
		<-exited
	}
	waitInflight(t, g, 0)
	for i := 0; runtime.NumGoroutine() > baseline && i < 10000; i++ {
		runtime.Gosched()
	}
	if n := runtime.NumGoroutine(); n > baseline {
		t.Fatalf("goroutines = %d, baseline = %d", n, baseline)
	}
}

func TestNoWallClock(t *testing.T) {
	banned := []string{
		"time.Now(", "time.Since(", "time.Sleep(", "time.After(", "time.AfterFunc(",
		"time.NewTimer(", "context.WithTimeout(", "context.WithDeadline(",
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(filepath.Join(".", name))
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range banned {
			if strings.Contains(string(src), b) {
				t.Fatalf("%s contains %q", name, b)
			}
		}
	}
}
