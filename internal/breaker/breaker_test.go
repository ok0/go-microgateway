package breaker

import (
	"errors"
	"fmt"
	"go/parser"
	gotoken "go/token"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
)

func newTest(t *testing.T, mod func(*Settings)) (*Breaker, *clock.Fake, *[]string) {
	t.Helper()
	var seen []string
	s := Settings{
		Window:              10 * time.Second,
		Buckets:             10,
		MinRequests:         4,
		FailureRatio:        0.5,
		ConsecutiveFailures: 3,
		OpenTimeout:         30 * time.Second,
		HalfOpenProbes:      2,
		SuccessThreshold:    2,
		OnStateChange: func(from, to State, reason Reason) {
			seen = append(seen, fmt.Sprintf("%s>%s:%s", from, to, reason))
		},
	}
	if mod != nil {
		mod(&s)
	}
	clk := clock.NewFake(time.Unix(0, 0))
	b, err := New(s, clk)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return b, clk, &seen
}

func do(t *testing.T, b *Breaker, o Outcome) {
	t.Helper()
	tok, err := b.Allow()
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	tok.Done(o)
}

func fail(t *testing.T, b *Breaker, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		do(t, b, Failure)
	}
}

func wantSeen(t *testing.T, seen *[]string, want ...string) {
	t.Helper()
	if !slices.Equal(*seen, want) {
		t.Fatalf("transitions = %v, want %v", *seen, want)
	}
}

func wantState(t *testing.T, b *Breaker, want State) {
	t.Helper()
	if got := b.State(); got != want {
		t.Fatalf("State() = %v, want %v", got, want)
	}
}

func validSettings() Settings {
	return Settings{
		Window:              10 * time.Second,
		Buckets:             10,
		MinRequests:         4,
		FailureRatio:        0.5,
		ConsecutiveFailures: 3,
		OpenTimeout:         30 * time.Second,
		HalfOpenProbes:      2,
		SuccessThreshold:    2,
	}
}

func TestStateString(t *testing.T) {
	cases := map[State]string{Closed: "closed", HalfOpen: "half_open", Open: "open", State(9): "State(9)"}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Fatalf("State(%d).String() = %q, want %q", int32(s), got, want)
		}
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name string
		mod  func(*Settings)
	}{
		{"window 0", func(s *Settings) { s.Window = 0 }},
		{"buckets 0", func(s *Settings) { s.Buckets = 0 }},
		{"window 5ms with buckets 10", func(s *Settings) { s.Window = 5 * time.Millisecond }},
		{"min requests 0", func(s *Settings) { s.MinRequests = 0 }},
		{"failure ratio 0", func(s *Settings) { s.FailureRatio = 0 }},
		{"failure ratio 1.5", func(s *Settings) { s.FailureRatio = 1.5 }},
		{"failure ratio NaN", func(s *Settings) { s.FailureRatio = math.NaN() }},
		{"consecutive -1", func(s *Settings) { s.ConsecutiveFailures = -1 }},
		{"open timeout 0", func(s *Settings) { s.OpenTimeout = 0 }},
		{"half open probes 0", func(s *Settings) { s.HalfOpenProbes = 0 }},
		{"success threshold 0", func(s *Settings) { s.SuccessThreshold = 0 }},
		{"success threshold above probes", func(s *Settings) { s.SuccessThreshold = 3 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := validSettings()
			c.mod(&s)
			err := s.Validate()
			if !errors.Is(err, ErrInvalidSettings) {
				t.Fatalf("Validate() = %v, want ErrInvalidSettings", err)
			}
			if _, err := New(s, nil); !errors.Is(err, ErrInvalidSettings) {
				t.Fatalf("New() = %v, want ErrInvalidSettings", err)
			}
		})
	}

	t.Run("valid", func(t *testing.T) {
		if err := validSettings().Validate(); err != nil {
			t.Fatalf("Validate() = %v, want nil", err)
		}
	})
	t.Run("consecutive zero is valid", func(t *testing.T) {
		s := validSettings()
		s.ConsecutiveFailures = 0
		if err := s.Validate(); err != nil {
			t.Fatalf("Validate() = %v, want nil", err)
		}
	})
	t.Run("two violations name both fields", func(t *testing.T) {
		s := validSettings()
		s.Window = 0
		s.Buckets = 0
		err := s.Validate()
		if err == nil {
			t.Fatal("Validate() = nil, want error")
		}
		msg := err.Error()
		if !strings.Contains(msg, "Window") || !strings.Contains(msg, "Buckets") {
			t.Fatalf("error %q does not name both Window and Buckets", msg)
		}
	})
}

func TestRatioNeedsMinRequests(t *testing.T) {
	b, _, seen := newTest(t, func(s *Settings) { s.ConsecutiveFailures = 0 })
	do(t, b, Success)
	fail(t, b, 2)
	wantState(t, b, Closed)
	wantSeen(t, seen)
	fail(t, b, 1)
	wantState(t, b, Open)
	wantSeen(t, seen, "closed>open:ratio")
}

func TestConsecutiveTrips(t *testing.T) {
	b, _, seen := newTest(t, nil)
	for i := 0; i < 5; i++ {
		do(t, b, Success)
	}
	fail(t, b, 3)
	wantState(t, b, Open)
	wantSeen(t, seen, "closed>open:consecutive")
}

func TestIgnoredChangesNothing(t *testing.T) {
	b, _, seen := newTest(t, nil)
	fail(t, b, 2)
	for i := 0; i < 10; i++ {
		do(t, b, Ignored)
	}
	if got := b.Snapshot().Requests; got != 2 {
		t.Fatalf("Requests after Ignored = %d, want 2", got)
	}
	do(t, b, Failure)
	wantState(t, b, Open)
	wantSeen(t, seen, "closed>open:consecutive")
}

func TestBucketsAgeOut(t *testing.T) {
	b, clk, seen := newTest(t, func(s *Settings) { s.ConsecutiveFailures = 0 })
	fail(t, b, 3)
	clk.Advance(10 * time.Second)
	do(t, b, Success)
	wantState(t, b, Closed)
	wantSeen(t, seen)
	if got := b.Snapshot().Requests; got != 1 {
		t.Fatalf("Requests = %d, want 1", got)
	}
}

func TestOpenRejectsUntilTimeout(t *testing.T) {
	b, clk, seen := newTest(t, nil)
	fail(t, b, 3)
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("Allow at +0 = %v, want ErrOpen", err)
	}
	clk.Advance(30*time.Second - time.Nanosecond)
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("Allow at +30s-1ns = %v, want ErrOpen", err)
	}
	if b.Ready() {
		t.Fatal("Ready() before timeout = true, want false")
	}
	clk.Advance(time.Nanosecond)
	if !b.Ready() {
		t.Fatal("Ready() at +30s = false, want true")
	}
	wantSeen(t, seen, "closed>open:consecutive")
	tok, err := b.Allow()
	if err != nil {
		t.Fatalf("Allow at +30s = %v, want nil", err)
	}
	tok.Done(Success)
	wantSeen(t, seen, "closed>open:consecutive", "open>half_open:timeout_elapsed")
}

func TestHalfOpenProbeLimit(t *testing.T) {
	b, clk, _ := newTest(t, nil)
	fail(t, b, 3)
	clk.Advance(30 * time.Second)
	for i := 0; i < 100; i++ {
		if !b.Ready() {
			t.Fatalf("Ready() call %d = false, want true", i)
		}
	}
	a, err := b.Allow()
	if err != nil {
		t.Fatalf("first probe: %v", err)
	}
	if _, err := b.Allow(); err != nil {
		t.Fatalf("second probe: %v", err)
	}
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("third probe = %v, want ErrOpen", err)
	}
	a.Done(Ignored)
}

func TestProbesClose(t *testing.T) {
	b, clk, seen := newTest(t, nil)
	fail(t, b, 3)
	clk.Advance(30 * time.Second)
	a, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	c, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	a.Done(Success)
	c.Done(Success)
	wantState(t, b, Closed)
	wantSeen(t, seen, "closed>open:consecutive", "open>half_open:timeout_elapsed", "half_open>closed:probes_succeeded")
	snap := b.Snapshot()
	if snap.Requests != 0 || snap.Failures != 0 || snap.ConsecutiveFailures != 0 || snap.Probes != 0 {
		t.Fatalf("counters not reset after close: %+v", snap)
	}
}

func TestProbeFailureReopens(t *testing.T) {
	b, clk, seen := newTest(t, nil)
	fail(t, b, 3)
	clk.Advance(30 * time.Second)
	tok, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	tok.Done(Failure)
	wantState(t, b, Open)
	wantSeen(t, seen, "closed>open:consecutive", "open>half_open:timeout_elapsed", "half_open>open:probe_failed")
	clk.Advance(30*time.Second - time.Nanosecond)
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("Allow before fresh timeout = %v, want ErrOpen", err)
	}
	clk.Advance(time.Nanosecond)
	if _, err := b.Allow(); err != nil {
		t.Fatalf("Allow after fresh timeout = %v, want nil", err)
	}
}

func TestIgnoredProbeFreesSlot(t *testing.T) {
	b, clk, _ := newTest(t, nil)
	fail(t, b, 3)
	clk.Advance(30 * time.Second)
	a, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	bt, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	a.Done(Ignored)
	wantState(t, b, HalfOpen)
	if _, err := b.Allow(); err != nil {
		t.Fatalf("Allow after ignored probe = %v, want nil", err)
	}
	bt.Done(Ignored)
}

func TestDoubleDone(t *testing.T) {
	b, _, _ := newTest(t, nil)
	tok, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	tok.Done(Failure)
	tok.Done(Failure)
	snap := b.Snapshot()
	if snap.InFlight != 0 {
		t.Fatalf("InFlight = %d, want 0", snap.InFlight)
	}
	if snap.Failures != 1 || snap.Requests != 1 {
		t.Fatalf("Failures = %d Requests = %d, want 1 and 1", snap.Failures, snap.Requests)
	}
}

func TestZeroTokenDone(t *testing.T) {
	var tok Token
	if tok.Valid() {
		t.Fatal("zero token Valid() = true")
	}
	tok.Done(Success)
	tok.Done(Failure)
}

func TestStaleTokenIgnored(t *testing.T) {
	b, clk, seen := newTest(t, nil)
	stale, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	fail(t, b, 3)
	wantState(t, b, Open)
	clk.Advance(10 * time.Second)
	before := b.Snapshot().UntilHalfOpen
	stale.Done(Failure)
	wantState(t, b, Open)
	if after := b.Snapshot().UntilHalfOpen; after != before {
		t.Fatalf("UntilHalfOpen changed from %v to %v after stale Done", before, after)
	}
	wantSeen(t, seen, "closed>open:consecutive")
}

func TestLazyStateOnSnapshot(t *testing.T) {
	b, clk, seen := newTest(t, nil)
	fail(t, b, 3)
	clk.Advance(30 * time.Second)
	wantState(t, b, HalfOpen)
	wantSeen(t, seen, "closed>open:consecutive", "open>half_open:timeout_elapsed")
}

func TestSnapshotFields(t *testing.T) {
	b, clk, _ := newTest(t, nil)
	fail(t, b, 3)
	clk.Advance(10 * time.Second)
	snap := b.Snapshot()
	if snap.State != Open {
		t.Fatalf("State = %v, want open", snap.State)
	}
	if snap.UntilHalfOpen != 20*time.Second {
		t.Fatalf("UntilHalfOpen = %v, want 20s", snap.UntilHalfOpen)
	}
	clk.Advance(20 * time.Second)
	if got := b.Snapshot().UntilHalfOpen; got != 0 {
		t.Fatalf("UntilHalfOpen after timeout = %v, want 0", got)
	}
}

func TestConcurrentProbes(t *testing.T) {
	b, clk, _ := newTest(t, nil)
	fail(t, b, 3)
	clk.Advance(30 * time.Second)

	var wg sync.WaitGroup
	var overflow atomic.Int64
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, 0))
			for i := 0; i < 200; i++ {
				tok, err := b.Allow()
				if err != nil {
					continue
				}
				if b.Snapshot().Probes > b.settings.HalfOpenProbes {
					overflow.Add(1)
				}
				tok.Done(Outcome(r.IntN(3)))
			}
		}(uint64(g))
	}
	go func() {
		for i := 0; i < 200; i++ {
			clk.Advance(time.Second)
		}
	}()
	wg.Wait()

	if n := overflow.Load(); n != 0 {
		t.Fatalf("probe count exceeded HalfOpenProbes %d times", n)
	}
	if got := b.Snapshot().InFlight; got != 0 {
		t.Fatalf("InFlight = %d, want 0", got)
	}
}

func TestNewTakesNoSharedHandle(t *testing.T) {
	fn := reflect.TypeOf(New)
	if fn.NumIn() != 2 {
		t.Fatalf("New has %d inputs, want 2", fn.NumIn())
	}
	if fn.In(0) != reflect.TypeOf(Settings{}) {
		t.Fatalf("New input 0 = %v, want Settings", fn.In(0))
	}
	if fn.In(1) != reflect.TypeOf((*clock.Clock)(nil)).Elem() {
		t.Fatalf("New input 1 = %v, want clock.Clock", fn.In(1))
	}

	st := reflect.TypeOf(Settings{})
	got := make([]string, 0, st.NumField())
	for i := 0; i < st.NumField(); i++ {
		got = append(got, st.Field(i).Name)
	}
	want := []string{"Window", "Buckets", "MinRequests", "FailureRatio", "ConsecutiveFailures", "OpenTimeout", "HalfOpenProbes", "SuccessThreshold", "OnStateChange"}
	if !slices.Equal(got, want) {
		t.Fatalf("Settings fields = %v, want %v", got, want)
	}
}

func TestImports(t *testing.T) {
	allowed := map[string]bool{
		"errors":      true,
		"fmt":         true,
		"sync":        true,
		"sync/atomic": true,
		"time":        true,
		"github.com/ok0/go-microgateway/internal/clock": true,
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	used := map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(".", name)
		f, err := parser.ParseFile(gotoken.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range f.Imports {
			p, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if !allowed[p] {
				t.Fatalf("%s imports disallowed package %q", name, p)
			}
			used[p] = true
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, banned := range []string{"time.Now(", "time.Since(", "time.Sleep(", "time.AfterFunc("} {
			if strings.Contains(string(src), banned) {
				t.Fatalf("%s contains %q", name, banned)
			}
		}
	}
	for p := range allowed {
		if !used[p] {
			t.Fatalf("allowed import %q is not used", p)
		}
	}
}

func benchSettings() Settings {
	return validSettings()
}

func BenchmarkAllowDone(b *testing.B) {
	br, err := New(benchSettings(), nil)
	if err != nil {
		b.Fatal(err)
	}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tok, err := br.Allow()
			if err == nil {
				tok.Done(Success)
			}
		}
	})
}

func BenchmarkReady(b *testing.B) {
	br, err := New(benchSettings(), nil)
	if err != nil {
		b.Fatal(err)
	}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = br.Ready()
		}
	})
}
