package reload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/ok0/go-microgateway/internal/clock"
)

var (
	t0      = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	errBad  = errors.New("rejected content")
	errFake = errors.New("fatal reload failure")
)

type fakeSource struct {
	events  chan fsnotify.Event
	errs    chan error
	mu      sync.Mutex
	watched []string
	closed  bool
}

func newFakeSource() *fakeSource {
	return &fakeSource{events: make(chan fsnotify.Event, 64), errs: make(chan error, 4)}
}

func (f *fakeSource) Watch(dirs []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.watched = append([]string(nil), dirs...)
}

func (f *fakeSource) Events() <-chan fsnotify.Event {
	return f.events
}

func (f *fakeSource) Errors() <-chan error {
	return f.errs
}

func (f *fakeSource) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeSource) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakeSource) watchedDirs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.watched)
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

type rig struct {
	t         *testing.T
	clk       *clock.Fake
	src       *fakeSource
	cfg       string
	poll      time.Duration
	logger    *slog.Logger
	newEvents func() (eventSource, error)
	paths     func() []string
	check     func(content string) error
	fatal     func(error) bool
	w         *Watcher
	cancel    context.CancelFunc
	done      chan error

	mu       sync.Mutex
	attempts int
	applied  string
	hold     chan struct{}
}

func newRig(t *testing.T, cfg string) *rig {
	r := &rig{t: t, clk: clock.NewFake(t0), src: newFakeSource(), cfg: cfg, poll: time.Hour}
	r.newEvents = func() (eventSource, error) { return r.src, nil }
	r.paths = func() []string { return []string{cfg} }
	return r
}

func (r *rig) apply() error {
	r.mu.Lock()
	r.attempts++
	hold := r.hold
	r.hold = nil
	r.mu.Unlock()

	b, err := os.ReadFile(r.cfg)
	if err != nil {
		return err
	}
	content := string(b)
	if hold != nil {
		<-hold
	}
	if r.check != nil {
		if err := r.check(content); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.applied = content
	r.mu.Unlock()
	return nil
}

func (r *rig) holdNextApply() chan struct{} {
	ch := make(chan struct{})
	r.mu.Lock()
	r.hold = ch
	r.mu.Unlock()
	return ch
}

func (r *rig) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attempts
}

func (r *rig) content() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.applied
}

func (r *rig) start() {
	r.w = New(Options{
		Paths:        r.paths,
		Debounce:     debounce,
		PollInterval: r.poll,
		Clock:        r.clk,
		Logger:       r.logger,
		Apply:        r.apply,
		Fatal:        r.fatal,
		newEvents:    r.newEvents,
	})
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan error, 1)
	go func() { r.done <- r.w.Run(ctx) }()
	synctest.Wait()
}

func (r *rig) finish() {
	r.cancel()
	synctest.Wait()
	if err := <-r.done; err != nil {
		r.t.Errorf("Run returned %v, want nil", err)
	}
}

func (r *rig) advance(d time.Duration) {
	r.clk.Advance(d)
	synctest.Wait()
}

func (r *rig) event(name string, op fsnotify.Op) {
	r.src.events <- fsnotify.Event{Name: name, Op: op}
	synctest.Wait()
}

func (r *rig) expectAttempts(n int) {
	r.t.Helper()
	if got := r.count(); got != n {
		r.t.Errorf("attempts = %d, want %d", got, n)
	}
}

func (r *rig) expectContent(want string) {
	r.t.Helper()
	if got := r.content(); got != want {
		r.t.Errorf("applied content = %q, want %q", got, want)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func k8sSwap(t *testing.T, dir string, n int, content string) {
	t.Helper()
	ver := fmt.Sprintf("..v%d", n)
	if err := os.Mkdir(filepath.Join(dir, ver), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, ver, "config.yaml"), content)
	tmp := filepath.Join(dir, "..data_tmp")
	if err := os.Symlink(ver, tmp); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	if n == 1 {
		if err := os.Symlink(filepath.Join("..data", "config.yaml"), filepath.Join(dir, "config.yaml")); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err := os.RemoveAll(filepath.Join(dir, fmt.Sprintf("..v%d", n-1))); err != nil {
		t.Fatal(err)
	}
}

const debounce = 100 * time.Millisecond

func TestStartCheck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		writeFile(t, cfg, "v1")
		r := newRig(t, cfg)
		r.start()
		r.expectAttempts(1)
		r.expectContent("v1")
		r.finish()
	})
}

func TestMissingPath(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "missing.yaml")
		attempts := 0
		w := New(Options{
			Paths:        func() []string { return []string{missing} },
			Debounce:     debounce,
			PollInterval: time.Hour,
			Clock:        clock.NewFake(t0),
			Apply: func() error {
				attempts++
				return nil
			},
			newEvents: func() (eventSource, error) { return newFakeSource(), nil },
		})
		err := w.Run(context.Background())
		if err == nil {
			t.Fatal("Run on a missing path returned nil, want an error")
		}
		if attempts != 0 {
			t.Fatalf("attempts = %d, want 0", attempts)
		}
	})
}

func TestBurstDebounced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		writeFile(t, cfg, "a")
		r := newRig(t, cfg)
		r.start()
		writeFile(t, cfg, "b")
		for i := 0; i < 20; i++ {
			r.src.events <- fsnotify.Event{Name: cfg, Op: fsnotify.Write}
		}
		synctest.Wait()
		r.advance(99 * time.Millisecond)
		r.expectAttempts(1)
		r.advance(time.Millisecond)
		r.expectAttempts(2)
		r.advance(time.Second)
		r.expectAttempts(2)
		r.expectContent("b")
		r.finish()
	})
}

func TestTrailingEdge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		writeFile(t, cfg, "a")
		r := newRig(t, cfg)
		r.start()
		writeFile(t, cfg, "b")
		r.event(cfg, fsnotify.Write)
		r.advance(50 * time.Millisecond)
		r.event(cfg, fsnotify.Write)
		r.advance(50 * time.Millisecond)
		r.expectAttempts(1)
		r.advance(50 * time.Millisecond)
		r.expectAttempts(2)
		r.expectContent("b")
		r.finish()
	})
}

func TestMaxWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		writeFile(t, cfg, "v0")
		r := newRig(t, cfg)
		r.start()
		for i := 0; i < 20; i++ {
			writeFile(t, cfg, fmt.Sprintf("v%d", i))
			r.event(cfg, fsnotify.Write)
			r.advance(50 * time.Millisecond)
			if i < 19 {
				r.expectAttempts(1)
			} else {
				r.expectAttempts(2)
			}
		}
		r.advance(time.Second)
		r.expectAttempts(2)
		r.expectContent("v19")
		r.finish()
	})
}

func TestEventDuringAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		writeFile(t, cfg, "a")
		r := newRig(t, cfg)
		r.start()
		writeFile(t, cfg, "b")
		hold := r.holdNextApply()
		r.event(cfg, fsnotify.Write)
		r.advance(100 * time.Millisecond)
		r.expectAttempts(2)
		writeFile(t, cfg, "c")
		for i := 0; i < 3; i++ {
			r.src.events <- fsnotify.Event{Name: cfg, Op: fsnotify.Write}
		}
		close(hold)
		synctest.Wait()
		r.advance(100 * time.Millisecond)
		r.expectAttempts(3)
		r.advance(time.Second)
		r.expectAttempts(3)
		r.expectContent("c")
		r.finish()
	})
}

func TestChmodAndSiblingIgnored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		cfg := filepath.Join(dir, "config.yaml")
		writeFile(t, cfg, "a")
		r := newRig(t, cfg)
		r.start()
		r.event(cfg, fsnotify.Chmod)
		sibling := filepath.Join(dir, "other.yaml")
		writeFile(t, sibling, "x")
		r.event(sibling, fsnotify.Write)
		r.advance(2 * time.Second)
		r.expectAttempts(1)
		r.finish()
	})
}

func TestIdenticalTouchSkipped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		writeFile(t, cfg, "a")
		r := newRig(t, cfg)
		r.start()
		writeFile(t, cfg, "a")
		r.event(cfg, fsnotify.Write)
		r.advance(time.Second)
		r.expectAttempts(1)
		r.finish()
	})
}

func TestNoRetryAfterFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		writeFile(t, cfg, "good")
		r := newRig(t, cfg)
		r.check = func(content string) error {
			if content == "bad" {
				return errBad
			}
			return nil
		}
		r.start()
		writeFile(t, cfg, "bad")
		r.event(cfg, fsnotify.Write)
		r.advance(100 * time.Millisecond)
		r.expectAttempts(2)
		r.event(cfg, fsnotify.Write)
		r.advance(100 * time.Millisecond)
		r.expectAttempts(2)
		r.w.Trigger()
		synctest.Wait()
		r.expectAttempts(3)
		r.finish()
	})
}

func TestTriggerCoalesces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		writeFile(t, cfg, "a")
		r := newRig(t, cfg)
		r.start()
		writeFile(t, cfg, "b")
		r.event(cfg, fsnotify.Write)
		r.w.Trigger()
		synctest.Wait()
		r.expectAttempts(2)
		r.advance(time.Second)
		r.expectAttempts(2)
		r.expectContent("b")
		r.finish()
	})
}

func TestPollDetectsChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		writeFile(t, cfg, "a")
		r := newRig(t, cfg)
		r.poll = 5 * time.Second
		r.start()
		writeFile(t, cfg, "bb")
		r.advance(5 * time.Second)
		r.expectAttempts(1)
		r.advance(100 * time.Millisecond)
		r.expectAttempts(2)
		r.expectContent("bb")
		r.finish()
	})
}

func TestPollKubernetesSwap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		k8sSwap(t, dir, 1, "v1")
		r := newRig(t, filepath.Join(dir, "config.yaml"))
		r.poll = 5 * time.Second
		r.start()
		r.expectContent("v1")
		for n := 2; n <= 4; n++ {
			content := fmt.Sprintf("v%d", n)
			k8sSwap(t, dir, n, content)
			r.advance(5 * time.Second)
			r.advance(100 * time.Millisecond)
			r.expectContent(content)
		}
		r.finish()
	})
}

func TestPollParentRecreated(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sub := filepath.Join(t.TempDir(), "sub")
		if err := os.Mkdir(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		cfg := filepath.Join(sub, "config.yaml")
		writeFile(t, cfg, "v1")
		r := newRig(t, cfg)
		r.poll = 5 * time.Second
		r.start()
		if err := os.RemoveAll(sub); err != nil {
			t.Fatal(err)
		}
		r.advance(5 * time.Second)
		r.advance(100 * time.Millisecond)
		r.expectAttempts(2)
		r.expectContent("v1")
		if err := os.Mkdir(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		writeFile(t, cfg, "v22")
		r.advance(5 * time.Second)
		r.advance(100 * time.Millisecond)
		r.expectContent("v22")
		if !slices.Contains(r.src.watchedDirs(), sub) {
			t.Errorf("watched dirs = %v, want to contain %s", r.src.watchedDirs(), sub)
		}
		r.finish()
	})
}

func TestOverflowTriggersCheck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		writeFile(t, cfg, "a")
		r := newRig(t, cfg)
		r.start()
		writeFile(t, cfg, "bb")
		r.src.errs <- fsnotify.ErrEventOverflow
		synctest.Wait()
		r.advance(100 * time.Millisecond)
		r.expectContent("bb")
		r.finish()
	})
}

func TestEventsUnavailable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		writeFile(t, cfg, "a")
		r := newRig(t, cfg)
		logs := &lockedBuffer{}
		r.logger = slog.New(slog.NewJSONHandler(logs, nil))
		r.newEvents = func() (eventSource, error) { return nil, errors.New("no event source") }
		r.poll = 5 * time.Second
		r.start()
		if !strings.Contains(logs.String(), "file events unavailable; polling only") {
			t.Errorf("log output %q lacks the polling-only warning", logs.String())
		}
		writeFile(t, cfg, "bb")
		r.advance(5 * time.Second)
		r.advance(100 * time.Millisecond)
		r.expectContent("bb")
		r.finish()
	})
}

func TestCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		writeFile(t, cfg, "a")
		r := newRig(t, cfg)
		r.start()
		r.cancel()
		synctest.Wait()
		if err := <-r.done; err != nil {
			t.Fatalf("Run returned %v after cancel, want nil", err)
		}
		if !r.src.isClosed() {
			t.Error("event source was not closed")
		}
		if n := r.clk.Waiters(); n != 0 {
			t.Errorf("Waiters() = %d, want 0", n)
		}
	})
}

func TestNoAttemptAfterCancel(t *testing.T) {
	t.Run("before_start", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := filepath.Join(t.TempDir(), "config.yaml")
			writeFile(t, cfg, "a")
			r := newRig(t, cfg)
			calls := 0
			w := New(Options{
				Paths:        r.paths,
				Debounce:     debounce,
				PollInterval: time.Hour,
				Clock:        r.clk,
				Apply:        r.apply,
				newEvents: func() (eventSource, error) {
					calls++
					return r.src, nil
				},
			})
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := w.Run(ctx); err != nil {
				t.Errorf("Run returned %v, want nil", err)
			}
			r.expectAttempts(0)
			if calls != 0 {
				t.Errorf("newEvents called %d times, want 0", calls)
			}
		})
	})
	t.Run("ready_trigger_after_cancel", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			synctest.Test(t, func(t *testing.T) {
				cfg := filepath.Join(t.TempDir(), "config.yaml")
				writeFile(t, cfg, "a")
				r := newRig(t, cfg)
				hold := r.holdNextApply()
				r.start()
				r.expectAttempts(1)
				writeFile(t, cfg, "b")
				r.src.events <- fsnotify.Event{Name: cfg, Op: fsnotify.Write}
				r.w.Trigger()
				r.cancel()
				close(hold)
				synctest.Wait()
				select {
				case err := <-r.done:
					if err != nil {
						t.Errorf("Run returned %v, want nil", err)
					}
				default:
					t.Error("Run did not return after cancel")
				}
				r.expectAttempts(1)
			})
		}
	})
}

func TestFatalStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		writeFile(t, cfg, "a")
		r := newRig(t, cfg)
		r.check = func(content string) error {
			if content == "fatal" {
				return errFake
			}
			return nil
		}
		r.fatal = func(err error) bool { return errors.Is(err, errFake) }
		r.start()
		writeFile(t, cfg, "fatal")
		r.event(cfg, fsnotify.Write)
		r.advance(100 * time.Millisecond)
		select {
		case err := <-r.done:
			if !errors.Is(err, errFake) {
				t.Fatalf("Run returned %v, want %v", err, errFake)
			}
		default:
			t.Fatal("Run did not return after a fatal attempt")
		}
	})
}

func TestPathsRefreshed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		cfg := filepath.Join(dir, "config.yaml")
		cert := filepath.Join(dir, "cert.pem")
		writeFile(t, cfg, "a")
		writeFile(t, cert, "c1")
		r := newRig(t, cfg)
		r.paths = func() []string {
			if r.count() == 0 {
				return []string{cfg}
			}
			return []string{cfg, cert}
		}
		r.start()
		r.expectAttempts(1)
		writeFile(t, cert, "c2")
		r.event(cert, fsnotify.Write)
		r.advance(100 * time.Millisecond)
		r.expectAttempts(2)
		r.finish()
	})
}

func TestRelevant(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		names := map[string]bool{"/d/config.yaml": true, "/r/real.yaml": true}
		pathDirs := map[string]bool{"/d": true}
		cases := []struct {
			name string
			ev   fsnotify.Event
			want bool
		}{
			{"chmod only", fsnotify.Event{Name: "/d/config.yaml", Op: fsnotify.Chmod}, false},
			{"write and chmod", fsnotify.Event{Name: "/d/config.yaml", Op: fsnotify.Write | fsnotify.Chmod}, true},
			{"watched name", fsnotify.Event{Name: "/d/config.yaml", Op: fsnotify.Write}, true},
			{"sibling", fsnotify.Event{Name: "/d/other.yaml", Op: fsnotify.Write}, false},
			{"kubernetes data link", fsnotify.Event{Name: "/d/..data", Op: fsnotify.Create}, true},
			{"kubernetes timestamp dir", fsnotify.Event{Name: "/d/..2024_01_01", Op: fsnotify.Create}, true},
			{"other directory data", fsnotify.Event{Name: "/o/..data", Op: fsnotify.Write}, false},
			{"resolved symlink target", fsnotify.Event{Name: "/r/real.yaml", Op: fsnotify.Write}, true},
		}
		for _, c := range cases {
			if got := relevant(c.ev, names, pathDirs); got != c.want {
				t.Errorf("%s: relevant = %v, want %v", c.name, got, c.want)
			}
		}
	})
}
