package reload

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testwait"
)

const fsDebounce = 20 * time.Millisecond

var fsStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// waitFor waits until cond holds. It advances fake only while a timer is due within fsDebounce, so a pending debounce fires.
func waitFor(t *testing.T, what string, fake *clock.Fake, cond func() bool) {
	t.Helper()
	var advanced time.Duration
	testwait.Until(t, what, func() bool {
		if cond() {
			return true
		}
		if fake.Due(fsDebounce) > 0 {
			fake.Advance(fsDebounce)
			advanced += fsDebounce
		}
		return false
	}, func() string {
		return fmt.Sprintf("fake clock advanced %v; timers due within %v: %d", advanced, fsDebounce, fake.Due(fsDebounce))
	})
}

func watch(t *testing.T, paths func() []string) (func() string, *atomic.Int32, *clock.Fake) {
	t.Helper()
	fake := clock.NewFake(fsStart)
	var applied atomic.Pointer[string]
	var attempts atomic.Int32
	w := New(Options{
		Paths:        paths,
		Clock:        fake,
		Debounce:     fsDebounce,
		PollInterval: time.Hour,
		Apply: func() error {
			attempts.Add(1)
			b, err := os.ReadFile(paths()[0])
			if err != nil {
				return err
			}
			s := string(b)
			applied.Store(&s)
			return nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err, _ := testwait.Recv(t, "Run to return after cancel", done, nil); err != nil {
			t.Errorf("Run returned %v", err)
		}
	})
	waitFor(t, "first apply", fake, func() bool { return applied.Load() != nil })
	get := func() string {
		if p := applied.Load(); p != nil {
			return *p
		}
		return ""
	}
	return get, &attempts, fake
}

func TestInPlaceWrite(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	writeFile(t, cfg, "v1")
	applied, _, fake := watch(t, func() []string { return []string{cfg} })
	waitFor(t, "v1 applied", fake, func() bool { return applied() == "v1" })
	writeFile(t, cfg, "v2")
	waitFor(t, "v2 applied", fake, func() bool { return applied() == "v2" })
}

func TestAtomicRenameRepeated(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	tmp := filepath.Join(dir, "config.yaml.tmp")
	writeFile(t, cfg, "v1")
	applied, _, fake := watch(t, func() []string { return []string{cfg} })
	for i := 2; i <= 6; i++ {
		want := fmt.Sprintf("v%d", i)
		writeFile(t, tmp, want)
		if err := os.Rename(tmp, cfg); err != nil {
			t.Fatal(err)
		}
		waitFor(t, want+" applied", fake, func() bool { return applied() == want })
	}
}

func TestBackupPattern(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	backup := cfg + "~"
	writeFile(t, cfg, "v1")
	applied, _, fake := watch(t, func() []string { return []string{cfg} })
	waitFor(t, "v1 applied", fake, func() bool { return applied() == "v1" })
	if err := os.Rename(cfg, backup); err != nil {
		t.Fatal(err)
	}
	writeFile(t, cfg, "v2")
	if err := os.Remove(backup); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "v2 applied", fake, func() bool { return applied() == "v2" })
}

func TestDeleteRecreate(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	writeFile(t, cfg, "v1")
	applied, _, fake := watch(t, func() []string { return []string{cfg} })
	waitFor(t, "v1 applied", fake, func() bool { return applied() == "v1" })
	if err := os.Remove(cfg); err != nil {
		t.Fatal(err)
	}
	writeFile(t, cfg, "v2")
	waitFor(t, "v2 applied", fake, func() bool { return applied() == "v2" })
}

func TestSymlinkTarget(t *testing.T) {
	dir := t.TempDir()
	dir2 := filepath.Join(dir, "dir2")
	if err := os.Mkdir(dir2, 0o755); err != nil {
		t.Fatal(err)
	}
	real := filepath.Join(dir2, "real.yaml")
	link := filepath.Join(dir, "link.yaml")
	writeFile(t, real, "v1")
	if err := os.Symlink(filepath.Join("dir2", "real.yaml"), link); err != nil {
		t.Fatal(err)
	}
	applied, _, fake := watch(t, func() []string { return []string{link} })
	waitFor(t, "v1 applied", fake, func() bool { return applied() == "v1" })
	for _, content := range []string{"v2", "v3"} {
		tmp := filepath.Join(dir2, "real.tmp")
		writeFile(t, tmp, content)
		if err := os.Rename(tmp, real); err != nil {
			t.Fatal(err)
		}
		waitFor(t, content+" applied", fake, func() bool { return applied() == content })
	}
	real2 := filepath.Join(dir2, "real2.yaml")
	writeFile(t, real2, "v4")
	tmpLink := filepath.Join(dir, "link.tmp")
	if err := os.Symlink(filepath.Join("dir2", "real2.yaml"), tmpLink); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmpLink, link); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "v4 applied", fake, func() bool { return applied() == "v4" })
}

func TestKubernetesSwap(t *testing.T) {
	dir := t.TempDir()
	k8sSwap(t, dir, 1, "v1")
	cfg := filepath.Join(dir, "config.yaml")
	applied, _, fake := watch(t, func() []string { return []string{cfg} })
	waitFor(t, "v1 applied", fake, func() bool { return applied() == "v1" })
	for n := 2; n <= 4; n++ {
		want := fmt.Sprintf("v%d", n)
		k8sSwap(t, dir, n, want)
		waitFor(t, want+" applied", fake, func() bool { return applied() == want })
	}
}

func TestRapidSaves(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	writeFile(t, cfg, "init")
	applied, _, fake := watch(t, func() []string { return []string{cfg} })
	for i := 0; i < 10; i++ {
		writeFile(t, cfg, fmt.Sprintf("v%d", i))
	}
	waitFor(t, "v9 applied", fake, func() bool { return applied() == "v9" })
}

func TestRunNoLeak(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	writeFile(t, cfg, "a")
	baseline := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		started := make(chan struct{}, 1)
		fake := clock.NewFake(fsStart)
		w := New(Options{
			Paths:        func() []string { return []string{cfg} },
			Clock:        fake,
			Debounce:     fsDebounce,
			PollInterval: time.Hour,
			Apply: func() error {
				select {
				case started <- struct{}{}:
				default:
				}
				return nil
			},
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- w.Run(ctx) }()
		testwait.Done(t, "first attempt", started, nil)
		cancel()
		if err, _ := testwait.Recv(t, "Run to return after cancel", done, nil); err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	}
	testwait.Until(t, "goroutines back to baseline", func() bool { return runtime.NumGoroutine() <= baseline }, func() string {
		return fmt.Sprintf("goroutines=%d baseline=%d", runtime.NumGoroutine(), baseline)
	})
}
