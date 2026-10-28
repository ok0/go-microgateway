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
)

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if cond() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("condition not met within 10s")
		case <-tick.C:
		}
	}
}

func watch(t *testing.T, paths func() []string) (func() string, *atomic.Int32) {
	t.Helper()
	var applied atomic.Pointer[string]
	var attempts atomic.Int32
	w := New(Options{
		Paths:        paths,
		Debounce:     20 * time.Millisecond,
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
		if err := <-done; err != nil {
			t.Errorf("Run returned %v", err)
		}
	})
	waitFor(t, func() bool { return applied.Load() != nil })
	get := func() string {
		if p := applied.Load(); p != nil {
			return *p
		}
		return ""
	}
	return get, &attempts
}

func TestInPlaceWrite(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	writeFile(t, cfg, "v1")
	applied, _ := watch(t, func() []string { return []string{cfg} })
	waitFor(t, func() bool { return applied() == "v1" })
	writeFile(t, cfg, "v2")
	waitFor(t, func() bool { return applied() == "v2" })
}

func TestAtomicRenameRepeated(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	tmp := filepath.Join(dir, "config.yaml.tmp")
	writeFile(t, cfg, "v1")
	applied, _ := watch(t, func() []string { return []string{cfg} })
	for i := 2; i <= 6; i++ {
		want := fmt.Sprintf("v%d", i)
		writeFile(t, tmp, want)
		if err := os.Rename(tmp, cfg); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return applied() == want })
	}
}

func TestBackupPattern(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	backup := cfg + "~"
	writeFile(t, cfg, "v1")
	applied, _ := watch(t, func() []string { return []string{cfg} })
	waitFor(t, func() bool { return applied() == "v1" })
	if err := os.Rename(cfg, backup); err != nil {
		t.Fatal(err)
	}
	writeFile(t, cfg, "v2")
	if err := os.Remove(backup); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return applied() == "v2" })
}

func TestDeleteRecreate(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	writeFile(t, cfg, "v1")
	applied, _ := watch(t, func() []string { return []string{cfg} })
	waitFor(t, func() bool { return applied() == "v1" })
	if err := os.Remove(cfg); err != nil {
		t.Fatal(err)
	}
	writeFile(t, cfg, "v2")
	waitFor(t, func() bool { return applied() == "v2" })
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
	applied, _ := watch(t, func() []string { return []string{link} })
	waitFor(t, func() bool { return applied() == "v1" })
	for _, content := range []string{"v2", "v3"} {
		tmp := filepath.Join(dir2, "real.tmp")
		writeFile(t, tmp, content)
		if err := os.Rename(tmp, real); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return applied() == content })
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
	waitFor(t, func() bool { return applied() == "v4" })
}

func TestKubernetesSwap(t *testing.T) {
	dir := t.TempDir()
	k8sSwap(t, dir, 1, "v1")
	cfg := filepath.Join(dir, "config.yaml")
	applied, _ := watch(t, func() []string { return []string{cfg} })
	waitFor(t, func() bool { return applied() == "v1" })
	for n := 2; n <= 4; n++ {
		want := fmt.Sprintf("v%d", n)
		k8sSwap(t, dir, n, want)
		waitFor(t, func() bool { return applied() == want })
	}
}

func TestRapidSaves(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	writeFile(t, cfg, "init")
	applied, _ := watch(t, func() []string { return []string{cfg} })
	for i := 0; i < 10; i++ {
		writeFile(t, cfg, fmt.Sprintf("v%d", i))
	}
	waitFor(t, func() bool { return applied() == "v9" })
}

func TestRunNoLeak(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	writeFile(t, cfg, "a")
	baseline := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		started := make(chan struct{}, 1)
		w := New(Options{
			Paths:        func() []string { return []string{cfg} },
			Debounce:     20 * time.Millisecond,
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
		<-started
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	}
	waitFor(t, func() bool { return runtime.NumGoroutine() <= baseline })
}
