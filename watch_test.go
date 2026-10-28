package lychgate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func watchDoc(backendURL, route string) string {
	return "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: r
    match:
      path: %s
    upstream: up
`, backendURL, route)
}

func awaitWatch(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("WatchFile did not return")
	}
	return nil
}

func TestWatchFileMissingPath(t *testing.T) {
	n := newMemNetwork()
	be := n.serve(t, echoHandler("m"))
	g := n.gateway(t, watchDoc(be.URL, "/x"))
	done := make(chan error, 1)
	go func() {
		done <- g.WatchFile(context.Background(), filepath.Join(t.TempDir(), "missing.yaml"))
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("WatchFile of a missing path returned nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WatchFile did not return for a missing path")
	}
}

func TestWatchFilePicksUpEarlierChange(t *testing.T) {
	n := newMemNetwork()
	beA := n.serve(t, echoHandler("A"))
	beB := n.serve(t, echoHandler("B"))
	g := n.gateway(t, watchDoc(beA.URL, "/x"))
	path := writeConfig(t, t.TempDir(), watchDoc(beB.URL, "/x"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- g.WatchFile(ctx, path) }()

	eventually(t, func() bool { return servedBy(g, "/x") == "B" })
	cancel()
	if err := awaitWatch(t, done); err != nil {
		t.Fatalf("WatchFile = %v, want nil", err)
	}
}

func TestWatchFileCancel(t *testing.T) {
	n := newMemNetwork()
	be := n.serve(t, echoHandler("c"))
	g := n.gateway(t, watchDoc(be.URL, "/x"))
	path := writeConfig(t, t.TempDir(), watchDoc(be.URL, "/x"))
	base := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- g.WatchFile(ctx, path) }()
	eventually(t, func() bool {
		_, _, u := counters(g)
		return u == 1
	})

	cancel()
	if err := awaitWatch(t, done); err != nil {
		t.Fatalf("WatchFile after cancel = %v, want nil", err)
	}
	eventually(t, func() bool { return runtime.NumGoroutine() <= base })
}

func TestWatchFileReturnsErrClosed(t *testing.T) {
	n := newMemNetwork()
	beA := n.serve(t, echoHandler("A"))
	beB := n.serve(t, echoHandler("B"))
	g := n.gateway(t, watchDoc(beA.URL, "/x"))
	dir := t.TempDir()
	path := writeConfig(t, dir, watchDoc(beA.URL, "/x"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- g.WatchFile(ctx, path) }()
	eventually(t, func() bool {
		_, _, u := counters(g)
		return u == 1
	})

	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	writeConfig(t, dir, watchDoc(beB.URL, "/x"))
	if err := awaitWatch(t, done); !errors.Is(err, ErrClosed) {
		t.Fatalf("WatchFile = %v, want ErrClosed", err)
	}
}

func TestWatchFileAtomicRename(t *testing.T) {
	n := newMemNetwork()
	be := n.serve(t, echoHandler("ok"))
	dir := t.TempDir()
	path := writeConfig(t, dir, watchDoc(be.URL, "/v0"))
	g := n.gateway(t, watchDoc(be.URL, "/v0"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- g.WatchFile(ctx, path) }()
	eventually(t, func() bool {
		_, _, u := counters(g)
		return u == 1
	})

	tmp := filepath.Join(dir, "config.yaml.tmp")
	for i := 1; i <= 5; i++ {
		route := fmt.Sprintf("/v%d", i)
		if err := os.WriteFile(tmp, []byte(watchDoc(be.URL, route)), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, path); err != nil {
			t.Fatal(err)
		}
		eventually(t, func() bool { return servedBy(g, route) == "ok" })
		if s, _, _ := counters(g); s != float64(i) {
			t.Fatalf("success after rename %d = %v, want %d", i, s, i)
		}
	}

	cancel()
	if err := awaitWatch(t, done); err != nil {
		t.Fatalf("WatchFile = %v, want nil", err)
	}
}
