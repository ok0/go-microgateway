package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

func buildBinary(t *testing.T, ldflags string) string {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not available")
	}
	bin := filepath.Join(t.TempDir(), "microgateway")
	args := []string{"build", "-o", bin}
	if ldflags != "" {
		args = append(args, "-ldflags", ldflags)
	}
	args = append(args, ".")
	cmd := exec.Command("go", args...)
	cmd.Env = append(os.Environ(), "GOWORK=off")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

func runBinary(t *testing.T, bin string, args ...string) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	cmd := exec.Command(bin, args...)
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	var ee *exec.ExitError
	switch {
	case err == nil:
		return out.String(), errb.String(), 0
	case errors.As(err, &ee):
		return out.String(), errb.String(), ee.ExitCode()
	default:
		t.Fatal(err)
		return "", "", -1
	}
}

const memcacheDoc = `version: 1
listeners:
  - name: public
    address: "127.0.0.1:0"
upstreams:
  - name: up
    hosts:
      - url: "http://127.0.0.1:9001"
stores:
  - name: shared
    type: memcache
routes:
  - name: rl
    match:
      path: /rl
    upstream: up
`

func TestBinary(t *testing.T) {
	t.Run("version", func(t *testing.T) {
		bin := buildBinary(t, "")
		out, _, code := runBinary(t, bin, "version")
		if code != 0 {
			t.Fatalf("exit %d, want 0", code)
		}
		if want := "microgateway dev (commit none, built unknown)\n"; out != want {
			t.Fatalf("stdout %q, want %q", out, want)
		}
	})

	t.Run("version ldflags", func(t *testing.T) {
		bin := buildBinary(t, "-X github.com/ok0/go-microgateway/internal/cli.version=1.2.3 -X github.com/ok0/go-microgateway/internal/cli.commit=abc123 -X github.com/ok0/go-microgateway/internal/cli.date=2026-01-02")
		out, _, code := runBinary(t, bin, "version")
		if code != 0 {
			t.Fatalf("exit %d, want 0", code)
		}
		if want := "microgateway 1.2.3 (commit abc123, built 2026-01-02)\n"; out != want {
			t.Fatalf("stdout %q, want %q", out, want)
		}
	})

	t.Run("no command", func(t *testing.T) {
		bin := buildBinary(t, "")
		_, errs, code := runBinary(t, bin)
		if code != 2 {
			t.Fatalf("exit %d, want 2", code)
		}
		if !strings.Contains(errs, "usage: microgateway") {
			t.Fatalf("stderr %q, want usage text", errs)
		}
	})

	t.Run("validate", func(t *testing.T) {
		bin := buildBinary(t, "")
		pw := uniqueSecret(t, "pw-")
		valid := instanceDoc(docOpts{
			Backend:   backend(t),
			RedisAddr: "127.0.0.1:1",
			Prefix:    uniquePrefix(t),
			Password:  true,
			UseStore:  true,
		})
		valid = strings.ReplaceAll(valid, "${MG_TEST_REDIS_PASSWORD}", pw)
		out, errs, code := runBinary(t, bin, "validate", "-config", writeDoc(t, valid))
		if code != 0 {
			t.Fatalf("valid document: exit %d, want 0: %s", code, errs)
		}
		if !strings.Contains(out, "configuration is valid") {
			t.Fatalf("valid document: stdout %q, want configuration is valid", out)
		}

		_, errs, code = runBinary(t, bin, "validate", "-config", writeDoc(t, memcacheDoc))
		if code != 1 {
			t.Fatalf("unknown store type: exit %d, want 1", code)
		}
		if !strings.Contains(errs, `store type "memcache" is not registered`) {
			t.Fatalf("unknown store type: stderr %q", errs)
		}
	})

	t.Run("signals", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("signals are not delivered to child processes on windows")
		}
		bin := buildBinary(t, "")
		path := writeDoc(t, instanceDoc(docOpts{
			Backend:   backend(t),
			RedisAddr: "127.0.0.1:1",
			Prefix:    uniquePrefix(t),
		}))
		errb := &syncBuffer{}
		cmd := exec.Command(bin, "run", "-config", path, "-log-format", "text")
		cmd.Stdout = io.Discard
		cmd.Stderr = errb
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill() })
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()

		eventually(t, func() bool { return strings.Contains(errb.String(), "msg=listening") })
		if err := cmd.Process.Signal(syscall.SIGHUP); err != nil {
			t.Fatal(err)
		}
		eventually(t, func() bool { return strings.Contains(errb.String(), "reload requested") })
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("run exited with %v, want 0: %s", err, errb.String())
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("run did not exit within 10s: %s", errb.String())
		}
		if !strings.Contains(errb.String(), "msg=stopped") {
			t.Fatalf("stderr does not report stopped: %s", errb.String())
		}
	})
}
