package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/ok0/go-microgateway/internal/testwait"
)

func buildBinary(t *testing.T, ldflags string) string {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not available")
	}
	bin := filepath.Join(t.TempDir(), "microgateway")
	// VCS stamping is off so the version subtests do not depend on tags of the checkout.
	args := []string{"build", "-buildvcs=false", "-o", bin}
	if ldflags != "" {
		args = append(args, "-ldflags", ldflags)
	}
	args = append(args, ".")
	cmd := exec.Command("go", args...)
	// The binary is built with the repository's workspace, against the core in the checkout.
	work, err := filepath.Abs(filepath.Join("..", "..", "..", "go.work"))
	if err != nil {
		t.Fatal(err)
	}
	cmd.Env = append(os.Environ(), "GOWORK="+work)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

func runBinary(t *testing.T, bin string, args ...string) (string, string, int) {
	t.Helper()
	var out, errb syncBuffer
	cmd := exec.Command(bin, args...)
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	err, _ := testwait.Recv(t, "microgateway "+strings.Join(args, " ")+" to exit", done, func() string {
		return "stdout:\n" + out.String() + "\nstderr:\n" + errb.String()
	})
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
	bin := buildBinary(t, "")
	t.Run("version", func(t *testing.T) {
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
		_, errs, code := runBinary(t, bin)
		if code != 2 {
			t.Fatalf("exit %d, want 2", code)
		}
		if !strings.Contains(errs, "usage: microgateway") {
			t.Fatalf("stderr %q, want usage text", errs)
		}
	})

	t.Run("validate", func(t *testing.T) {
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
		path := writeDoc(t, instanceDoc(docOpts{
			Backend:   backend(t),
			RedisAddr: "127.0.0.1:1",
			Prefix:    uniquePrefix(t),
		}))
		outb := &syncBuffer{}
		errb := &syncBuffer{}
		cmd := exec.Command(bin, "run", "-config", path, "-log-format", "text")
		cmd.Stdout = outb
		cmd.Stderr = errb
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill() })
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()

		child := func() string {
			return "stdout:\n" + outb.String() + "\nstderr:\n" + errb.String()
		}
		eventually(t, "child to log listening", func() bool { return strings.Contains(errb.String(), "msg=listening") }, child)
		if err := cmd.Process.Signal(syscall.SIGHUP); err != nil {
			t.Fatal(err)
		}
		eventually(t, "child to log reload requested", func() bool { return strings.Contains(errb.String(), "reload requested") }, child)
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		err, _ := testwait.Recv(t, "child to exit after SIGTERM", done, child)
		if err != nil {
			t.Fatalf("run exited with %v, want 0: %s", err, errb.String())
		}
		if !strings.Contains(errb.String(), "msg=stopped") {
			t.Fatalf("stderr does not report stopped: %s", errb.String())
		}
	})
}
