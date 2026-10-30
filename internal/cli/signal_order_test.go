//go:build unix

package cli

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testwait"
)

type signalWriter struct {
	mu   sync.Mutex
	buf  bytes.Buffer
	hook map[string]func()
}

func (w *signalWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.buf.Write(p)
	var fire []func()
	for match, f := range w.hook {
		if bytes.Contains(p, []byte(match)) {
			fire = append(fire, f)
			delete(w.hook, match)
		}
	}
	w.mu.Unlock()
	for _, f := range fire {
		f()
	}
	return len(p), nil
}

func (w *signalWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func TestRunSignalsSentAtReadyAreHandled(t *testing.T) {
	backend := echoBackend(t, "backend")
	path := writeTemp(t, lifecycleConfig("", backend.URL))

	t.Run("sigterm at ready", func(t *testing.T) {
		errb := &signalWriter{}
		done := make(chan int, 1)
		go func() {
			done <- Main(context.Background(), Env{
				Args:   []string{"run", "-config", path},
				Stderr: errb,
				Clock:  clock.NewFake(time.Now()),
				Ready:  func(Addrs) { _ = syscall.Kill(syscall.Getpid(), syscall.SIGTERM) },
			})
		}()
		code, _ := testwait.Recv(t, "run to return after the signals", done, errb.String)
		if code != 0 {
			t.Errorf("exit code = %d, want 0; stderr:\n%s", code, errb.String())
		}
		if !strings.Contains(errb.String(), "shutting down") {
			t.Errorf("stderr missing shutdown log; stderr:\n%s", errb.String())
		}
	})

	t.Run("sighup at listening then sigterm after reload", func(t *testing.T) {
		reloaded := make(chan struct{})
		errb := &signalWriter{hook: map[string]func(){
			"listening":        func() { _ = syscall.Kill(syscall.Getpid(), syscall.SIGHUP) },
			"reload requested": func() { close(reloaded) },
		}}
		done := make(chan int, 1)
		go func() {
			done <- Main(context.Background(), Env{
				Args:   []string{"run", "-config", path},
				Stderr: errb,
				Clock:  clock.NewFake(time.Now()),
				Ready: func(Addrs) {
					go func() {
						<-reloaded
						_ = syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
					}()
				},
			})
		}()
		code, _ := testwait.Recv(t, "run to return after the signals", done, errb.String)
		if code != 0 {
			t.Errorf("exit code = %d, want 0; stderr:\n%s", code, errb.String())
		}
		if !strings.Contains(errb.String(), "shutting down") {
			t.Errorf("stderr missing shutdown log; stderr:\n%s", errb.String())
		}
	})
}
