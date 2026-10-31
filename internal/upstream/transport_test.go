package upstream

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/testwait"
)

var errDialProbe = errors.New("probe")

type dialResult struct {
	conn net.Conn
	err  error
}

func dueWithin(t *testing.T, fake *clock.Fake, n int, within time.Duration, what string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	due := make(chan struct{})
	go func() {
		defer close(due)
		_ = fake.WaitDue(ctx, n, within)
	}()
	testwait.Done(t, what, due, func() string {
		return fmt.Sprintf("fake waiters=%d due within %v=%d", fake.Waiters(), within, fake.Due(within))
	})
}

func recvDial(t *testing.T, ch <-chan dialResult, what string) dialResult {
	t.Helper()
	r, _ := testwait.Recv(t, what, ch, nil)
	return r
}

// transportClocks lists the clocks under test; connect is a limit meant to fire (used only with dialers that hang until it does). Tests where the limit must not fire use an hour under both clocks.
func transportClocks(t *testing.T) []struct {
	name    string
	clk     clock.Clock
	fake    *clock.Fake
	connect time.Duration
} {
	t.Helper()
	f := clock.NewFake(testStart)
	return []struct {
		name    string
		clk     clock.Clock
		fake    *clock.Fake
		connect time.Duration
	}{
		// Real clock: a hung dialer must hit the 20ms connect limit on the production clock.
		{"real", clock.Real(), nil, 20 * time.Millisecond},
		{"fake", f, f, time.Hour},
	}
}

func cfgWithConnect(d time.Duration) config.Upstream {
	return config.Upstream{
		Name:     "u",
		Timeouts: config.Timeouts{Connect: config.Duration(d), ResponseHeader: config.Duration(time.Hour)},
	}
}

func TestDialContextHasNoDeadline(t *testing.T) {
	for _, c := range transportClocks(t) {
		t.Run(c.name, func(t *testing.T) {
			var sawDeadline, sawDone bool
			dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
				_, sawDeadline = ctx.Deadline()
				sawDone = ctx.Done() != nil
				return nil, errDialProbe
			}
			tr := NewTransport(cfgWithConnect(time.Hour), dial, c.clk)

			if _, err := tr.inner.DialContext(context.Background(), "tcp", "backend.test:80"); err != errDialProbe {
				t.Fatalf("err = %v, want the dialer's error by identity", err)
			}
			if sawDeadline || !sawDone {
				t.Fatalf("dialer saw deadline=%v done=%v, want no deadline and a Done channel", sawDeadline, sawDone)
			}

			timed, cancelTimed := context.WithTimeout(context.Background(), time.Hour)
			defer cancelTimed()
			if _, err := tr.inner.DialContext(timed, "tcp", "backend.test:80"); err != errDialProbe {
				t.Fatalf("err = %v, want the dialer's error by identity", err)
			}
			if sawDeadline || !sawDone {
				t.Fatalf("dialer saw deadline=%v done=%v, want no deadline and a Done channel", sawDeadline, sawDone)
			}

			parent, cancelParent := context.WithCancel(context.Background())
			defer cancelParent()
			started := make(chan struct{})
			blocking := func(ctx context.Context, _, _ string) (net.Conn, error) {
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			tr = NewTransport(cfgWithConnect(time.Hour), blocking, c.clk)
			ch := make(chan dialResult, 1)
			go func() {
				conn, err := tr.inner.DialContext(parent, "tcp", "backend.test:80")
				ch <- dialResult{conn, err}
			}()
			testwait.Done(t, "dialer started", started, nil)
			if c.fake != nil {
				dueWithin(t, c.fake, 1, time.Hour, "connect timer armed")
			}
			cancelParent()
			r := recvDial(t, ch, "dial to return after parent cancel")
			if !errors.Is(r.err, context.Canceled) {
				t.Fatalf("err = %v, want context.Canceled", r.err)
			}
			if errors.Is(r.err, errConnectTimeout) {
				t.Fatalf("err = %v, parent cancellation must not report errConnectTimeout", r.err)
			}
			if c.fake != nil {
				if n := c.fake.Waiters(); n != 0 {
					t.Fatalf("Waiters() = %d after parent cancel, want 0 (connect timer stopped)", n)
				}
				if now := c.fake.Now(); !now.Equal(testStart) {
					t.Fatalf("Now() = %v, want %v (the connect limit played no part)", now, testStart)
				}
			}
		})
	}
}

func TestConnectTimeoutReplacesDialError(t *testing.T) {
	variants := []struct {
		name string
		err  error
	}{
		{"deadline exceeded", context.DeadlineExceeded},
		{"i/o timeout", &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}},
		{"plain", errors.New("dialer gave up")},
	}
	for _, c := range transportClocks(t) {
		for _, v := range variants {
			t.Run(c.name+"/"+v.name, func(t *testing.T) {
				started := make(chan struct{})
				dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
					close(started)
					<-ctx.Done()
					return nil, v.err
				}
				tr := NewTransport(cfgWithConnect(c.connect), dial, c.clk)
				ch := make(chan dialResult, 1)
				go func() {
					conn, err := tr.inner.DialContext(context.Background(), "tcp", "backend.test:80")
					ch <- dialResult{conn, err}
				}()
				testwait.Done(t, "dialer started", started, nil)
				if c.fake != nil {
					dueWithin(t, c.fake, 1, time.Hour, "connect timer armed")
					c.fake.Advance(time.Hour)
				}
				r := recvDial(t, ch, "dial to return after connect limit")
				if r.conn != nil {
					t.Fatal("conn != nil, want nil")
				}
				var oe *net.OpError
				if !errors.As(r.err, &oe) || oe.Op != "dial" || oe.Err != errConnectTimeout {
					t.Fatalf("err = %v, want *net.OpError{Op: dial, Err: errConnectTimeout}", r.err)
				}
				if !errors.Is(r.err, errConnectTimeout) {
					t.Fatalf("err = %v, want errConnectTimeout", r.err)
				}
				if errors.Is(r.err, v.err) {
					t.Fatalf("err = %v, dialer error %v must be replaced", r.err, v.err)
				}
				if status, _ := httperr.FromError(r.err); status != 502 {
					t.Errorf("status = %d, want 502", status)
				}
			})
		}
	}
}

func TestDialErrorBeforeConnectTimer(t *testing.T) {
	for _, c := range transportClocks(t) {
		t.Run(c.name, func(t *testing.T) {
			refused := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
			dial := func(context.Context, string, string) (net.Conn, error) {
				return nil, refused
			}
			tr := NewTransport(cfgWithConnect(time.Hour), dial, c.clk)
			_, err := tr.inner.DialContext(context.Background(), "tcp", "backend.test:80")
			if err != refused {
				t.Fatalf("err = %v, want the dialer's error by identity", err)
			}
			if errors.Is(err, errConnectTimeout) {
				t.Fatalf("err = %v, must not report errConnectTimeout", err)
			}
			if c.fake != nil {
				if n := c.fake.Waiters(); n != 0 {
					t.Fatalf("Waiters() = %d after return, want 0", n)
				}
				c.fake.Advance(2 * time.Hour)
				if n := c.fake.Waiters(); n != 0 {
					t.Fatalf("Waiters() = %d after Advance, want 0", n)
				}
			}
		})
	}
}
