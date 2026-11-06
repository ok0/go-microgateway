package cli

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/testwait"
)

func limitsRunConfig(backend string) string {
	return fmt.Sprintf(`version: 1
listeners:
  - name: public
    address: "127.0.0.1:0"
    read_header_timeout: 2s
    idle_timeout: 7s
admin:
  address: "127.0.0.1:0"
upstreams:
  - name: echo
    hosts:
      - url: %q
routes:
  - name: all
    match:
      path: /
      path_type: prefix
    upstream: echo
`, backend)
}

type watchConn struct {
	net.Conn
	read  atomic.Int64
	ended chan struct{}
	once  sync.Once
}

func newWatchConn(conn net.Conn) *watchConn {
	return &watchConn{Conn: conn, ended: make(chan struct{})}
}

func (c *watchConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.read.Add(int64(n))
	if err != nil {
		c.once.Do(func() { close(c.ended) })
	}
	return n, err
}

func stillOpen(t *testing.T, ended <-chan struct{}) {
	t.Helper()
	select {
	case <-ended:
		t.Fatal("connection ended before its limit")
	default:
	}
}

func TestRunServerLimitsOnClock(t *testing.T) {
	backend := echoBackend(t, "echo")

	t.Run("header", func(t *testing.T) {
		h := startRun(t, writeTemp(t, limitsRunConfig(backend.URL)))
		public := h.addrs.Listeners["public"].String()

		before := h.clk.Due(2 * time.Second)
		conn, err := net.Dial("tcp", public)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: "+public+"\r\n"); err != nil {
			t.Fatal(err)
		}
		awaitDue(t, h.clk, before+1, 2*time.Second, "header read limit armed", nil)
		wc := newWatchConn(conn)
		go func() { _, _ = io.Copy(io.Discard, wc) }()

		h.clk.Advance(2*time.Second - time.Nanosecond)
		stillOpen(t, wc.ended)
		h.clk.Advance(time.Nanosecond)
		testwait.Done(t, "partial-header connection to end at the header limit", wc.ended, nil)

		if code := h.stop(); code != 0 {
			t.Errorf("exit code = %d, want 0", code)
		}
	})

	t.Run("idle", func(t *testing.T) {
		h := startRun(t, writeTemp(t, limitsRunConfig(backend.URL)))
		public := h.addrs.Listeners["public"].String()

		var wc *watchConn
		tr := &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				var d net.Dialer
				conn, err := d.DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				wc = newWatchConn(conn)
				return wc, nil
			},
		}
		t.Cleanup(tr.CloseIdleConnections)
		client := &http.Client{Transport: tr, Timeout: testwait.Guard(t)}
		resp, err := client.Get("http://" + public + "/")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadAll(resp.Body); err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}

		var n int
		testwait.Until(t, "keep-alive idle limit armed on the housekeeping clock", func() bool {
			n = h.clk.HousekeepingWaiters()
			return n >= 2
		}, func() string { return fmt.Sprintf("housekeeping waiters: %d", h.clk.HousekeepingWaiters()) })
		received := wc.read.Load()

		h.clk.Advance(7*time.Second - time.Nanosecond)
		if c := h.clk.HousekeepingWaiters(); c != n {
			t.Fatalf("housekeeping waiters = %d before the idle limit, want %d", c, n)
		}
		stillOpen(t, wc.ended)
		h.clk.Advance(time.Nanosecond)
		testwait.Done(t, "idle keep-alive connection to end at the idle limit", wc.ended, nil)
		if got := wc.read.Load() - received; got != 0 {
			t.Errorf("bytes received after the idle limit = %d, want 0", got)
		}

		if code := h.stop(); code != 0 {
			t.Errorf("exit code = %d, want 0", code)
		}
	})
}
