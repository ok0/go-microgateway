package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
)

// Option values that turn off every limit go-redis would run on the wall clock. The store's own limits replace them
// (newDialer, timedConn). Their meaning for the go-redis version in go.mod is pinned by tests.
const (
	// noSocketDeadline makes go-redis never set a read or write deadline on a connection.
	noSocketDeadline time.Duration = -2
	// ownDialer makes go-redis apply no dial limit of its own; the dialer bounds the dial.
	ownDialer time.Duration = -1
	// poolWaitForever outlasts any process, so only the caller's context ends a wait for a pool connection.
	poolWaitForever = 100 * 365 * 24 * time.Hour
	// keepIdleConns makes go-redis never close a connection for being idle.
	keepIdleConns time.Duration = -1
)

// connLimits are the connection limits of a client. A zero read or write limit means none.
type connLimits struct {
	dial, read, write time.Duration
}

// limitsOf returns the connection limits configured by cfg: 1s, 500ms and 500ms when unset. Values that
// configuration validation rejects keep the meaning go-redis gave them: zero selects 5s for dial and read and the
// read limit for write, a negative read or write limit means none, and a negative dial limit ends every dial at once.
func limitsOf(cfg *config.RedisStore) connLimits {
	l := connLimits{
		dial:  dur(cfg.DialTimeout, time.Second),
		read:  dur(cfg.ReadTimeout, 500*time.Millisecond),
		write: dur(cfg.WriteTimeout, 500*time.Millisecond),
	}
	if l.dial == 0 {
		l.dial = 5 * time.Second
	}
	switch {
	case l.read == 0:
		l.read = 5 * time.Second
	case l.read < 0:
		l.read = 0
	}
	switch {
	case l.write == 0:
		l.write = l.read
	case l.write < 0:
		l.write = 0
	}
	return l
}

// dialLimitReached is the cause of a dial that its limit ended.
type dialLimitReached struct{}

func (dialLimitReached) Error() string { return "redis: dial limit reached" }

// newDialer returns the dialer of a client whose limits run on clk. It dials network/addr and, when tlsCfg is set,
// runs the TLS handshake; one timer of clk bounds both by l.dial, as go-redis's own dialer bounded them. The
// connection it returns bounds every blocking read and write by l.read and l.write on clk.
func newDialer(clk clock.Clock, l connLimits, tlsCfg *tls.Config) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dctx, cancel := clock.WithTimeoutCause(ctx, clk, l.dial, dialLimitReached{})
		defer cancel()
		// The deadline of dctx is in the time of clk, so the dial and the handshake end only by cancellation.
		nctx := clock.WithoutDeadline(dctx)
		d := net.Dialer{KeepAliveConfig: net.KeepAliveConfig{Enable: true, Idle: 30 * time.Second, Interval: 5 * time.Second, Count: 3}}
		raw, err := d.DialContext(nctx, network, addr)
		if err != nil {
			return nil, dialFailure(dctx, network, err)
		}
		tc := newTimedConn(raw, clk)
		if tlsCfg == nil {
			tc.setLimits(l.read, l.write)
			return tc, nil
		}
		cfg := tlsCfg
		if cfg.ServerName == "" {
			cfg = cfg.Clone()
			cfg.ServerName = tlsServerName(addr)
		}
		tl := tls.Client(tc, cfg)
		if err := tl.HandshakeContext(nctx); err != nil {
			_ = tc.Close()
			return nil, dialFailure(dctx, network, err)
		}
		tc.setLimits(l.read, l.write)
		return tl, nil
	}
}

// tlsServerName returns the host part of addr, derived the way crypto/tls derives a server name from a dial address.
func tlsServerName(addr string) string {
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		return addr[:i]
	}
	return addr
}

// dialFailure returns err, or a timeout error without the address when the dial limit of dctx ended the dial.
func dialFailure(dctx context.Context, network string, err error) error {
	if _, ok := context.Cause(dctx).(dialLimitReached); ok {
		return &net.OpError{Op: "dial", Net: network, Err: os.ErrDeadlineExceeded}
	}
	return err
}

// timedConn bounds every blocking Read and Write of a connection by a timer of a clock. A timer that fires while its
// operation runs sets a deadline in the past on the connection, which ends the operation with a timeout error.
type timedConn struct {
	net.Conn
	rd, wr ioTimer
}

func newTimedConn(raw net.Conn, clk clock.Clock) *timedConn {
	c := &timedConn{Conn: raw}
	c.rd.clk, c.rd.set = clk, raw.SetReadDeadline
	c.wr.clk, c.wr.set = clk, raw.SetWriteDeadline
	return c
}

// setLimits sets the read and write limits. It is called before the connection is in use; zero means no limit.
func (c *timedConn) setLimits(read, write time.Duration) {
	c.rd.limit, c.wr.limit = read, write
}

func (c *timedConn) Read(b []byte) (int, error) {
	c.rd.start()
	n, err := c.Conn.Read(b)
	c.rd.stop()
	return n, err
}

func (c *timedConn) Write(b []byte) (int, error) {
	c.wr.start()
	n, err := c.Conn.Write(b)
	c.wr.stop()
	return n, err
}

func (c *timedConn) Close() error {
	c.rd.close()
	c.wr.close()
	return c.Conn.Close()
}

// SyscallConn returns the raw socket of the connection, so go-redis can check an idle connection's health.
func (c *timedConn) SyscallConn() (syscall.RawConn, error) {
	sc, ok := c.Conn.(syscall.Conn)
	if !ok {
		return nil, errors.New("redis: connection has no raw socket")
	}
	return sc.SyscallConn()
}

// ioTimer is the timer of one direction of a timedConn. At most one operation per direction runs at a time.
type ioTimer struct {
	clk   clock.Clock
	set   func(time.Time) error // sets the deadline of this direction on the raw connection
	limit time.Duration         // zero: no limit

	mu     sync.Mutex
	t      clock.Timer
	active bool      // an operation is running
	due    time.Time // when the running operation reaches its limit, in the time of clk
	fired  bool      // the timer set a past deadline during the running operation
}

// start arms the timer for an operation that is about to run.
func (x *ioTimer) start() {
	if x.limit <= 0 {
		return
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	x.active = true
	x.due = x.clk.Now().Add(x.limit)
	if x.t == nil {
		x.t = x.clk.AfterFunc(x.limit, x.fire)
	} else {
		x.t.Reset(x.limit)
	}
}

// stop disarms the timer after the operation returned. When the timer fired as the operation completed, the past
// deadline is cleared, so the connection stays usable.
func (x *ioTimer) stop() {
	if x.limit <= 0 {
		return
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	x.active = false
	if x.t != nil {
		x.t.Stop()
	}
	if x.fired {
		x.fired = false
		_ = x.set(time.Time{})
	}
}

// fire interrupts the running operation once its limit has passed. A late call for an operation that already
// returned, or for an earlier operation, does nothing.
func (x *ioTimer) fire() {
	x.mu.Lock()
	defer x.mu.Unlock()
	if !x.active || x.clk.Now().Before(x.due) {
		return
	}
	x.fired = true
	_ = x.set(time.Unix(1, 0))
}

// close disarms the timer for good.
func (x *ioTimer) close() {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.active = false
	if x.t != nil {
		x.t.Stop()
	}
}
