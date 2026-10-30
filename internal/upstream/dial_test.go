package upstream

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testwait"
)

type stubResolver struct {
	mu          sync.Mutex
	addrs       map[string][]net.IPAddr
	err         error
	block       bool
	calls       int
	sawDeadline bool
}

func (r *stubResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	r.mu.Lock()
	r.calls++
	_, ok := ctx.Deadline()
	r.sawDeadline = r.sawDeadline || ok
	block, err, addrs := r.block, r.err, r.addrs[host]
	r.mu.Unlock()
	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, err
	}
	return addrs, nil
}

func ipAddrs(ss ...string) []net.IPAddr {
	out := make([]net.IPAddr, 0, len(ss))
	for _, s := range ss {
		out = append(out, net.IPAddr{IP: net.ParseIP(s)})
	}
	return out
}

type addrDialer struct {
	t           *testing.T
	mu          sync.Mutex
	order       []string
	sawDeadline bool
	started     map[string]chan struct{}
	ends        map[string]chan struct{}
	behave      map[string]string
}

func newAddrDialer(t *testing.T, behave map[string]string) *addrDialer {
	t.Helper()
	d := &addrDialer{
		t:       t,
		started: map[string]chan struct{}{},
		ends:    map[string]chan struct{}{},
		behave:  behave,
	}
	for addr := range behave {
		d.started[addr] = make(chan struct{})
		d.ends[addr] = make(chan struct{})
	}
	return d
}

func (d *addrDialer) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	d.mu.Lock()
	d.order = append(d.order, addr)
	_, ok := ctx.Deadline()
	d.sawDeadline = d.sawDeadline || ok
	behavior, known := d.behave[addr]
	d.mu.Unlock()
	if !known {
		d.t.Errorf("unexpected dial %s", addr)
		behavior = "refuse"
	}
	switch behavior {
	case "hang":
		close(d.started[addr])
		<-ctx.Done()
		close(d.ends[addr])
		return nil, ctx.Err()
	case "refuse":
		return nil, &net.OpError{Op: "dial", Net: network, Err: errors.New("refused " + addr)}
	default:
		c1, c2 := net.Pipe()
		d.t.Cleanup(func() {
			_ = c1.Close()
			_ = c2.Close()
		})
		return c1, nil
	}
}

func orderOf(d *addrDialer) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.order...)
}

func awaitStarted(t *testing.T, d *addrDialer, addr string) {
	t.Helper()
	testwait.Done(t, "dial to "+addr+" started", d.started[addr], func() string {
		return fmt.Sprintf("dialed so far %v", orderOf(d))
	})
}

func awaitEnded(t *testing.T, d *addrDialer, addr string) {
	t.Helper()
	testwait.Done(t, "dial to "+addr+" ended", d.ends[addr], func() string {
		return fmt.Sprintf("dialed so far %v", orderOf(d))
	})
}

func TestConnectFailsOverWithinConnectTimeout(t *testing.T) {
	f := clock.NewFake(testStart)
	res := &stubResolver{addrs: map[string][]net.IPAddr{"backend.test": ipAddrs("10.0.0.1", "10.0.0.2")}}
	d := newAddrDialer(t, map[string]string{"10.0.0.1:80": "hang", "10.0.0.2:80": "ok"})
	dial := connectTimeout(d.dial, res, 5*time.Second, f)
	ch := make(chan dialResult, 1)
	go func() {
		conn, err := dial(context.Background(), "tcp", "backend.test:80")
		ch <- dialResult{conn, err}
	}()
	awaitStarted(t, d, "10.0.0.1:80")
	dueWithin(t, f, 2, 5*time.Second, "connect timer and first address window armed")
	if n := f.Due(2500 * time.Millisecond); n != 1 {
		t.Fatalf("Due(2.5s) = %d, want 1 (first address window); orderOf=%v waiters=%d", n, orderOf(d), f.Waiters())
	}
	if n := f.Due(2500*time.Millisecond - time.Nanosecond); n != 0 {
		t.Fatalf("Due(2.5s-1ns) = %d, want 0; orderOf=%v", n, orderOf(d))
	}
	f.Advance(2500 * time.Millisecond)
	r := recvDial(t, ch, "dial to fail over to the second address")
	if r.err != nil || r.conn == nil {
		t.Fatalf("dial = (%v, %v), want a connection to the second address", r.conn, r.err)
	}
	if got, want := orderOf(d), []string{"10.0.0.1:80", "10.0.0.2:80"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("dial order = %v, want %v", got, want)
	}
	awaitEnded(t, d, "10.0.0.1:80")
	if d.sawDeadline {
		t.Error("dialer saw a deadline, want none")
	}
	if res.sawDeadline {
		t.Error("resolver saw a deadline, want none")
	}
	if n := f.Waiters(); n != 0 {
		t.Fatalf("Waiters() = %d, want 0", n)
	}
}

func TestConnectSecondAddressRefusedAfterWindow(t *testing.T) {
	f := clock.NewFake(testStart)
	res := &stubResolver{addrs: map[string][]net.IPAddr{"backend.test": ipAddrs("10.0.0.1", "10.0.0.2")}}
	d := newAddrDialer(t, map[string]string{"10.0.0.1:80": "hang", "10.0.0.2:80": "refuse"})
	dial := connectTimeout(d.dial, res, 5*time.Second, f)
	ch := make(chan dialResult, 1)
	go func() {
		conn, err := dial(context.Background(), "tcp", "backend.test:80")
		ch <- dialResult{conn, err}
	}()
	awaitStarted(t, d, "10.0.0.1:80")
	dueWithin(t, f, 2, 5*time.Second, "connect timer and first address window armed")
	f.Advance(2500 * time.Millisecond)
	r := recvDial(t, ch, "dial to return after the first address window")
	if !errors.Is(r.err, errConnectTimeout) {
		t.Fatalf("err = %v, want errConnectTimeout from the first address window", r.err)
	}
	if got := orderOf(d); len(got) != 2 {
		t.Fatalf("dial order = %v, want both addresses dialed", got)
	}
	if n := f.Waiters(); n != 0 {
		t.Fatalf("Waiters() = %d, want 0", n)
	}
	if elapsed := f.Now().Sub(testStart); elapsed != 2500*time.Millisecond {
		t.Fatalf("elapsed = %v, want 2.5s (the overall 5s limit must not have run)", elapsed)
	}
}

func TestConnectLastAddressBoundedByConnectTimer(t *testing.T) {
	f := clock.NewFake(testStart)
	res := &stubResolver{addrs: map[string][]net.IPAddr{"backend.test": ipAddrs("10.0.0.1")}}
	d := newAddrDialer(t, map[string]string{"10.0.0.1:80": "hang"})
	dial := connectTimeout(d.dial, res, 5*time.Second, f)
	ch := make(chan dialResult, 1)
	go func() {
		conn, err := dial(context.Background(), "tcp", "backend.test:80")
		ch <- dialResult{conn, err}
	}()
	awaitStarted(t, d, "10.0.0.1:80")
	dueWithin(t, f, 1, 5*time.Second, "connect timer armed")
	if n := f.Waiters(); n != 1 {
		t.Fatalf("Waiters() = %d, want 1 (no per-address timer for a lone address)", n)
	}
	f.Advance(5 * time.Second)
	r := recvDial(t, ch, "dial to return after the connect limit")
	var oe *net.OpError
	if !errors.As(r.err, &oe) || oe.Op != "dial" || oe.Err != errConnectTimeout {
		t.Fatalf("err = %v, want *net.OpError{Op: dial, Err: errConnectTimeout}", r.err)
	}
	if r.conn != nil {
		t.Fatal("conn != nil, want nil")
	}
	if n := f.Waiters(); n != 0 {
		t.Fatalf("Waiters() = %d, want 0", n)
	}
}

func TestAddrShare(t *testing.T) {
	cases := []struct {
		name      string
		remaining time.Duration
		left      int
		want      time.Duration
		wantOK    bool
	}{
		{"5s over 2", 5 * time.Second, 2, 2500 * time.Millisecond, true},
		{"5s over 3", 5 * time.Second, 3, 2 * time.Second, true},
		{"3s over 2", 3 * time.Second, 2, 2 * time.Second, true},
		{"1s over 2", time.Second, 2, 0, false},
		{"10s over 1", 10 * time.Second, 1, 0, false},
		{"10s over 4", 10 * time.Second, 4, 2500 * time.Millisecond, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := clock.NewFake(testStart)
			ctx, cancel := clock.WithTimeout(context.Background(), f, c.remaining)
			defer cancel()
			got, ok := addrShare(ctx, f, c.left)
			if got != c.want || ok != c.wantOK {
				t.Fatalf("addrShare = (%v, %v), want (%v, %v)", got, ok, c.want, c.wantOK)
			}
		})
	}
	t.Run("no deadline", func(t *testing.T) {
		f := clock.NewFake(testStart)
		got, ok := addrShare(context.Background(), f, 3)
		if got != 0 || ok {
			t.Fatalf("addrShare = (%v, %v), want (0, false)", got, ok)
		}
	})
	t.Run("deadline now", func(t *testing.T) {
		f := clock.NewFake(testStart)
		ctx, cancel := clock.WithTimeout(context.Background(), f, 0)
		defer cancel()
		got, ok := addrShare(ctx, f, 3)
		if got != 0 || ok {
			t.Fatalf("addrShare = (%v, %v), want (0, false)", got, ok)
		}
	})
}

func TestConnectFallsBackToOtherFamily(t *testing.T) {
	f := clock.NewFake(testStart)
	res := &stubResolver{addrs: map[string][]net.IPAddr{"backend.test": ipAddrs("2001:db8::1", "10.0.0.1")}}
	d := newAddrDialer(t, map[string]string{"[2001:db8::1]:80": "hang", "10.0.0.1:80": "ok"})
	dial := connectTimeout(d.dial, res, 5*time.Second, f)
	ch := make(chan dialResult, 1)
	go func() {
		conn, err := dial(context.Background(), "tcp", "backend.test:80")
		ch <- dialResult{conn, err}
	}()
	awaitStarted(t, d, "[2001:db8::1]:80")
	dueWithin(t, f, 1, fallbackDelay, "fallback delay armed")
	f.Advance(fallbackDelay)
	r := recvDial(t, ch, "dial to succeed over the other family")
	if r.err != nil || r.conn == nil {
		t.Fatalf("dial = (%v, %v), want a connection over IPv4", r.conn, r.err)
	}
	if got, want := orderOf(d), []string{"[2001:db8::1]:80", "10.0.0.1:80"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("dial order = %v, want %v", got, want)
	}
	awaitEnded(t, d, "[2001:db8::1]:80")
	if n := f.Waiters(); n != 0 {
		t.Fatalf("Waiters() = %d, want 0", n)
	}
}

func TestConnectFallbackStartsWhenFirstFamilyFails(t *testing.T) {
	f := clock.NewFake(testStart)
	res := &stubResolver{addrs: map[string][]net.IPAddr{"backend.test": ipAddrs("2001:db8::1", "10.0.0.1")}}
	d := newAddrDialer(t, map[string]string{"[2001:db8::1]:80": "refuse", "10.0.0.1:80": "ok"})
	dial := connectTimeout(d.dial, res, 5*time.Second, f)
	conn, err := dial(context.Background(), "tcp", "backend.test:80")
	if err != nil || conn == nil {
		t.Fatalf("dial = (%v, %v), want a connection over IPv4", conn, err)
	}
	if got, want := orderOf(d), []string{"[2001:db8::1]:80", "10.0.0.1:80"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("dial order = %v, want %v", got, want)
	}
	if now := f.Now(); !now.Equal(testStart) {
		t.Fatalf("Now() = %v, want %v (no fallback delay waited)", now, testStart)
	}
	if n := f.Waiters(); n != 0 {
		t.Fatalf("Waiters() = %d, want 0", n)
	}
}

func TestConnectAllAddressesFail(t *testing.T) {
	f := clock.NewFake(testStart)
	res := &stubResolver{addrs: map[string][]net.IPAddr{"backend.test": ipAddrs("10.0.0.1", "10.0.0.2")}}
	d := newAddrDialer(t, map[string]string{"10.0.0.1:80": "refuse", "10.0.0.2:80": "refuse"})
	dial := connectTimeout(d.dial, res, 5*time.Second, f)
	_, err := dial(context.Background(), "tcp", "backend.test:80")
	if err == nil || err.Error() != "dial tcp: refused 10.0.0.1:80" {
		t.Fatalf("err = %v, want the first address's error", err)
	}
	if got := orderOf(d); len(got) != 2 {
		t.Fatalf("dial order = %v, want both addresses dialed", got)
	}
	if n := f.Waiters(); n != 0 {
		t.Fatalf("Waiters() = %d, want 0", n)
	}
}

func TestConnectResolution(t *testing.T) {
	t.Run("literal address skips lookup", func(t *testing.T) {
		f := clock.NewFake(testStart)
		res := &stubResolver{}
		d := newAddrDialer(t, map[string]string{"10.0.0.9:80": "ok"})
		dial := connectTimeout(d.dial, res, 5*time.Second, f)
		conn, err := dial(context.Background(), "tcp", "10.0.0.9:80")
		if err != nil || conn == nil {
			t.Fatalf("dial = (%v, %v), want a connection", conn, err)
		}
		if res.calls != 0 {
			t.Fatalf("resolver calls = %d, want 0", res.calls)
		}
	})

	t.Run("lookup error", func(t *testing.T) {
		f := clock.NewFake(testStart)
		res := &stubResolver{err: &net.DNSError{Err: "no such host", Name: "backend.test", IsNotFound: true}}
		d := newAddrDialer(t, map[string]string{})
		dial := connectTimeout(d.dial, res, 5*time.Second, f)
		conn, err := dial(context.Background(), "tcp", "backend.test:80")
		if conn != nil {
			t.Fatal("conn != nil, want nil")
		}
		var de *net.DNSError
		if !errors.As(err, &de) {
			t.Fatalf("err = %v, want *net.DNSError", err)
		}
		if got := orderOf(d); len(got) != 0 {
			t.Fatalf("dial order = %v, want no dial", got)
		}
	})

	t.Run("no suitable address", func(t *testing.T) {
		f := clock.NewFake(testStart)
		res := &stubResolver{addrs: map[string][]net.IPAddr{"backend.test": ipAddrs("2001:db8::1")}}
		d := newAddrDialer(t, map[string]string{})
		dial := connectTimeout(d.dial, res, 5*time.Second, f)
		_, err := dial(context.Background(), "tcp4", "backend.test:80")
		if err == nil || !strings.Contains(err.Error(), "no suitable address found") {
			t.Fatalf("err = %v, want \"no suitable address found\"", err)
		}
		if got := orderOf(d); len(got) != 0 {
			t.Fatalf("dial order = %v, want no dial", got)
		}
	})

	t.Run("lookup counts toward the connect limit", func(t *testing.T) {
		f := clock.NewFake(testStart)
		res := &stubResolver{block: true}
		d := newAddrDialer(t, map[string]string{})
		dial := connectTimeout(d.dial, res, 5*time.Second, f)
		ch := make(chan dialResult, 1)
		go func() {
			conn, err := dial(context.Background(), "tcp", "backend.test:80")
			ch <- dialResult{conn, err}
		}()
		dueWithin(t, f, 1, 5*time.Second, "connect timer armed before lookup")
		f.Advance(5 * time.Second)
		r := recvDial(t, ch, "dial to return after the connect limit during lookup")
		if !errors.Is(r.err, errConnectTimeout) {
			t.Fatalf("err = %v, want errConnectTimeout", r.err)
		}
		if res.sawDeadline {
			t.Error("resolver saw a deadline, want none")
		}
		if n := f.Waiters(); n != 0 {
			t.Fatalf("Waiters() = %d, want 0", n)
		}
	})
}

func TestDefaultDialerDialsLiteralAddress(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	tr := NewTransport(cfgWithConnect(time.Hour), nil, clock.NewFake(testStart))
	conn, err := tr.DialContext(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	_ = conn.Close()
}
