package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	microgateway "github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

// holdPeer relays RESP2 traffic to a Redis server. Setup commands are relayed at once. While the peer holds, the
// server's reply to each data-plane command is kept until Release. Commands counts the data-plane commands the
// server has answered.
type holdPeer struct {
	ln       net.Listener
	target   string
	done     chan struct{}
	mu       sync.Mutex
	held     chan struct{}
	commands atomic.Int64
}

func newHoldPeer(t *testing.T, target string) *holdPeer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &holdPeer{ln: ln, target: target, done: make(chan struct{})}
	t.Cleanup(func() {
		close(p.done)
		_ = ln.Close()
	})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.handle(c)
		}
	}()
	return p
}

func (p *holdPeer) Addr() string { return p.ln.Addr().String() }

func (p *holdPeer) Commands() int64 { return p.commands.Load() }

// Hold makes the peer keep the replies to data-plane commands until Release.
func (p *holdPeer) Hold() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.held == nil {
		p.held = make(chan struct{})
	}
}

// Release sends the kept replies and relays later replies at once.
func (p *holdPeer) Release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.held != nil {
		close(p.held)
		p.held = nil
	}
}

func (p *holdPeer) handle(client net.Conn) {
	defer client.Close()
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		return
	}
	defer up.Close()
	cr, ur := bufio.NewReader(client), bufio.NewReader(up)
	for {
		raw, args, err := readRequest(cr)
		if err != nil {
			return
		}
		if _, err := up.Write(raw); err != nil {
			return
		}
		reply, err := readReply(ur)
		if err != nil {
			return
		}
		if !setupCommands[strings.ToUpper(args[0])] {
			p.mu.Lock()
			held := p.held
			p.mu.Unlock()
			p.commands.Add(1)
			if held != nil {
				select {
				case <-held:
				case <-p.done:
					return
				}
			}
		}
		if _, err := client.Write(reply); err != nil {
			return
		}
	}
}

// silentPeer accepts connections and never reads from them, so a large write to one of them stalls.
func silentPeer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu     sync.Mutex
		conns  []net.Conn
		closed bool
	)
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		closed = true
		for _, c := range conns {
			_ = c.Close()
		}
	})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetReadBuffer(16 << 10)
			}
			mu.Lock()
			if closed {
				_ = c.Close()
			} else {
				conns = append(conns, c)
			}
			mu.Unlock()
		}
	}()
	return ln.Addr().String()
}

// storeOnFake returns a store for addr with the given connection limits on a new fake clock.
func storeOnFake(t *testing.T, addr string, dial, read, write time.Duration, tlsCfg *config.RedisTLS) (*Store, *clock.Fake) {
	t.Helper()
	fake := clock.NewFake(miniStart)
	s, err := newFromConfig(&config.RedisStore{
		Mode:         "standalone",
		Addresses:    []string{addr},
		DialTimeout:  durPtr(dial),
		ReadTimeout:  durPtr(read),
		WriteTimeout: durPtr(write),
		TLS:          tlsCfg,
	}, nil, fake)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, fake
}

// takeAsync runs one Take of key on s with ctx and returns the channel that receives its error.
func takeAsync(s *Store, ctx context.Context, key string) <-chan error {
	errc := make(chan error, 1)
	go func() {
		_, err := s.Take(ctx, key, storeTB)
		errc <- err
	}()
	return errc
}

// awaitTimer waits until more than base timers of fake are due within d.
func awaitTimer(t *testing.T, fake *clock.Fake, base int, d time.Duration, what string) {
	t.Helper()
	testwait.Until(t, what, func() bool { return fake.Due(d) > base }, func() string {
		return fmt.Sprintf("timers due within %v: %d, want more than %d", d, fake.Due(d), base)
	})
}

// realStall waits until d of real time has passed. It is a lower bound: no limit on the wall clock may end a call
// meanwhile.
func realStall(t *testing.T, d time.Duration) {
	t.Helper()
	end := time.Now().Add(d)
	testwait.Until(t, d.String()+" of real time to pass", func() bool { return !time.Now().Before(end) }, nil)
}

// stillPending fails the test when errc already holds a result. Callers have checked that the only event that can
// end the call, a timer of a fake clock, is still pending.
func stillPending(t *testing.T, what string, errc <-chan error) {
	t.Helper()
	select {
	case err := <-errc:
		t.Fatalf("%s ended early: %v", what, err)
	default:
	}
}

// crossLimit advances fake to 1ns before limit, checks that a timer is still pending and the call has not ended,
// and then advances the last nanosecond. The timer under test was armed at the current fake time.
func crossLimit(t *testing.T, fake *clock.Fake, limit time.Duration, what string, errc <-chan error) {
	t.Helper()
	fake.Advance(limit - time.Nanosecond)
	if fake.Due(time.Nanosecond) < 1 {
		t.Fatalf("%s: no timer pending 1ns before the limit", what)
	}
	stillPending(t, what, errc)
	fake.Advance(time.Nanosecond)
}

// checkTimeout checks that err is the error Take returns for a connection limit.
func checkTimeout(t *testing.T, err error) {
	t.Helper()
	want := "redis: take: timeout: " + store.ErrUnavailable.Error()
	if err == nil || err.Error() != want || !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("error %v, want %q wrapping store.ErrUnavailable", err, want)
	}
}

// inUse returns the number of pool connections a call holds.
func inUse(c goredis.UniversalClient) uint32 {
	st := c.PoolStats()
	return st.TotalConns - st.IdleConns
}

// deadlineRecorder counts the non-zero deadlines go-redis sets on a dialed connection.
type deadlineRecorder struct {
	net.Conn
	set *atomic.Int64
}

func (r *deadlineRecorder) count(t time.Time) {
	if !t.IsZero() {
		r.set.Add(1)
	}
}

func (r *deadlineRecorder) SetDeadline(t time.Time) error {
	r.count(t)
	if err := r.Conn.SetReadDeadline(t); err != nil {
		return err
	}
	return r.Conn.SetWriteDeadline(t)
}

func (r *deadlineRecorder) SetReadDeadline(t time.Time) error {
	r.count(t)
	return r.Conn.SetReadDeadline(t)
}

func (r *deadlineRecorder) SetWriteDeadline(t time.Time) error {
	r.count(t)
	return r.Conn.SetWriteDeadline(t)
}

// SyscallConn keeps go-redis's health check of idle connections working through the recorder.
func (r *deadlineRecorder) SyscallConn() (syscall.RawConn, error) {
	return r.Conn.(syscall.Conn).SyscallConn()
}

func TestLimitsOf(t *testing.T) {
	cases := []struct {
		name string
		cfg  *config.RedisStore
		want connLimits
	}{
		{"unset", &config.RedisStore{}, connLimits{dial: time.Second, read: 500 * time.Millisecond, write: 500 * time.Millisecond}},
		{"explicit", &config.RedisStore{DialTimeout: durPtr(2 * time.Second), ReadTimeout: durPtr(3 * time.Second), WriteTimeout: durPtr(4 * time.Second)},
			connLimits{dial: 2 * time.Second, read: 3 * time.Second, write: 4 * time.Second}},
		{"zero", &config.RedisStore{DialTimeout: durPtr(0), ReadTimeout: durPtr(0), WriteTimeout: durPtr(0)},
			connLimits{dial: 5 * time.Second, read: 5 * time.Second, write: 5 * time.Second}},
		{"zero write follows read", &config.RedisStore{ReadTimeout: durPtr(3 * time.Second), WriteTimeout: durPtr(0)},
			connLimits{dial: time.Second, read: 3 * time.Second, write: 3 * time.Second}},
		{"negative read and write", &config.RedisStore{ReadTimeout: durPtr(-1), WriteTimeout: durPtr(-2)},
			connLimits{dial: time.Second, read: 0, write: 0}},
		{"negative read, zero write", &config.RedisStore{ReadTimeout: durPtr(-1), WriteTimeout: durPtr(0)},
			connLimits{dial: time.Second, read: 0, write: 0}},
		{"negative dial", &config.RedisStore{DialTimeout: durPtr(-1)},
			connLimits{dial: -1, read: 500 * time.Millisecond, write: 500 * time.Millisecond}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := limitsOf(tc.cfg); got != tc.want {
				t.Fatalf("limitsOf = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestConnLimitsOnStoreClock(t *testing.T) {
	const dialLimit = 200 * time.Millisecond
	dialAddr, accepted := blackhole(t)
	// The peer never answers the TLS ClientHello, so the dial stalls in the handshake.
	dialStore, dialFake := storeOnFake(t, dialAddr, dialLimit, time.Hour, time.Hour, &config.RedisTLS{Enabled: true, InsecureSkipVerify: true})
	dialErr := takeAsync(dialStore, context.Background(), "dial")
	testwait.Recv(t, "the dial to reach the peer", accepted, nil)
	awaitTimer(t, dialFake, 0, dialLimit, "the dial timer to be armed")

	const readLimit = 300 * time.Millisecond
	readMini := newMini(t)
	readPeer := newHoldPeer(t, readMini.Addr())
	readPeer.Hold()
	readStore, readFake := storeOnFake(t, readPeer.Addr(), time.Hour, readLimit, time.Hour, nil)
	readErr := takeAsync(readStore, context.Background(), "read")
	testwait.Until(t, "the reply to the call to be held", func() bool { return readPeer.Commands() >= 1 }, nil)
	awaitTimer(t, readFake, 0, readLimit, "the read timer to be armed")

	const writeLimit = 250 * time.Millisecond
	writeFake := clock.NewFake(miniStart)
	writeDial := newDialer(writeFake, connLimits{dial: time.Hour, read: time.Hour, write: writeLimit}, nil)
	writeConn, err := writeDial(context.Background(), "tcp", silentPeer(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writeConn.Close() })
	if tcp, ok := writeConn.(*timedConn).Conn.(*net.TCPConn); ok {
		_ = tcp.SetWriteBuffer(16 << 10)
	}
	writeErr := make(chan error, 1)
	go func() {
		_, err := writeConn.Write(make([]byte, 32<<20))
		writeErr <- err
	}()
	awaitTimer(t, writeFake, 0, writeLimit, "the write timer to be armed")

	healthyMini := newMini(t)
	healthyPeer := newHoldPeer(t, healthyMini.Addr())
	healthyPeer.Hold()
	// The default limits are 1 s for dial and 500 ms for read and write.
	healthyFake := clock.NewFake(miniStart)
	healthyStore, err := newFromConfig(&config.RedisStore{Mode: "standalone", Addresses: []string{healthyPeer.Addr()}}, nil, healthyFake)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = healthyStore.Close() })
	healthyErr := takeAsync(healthyStore, context.Background(), "healthy")
	testwait.Until(t, "the reply to the call to be held", func() bool { return healthyPeer.Commands() >= 1 }, nil)
	awaitTimer(t, healthyFake, 0, 500*time.Millisecond, "the read timer to be armed")

	realStall(t, 2*time.Second)

	t.Run("dial", func(t *testing.T) {
		stillPending(t, "stalled dial", dialErr)
		crossLimit(t, dialFake, dialLimit, "stalled dial", dialErr)
		err, _ := testwait.Recv(t, "the stalled dial to end at its limit", dialErr, nil)
		checkTimeout(t, err)
	})
	t.Run("read", func(t *testing.T) {
		stillPending(t, "stalled read", readErr)
		crossLimit(t, readFake, readLimit, "stalled read", readErr)
		err, _ := testwait.Recv(t, "the stalled read to end at its limit", readErr, nil)
		checkTimeout(t, err)
		readPeer.Release()
	})
	t.Run("write", func(t *testing.T) {
		stillPending(t, "stalled write", writeErr)
		crossLimit(t, writeFake, writeLimit, "stalled write", writeErr)
		err, _ := testwait.Recv(t, "the stalled write to end at its limit", writeErr, nil)
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("write error %v, want a deadline error", err)
		}
		// The store reports this error as it reports every connection timeout.
		checkTimeout(t, (&Store{}).fail(context.Background(), err))
	})
	t.Run("healthy", func(t *testing.T) {
		stillPending(t, "slow healthy call", healthyErr)
		healthyPeer.Release()
		err, _ := testwait.Recv(t, "the slow healthy call to succeed", healthyErr, nil)
		if err != nil {
			t.Fatalf("slow healthy call: %v", err)
		}
		if d := healthyFake.Since(miniStart); d != 0 {
			t.Fatalf("the fake clock moved by %v", d)
		}
	})
}

func TestGatewayStoreReadLimit(t *testing.T) {
	m := newMini(t)
	p := newHoldPeer(t, m.Addr())
	const limit = 300 * time.Millisecond
	stores := redisStoreYAML("shared", []string{p.Addr()}, "read_timeout: 300ms\n", "")
	route := "rate: 100\nper: 1s\nburst: 100\nstore: shared\non_store_error: deny\nstore_timeout: 2s\n"
	fake := clock.NewFake(miniStart)
	g := mustGateway(t, gwDoc(stores, routeYAML("rl", route), upstream(t)), fake, nil)
	base := fake.Due(limit)
	p.Hold()
	var rec *httptest.ResponseRecorder
	done := make(chan struct{})
	go func() {
		rec = doGet(g, "/rl", outageClientAddr)
		close(done)
	}()
	testwait.Until(t, "the reply to the request to be held", func() bool { return p.Commands() >= 1 }, nil)
	awaitTimer(t, fake, base, limit, "the read timer of the gateway's store to be armed")
	fake.Advance(limit - time.Nanosecond)
	if fake.Due(time.Nanosecond) < 1 {
		t.Fatal("no timer pending 1ns before the read limit")
	}
	select {
	case <-done:
		t.Fatal("the request ended before the read limit")
	default:
	}
	fake.Advance(time.Nanosecond)
	testwait.Done(t, "the request to end at the store's read limit", done, nil)
	checkOutageReply(t, "deny", "read-limited request", rec, http.StatusServiceUnavailable)
	if d := fake.Since(miniStart); d != limit {
		t.Errorf("fake time passed %v, want %v (the read limit, before the 2s store timeout)", d, limit)
	}
	p.Release()
}

func TestCanceledCallFreesConnAtReadLimit(t *testing.T) {
	const limit = 300 * time.Millisecond
	t.Run("store", func(t *testing.T) {
		m := newMini(t)
		p := newHoldPeer(t, m.Addr())
		s, fake := storeOnFake(t, p.Addr(), time.Hour, limit, time.Hour, nil)
		if _, err := s.Take(context.Background(), "warm", storeTB); err != nil {
			t.Fatal(err)
		}
		before := inUse(s.c)
		sent := p.Commands()
		p.Hold()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errc := takeAsync(s, ctx, "canceled")
		testwait.Until(t, "the reply to the canceled call to be held", func() bool { return p.Commands() > sent }, nil)
		awaitTimer(t, fake, 0, limit, "the read timer to be armed")
		cancel()
		err, _ := testwait.Recv(t, "the canceled Take to return", errc, nil)
		if !errors.Is(err, context.Canceled) || errors.Is(err, store.ErrUnavailable) {
			t.Fatalf("canceled Take error %v, want context.Canceled unwrapped", err)
		}
		if d := fake.Since(miniStart); d != 0 {
			t.Fatalf("the fake clock moved by %v before the cancellation was observed", d)
		}
		if n := inUse(s.c); n != before+1 {
			t.Fatalf("connections in use %d, want %d (the abandoned call holds its connection)", n, before+1)
		}
		fake.Advance(limit - time.Nanosecond)
		if n := inUse(s.c); n != before+1 {
			t.Fatalf("connections in use %d 1ns before the read limit, want %d", n, before+1)
		}
		fake.Advance(time.Nanosecond)
		testwait.Until(t, "the abandoned call to free its connection", func() bool { return inUse(s.c) == before }, func() string {
			return fmt.Sprintf("connections in use %d, want %d", inUse(s.c), before)
		})
		p.Release()
		if _, err := s.Take(context.Background(), "after", storeTB); err != nil {
			t.Fatalf("Take after release: %v", err)
		}
	})
	t.Run("gateway", func(t *testing.T) {
		m := newMini(t)
		p := newHoldPeer(t, m.Addr())
		var captured atomic.Pointer[Store]
		capture := func(sp microgateway.StoreParams) (store.Backend, error) {
			b, err := factory(sp)
			if s, ok := b.(*Store); ok {
				captured.Store(s)
			}
			return b, err
		}
		stores := redisStoreYAML("shared", []string{p.Addr()}, "dial_timeout: 1h\nread_timeout: 300ms\nwrite_timeout: 1h\n", "")
		route := "rate: 100\nper: 1s\nburst: 100\nstore: shared\non_store_error: deny\nstore_timeout: 2s\n"
		fake := clock.NewFake(miniStart)
		g, err := newGateway(t, gwDoc(stores, routeYAML("rl", route), upstream(t)), false, fake, nil, microgateway.WithStoreType("redis", capabilities, capture))
		if err != nil {
			t.Fatal(err)
		}
		if rec := doGet(g, "/rl", outageClientAddr); rec.Code != http.StatusOK {
			t.Fatalf("warm request status %d, want 200", rec.Code)
		}
		s := captured.Load()
		if s == nil {
			t.Fatal("the gateway did not build a redis store")
		}
		before := inUse(s.c)
		sent := p.Commands()
		base := fake.Due(limit)
		p.Hold()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		req := httptest.NewRequest(http.MethodGet, "/rl", nil).WithContext(ctx)
		req.RemoteAddr = outageClientAddr
		rec := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			g.ServeHTTP(rec, req)
			close(done)
		}()
		testwait.Until(t, "the reply to the canceled request to be held", func() bool { return p.Commands() > sent }, nil)
		awaitTimer(t, fake, base, limit, "the read timer of the gateway's store to be armed")
		cancel()
		testwait.Done(t, "the canceled request to return", done, nil)
		if d := fake.Since(miniStart); d != 0 {
			t.Fatalf("the fake clock moved by %v before the cancellation was observed", d)
		}
		if n := inUse(s.c); n != before+1 {
			t.Fatalf("connections in use %d, want %d (the abandoned call holds its connection)", n, before+1)
		}
		fake.Advance(limit - time.Nanosecond)
		if n := inUse(s.c); n != before+1 {
			t.Fatalf("connections in use %d 1ns before the read limit, want %d", n, before+1)
		}
		fake.Advance(time.Nanosecond)
		testwait.Until(t, "the abandoned call to free its connection", func() bool { return inUse(s.c) == before }, func() string {
			return fmt.Sprintf("connections in use %d, want %d", inUse(s.c), before)
		})
		p.Release()
		if rec := doGet(g, "/rl", outageClientAddr); rec.Code != http.StatusOK {
			t.Fatalf("request after release status %d, want 200", rec.Code)
		}
	})
}

func TestNoLibraryDeadline(t *testing.T) {
	m := newMini(t)
	p := newHoldPeer(t, m.Addr())
	fake := clock.NewFake(miniStart)
	opts, err := clientOptions(&config.RedisStore{Mode: "standalone", Addresses: []string{p.Addr()}, PoolSize: 1}, nil, fake)
	if err != nil {
		t.Fatal(err)
	}
	o := opts.(*goredis.Options)
	var set atomic.Int64
	inner := o.Dialer
	o.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := inner(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &deadlineRecorder{Conn: c, set: &set}, nil
	}
	c := goredis.NewClient(o)
	t.Cleanup(func() { _ = c.Close() })
	s := New(c, Options{KeyPrefix: "mgtest"})
	// A deadline in the time of the fake clock, long past on the wall clock: go-redis would fail every call if it made a socket deadline of it.
	ctx, cancel := clock.WithTimeoutCause(context.Background(), fake, time.Hour, nil)
	defer cancel()
	if _, err := s.Take(ctx, "warm", storeTB); err != nil {
		t.Fatal(err)
	}
	sent := p.Commands()
	p.Hold()
	errA := takeAsync(s, ctx, "a")
	testwait.Until(t, "the first call to reach the peer", func() bool { return p.Commands() > sent }, nil)
	errB := takeAsync(s, ctx, "b")
	testwait.Until(t, "the second call to wait for the only pool connection", func() bool { return c.PoolStats().PendingRequests >= 1 }, nil)
	realStall(t, 2*time.Second)
	stillPending(t, "first call", errA)
	stillPending(t, "second call", errB)
	p.Release()
	if err, _ := testwait.Recv(t, "the first call to succeed", errA, nil); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if err, _ := testwait.Recv(t, "the second call to succeed", errB, nil); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if _, err := s.Take(ctx, "after", storeTB); err != nil {
		t.Fatalf("Take after release: %v", err)
	}
	if n := set.Load(); n != 0 {
		t.Errorf("go-redis set %d non-zero deadlines, want none", n)
	}
	st := c.PoolStats()
	if st.Timeouts != 0 {
		t.Errorf("pool timeouts %d, want 0", st.Timeouts)
	}
	if st.WaitCount < 1 {
		t.Errorf("pool wait count %d, want at least 1", st.WaitCount)
	}
}

func TestTimedConnLateFireClearsDeadline(t *testing.T) {
	a, z := net.Pipe()
	t.Cleanup(func() {
		_ = a.Close()
		_ = z.Close()
	})
	fake := clock.NewFake(miniStart)
	c := newTimedConn(a, fake)
	c.setLimits(100*time.Millisecond, 100*time.Millisecond)

	c.rd.start()
	fake.Advance(100 * time.Millisecond)
	c.rd.stop()
	go func() { _, _ = z.Write([]byte("x")) }()
	readErr := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 1))
		readErr <- err
	}()
	if err, _ := testwait.Recv(t, "the read after a late fire to succeed", readErr, nil); err != nil {
		t.Fatalf("read after a late fire: %v", err)
	}

	c.wr.start()
	fake.Advance(100 * time.Millisecond)
	c.wr.stop()
	go func() { _, _ = io.ReadFull(z, make([]byte, 1)) }()
	writeErr := make(chan error, 1)
	go func() {
		_, err := c.Write([]byte("y"))
		writeErr <- err
	}()
	if err, _ := testwait.Recv(t, "the write after a late fire to succeed", writeErr, nil); err != nil {
		t.Fatalf("write after a late fire: %v", err)
	}
}

func BenchmarkTimedConn(b *testing.B) {
	for _, bc := range []struct {
		name string
		clk  clock.Clock
	}{
		{"raw", nil},
		{"fake", clock.NewFake(miniStart)},
		// Real clock: measures the cost of the production timer; a benchmark decides no test.
		{"real", clock.Real()},
	} {
		b.Run(bc.name, func(b *testing.B) {
			a, z := net.Pipe()
			defer a.Close()
			defer z.Close()
			go func() { _, _ = io.Copy(z, z) }()
			var conn net.Conn = a
			if bc.clk != nil {
				tc := newTimedConn(a, bc.clk)
				tc.setLimits(time.Hour, time.Hour)
				conn = tc
			}
			buf := make([]byte, 64)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := conn.Write(buf); err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(conn, buf); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
