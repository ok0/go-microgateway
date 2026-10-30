package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

var (
	storeTB = store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
	storeSW = store.Limit{Algorithm: store.SlidingWindow, Count: 4, Period: 8 * time.Second}
)

func mustTake(t *testing.T, s store.RateLimiter, key string, l store.Limit) store.Decision {
	t.Helper()
	d, err := s.Take(context.Background(), key, l)
	if err != nil {
		t.Fatalf("Take(%q): %v", key, err)
	}
	return d
}

func mustPanic(t *testing.T, want string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != want {
			t.Errorf("panic = %v, want %q", r, want)
		}
	}()
	f()
}

func newStoreWithReadTimeout(t *testing.T, addr string, readTimeout time.Duration) (*Store, *clock.Fake) {
	t.Helper()
	d := config.Duration(readTimeout)
	fake := clock.NewFake(miniStart)
	s, err := newFromConfig(&config.RedisStore{
		Mode:        "standalone",
		Addresses:   []string{addr},
		ReadTimeout: &d,
	}, nil, fake)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, fake
}

func TestTakeOrder(t *testing.T) {
	ctx := context.Background()

	t.Run("CanceledContext", func(t *testing.T) {
		m := newMini(t)
		s := newTestStore(t, m.Addr(), 0, "mgtest")
		before := m.CommandCount()
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		_, err := s.Take(cctx, "order", storeTB)
		if !errors.Is(err, context.Canceled) || errors.Is(err, store.ErrUnavailable) {
			t.Fatalf("canceled Take: error %v, want context.Canceled and not ErrUnavailable", err)
		}
		if n := m.CommandCount(); n != before {
			t.Errorf("canceled Take sent %d commands, want 0", n-before)
		}
		if d := mustTake(t, s, "order", storeTB); !d.Allowed || d.Remaining != 2 {
			t.Errorf("take after canceled: %+v, want allowed with Remaining 2", d)
		}
	})

	t.Run("InvalidLimits", func(t *testing.T) {
		m := newMini(t)
		s := newTestStore(t, m.Addr(), 0, "mgtest")
		invalid := []store.Limit{
			{Algorithm: 0, Count: 1, Period: time.Second, Burst: 1},
			{Algorithm: 9, Count: 1, Period: time.Second, Burst: 1},
			{Algorithm: store.TokenBucket, Count: 0, Period: time.Second, Burst: 1},
			{Algorithm: store.SlidingWindow, Count: 0, Period: time.Second},
			{Algorithm: store.TokenBucket, Count: 1, Period: 0, Burst: 1},
			{Algorithm: store.SlidingWindow, Count: 1, Period: -1},
			{Algorithm: store.TokenBucket, Count: 1, Period: time.Second, Burst: 0},
			{Algorithm: store.TokenBucket, Count: 2, Period: time.Microsecond, Burst: 1},
			{Algorithm: store.TokenBucket, Count: 1, Period: time.Hour, Burst: 25},
			{Algorithm: store.SlidingWindow, Count: 1, Period: 25 * time.Hour},
		}
		before := m.CommandCount()
		for _, l := range invalid {
			_, err := s.Take(ctx, "order", l)
			if !errors.Is(err, store.ErrInvalidLimit) || errors.Is(err, store.ErrUnavailable) {
				t.Errorf("Take with limit %+v: error %v, want ErrInvalidLimit and not ErrUnavailable", l, err)
			}
		}
		if n := m.CommandCount(); n != before {
			t.Errorf("invalid limits sent %d commands, want 0", n-before)
		}
	})

	t.Run("Close", func(t *testing.T) {
		m := newMini(t)
		a := newTestStore(t, m.Addr(), 0, "mgtest")
		b := newTestStore(t, m.Addr(), 0, "mgtest")
		if err := a.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		before := m.CommandCount()
		if _, err := a.Take(ctx, "order", storeTB); !errors.Is(err, store.ErrUnavailable) {
			t.Errorf("Take on closed store: error %v, want ErrUnavailable", err)
		}
		if n := m.CommandCount(); n != before {
			t.Errorf("Take on closed store sent %d commands, want 0", n-before)
		}
		if err := a.Close(); err != nil {
			t.Errorf("second Close: %v", err)
		}
		if d := mustTake(t, b, "order", storeTB); !d.Allowed {
			t.Errorf("take on open store: %+v, want allowed", d)
		}
	})

	t.Run("CallerClientStaysOpen", func(t *testing.T) {
		m := newMini(t)
		c := newTestClient(t, m.Addr(), 0)
		s := New(c, Options{})
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := c.Ping(ctx).Err(); err != nil {
			t.Errorf("client after store Close: %v", err)
		}
	})

	t.Run("NewPanics", func(t *testing.T) {
		m := newMini(t)
		c := newTestClient(t, m.Addr(), 0)
		mustPanic(t, "redis: New requires a client", func() { New(nil, Options{}) })
		mustPanic(t, "redis: key prefix must not contain { or }", func() { New(c, Options{KeyPrefix: "x}"}) })
	})
}

func TestFailuresAreUnavailable(t *testing.T) {
	t.Run("LoadingReply", func(t *testing.T) {
		m := newMini(t)
		s := newTestStore(t, m.Addr(), 0, "mgtest")
		m.SetError("LOADING server is loading")
		_, err := s.Take(context.Background(), "fail", storeTB)
		if !errors.Is(err, store.ErrUnavailable) || !strings.Contains(err.Error(), "server error LOADING") {
			t.Errorf("error %v, want ErrUnavailable containing server error LOADING", err)
		}
	})

	t.Run("WrongType", func(t *testing.T) {
		for _, l := range []store.Limit{storeTB, storeSW} {
			m := newMini(t)
			s := newTestStore(t, m.Addr(), 0, "mgtest")
			m.HSet(s.keyFor("fail", l.Algorithm), "f", "v")
			_, err := s.Take(context.Background(), "fail", l)
			if !errors.Is(err, store.ErrUnavailable) || !strings.Contains(err.Error(), "server error WRONGTYPE") {
				t.Errorf("%v: error %v, want ErrUnavailable containing server error WRONGTYPE", l.Algorithm, err)
			}
		}
	})

	t.Run("GarbageState", func(t *testing.T) {
		for _, l := range []store.Limit{storeTB, storeSW} {
			m := newMini(t)
			s := newTestStore(t, m.Addr(), 0, "mgtest")
			m.Set(s.keyFor("fail", l.Algorithm), "garbage")
			_, err := s.Take(context.Background(), "fail", l)
			if !errors.Is(err, store.ErrUnavailable) {
				t.Errorf("%v: error %v, want ErrUnavailable", l.Algorithm, err)
			}
		}
	})

	t.Run("UnexpectedReply", func(t *testing.T) {
		for _, src := range []string{"return {1, 2}", "return 'x'"} {
			m := newMini(t)
			s := newTestStore(t, m.Addr(), 0, "mgtest")
			s.sc.tb = goredis.NewScript(src)
			_, err := s.Take(context.Background(), "fail", storeTB)
			if !errors.Is(err, store.ErrUnavailable) || !strings.Contains(err.Error(), "unexpected script reply") {
				t.Errorf("script %q: error %v, want ErrUnavailable containing unexpected script reply", src, err)
			}
		}
	})

	t.Run("ClosedServer", func(t *testing.T) {
		m := newMini(t)
		s := newTestStore(t, m.Addr(), 0, "mgtest")
		m.Close()
		_, err := s.Take(context.Background(), "fail", storeTB)
		if !errors.Is(err, store.ErrUnavailable) {
			t.Errorf("error %v, want ErrUnavailable", err)
		}
	})
}

type testRedisErr string

func (e testRedisErr) Error() string { return string(e) }

func (e testRedisErr) RedisError() {}

type testTimeoutErr struct{}

func (testTimeoutErr) Error() string   { return "secret-xyz" }
func (testTimeoutErr) Timeout() bool   { return true }
func (testTimeoutErr) Temporary() bool { return false }

func TestDescribe(t *testing.T) {
	const secret = "secret-xyz"
	const sha = "0123abcd"
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"deadline", context.DeadlineExceeded, "timeout"},
		{"canceled", context.Canceled, "canceled"},
		{"client closed", goredis.ErrClosed, "client closed"},
		{"server code", testRedisErr("WRONGPASS " + secret), "server error WRONGPASS"},
		{"server lower case", testRedisErr("lower case"), "server error ERR"},
		{"redis 7 script error", testRedisErr("WRONGTYPE Operation against a key holding the wrong kind of value script: " + sha + ", on @user_script:1."), "server error WRONGTYPE"},
		{"redis 6.2 script error", testRedisErr("ERR Error running script (call to f_" + sha + "): @user_script:1: WRONGTYPE Operation against a key holding the wrong kind of value"), "server error WRONGTYPE"},
		{"miniredis script error", testRedisErr("ERR Error compiling script (new function): <string>:1: WRONGTYPE Operation against a key holding the wrong kind of value"), "server error WRONGTYPE"},
		{"lua runtime error", testRedisErr("ERR Error running script (call to f_" + sha + "): @user_script:1: user_script:1: attempt to perform arithmetic on a nil value"), "server error ERR"},
		{"script wrapper with code", testRedisErr("ERR Error running script (call to f_x): @user_script:1: WRONGPASS " + secret), "server error WRONGPASS"},
		{"script wrapper with text", testRedisErr("ERR Error running script (call to f_x): @user_script:1: " + secret), "server error ERR"},
		{"unwrapped lower case", testRedisErr("ERR microgateway: LOADING x"), "server error ERR"},
		{"certificate", &tls.CertificateVerificationError{}, "tls handshake failed"},
		{"record header", tls.RecordHeaderError{}, "tls handshake failed"},
		{"net timeout", testTimeoutErr{}, "timeout"},
		{"socket deadline", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}, "timeout"},
		{"dial timeout", &net.OpError{Op: "dial", Err: testTimeoutErr{}}, "timeout"},
		{"dial", &net.OpError{Op: "dial", Err: errors.New(secret)}, "connection failed during dial"},
		{"close", &net.OpError{Op: "close"}, "connection failed during network operation"},
		{"eof", io.EOF, "connection closed"},
		{"other", errors.New(secret), "request failed"},
	}
	for _, c := range cases {
		got := describe(c.err)
		if got != c.want {
			t.Errorf("%s: describe = %q, want %q", c.name, got, c.want)
		}
		if strings.Contains(got, secret) {
			t.Errorf("%s: describe leaks the error text: %q", c.name, got)
		}
	}
}

func TestHangBoundedByDeadline(t *testing.T) {
	want := "redis: take: timeout: " + store.ErrUnavailable.Error()
	t.Run("ContextDeadline", func(t *testing.T) {
		addr, _ := blackhole(t)
		// The read timeout cannot fire during the test, so only the context deadline can end the call.
		s, _ := newStoreWithReadTimeout(t, addr, time.Hour)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		errc := make(chan error, 1)
		go func() {
			_, err := s.Take(ctx, "hang", storeTB)
			errc <- err
		}()
		err, _ := testwait.Recv(t, "Take to end at the context deadline", errc, nil)
		if err == nil || err.Error() != want {
			t.Errorf("error %v, want %q", err, want)
		}
		if !errors.Is(err, store.ErrUnavailable) || errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error %v, want ErrUnavailable and not context.DeadlineExceeded", err)
		}
	})

	t.Run("ReadTimeout", func(t *testing.T) {
		addr, _ := blackhole(t)
		s, fake := newStoreWithReadTimeout(t, addr, 100*time.Millisecond)
		errc := make(chan error, 1)
		go func() {
			_, err := s.Take(context.Background(), "hang", storeTB)
			errc <- err
		}()
		testwait.Until(t, "the read timer to be armed", func() bool { return fake.Due(100*time.Millisecond) >= 1 }, nil)
		fake.Advance(100 * time.Millisecond)
		err, _ := testwait.Recv(t, "Take to end at the read timeout", errc, nil)
		if err == nil || err.Error() != want {
			t.Errorf("error %v, want %q", err, want)
		}
		if !errors.Is(err, store.ErrUnavailable) {
			t.Errorf("error %v, want ErrUnavailable", err)
		}
	})

	t.Run("CallerCancel", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		conns := make(chan net.Conn, 1)
		go func() {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			// Hand the connection over once the client has sent its first bytes, so the call is waiting for a reply.
			if _, err := c.Read(make([]byte, 1)); err != nil {
				_ = c.Close()
				return
			}
			conns <- c
		}()
		// The read timeout cannot fire during the test, so the canceled context alone has to end the call.
		s, _ := newStoreWithReadTimeout(t, ln.Addr().String(), time.Hour)
		cctx, ccancel := context.WithCancel(context.Background())
		errc := make(chan error, 1)
		go func() {
			_, err := s.Take(cctx, "hang", storeTB)
			errc <- err
		}()
		c, _ := testwait.Recv(t, "the store's call to reach the black hole", conns, nil)
		t.Cleanup(func() { _ = c.Close() })
		ccancel()
		err, _ = testwait.Recv(t, "the canceled Take to return", errc, nil)
		if !errors.Is(err, context.Canceled) || errors.Is(err, store.ErrUnavailable) {
			t.Errorf("canceled hung Take: error %v, want context.Canceled and not ErrUnavailable", err)
		}
	})
}

func TestNoRetryOnAmbiguousFailure(t *testing.T) {
	ctx := context.Background()
	tb := store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: time.Hour, Burst: 3}
	checkSetup := func(t *testing.T, p *respProxy) {
		t.Helper()
		for _, c := range p.Setup() {
			if c != "HELLO" {
				t.Errorf("setup command %q, want only HELLO", c)
			}
		}
	}

	t.Run("ExecutedReplyLost", func(t *testing.T) {
		m := newMini(t)
		p := newRespProxy(t, m.Addr())
		s := newTestStore(t, p.Addr(), 0, "mgtest")

		if d := mustTake(t, s, "retry", tb); !d.Allowed || d.Remaining != 2 {
			t.Fatalf("warm-up take: %+v, want allowed with Remaining 2", d)
		}
		p.Reset()
		p.SetMode(proxyDropReply)
		_, err := s.Take(ctx, "retry", tb)
		if !errors.Is(err, store.ErrUnavailable) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("dropped reply: error %v, want ErrUnavailable and not a context error", err)
		}
		if cmds := p.Commands(); fmt.Sprint(cmds) != "[EVALSHA]" {
			t.Errorf("dropped reply: commands %v, want [EVALSHA]", cmds)
		}
		raw := newTestClient(t, m.Addr(), 0)
		if err := raw.Get(ctx, s.keyFor("retry", store.TokenBucket)).Err(); err != nil {
			t.Errorf("state after dropped reply: %v, want the key to exist", err)
		}

		p.Reset()
		p.SetMode(proxyForward)
		if d := mustTake(t, s, "retry", tb); !d.Allowed || d.Remaining != 0 {
			t.Errorf("take after dropped reply: %+v, want allowed with Remaining 0", d)
		}
		if cmds := p.Commands(); fmt.Sprint(cmds) != "[EVALSHA]" {
			t.Errorf("take after dropped reply: commands %v, want [EVALSHA]", cmds)
		}
		if d := mustTake(t, s, "retry", tb); d.Allowed {
			t.Errorf("take after that: %+v, want denied", d)
		}
		checkSetup(t, p)
	})

	t.Run("NoScriptReplyLost", func(t *testing.T) {
		m := newMini(t)
		p := newRespProxy(t, m.Addr())
		s := newTestStore(t, p.Addr(), 0, "mgtest")

		p.SetMode(proxyDropReply)
		_, err := s.Take(ctx, "retry", tb)
		if !errors.Is(err, store.ErrUnavailable) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("dropped NOSCRIPT reply: error %v, want ErrUnavailable and not a context error", err)
		}
		if cmds := p.Commands(); fmt.Sprint(cmds) != "[EVALSHA]" {
			t.Errorf("dropped NOSCRIPT reply: commands %v, want [EVALSHA]", cmds)
		}

		p.Reset()
		p.SetMode(proxyForward)
		if d := mustTake(t, s, "retry", tb); !d.Allowed || d.Remaining != 2 {
			t.Errorf("take after dropped NOSCRIPT reply: %+v, want allowed with Remaining 2", d)
		}
		if cmds := p.Commands(); fmt.Sprint(cmds) != "[EVALSHA EVAL]" {
			t.Errorf("take after dropped NOSCRIPT reply: commands %v, want [EVALSHA EVAL]", cmds)
		}
		checkSetup(t, p)
	})
}

func TestScriptFlush(t *testing.T) {
	ctx := context.Background()
	m := newMini(t)
	p := newRespProxy(t, m.Addr())
	s := newTestStore(t, p.Addr(), 0, "mgtest")
	tb := store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: time.Hour, Burst: 5}

	if d := mustTake(t, s, "flush", tb); d.Remaining != 4 {
		t.Fatalf("first take: Remaining %d, want 4", d.Remaining)
	}
	raw := newTestClient(t, m.Addr(), 0)
	if err := raw.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	p.Reset()
	if d := mustTake(t, s, "flush", tb); d.Remaining != 3 {
		t.Errorf("take after flush: Remaining %d, want 3", d.Remaining)
	}
	if cmds := p.Commands(); fmt.Sprint(cmds) != "[EVALSHA EVAL]" {
		t.Errorf("commands after flush: %v, want [EVALSHA EVAL]", cmds)
	}
	p.Reset()
	if d := mustTake(t, s, "flush", tb); d.Remaining != 2 {
		t.Errorf("take after reload: Remaining %d, want 2", d.Remaining)
	}
	if cmds := p.Commands(); fmt.Sprint(cmds) != "[EVALSHA]" {
		t.Errorf("commands after reload: %v, want [EVALSHA]", cmds)
	}
}

func TestPrefixIsolation(t *testing.T) {
	m := newMini(t)
	p1a := newTestStore(t, m.Addr(), 0, "p1")
	p2 := newTestStore(t, m.Addr(), 0, "p2")
	p1b := newTestStore(t, m.Addr(), 0, "p1")
	tb := store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: time.Hour, Burst: 1}

	if d := mustTake(t, p1a, "iso", tb); !d.Allowed {
		t.Fatalf("p1 first take: %+v, want allowed", d)
	}
	if d := mustTake(t, p1b, "iso", tb); d.Allowed {
		t.Errorf("p1 second store take: %+v, want denied", d)
	}
	if d := mustTake(t, p2, "iso", tb); !d.Allowed {
		t.Errorf("p2 take: %+v, want allowed", d)
	}
}

func TestKeyLayoutAndSlots(t *testing.T) {
	m := newMini(t)
	s := newTestStore(t, m.Addr(), 0, DefaultKeyPrefix)
	const key = "r:abcd1234:ip:1.2.3.4"
	if got, want := s.keyFor(key, store.TokenBucket), "microgateway:rl:{r:abcd1234:ip:1.2.3.4}"; got != want {
		t.Errorf("token bucket key = %q, want %q", got, want)
	}
	if got, want := s.keyFor(key, store.SlidingWindow), "microgateway:rl:{r:abcd1234:ip:1.2.3.4}:sw"; got != want {
		t.Errorf("sliding window key = %q, want %q", got, want)
	}

	if got := crc16([]byte("123456789")); got != 0x31C3 {
		t.Errorf("crc16(\"123456789\") = %#x, want 0x31C3", got)
	}
	if keySlot("{user1000}.following") != keySlot("{user1000}.followers") {
		t.Errorf("hash tag does not pin both keys to one slot")
	}

	for _, prefix := range []string{"a", "x:y"} {
		for _, k := range []string{"k", "r:1:ip:10.0.0.1", "odd}key", "{x}"} {
			ps := newTestStore(t, m.Addr(), 0, prefix)
			tbKey := ps.keyFor(k, store.TokenBucket)
			swKey := ps.keyFor(k, store.SlidingWindow)
			if keySlot(tbKey) != keySlot(swKey) {
				t.Errorf("prefix %q key %q: token bucket slot %d, sliding window slot %d", prefix, k, keySlot(tbKey), keySlot(swKey))
			}
		}
	}

	p := newRespProxy(t, m.Addr())
	for _, prefix := range []string{"a", "x:y"} {
		ps := newTestStore(t, p.Addr(), 0, prefix)
		for _, k := range []string{"k", "r:1:ip:10.0.0.1", "odd}key", "{x}"} {
			mustTake(t, ps, k, storeTB)
			mustTake(t, ps, k, storeSW)
		}
	}
	scripts := 0
	for _, args := range p.Requests() {
		if len(args) == 0 {
			continue
		}
		switch strings.ToUpper(args[0]) {
		case "EVALSHA", "EVAL":
			scripts++
			if len(args) < 3 || args[2] != "1" {
				t.Errorf("script request %v: numkeys is not 1", args)
			}
		}
	}
	if scripts == 0 {
		t.Errorf("no script requests recorded through the proxy")
	}
}

func TestClockSkewIrrelevant(t *testing.T) {
	m := newMini(t)
	a := newTestStore(t, m.Addr(), 0, "mgtest")
	b := newTestStore(t, m.Addr(), 0, "mgtest")
	tb := store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: time.Hour, Burst: 3}
	admitted := 0
	for i := 0; i < 10; i++ {
		s := a
		if i%2 == 1 {
			s = b
		}
		d := mustTake(t, s, "skew", tb)
		if d.Allowed {
			admitted++
		}
		if d.ResetAfter%time.Hour != 0 {
			t.Errorf("take %d: ResetAfter %s is not a whole multiple of 1h", i, d.ResetAfter)
		}
	}
	if admitted != 3 {
		t.Errorf("admitted %d, want 3", admitted)
	}
}
