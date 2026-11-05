package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testcert"
	"github.com/ok0/go-microgateway/internal/testwait"
)

// limitCfg returns the test configuration with muts applied after validation, so that zero limits stay zero.
func limitCfg(t *testing.T, muts ...func(*config.Listener)) *config.Config {
	t.Helper()
	cfg := testConfig(t, nil)
	for _, m := range muts {
		m(&cfg.Listeners[0])
	}
	return cfg
}

func limits(h, r, w, i time.Duration) func(*config.Listener) {
	return func(l *config.Listener) {
		l.ReadHeaderTimeout = config.Duration(h)
		l.ReadTimeout = config.Duration(r)
		l.WriteTimeout = config.Duration(w)
		l.IdleTimeout = config.Duration(i)
	}
}

func tlsOn(certFile, keyFile string, http2 bool) func(*config.Listener) {
	return func(l *config.Listener) {
		l.TLS = &config.TLS{CertFile: certFile, KeyFile: keyFile}
		l.HTTP2 = &http2
	}
}

func h2cOn(l *config.Listener) {
	l.H2C = true
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	})
}

// dialRaw opens a plain TCP connection to addr.
func dialRaw(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// dialTLS opens a TCP connection to addr and completes a TLS handshake that trusts pool.
func dialTLS(t *testing.T, addr string, pool *x509.CertPool, protos ...string) *tls.Conn {
	t.Helper()
	raw := dialRaw(t, addr)
	tc := tls.Client(raw, &tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: protos})
	if err := tc.Handshake(); err != nil {
		t.Fatalf("TLS handshake: %v", err)
	}
	return tc
}

// dialConn returns a plain connection when pool is nil and a connection with a completed TLS handshake otherwise.
func dialConn(t *testing.T, addr string, pool *x509.CertPool) net.Conn {
	t.Helper()
	if pool == nil {
		return dialRaw(t, addr)
	}
	return dialTLS(t, addr, pool)
}

// waitHandshakeReleased waits until the handshake limit timer of a TLS connection has been stopped. It relies on
// the header limit being longer than limit, so that no timer of the header phase is due within limit.
func waitHandshakeReleased(t *testing.T, fake *clock.Fake, limit time.Duration) {
	t.Helper()
	testwait.Until(t, "handshake limit timer released", func() bool { return fake.Due(limit) == 0 }, nil)
}

// capture reads from a connection in the background, keeping every byte and the error that ended the reading.
type capture struct {
	mu   sync.Mutex
	data []byte
	err  error
	done chan struct{}
}

func capturing(r io.Reader) *capture {
	c := &capture{done: make(chan struct{})}
	go func() {
		defer close(c.done)
		buf := make([]byte, 32<<10)
		for {
			n, err := r.Read(buf)
			c.mu.Lock()
			c.data = append(c.data, buf[:n]...)
			if err != nil {
				c.err = err
			}
			c.mu.Unlock()
			if err != nil {
				return
			}
		}
	}()
	return c
}

func (c *capture) bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.data...)
}

func (c *capture) readErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// stillOpen fails the test if the reading of c has already ended.
func stillOpen(t *testing.T, c *capture) {
	t.Helper()
	select {
	case <-c.done:
		t.Fatalf("connection ended before its limit: %v", c.readErr())
	default:
	}
}

// awaitCut waits until the reading of c ends, meaning the server closed the connection.
func awaitCut(t *testing.T, c *capture, what string) {
	t.Helper()
	testwait.Done(t, what, c.done, nil)
}

// requireBytes fails the test unless exactly n bytes were received before the end of c.
func requireBytes(t *testing.T, c *capture, n int) {
	t.Helper()
	if got := len(c.bytes()); got != n {
		t.Fatalf("client received %d bytes, want %d", got, n)
	}
}

// requireNoResponse fails the test if c received an HTTP response.
func requireNoResponse(t *testing.T, c *capture) {
	t.Helper()
	if strings.Contains(string(c.bytes()), "HTTP/") {
		t.Fatalf("client received an HTTP response before the cut: %q", c.bytes())
	}
}

// advanceBelow moves fake to just short of limit and checks that exactly one limit timer is pending and c is open.
func advanceBelow(t *testing.T, fake *clock.Fake, limit time.Duration, c *capture) {
	t.Helper()
	fake.Advance(limit - time.Nanosecond)
	if n := fake.Due(time.Nanosecond); n != 1 {
		t.Fatalf("limit timers due within 1ns = %d, want 1", n)
	}
	stillOpen(t, c)
}

// advanceBelowIdle moves fake to just short of limit and checks that exactly one idle timer is pending and c is open.
func advanceBelowIdle(t *testing.T, fake *clock.Fake, limit time.Duration, c *capture) {
	t.Helper()
	fake.Advance(limit - time.Nanosecond)
	if n := fake.HousekeepingWaiters(); n != 1 {
		t.Fatalf("housekeeping timers pending = %d, want 1", n)
	}
	stillOpen(t, c)
}

// awaitIdle waits until n housekeeping timers are pending on fake.
func awaitIdle(t *testing.T, fake *clock.Fake, n int) {
	t.Helper()
	testwait.Until(t, "housekeeping timer armed", func() bool { return fake.HousekeepingWaiters() == n }, func() string {
		return fmt.Sprintf("housekeeping waiters=%d", fake.HousekeepingWaiters())
	})
}

func writeString(t *testing.T, w io.Writer, s string) {
	t.Helper()
	if _, err := io.WriteString(w, s); err != nil {
		t.Fatal(err)
	}
}

// readResponse reads one response from br and returns its status code and body.
func readResponse(t *testing.T, br *bufio.Reader) (int, string) {
	t.Helper()
	type result struct {
		status int
		body   string
		err    error
	}
	ch := make(chan result, 1)
	go func() {
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			ch <- result{err: err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		ch <- result{status: resp.StatusCode, body: string(b), err: err}
	}()
	r, _ := testwait.Recv(t, "HTTP response", ch, nil)
	if r.err != nil {
		t.Fatalf("read response: %v", r.err)
	}
	return r.status, r.body
}

// readLine reads one line from br, failing the test if the hang guard expires first.
func readLine(t *testing.T, br *bufio.Reader) (string, error) {
	t.Helper()
	type result struct {
		s   string
		err error
	}
	ch := make(chan result, 1)
	go func() {
		s, err := br.ReadString('\n')
		ch <- result{s, err}
	}()
	r, _ := testwait.Recv(t, "stream line", ch, nil)
	return strings.TrimRight(r.s, "\r\n"), r.err
}

// exchange sends a GET on conn and requires a 200 response with body "ok".
func exchange(t *testing.T, conn net.Conn, br *bufio.Reader) {
	t.Helper()
	writeString(t, conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	if status, body := readResponse(t, br); status != http.StatusOK || body != "ok" {
		t.Fatalf("status = %d, body = %q; want 200 \"ok\"", status, body)
	}
}

// clientHello returns the first TLS record that a real client sends when it starts a handshake.
func clientHello(t *testing.T, protos ...string) []byte {
	t.Helper()
	cli, srv := net.Pipe()
	defer srv.Close()
	go func() {
		tls.Client(cli, &tls.Config{InsecureSkipVerify: true, ServerName: "x", NextProtos: protos}).Handshake()
		cli.Close()
	}()
	hdr := make([]byte, 5)
	if _, err := io.ReadFull(srv, hdr); err != nil {
		t.Fatal(err)
	}
	body := make([]byte, int(binary.BigEndian.Uint16(hdr[3:5])))
	if _, err := io.ReadFull(srv, body); err != nil {
		t.Fatal(err)
	}
	return append(hdr, body...)
}

// h2Client returns a client that speaks HTTP/2 over TLS when h2c is false and prior-knowledge HTTP/2 when h2c is true.
func h2Client(t *testing.T, pool *x509.CertPool, h2c bool) *http.Client {
	t.Helper()
	tr := &http.Transport{}
	if h2c {
		var p http.Protocols
		p.SetUnencryptedHTTP2(true)
		tr.Protocols = &p
	} else {
		tr.TLSClientConfig = &tls.Config{RootCAs: pool}
		tr.ForceAttemptHTTP2 = true
	}
	return newClient(t, tr)
}

type fetched struct {
	status int
	major  int
	body   string
	reused bool
	alpn   string
}

// getVia performs a GET through client and reports the response and whether the connection was reused.
func getVia(t *testing.T, client *http.Client, url string) fetched {
	t.Helper()
	var reused atomic.Bool
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused.Store(info.Reused) }}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(context.Background(), trace), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	f := fetched{status: resp.StatusCode, major: resp.ProtoMajor, body: string(b), reused: reused.Load()}
	if resp.TLS != nil {
		f.alpn = resp.TLS.NegotiatedProtocol
	}
	return f
}

// checkLimitFields fails the test unless hs carries the four net/http limits in want, maxHeader as its header limit,
// and the connection tracker.
func checkLimitFields(t *testing.T, hs *http.Server, want [4]time.Duration, maxHeader int) {
	t.Helper()
	got := [4]time.Duration{hs.ReadHeaderTimeout, hs.ReadTimeout, hs.WriteTimeout, hs.IdleTimeout}
	if got != want {
		t.Errorf("net/http limits = %v, want %v", got, want)
	}
	if hs.MaxHeaderBytes != maxHeader {
		t.Errorf("MaxHeaderBytes = %d, want %d", hs.MaxHeaderBytes, maxHeader)
	}
	if hs.ConnState == nil {
		t.Error("ConnState = nil, want the limit tracker")
	}
}

func TestServersCarryLimitFields(t *testing.T) {
	certFile, keyFile, _ := testcert.Write(t, t.TempDir())
	all := limits(3*time.Second, 4*time.Second, 5*time.Second, 6*time.Second)
	cases := []struct {
		name string
		muts []func(*config.Listener)
		want connLimits
	}{
		{"plain", []func(*config.Listener){all}, connLimits{header: 3 * time.Second, read: 4 * time.Second, write: 5 * time.Second, idle: 6 * time.Second}},
		{"tls-h2", []func(*config.Listener){all, tlsOn(certFile, keyFile, true)}, connLimits{header: 3 * time.Second, read: 4 * time.Second, write: 5 * time.Second, idle: 6 * time.Second, tls: true, h2: true}},
		{"tls-http1", []func(*config.Listener){all, tlsOn(certFile, keyFile, false)}, connLimits{header: 3 * time.Second, read: 4 * time.Second, write: 5 * time.Second, idle: 6 * time.Second, tls: true}},
		{"h2c", []func(*config.Listener){all, h2cOn}, connLimits{header: 3 * time.Second, read: 4 * time.Second, write: 5 * time.Second, idle: 6 * time.Second, h2c: true}},
	}
	clocks := []struct {
		name string
		clk  clock.Clock
	}{
		{"fake", clock.NewFake(serverStart)},
		// Real clock: checks the built servers only; no connection is made
		{"real", clock.Real()},
	}
	for _, c := range clocks {
		for _, tc := range cases {
			t.Run(c.name+"/"+tc.name, func(t *testing.T) {
				srv, err := New(limitCfg(t, tc.muts...), okHandler(), okHandler(), nil, nil, WithClock(c.clk))
				if err != nil {
					t.Fatal(err)
				}
				hs := srv.data[0].srv
				checkLimitFields(t, hs, [4]time.Duration{3 * time.Second, 4 * time.Second, 5 * time.Second, 6 * time.Second}, 1048576)
				if hs.HTTP2 == nil {
					t.Error("HTTP2 = nil, want the HTTP/2 configuration")
				}
				if hs.TLSNextProto != nil {
					t.Errorf("TLSNextProto = %v before serving, want nil", hs.TLSNextProto)
				}
				if srv.data[0].lim != tc.want {
					t.Errorf("limits = %+v, want %+v", srv.data[0].lim, tc.want)
				}
				checkLimitFields(t, srv.admin.srv, [4]time.Duration{10 * time.Second, 0, 0, 120 * time.Second}, 0)
				if want := (connLimits{header: 10 * time.Second, idle: 120 * time.Second}); srv.admin.lim != want {
					t.Errorf("admin limits = %+v, want %+v", srv.admin.lim, want)
				}
			})
		}
	}
}

func TestHTTP1HeaderLimit(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	for _, kind := range []struct {
		name string
		tls  bool
	}{{"plain", false}, {"tls", true}} {
		t.Run(kind.name, func(t *testing.T) {
			fake := clock.NewFake(serverStart)
			muts := []func(*config.Listener){limits(3*time.Second, 0, 0, 0)}
			var p *x509.CertPool
			if kind.tls {
				muts = append(muts, tlsOn(certFile, keyFile, true))
				p = pool
			}
			srv := startServer(t, limitCfg(t, muts...), okHandler(), nil, nil, WithClock(fake))
			conn := dialConn(t, srv.Addrs()["public"].String(), p)
			c := capturing(conn)
			if kind.tls {
				// The handshake limit equals the header limit, so the pending handshake timer cannot be told
				// from the header timer by its due time. Step the clock once so that it is due sooner than
				// the header timer armed after it, then wait until it has been released.
				fake.Advance(time.Nanosecond)
				waitHandshakeReleased(t, fake, 3*time.Second-time.Nanosecond)
			}
			writeString(t, conn, "GET / HTTP/1.1\r\nHost: x\r\n")
			awaitDue(t, fake, 1, 3*time.Second, "header limit timer")
			advanceBelow(t, fake, 3*time.Second, c)
			fake.Advance(time.Nanosecond)
			awaitCut(t, c, "header limit cut")
			requireNoResponse(t, c)
		})
	}
}

func TestTLSHandshakeLimit(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	t.Run("silent", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		srv := startServer(t, limitCfg(t, tlsOn(certFile, keyFile, true), limits(10*time.Second, 4*time.Second, 6*time.Second, time.Hour)), okHandler(), nil, nil, WithClock(fake))
		c := capturing(dialRaw(t, srv.Addrs()["public"].String()))
		awaitDue(t, fake, 1, 4*time.Second, "handshake limit timer")
		advanceBelow(t, fake, 4*time.Second, c)
		fake.Advance(time.Nanosecond)
		awaitCut(t, c, "handshake limit cut")
		requireBytes(t, c, 0)
		testwait.Until(t, "handshake timer released", func() bool { return fake.Waiters() == 0 }, nil)
	})
	t.Run("partial-client-hello", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		srv := startServer(t, limitCfg(t, tlsOn(certFile, keyFile, false), limits(10*time.Second, 4*time.Second, 6*time.Second, time.Hour)), okHandler(), nil, nil, WithClock(fake))
		hello := clientHello(t)
		conn := dialRaw(t, srv.Addrs()["public"].String())
		c := capturing(conn)
		writeString(t, conn, string(hello[:len(hello)/2]))
		awaitDue(t, fake, 1, 4*time.Second, "handshake limit timer")
		advanceBelow(t, fake, 4*time.Second, c)
		fake.Advance(time.Nanosecond)
		awaitCut(t, c, "handshake limit cut")
		requireBytes(t, c, 0)
		testwait.Until(t, "handshake timer released", func() bool { return fake.Waiters() == 0 }, nil)
	})
	t.Run("header-only", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		srv := startServer(t, limitCfg(t, tlsOn(certFile, keyFile, true), limits(3*time.Second, 0, 0, 0)), okHandler(), nil, nil, WithClock(fake))
		c := capturing(dialRaw(t, srv.Addrs()["public"].String()))
		awaitDue(t, fake, 1, 3*time.Second, "handshake limit timer")
		advanceBelow(t, fake, 3*time.Second, c)
		fake.Advance(time.Nanosecond)
		awaitCut(t, c, "handshake limit cut")
		requireBytes(t, c, 0)
	})
	t.Run("write-only", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		srv := startServer(t, limitCfg(t, tlsOn(certFile, keyFile, true), limits(0, 0, 5*time.Second, time.Hour)), okHandler(), nil, nil, WithClock(fake))
		c := capturing(dialRaw(t, srv.Addrs()["public"].String()))
		awaitDue(t, fake, 1, 5*time.Second, "handshake limit timer")
		advanceBelow(t, fake, 5*time.Second, c)
		fake.Advance(time.Nanosecond)
		awaitCut(t, c, "handshake limit cut")
		requireBytes(t, c, 0)
	})
	t.Run("no-limit", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		srv := startServer(t, limitCfg(t, tlsOn(certFile, keyFile, true), limits(0, 0, 0, time.Hour)), okHandler(), nil, nil, WithClock(fake))
		conn := dialRaw(t, srv.Addrs()["public"].String())
		// Real stall: 20ms with the clock frozen, to observe that no handshake timer is armed
		time.Sleep(20 * time.Millisecond)
		if n := fake.Waiters(); n != 0 {
			t.Fatalf("pending timers = %d, want 0", n)
		}
		fake.Advance(time.Hour - time.Nanosecond)
		tc := tls.Client(conn, &tls.Config{RootCAs: pool, ServerName: "localhost"})
		if err := tc.Handshake(); err != nil {
			t.Fatalf("handshake after silent period: %v", err)
		}
		br := bufio.NewReader(tc)
		writeString(t, tc, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
		if status, body := readResponse(t, br); status != http.StatusOK || body != "ok" {
			t.Fatalf("status = %d, body = %q; want 200 \"ok\"", status, body)
		}
	})
	t.Run("completed-handshake-stops-timer", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		srv := startServer(t, limitCfg(t, tlsOn(certFile, keyFile, false), limits(10*time.Second, 4*time.Second, 6*time.Second, time.Hour)), okHandler(), nil, nil, WithClock(fake))
		tc := dialTLS(t, srv.Addrs()["public"].String(), pool)
		waitHandshakeReleased(t, fake, 4*time.Second)
		c := capturing(tc)
		writeString(t, tc, "GET / HTTP/1.1\r\nHost: x\r\n")
		awaitDue(t, fake, 1, 10*time.Second, "header limit timer")
		if n := fake.Due(4 * time.Second); n != 0 {
			t.Fatalf("handshake limit still armed: due within 4s = %d", n)
		}
		fake.Advance(4 * time.Second)
		stillOpen(t, c)
		fake.Advance(6*time.Second - time.Nanosecond)
		stillOpen(t, c)
		fake.Advance(time.Nanosecond)
		awaitCut(t, c, "header limit cut")
	})
}

func TestHTTP2StillNegotiatedAndServed(t *testing.T) {
	wires := bubbleWires(t)
	tlsWire, plainWire := wires[0], wires[1]
	body := strings.Repeat("abcdefgh", 1024)
	frozen := limits(time.Second, time.Second, time.Second, time.Second)
	cases := []struct {
		name   string
		w      bubbleWire
		muts   []func(*config.Listener)
		tlsOn  bool
		proto  int
		alpnOK func(string) bool
	}{
		{"tls-h2", tlsWire, []func(*config.Listener){frozen, tlsOn(tlsWire.certFile, tlsWire.keyFile, true)}, true, 2, func(s string) bool { return s == "h2" }},
		{"h2c", plainWire, []func(*config.Listener){frozen, h2cOn}, false, 2, func(s string) bool { return s == "" }},
		{"tls-http1-only", tlsWire, []func(*config.Listener){frozen, tlsOn(tlsWire.certFile, tlsWire.keyFile, false)}, true, 1, func(s string) bool { return s == "" || s == "http/1.1" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				m := newMemNet(64 << 10)
				t.Cleanup(m.close)
				srv := startBubbleGateway(t, m, limitCfg(t, tc.muts...), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					io.WriteString(w, body)
				}), &logBuffer{}, WithClock(clock.NewFake(serverStart)))
				addr := srv.Addrs()["public"].String()
				scheme := "http://"
				if tc.tlsOn {
					scheme = "https://"
				}
				client := bubbleClient(t, m, tc.w)
				for i := range 2 {
					got := getVia(t, client, scheme+addr+"/")
					if got.status != http.StatusOK || got.body != body {
						t.Fatalf("request %d: status = %d, body length = %d; want 200 and the full body", i+1, got.status, len(got.body))
					}
					if got.major != tc.proto {
						t.Fatalf("request %d: ProtoMajor = %d, want %d", i+1, got.major, tc.proto)
					}
					if tc.tlsOn && !tc.alpnOK(got.alpn) {
						t.Fatalf("request %d: negotiated protocol = %q", i+1, got.alpn)
					}
					if i == 1 && !got.reused {
						t.Fatal("second request did not reuse the connection")
					}
				}
			})
		})
	}
}

func TestHTTP1ReadLimit(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	for _, kind := range []struct {
		name string
		tls  bool
	}{{"plain", false}, {"tls", true}} {
		t.Run(kind.name, func(t *testing.T) {
			fake := clock.NewFake(serverStart)
			errc := make(chan error, 1)
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, err := io.ReadAll(r.Body)
				errc <- err
			})
			muts := []func(*config.Listener){limits(time.Hour, 4*time.Second, 0, 0)}
			var p *x509.CertPool
			if kind.tls {
				muts = append(muts, tlsOn(certFile, keyFile, true))
				p = pool
			}
			srv := startServer(t, limitCfg(t, muts...), h, nil, nil, WithClock(fake))
			conn := dialConn(t, srv.Addrs()["public"].String(), p)
			if kind.tls {
				waitHandshakeReleased(t, fake, 4*time.Second)
			}
			writeString(t, conn, "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n0123456789")
			awaitDue(t, fake, 1, 4*time.Second, "read limit timer")
			fake.Advance(4*time.Second - time.Nanosecond)
			select {
			case err := <-errc:
				t.Fatalf("handler ended before the read limit: %v", err)
			default:
			}
			fake.Advance(time.Nanosecond)
			err, _ := testwait.Recv(t, "handler read error", errc, nil)
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("handler read error = %v, want a deadline error", err)
			}
		})
	}
}

func TestHTTP1ReadLimitClearedAtBodyEnd(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	for _, kind := range []struct {
		name string
		tls  bool
	}{{"plain", false}, {"tls", true}} {
		t.Run(kind.name, func(t *testing.T) {
			fake := clock.NewFake(serverStart)
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read body: %v", err)
					return
				}
				entered <- struct{}{}
				<-release
				io.WriteString(w, "ok:"+string(b))
			})
			muts := []func(*config.Listener){limits(time.Hour, 4*time.Second, 0, 0)}
			var p *x509.CertPool
			if kind.tls {
				muts = append(muts, tlsOn(certFile, keyFile, true))
				p = pool
			}
			srv := startServer(t, limitCfg(t, muts...), h, nil, nil, WithClock(fake))
			conn := dialConn(t, srv.Addrs()["public"].String(), p)
			if kind.tls {
				waitHandshakeReleased(t, fake, 4*time.Second)
			}
			writeString(t, conn, "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello")
			testwait.Done(t, "body read by handler", entered, nil)
			fake.Advance(10 * time.Second)
			close(release)
			br := bufio.NewReader(conn)
			if status, body := readResponse(t, br); status != http.StatusOK || body != "ok:hello" {
				t.Fatalf("status = %d, body = %q; want 200 \"ok:hello\"", status, body)
			}
		})
	}
}

func TestHTTP1WriteLimit(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	const total = 64 << 20
	for _, kind := range []struct {
		name string
		tls  bool
	}{{"plain", false}, {"tls", true}} {
		t.Run(kind.name, func(t *testing.T) {
			fake := clock.NewFake(serverStart)
			errc := make(chan error, 1)
			started := make(chan struct{}, 1)
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				chunk := make([]byte, 32<<10)
				for written := 0; written < total; written += len(chunk) {
					if _, err := w.Write(chunk); err != nil {
						errc <- err
						return
					}
					if written == 0 {
						if err := http.NewResponseController(w).Flush(); err != nil {
							errc <- err
							return
						}
						started <- struct{}{}
					}
				}
				errc <- nil
			})
			muts := []func(*config.Listener){limits(0, 0, 5*time.Second, 0)}
			var p *x509.CertPool
			if kind.tls {
				muts = append(muts, tlsOn(certFile, keyFile, true))
				p = pool
			}
			srv := startServer(t, limitCfg(t, muts...), h, nil, nil, WithClock(fake))
			conn := dialConn(t, srv.Addrs()["public"].String(), p)
			if kind.tls {
				waitHandshakeReleased(t, fake, 5*time.Second)
			}
			writeString(t, conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
			awaitDue(t, fake, 1, 5*time.Second, "write limit timer")
			testwait.Done(t, "first response bytes flushed", started, nil)
			fake.Advance(5*time.Second - time.Nanosecond)
			select {
			case err := <-errc:
				t.Fatalf("handler write ended before the write limit: %v", err)
			default:
			}
			fake.Advance(time.Nanosecond)
			err, _ := testwait.Recv(t, "handler write error", errc, nil)
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("handler write error = %v, want a deadline error", err)
			}
			type drained struct {
				status int
				n      int64
				err    error
			}
			ch := make(chan drained, 1)
			go func() {
				resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
				if err != nil {
					ch <- drained{err: err}
					return
				}
				n, _ := io.Copy(io.Discard, resp.Body)
				ch <- drained{status: resp.StatusCode, n: n}
			}()
			d, _ := testwait.Recv(t, "client drain", ch, nil)
			if d.err != nil {
				t.Fatalf("read response: %v", d.err)
			}
			if d.status != http.StatusOK || d.n >= total {
				t.Fatalf("status = %d, body bytes = %d; want 200 and fewer than %d", d.status, d.n, total)
			}
		})
	}
}

func TestHTTP1StreamingWriteLimit(t *testing.T) {
	fake := clock.NewFake(serverStart)
	ticks := make(chan struct{})
	errc := make(chan error, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		rc := http.NewResponseController(w)
		for i := 1; ; i++ {
			fmt.Fprintf(w, "data: %d\n\n", i)
			if err := rc.Flush(); err != nil {
				errc <- err
				return
			}
			select {
			case <-ticks:
			case <-r.Context().Done():
				return
			}
		}
	})
	srv := startServer(t, limitCfg(t, limits(0, 0, 5*time.Second, 0)), h, nil, nil, WithClock(fake))
	conn := dialRaw(t, srv.Addrs()["public"].String())
	writeString(t, conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	events := bufio.NewReader(resp.Body)
	awaitDue(t, fake, 1, 5*time.Second, "write limit timer")
	expectLine(t, events, "data: 1")
	expectLine(t, events, "")
	fake.Advance(5*time.Second - time.Nanosecond)
	ticks <- struct{}{}
	expectLine(t, events, "data: 2")
	expectLine(t, events, "")
	fake.Advance(time.Nanosecond)
	ticks <- struct{}{}
	err, _ = testwait.Recv(t, "streaming handler write error", errc, nil)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("handler write error = %v, want a deadline error", err)
	}
	if line, err := readLine(t, events); err == nil {
		t.Fatalf("stream continued after the write limit with line %q", line)
	}
}

func expectLine(t *testing.T, br *bufio.Reader, want string) {
	t.Helper()
	line, err := readLine(t, br)
	if err != nil {
		t.Fatalf("read line %q: %v", want, err)
	}
	if line != want {
		t.Fatalf("line = %q, want %q", line, want)
	}
}

func TestHTTP1IdleLimit(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	for _, kind := range []struct {
		name string
		tls  bool
	}{{"plain", false}, {"tls", true}} {
		t.Run(kind.name, func(t *testing.T) {
			muts := []func(*config.Listener){limits(0, 0, 0, 6*time.Second)}
			var p *x509.CertPool
			if kind.tls {
				muts = append(muts, tlsOn(certFile, keyFile, true))
				p = pool
			}
			t.Run("idle-cut", func(t *testing.T) {
				fake := clock.NewFake(serverStart)
				srv := startServer(t, limitCfg(t, muts...), okHandler(), nil, nil, WithClock(fake))
				conn := dialConn(t, srv.Addrs()["public"].String(), p)
				br := bufio.NewReader(conn)
				exchange(t, conn, br)
				awaitIdle(t, fake, 1)
				c := capturing(br)
				advanceBelowIdle(t, fake, 6*time.Second, c)
				fake.Advance(time.Nanosecond)
				awaitCut(t, c, "idle limit cut")
				requireNoResponse(t, c)
			})
			t.Run("new-request-stops-timer", func(t *testing.T) {
				fake := clock.NewFake(serverStart)
				srv := startServer(t, limitCfg(t, muts...), okHandler(), nil, nil, WithClock(fake))
				conn := dialConn(t, srv.Addrs()["public"].String(), p)
				br := bufio.NewReader(conn)
				exchange(t, conn, br)
				awaitIdle(t, fake, 1)
				fake.Advance(6*time.Second - time.Nanosecond)
				exchange(t, conn, br)
				awaitIdle(t, fake, 1)
				fake.Advance(time.Nanosecond)
				exchange(t, conn, br)
			})
		})
	}
}

// stall holds a test for a real interval so that the connection is between two steps of an exchange.
func stall() {
	// Real stall: 20ms exceeds every 1ms limit in real time
	time.Sleep(20 * time.Millisecond)
}

// holdFirstWrite splits the first write made through it in two and stalls between the halves.
type holdFirstWrite struct {
	net.Conn
	split int
	once  sync.Once
}

func (h *holdFirstWrite) Write(b []byte) (int, error) {
	first := false
	h.once.Do(func() { first = true })
	if !first || len(b) <= h.split {
		return h.Conn.Write(b)
	}
	n, err := h.Conn.Write(b[:h.split])
	if err != nil {
		return n, err
	}
	stall()
	m, err := h.Conn.Write(b[h.split:])
	return n + m, err
}

// stallHandler answers /big with 8 MiB, and every other request with "ok"; POST bodies are sent to got.
func stallHandler(got chan<- string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/big" {
			chunk := make([]byte, 64<<10)
			for range 128 {
				w.Write(chunk)
			}
			return
		}
		if r.Method == http.MethodPost {
			b, _ := io.ReadAll(r.Body)
			got <- string(b)
		}
		io.WriteString(w, "ok")
	})
}

func TestFrozenClockRealStallCutsNothing(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	all := limits(time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond)

	t.Run("plain", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		got := make(chan string, 16)
		srv := startServer(t, limitCfg(t, all), stallHandler(got), nil, nil, WithClock(fake))
		addr := srv.Addrs()["public"].String()
		runRawStalls(t, dialConn(t, addr, nil), got)
		runRawStalls(t, dialConn(t, addr, nil), got)
	})

	t.Run("tls", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		got := make(chan string, 16)
		srv := startServer(t, limitCfg(t, all, tlsOn(certFile, keyFile, false)), stallHandler(got), nil, nil, WithClock(fake))
		addr := srv.Addrs()["public"].String()
		runRawStalls(t, dialConn(t, addr, pool), got)
		t.Run("stall-before-client-hello", func(t *testing.T) {
			raw := dialRaw(t, addr)
			stall()
			tc := tls.Client(raw, &tls.Config{RootCAs: pool, ServerName: "localhost"})
			if err := tc.Handshake(); err != nil {
				t.Fatal(err)
			}
			br := bufio.NewReader(tc)
			writeString(t, tc, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
			if status, _ := readResponse(t, br); status != http.StatusOK {
				t.Fatalf("status = %d, want 200", status)
			}
		})
		t.Run("split-client-hello", func(t *testing.T) {
			raw := dialRaw(t, addr)
			tc := tls.Client(&holdFirstWrite{Conn: raw, split: 16}, &tls.Config{RootCAs: pool, ServerName: "localhost"})
			if err := tc.Handshake(); err != nil {
				t.Fatal(err)
			}
			br := bufio.NewReader(tc)
			writeString(t, tc, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
			if status, _ := readResponse(t, br); status != http.StatusOK {
				t.Fatalf("status = %d, want 200", status)
			}
		})
	})

	t.Run("h2", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		got := make(chan string, 16)
		srv := startServer(t, limitCfg(t, limits(time.Millisecond, 0, 0, 0), tlsOn(certFile, keyFile, true)), stallHandler(got), nil, nil, WithClock(fake))
		base := "https://" + srv.Addrs()["public"].String()
		t.Run("stall-before-client-hello", func(t *testing.T) {
			tr := &http.Transport{
				DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					raw, err := (&net.Dialer{}).DialContext(ctx, network, addr)
					if err != nil {
						return nil, err
					}
					stall()
					tc := tls.Client(raw, &tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{"h2", "http/1.1"}})
					if err := tc.HandshakeContext(ctx); err != nil {
						raw.Close()
						return nil, err
					}
					return tc, nil
				},
				ForceAttemptHTTP2: true,
			}
			r := getVia(t, newClient(t, tr), base+"/")
			if r.status != http.StatusOK || r.major != 2 || r.alpn != "h2" {
				t.Fatalf("status = %d, ProtoMajor = %d, ALPN = %q; want 200, 2, h2", r.status, r.major, r.alpn)
			}
		})
		t.Run("split-client-hello", func(t *testing.T) {
			tr := &http.Transport{
				DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					raw, err := (&net.Dialer{}).DialContext(ctx, network, addr)
					if err != nil {
						return nil, err
					}
					tc := tls.Client(&holdFirstWrite{Conn: raw, split: 16}, &tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{"h2", "http/1.1"}})
					if err := tc.HandshakeContext(ctx); err != nil {
						raw.Close()
						return nil, err
					}
					return tc, nil
				},
				ForceAttemptHTTP2: true,
			}
			r := getVia(t, newClient(t, tr), base+"/")
			if r.status != http.StatusOK || r.major != 2 || r.alpn != "h2" {
				t.Fatalf("status = %d, ProtoMajor = %d, ALPN = %q; want 200, 2, h2", r.status, r.major, r.alpn)
			}
		})
	})
}

// runRawStalls runs the stalled HTTP/1 exchanges on conn, whose TLS handshake (if any) is already complete.
func runRawStalls(t *testing.T, conn net.Conn, got <-chan string) {
	t.Helper()
	br := bufio.NewReader(conn)
	writeString(t, conn, "GET / HTTP/1.1\r\nHost: x\r\n")
	stall()
	writeString(t, conn, "\r\n")
	if status, body := readResponse(t, br); status != http.StatusOK || body != "ok" {
		t.Fatalf("partial header: status = %d, body = %q", status, body)
	}
	writeString(t, conn, "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\n01234")
	stall()
	writeString(t, conn, "56789")
	if status, _ := readResponse(t, br); status != http.StatusOK {
		t.Fatalf("partial body: status = %d", status)
	}
	if body, _ := testwait.Recv(t, "handler body", got, nil); body != "0123456789" {
		t.Fatalf("handler body = %q, want the full body", body)
	}
	writeString(t, conn, "GET /big HTTP/1.1\r\nHost: x\r\n\r\n")
	stall()
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	n, err := io.Copy(io.Discard, resp.Body)
	if err != nil || n != 8<<20 {
		t.Fatalf("response body = %d bytes, err = %v; want 8 MiB", n, err)
	}
	writeString(t, conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	if status, _ := readResponse(t, br); status != http.StatusOK {
		t.Fatalf("keep-alive request after stall: status = %d", status)
	}
}

func TestHTTP1WriteLimitArmedWhenRequestReadEnds(t *testing.T) {
	newConn := func(t *testing.T, fake *clock.Fake) (*limitConn, net.Conn) {
		t.Helper()
		srvSide, peer := net.Pipe()
		t.Cleanup(func() {
			srvSide.Close()
			peer.Close()
		})
		ln := &limitListener{
			lim:     connLimits{header: time.Hour, write: 5 * time.Second},
			clk:     fake,
			idleClk: clock.Housekeeping(fake),
		}
		return &limitConn{Conn: srvSide, ln: ln, startSet: true}, peer
	}
	startWrite := func(lc *limitConn) <-chan error {
		errc := make(chan error, 1)
		go func() {
			_, err := lc.Write([]byte("response"))
			errc <- err
		}()
		return errc
	}
	t.Run("success", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		lc, _ := newConn(t, fake)
		lc.state(http.StateNew)
		lc.SetReadDeadline(time.Time{})
		if n := fake.Due(5 * time.Second); n != 0 {
			t.Fatalf("write limit armed before the response: due = %d", n)
		}
		lc.state(http.StateActive)
		if n := fake.Due(5 * time.Second); n != 1 {
			t.Fatalf("write limit due within 5s = %d, want 1", n)
		}
		errc := startWrite(lc)
		fake.Advance(5*time.Second - time.Nanosecond)
		select {
		case err := <-errc:
			t.Fatalf("write returned before the limit: %v", err)
		default:
		}
		fake.Advance(time.Nanosecond)
		err, _ := testwait.Recv(t, "write error", errc, nil)
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("write error = %v, want a deadline error", err)
		}
	})
	t.Run("error-return", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		lc, _ := newConn(t, fake)
		lc.state(http.StateNew)
		lc.state(http.StateActive)
		if n := fake.Due(5 * time.Second); n != 1 {
			t.Fatalf("write limit due within 5s = %d, want 1", n)
		}
		errc := startWrite(lc)
		fake.Advance(5*time.Second - time.Nanosecond)
		select {
		case err := <-errc:
			t.Fatalf("write returned before the limit: %v", err)
		default:
		}
		fake.Advance(time.Nanosecond)
		err, _ := testwait.Recv(t, "write error", errc, nil)
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("write error = %v, want a deadline error", err)
		}
	})
	t.Run("cleared", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		lc, _ := newConn(t, fake)
		lc.state(http.StateNew)
		lc.state(http.StateActive)
		lc.SetWriteDeadline(time.Time{})
		if n := fake.Due(time.Hour); n != 1 {
			t.Fatalf("pending limits due within 1h = %d, want the header limit only", n)
		}
		if n := fake.Due(5 * time.Second); n != 0 {
			t.Fatalf("write limit still armed after clearing: due = %d", n)
		}
		errc := startWrite(lc)
		fake.Advance(5 * time.Second)
		select {
		case err := <-errc:
			t.Fatalf("write cut after the cleared limit: %v", err)
		default:
		}
	})
}

func TestHTTP1ErrorResponseUnderWriteLimit(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	for _, kind := range []struct {
		name string
		tls  bool
	}{{"plain", false}, {"tls", true}} {
		t.Run(kind.name, func(t *testing.T) {
			fake := clock.NewFake(serverStart)
			muts := []func(*config.Listener){limits(time.Hour, 0, 5*time.Second, 0)}
			var p *x509.CertPool
			if kind.tls {
				muts = append(muts, tlsOn(certFile, keyFile, true))
				p = pool
			}
			srv := startServer(t, limitCfg(t, muts...), okHandler(), nil, nil, WithClock(fake))
			conn := dialConn(t, srv.Addrs()["public"].String(), p)
			if kind.tls {
				waitHandshakeReleased(t, fake, 5*time.Second)
			}
			c := capturing(conn)
			writeString(t, conn, "GET / HTTP/1.1\r\nHost: x\r\nBad Header\r\n\r\n")
			awaitCut(t, c, "server closes after the error response")
			if !strings.HasPrefix(string(c.bytes()), "HTTP/1.1 400 Bad Request") {
				t.Fatalf("response = %q, want 400 Bad Request", c.bytes())
			}
			testwait.Until(t, "limit timers released", func() bool { return fake.Waiters() == 0 }, nil)
		})
	}
}

func TestH2cFirstRequestStartsAfterPeek(t *testing.T) {
	for _, kind := range []struct {
		name string
		h2c  bool
	}{{"h2c", true}, {"plain", false}} {
		t.Run(kind.name, func(t *testing.T) {
			fake := clock.NewFake(serverStart)
			errc := make(chan error, 1)
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, err := io.ReadAll(r.Body)
				errc <- err
			})
			muts := []func(*config.Listener){limits(time.Hour, 4*time.Second, 0, 0)}
			if kind.h2c {
				muts = append(muts, h2cOn)
			}
			srv := startServer(t, limitCfg(t, muts...), h, nil, nil, WithClock(fake))
			conn := dialRaw(t, srv.Addrs()["public"].String())
			testwait.Until(t, "connection accepted", func() bool { return fake.Waiters() == 1 }, nil)
			writeString(t, conn, "GET / HTTP")
			fake.Advance(3 * time.Second)
			writeString(t, conn, "/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nab")
			if kind.h2c {
				awaitDue(t, fake, 1, 4*time.Second, "read limit timer")
				if n := fake.Due(4*time.Second - time.Nanosecond); n != 0 {
					t.Fatalf("read limit due before 4s after the 14th byte = %d", n)
				}
				fake.Advance(4*time.Second - time.Nanosecond)
				select {
				case err := <-errc:
					t.Fatalf("handler ended before the read limit: %v", err)
				default:
				}
				fake.Advance(time.Nanosecond)
			} else {
				awaitDue(t, fake, 1, time.Second, "read limit timer")
				if n := fake.Due(time.Second); n != 1 {
					t.Fatalf("read limit due within 1s = %d, want 1", n)
				}
				fake.Advance(time.Second)
			}
			err, _ := testwait.Recv(t, "handler read error", errc, nil)
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("handler read error = %v, want a deadline error", err)
			}
		})
	}
}

func TestHijackedConnNeverCut(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	for _, kind := range []struct {
		name string
		tls  bool
	}{{"plain", false}, {"tls", true}} {
		t.Run(kind.name, func(t *testing.T) {
			fake := clock.NewFake(serverStart)
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, rw, err := http.NewResponseController(w).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
				rw.Flush()
				for {
					line, err := rw.ReadString('\n')
					if err != nil {
						return
					}
					rw.WriteString(line)
					rw.Flush()
				}
			})
			muts := []func(*config.Listener){limits(time.Second, time.Second, time.Second, time.Second)}
			var p *x509.CertPool
			if kind.tls {
				muts = append(muts, tlsOn(certFile, keyFile, true))
				p = pool
			}
			srv := startServer(t, limitCfg(t, muts...), h, nil, nil, WithClock(fake))
			conn := dialConn(t, srv.Addrs()["public"].String(), p)
			writeString(t, conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
			br := bufio.NewReader(conn)
			resp, err := http.ReadResponse(br, nil)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusSwitchingProtocols {
				t.Fatalf("status = %d, want 101", resp.StatusCode)
			}
			if fake.Waiters() != 0 || fake.HousekeepingWaiters() != 0 {
				t.Fatalf("pending timers after hijack: %d, housekeeping %d; want 0", fake.Waiters(), fake.HousekeepingWaiters())
			}
			fake.Advance(time.Hour)
			writeString(t, conn, "ping\n")
			if line, err := readLine(t, br); err != nil || line != "ping" {
				t.Fatalf("echo = %q, %v; want \"ping\"", line, err)
			}
		})
	}
}

func TestShutdownStopsLimitTimers(t *testing.T) {
	base := runtime.NumGoroutine()
	fake := clock.NewFake(serverStart)
	entered := make(chan struct{}, 2)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/hang":
			entered <- struct{}{}
			<-r.Context().Done()
		case "/hijack":
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			entered <- struct{}{}
			<-r.Context().Done()
			conn.Close()
		default:
			io.WriteString(w, "ok")
		}
	})
	srv := startServer(t, limitCfg(t, limits(time.Hour, time.Hour, time.Hour, time.Hour)), h, nil, nil, WithClock(fake))
	addr := srv.Addrs()["public"].String()

	header := dialRaw(t, addr)
	writeString(t, header, "GET / HTTP/1.1\r\nHost: x\r\n")

	idle := dialRaw(t, addr)
	exchange(t, idle, bufio.NewReader(idle))

	hang := dialRaw(t, addr)
	writeString(t, hang, "GET /hang HTTP/1.1\r\nHost: x\r\n\r\n")
	testwait.Done(t, "hang handler entered", entered, nil)

	hijacked := dialRaw(t, addr)
	writeString(t, hijacked, "GET /hijack HTTP/1.1\r\nHost: x\r\n\r\n")
	testwait.Done(t, "hijack handler entered", entered, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := srv.Shutdown(ctx); !errors.Is(err, ErrForcedClose) {
		t.Fatalf("Shutdown error = %v, want ErrForcedClose", err)
	}
	testwait.Until(t, "limit timers released", func() bool {
		return fake.Waiters() == 0 && fake.HousekeepingWaiters() == 0
	}, func() string {
		return fmt.Sprintf("waiters=%d housekeeping=%d", fake.Waiters(), fake.HousekeepingWaiters())
	})
	testwait.Until(t, "goroutines back to baseline", func() bool {
		return runtime.NumGoroutine() <= base
	}, nil)
}

func TestTwoServersOwnClocks(t *testing.T) {
	f1 := clock.NewFake(serverStart)
	f2 := clock.NewFake(serverStart)
	cfg := func() *config.Config { return limitCfg(t, limits(time.Second, 0, 0, 0)) }
	s1 := startServer(t, cfg(), okHandler(), nil, nil, WithClock(f1))
	s2 := startServer(t, cfg(), okHandler(), nil, nil, WithClock(f2))
	conn1 := dialRaw(t, s1.Addrs()["public"].String())
	conn2 := dialRaw(t, s2.Addrs()["public"].String())
	c1 := capturing(conn1)
	c2 := capturing(conn2)
	writeString(t, conn1, "GET / HTTP/1.1\r\nHost: x\r\n")
	writeString(t, conn2, "GET / HTTP/1.1\r\nHost: x\r\n")
	awaitDue(t, f1, 1, time.Second, "server one header timer")
	awaitDue(t, f2, 1, time.Second, "server two header timer")
	f1.Advance(time.Second)
	awaitCut(t, c1, "server one connection cut")
	stillOpen(t, c2)
	if n := f2.Due(time.Second); n != 1 {
		t.Fatalf("server two due within 1s = %d, want 1", n)
	}
	f2.Advance(time.Second)
	awaitCut(t, c2, "server two connection cut")
}

type closeWriteConn struct {
	net.Conn
	closed bool
}

func (c *closeWriteConn) CloseWrite() error {
	c.closed = true
	return nil
}

type plainConn struct {
	net.Conn
}

func TestLimitConnForwardsCloseWrite(t *testing.T) {
	stub := &closeWriteConn{}
	if err := (&limitConn{Conn: stub}).CloseWrite(); err != nil {
		t.Fatalf("CloseWrite = %v, want nil", err)
	}
	if !stub.closed {
		t.Fatal("CloseWrite was not forwarded to the wrapped conn")
	}
	if err := (&limitConn{Conn: plainConn{}}).CloseWrite(); err != nil {
		t.Fatalf("CloseWrite over a conn without CloseWrite = %v, want nil", err)
	}
}
