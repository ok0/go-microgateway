package upstream

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testwait"
)

type loggedConn struct {
	net.Conn
	wrote     atomic.Int64
	closed    atomic.Bool
	done      chan struct{}
	once      sync.Once
	hold      chan struct{}
	entered   chan struct{}
	enterOnce sync.Once
}

func (c *loggedConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.wrote.Add(int64(n))
	return n, err
}

func (c *loggedConn) Close() error {
	if c.hold != nil {
		c.enterOnce.Do(func() { close(c.entered) })
		<-c.hold
	}
	c.closed.Store(true)
	c.once.Do(func() { close(c.done) })
	return c.Conn.Close()
}

type connLog struct {
	mu    sync.Mutex
	conns []*loggedConn
	hold  chan struct{}
}

func (l *connLog) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	raw, err := (&net.Dialer{}).DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	c := &loggedConn{Conn: raw, done: make(chan struct{}), hold: l.hold, entered: make(chan struct{})}
	l.mu.Lock()
	l.conns = append(l.conns, c)
	l.mu.Unlock()
	return c, nil
}

func (l *connLog) n() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.conns)
}

func (l *connLog) at(i int) *loggedConn {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.conns[i]
}

func idleCfg(h2 bool) config.Upstream {
	return config.Upstream{
		Name:  "u",
		HTTP2: h2,
		Timeouts: config.Timeouts{
			Connect:        dur(time.Hour),
			TLSHandshake:   dur(time.Hour),
			ResponseHeader: dur(time.Hour),
			Idle:           dur(time.Minute),
		},
	}
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	})
}

// backend starts a plain HTTP/1 server when h2 is false, and a TLS server with HTTP/2 enabled otherwise.
func backend(t *testing.T, h2 bool, h http.Handler) (string, *tls.Config) {
	t.Helper()
	if !h2 {
		srv := httptest.NewServer(h)
		t.Cleanup(srv.Close)
		return srv.URL, nil
	}
	return tlsBackend(t, h)
}

// tlsBackend starts a TLS server with HTTP/2 enabled and returns its URL and a client config that trusts it.
func tlsBackend(t *testing.T, h http.Handler) (string, *tls.Config) {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	srv.EnableHTTP2 = true
	srv.TLS = &tls.Config{NextProtos: []string{"h2", "http/1.1"}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	return srv.URL, &tls.Config{RootCAs: pool}
}

func connOf(info httptrace.GotConnInfo) *loggedConn {
	return trackedOf(info.Conn).Conn.(*loggedConn)
}

func doReq(t *testing.T, tr *Transport, ctx context.Context, method, url string, body io.Reader, n int64) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = n
	return tr.RoundTrip(req)
}

type rtResult struct {
	resp *http.Response
	err  error
}

// startReq runs doReq on its own goroutine and delivers the outcome on the returned channel.
func startReq(t *testing.T, tr *Transport, ctx context.Context, method, url string, body io.Reader, n int64) <-chan rtResult {
	ch := make(chan rtResult, 1)
	go func() {
		resp, err := doReq(t, tr, ctx, method, url, body, n)
		ch <- rtResult{resp, err}
	}()
	return ch
}

// settleTrace returns ctx carrying a client trace whose PutIdleConn hook closes the returned channel once the
// connection has been put back into the idle pool. got, when non-nil, is the GotConn hook.
func settleTrace(ctx context.Context, got func(httptrace.GotConnInfo)) (context.Context, <-chan struct{}) {
	settled := make(chan struct{})
	var once sync.Once
	tr := &httptrace.ClientTrace{
		GotConn: got,
		PutIdleConn: func(err error) {
			if err == nil {
				once.Do(func() { close(settled) })
			}
		},
	}
	return httptrace.WithClientTrace(ctx, tr), settled
}

func readClose(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("body read: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("body close: %v", err)
	}
	return string(b)
}

func checkTimers(t *testing.T, fake *clock.Fake, waiters, housekeeping int) {
	t.Helper()
	if n := fake.Waiters(); n != waiters {
		t.Fatalf("Waiters() = %d, want %d", n, waiters)
	}
	if n := fake.HousekeepingWaiters(); n != housekeeping {
		t.Fatalf("HousekeepingWaiters() = %d, want %d", n, housekeeping)
	}
}

type onceBody struct {
	data     []byte
	n        atomic.Int64
	afterEOF atomic.Int32
	done     bool
}

func (b *onceBody) Read(p []byte) (int, error) {
	if len(b.data) == 0 {
		if b.done {
			b.afterEOF.Add(1)
		}
		b.done = true
		return 0, io.EOF
	}
	k := copy(p, b.data)
	b.data = b.data[k:]
	b.n.Add(int64(k))
	return k, nil
}

func TestIdleConnReusedBelowLimit(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		name := "http1"
		if h2 {
			name = "h2"
		}
		t.Run(name, func(t *testing.T) {
			url, tlsCfg := backend(t, h2, okHandler())
			fake := clock.NewFake(testStart)
			d := &connLog{}
			tr := newTransport(idleCfg(h2), d.dial, fake, tlsCfg)
			t.Cleanup(func() { _ = tr.Close() })

			ctx1, settled1 := settleTrace(context.Background(), nil)
			resp, err := doReq(t, tr, ctx1, "GET", url+"/", nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			if got := readClose(t, resp); got != "ok" {
				t.Fatalf("body = %q, want ok", got)
			}
			if !h2 {
				testwait.Done(t, "request 1 returned to idle pool", settled1, nil)
			}
			checkTimers(t, fake, 0, 1)

			fake.Advance(time.Minute - time.Nanosecond)

			var reused atomic.Bool
			ctx2, settled2 := settleTrace(context.Background(), func(info httptrace.GotConnInfo) {
				reused.Store(info.Reused)
			})
			resp, err = doReq(t, tr, ctx2, "GET", url+"/", nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !reused.Load() {
				t.Fatal("GotConn Reused = false, want true")
			}
			if n := d.n(); n != 1 {
				t.Fatalf("dials = %d, want 1", n)
			}
			if got := readClose(t, resp); got != "ok" {
				t.Fatalf("body = %q, want ok", got)
			}
			if !h2 {
				testwait.Done(t, "request 2 returned to idle pool", settled2, nil)
			}

			fake.Advance(time.Minute - time.Nanosecond)
			if d.at(0).closed.Load() {
				t.Fatal("idle connection closed before its limit")
			}
			fake.Advance(time.Nanosecond)
			if !d.at(0).closed.Load() {
				t.Fatal("idle connection still open at its limit")
			}
			checkTimers(t, fake, 0, 0)

			resp, err = doReq(t, tr, context.Background(), "GET", url+"/", nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			if got := readClose(t, resp); got != "ok" {
				t.Fatalf("body = %q, want ok", got)
			}
			if n := d.n(); n != 2 {
				t.Fatalf("dials = %d, want 2", n)
			}
		})
	}
}

func TestIdleExpiryClosesOnlyThatConn(t *testing.T) {
	gates := map[string]chan struct{}{"a": make(chan struct{}), "b": make(chan struct{})}
	arrived := make(chan string, 2)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		arrived <- id
		select {
		case <-gates[id]:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "ok")
	})
	url, _ := backend(t, false, h)
	fake := clock.NewFake(testStart)
	d := &connLog{}
	tr := newTransport(idleCfg(false), d.dial, fake, nil)
	t.Cleanup(func() { _ = tr.Close() })

	var mu sync.Mutex
	byID := map[string]*loggedConn{}
	start := func(id string) <-chan rtResult {
		ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) {
				mu.Lock()
				byID[id] = connOf(info)
				mu.Unlock()
			},
		})
		return startReq(t, tr, ctx, "GET", url+"/?id="+id, nil, 0)
	}
	resA := start("a")
	resB := start("b")
	testwait.Recv(t, "handler arrival 1", arrived, nil)
	testwait.Recv(t, "handler arrival 2", arrived, nil)
	if n := d.n(); n != 2 {
		t.Fatalf("dials = %d, want 2", n)
	}

	close(gates["a"])
	ra, _ := testwait.Recv(t, "response a", resA, nil)
	if ra.err != nil {
		t.Fatal(ra.err)
	}
	if got := readClose(t, ra.resp); got != "ok" {
		t.Fatalf("body a = %q, want ok", got)
	}

	fake.Advance(30 * time.Second)

	close(gates["b"])
	rb, _ := testwait.Recv(t, "response b", resB, nil)
	if rb.err != nil {
		t.Fatal(rb.err)
	}
	if got := readClose(t, rb.resp); got != "ok" {
		t.Fatalf("body b = %q, want ok", got)
	}

	fake.Advance(30 * time.Second)
	mu.Lock()
	connA, connB := byID["a"], byID["b"]
	mu.Unlock()
	if !connA.closed.Load() {
		t.Fatal("conn of a is open at its idle limit")
	}
	if connB.closed.Load() {
		t.Fatal("conn of b closed before its idle limit")
	}
	if n := fake.HousekeepingWaiters(); n != 1 {
		t.Fatalf("HousekeepingWaiters() = %d, want 1", n)
	}

	fake.Advance(30 * time.Second)
	if !connB.closed.Load() {
		t.Fatal("conn of b open at its idle limit")
	}
	if n := fake.HousekeepingWaiters(); n != 0 {
		t.Fatalf("HousekeepingWaiters() = %d, want 0", n)
	}
}

func TestIdleTimerNeverClosesActiveConn(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		name := "http1"
		if h2 {
			name = "h2"
		}
		t.Run(name, func(t *testing.T) {
			gate := make(chan struct{})
			arrived := make(chan struct{}, 1)
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				arrived <- struct{}{}
				select {
				case <-gate:
				case <-r.Context().Done():
					return
				}
				_, _ = io.WriteString(w, "ok")
			})
			url, tlsCfg := backend(t, h2, h)
			fake := clock.NewFake(testStart)
			d := &connLog{}
			tr := newTransport(idleCfg(h2), d.dial, fake, tlsCfg)
			t.Cleanup(func() { _ = tr.Close() })

			res := startReq(t, tr, context.Background(), "GET", url+"/", nil, 0)
			testwait.Done(t, "handler arrival", arrived, nil)

			fake.Advance(10 * time.Minute)
			if d.at(0).closed.Load() {
				t.Fatal("in-use connection closed by its idle timer")
			}
			if n := fake.HousekeepingWaiters(); n != 0 {
				t.Fatalf("HousekeepingWaiters() = %d while in use, want 0", n)
			}

			close(gate)
			r, _ := testwait.Recv(t, "response", res, nil)
			if r.err != nil {
				t.Fatal(r.err)
			}
			if r.resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", r.resp.StatusCode)
			}
			if got := readClose(t, r.resp); got != "ok" {
				t.Fatalf("body = %q, want ok", got)
			}
			if n := fake.HousekeepingWaiters(); n != 1 {
				t.Fatalf("HousekeepingWaiters() after Close = %d, want 1", n)
			}
		})
	}
}

func TestCloseStopsIdleTimers(t *testing.T) {
	arrived := make(chan struct{}, 1)
	gate := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("block") == "" {
			_, _ = io.WriteString(w, "ok")
			return
		}
		arrived <- struct{}{}
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "ok")
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	base := runtime.NumGoroutine()

	fake := clock.NewFake(testStart)
	d := &connLog{}
	tr := newTransport(idleCfg(false), d.dial, fake, nil)
	t.Cleanup(func() { _ = tr.Close() })

	ctxX, settledX := settleTrace(context.Background(), nil)
	respX, err := doReq(t, tr, ctxX, "GET", srv.URL+"/", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if respX.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", respX.StatusCode)
	}
	resY := startReq(t, tr, context.Background(), "GET", srv.URL+"/?block=1", nil, 0)
	testwait.Done(t, "blocking handler arrival", arrived, nil)
	if n := d.n(); n != 2 {
		t.Fatalf("dials = %d, want 2", n)
	}

	if got := readClose(t, respX); got != "ok" {
		t.Fatalf("body x = %q, want ok", got)
	}
	testwait.Done(t, "request x returned to idle pool", settledX, nil)
	checkTimers(t, fake, 0, 1)

	_ = tr.Close()
	checkTimers(t, fake, 0, 0)
	if !d.at(0).closed.Load() {
		t.Fatal("idle connection open after Close")
	}
	if d.at(1).closed.Load() {
		t.Fatal("in-use connection closed by Close before its request ended")
	}

	close(gate)
	ry, _ := testwait.Recv(t, "response y", resY, nil)
	if ry.err != nil {
		t.Fatal(ry.err)
	}
	if got := readClose(t, ry.resp); got != "ok" {
		t.Fatalf("body y = %q, want ok", got)
	}
	testwait.Done(t, "in-use connection closed after its request ended on a closed transport", d.at(1).done, nil)
	checkTimers(t, fake, 0, 0)

	srv.CloseClientConnections()
	testwait.Until(t, "goroutines back to baseline", func() bool {
		return runtime.NumGoroutine() <= base
	}, func() string {
		return fmt.Sprintf("goroutines=%d, baseline=%d", runtime.NumGoroutine(), base)
	})
}

// TestCloseInUseConnClosedByReadLoop pins who closes an in-use connection after Transport.Close. Two goroutines
// can close it: net/http's read loop, which closes it once the response body reaches EOF on a closed transport
// (its idle put fails), and the body reader, whose release finds the connection already closed and closes nothing.
// The test holds the read loop inside the socket close, checks that the reader's release does not close the
// connection, then checks that the connection is closed exactly once, by the read loop, after it finishes.
func TestCloseInUseConnClosedByReadLoop(t *testing.T) {
	arrived := make(chan struct{}, 1)
	release := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "ok")
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	base := runtime.NumGoroutine()

	var relOnce sync.Once
	unblock := func() { relOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)

	fake := clock.NewFake(testStart)
	hold := make(chan struct{})
	var holdOnce sync.Once
	open := func() { holdOnce.Do(func() { close(hold) }) }
	d := &connLog{hold: hold}
	tr := newTransport(idleCfg(false), d.dial, fake, nil)
	t.Cleanup(func() { _ = tr.Close() })
	t.Cleanup(open)

	var tc atomic.Pointer[trackedConn]
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) { tc.Store(trackedOf(info.Conn)) },
	})
	res := startReq(t, tr, ctx, "GET", srv.URL+"/?block=1", nil, 0)
	testwait.Done(t, "handler arrival", arrived, nil)
	if n := d.n(); n != 1 {
		t.Fatalf("dials = %d, want 1", n)
	}

	_ = tr.Close()
	checkTimers(t, fake, 0, 0)
	if d.at(0).closed.Load() {
		t.Fatal("in-use connection closed by Close before its request ended")
	}

	unblock()
	r, _ := testwait.Recv(t, "response", res, nil)
	if r.err != nil {
		t.Fatal(r.err)
	}
	rb, ok := r.resp.Body.(*releaseBody)
	if !ok {
		t.Fatalf("body type = %T, want *releaseBody", r.resp.Body)
	}

	b, err := io.ReadAll(rb.ReadCloser)
	if err != nil {
		t.Fatalf("body read: %v", err)
	}
	if string(b) != "ok" {
		t.Fatalf("body = %q, want ok", b)
	}

	testwait.Done(t, "read loop entered the connection close", d.at(0).entered, nil)
	c := tc.Load()
	c.mu.Lock()
	closed, inFlight := c.closed, c.inFlight
	c.mu.Unlock()
	if !closed || inFlight != 1 {
		t.Fatalf("closed = %v, inFlight = %d; want closed=true, inFlight=1", closed, inFlight)
	}

	rb.rel.done()
	c.mu.Lock()
	inFlight, expired := c.inFlight, c.expired.Load()
	c.mu.Unlock()
	if inFlight != 0 || expired {
		t.Fatalf("after release: inFlight = %d, expired = %v; want 0, false", inFlight, expired)
	}
	checkTimers(t, fake, 0, 0)
	if d.at(0).closed.Load() {
		t.Fatal("the socket close is still owned by the read loop: a synchronous check here is the pre-fix assertion and cannot pass")
	}

	open()
	testwait.Done(t, "in-use connection closed after its request ended on a closed transport", d.at(0).done, nil)
	checkTimers(t, fake, 0, 0)
	tr.mu.Lock()
	n := len(tr.conns)
	tr.mu.Unlock()
	if n != 0 {
		t.Fatalf("tracked connections = %d after the close, want 0", n)
	}
	_ = r.resp.Body.Close()

	srv.CloseClientConnections()
	testwait.Until(t, "goroutines back to baseline", func() bool {
		return runtime.NumGoroutine() <= base
	}, func() string {
		return fmt.Sprintf("goroutines=%d, baseline=%d", runtime.NumGoroutine(), base)
	})
}

func TestStaleHandoutReissued(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		for _, mode := range []string{"timer", "close-idle"} {
			proto := "http1"
			if h2 {
				proto = "h2"
			}
			t.Run(proto+"/"+mode, func(t *testing.T) {
				var count atomic.Int32
				var mu sync.Mutex
				var bodies []string
				h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					count.Add(1)
					b, _ := io.ReadAll(r.Body)
					mu.Lock()
					bodies = append(bodies, string(b))
					mu.Unlock()
					_, _ = io.WriteString(w, "ok")
				})
				url, tlsCfg := backend(t, h2, h)
				fake := clock.NewFake(testStart)
				d := &connLog{}
				tr := newTransport(idleCfg(h2), d.dial, fake, tlsCfg)
				t.Cleanup(func() { _ = tr.Close() })

				ctx1, settled1 := settleTrace(context.Background(), nil)
				resp, err := doReq(t, tr, ctx1, "GET", url+"/", nil, 0)
				if err != nil {
					t.Fatal(err)
				}
				readClose(t, resp)
				if !h2 {
					testwait.Done(t, "request 1 returned to idle pool", settled1, nil)
				}
				before := d.at(0).wrote.Load()

				var armed atomic.Bool
				armed.Store(true)
				fn := func() { fake.Advance(time.Minute) }
				if mode == "close-idle" {
					fn = tr.CloseIdleConnections
				}
				tr.onHandout = func() {
					if armed.CompareAndSwap(true, false) {
						fn()
					}
				}

				ob := &onceBody{data: []byte("payload-123")}
				var gotConn, wroteHeaders, wroteRequest atomic.Int32
				ctx2 := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
					GotConn:      func(httptrace.GotConnInfo) { gotConn.Add(1) },
					WroteHeaders: func() { wroteHeaders.Add(1) },
					WroteRequest: func(httptrace.WroteRequestInfo) { wroteRequest.Add(1) },
				})
				resp, err = doReq(t, tr, ctx2, "POST", url+"/", ob, 11)
				if err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("status = %d, want 200", resp.StatusCode)
				}
				if got := readClose(t, resp); got != "ok" {
					t.Fatalf("body = %q, want ok", got)
				}

				if n := count.Load(); n != 2 {
					t.Fatalf("backend requests = %d, want 2", n)
				}
				mu.Lock()
				if len(bodies) != 2 || bodies[1] != "payload-123" {
					t.Fatalf("backend bodies = %q, want second body payload-123", bodies)
				}
				mu.Unlock()
				if n := ob.n.Load(); n != 11 {
					t.Fatalf("body bytes read = %d, want 11", n)
				}
				if n := ob.afterEOF.Load(); n != 0 {
					t.Fatalf("body reads after EOF = %d, want 0", n)
				}
				if n := d.n(); n != 2 {
					t.Fatalf("dials = %d, want 2", n)
				}
				if got := d.at(0).wrote.Load(); got != before {
					t.Fatalf("stale connection wrote %d bytes, want none", got-before)
				}
				if n := gotConn.Load(); n != 1 {
					t.Fatalf("GotConn hooks = %d, want 1", n)
				}
				if n := wroteHeaders.Load(); n != 1 {
					t.Fatalf("WroteHeaders hooks = %d, want 1", n)
				}
				if n := wroteRequest.Load(); n != 1 {
					t.Fatalf("WroteRequest hooks = %d, want 1", n)
				}
				if n := fake.HousekeepingWaiters(); n != 1 {
					t.Fatalf("HousekeepingWaiters() after response close = %d, want 1", n)
				}
			})
		}
	}
}
