package upstream

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"sync"
	"sync/atomic"

	"github.com/ok0/go-microgateway/internal/clock"
)

var errIdleExpired = errors.New("upstream: write on an expired idle connection")
var errStaleHandout = errors.New("upstream: request body read refused on an ended connection")

// trackedConn is a dialed connection with its in-use count and idle timer; it lies beneath TLS.
type trackedConn struct {
	net.Conn
	t        *Transport
	expired  atomic.Bool // set under mu; once set, Write sends nothing
	mu       sync.Mutex
	inFlight int
	closed   bool
	gen      uint64
	timer    clock.Timer
}

func (c *trackedConn) Write(b []byte) (int, error) {
	if c.expired.Load() {
		return 0, errIdleExpired
	}
	return c.Conn.Write(b)
}

func (c *trackedConn) Close() error {
	c.mu.Lock()
	first := !c.closed
	c.closed = true
	c.stopLocked()
	c.mu.Unlock()
	if first {
		c.t.forget(c)
	}
	return c.Conn.Close()
}

func (c *trackedConn) acquire() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.expired.Load() {
		return false
	}
	c.inFlight++
	c.stopLocked()
	return true
}

func (c *trackedConn) release() {
	c.mu.Lock()
	c.inFlight--
	var ok bool
	if c.inFlight == 0 {
		if c.t.closed.Load() {
			ok = c.expireLocked()
		} else {
			c.armLocked()
		}
	}
	c.mu.Unlock()
	if ok {
		c.finishExpire()
	}
}

func (c *trackedConn) startIdle() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.t.closed.Load() {
		c.armLocked()
	}
}

func (c *trackedConn) armLocked() {
	if c.inFlight > 0 || c.closed || c.expired.Load() || c.t.idle <= 0 {
		return
	}
	c.stopLocked()
	g := c.gen
	c.timer = c.t.idleClk.AfterFunc(c.t.idle, func() { c.fire(g) })
}

func (c *trackedConn) stopLocked() {
	c.gen++
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
}

func (c *trackedConn) expireLocked() bool {
	if c.inFlight > 0 || c.closed || c.expired.Load() {
		return false
	}
	c.expired.Store(true)
	c.stopLocked()
	return true
}

func (c *trackedConn) finishExpire() {
	c.t.forget(c)
	_ = c.Conn.Close()
}

func (c *trackedConn) fire(g uint64) {
	c.mu.Lock()
	ok := g == c.gen && c.expireLocked()
	c.mu.Unlock()
	if ok {
		c.finishExpire()
	}
}

func (c *trackedConn) expireIdle() {
	c.mu.Lock()
	ok := c.expireLocked()
	c.mu.Unlock()
	if ok {
		c.finishExpire()
	}
}

func trackedOf(conn net.Conn) *trackedConn {
	if tc, ok := conn.(*tls.Conn); ok {
		conn = tc.NetConn()
	}
	c, _ := conn.(*trackedConn)
	return c
}

type attempt struct {
	t           *Transport
	outer       *httptrace.ClientTrace // caller's trace or nil
	getConnSeen *bool                  // shared by the attempts of one RoundTrip; touched only on the RoundTrip goroutine
	stale       atomic.Bool            // last handout was an ended connection
	mu          sync.Mutex
	conn        *trackedConn // live connection acquired by this attempt
	body        *gateBody
	req         *http.Request
}

func (t *Transport) newAttempt(req *http.Request, outer *httptrace.ClientTrace, seen *bool) *attempt {
	a := &attempt{t: t, outer: outer, getConnSeen: seen}
	tr := a.trace()
	var ctx context.Context
	if outer == nil {
		ctx = httptrace.WithClientTrace(req.Context(), tr)
	} else {
		ctx = traceCtx{Context: req.Context(), trace: tr}
	}
	r := req.WithContext(ctx)
	r.GetBody = nil
	if req.Body != nil && req.Body != http.NoBody {
		a.body = &gateBody{rc: req.Body, a: a, closed: make(chan struct{})}
		r.Body = a.body
	}
	a.req = r
	return a
}

// traceCtx hides the caller's trace from the stdlib, so hooks of a stale handout never reach it.
type traceCtx struct {
	context.Context
	trace *httptrace.ClientTrace
}

func (c traceCtx) Value(key any) any {
	v := c.Context.Value(key)
	if _, ok := v.(*httptrace.ClientTrace); ok {
		return c.trace
	}
	return v
}

func (a *attempt) trace() *httptrace.ClientTrace {
	tr := &httptrace.ClientTrace{GotConn: a.gotConn}
	o := a.outer
	if o == nil {
		return tr
	}
	live := func() bool { return !a.stale.Load() }
	if o.GetConn != nil {
		tr.GetConn = func(hostPort string) {
			if !*a.getConnSeen {
				*a.getConnSeen = true
				o.GetConn(hostPort)
			}
		}
	}
	if o.TLSHandshakeStart != nil {
		tr.TLSHandshakeStart = o.TLSHandshakeStart
	}
	if o.TLSHandshakeDone != nil {
		tr.TLSHandshakeDone = o.TLSHandshakeDone
	}
	if o.PutIdleConn != nil {
		tr.PutIdleConn = func(err error) {
			if live() {
				o.PutIdleConn(err)
			}
		}
	}
	if o.GotFirstResponseByte != nil {
		tr.GotFirstResponseByte = func() {
			if live() {
				o.GotFirstResponseByte()
			}
		}
	}
	if o.Got100Continue != nil {
		tr.Got100Continue = func() {
			if live() {
				o.Got100Continue()
			}
		}
	}
	if o.Got1xxResponse != nil {
		tr.Got1xxResponse = func(code int, header textproto.MIMEHeader) error {
			if !live() {
				return nil
			}
			return o.Got1xxResponse(code, header)
		}
	}
	if o.WroteHeaderField != nil {
		tr.WroteHeaderField = func(key string, value []string) {
			if live() {
				o.WroteHeaderField(key, value)
			}
		}
	}
	if o.WroteHeaders != nil {
		tr.WroteHeaders = func() {
			if live() {
				o.WroteHeaders()
			}
		}
	}
	if o.Wait100Continue != nil {
		tr.Wait100Continue = func() {
			if live() {
				o.Wait100Continue()
			}
		}
	}
	if o.WroteRequest != nil {
		tr.WroteRequest = func(info httptrace.WroteRequestInfo) {
			if live() {
				o.WroteRequest(info)
			}
		}
	}
	return tr
}

func (a *attempt) gotConn(info httptrace.GotConnInfo) {
	if a.t.onHandout != nil {
		a.t.onHandout()
	}
	a.releaseConn()
	c := trackedOf(info.Conn)
	live := c == nil || c.acquire()
	if live && c != nil {
		a.mu.Lock()
		a.conn = c
		a.mu.Unlock()
	}
	a.stale.Store(!live)
	if live && a.outer != nil && a.outer.GotConn != nil {
		a.outer.GotConn(info)
	}
}

func (a *attempt) releaseConn() {
	a.mu.Lock()
	c := a.conn
	a.conn = nil
	a.mu.Unlock()
	if c != nil {
		c.release()
	}
}

func (a *attempt) response(resp *http.Response) *http.Response {
	a.mu.Lock()
	c := a.conn
	a.conn = nil
	a.mu.Unlock()
	if c == nil {
		return resp
	}
	if resp.Body == nil || resp.Body == http.NoBody {
		c.release()
		return resp
	}
	rel := &releaser{c: c}
	if rwc, ok := resp.Body.(io.ReadWriteCloser); ok && resp.StatusCode == http.StatusSwitchingProtocols {
		resp.Body = &releaseRWC{rwc, rel}
	} else {
		resp.Body = &releaseBody{resp.Body, rel}
	}
	return resp
}

type releaser struct {
	c    *trackedConn
	once sync.Once
}

func (r *releaser) done() {
	r.once.Do(r.c.release)
}

type releaseBody struct {
	io.ReadCloser
	rel *releaser
}

func (b *releaseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.rel.done()
	}
	return n, err
}

func (b *releaseBody) Close() error {
	err := b.ReadCloser.Close()
	b.rel.done()
	return err
}

type releaseRWC struct {
	io.ReadWriteCloser
	rel *releaser
}

func (b *releaseRWC) Close() error {
	err := b.ReadWriteCloser.Close()
	b.rel.done()
	return err
}

type gateBody struct {
	rc     io.ReadCloser
	a      *attempt
	once   sync.Once
	closed chan struct{}
}

func (b *gateBody) Read(p []byte) (int, error) {
	if b.a.stale.Load() {
		return 0, errStaleHandout
	}
	return b.rc.Read(p)
}

// Close forwards only for a live handout; a stale attempt leaves the caller's body open for the re-issue.
func (b *gateBody) Close() error {
	var err error
	b.once.Do(func() {
		if !b.a.stale.Load() {
			err = b.rc.Close()
		}
		close(b.closed)
	})
	return err
}
