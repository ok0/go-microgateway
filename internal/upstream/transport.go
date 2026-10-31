package upstream

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
)

type connectTimeoutError struct{}

func (connectTimeoutError) Error() string   { return "connect timeout" }
func (connectTimeoutError) Timeout() bool   { return true }
func (connectTimeoutError) Temporary() bool { return true }

var errConnectTimeout error = connectTimeoutError{}

type handshakeTimeoutError struct{}

func (handshakeTimeoutError) Error() string   { return "TLS handshake timeout" }
func (handshakeTimeoutError) Timeout() bool   { return true }
func (handshakeTimeoutError) Temporary() bool { return true }

var errHandshakeTimeout error = handshakeTimeoutError{}

func newDialer() *net.Dialer {
	return &net.Dialer{KeepAlive: 30 * time.Second}
}

// connectTimeout bounds each dial by connect on clk, the only connect limit. With res, the host is resolved and its addresses are dialed one at a time by dialAddrs; otherwise base dials addr. Stdlib code sees cancellation but no deadline; a dial that fails after the limit ended it reports errConnectTimeout.
func connectTimeout(base DialFunc, res ipResolver, connect time.Duration, clk clock.Clock) DialFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dial := func(ctx context.Context) (net.Conn, error) {
			if res != nil {
				return dialAddrs(ctx, res, base, clk, network, addr)
			}
			return base(clock.WithoutDeadline(ctx), network, addr)
		}
		if connect <= 0 {
			return dial(ctx)
		}
		ctx, cancel := clock.WithTimeoutCause(ctx, clk, connect, errConnectTimeout)
		conn, err := dial(ctx)
		cancel()
		if err != nil && context.Cause(ctx) == errConnectTimeout {
			if conn != nil {
				_ = conn.Close()
			}
			return nil, &net.OpError{Op: "dial", Net: network, Err: errConnectTimeout}
		}
		return conn, err
	}
}

// Transport is the RoundTripper of an upstream: an *http.Transport whose connect, TLS handshake and idle limits run on the injected clock; the inner transport owns no wall-clock limit.
type Transport struct {
	inner      *http.Transport
	clk        clock.Clock   // connect and TLS handshake limits
	idleClk    clock.Clock   // clock.Housekeeping(clk): per-connection idle expiry
	connect    DialFunc      // connectTimeout(...)
	handshake  time.Duration // cfg.Timeouts.TLSHandshake; <= 0 = no limit
	idle       time.Duration // cfg.Timeouts.Idle; <= 0 = no expiry
	tlsConfig  *tls.Config   // base client TLS settings; nil = zero config (system roots)
	nextProtos []string      // ALPN: {"h2","http/1.1"} with HTTP/2, else {"http/1.1"}
	onHandout  func()        // test seam: runs first in every connection handout; nil in production
	closing    context.Context
	stop       context.CancelFunc
	closed     atomic.Bool
	mu         sync.Mutex
	conns      map[*trackedConn]struct{}
}

var (
	_ http.RoundTripper = (*Transport)(nil)
	_ io.Closer         = (*Transport)(nil)
)

func NewTransport(cfg config.Upstream, dial DialFunc, clk clock.Clock) *Transport {
	return newTransport(cfg, dial, clk, nil)
}

func newTransport(cfg config.Upstream, dial DialFunc, clk clock.Clock, tlsConfig *tls.Config) *Transport {
	if clk == nil {
		clk = clock.Real()
	}
	base := dial
	var res ipResolver
	if base == nil {
		base, res = newDialer().DialContext, net.DefaultResolver
	}
	closing, stop := context.WithCancel(context.Background())
	t := &Transport{
		clk:       clk,
		idleClk:   clock.Housekeeping(clk),
		connect:   connectTimeout(base, res, cfg.Timeouts.Connect.Std(), clk),
		handshake: cfg.Timeouts.TLSHandshake.Std(),
		idle:      cfg.Timeouts.Idle.Std(),
		tlsConfig: tlsConfig,
		closing:   closing,
		stop:      stop,
		conns:     make(map[*trackedConn]struct{}),
	}
	t.nextProtos = []string{"http/1.1"}
	if cfg.HTTP2 {
		t.nextProtos = []string{"h2", "http/1.1"}
	}
	in := http.DefaultTransport.(*http.Transport).Clone()
	in.Proxy = nil
	in.DialContext = t.dialPlain
	in.DialTLSContext = t.dialTLS
	in.TLSHandshakeTimeout = 0
	in.IdleConnTimeout = 0
	in.ResponseHeaderTimeout = 0
	in.ExpectContinueTimeout = 0
	in.MaxIdleConnsPerHost = cfg.Pool.MaxIdlePerHost
	in.MaxConnsPerHost = cfg.Pool.MaxConnsPerHost
	in.MaxIdleConns = 0
	p := new(http.Protocols)
	p.SetHTTP1(true)
	if cfg.HTTP2 {
		p.SetHTTP2(true)
	}
	in.Protocols = p
	in.ForceAttemptHTTP2 = cfg.HTTP2
	t.inner = in
	return t
}

func (t *Transport) dialPlain(ctx context.Context, network, addr string) (net.Conn, error) {
	raw, err := t.connect(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	c := t.track(raw)
	c.startIdle()
	return c, nil
}

func (t *Transport) dialTLS(ctx context.Context, network, addr string) (net.Conn, error) {
	raw, err := t.connect(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	cfg := &tls.Config{}
	if t.tlsConfig != nil {
		cfg = t.tlsConfig.Clone()
	}
	if cfg.ServerName == "" {
		cfg.ServerName = host
	}
	cfg.NextProtos = slices.Clone(t.nextProtos)
	c := t.track(raw)
	tc := tls.Client(c, cfg)
	if err := t.handshakeTLS(ctx, tc); err != nil {
		_ = c.Close()
		return nil, err
	}
	c.startIdle()
	return tc, nil
}

func (t *Transport) handshakeTLS(ctx context.Context, tc *tls.Conn) error {
	hctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(t.closing, cancel)
	defer stop()
	if t.handshake > 0 {
		var cancelLimit context.CancelFunc
		hctx, cancelLimit = clock.WithTimeoutCause(hctx, t.clk, t.handshake, errHandshakeTimeout)
		defer cancelLimit()
	}
	err := tc.HandshakeContext(clock.WithoutDeadline(hctx))
	if err != nil && context.Cause(hctx) == errHandshakeTimeout {
		return errHandshakeTimeout
	}
	return err
}

func (t *Transport) track(raw net.Conn) *trackedConn {
	c := &trackedConn{Conn: raw, t: t}
	t.mu.Lock()
	t.conns[c] = struct{}{}
	t.mu.Unlock()
	return c
}

func (t *Transport) forget(c *trackedConn) {
	t.mu.Lock()
	delete(t.conns, c)
	t.mu.Unlock()
}

// RoundTrip sends req on a pooled or new connection. A request handed a connection that its idle limit (or CloseIdleConnections) has already ended writes nothing on it, does not read its body, and is sent again on another connection; this re-issue is internal and invisible to the caller. Request.GetBody is not used.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	outer := httptrace.ContextClientTrace(req.Context())
	var getConnSeen bool
	for {
		a := t.newAttempt(req, outer, &getConnSeen)
		resp, err := t.inner.RoundTrip(a.req)
		if err == nil {
			return a.response(resp), nil
		}
		a.releaseConn()
		if !a.stale.Load() {
			return nil, err
		}
		if a.body != nil { // RoundTripper contract: the body may be closed after RoundTrip returns; wait before reusing it
			select {
			case <-a.body.closed:
			case <-req.Context().Done():
			}
		}
		if req.Context().Err() != nil {
			if req.Body != nil {
				_ = req.Body.Close()
			}
			return nil, context.Cause(req.Context())
		}
	}
}

// CloseIdleConnections closes every connection no request is using and stops its idle timer.
func (t *Transport) CloseIdleConnections() {
	t.mu.Lock()
	conns := make([]*trackedConn, 0, len(t.conns))
	for c := range t.conns {
		conns = append(conns, c)
	}
	t.mu.Unlock()
	for _, c := range conns {
		c.expireIdle()
	}
	t.inner.CloseIdleConnections()
}

// Close ends the transport: in-flight TLS handshakes are cancelled, unused connections are closed now and in-use ones as soon as their request ends; no idle timer remains.
func (t *Transport) Close() error {
	t.closed.Store(true)
	t.stop()
	t.CloseIdleConnections()
	return nil
}
