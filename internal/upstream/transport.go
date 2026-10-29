package upstream

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
)

type connectTimeoutError struct{}

func (connectTimeoutError) Error() string   { return "connect timeout" }
func (connectTimeoutError) Timeout() bool   { return true }
func (connectTimeoutError) Temporary() bool { return true }

var errConnectTimeout error = connectTimeoutError{}

func newDialer(cfg config.Upstream) *net.Dialer {
	return &net.Dialer{
		Timeout:   cfg.Timeouts.Connect.Std(),
		KeepAlive: 30 * time.Second,
	}
}

func connectTimeout(base DialFunc, connect time.Duration, clk clock.Clock) DialFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if connect <= 0 {
			return base(ctx, network, addr)
		}
		ctx, cancel := clock.WithTimeoutCause(ctx, clk, connect, errConnectTimeout)
		defer cancel()
		dialCtx := ctx
		if !clock.IsReal(clk) {
			dialCtx = clock.WithoutDeadline(ctx)
		}
		conn, err := base(dialCtx, network, addr)
		if err != nil && context.Cause(ctx) == errConnectTimeout {
			return nil, &net.OpError{Op: "dial", Net: network, Err: errConnectTimeout}
		}
		return conn, err
	}
}

func NewTransport(cfg config.Upstream, dial DialFunc, clk clock.Clock) *http.Transport {
	realClk := clock.IsReal(clk)
	if clk == nil {
		clk = clock.Real()
	}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil
	base := dial
	if base == nil {
		d := newDialer(cfg)
		if !realClk {
			d.Timeout = 0
		}
		base = d.DialContext
	}
	t.DialContext = connectTimeout(base, cfg.Timeouts.Connect.Std(), clk)
	t.TLSHandshakeTimeout = cfg.Timeouts.TLSHandshake.Std()
	if realClk {
		// With another clock, the connect and response-header limits run only on that clock
		// (connectTimeout, proxy.AttemptTransport); a wall-clock copy would race it.
		t.ResponseHeaderTimeout = cfg.Timeouts.ResponseHeader.Std()
	}
	t.IdleConnTimeout = cfg.Timeouts.Idle.Std()
	t.MaxIdleConnsPerHost = cfg.Pool.MaxIdlePerHost
	t.MaxConnsPerHost = cfg.Pool.MaxConnsPerHost
	t.MaxIdleConns = 0
	p := new(http.Protocols)
	p.SetHTTP1(true)
	if cfg.HTTP2 {
		p.SetHTTP2(true)
	}
	t.Protocols = p
	t.ForceAttemptHTTP2 = cfg.HTTP2
	return t
}
