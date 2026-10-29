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

func NewTransport(cfg config.Upstream, dial DialFunc, clk clock.Clock) *http.Transport {
	if clk == nil {
		clk = clock.Real()
	}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil
	base := dial
	var res ipResolver
	if base == nil {
		base, res = newDialer().DialContext, net.DefaultResolver
	}
	t.DialContext = connectTimeout(base, res, cfg.Timeouts.Connect.Std(), clk)
	t.TLSHandshakeTimeout = cfg.Timeouts.TLSHandshake.Std()
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
