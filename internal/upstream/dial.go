package upstream

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
)

// fallbackDelay and minAddrShare are net.Dialer's dual-stack fallback delay and least per-address window.
const (
	fallbackDelay = 300 * time.Millisecond
	minAddrShare  = 2 * time.Second
)

// ipResolver resolves a host name; *net.Resolver implements it.
type ipResolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

// addrShare is the window of the next of left addresses, as net.Dialer's partialDeadline computes it on clk; false means the address is bounded by ctx alone.
func addrShare(ctx context.Context, clk clock.Clock, left int) (time.Duration, bool) {
	deadline, ok := ctx.Deadline()
	if !ok || left <= 1 {
		return 0, false
	}
	remaining := deadline.Sub(clk.Now())
	if remaining <= 0 {
		return 0, false
	}
	share := remaining / time.Duration(left)
	if share < minAddrShare {
		share = min(remaining, minAddrShare)
	}
	if share >= remaining {
		return 0, false
	}
	return share, true
}

// dialSerial dials ips in order until one connects; each address but the last gets its addrShare as its own timer on clk. It returns the first error.
func dialSerial(ctx context.Context, dial DialFunc, clk clock.Clock, network, port string, ips []netip.Addr) (net.Conn, error) {
	var firstErr error
	for i, ip := range ips {
		if err := ctx.Err(); err != nil {
			if firstErr == nil {
				firstErr = &net.OpError{Op: "dial", Net: network, Err: err}
			}
			break
		}
		actx, cancel := ctx, context.CancelFunc(func() {})
		share, limited := addrShare(ctx, clk, len(ips)-i)
		if limited {
			actx, cancel = clock.WithTimeoutCause(ctx, clk, share, errConnectTimeout)
		}
		conn, err := dial(clock.WithoutDeadline(actx), network, net.JoinHostPort(ip.String(), port))
		cancel()
		if err == nil {
			return conn, nil
		}
		if conn != nil {
			_ = conn.Close()
		}
		if limited && context.Cause(actx) == errConnectTimeout {
			err = &net.OpError{Op: "dial", Net: network, Err: errConnectTimeout}
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return nil, firstErr
}

// dialAddrs resolves the host of addr and dials its addresses with dial, one IP:port at a time, as net.Dialer does but on clk: the first address's family is dialed serially; for tcp the other family starts fallbackDelay later or as soon as the first family has failed, and the first connection wins. Stdlib code sees no deadline. It returns the first family's error when every address fails.
func dialAddrs(ctx context.Context, res ipResolver, dial DialFunc, clk clock.Clock, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, &net.OpError{Op: "dial", Net: network, Err: err}
	}
	var ips []netip.Addr
	if ip, perr := netip.ParseAddr(host); perr == nil {
		ips = []netip.Addr{ip.Unmap()}
	} else {
		ias, err := res.LookupIPAddr(clock.WithoutDeadline(ctx), host)
		if err != nil {
			return nil, &net.OpError{Op: "dial", Net: network, Err: err}
		}
		for _, ia := range ias {
			a, ok := netip.AddrFromSlice(ia.IP)
			if !ok {
				continue
			}
			ips = append(ips, a.Unmap().WithZone(ia.Zone))
		}
	}
	filtered := ips[:0]
	for _, ip := range ips {
		switch {
		case network == "tcp4" && !ip.Is4():
		case network == "tcp6" && !ip.Is6():
		default:
			filtered = append(filtered, ip)
		}
	}
	ips = filtered
	if len(ips) == 0 {
		return nil, &net.OpError{Op: "dial", Net: network, Err: &net.AddrError{Err: "no suitable address found", Addr: host}}
	}
	var primaries, fallbacks []netip.Addr
	if network == "tcp" {
		for _, ip := range ips {
			if ip.Is4() == ips[0].Is4() {
				primaries = append(primaries, ip)
			} else {
				fallbacks = append(fallbacks, ip)
			}
		}
	} else {
		primaries = ips
	}
	if len(fallbacks) == 0 {
		return dialSerial(ctx, dial, clk, network, port, primaries)
	}

	type raced struct {
		conn    net.Conn
		err     error
		primary bool
	}
	results := make(chan raced, 2)
	race := func(ctx context.Context, ips []netip.Addr, primary bool) {
		conn, err := dialSerial(ctx, dial, clk, network, port, ips)
		results <- raced{conn, err, primary}
	}
	pctx, pcancel := context.WithCancel(ctx)
	defer pcancel()
	fctx, fcancel := context.WithCancel(ctx)
	defer fcancel()
	go race(pctx, primaries, true)
	fire := make(chan struct{})
	var fireOnce sync.Once
	timer := clk.AfterFunc(fallbackDelay, func() { fireOnce.Do(func() { close(fire) }) })
	defer timer.Stop()
	wait := (<-chan struct{})(fire)
	running, fallbackStarted := 1, false
	startFallback := func() {
		timer.Stop()
		wait, fallbackStarted = nil, true
		running++
		go race(fctx, fallbacks, false)
	}
	var primaryErr error
	for {
		select {
		case <-wait:
			startFallback()
		case r := <-results:
			running--
			if r.err == nil {
				pcancel()
				fcancel()
				go func(n int) {
					for ; n > 0; n-- {
						if o := <-results; o.conn != nil {
							_ = o.conn.Close()
						}
					}
				}(running)
				return r.conn, nil
			}
			if r.primary {
				primaryErr = r.err
				if !fallbackStarted {
					startFallback()
				}
			}
			if running == 0 {
				return nil, primaryErr
			}
		}
	}
}
