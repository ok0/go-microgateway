// Package upstream holds backend hosts, selects a host per attempt, and builds upstream transports.
package upstream

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/httperr"
)

var ErrNoHosts = httperr.ErrNoHealthyUpstream

type Host struct {
	URL    *url.URL
	Weight int

	inflight atomic.Int64
}

func (h *Host) InFlight() int64 {
	return h.inflight.Load()
}

func (h *Host) Begin() {
	h.inflight.Add(1)
}

func (h *Host) Done() {
	h.inflight.Add(-1)
}

type Upstream struct {
	Name      string
	Hosts     []*Host
	Transport *http.Transport

	eligible []*Host
	next     atomic.Uint64
}

func Build(cfg config.Upstream) (*Upstream, error) {
	if cfg.DNS != nil {
		return nil, fmt.Errorf("upstream %q: dns discovery is not yet supported", cfg.Name)
	}
	hosts := make([]*Host, 0, len(cfg.Hosts))
	var eligible []*Host
	for i, hc := range cfg.Hosts {
		u, err := url.Parse(hc.URL)
		if err != nil {
			return nil, fmt.Errorf("upstream %q: host %d: %w", cfg.Name, i, err)
		}
		weight := 1
		if hc.Weight != nil {
			weight = *hc.Weight
		}
		h := &Host{URL: u, Weight: weight}
		hosts = append(hosts, h)
		if weight > 0 {
			eligible = append(eligible, h)
		}
	}
	return &Upstream{
		Name:      cfg.Name,
		Hosts:     hosts,
		Transport: NewTransport(cfg),
		eligible:  eligible,
	}, nil
}

func (u *Upstream) Acquire(r *http.Request, tried map[*Host]bool) (*Host, error) {
	if len(u.eligible) == 0 {
		return nil, ErrNoHosts
	}
	n := u.next.Add(1) - 1
	size := uint64(len(u.eligible))
	for i := uint64(0); i < size; i++ {
		h := u.eligible[(n+i)%size]
		if !tried[h] {
			return h, nil
		}
	}
	return u.eligible[n%size], nil
}

func (u *Upstream) Close() error {
	u.Transport.CloseIdleConnections()
	return nil
}

func newDialer(cfg config.Upstream) *net.Dialer {
	return &net.Dialer{
		Timeout:   cfg.Timeouts.Connect.Std(),
		KeepAlive: 30 * time.Second,
	}
}

func NewTransport(cfg config.Upstream) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil
	t.DialContext = newDialer(cfg).DialContext
	t.TLSHandshakeTimeout = cfg.Timeouts.TLSHandshake.Std()
	t.ResponseHeaderTimeout = cfg.Timeouts.ResponseHeader.Std()
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
