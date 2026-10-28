// Package upstream holds backend hosts, selects a host per attempt, and builds upstream transports.
package upstream

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/httperr"
	"github.com/ok0/lychgate/internal/state"
)

var ErrNoHosts = httperr.ErrNoHealthyUpstream

const (
	KindTransport = "upstream.transport"
	KindHost      = "upstream.host"
	KindCursor    = "upstream.cursor"
)

// DialFunc opens a connection to addr; it has the signature of net.Dialer.DialContext.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// Deps are the dependencies of Build. State is required.
type Deps struct {
	State *state.Tx
	Dial  DialFunc // Dial opens backend connections; nil dials TCP.
}

type hostState struct{ inflight atomic.Int64 }

type cursor struct{ n atomic.Uint64 }

type pooledTransport struct{ *http.Transport }

func (t *pooledTransport) Close() error {
	t.CloseIdleConnections()
	return nil
}

type Host struct {
	URL    *url.URL
	Weight int

	state *hostState
}

func (h *Host) InFlight() int64 {
	return h.state.inflight.Load()
}

func (h *Host) Begin() {
	h.state.inflight.Add(1)
}

func (h *Host) Done() {
	h.state.inflight.Add(-1)
}

type Upstream struct {
	Name      string
	Hosts     []*Host
	Transport *http.Transport

	eligible []*Host
	cursor   *cursor
}

func TransportFingerprint(cfg config.Upstream) string {
	b, err := json.Marshal(struct {
		Timeouts config.Timeouts `json:"timeouts"`
		Pool     config.Pool     `json:"pool"`
		HTTP2    bool            `json:"http2"`
	}{cfg.Timeouts, cfg.Pool, cfg.HTTP2})
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func Build(cfg config.Upstream, d Deps) (*Upstream, error) {
	if cfg.DNS != nil {
		return nil, fmt.Errorf("upstream %q: dns discovery is not yet supported", cfg.Name)
	}
	urls := make([]*url.URL, len(cfg.Hosts))
	for i, hc := range cfg.Hosts {
		u, err := url.Parse(hc.URL)
		if err != nil {
			return nil, fmt.Errorf("upstream %q: host %d: %w", cfg.Name, i, err)
		}
		urls[i] = u
	}
	tr, err := state.Get(d.State, KindTransport, cfg.Name, TransportFingerprint(cfg), func() (*pooledTransport, error) {
		return &pooledTransport{NewTransport(cfg, d.Dial)}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("upstream %q: %w", cfg.Name, err)
	}
	cur, err := state.Get(d.State, KindCursor, cfg.Name, "", func() (*cursor, error) {
		return &cursor{}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("upstream %q: %w", cfg.Name, err)
	}
	hosts := make([]*Host, 0, len(cfg.Hosts))
	var eligible []*Host
	for i, hc := range cfg.Hosts {
		hs, err := state.Get(d.State, KindHost, cfg.Name+" "+urls[i].String(), "", func() (*hostState, error) {
			return &hostState{}, nil
		})
		if err != nil {
			return nil, fmt.Errorf("upstream %q: %w", cfg.Name, err)
		}
		weight := 1
		if hc.Weight != nil {
			weight = *hc.Weight
		}
		h := &Host{URL: urls[i], Weight: weight, state: hs}
		hosts = append(hosts, h)
		if weight > 0 {
			eligible = append(eligible, h)
		}
	}
	return &Upstream{
		Name:      cfg.Name,
		Hosts:     hosts,
		Transport: tr.Transport,
		eligible:  eligible,
		cursor:    cur,
	}, nil
}

func (u *Upstream) Acquire(r *http.Request, tried map[*Host]bool) (*Host, error) {
	if len(u.eligible) == 0 {
		return nil, ErrNoHosts
	}
	n := u.cursor.n.Add(1) - 1
	size := uint64(len(u.eligible))
	for i := uint64(0); i < size; i++ {
		h := u.eligible[(n+i)%size]
		if !tried[h] {
			return h, nil
		}
	}
	return u.eligible[n%size], nil
}

func newDialer(cfg config.Upstream) *net.Dialer {
	return &net.Dialer{
		Timeout:   cfg.Timeouts.Connect.Std(),
		KeepAlive: 30 * time.Second,
	}
}

func NewTransport(cfg config.Upstream, dial DialFunc) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil
	if dial == nil {
		t.DialContext = newDialer(cfg).DialContext
	} else {
		t.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			if connect := cfg.Timeouts.Connect.Std(); connect > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, connect)
				defer cancel()
			}
			return dial(ctx, network, addr)
		}
	}
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
