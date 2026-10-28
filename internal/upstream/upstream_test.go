package upstream

import (
	"context"
	"errors"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/httperr"
	"github.com/ok0/lychgate/internal/state"
)

func w(n int) *int { return &n }

func buildTx(t *testing.T, reg *state.Registry, cfg config.Upstream) (*Upstream, *state.Lease) {
	t.Helper()
	tx := reg.Begin()
	u, err := Build(cfg, Deps{State: tx})
	if err != nil {
		tx.Abort()
		t.Fatal(err)
	}
	return u, tx.Commit()
}

func build(t *testing.T, cfg config.Upstream) *Upstream {
	t.Helper()
	reg := state.NewRegistry(nil)
	u, lease := buildTx(t, reg, cfg)
	t.Cleanup(func() {
		reg.Close()
		_ = lease.Release()
	})
	return u
}

func TestRoundRobin(t *testing.T) {
	u := build(t, config.Upstream{Name: "u", Hosts: []config.Host{
		{URL: "http://a:1", Weight: w(1)},
		{URL: "http://b:1", Weight: w(2)},
		{URL: "http://c:1", Weight: w(0)},
	}})
	want := []string{"a:1", "b:1", "a:1", "b:1", "a:1", "b:1"}
	for i, wantHost := range want {
		h, err := u.Acquire(nil, nil)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if h.URL.Host != wantHost {
			t.Errorf("call %d: host = %s, want %s", i, h.URL.Host, wantHost)
		}
	}
}

func TestRoundRobinEven(t *testing.T) {
	u := build(t, config.Upstream{Name: "u", Hosts: []config.Host{
		{URL: "http://a:1"},
		{URL: "http://b:1", Weight: w(1)},
		{URL: "http://c:1", Weight: w(5)},
	}})
	counts := make(map[*Host]int)
	for i := 0; i < 300; i++ {
		h, err := u.Acquire(nil, nil)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		counts[h]++
	}
	for _, h := range u.Hosts {
		if counts[h] != 100 {
			t.Errorf("host %s: count = %d, want 100", h.URL.Host, counts[h])
		}
	}
}

func TestAcquireSkipsTried(t *testing.T) {
	u := build(t, config.Upstream{Name: "u", Hosts: []config.Host{
		{URL: "http://a:1"},
		{URL: "http://b:1"},
	}})
	a, b := u.Hosts[0], u.Hosts[1]

	h, err := u.Acquire(nil, map[*Host]bool{a: true})
	if err != nil {
		t.Fatal(err)
	}
	if h != b {
		t.Errorf("host = %s, want b:1", h.URL.Host)
	}

	h, err = u.Acquire(nil, map[*Host]bool{a: true, b: true})
	if err != nil {
		t.Fatalf("all tried: unexpected error %v", err)
	}
	if h == nil {
		t.Fatal("all tried: host is nil")
	}
}

func TestNoEligibleHosts(t *testing.T) {
	u := build(t, config.Upstream{Name: "u", Hosts: []config.Host{
		{URL: "http://a:1", Weight: w(0)},
		{URL: "http://b:1", Weight: w(0)},
	}})
	_, err := u.Acquire(nil, nil)
	if !errors.Is(err, ErrNoHosts) {
		t.Errorf("err = %v, want ErrNoHosts", err)
	}
	if !errors.Is(err, httperr.ErrNoHealthyUpstream) {
		t.Errorf("err = %v, want httperr.ErrNoHealthyUpstream", err)
	}
}

func TestSnapshot(t *testing.T) {
	u := build(t, config.Upstream{Name: "u", Hosts: []config.Host{
		{URL: "http://a:1", Weight: w(1)},
		{URL: "http://b:1", Weight: w(3)},
	}})
	u.Hosts[1].Begin()

	want := []HostStatus{
		{URL: "http://a:1", Weight: 1, Healthy: true, Ejected: false, BreakerState: "closed", InFlight: 0},
		{URL: "http://b:1", Weight: 3, Healthy: true, Ejected: false, BreakerState: "closed", InFlight: 1},
	}
	got := u.Snapshot()
	if got == nil {
		t.Fatal("Snapshot returned nil slice")
	}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("host %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestBuildDNSUnsupported(t *testing.T) {
	reg := state.NewRegistry(nil)
	tx := reg.Begin()
	_, err := Build(config.Upstream{Name: "d", DNS: &config.DNS{Hostname: "x", Port: 80}}, Deps{State: tx})
	tx.Abort()
	if err == nil || !strings.Contains(err.Error(), "not yet supported") {
		t.Errorf("err = %v, want not yet supported", err)
	}
	if n := reg.Len(); n != 0 {
		t.Errorf("registry Len = %d, want 0", n)
	}
}

func TestNewTransport(t *testing.T) {
	cfg := config.Upstream{
		Name: "u",
		Timeouts: config.Timeouts{
			Connect:        config.Duration(time.Second),
			TLSHandshake:   config.Duration(2 * time.Second),
			ResponseHeader: config.Duration(3 * time.Second),
			Idle:           config.Duration(4 * time.Second),
		},
		Pool: config.Pool{MaxIdlePerHost: 7, MaxConnsPerHost: 9},
	}
	tr := NewTransport(cfg, nil)
	if tr.TLSHandshakeTimeout != 2*time.Second {
		t.Errorf("TLSHandshakeTimeout = %v", tr.TLSHandshakeTimeout)
	}
	if tr.ResponseHeaderTimeout != 3*time.Second {
		t.Errorf("ResponseHeaderTimeout = %v", tr.ResponseHeaderTimeout)
	}
	if tr.IdleConnTimeout != 4*time.Second {
		t.Errorf("IdleConnTimeout = %v", tr.IdleConnTimeout)
	}
	if tr.MaxIdleConnsPerHost != 7 {
		t.Errorf("MaxIdleConnsPerHost = %d", tr.MaxIdleConnsPerHost)
	}
	if tr.MaxConnsPerHost != 9 {
		t.Errorf("MaxConnsPerHost = %d", tr.MaxConnsPerHost)
	}
	if d := newDialer(cfg).Timeout; d != time.Second {
		t.Errorf("dialer timeout = %v", d)
	}
	if tr.Protocols.HTTP2() {
		t.Error("HTTP2 enabled, want disabled")
	}

	cfg.HTTP2 = true
	if !NewTransport(cfg, nil).Protocols.HTTP2() {
		t.Error("HTTP2 disabled, want enabled")
	}
}

func TestBuildUsesDial(t *testing.T) {
	errDial := errors.New("dial seam")
	var mu sync.Mutex
	var gotAddr string
	var gotDeadline bool
	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		mu.Lock()
		defer mu.Unlock()
		gotAddr = addr
		_, gotDeadline = ctx.Deadline()
		return nil, errDial
	}

	reg := state.NewRegistry(nil)
	cfg := config.Upstream{
		Name:     "u",
		Hosts:    []config.Host{{URL: "http://backend.test:8080"}},
		Timeouts: config.Timeouts{Connect: config.Duration(3 * time.Second)},
	}
	tx := reg.Begin()
	u, err := Build(cfg, Deps{State: tx, Dial: dial})
	if err != nil {
		tx.Abort()
		t.Fatal(err)
	}
	lease := tx.Commit()
	t.Cleanup(func() {
		reg.Close()
		_ = lease.Release()
	})

	_, err = u.Transport.RoundTrip(httptest.NewRequest("GET", "http://backend.test:8080/", nil))
	if !errors.Is(err, errDial) {
		t.Fatalf("err = %v, want errDial", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotAddr != "backend.test:8080" {
		t.Errorf("dialed addr = %q, want backend.test:8080", gotAddr)
	}
	if !gotDeadline {
		t.Error("dial context has no deadline, want connect timeout")
	}
}

func carryBase() config.Upstream {
	return config.Upstream{
		Name: "u",
		Hosts: []config.Host{
			{URL: "http://h1:1", Weight: w(1)},
			{URL: "http://h2:1", Weight: w(1)},
		},
		Timeouts: config.Timeouts{
			Connect:        config.Duration(time.Second),
			TLSHandshake:   config.Duration(2 * time.Second),
			ResponseHeader: config.Duration(3 * time.Second),
			Idle:           config.Duration(4 * time.Second),
		},
		Pool: config.Pool{MaxIdlePerHost: 7, MaxConnsPerHost: 9},
	}
}

func TestCarryOverUnrelatedChange(t *testing.T) {
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)

	u1, l1 := buildTx(t, reg, carryBase())
	t.Cleanup(func() { _ = l1.Release() })
	u1.Hosts[0].Begin()

	changed := carryBase()
	changed.Hosts[1].Weight = w(2)
	u2, l2 := buildTx(t, reg, changed)
	t.Cleanup(func() { _ = l2.Release() })

	if u2.Transport != u1.Transport {
		t.Error("transport was recreated on an unrelated change")
	}
	if got := u2.Hosts[0].InFlight(); got != 1 {
		t.Errorf("in-flight = %d, want 1", got)
	}
	u2.Hosts[0].Done()
	if got := u1.Hosts[0].InFlight(); got != 0 {
		t.Errorf("old in-flight = %d, want 0", got)
	}
}

func TestTransportRecreatedOnTimeoutChange(t *testing.T) {
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)

	u1, l1 := buildTx(t, reg, carryBase())
	t.Cleanup(func() { _ = l1.Release() })
	u1.Hosts[0].Begin()

	changed := carryBase()
	changed.Timeouts.Connect = config.Duration(7 * time.Second)
	u2, l2 := buildTx(t, reg, changed)
	t.Cleanup(func() { _ = l2.Release() })

	if u2.Transport == u1.Transport {
		t.Error("transport was not recreated on a timeout change")
	}
	if got := u2.Hosts[0].InFlight(); got != 1 {
		t.Errorf("in-flight = %d, want 1 (shared host state)", got)
	}
	u1.Hosts[0].Done()
}

func TestRenameResets(t *testing.T) {
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)

	u1, l1 := buildTx(t, reg, carryBase())
	t.Cleanup(func() { _ = l1.Release() })
	u1.Hosts[0].Begin()

	renamed := carryBase()
	renamed.Name = "v"
	u2, l2 := buildTx(t, reg, renamed)
	t.Cleanup(func() { _ = l2.Release() })

	if u2.Transport == u1.Transport {
		t.Error("transport was not recreated on rename")
	}
	if got := u2.Hosts[0].InFlight(); got != 0 {
		t.Errorf("in-flight after rename = %d, want 0", got)
	}
	u1.Hosts[0].Done()
	if got := reg.Len(); got != 4 {
		t.Errorf("registry Len = %d, want 4 (transport, cursor and two hosts of v)", got)
	}
}

func TestHostRemovedReleased(t *testing.T) {
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)

	_, l1 := buildTx(t, reg, carryBase())
	t.Cleanup(func() { _ = l1.Release() })
	before := reg.Len()

	dropped := carryBase()
	dropped.Hosts = dropped.Hosts[:1]
	u2, l2 := buildTx(t, reg, dropped)
	t.Cleanup(func() { _ = l2.Release() })

	if len(u2.Hosts) != 1 || u2.Hosts[0].URL.Host != "h1:1" {
		t.Fatalf("hosts = %v, want only h1:1", u2.Hosts)
	}
	if got := reg.Len(); got != before-1 {
		t.Errorf("registry Len = %d, want %d", got, before-1)
	}
}

func TestTransportFingerprint(t *testing.T) {
	base := carryBase()
	fp := TransportFingerprint(base)

	unchanged := map[string]func(*config.Upstream){
		"weights": func(c *config.Upstream) { c.Hosts[0].Weight = w(9) },
		"load_balancer": func(c *config.Upstream) {
			c.LoadBalancer = config.LoadBalancer{Algorithm: "round_robin"}
		},
		"passive_health": func(c *config.Upstream) {
			c.PassiveHealth = &config.PassiveHealth{ConsecutiveFailures: 3}
		},
	}
	for name, mut := range unchanged {
		c := carryBase()
		mut(&c)
		if got := TransportFingerprint(c); got != fp {
			t.Errorf("%s changed the fingerprint", name)
		}
	}

	changed := map[string]func(*config.Upstream){
		"timeouts.connect":         func(c *config.Upstream) { c.Timeouts.Connect = config.Duration(9 * time.Second) },
		"timeouts.tls_handshake":   func(c *config.Upstream) { c.Timeouts.TLSHandshake = config.Duration(9 * time.Second) },
		"timeouts.response_header": func(c *config.Upstream) { c.Timeouts.ResponseHeader = config.Duration(9 * time.Second) },
		"timeouts.idle":            func(c *config.Upstream) { c.Timeouts.Idle = config.Duration(9 * time.Second) },
		"pool.max_idle_per_host":   func(c *config.Upstream) { c.Pool.MaxIdlePerHost = 99 },
		"pool.max_conns_per_host":  func(c *config.Upstream) { c.Pool.MaxConnsPerHost = 99 },
		"http2":                    func(c *config.Upstream) { c.HTTP2 = true },
	}
	for name, mut := range changed {
		c := carryBase()
		mut(&c)
		if got := TransportFingerprint(c); got == fp {
			t.Errorf("%s did not change the fingerprint", name)
		}
	}
}

func TestBuildBadURLCreatesNothing(t *testing.T) {
	reg := state.NewRegistry(nil)
	tx := reg.Begin()
	cfg := carryBase()
	cfg.Hosts[1].URL = "http://[::1"
	_, err := Build(cfg, Deps{State: tx})
	tx.Abort()
	if err == nil {
		t.Fatal("Build error = nil, want URL parse error")
	}
	if n := reg.Len(); n != 0 {
		t.Errorf("registry Len = %d, want 0", n)
	}
}
