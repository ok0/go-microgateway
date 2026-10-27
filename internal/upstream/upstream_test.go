package upstream

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/httperr"
)

func w(n int) *int { return &n }

func TestRoundRobin(t *testing.T) {
	u, err := Build(config.Upstream{Name: "u", Hosts: []config.Host{
		{URL: "http://a:1", Weight: w(1)},
		{URL: "http://b:1", Weight: w(2)},
		{URL: "http://c:1", Weight: w(0)},
	}})
	if err != nil {
		t.Fatal(err)
	}
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
	u, err := Build(config.Upstream{Name: "u", Hosts: []config.Host{
		{URL: "http://a:1"},
		{URL: "http://b:1", Weight: w(1)},
		{URL: "http://c:1", Weight: w(5)},
	}})
	if err != nil {
		t.Fatal(err)
	}
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
	u, err := Build(config.Upstream{Name: "u", Hosts: []config.Host{
		{URL: "http://a:1"},
		{URL: "http://b:1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
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
	u, err := Build(config.Upstream{Name: "u", Hosts: []config.Host{
		{URL: "http://a:1", Weight: w(0)},
		{URL: "http://b:1", Weight: w(0)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = u.Acquire(nil, nil)
	if !errors.Is(err, ErrNoHosts) {
		t.Errorf("err = %v, want ErrNoHosts", err)
	}
	if !errors.Is(err, httperr.ErrNoHealthyUpstream) {
		t.Errorf("err = %v, want httperr.ErrNoHealthyUpstream", err)
	}
}

func TestBuildDNSUnsupported(t *testing.T) {
	_, err := Build(config.Upstream{Name: "d", DNS: &config.DNS{Hostname: "x", Port: 80}})
	if err == nil || !strings.Contains(err.Error(), "not yet supported") {
		t.Errorf("err = %v, want not yet supported", err)
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
	tr := NewTransport(cfg)
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
	if !NewTransport(cfg).Protocols.HTTP2() {
		t.Error("HTTP2 disabled, want enabled")
	}
}
