package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/breaker"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/health"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/internal/state"
)

var testStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func w(n int) *int { return &n }

func intp(v int) *int { return &v }

func floatp(v float64) *float64 { return &v }

func dur(d time.Duration) config.Duration { return config.Duration(d) }

func hosts(urls ...string) []config.Host {
	out := make([]config.Host, 0, len(urls))
	for _, u := range urls {
		out = append(out, config.Host{URL: config.NewSecretURL(u)})
	}
	return out
}

type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func newTestMetrics(t *testing.T) *Metrics {
	t.Helper()
	m, err := newMetrics(metrics.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

func hostByURL(t *testing.T, u *Upstream, rawURL string) *Host {
	t.Helper()
	for _, h := range u.Hosts() {
		if h.URL.String() == rawURL {
			return h
		}
	}
	t.Fatalf("host %s not in upstream", rawURL)
	return nil
}

func hostURLs(u *Upstream) []string {
	out := make([]string, 0, len(u.Hosts()))
	for _, h := range u.Hosts() {
		out = append(out, h.URL.String())
	}
	return out
}

func gaugeValue(g *metrics.GaugeVec, labels ...string) float64 {
	return g.With(labels...).Value()
}

func buildTx(t *testing.T, reg *state.Registry, cfg config.Upstream) (*Upstream, *state.Lease) {
	t.Helper()
	return buildDeps(t, reg, cfg, Deps{})
}

func buildDeps(t *testing.T, reg *state.Registry, cfg config.Upstream, d Deps) (*Upstream, *state.Lease) {
	t.Helper()
	tx := reg.Begin()
	d.State = tx
	u, err := Build(cfg, d)
	if err != nil {
		tx.Abort()
		t.Fatal(err)
	}
	lease := tx.Commit()
	t.Cleanup(func() { _ = lease.Release() })
	u.Activate()
	return u, lease
}

func build(t *testing.T, cfg config.Upstream) *Upstream {
	t.Helper()
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	u, _ := buildTx(t, reg, cfg)
	return u
}

func pickCounts(t *testing.T, u *Upstream, r *http.Request, n int) map[*Host]int {
	t.Helper()
	counts := make(map[*Host]int)
	for i := 0; i < n; i++ {
		h, _, err := u.Acquire(r, nil, false)
		if err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
		counts[h]++
	}
	return counts
}

var allAlgorithms = []string{"", "round_robin", "weighted_round_robin", "random", "consistent_hash"}

func TestRoundRobin(t *testing.T) {
	u := build(t, config.Upstream{Name: "u", LoadBalancer: config.LoadBalancer{Algorithm: "round_robin"}, Hosts: []config.Host{
		{URL: config.NewSecretURL("http://a:1"), Weight: w(1)},
		{URL: config.NewSecretURL("http://b:1"), Weight: w(2)},
		{URL: config.NewSecretURL("http://c:1"), Weight: w(0)},
	}})
	want := []string{"a:1", "b:1", "a:1", "b:1", "a:1", "b:1"}
	for i, wantHost := range want {
		h, _, err := u.Acquire(nil, nil, false)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if h.URL.Host != wantHost {
			t.Errorf("call %d: host = %s, want %s", i, h.URL.Host, wantHost)
		}
	}
}

func TestRoundRobinEven(t *testing.T) {
	u := build(t, config.Upstream{Name: "u", LoadBalancer: config.LoadBalancer{Algorithm: "round_robin"}, Hosts: []config.Host{
		{URL: config.NewSecretURL("http://a:1")},
		{URL: config.NewSecretURL("http://b:1"), Weight: w(1)},
		{URL: config.NewSecretURL("http://c:1"), Weight: w(5)},
	}})
	counts := pickCounts(t, u, nil, 300)
	for _, h := range u.Hosts() {
		if counts[h] != 100 {
			t.Errorf("host %s: count = %d, want 100", h.URL.Host, counts[h])
		}
	}
}

func TestAcquireSkipsTried(t *testing.T) {
	u := build(t, config.Upstream{Name: "u", Hosts: []config.Host{
		{URL: config.NewSecretURL("http://a:1")},
		{URL: config.NewSecretURL("http://b:1")},
	}})
	a, b := u.Hosts()[0], u.Hosts()[1]

	h, _, err := u.Acquire(nil, map[*Host]bool{a: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	if h != b {
		t.Errorf("host = %s, want b:1", h.URL.Host)
	}

	h, _, err = u.Acquire(nil, map[*Host]bool{a: true, b: true}, false)
	if err != nil {
		t.Fatalf("all tried: unexpected error %v", err)
	}
	if h == nil {
		t.Fatal("all tried: host is nil")
	}
}

func TestNoEligibleHosts(t *testing.T) {
	u := build(t, config.Upstream{Name: "u", Hosts: []config.Host{
		{URL: config.NewSecretURL("http://a:1"), Weight: w(0)},
		{URL: config.NewSecretURL("http://b:1"), Weight: w(0)},
	}})
	_, _, err := u.Acquire(nil, nil, false)
	if !errors.Is(err, ErrNoHosts) {
		t.Errorf("err = %v, want ErrNoHosts", err)
	}
	if !errors.Is(err, httperr.ErrNoHealthyUpstream) {
		t.Errorf("err = %v, want httperr.ErrNoHealthyUpstream", err)
	}
}

func TestAllWeightZeroNoHosts(t *testing.T) {
	for _, algo := range allAlgorithms {
		t.Run("algorithm="+algo, func(t *testing.T) {
			u := build(t, config.Upstream{Name: "u", LoadBalancer: config.LoadBalancer{Algorithm: algo}, Hosts: []config.Host{
				{URL: config.NewSecretURL("http://a:1"), Weight: w(0)},
				{URL: config.NewSecretURL("http://b:1"), Weight: w(0)},
			}})
			for i := 0; i < 100; i++ {
				if _, _, err := u.Acquire(nil, nil, false); !errors.Is(err, ErrNoHosts) {
					t.Fatalf("pick %d: err = %v, want ErrNoHosts", i, err)
				}
			}
		})
	}
}

func TestWeightZeroNeverSelected(t *testing.T) {
	for _, algo := range allAlgorithms {
		for _, panicMode := range []bool{false, true} {
			name := fmt.Sprintf("algorithm=%s/panic=%v", algo, panicMode)
			t.Run(name, func(t *testing.T) {
				cfg := config.Upstream{
					Name:         "u",
					LoadBalancer: config.LoadBalancer{Algorithm: algo, Hash: config.Hash{Source: "header", Name: "X-K"}},
					Hosts: []config.Host{
						{URL: config.NewSecretURL("http://a:1"), Weight: w(1)},
						{URL: config.NewSecretURL("http://b:1"), Weight: w(0)},
						{URL: config.NewSecretURL("http://c:1"), Weight: w(3)},
					},
				}
				if panicMode {
					cfg.PassiveHealth = &config.PassiveHealth{ConsecutiveFailures: 1, MaxEjectionPercent: intp(100)}
				}
				u := build(t, cfg)
				if panicMode {
					u.Record(hostByURL(t, u, "http://a:1"), breaker.Token{}, health.Failure)
					u.Record(hostByURL(t, u, "http://c:1"), breaker.Token{}, health.Failure)
				}
				zero := hostByURL(t, u, "http://b:1")
				for i := 0; i < 1000; i++ {
					r := httptest.NewRequest(http.MethodGet, "/", nil)
					r.Header.Set("X-K", fmt.Sprintf("key-%d", i))
					h, _, err := u.Acquire(r, nil, false)
					if err != nil {
						t.Fatalf("pick %d: %v", i, err)
					}
					if h == zero {
						t.Fatalf("pick %d: weight-0 host selected", i)
					}
				}
			})
		}
	}
}

func TestPassiveEjectionAcquire(t *testing.T) {
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	u, _ := buildDeps(t, reg, config.Upstream{Name: "u", Hosts: hosts("http://a:1", "http://b:1"), PassiveHealth: &config.PassiveHealth{
		ConsecutiveFailures: 3,
		BaseEjectionTime:    dur(30 * time.Second),
		MaxEjectionTime:     dur(300 * time.Second),
	}}, Deps{Clock: fake})
	a := hostByURL(t, u, "http://a:1")
	for i := 0; i < 3; i++ {
		u.Record(a, breaker.Token{}, health.Failure)
	}
	if got := pickCounts(t, u, nil, 1000)[a]; got != 0 {
		t.Fatalf("ejected host picked %d times, want 0", got)
	}
	fake.Advance(30 * time.Second)
	if got := pickCounts(t, u, nil, 1000)[a]; got == 0 {
		t.Fatal("host not picked after ejection expired")
	}
}

func TestPanicMode(t *testing.T) {
	fake := clock.NewFake(testStart)
	m := newTestMetrics(t)
	var logs syncBuf
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	cfg := config.Upstream{
		Name: "u",
		Hosts: []config.Host{
			{URL: config.NewSecretURL("http://a:1"), Weight: w(1)},
			{URL: config.NewSecretURL("http://b:1"), Weight: w(1)},
			{URL: config.NewSecretURL("http://c:1"), Weight: w(0)},
		},
		PassiveHealth: &config.PassiveHealth{
			ConsecutiveFailures: 1,
			BaseEjectionTime:    dur(100 * time.Millisecond),
			MaxEjectionTime:     dur(100 * time.Millisecond),
			MaxEjectionPercent:  intp(100),
		},
	}
	u, _ := buildDeps(t, reg, cfg, Deps{
		Clock:   fake,
		Logger:  slog.New(slog.NewTextHandler(&logs, nil)),
		Metrics: m,
	})
	a, b, c := hostByURL(t, u, "http://a:1"), hostByURL(t, u, "http://b:1"), hostByURL(t, u, "http://c:1")
	eject := func() {
		u.Record(a, breaker.Token{}, health.Failure)
		u.Record(b, breaker.Token{}, health.Failure)
	}

	eject()
	counts := pickCounts(t, u, nil, 1000)
	if counts[c] != 0 || counts[a]+counts[b] != 1000 {
		t.Fatalf("panic picks = %v, want only weight>0 hosts", counts)
	}
	if !u.Status().PanicMode {
		t.Error("Status().PanicMode = false, want true")
	}
	if got := gaugeValue(m.PanicMode, "u"); got != 1 {
		t.Errorf("panic gauge = %v, want 1", got)
	}
	if !strings.Contains(logs.String(), "upstream entered panic mode") {
		t.Error("missing panic entry log")
	}

	fake.Advance(100 * time.Millisecond)
	pickCounts(t, u, nil, 1)
	if u.Status().PanicMode {
		t.Error("Status().PanicMode = true after expiry, want false")
	}
	if got := gaugeValue(m.PanicMode, "u"); got != 0 {
		t.Errorf("panic gauge after expiry = %v, want 0", got)
	}

	eject()
	pickCounts(t, u, nil, 1)
	fake.Advance(2 * time.Second)
	pickCounts(t, u, nil, 1)

	out := logs.String()
	if n := strings.Count(out, "upstream entered panic mode"); n != 1 {
		t.Errorf("entered panic logs = %d, want 1 (flapping within one second is rate-limited)", n)
	}
	if n := strings.Count(out, "upstream left panic mode"); n != 1 {
		t.Errorf("left panic logs = %d, want 1", n)
	}
	if !strings.Contains(out, "suppressed=2") {
		t.Errorf("log = %q, want suppressed=2 on the next logged transition", out)
	}
}

func TestExclusionFallback(t *testing.T) {
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	u, _ := buildTx(t, reg, config.Upstream{Name: "u", Hosts: hosts("http://a:1", "http://b:1", "http://c:1"), PassiveHealth: &config.PassiveHealth{
		ConsecutiveFailures: 1,
		MaxEjectionPercent:  intp(100),
	}})
	c := hostByURL(t, u, "http://c:1")
	u.Record(c, breaker.Token{}, health.Failure)
	a, b := hostByURL(t, u, "http://a:1"), hostByURL(t, u, "http://b:1")
	for i := 0; i < 100; i++ {
		h, _, err := u.Acquire(nil, map[*Host]bool{a: true, b: true}, false)
		if err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
		if h == c || h.Ejected() {
			t.Fatalf("pick %d: returned ejected host %s", i, h.URL.Host)
		}
	}
}

func TestHashKeySources(t *testing.T) {
	ip := netip.MustParseAddr("192.0.2.9")
	cases := []struct {
		name    string
		hash    config.Hash
		mutate  func(*http.Request)
		ctxIP   bool
		want    string
		wantKey bool
	}{
		{"header", config.Hash{Source: "header", Name: "X-Tenant"}, func(r *http.Request) { r.Header.Set("X-Tenant", "acme") }, true, "acme", true},
		{"cookie", config.Hash{Source: "cookie", Name: "sid"}, func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "sid", Value: "s1"}) }, true, "s1", true},
		{"path", config.Hash{Source: "path"}, nil, true, "/a/b", true},
		{"ip", config.Hash{Source: "ip"}, nil, true, "192.0.2.9", true},
		{"missing header uses client ip", config.Hash{Source: "header", Name: "X-Tenant"}, nil, true, "192.0.2.9", true},
		{"no client ip", config.Hash{Source: "header", Name: "X-Tenant"}, nil, false, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodGet, "http://gw.test/a/b", nil)
			if err != nil {
				t.Fatal(err)
			}
			r.RemoteAddr = ""
			if c.mutate != nil {
				c.mutate(r)
			}
			if c.ctxIP {
				r = r.WithContext(reqctx.WithClientIP(r.Context(), ip))
			}
			u := &Upstream{lbCfg: config.LoadBalancer{Hash: c.hash}}
			key, ok := u.hashKey(r)
			if ok != c.wantKey || key != c.want {
				t.Errorf("hashKey = (%q, %v), want (%q, %v)", key, ok, c.want, c.wantKey)
			}
		})
	}

	noIP := func() *http.Request {
		r, err := http.NewRequest(http.MethodGet, "http://gw.test/", nil)
		if err != nil {
			t.Fatal(err)
		}
		r.RemoteAddr = ""
		return r
	}
	seq := func() []int {
		reg := state.NewRegistry(nil)
		t.Cleanup(reg.Close)
		u, _ := buildDeps(t, reg, config.Upstream{Name: "u", LoadBalancer: config.LoadBalancer{
			Algorithm: "consistent_hash",
			Hash:      config.Hash{Source: "ip"},
		}, Hosts: hosts("http://a:1", "http://b:1", "http://c:1")}, Deps{Rand: clock.NewRand(7)})
		out := make([]int, 0, 50)
		for i := 0; i < 50; i++ {
			h, _, err := u.Acquire(noIP(), nil, false)
			if err != nil {
				t.Fatal(err)
			}
			for j, x := range u.Hosts() {
				if x == h {
					out = append(out, j)
				}
			}
		}
		return out
	}
	if first, second := seq(), seq(); !reflect.DeepEqual(first, second) {
		t.Errorf("no-key picks differ between equal seeds: %v vs %v", first, second)
	}
}

func TestStatus(t *testing.T) {
	cfg := config.Upstream{Name: "u", Hosts: []config.Host{
		{URL: config.NewSecretURL("http://a:1"), Weight: w(1)},
		{URL: config.NewSecretURL("http://b:1"), Weight: w(3)},
	}}
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	tx := reg.Begin()
	pre, err := Build(cfg, Deps{State: tx})
	if err != nil {
		tx.Abort()
		t.Fatal(err)
	}
	lease := tx.Commit()
	t.Cleanup(func() { _ = lease.Release() })
	if st := pre.Status(); st.Hosts == nil || len(st.Hosts) != 0 {
		t.Fatalf("Hosts before Activate = %v, want non-nil empty", st.Hosts)
	}

	pre.Activate()
	pre.Hosts()[1].Begin()
	want := []HostStatus{
		{URL: "http://a:1", Weight: 1, Healthy: true, BreakerState: "closed"},
		{URL: "http://b:1", Weight: 3, Healthy: true, BreakerState: "closed", InFlight: 1},
	}
	st := pre.Status()
	if st.Name != "u" || st.Algorithm != "p2c" || st.PanicMode {
		t.Errorf("status = %+v, want name u, algorithm p2c, panic false", st)
	}
	if len(st.Hosts) != len(want) {
		t.Fatalf("hosts = %d, want %d", len(st.Hosts), len(want))
	}
	for i := range want {
		if !reflect.DeepEqual(st.Hosts[i], want[i]) {
			t.Errorf("host %d: got %+v, want %+v", i, st.Hosts[i], want[i])
		}
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
	tr := NewTransport(cfg, nil, nil)
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
	if !NewTransport(cfg, nil, nil).Protocols.HTTP2() {
		t.Error("HTTP2 disabled, want enabled")
	}
}

func TestNewTransportInjectedClockHasNoWallLimits(t *testing.T) {
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
	tr := NewTransport(cfg, nil, clock.NewFake(testStart))
	if tr.ResponseHeaderTimeout != 0 {
		t.Errorf("ResponseHeaderTimeout = %v, want 0", tr.ResponseHeaderTimeout)
	}
	if tr.TLSHandshakeTimeout != 2*time.Second {
		t.Errorf("TLSHandshakeTimeout = %v", tr.TLSHandshakeTimeout)
	}
	if tr.IdleConnTimeout != 4*time.Second {
		t.Errorf("IdleConnTimeout = %v", tr.IdleConnTimeout)
	}
}

func TestConnectTimeoutFakeClock(t *testing.T) {
	fake := clock.NewFake(testStart)
	cfg := config.Upstream{
		Name:     "u",
		Timeouts: config.Timeouts{Connect: dur(time.Hour)},
	}
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	tr := NewTransport(cfg, dial, fake)
	req, err := http.NewRequest(http.MethodGet, "http://backend.test:8080/", nil)
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		resp, err := tr.RoundTrip(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		errCh <- err
	}()
	fake.BlockUntil(1)
	fake.Advance(time.Hour)
	err = <-errCh
	var oe *net.OpError
	if !errors.As(err, &oe) || oe.Op != "dial" {
		t.Fatalf("err = %v, want *net.OpError with Op dial", err)
	}
	if status, _ := httperr.FromError(err); status != 502 {
		t.Errorf("status = %d, want 502", status)
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
		Hosts:    []config.Host{{URL: config.NewSecretURL("http://backend.test:8080")}},
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
			{URL: config.NewSecretURL("http://h1:1"), Weight: w(1)},
			{URL: config.NewSecretURL("http://h2:1"), Weight: w(1)},
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
	u1.Hosts()[0].Begin()

	changed := carryBase()
	changed.Hosts[1].Weight = w(2)
	u2, l2 := buildTx(t, reg, changed)
	t.Cleanup(func() { _ = l2.Release() })

	if u2.Transport != u1.Transport {
		t.Error("transport was recreated on an unrelated change")
	}
	if got := u2.Hosts()[0].InFlight(); got != 1 {
		t.Errorf("in-flight = %d, want 1", got)
	}
	u2.Hosts()[0].Done()
	if got := u1.Hosts()[0].InFlight(); got != 0 {
		t.Errorf("old in-flight = %d, want 0", got)
	}
}

func TestTransportRecreatedOnTimeoutChange(t *testing.T) {
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)

	u1, l1 := buildTx(t, reg, carryBase())
	t.Cleanup(func() { _ = l1.Release() })
	u1.Hosts()[0].Begin()

	changed := carryBase()
	changed.Timeouts.Connect = config.Duration(7 * time.Second)
	u2, l2 := buildTx(t, reg, changed)
	t.Cleanup(func() { _ = l2.Release() })

	if u2.Transport == u1.Transport {
		t.Error("transport was not recreated on a timeout change")
	}
	if got := u2.Hosts()[0].InFlight(); got != 1 {
		t.Errorf("in-flight = %d, want 1 (shared host state)", got)
	}
	u1.Hosts()[0].Done()
}

func TestRenameResets(t *testing.T) {
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)

	u1, l1 := buildTx(t, reg, carryBase())
	t.Cleanup(func() { _ = l1.Release() })
	u1.Hosts()[0].Begin()

	renamed := carryBase()
	renamed.Name = "v"
	u2, l2 := buildTx(t, reg, renamed)
	t.Cleanup(func() { _ = l2.Release() })

	if u2.Transport == u1.Transport {
		t.Error("transport was not recreated on rename")
	}
	if got := u2.Hosts()[0].InFlight(); got != 0 {
		t.Errorf("in-flight after rename = %d, want 0", got)
	}
	u1.Hosts()[0].Done()
	if got := reg.Len(); got != 2 {
		t.Errorf("registry Len = %d, want 2 (transport and pool of v)", got)
	}
}

func TestHostRemovedReleased(t *testing.T) {
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)

	u1, l1 := buildTx(t, reg, carryBase())
	t.Cleanup(func() { _ = l1.Release() })
	u1.Hosts()[0].Begin()
	u1.Hosts()[1].Begin()

	dropped := carryBase()
	dropped.Hosts = dropped.Hosts[:1]
	u2, l2 := buildTx(t, reg, dropped)
	t.Cleanup(func() { _ = l2.Release() })

	if len(u2.Hosts()) != 1 || u2.Hosts()[0].URL.Host != "h1:1" {
		t.Fatalf("hosts = %v, want only h1:1", hostURLs(u2))
	}
	if got := u2.Hosts()[0].InFlight(); got != 1 {
		t.Errorf("h1 in-flight after drop = %d, want 1", got)
	}

	u3, l3 := buildTx(t, reg, carryBase())
	t.Cleanup(func() { _ = l3.Release() })
	if got := u3.Hosts()[1].InFlight(); got != 0 {
		t.Errorf("re-added h2 in-flight = %d, want 0", got)
	}
	if got := u3.Hosts()[0].InFlight(); got != 1 {
		t.Errorf("h1 in-flight = %d, want 1", got)
	}
	u1.Hosts()[1].Done()
	if got := u3.Hosts()[1].InFlight(); got != 0 {
		t.Errorf("re-added h2 in-flight after old Done = %d, want 0", got)
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

func TestBuildInvalidURLHidesCredentials(t *testing.T) {
	reg := state.NewRegistry(nil)
	tx := reg.Begin()
	_, err := Build(config.Upstream{Name: "u", Hosts: hosts("http://u:p%zz@h:1")}, Deps{State: tx})
	tx.Abort()
	if err == nil {
		t.Fatal("Build error = nil, want invalid URL error")
	}
	for _, leak := range []string{"p%zz", "%zz"} {
		if strings.Contains(err.Error(), leak) {
			t.Errorf("build error surfaces credentials: %v", err)
		}
	}
}

func TestBuildBadURLCreatesNothing(t *testing.T) {
	reg := state.NewRegistry(nil)
	tx := reg.Begin()
	cfg := carryBase()
	cfg.Hosts[1].URL = config.NewSecretURL("http://[::1")
	_, err := Build(cfg, Deps{State: tx})
	tx.Abort()
	if err == nil {
		t.Fatal("Build error = nil, want URL parse error")
	}
	if n := reg.Len(); n != 0 {
		t.Errorf("registry Len = %d, want 0", n)
	}
}

func healthUpstream(hs ...config.Host) config.Upstream {
	return config.Upstream{
		Name:  "u",
		Hosts: hs,
		PassiveHealth: &config.PassiveHealth{
			ConsecutiveFailures: 3,
			BaseEjectionTime:    dur(time.Hour),
			MaxEjectionTime:     dur(time.Hour),
			MaxEjectionPercent:  intp(100),
		},
		ActiveHealth: &config.ActiveHealth{
			Path:               "/health",
			Interval:           dur(10 * time.Second),
			Timeout:            dur(time.Hour),
			Jitter:             floatp(0),
			HealthyThreshold:   intp(1),
			UnhealthyThreshold: intp(1),
			ExpectedStatuses:   []string{"200-299"},
		},
	}
}

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }

// pipeNet serves HTTP over in-memory pipes so that transports built by Build can be used without sockets.
type pipeNet struct {
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func startPipeServer(t *testing.T, h http.Handler) *pipeNet {
	t.Helper()
	pn := &pipeNet{conns: make(chan net.Conn), done: make(chan struct{})}
	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(pn) }()
	t.Cleanup(func() { _ = srv.Close() })
	return pn
}

func (pn *pipeNet) dial(_ context.Context, _, _ string) (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case pn.conns <- server:
		return client, nil
	case <-pn.done:
		return nil, net.ErrClosed
	}
}

func (pn *pipeNet) Accept() (net.Conn, error) {
	select {
	case c := <-pn.conns:
		return c, nil
	case <-pn.done:
		return nil, net.ErrClosed
	}
}

func (pn *pipeNet) Close() error {
	pn.once.Do(func() { close(pn.done) })
	return nil
}

func (pn *pipeNet) Addr() net.Addr { return pipeAddr{} }

func probeHandler() http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Host == "c:1" {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		rw.WriteHeader(http.StatusOK)
	})
}

func probeSum(m *Metrics, hostLabels ...string) float64 {
	var total float64
	for _, h := range hostLabels {
		total += m.ActiveChecks.With("u", h, "pass").Value()
		total += m.ActiveChecks.With("u", h, "fail").Value()
	}
	return total
}

func TestCarryOverHealth(t *testing.T) {
	fake := clock.NewFake(testStart)
	m := newTestMetrics(t)
	pn := startPipeServer(t, probeHandler())
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	deps := Deps{Clock: fake, Rand: clock.NewRand(1), Metrics: m, Dial: pn.dial}

	u1, _ := buildDeps(t, reg, healthUpstream(hosts("http://a:1", "http://b:1", "http://c:1", "http://d:1", "http://f:1")...), deps)
	a := hostByURL(t, u1, "http://a:1")
	b := hostByURL(t, u1, "http://b:1")
	d := hostByURL(t, u1, "http://d:1")

	fake.BlockUntil(5)
	for i := 0; i < 3; i++ {
		u1.Record(a, breaker.Token{}, health.Failure)
	}
	for i := 0; i < 2; i++ {
		u1.Record(b, breaker.Token{}, health.Failure)
	}
	d.Begin()
	fake.Advance(10 * time.Second)
	eventually(t, "first probe round", func() bool {
		return probeSum(m, "a:1", "b:1", "c:1", "d:1", "f:1") == 5
	})
	fake.BlockUntil(5)
	fake.Advance(5 * time.Second)

	withE := hosts("http://a:1", "http://b:1", "http://c:1", "http://d:1", "http://e:1")
	withE[3].Weight = w(2)
	u2, _ := buildDeps(t, reg, healthUpstream(withE...), deps)

	if u2.active != u1.active {
		t.Error("active checker was recreated on an unrelated change")
	}
	if u2.passive != u1.passive {
		t.Error("passive ejector was recreated on an unrelated change")
	}
	for _, rawURL := range []string{"http://a:1", "http://b:1", "http://c:1", "http://d:1"} {
		old := hostByURL(t, u1, rawURL)
		cur := hostByURL(t, u2, rawURL)
		if cur.active != old.active {
			t.Errorf("%s: status pointer changed on reload", rawURL)
		}
		if cur.passive != old.passive {
			t.Errorf("%s: record pointer changed on reload", rawURL)
		}
	}
	for _, rawURL := range []string{"http://f:1"} {
		for _, h := range u2.Hosts() {
			if h.URL.String() == rawURL {
				t.Errorf("removed host %s still present", rawURL)
			}
		}
	}
	if !hostByURL(t, u2, "http://a:1").Ejected() {
		t.Error("ejection of a was not carried over")
	}
	if got := hostByURL(t, u2, "http://b:1").passive.Status().ConsecutiveFailures; got != 2 {
		t.Errorf("consecutive failures of b = %d, want 2", got)
	}
	if hostByURL(t, u2, "http://c:1").ActiveHealthy() {
		t.Error("active failure of c was not carried over")
	}
	d2 := hostByURL(t, u2, "http://d:1")
	if got := d2.InFlight(); got != 1 {
		t.Errorf("in-flight of d = %d, want 1", got)
	}
	if d2.Weight != 2 {
		t.Errorf("weight of d = %d, want 2", d2.Weight)
	}

	fake.BlockUntil(5)
	fake.Advance(5 * time.Second)
	eventually(t, "second probe round", func() bool {
		return probeSum(m, "a:1", "b:1", "c:1", "d:1") == 8
	})
	if got := m.ActiveChecks.With("u", "a:1", "pass").Value(); got != 2 {
		t.Errorf("probes of a = %v, want 2 (prober restarted on reload?)", got)
	}
}

func TestHealthDefinitionChange(t *testing.T) {
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	deps := Deps{Clock: fake, Rand: clock.NewRand(1)}
	pair := func() []config.Host { return hosts("http://a:1", "http://b:1") }

	u1, _ := buildDeps(t, reg, healthUpstream(pair()...), deps)
	u1.Record(u1.Hosts()[0], breaker.Token{}, health.Failure)
	u1.Record(u1.Hosts()[0], breaker.Token{}, health.Failure)
	u1.Record(u1.Hosts()[0], breaker.Token{}, health.Failure)
	if !u1.Hosts()[0].Ejected() {
		t.Fatal("setup: host a not ejected")
	}

	passiveChanged := healthUpstream(pair()...)
	passiveChanged.PassiveHealth.ConsecutiveFailures = 4
	u2, _ := buildDeps(t, reg, passiveChanged, deps)
	if u2.passive == u1.passive {
		t.Error("passive definition change kept the ejector")
	}
	if u2.active != u1.active {
		t.Error("passive definition change recreated the checker")
	}
	if u2.Hosts()[0].Ejected() {
		t.Error("passive definition change kept the ejection")
	}
	u2.Record(u2.Hosts()[0], breaker.Token{}, health.Failure)
	u2.Record(u2.Hosts()[0], breaker.Token{}, health.Failure)
	u2.Record(u2.Hosts()[0], breaker.Token{}, health.Failure)
	u2.Record(u2.Hosts()[0], breaker.Token{}, health.Failure)

	activeChanged := healthUpstream(pair()...)
	activeChanged.PassiveHealth.ConsecutiveFailures = 4
	activeChanged.ActiveHealth.Interval = dur(20 * time.Second)
	u3, _ := buildDeps(t, reg, activeChanged, deps)
	if u3.active == u2.active {
		t.Error("active definition change kept the checker")
	}
	if u3.passive != u2.passive {
		t.Error("active definition change recreated the ejector")
	}
	if !u3.Hosts()[0].Ejected() {
		t.Error("active definition change dropped the ejection")
	}

	renamed := activeChanged
	renamed.Name = "v"
	u4, _ := buildDeps(t, reg, renamed, deps)
	if u4.passive == u3.passive || u4.active == u3.active {
		t.Error("rename kept passive or active state")
	}
	if u4.Hosts()[0].Ejected() {
		t.Error("rename kept the ejection")
	}
}

func TestFailedBuildTouchesNothing(t *testing.T) {
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	deps := Deps{Clock: fake, Rand: clock.NewRand(1)}
	u, _ := buildDeps(t, reg, healthUpstream(hosts("http://a:1", "http://b:1")...), deps)
	before := u.Hosts()
	beforeStates := len(u.pool.states)
	u.Record(before[0], breaker.Token{}, health.Failure)
	u.Record(before[0], breaker.Token{}, health.Failure)
	u.Record(before[0], breaker.Token{}, health.Failure)

	changed := healthUpstream(hosts("http://a:1", "http://z:1")...)
	changed.PassiveHealth.ConsecutiveFailures = 9
	changed.ActiveHealth.Interval = dur(20 * time.Second)
	tx := reg.Begin()
	built, err := Build(changed, Deps{State: tx, Clock: fake, Rand: clock.NewRand(1)})
	if err != nil {
		tx.Abort()
		t.Fatal(err)
	}
	if len(built.Hosts()) != 0 {
		t.Fatal("Build published hosts before Activate")
	}
	tx.Abort()

	if got := u.Hosts(); len(got) != len(before) || got[0] != before[0] || got[1] != before[1] {
		t.Error("active hosts changed after an aborted build")
	}
	if u.active.Status("http://z:1") != nil {
		t.Error("aborted build added a checker target")
	}
	if u.passive.Record("http://z:1") != nil {
		t.Error("aborted build added an ejector member")
	}
	if u.active.Status("http://a:1") == nil || u.passive.Record("http://a:1") == nil {
		t.Error("active generation lost its members")
	}
	if !before[0].Ejected() {
		t.Error("ejection of a changed after an aborted build")
	}
	if got := len(u.pool.states); got != beforeStates {
		t.Errorf("pool states = %d, want %d", got, beforeStates)
	}
}

func TestConcurrentInvariants(t *testing.T) {
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	urls := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		urls = append(urls, fmt.Sprintf("http://h%d:1", i))
	}
	cfgOf := func(weight int) config.Upstream {
		hs := hosts(urls...)
		hs[0].Weight = w(weight)
		return config.Upstream{Name: "u", Hosts: hs, PassiveHealth: &config.PassiveHealth{
			ConsecutiveFailures: 2,
			BaseEjectionTime:    dur(time.Hour),
			MaxEjectionTime:     dur(time.Hour),
			MaxEjectionPercent:  intp(50),
		}}
	}
	var cur atomic.Pointer[Upstream]
	first, _ := buildDeps(t, reg, cfgOf(1), Deps{Clock: fake, Rand: clock.NewRand(3)})
	cur.Store(first)

	var wg sync.WaitGroup
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rnd := clock.NewRand(seed)
			for i := 0; i < 200; i++ {
				u := cur.Load()
				hs := u.Hosts()
				before := make([]bool, len(hs))
				for j, h := range hs {
					before[j] = h.Ejected()
				}
				h, _, err := u.Acquire(nil, nil, false)
				if err != nil {
					t.Errorf("acquire: %v", err)
					return
				}
				for j := range hs {
					if hs[j] == h && before[j] {
						t.Errorf("picked host %s ejected before the pick", h.URL.Host)
						return
					}
				}
				h.Begin()
				outcome := health.Success
				if rnd.Int64N(2) == 0 {
					outcome = health.Failure
				}
				u.Record(h, breaker.Token{}, outcome)
				h.Done()
				if h.InFlight() < 0 {
					t.Errorf("in-flight of %s is negative", h.URL.Host)
					return
				}
			}
		}(uint64(g + 1))
	}
	for i := 0; i < 5; i++ {
		next, _ := buildDeps(t, reg, cfgOf(1+i%2), Deps{Clock: fake, Rand: clock.NewRand(uint64(i + 10))})
		cur.Store(next)
	}
	wg.Wait()

	final := cur.Load()
	ejected := 0
	for _, h := range final.Hosts() {
		if h.Ejected() {
			ejected++
		}
	}
	if ejected > 3 {
		t.Errorf("ejected hosts = %d, want at most 3", ejected)
	}
}

func TestDepsHasNoSharedHandle(t *testing.T) {
	typ := reflect.TypeOf(Deps{})
	want := []string{"State", "Dial", "Clock", "Rand", "Logger", "Metrics", "Resolver"}
	if typ.NumField() != len(want) {
		t.Fatalf("Deps has %d fields, want %d", typ.NumField(), len(want))
	}
	for i, name := range want {
		if got := typ.Field(i).Name; got != name {
			t.Errorf("field %d = %s, want %s", i, got, name)
		}
	}
}

func TestHostCredentialsNotSurfaced(t *testing.T) {
	var logs syncBuf
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	mreg := metrics.NewRegistry()
	m, err := newMetrics(mreg)
	if err != nil {
		t.Fatal(err)
	}
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	cfg := config.Upstream{
		Name:  "u",
		Hosts: hosts("http://user:s3cret@127.0.0.1:9001"),
		PassiveHealth: &config.PassiveHealth{
			ConsecutiveFailures: 1,
			BaseEjectionTime:    dur(time.Hour),
			MaxEjectionTime:     dur(time.Hour),
			MaxEjectionPercent:  intp(100),
		},
	}
	deps := Deps{Clock: fake, Metrics: m, Logger: logger}
	u, _ := buildDeps(t, reg, cfg, deps)

	if got := u.Status().Hosts[0].URL; got != "http://REDACTED@127.0.0.1:9001" {
		t.Fatalf("status URL = %q, want redacted form", got)
	}
	h := u.Hosts()[0]
	if h.URL.User != nil {
		t.Errorf("host URL still carries userinfo: %s", h.URL)
	}
	h.Begin()
	u.Record(h, breaker.Token{}, health.Failure)
	if !h.Ejected() {
		t.Fatal("host not ejected after failure")
	}

	var out strings.Builder
	if err := mreg.WriteText(&out); err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{"logs": logs.String(), "metrics": out.String()} {
		for _, secret := range []string{"s3cret", "user:"} {
			if strings.Contains(text, secret) {
				t.Errorf("%s contain %q", name, secret)
			}
		}
	}

	tx := reg.Begin()
	_, err = Build(config.Upstream{Name: "u", Hosts: hosts("http://user:s3cret@h:bad")}, Deps{State: tx})
	tx.Abort()
	if err == nil {
		t.Fatal("Build error = nil, want invalid URL error")
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Errorf("build error surfaces credentials: %v", err)
	}

	u2, _ := buildDeps(t, reg, cfg, deps)
	if got := u2.Hosts()[0].InFlight(); got != 1 {
		t.Errorf("in-flight after rebuild with identical URL = %d, want 1", got)
	}

	cb := cbCfg()
	reg2 := state.NewRegistry(nil)
	t.Cleanup(reg2.Close)
	bcfg := config.Upstream{
		Name:           "u",
		Hosts:          hosts("http://user:pw@a:1/?token=tok"),
		CircuitBreaker: &cb,
	}
	bu, _ := buildDeps(t, reg2, bcfg, deps)
	bh := bu.Hosts()[0]
	trip(t, bu, bh)
	if bh.Breaker.State() != breaker.Open {
		t.Fatal("breaker not open after failure")
	}
	var bout strings.Builder
	if err := mreg.WriteText(&bout); err != nil {
		t.Fatal(err)
	}
	statusJSON, err := json.Marshal(bu.Status())
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{"logs": logs.String(), "metrics": bout.String(), "status": string(statusJSON)} {
		for _, secret := range []string{"pw", "=tok"} {
			if strings.Contains(text, secret) {
				t.Errorf("%s contain %q", name, secret)
			}
		}
	}
}

func cbCfg() config.CircuitBreaker {
	return config.CircuitBreaker{
		Window:              dur(10 * time.Second),
		Buckets:             intp(10),
		MinRequests:         intp(100),
		FailureRatio:        floatp(1),
		ConsecutiveFailures: intp(1),
		OpenTimeout:         dur(30 * time.Second),
		HalfOpenProbes:      intp(1),
		SuccessThreshold:    intp(1),
	}
}

func rrBreakerCfg(hs ...config.Host) config.Upstream {
	cb := cbCfg()
	return config.Upstream{
		Name:           "u",
		LoadBalancer:   config.LoadBalancer{Algorithm: "round_robin"},
		Hosts:          hs,
		CircuitBreaker: &cb,
	}
}

func trip(t *testing.T, u *Upstream, h *Host) {
	t.Helper()
	tok, err := h.Breaker.Allow()
	if err != nil {
		t.Fatalf("trip %s: %v", h.URL.Host, err)
	}
	u.Record(h, tok, health.Failure)
}

func TestBreakerExcludesOpenHost(t *testing.T) {
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	u, _ := buildDeps(t, reg, rrBreakerCfg(hosts("http://a:1", "http://b:1")...), Deps{Clock: fake})
	a, b := hostByURL(t, u, "http://a:1"), hostByURL(t, u, "http://b:1")
	trip(t, u, a)
	for i := 0; i < 100; i++ {
		h, tok, err := u.Acquire(nil, nil, true)
		if err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
		if h != b {
			t.Fatalf("pick %d: host = %s, want b:1", i, h.URL.Host)
		}
		u.Record(h, tok, health.Success)
	}
	seen := map[*Host]bool{}
	for i := 0; i < 4; i++ {
		h, _, err := u.Acquire(nil, nil, false)
		if err != nil {
			t.Fatalf("unchecked pick %d: %v", i, err)
		}
		seen[h] = true
	}
	if !seen[a] || !seen[b] {
		t.Fatal("useBreaker=false did not pick both hosts")
	}
}

func TestEligibilityDoesNotConsumeProbes(t *testing.T) {
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	cfg := rrBreakerCfg(hosts("http://a:1", "http://b:1", "http://c:1")...)
	cfg.LoadBalancer.Algorithm = "p2c"
	u, _ := buildDeps(t, reg, cfg, Deps{Clock: fake, Rand: clock.NewRand(5)})
	a := hostByURL(t, u, "http://a:1")
	trip(t, u, a)
	fake.Advance(30 * time.Second)

	type held struct {
		h   *Host
		tok breaker.Token
	}
	var toks []held
	for i := 0; i < 50; i++ {
		h, tok, err := u.Acquire(nil, nil, true)
		if err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
		toks = append(toks, held{h, tok})
	}
	if p := a.Breaker.Snapshot().Probes; p > 1 {
		t.Fatalf("probes in flight = %d, want <= 1", p)
	}
	for _, x := range toks {
		if x.h == a {
			u.Record(x.h, x.tok, health.Ignored)
		} else {
			u.Record(x.h, x.tok, health.Success)
		}
	}
	tok, err := a.Breaker.Allow()
	if err != nil {
		t.Fatalf("first probe after completion: %v", err)
	}
	if _, err := a.Breaker.Allow(); !errors.Is(err, breaker.ErrOpen) {
		t.Fatalf("second probe: err = %v, want breaker.ErrOpen", err)
	}
	u.Record(a, tok, health.Ignored)
}

func TestProbeExhaustionRepicks(t *testing.T) {
	fake := clock.NewFake(testStart)
	m := newTestMetrics(t)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	u, _ := buildDeps(t, reg, rrBreakerCfg(hosts("http://a:1", "http://b:1")...), Deps{Clock: fake, Metrics: m})
	control := build(t, config.Upstream{Name: "u", LoadBalancer: config.LoadBalancer{Algorithm: "round_robin"}, Hosts: hosts("http://a:1", "http://b:1")})
	a, b := hostByURL(t, u, "http://a:1"), hostByURL(t, u, "http://b:1")
	trip(t, u, a)
	fake.Advance(30 * time.Second)

	calls := 0
	beforeAllow = func(h *Host) {
		if h != a {
			return
		}
		calls++
		if _, err := a.Breaker.Allow(); err != nil {
			t.Errorf("slot take inside hook: %v", err)
		}
	}
	t.Cleanup(func() { beforeAllow = nil })

	h, tok, err := u.Acquire(nil, nil, true)
	beforeAllow = nil
	if err != nil {
		t.Fatal(err)
	}
	if h != b {
		t.Fatalf("host = %s, want b:1", h.URL.Host)
	}
	if calls != 1 {
		t.Fatalf("beforeAllow calls for a = %d, want 1", calls)
	}
	if got := m.BreakerRejected.With("u", "a:1").Value(); got != 1 {
		t.Errorf("rejected for a = %v, want 1", got)
	}
	u.Record(h, tok, health.Success)

	control.Acquire(nil, nil, false)
	control.Acquire(nil, nil, false)
	for i := 0; i < 4; i++ {
		got, _, err := u.Acquire(nil, nil, false)
		if err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
		want, _, _ := control.Acquire(nil, nil, false)
		if got.URL.Host != want.URL.Host {
			t.Fatalf("pick %d: host = %s, control = %s", i, got.URL.Host, want.URL.Host)
		}
	}
}

func TestPanicModeHonorsBreakers(t *testing.T) {
	fake := clock.NewFake(testStart)
	m := newTestMetrics(t)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	cfg := rrBreakerCfg(
		config.Host{URL: config.NewSecretURL("http://a:1"), Weight: w(1)},
		config.Host{URL: config.NewSecretURL("http://b:1"), Weight: w(1)},
		config.Host{URL: config.NewSecretURL("http://c:1"), Weight: w(0)},
	)
	cfg.PassiveHealth = &config.PassiveHealth{
		ConsecutiveFailures: 1,
		BaseEjectionTime:    dur(time.Hour),
		MaxEjectionTime:     dur(time.Hour),
		MaxEjectionPercent:  intp(100),
	}
	u, _ := buildDeps(t, reg, cfg, Deps{Clock: fake, Metrics: m})
	a, b, c := hostByURL(t, u, "http://a:1"), hostByURL(t, u, "http://b:1"), hostByURL(t, u, "http://c:1")
	u.Record(a, breaker.Token{}, health.Failure)
	u.Record(b, breaker.Token{}, health.Failure)
	if !a.Ejected() || !b.Ejected() {
		t.Fatal("setup: a and b not ejected")
	}

	for i := 0; i < 1000; i++ {
		var tried map[*Host]bool
		if i%2 == 1 {
			tried = map[*Host]bool{a: true}
		}
		h, tok, err := u.Acquire(nil, tried, true)
		if err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
		if h == c {
			t.Fatalf("pick %d: weight-0 host selected", i)
		}
		u.Record(h, tok, health.Success)
	}

	trip(t, u, a)
	h, tok, err := u.Acquire(nil, nil, true)
	if err != nil || h != b {
		t.Fatalf("with a open: host = %v, err = %v; want b:1", h, err)
	}
	u.Record(h, tok, health.Success)
	if got := gaugeValue(m.PanicMode, "u"); got != 1 {
		t.Errorf("panic gauge = %v, want 1", got)
	}

	trip(t, u, b)
	if _, _, err := u.Acquire(nil, nil, true); !errors.Is(err, ErrAllOpen) {
		t.Fatalf("all open: err = %v, want ErrAllOpen", err)
	}
	for _, x := range []*Host{a, b} {
		if got := m.BreakerRejected.With("u", x.URL.Host).Value(); got < 1 {
			t.Errorf("rejected for %s = %v, want >= 1", x.URL.Host, got)
		}
	}
}

func TestAllOpenVsNoHosts(t *testing.T) {
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)

	zero, _ := buildDeps(t, reg, rrBreakerCfg(
		config.Host{URL: config.NewSecretURL("http://a:1"), Weight: w(0)},
		config.Host{URL: config.NewSecretURL("http://b:1"), Weight: w(0)},
	), Deps{Clock: fake})
	if _, _, err := zero.Acquire(nil, nil, true); !errors.Is(err, ErrNoHosts) || errors.Is(err, ErrAllOpen) {
		t.Fatalf("all weight zero: err = %v, want ErrNoHosts", err)
	}

	open, _ := buildDeps(t, reg, rrBreakerCfg(hosts("http://c:1", "http://d:1")...), Deps{Clock: fake})
	trip(t, open, hostByURL(t, open, "http://c:1"))
	trip(t, open, hostByURL(t, open, "http://d:1"))
	if _, _, err := open.Acquire(nil, nil, true); !errors.Is(err, httperr.ErrCircuitOpen) {
		t.Fatalf("all open: err = %v, want httperr.ErrCircuitOpen", err)
	}
}

func TestRetryExclusionWithBreakers(t *testing.T) {
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	u, _ := buildDeps(t, reg, rrBreakerCfg(hosts("http://a:1", "http://b:1", "http://c:1")...), Deps{Clock: fake})
	a, b, c := hostByURL(t, u, "http://a:1"), hostByURL(t, u, "http://b:1"), hostByURL(t, u, "http://c:1")
	trip(t, u, b)

	for i := 0; i < 20; i++ {
		h, tok, err := u.Acquire(nil, map[*Host]bool{a: true}, true)
		if err != nil || h != c {
			t.Fatalf("tried {a}: host = %v, err = %v; want c:1", h, err)
		}
		u.Record(h, tok, health.Success)
	}
	for i := 0; i < 20; i++ {
		h, tok, err := u.Acquire(nil, map[*Host]bool{a: true, c: true}, true)
		if err != nil {
			t.Fatalf("tried {a, c}: %v", err)
		}
		if h == b {
			t.Fatal("open host b returned")
		}
		if h != a && h != c {
			t.Fatalf("tried {a, c}: host = %s", h.URL.Host)
		}
		u.Record(h, tok, health.Success)
	}
}

func TestBreakerRetainedAcrossBuilds(t *testing.T) {
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	cfg := rrBreakerCfg(hosts("http://a:1", "http://b:1")...)
	u1, _ := buildDeps(t, reg, cfg, Deps{Clock: fake})
	trip(t, u1, hostByURL(t, u1, "http://a:1"))
	before := hostByURL(t, u1, "http://a:1").Breaker

	weighted := cfg
	weighted.Hosts = hosts("http://a:1", "http://b:1")
	weighted.Hosts[1].Weight = w(2)
	u2, _ := buildDeps(t, reg, weighted, Deps{Clock: fake})
	if got := hostByURL(t, u2, "http://a:1").Breaker; got != before {
		t.Fatal("breaker of a was not retained across a same-fingerprint build")
	}
	if st := before.Snapshot(); st.State != breaker.Open || st.UntilHalfOpen != 30*time.Second {
		t.Fatalf("retained breaker: state %v, until half-open %v", st.State, st.UntilHalfOpen)
	}

	cb3 := cbCfg()
	cb3.OpenTimeout = dur(60 * time.Second)
	changed := weighted
	changed.CircuitBreaker = &cb3

	tx := reg.Begin()
	if _, err := Build(changed, Deps{State: tx, Clock: fake}); err != nil {
		tx.Abort()
		t.Fatal(err)
	}
	tx.Abort()
	if got := hostByURL(t, u2, "http://a:1").Breaker; got != before || before.State() != breaker.Open {
		t.Fatal("aborted build changed the breakers of the active generation")
	}

	u3, _ := buildDeps(t, reg, changed, Deps{Clock: fake})
	if got := hostByURL(t, u3, "http://a:1").Breaker; got == before || got.State() != breaker.Closed {
		t.Fatal("changed circuit_breaker did not produce a fresh closed breaker")
	}
}

func TestBreakerMetricsAndLogs(t *testing.T) {
	var logs syncBuf
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	mreg := metrics.NewRegistry()
	m, err := newMetrics(mreg)
	if err != nil {
		t.Fatal(err)
	}
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	u, _ := buildDeps(t, reg, rrBreakerCfg(hosts("http://a:1", "http://b:1")...), Deps{Clock: fake, Metrics: m, Logger: logger})

	var out strings.Builder
	if err := mreg.WriteText(&out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		`microgateway_circuit_breaker_state{upstream="u",host="a:1"} 0`,
		`microgateway_circuit_breaker_state{upstream="u",host=""} 0`,
		`microgateway_circuit_breaker_transitions_total{upstream="u",host="a:1",from="closed",to="open"} 0`,
		`microgateway_circuit_breaker_transitions_total{upstream="u",host="a:1",from="open",to="half_open"} 0`,
		`microgateway_circuit_breaker_transitions_total{upstream="u",host="a:1",from="half_open",to="open"} 0`,
		`microgateway_circuit_breaker_transitions_total{upstream="u",host="a:1",from="half_open",to="closed"} 0`,
		`microgateway_circuit_breaker_rejected_total{upstream="u",host="a:1"} 0`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing series %s", want)
		}
	}

	trip(t, u, hostByURL(t, u, "http://a:1"))
	if got := gaugeValue(m.BreakerState, "u", "a:1"); got != 2 {
		t.Errorf("state gauge = %v, want 2", got)
	}
	if got := m.BreakerTransitions.With("u", "a:1", "closed", "open").Value(); got != 1 {
		t.Errorf("closed->open transitions = %v, want 1", got)
	}
	line := logs.String()
	for _, want := range []string{
		"level=WARN",
		`msg="circuit breaker state changed"`,
		"upstream=u",
		"host=a:1",
		"scope=host",
		"from=closed",
		"to=open",
		"reason=consecutive",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("log missing %q: %s", want, line)
		}
	}
}

func TestBreakerStatus(t *testing.T) {
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	u, _ := buildDeps(t, reg, rrBreakerCfg(hosts("http://a:1", "http://b:1")...), Deps{Clock: fake})
	trip(t, u, hostByURL(t, u, "http://a:1"))

	st := u.Status()
	if len(st.Hosts) != 2 {
		t.Fatalf("hosts = %d, want 2", len(st.Hosts))
	}
	if st.Hosts[0].BreakerState != "open" || st.Hosts[0].Breaker == nil || st.Hosts[0].Breaker.HalfOpenInSeconds != 30 {
		t.Errorf("host a status = %+v, want open with 30s to half-open", st.Hosts[0])
	}
	if st.Hosts[1].BreakerState != "closed" || st.Hosts[1].Breaker == nil {
		t.Errorf("host b status = %+v, want closed", st.Hosts[1])
	}
	if st.BreakerState != "closed" || st.Breaker == nil {
		t.Errorf("aggregate status = %q / %v, want closed with a breaker", st.BreakerState, st.Breaker)
	}

	plain := build(t, config.Upstream{Name: "n", Hosts: hosts("http://c:1")})
	ps := plain.Status()
	if ps.BreakerState != "closed" || ps.Breaker != nil {
		t.Errorf("upstream without circuit_breaker: state %q, breaker %v", ps.BreakerState, ps.Breaker)
	}
	if ps.Hosts[0].BreakerState != "closed" || ps.Hosts[0].Breaker != nil {
		t.Errorf("host without circuit_breaker: %+v", ps.Hosts[0])
	}
}

func TestRecordCompletesToken(t *testing.T) {
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	u, _ := buildDeps(t, reg, rrBreakerCfg(hosts("http://a:1")...), Deps{Clock: fake})
	a := hostByURL(t, u, "http://a:1")
	tok, err := a.Breaker.Allow()
	if err != nil {
		t.Fatal(err)
	}
	if got := a.Breaker.Snapshot().InFlight; got != 1 {
		t.Fatalf("in-flight after Allow = %d, want 1", got)
	}
	u.Record(a, tok, health.Success)
	if got := a.Breaker.Snapshot().InFlight; got != 0 {
		t.Errorf("in-flight after Record = %d, want 0", got)
	}
	u.Record(a, breaker.Token{}, health.Failure)
	if got := a.Breaker.Snapshot().Failures; got != 0 {
		t.Errorf("zero token recorded a failure: %d", got)
	}
}

func TestActivateFailureMutatesNothing(t *testing.T) {
	var logs syncBuf
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	cfg := rrBreakerCfg(hosts("http://a:1", "http://b:1")...)
	cfg.PassiveHealth = &config.PassiveHealth{ConsecutiveFailures: 1, MaxEjectionPercent: intp(100)}
	u1, _ := buildDeps(t, reg, cfg, Deps{Clock: fake, Logger: logger})
	a1 := hostByURL(t, u1, "http://a:1")
	trip(t, u1, a1)
	before := u1.pool.current.Load()

	cfg2 := rrBreakerCfg(hosts("http://a:1", "http://c:1")...)
	cfg2.PassiveHealth = cfg.PassiveHealth
	tx := reg.Begin()
	u2, err := Build(cfg2, Deps{State: tx, Clock: fake, Logger: logger})
	if err != nil {
		tx.Abort()
		t.Fatal(err)
	}
	lease := tx.Commit()
	t.Cleanup(func() { _ = lease.Release() })
	u2.lbCfg.Algorithm = "bogus"
	u2.Activate()

	if u1.pool.current.Load() != before {
		t.Error("failed activation replaced the published host set")
	}
	if len(u1.brk.byKey) != 2 || u1.brk.byKey["http://a:1"] != a1.Breaker || u1.brk.byKey["http://b:1"] == nil {
		t.Error("failed activation changed the host breakers")
	}
	if a1.Breaker.State() != breaker.Open {
		t.Error("failed activation changed the state of a's breaker")
	}
	if len(u1.pool.states) != 2 || u1.pool.states["http://a:1"] == nil || u1.pool.states["http://b:1"] == nil {
		t.Errorf("pool states keys changed after a failed activation: %d entries", len(u1.pool.states))
	}
	if got := strings.Count(logs.String(), "upstream host set not applied"); got != 1 {
		t.Errorf("host set failure logged %d times, want 1", got)
	}

	u2.lbCfg.Algorithm = "round_robin"
	u2.Activate()
	if got := hostByURL(t, u2, "http://a:1").Breaker; got != a1.Breaker || got.State() != breaker.Open {
		t.Error("retained host a lost its open breaker")
	}
	if _, ok := u2.brk.byKey["http://b:1"]; ok {
		t.Error("removed host b kept its breaker")
	}
	if c := hostByURL(t, u2, "http://c:1").Breaker; c == nil || c.State() != breaker.Closed {
		t.Error("added host c did not get a closed breaker")
	}
}
