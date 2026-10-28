// Package upstream holds backend hosts, selects a host per attempt, and builds upstream transports.
// All state is per gateway instance and is never shared across instances; instances may disagree about host health.
package upstream

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/breaker"
	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/health"
	"github.com/ok0/lychgate/internal/httperr"
	"github.com/ok0/lychgate/internal/lb"
	"github.com/ok0/lychgate/internal/metrics"
	"github.com/ok0/lychgate/internal/reqctx"
	"github.com/ok0/lychgate/internal/state"
)

var ErrNoHosts = httperr.ErrNoHealthyUpstream

const (
	KindTransport = "upstream.transport"
	KindPool      = "upstream.pool"
	KindPassive   = "upstream.passive"
	KindActive    = "upstream.active"
	KindDNS       = "upstream.dns"
)

// DialFunc opens a connection to addr; it has the signature of net.Dialer.DialContext.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// Deps are the dependencies of Build. State is required; nil values take the defaults noted on each field.
type Deps struct {
	State    *state.Tx
	Dial     DialFunc     // Dial opens backend connections; nil dials TCP.
	Clock    clock.Clock  // nil: clock.Real().
	Rand     clock.Rand   // nil: clock.NewRand(1).
	Logger   *slog.Logger // nil: discard.
	Metrics  *Metrics     // nil: metrics are recorded in a discarded registry.
	Resolver Resolver     // nil: net.DefaultResolver.
}

// Metrics are the upstream series. They are registered by the owner of the registry.
type Metrics struct {
	Attempts           *metrics.CounterVec
	Ejections          *metrics.CounterVec
	EjectionsRefused   *metrics.CounterVec
	DNSRefresh         *metrics.CounterVec
	ActiveChecks       *metrics.CounterVec
	HostHealthy        *metrics.GaugeVec
	PanicMode          *metrics.GaugeVec
	BreakerState       *metrics.GaugeVec
	BreakerTransitions *metrics.CounterVec
	BreakerRejected    *metrics.CounterVec
}

func newMetrics(r *metrics.Registry) (*Metrics, error) {
	var (
		m   Metrics
		err error
	)
	if m.Attempts, err = r.Counter("lychgate_upstream_attempts_total", "Upstream attempts by outcome.", "upstream", "host", "outcome"); err != nil {
		return nil, err
	}
	if m.Ejections, err = r.Counter("lychgate_upstream_ejections_total", "Upstream host ejections.", "upstream", "host"); err != nil {
		return nil, err
	}
	if m.EjectionsRefused, err = r.Counter("lychgate_upstream_ejections_refused_total", "Ejections refused by max_ejection_percent.", "upstream"); err != nil {
		return nil, err
	}
	if m.DNSRefresh, err = r.Counter("lychgate_upstream_dns_refresh_total", "Upstream DNS refreshes by result.", "upstream", "result"); err != nil {
		return nil, err
	}
	if m.ActiveChecks, err = r.Counter("lychgate_upstream_active_checks_total", "Upstream active health probes by result.", "upstream", "host", "result"); err != nil {
		return nil, err
	}
	if m.HostHealthy, err = r.Gauge("lychgate_upstream_host_healthy", "Whether an upstream host is currently healthy.", "upstream", "host"); err != nil {
		return nil, err
	}
	if m.PanicMode, err = r.Gauge("lychgate_upstream_panic_mode", "Whether an upstream is in panic mode.", "upstream"); err != nil {
		return nil, err
	}
	if m.BreakerState, err = r.Gauge("lychgate_circuit_breaker_state", "Circuit breaker state (0 closed, 1 half-open, 2 open); host is empty for the upstream aggregate.", "upstream", "host"); err != nil {
		return nil, err
	}
	if m.BreakerTransitions, err = r.Counter("lychgate_circuit_breaker_transitions_total", "Circuit breaker state transitions.", "upstream", "host", "from", "to"); err != nil {
		return nil, err
	}
	if m.BreakerRejected, err = r.Counter("lychgate_circuit_breaker_rejected_total", "Requests or attempts rejected by an open circuit breaker.", "upstream", "host"); err != nil {
		return nil, err
	}
	return &m, nil
}

func discardMetrics() *Metrics {
	m, err := newMetrics(metrics.NewRegistry())
	if err != nil {
		panic(err)
	}
	return m
}

func (d Deps) withDefaults() Deps {
	if d.Clock == nil {
		d.Clock = clock.Real()
	}
	if d.Rand == nil {
		d.Rand = clock.NewRand(1)
	}
	if d.Logger == nil {
		d.Logger = slog.New(slog.DiscardHandler)
	}
	if d.Metrics == nil {
		d.Metrics = discardMetrics()
	}
	if d.Resolver == nil {
		d.Resolver = net.DefaultResolver
	}
	return d
}

type pooledTransport struct{ *http.Transport }

func (t *pooledTransport) Close() error {
	t.CloseIdleConnections()
	return nil
}

type Host struct {
	// URL never carries userinfo.
	URL    *url.URL
	Weight int

	key     string
	display string
	state   *hostState
	passive *health.Record
	active  *health.Status
	Breaker *breaker.Breaker
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

func (h *Host) Ejected() bool {
	return h.passive != nil && h.passive.Ejected()
}

func (h *Host) ActiveHealthy() bool {
	return h.active == nil || h.active.Healthy()
}

type hostSpec struct {
	key     string
	display string
	url     *url.URL
	weight  int
}

type hostSet struct {
	hosts    []*Host
	byKey    map[string]*Host
	bal      lb.Balancer
	eligible func(i int) bool
	usable   bool
	ejector  *health.Ejector
	checker  *health.Checker
}

func (s *hostSet) isEligible(i int) bool {
	h := s.hosts[i]
	return h.Weight > 0 && !h.Ejected() && h.ActiveHealthy()
}

type Upstream struct {
	Name                  string
	Algorithm             string
	Transport             *http.Transport
	ResponseHeaderTimeout time.Duration

	lbCfg   config.LoadBalancer
	static  []hostSpec
	pool    *pool
	passive *health.Ejector
	active  *health.Checker
	dns     *dnsWatcher
	rnd     clock.Rand
	logger  *slog.Logger
	set     atomic.Pointer[hostSet]
	Breaker *breaker.Breaker
	brk     *hostBreakers
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

func fingerprint(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func maxEjectionPercent(cfg config.PassiveHealth) int {
	if cfg.MaxEjectionPercent != nil {
		return *cfg.MaxEjectionPercent
	}
	return 50
}

func Build(cfg config.Upstream, d Deps) (*Upstream, error) {
	d = d.withDefaults()
	static := make([]hostSpec, 0, len(cfg.Hosts))
	for i, hc := range cfg.Hosts {
		pu, err := url.Parse(hc.URL)
		if err != nil {
			inner := err
			var ue *url.Error
			if errors.As(err, &ue) {
				inner = ue.Err
			}
			return nil, fmt.Errorf("upstream %q: host %d: invalid URL %q: %w", cfg.Name, i, config.RedactURL(hc.URL), inner)
		}
		weight := 1
		if hc.Weight != nil {
			weight = *hc.Weight
		}
		key := pu.String()
		pu.User = nil
		static = append(static, hostSpec{key: key, display: config.RedactURL(key), url: pu, weight: weight})
	}
	if _, err := lb.New(cfg.LoadBalancer, nil, d.Rand); err != nil {
		return nil, fmt.Errorf("upstream %q: %w", cfg.Name, err)
	}
	algo := cfg.LoadBalancer.Algorithm
	if algo == "" {
		algo = "p2c"
	}

	tr, err := state.Get(d.State, KindTransport, cfg.Name, TransportFingerprint(cfg), func() (*pooledTransport, error) {
		return &pooledTransport{NewTransport(cfg, d.Dial, d.Clock)}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("upstream %q: %w", cfg.Name, err)
	}
	pl, err := state.Get(d.State, KindPool, cfg.Name, "", func() (*pool, error) {
		return newPool(cfg.Name, d), nil
	})
	if err != nil {
		return nil, fmt.Errorf("upstream %q: %w", cfg.Name, err)
	}

	var passive *health.Ejector
	if cfg.PassiveHealth != nil {
		ph := *cfg.PassiveHealth
		passive, err = state.Get(d.State, KindPassive, cfg.Name, fingerprint(ph), func() (*health.Ejector, error) {
			return health.NewEjector(ph, d.Clock, pl.ejectorEvents(maxEjectionPercent(ph))), nil
		})
		if err != nil {
			return nil, fmt.Errorf("upstream %q: %w", cfg.Name, err)
		}
	}

	var active *health.Checker
	if cfg.ActiveHealth != nil {
		ah := *cfg.ActiveHealth
		active, err = state.Get(d.State, KindActive, cfg.Name, fingerprint(ah), func() (*health.Checker, error) {
			var c *health.Checker
			var cerr error
			c, cerr = health.NewChecker(ah, health.CheckerDeps{
				Clock: d.Clock,
				Rand:  d.Rand,
				OnProbe: func(key string, pass bool, err error) {
					pl.onProbe(key, pass, err)
				},
				OnChange: func(key string, healthy bool, st health.ActiveStatus) {
					pl.onActiveChange(c, key, healthy, st)
				},
			})
			return c, cerr
		})
		if err != nil {
			return nil, fmt.Errorf("upstream %q: %w", cfg.Name, err)
		}
	}

	var dns *dnsWatcher
	if cfg.DNS != nil {
		dc := *cfg.DNS
		dns, err = state.Get(d.State, KindDNS, cfg.Name, fingerprint(dc), func() (*dnsWatcher, error) {
			return newDNSWatcher(cfg.Name, dc, d), nil
		})
		if err != nil {
			return nil, fmt.Errorf("upstream %q: %w", cfg.Name, err)
		}
	}

	var agg *breaker.Breaker
	var brk *hostBreakers
	if cfg.CircuitBreaker != nil {
		cb := *cfg.CircuitBreaker
		cbFP := fingerprint(cb)
		agg, err = state.Get(d.State, KindBreaker, cfg.Name, cbFP, func() (*breaker.Breaker, error) {
			return newAggregate(cfg.Name, cb, d.Clock, pl)
		})
		if err != nil {
			return nil, fmt.Errorf("upstream %q: circuit_breaker: %w", cfg.Name, err)
		}
		brk, err = state.Get(d.State, KindBreakerHosts, cfg.Name, cbFP, func() (*hostBreakers, error) {
			return newHostBreakers(cfg.Name, cb, d.Clock, pl)
		})
		if err != nil {
			return nil, fmt.Errorf("upstream %q: circuit_breaker: %w", cfg.Name, err)
		}
	}

	return &Upstream{
		Name:                  cfg.Name,
		Algorithm:             algo,
		Transport:             tr.Transport,
		ResponseHeaderTimeout: cfg.Timeouts.ResponseHeader.Std(),
		lbCfg:                 cfg.LoadBalancer,
		static:                static,
		pool:                  pl,
		passive:               passive,
		active:                active,
		dns:                   dns,
		rnd:                   d.Rand,
		logger:                d.Logger,
		Breaker:               agg,
		brk:                   brk,
	}, nil
}

// Activate publishes the host set of u. It is the only point where a generation changes shared state.
func (u *Upstream) Activate() {
	p := u.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	p.owner = u
	if u.dns != nil {
		res := u.dns.bind(u.dnsUpdate)
		p.dnsSrc = u.dns
		p.dnsSeq = res.seq
		u.applyLocked(dnsSpecs(res), true)
		return
	}
	p.dnsSrc = nil
	u.applyLocked(u.static, false)
}

func (u *Upstream) dnsUpdate(res dnsResult) {
	p := u.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.owner != u || p.dnsSrc != u.dns || res.seq <= p.dnsSeq {
		return
	}
	p.dnsSeq = res.seq
	u.applyLocked(dnsSpecs(res), true)
}

func dnsSpecs(res dnsResult) []hostSpec {
	specs := make([]hostSpec, 0, len(res.urls))
	for _, s := range res.urls {
		pu, err := url.Parse(s)
		if err != nil {
			continue
		}
		key := pu.String()
		specs = append(specs, hostSpec{key: key, display: key, url: pu, weight: 1})
	}
	return specs
}

// applyLocked must be called with pool.mu held. It mutates no shared state until lb.New has succeeded.
func (u *Upstream) applyLocked(specs []hostSpec, dns bool) {
	p := u.pool
	states := make(map[string]*hostState, len(specs))
	for _, s := range specs {
		st, ok := p.states[s.key]
		if !ok {
			st = &hostState{}
		}
		states[s.key] = st
	}

	hosts := make([]*Host, 0, len(specs))
	byKey := make(map[string]*Host, len(specs))
	eps := make([]lb.Endpoint, 0, len(specs))
	usable := false
	for _, s := range specs {
		h := &Host{URL: s.url, Weight: s.weight, key: s.key, display: s.display, state: states[s.key]}
		hosts = append(hosts, h)
		byKey[s.key] = h
		eps = append(eps, lb.Endpoint{ID: s.key, Weight: s.weight, InFlight: h.InFlight})
		if s.weight > 0 {
			usable = true
		}
	}
	bal, err := lb.New(u.lbCfg, eps, u.rnd)
	if err != nil {
		u.logger.Error("upstream host set not applied", "upstream", u.Name, "error", err)
		return
	}

	p.states = states
	var bm map[string]*breaker.Breaker
	if u.brk != nil {
		keys := make([]string, 0, len(specs))
		for _, s := range specs {
			keys = append(keys, s.key)
		}
		bm = u.brk.sync(keys)
	}
	members := make([]health.Member, 0, len(specs))
	targets := make([]health.Target, 0, len(specs))
	for _, s := range specs {
		members = append(members, health.Member{Key: s.key, Weight: s.weight})
		targets = append(targets, health.Target{Key: s.key, URL: s.url})
	}
	if u.passive != nil {
		u.passive.SetMembers(members)
	}
	if u.active != nil {
		u.active.SetTargets(targets, u.Transport)
	}
	for _, h := range hosts {
		h.Breaker = bm[h.key]
		if u.passive != nil {
			h.passive = u.passive.Record(h.key)
		}
		if u.active != nil {
			h.active = u.active.Status(h.key)
		}
	}

	next := &hostSet{
		hosts:   hosts,
		byKey:   byKey,
		bal:     bal,
		usable:  usable,
		ejector: u.passive,
		checker: u.active,
	}
	next.eligible = next.isEligible

	prev := p.current.Load()
	u.set.Store(next)
	p.current.Store(next)
	if dns {
		added, removed := changedKeys(prev, next)
		if added+removed > 0 {
			u.logger.Info("upstream dns host set changed", "upstream", u.Name, "added", added, "removed", removed, "hosts", len(hosts))
		}
	}
	p.brkHosts.Store(u.brk)
	p.brkAgg.Store(u.Breaker)
	p.prefill(u, next)
}

func changedKeys(prev, next *hostSet) (added, removed int) {
	var old map[string]*Host
	if prev != nil {
		old = prev.byKey
	}
	for k := range next.byKey {
		if _, ok := old[k]; !ok {
			added++
		}
	}
	for k := range old {
		if _, ok := next.byKey[k]; !ok {
			removed++
		}
	}
	return added, removed
}

// Hosts returns the current host set of u. It is empty before Activate.
func (u *Upstream) Hosts() []*Host {
	s := u.set.Load()
	if s == nil {
		return []*Host{}
	}
	return s.hosts
}

// beforeAllow is a test seam. When non-nil it runs before each breaker admission check.
var beforeAllow func(*Host)

const (
	flagBaseSet uint8 = 1 << iota
	flagBase
	flagBrkSet
	flagBrk
	flagExcluded
)

// hostView memoizes the eligibility answers of one Acquire call, so each host is evaluated at most once per call.
type hostView struct {
	set        *hostSet
	useBreaker bool
	flags      []uint8
	inline     [64]uint8
}

func newHostView(s *hostSet, useBreaker bool) *hostView {
	v := &hostView{set: s, useBreaker: useBreaker}
	if n := len(s.hosts); n <= len(v.inline) {
		v.flags = v.inline[:n]
	} else {
		v.flags = make([]uint8, n)
	}
	return v
}

func (v *hostView) base(i int) bool {
	f := &v.flags[i]
	if *f&flagBaseSet == 0 {
		*f |= flagBaseSet
		if v.set.isEligible(i) {
			*f |= flagBase
		}
	}
	return *f&flagBase != 0
}

// brk reads the breaker without taking a probe slot: an open breaker is not ready, a half-open one is ready while a slot is free.
func (v *hostView) brk(i int) bool {
	f := &v.flags[i]
	if *f&flagBrkSet == 0 {
		*f |= flagBrkSet
		if h := v.set.hosts[i]; !v.useBreaker || h.Breaker == nil || h.Breaker.Ready() {
			*f |= flagBrk
		}
	}
	return *f&flagBrk != 0
}

func (v *hostView) excluded(i int) bool { return v.flags[i]&flagExcluded != 0 }

func (v *hostView) exclude(i int) { v.flags[i] |= flagExcluded }

func (v *hostView) admits(i, phase int, tried map[*Host]bool) bool {
	h := v.set.hosts[i]
	if v.excluded(i) || h.Weight <= 0 || !v.brk(i) {
		return false
	}
	switch phase {
	case 1:
		return !tried[h] && v.base(i)
	case 2:
		return v.base(i)
	case 3:
		return !tried[h]
	}
	return true
}

// Acquire picks a host for one attempt. When useBreaker is set, the picked host's breaker admits the attempt and the returned token must be completed with Record.
// Eligibility is read once per host per call and breaker reads never take probe slots; only the picked host's slot is taken.
func (u *Upstream) Acquire(r *http.Request, tried map[*Host]bool, useBreaker bool) (*Host, breaker.Token, error) {
	s := u.set.Load()
	if s == nil || !s.usable {
		return nil, breaker.Token{}, ErrNoHosts
	}
	var in lb.Input
	if u.Algorithm == "consistent_hash" {
		in.Key, in.HasKey = u.hashKey(r)
	}
	v := newHostView(s, useBreaker)
	for phase := 1; phase <= 4; phase++ {
		if (phase == 1 || phase == 3) && len(tried) == 0 {
			continue
		}
		in.Eligible = func(i int) bool { return v.admits(i, phase, tried) }
		for {
			i, ok := s.bal.Pick(in)
			if !ok {
				break
			}
			h := s.hosts[i]
			var tok breaker.Token
			if useBreaker && h.Breaker != nil {
				if beforeAllow != nil {
					beforeAllow(h)
				}
				t, err := h.Breaker.Allow()
				if err != nil {
					v.exclude(i)
					u.pool.metrics.BreakerRejected.With(u.Name, h.URL.Host).Inc()
					continue
				}
				tok = t
			}
			if phase <= 2 {
				u.pool.leaveIfPanic()
			} else {
				u.pool.enterPanic()
			}
			return h, tok, nil
		}
	}
	if useBreaker {
		for i, h := range s.hosts {
			if h.Weight > 0 && !v.brk(i) {
				u.pool.metrics.BreakerRejected.With(u.Name, h.URL.Host).Inc()
			}
		}
		return nil, breaker.Token{}, ErrAllOpen
	}
	return nil, breaker.Token{}, ErrNoHosts
}

// HasBreaker reports whether the upstream defines circuit_breaker.
func (u *Upstream) HasBreaker() bool { return u.Breaker != nil }

// AllowAggregate admits a request through the upstream aggregate breaker. Without a breaker it admits with a zero token.
func (u *Upstream) AllowAggregate() (breaker.Token, error) {
	if u.Breaker == nil {
		return breaker.Token{}, nil
	}
	tok, err := u.Breaker.Allow()
	if err != nil {
		u.pool.metrics.BreakerRejected.With(u.Name, "").Inc()
		return breaker.Token{}, ErrAllOpen
	}
	return tok, nil
}

func (u *Upstream) hashKey(r *http.Request) (string, bool) {
	if r == nil {
		return "", false
	}
	var key string
	switch u.lbCfg.Hash.Source {
	case "header":
		key = r.Header.Get(u.lbCfg.Hash.Name)
	case "cookie":
		if c, err := r.Cookie(u.lbCfg.Hash.Name); err == nil {
			key = c.Value
		}
	case "path":
		if r.URL != nil {
			key = r.URL.Path
		}
	}
	if key != "" {
		return key, true
	}
	ip := reqctx.ClientIP(r.Context())
	if !ip.IsValid() {
		ip = reqctx.PeerAddr(r)
	}
	if !ip.IsValid() {
		return "", false
	}
	return ip.String(), true
}

// Record completes the breaker token of an attempt and reports its outcome to the passive ejector of the generation that built h.
func (u *Upstream) Record(h *Host, tok breaker.Token, o health.Outcome) {
	tok.Done(BreakerOutcome(o))
	if h == nil || h.passive == nil {
		return
	}
	h.passive.Observe(o)
}

// RefreshHealth evaluates ejection expiry for every current host and updates the health gauges.
func (u *Upstream) RefreshHealth() {
	if u.Breaker != nil {
		u.pool.metrics.BreakerState.With(u.Name, "").Set(float64(u.Breaker.State()))
	}
	s := u.set.Load()
	if s == nil {
		return
	}
	for _, h := range s.hosts {
		u.pool.setHealthGauge(h)
		if h.Breaker != nil {
			u.pool.metrics.BreakerState.With(u.Name, h.URL.Host).Set(float64(h.Breaker.State()))
		}
	}
}
