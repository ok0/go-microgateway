package health

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
)

const maxProbeDrain = 64 << 10

var errNilRand = errors.New("health: random source is required")

// Target is one upstream host probed by a Checker. Key identifies the host in the callbacks, and URL supplies the scheme and host of each probe request.
type Target struct {
	Key string
	URL *url.URL
}

// CheckerDeps supplies the clock, the random source for probe jitter and the optional callbacks of a Checker. Rand is required and Clock defaults to the real clock. OnProbe runs after every probe and OnChange runs when a host changes between healthy and unhealthy.
type CheckerDeps struct {
	Clock    clock.Clock
	Rand     clock.Rand
	OnProbe  func(key string, pass bool, err error)
	OnChange func(key string, healthy bool, st ActiveStatus)
}

// ActiveStatus is the active-check state of one host as returned by Status.Snapshot. State is "unknown" before the first probe, then "healthy" or "unhealthy".
type ActiveStatus struct {
	State               string
	ConsecutivePasses   int
	ConsecutiveFailures int
	LastResult          time.Time
}

type statusRange struct {
	lo, hi int
}

// Checker probes a set of upstream hosts on a jittered schedule. Each host has its own goroutine; SetTargets starts and stops them and Close stops all of them. Checker is safe for concurrent use.
type Checker struct {
	mu         sync.Mutex
	closed     bool
	probers    map[string]*prober
	clk        clock.Clock
	rnd        clock.Rand
	onProbe    func(key string, pass bool, err error)
	onChange   func(key string, healthy bool, st ActiveStatus)
	interval   time.Duration
	timeout    time.Duration
	jitter     float64
	path       string
	healthyN   int
	unhealthyN int
	expected   []statusRange
	rtp        atomic.Pointer[http.RoundTripper]
}

type prober struct {
	target Target
	status *Status
	cancel context.CancelFunc
	done   chan struct{}
}

// Status holds the active-check result of one host. It is safe for concurrent use.
type Status struct {
	mu      sync.Mutex
	healthy atomic.Bool
	st      ActiveStatus
}

// NewChecker returns a Checker for cfg. It fails when d.Rand is nil or an expected status range is malformed. Unset jitter, healthy threshold and unhealthy threshold default to 0.1, 2 and 3.
func NewChecker(cfg config.ActiveHealth, d CheckerDeps) (*Checker, error) {
	if d.Rand == nil {
		return nil, errNilRand
	}
	clk := d.Clock
	if clk == nil {
		clk = clock.Real()
	}
	jitter := 0.1
	if cfg.Jitter != nil {
		jitter = *cfg.Jitter
	}
	healthyN := 2
	if cfg.HealthyThreshold != nil {
		healthyN = *cfg.HealthyThreshold
	}
	unhealthyN := 3
	if cfg.UnhealthyThreshold != nil {
		unhealthyN = *cfg.UnhealthyThreshold
	}
	expected := make([]statusRange, 0, len(cfg.ExpectedStatuses))
	for _, s := range cfg.ExpectedStatuses {
		r, err := parseStatusRange(s)
		if err != nil {
			return nil, err
		}
		expected = append(expected, r)
	}
	return &Checker{
		probers:    map[string]*prober{},
		clk:        clk,
		rnd:        d.Rand,
		onProbe:    d.OnProbe,
		onChange:   d.OnChange,
		interval:   cfg.Interval.Std(),
		timeout:    cfg.Timeout.Std(),
		jitter:     jitter,
		path:       cfg.Path,
		healthyN:   healthyN,
		unhealthyN: unhealthyN,
		expected:   expected,
	}, nil
}

// SetTargets replaces the probed hosts with ts and uses rt for every probe. A host that stays in the set keeps its status, a new host starts in the unknown state, and a removed host is stopped before SetTargets returns. It does nothing after Close.
func (c *Checker) SetTargets(ts []Target, rt http.RoundTripper) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.rtp.Store(&rt)
	next := make(map[string]*prober, len(ts))
	for _, t := range ts {
		if _, ok := next[t.Key]; ok {
			continue
		}
		if p, ok := c.probers[t.Key]; ok {
			next[t.Key] = p
			continue
		}
		next[t.Key] = c.startLocked(t)
	}
	var stopped []*prober
	for k, p := range c.probers {
		if _, ok := next[k]; !ok {
			stopped = append(stopped, p)
		}
	}
	c.probers = next
	c.mu.Unlock()
	for _, p := range stopped {
		p.cancel()
		<-p.done
	}
}

// Status returns the status of the host with the given key, or nil when that key is not probed.
func (c *Checker) Status(key string) *Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p, ok := c.probers[key]; ok {
		return p.status
	}
	return nil
}

// Close stops every probe goroutine and waits for them to exit. It is idempotent and always returns nil.
func (c *Checker) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	ps := make([]*prober, 0, len(c.probers))
	for _, p := range c.probers {
		ps = append(ps, p)
	}
	c.mu.Unlock()
	for _, p := range ps {
		p.cancel()
		<-p.done
	}
	return nil
}

// Healthy reports the current health decision for the host. It is true until the host becomes unhealthy and is true again once it recovers.
func (s *Status) Healthy() bool {
	return s.healthy.Load()
}

// Snapshot returns a copy of the active-check state of the host.
func (s *Status) Snapshot() ActiveStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st
}

func (c *Checker) startLocked(t Target) *prober {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Status{st: ActiveStatus{State: "unknown"}}
	s.healthy.Store(true)
	p := &prober{target: t, status: s, cancel: cancel, done: make(chan struct{})}
	go c.run(ctx, p)
	return p
}

func (c *Checker) run(ctx context.Context, p *prober) {
	defer close(p.done)
	for {
		wake := make(chan struct{})
		t := c.clk.AfterFunc(nextDelay(c.interval, c.jitter, c.rnd), func() { close(wake) })
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-wake:
		}
		c.probe(ctx, p)
	}
}

func (c *Checker) probe(ctx context.Context, p *prober) {
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	timer := c.clk.AfterFunc(c.timeout, cancel)
	pass, err := c.request(pctx, p.target)
	timer.Stop()
	fire, healthy, st := p.status.apply(pass, c.clk.Now(), c.healthyN, c.unhealthyN)
	if fire && c.onChange != nil {
		c.onChange(p.target.Key, healthy, st)
	}
	if c.onProbe != nil {
		c.onProbe(p.target.Key, pass, err)
	}
}

func (c *Checker) request(pctx context.Context, t Target) (bool, error) {
	target := url.URL{Scheme: t.URL.Scheme, Host: t.URL.Host, Path: c.path}
	req, err := http.NewRequestWithContext(pctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("User-Agent", "microgateway-health-check")
	rt := *c.rtp.Load()
	resp, err := rt.RoundTrip(req)
	if err != nil {
		return false, err
	}
	_, _ = io.CopyN(io.Discard, resp.Body, maxProbeDrain)
	_ = resp.Body.Close()
	return c.expects(resp.StatusCode) && pctx.Err() == nil, nil
}

func (c *Checker) expects(code int) bool {
	for _, r := range c.expected {
		if code >= r.lo && code <= r.hi {
			return true
		}
	}
	return false
}

func (s *Status) apply(pass bool, now time.Time, healthyN, unhealthyN int) (bool, bool, ActiveStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if pass {
		s.st.ConsecutivePasses++
		s.st.ConsecutiveFailures = 0
		s.st.LastResult = now
		switch {
		case s.st.State == "unknown":
			s.st.State = "healthy"
		case s.st.State == "unhealthy" && s.st.ConsecutivePasses >= healthyN:
			s.st.State = "healthy"
			s.healthy.Store(true)
			return true, true, s.st
		}
		return false, false, s.st
	}
	s.st.ConsecutiveFailures++
	s.st.ConsecutivePasses = 0
	if s.st.State != "unhealthy" && s.st.ConsecutiveFailures >= unhealthyN {
		s.st.State = "unhealthy"
		s.healthy.Store(false)
		return true, false, s.st
	}
	return false, false, s.st
}

func nextDelay(interval time.Duration, jitter float64, rnd clock.Rand) time.Duration {
	return time.Duration(float64(interval) * (1 + jitter*(2*rnd.Float64()-1)))
}

func parseStatusRange(s string) (statusRange, error) {
	loS, hiS, ranged := strings.Cut(s, "-")
	if !ranged {
		hiS = loS
	}
	lo, lerr := parseStatusCode(loS)
	hi, herr := parseStatusCode(hiS)
	if lerr != nil || herr != nil || lo > hi {
		return statusRange{}, fmt.Errorf("health: invalid expected status %q", s)
	}
	return statusRange{lo: lo, hi: hi}, nil
}

func parseStatusCode(s string) (int, error) {
	if len(s) != 3 {
		return 0, errors.New("health: status code must have three digits")
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	if n < 100 || n > 599 {
		return 0, errors.New("health: status code out of range")
	}
	return n, nil
}
