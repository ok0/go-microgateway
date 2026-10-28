package upstream

import (
	"log/slog"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/breaker"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/health"
)

type hostState struct{ inflight atomic.Int64 }

type pool struct {
	name         string
	mu           sync.Mutex
	owner        *Upstream
	dnsSrc       *dnsWatcher
	dnsSeq       uint64
	states       map[string]*hostState
	current      atomic.Pointer[hostSet]
	panic        atomic.Bool
	lastPanicLog atomic.Int64
	suppressed   atomic.Int64
	logger       *slog.Logger
	metrics      *Metrics
	clk          clock.Clock
	brkHosts     atomic.Pointer[hostBreakers]
	brkAgg       atomic.Pointer[breaker.Breaker]
}

func newPool(name string, d Deps) *pool {
	return &pool{
		name:    name,
		states:  map[string]*hostState{},
		logger:  d.Logger,
		metrics: d.Metrics,
		clk:     d.Clock,
	}
}

func (p *pool) ejectorEvents(maxPercent int) health.Events {
	return health.Events{
		Ejected:  p.onEjected,
		Restored: p.onRestored,
		Refused: func(key string) {
			p.onRefused(key, maxPercent)
		},
	}
}

func (p *pool) currentHost(key string) *Host {
	cur := p.current.Load()
	if cur == nil {
		return nil
	}
	return cur.byKey[key]
}

func (p *pool) hostLabel(key string) string {
	if h := p.currentHost(key); h != nil {
		return h.URL.Host
	}
	if pu, err := url.Parse(key); err == nil {
		return pu.Host
	}
	return config.RedactURL(key)
}

func (p *pool) setHealthGauge(h *Host) {
	v := 0.0
	if !h.Ejected() && h.ActiveHealthy() {
		v = 1
	}
	p.metrics.HostHealthy.With(p.name, h.URL.Host).Set(v)
}

func (p *pool) prefill(u *Upstream, s *hostSet) {
	panicVal := 0.0
	if p.panic.Load() {
		panicVal = 1
	}
	p.metrics.PanicMode.With(p.name).Set(panicVal)
	p.metrics.EjectionsRefused.With(p.name)
	for _, h := range s.hosts {
		host := h.URL.Host
		for _, o := range [...]string{"success", "failure", "ignored"} {
			p.metrics.Attempts.With(p.name, host, o)
		}
		p.metrics.Ejections.With(p.name, host)
		p.setHealthGauge(h)
		if h.active != nil {
			for _, r := range [...]string{"pass", "fail"} {
				p.metrics.ActiveChecks.With(p.name, host, r)
			}
		}
		if h.Breaker != nil {
			p.prefillBreaker(host, h.Breaker)
		}
	}
	if u.Breaker != nil {
		p.prefillBreaker("", u.Breaker)
	}
	if u.dns != nil {
		for _, r := range [...]string{"success", "error", "empty"} {
			p.metrics.DNSRefresh.With(p.name, r)
		}
	}
}

var breakerTransitionPairs = [...][2]breaker.State{
	{breaker.Closed, breaker.Open},
	{breaker.Open, breaker.HalfOpen},
	{breaker.HalfOpen, breaker.Open},
	{breaker.HalfOpen, breaker.Closed},
}

func (p *pool) prefillBreaker(host string, b *breaker.Breaker) {
	p.metrics.BreakerState.With(p.name, host).Set(float64(b.State()))
	for _, pair := range breakerTransitionPairs {
		p.metrics.BreakerTransitions.With(p.name, host, pair[0].String(), pair[1].String())
	}
	p.metrics.BreakerRejected.With(p.name, host)
}

func (p *pool) onHostBreaker(hb *hostBreakers, key string, from, to breaker.State, reason breaker.Reason) {
	host := p.hostLabel(key)
	p.logBreaker(host, "host", from, to, reason)
	if p.brkHosts.Load() == hb && p.currentHost(key) != nil {
		p.recordBreakerTransition(host, from, to)
	}
}

func (p *pool) onAggregateBreaker(b *breaker.Breaker, from, to breaker.State, reason breaker.Reason) {
	p.logBreaker("", "aggregate", from, to, reason)
	if p.brkAgg.Load() == b {
		p.recordBreakerTransition("", from, to)
	}
}

func (p *pool) recordBreakerTransition(host string, from, to breaker.State) {
	p.metrics.BreakerTransitions.With(p.name, host, from.String(), to.String()).Inc()
	p.metrics.BreakerState.With(p.name, host).Set(float64(to))
}

func (p *pool) logBreaker(host, scope string, from, to breaker.State, reason breaker.Reason) {
	attrs := []any{"upstream", p.name, "host", host, "scope", scope, "from", from.String(), "to", to.String(), "reason", string(reason)}
	if to == breaker.Open {
		p.logger.Warn("circuit breaker state changed", attrs...)
		return
	}
	p.logger.Info("circuit breaker state changed", attrs...)
}

func (p *pool) onEjected(key string, d time.Duration, count int) {
	host := p.hostLabel(key)
	p.metrics.Ejections.With(p.name, host).Inc()
	p.logger.Warn("upstream host ejected", "upstream", p.name, "host", host, "duration", d, "ejection_count", count)
	if p.currentHost(key) != nil {
		p.metrics.HostHealthy.With(p.name, host).Set(0)
	}
}

func (p *pool) onRestored(key, reason string) {
	p.logger.Info("upstream host ejection ended", "upstream", p.name, "host", p.hostLabel(key), "reason", reason)
	if h := p.currentHost(key); h != nil {
		p.setHealthGauge(h)
	}
}

func (p *pool) onRefused(key string, maxPercent int) {
	p.metrics.EjectionsRefused.With(p.name).Inc()
	p.logger.Debug("upstream host ejection refused", "upstream", p.name, "host", p.hostLabel(key), "max_ejection_percent", maxPercent)
}

func (p *pool) onProbe(key string, pass bool, err error) {
	host := p.hostLabel(key)
	result := "fail"
	if pass {
		result = "pass"
	}
	p.metrics.ActiveChecks.With(p.name, host, result).Inc()
	p.logger.Debug("active health probe", "upstream", p.name, "host", host, "pass", pass, "error", err)
}

func (p *pool) onActiveChange(src *health.Checker, key string, healthy bool, st health.ActiveStatus) {
	host := p.hostLabel(key)
	if healthy {
		p.logger.Info("upstream host passed active health check", "upstream", p.name, "host", host, "consecutive_passes", st.ConsecutivePasses)
	} else {
		p.logger.Warn("upstream host failed active health check", "upstream", p.name, "host", host, "consecutive_failures", st.ConsecutiveFailures)
	}
	cur := p.current.Load()
	if cur == nil || cur.checker != src {
		return
	}
	h := cur.byKey[key]
	if h == nil {
		return
	}
	if healthy && h.passive != nil {
		h.passive.Clear()
	}
	p.setHealthGauge(h)
}

func (p *pool) enterPanic() {
	p.metrics.PanicMode.With(p.name).Set(1)
	if p.panic.CompareAndSwap(false, true) {
		p.panicLog("upstream entered panic mode")
	}
}

func (p *pool) leavePanic() {
	p.metrics.PanicMode.With(p.name).Set(0)
	if p.panic.CompareAndSwap(true, false) {
		p.panicLog("upstream left panic mode")
	}
}

func (p *pool) leaveIfPanic() {
	if p.panic.Load() {
		p.leavePanic()
	}
}

func (p *pool) panicLog(msg string) {
	now := p.clk.Now().UnixNano()
	last := p.lastPanicLog.Load()
	if last != 0 && now-last < int64(time.Second) {
		p.suppressed.Add(1)
		return
	}
	if !p.lastPanicLog.CompareAndSwap(last, now) {
		p.suppressed.Add(1)
		return
	}
	p.logger.Warn(msg, "upstream", p.name, "suppressed", p.suppressed.Swap(0))
}
