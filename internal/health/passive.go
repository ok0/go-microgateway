// Package health tracks per-upstream host health. Passive ejection removes a host after
// observed request failures, and active checking probes hosts on a schedule. State is
// per Ejector and per Checker instance. Ejection expiry is evaluated lazily when a host
// is consulted, so no timers are involved. The ejection count drops by one for every
// full max_ejection_time elapsed since the last ejection ended (by expiry or by clear).
package health

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
)

type Events struct {
	Ejected  func(key string, d time.Duration, count int)
	Restored func(key, reason string)
	Refused  func(key string)
}

type Member struct {
	Key    string
	Weight int
}

type PassiveStatus struct {
	Ejected             bool
	Remaining           time.Duration
	EjectionCount       int
	ConsecutiveFailures int
}

type Ejector struct {
	mu         sync.Mutex
	threshold  int
	base       time.Duration
	maxEject   time.Duration
	maxPercent int
	clk        clock.Clock
	ev         Events
	members    map[string]*Record
}

type Record struct {
	e           *Ejector
	key         string
	consecutive atomic.Int64
	until       atomic.Int64
	count       int
	lastEnd     time.Time
	weight      int
}

func NewEjector(cfg config.PassiveHealth, clk clock.Clock, ev Events) *Ejector {
	e := &Ejector{
		threshold:  cfg.ConsecutiveFailures,
		base:       cfg.BaseEjectionTime.Std(),
		maxEject:   cfg.MaxEjectionTime.Std(),
		maxPercent: 50,
		clk:        clk,
		ev:         ev,
		members:    map[string]*Record{},
	}
	if e.threshold == 0 {
		e.threshold = 5
	}
	if e.base == 0 {
		e.base = 30 * time.Second
	}
	if e.maxEject == 0 {
		e.maxEject = 300 * time.Second
	}
	if e.maxEject < e.base {
		e.maxEject = e.base
	}
	if cfg.MaxEjectionPercent != nil {
		e.maxPercent = *cfg.MaxEjectionPercent
	}
	return e
}

func (e *Ejector) SetMembers(ms []Member) {
	e.mu.Lock()
	defer e.mu.Unlock()
	next := make(map[string]*Record, len(ms))
	for _, m := range ms {
		if r, ok := next[m.Key]; ok {
			r.weight = m.Weight
			continue
		}
		r, ok := e.members[m.Key]
		if !ok {
			r = &Record{e: e, key: m.Key}
		}
		r.weight = m.Weight
		next[m.Key] = r
	}
	e.members = next
}

func (e *Ejector) Record(key string) *Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.members[key]
}

func (r *Record) Observe(o Outcome) {
	if r.Ejected() {
		return
	}
	switch o {
	case Success:
		r.consecutive.Store(0)
	case Failure:
		if r.consecutive.Add(1) >= int64(r.e.threshold) {
			r.e.tryEject(r)
		}
	}
}

func (r *Record) Ejected() bool {
	u := r.until.Load()
	if u == 0 {
		return false
	}
	if r.e.clk.Now().UnixNano() < u {
		return true
	}
	if r.until.CompareAndSwap(u, 0) && r.e.ev.Restored != nil {
		r.e.ev.Restored(r.key, "expired")
	}
	return false
}

func (r *Record) Clear() {
	e := r.e
	e.mu.Lock()
	now := e.clk.Now()
	u := r.until.Load()
	cleared := u != 0 && r.until.CompareAndSwap(u, 0)
	if cleared {
		if now.UnixNano() < u {
			r.lastEnd = now
		}
		r.consecutive.Store(0)
	}
	e.mu.Unlock()
	if cleared && e.ev.Restored != nil {
		e.ev.Restored(r.key, "active_check")
	}
}

func (r *Record) Status() PassiveStatus {
	e := r.e
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.clk.Now()
	st := PassiveStatus{
		ConsecutiveFailures: int(r.consecutive.Load()),
		EjectionCount:       r.count,
	}
	u := r.until.Load()
	rem := time.Duration(u - now.UnixNano())
	if u != 0 && rem > 0 {
		st.Ejected = true
		st.Remaining = rem
		return st
	}
	if r.count > 0 && !r.lastEnd.IsZero() {
		st.EjectionCount = max(0, r.count-int(now.Sub(r.lastEnd)/e.maxEject))
	}
	return st
}

func (e *Ejector) tryEject(r *Record) {
	e.mu.Lock()
	if e.members[r.key] != r {
		e.mu.Unlock()
		return
	}
	now := e.clk.Now()
	nowN := now.UnixNano()
	if u := r.until.Load(); u != 0 && nowN < u {
		e.mu.Unlock()
		return
	}
	if r.consecutive.Load() < int64(e.threshold) {
		e.mu.Unlock()
		return
	}
	if e.maxPercent == 0 {
		e.mu.Unlock()
		return
	}
	n, ejected := 0, 0
	for _, m := range e.members {
		if m.weight <= 0 {
			continue
		}
		n++
		if m.until.Load() > nowN {
			ejected++
		}
	}
	allowed := max(1, n*e.maxPercent/100)
	if ejected+1 > allowed {
		e.mu.Unlock()
		if e.ev.Refused != nil {
			e.ev.Refused(r.key)
		}
		return
	}
	if r.count > 0 && !r.lastEnd.IsZero() {
		r.count = max(0, r.count-int(now.Sub(r.lastEnd)/e.maxEject))
	}
	r.count++
	d := min(e.base*time.Duration(r.count), e.maxEject)
	staleExpired := r.until.Load() != 0
	end := now.Add(d)
	r.until.Store(end.UnixNano())
	r.lastEnd = end
	r.consecutive.Store(0)
	count := r.count
	e.mu.Unlock()
	if staleExpired && e.ev.Restored != nil {
		e.ev.Restored(r.key, "expired")
	}
	if e.ev.Ejected != nil {
		e.ev.Ejected(r.key, d, count)
	}
}
