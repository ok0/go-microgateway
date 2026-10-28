package upstream

import (
	"time"

	"github.com/ok0/lychgate/internal/breaker"
)

// ActiveCheckStatus is the observable state of active health checking for one host.
type ActiveCheckStatus struct {
	Status              string     `json:"status"`
	ConsecutivePasses   int        `json:"consecutive_passes"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	LastResult          *time.Time `json:"last_result"`
}

// BreakerStatus is the observable state of one circuit breaker.
// Requests, Failures and ConsecutiveFailures describe the current closed-state rolling window. Every transition starts a fresh window, so they read 0 while open and half-open; why a breaker opened is reported by the transition log (reason) and the transitions metric.
// HalfOpenInSeconds is the remaining open time (0 unless open); ProbesInFlight shows half-open progress.
type BreakerStatus struct {
	State               string  `json:"state"`
	Requests            int64   `json:"requests"`
	Failures            int64   `json:"failures"`
	ConsecutiveFailures int     `json:"consecutive_failures"`
	HalfOpenInSeconds   float64 `json:"half_open_in_seconds"`
	ProbesInFlight      int     `json:"probes_in_flight"`
	InFlight            int64   `json:"in_flight"`
}

// HostStatus is the observable state of one upstream host.
type HostStatus struct {
	URL                      string             `json:"url"`
	Weight                   int                `json:"weight"`
	Healthy                  bool               `json:"healthy"`
	Ejected                  bool               `json:"ejected"`
	EjectionRemainingSeconds float64            `json:"ejection_remaining_seconds"`
	EjectionCount            int                `json:"ejection_count"`
	ConsecutiveFailures      int                `json:"consecutive_failures"`
	ActiveCheck              *ActiveCheckStatus `json:"active_check"`
	BreakerState             string             `json:"breaker_state"`
	Breaker                  *BreakerStatus     `json:"breaker,omitempty"`
	InFlight                 int64              `json:"in_flight"`
}

// Status is the observable state of one upstream.
type Status struct {
	Name         string         `json:"name"`
	Algorithm    string         `json:"algorithm"`
	PanicMode    bool           `json:"panic_mode"`
	BreakerState string         `json:"breaker_state,omitempty"`
	Breaker      *BreakerStatus `json:"breaker,omitempty"`
	Hosts        []HostStatus   `json:"hosts"`
}

// Status reports the upstream and its hosts in current-set order.
func (u *Upstream) Status() Status {
	st := Status{
		Name:         u.Name,
		Algorithm:    u.Algorithm,
		PanicMode:    u.pool.panic.Load(),
		BreakerState: "closed",
		Hosts:        []HostStatus{},
	}
	if u.Breaker != nil {
		st.Breaker = breakerStatus(u.Breaker)
		st.BreakerState = st.Breaker.State
	}
	s := u.set.Load()
	if s == nil {
		return st
	}
	st.Hosts = make([]HostStatus, 0, len(s.hosts))
	for _, h := range s.hosts {
		st.Hosts = append(st.Hosts, h.status())
	}
	return st
}

func breakerStatus(b *breaker.Breaker) *BreakerStatus {
	snap := b.Snapshot()
	return &BreakerStatus{
		State:               snap.State.String(),
		Requests:            snap.Requests,
		Failures:            snap.Failures,
		ConsecutiveFailures: snap.ConsecutiveFailures,
		HalfOpenInSeconds:   snap.UntilHalfOpen.Seconds(),
		ProbesInFlight:      snap.Probes,
		InFlight:            snap.InFlight,
	}
}

func (h *Host) status() HostStatus {
	ejected := h.Ejected()
	hs := HostStatus{
		URL:          h.display,
		Weight:       h.Weight,
		Healthy:      !ejected && h.ActiveHealthy(),
		Ejected:      ejected,
		BreakerState: "closed",
		InFlight:     h.InFlight(),
	}
	if h.Breaker != nil {
		hs.Breaker = breakerStatus(h.Breaker)
		hs.BreakerState = hs.Breaker.State
	}
	if h.passive != nil {
		ps := h.passive.Status()
		hs.EjectionRemainingSeconds = ps.Remaining.Seconds()
		hs.EjectionCount = ps.EjectionCount
		hs.ConsecutiveFailures = ps.ConsecutiveFailures
	}
	if h.active != nil {
		snap := h.active.Snapshot()
		ac := &ActiveCheckStatus{
			Status:              snap.State,
			ConsecutivePasses:   snap.ConsecutivePasses,
			ConsecutiveFailures: snap.ConsecutiveFailures,
		}
		if !snap.LastResult.IsZero() {
			last := snap.LastResult
			ac.LastResult = &last
		}
		hs.ActiveCheck = ac
	}
	return hs
}
