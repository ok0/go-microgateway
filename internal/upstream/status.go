package upstream

import "time"

// ActiveCheckStatus is the observable state of active health checking for one host.
type ActiveCheckStatus struct {
	Status              string     `json:"status"`
	ConsecutivePasses   int        `json:"consecutive_passes"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	LastResult          *time.Time `json:"last_result"`
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
	InFlight                 int64              `json:"in_flight"`
}

// Status is the observable state of one upstream.
type Status struct {
	Name      string       `json:"name"`
	Algorithm string       `json:"algorithm"`
	PanicMode bool         `json:"panic_mode"`
	Hosts     []HostStatus `json:"hosts"`
}

// Status reports the upstream and its hosts in current-set order.
func (u *Upstream) Status() Status {
	st := Status{
		Name:      u.Name,
		Algorithm: u.Algorithm,
		PanicMode: u.pool.panic.Load(),
		Hosts:     []HostStatus{},
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
