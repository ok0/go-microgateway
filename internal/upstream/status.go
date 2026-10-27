package upstream

// HostStatus is the observable state of one upstream host.
type HostStatus struct {
	URL          string `json:"url"`
	Weight       int    `json:"weight"`
	Healthy      bool   `json:"healthy"`
	Ejected      bool   `json:"ejected"`
	BreakerState string `json:"breaker_state"`
	InFlight     int64  `json:"in_flight"`
}

// Status is the observable state of one upstream.
type Status struct {
	Name  string       `json:"name"`
	Hosts []HostStatus `json:"hosts"`
}

// Snapshot reports every host in configuration order.
func (u *Upstream) Snapshot() []HostStatus {
	out := make([]HostStatus, 0, len(u.Hosts))
	for _, h := range u.Hosts {
		out = append(out, HostStatus{
			URL:          h.URL.String(),
			Weight:       h.Weight,
			Healthy:      true,
			Ejected:      false,
			BreakerState: "closed",
			InFlight:     h.InFlight(),
		})
	}
	return out
}
