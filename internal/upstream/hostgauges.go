package upstream

import (
	"sync"

	"github.com/ok0/go-microgateway/internal/metrics"
)

// hostKey names the per-host gauge series of one host of one upstream; host is URL.Host.
type hostKey struct{ upstream, host string }

// HostGauges decides which per-host gauge series exist in one metrics registry. A series of a key exists only
// while an activated upstream instance of a live generation has that host in its host set. Every Metrics built on
// one registry shares that registry's HostGauges (see RegistryHosts).
//
// Lock order: callers may hold pool.mu; mu is a leaf and only GaugeVec calls run under it.
type HostGauges struct {
	mu      sync.Mutex
	holders map[*Upstream][]hostKey // activated, not retired instances -> deduplicated keys of their host set
	refs    map[hostKey]int         // number of holders holding each key
}

type hostGaugesKey struct{}

// RegistryHosts returns the per-host gauge reference table of r, creating it on first use.
func RegistryHosts(r *metrics.Registry) *HostGauges {
	return metrics.Attach(r, hostGaugesKey{}, func() *HostGauges { return &HostGauges{} })
}

func (m *Metrics) holdHosts(u *Upstream, hosts []*Host) {
	m.Hosts.mu.Lock()
	defer m.Hosts.mu.Unlock()
	if u.gaugesRetired {
		return
	}
	if m.Hosts.holders == nil {
		m.Hosts.holders = make(map[*Upstream][]hostKey)
		m.Hosts.refs = make(map[hostKey]int)
	}
	next := make([]hostKey, 0, len(hosts))
	seen := make(map[hostKey]bool, len(hosts))
	for _, h := range hosts {
		k := hostKey{u.Name, h.URL.Host}
		if !seen[k] {
			seen[k] = true
			next = append(next, k)
		}
	}
	for _, k := range next {
		m.Hosts.refs[k]++
	}
	var collected []hostKey
	for _, k := range m.Hosts.holders[u] {
		m.Hosts.refs[k]--
		if m.Hosts.refs[k] == 0 {
			delete(m.Hosts.refs, k)
			collected = append(collected, k)
		}
	}
	m.Hosts.holders[u] = next
	m.deleteHostsLocked(collected)
}

// RetainHosts retires every holder not in live and deletes the per-host gauge series of hosts no remaining holder holds. runtime.PruneGauges calls it when a generation drains.
func (m *Metrics) RetainHosts(live []*Upstream) {
	m.Hosts.mu.Lock()
	defer m.Hosts.mu.Unlock()
	keep := make(map[*Upstream]bool, len(live))
	for _, u := range live {
		keep[u] = true
	}
	var collected []hostKey
	for u, keys := range m.Hosts.holders {
		if keep[u] {
			continue
		}
		u.gaugesRetired = true
		for _, k := range keys {
			m.Hosts.refs[k]--
			if m.Hosts.refs[k] == 0 {
				delete(m.Hosts.refs, k)
				collected = append(collected, k)
			}
		}
		delete(m.Hosts.holders, u)
	}
	m.deleteHostsLocked(collected)
}

func (m *Metrics) deleteHostsLocked(keys []hostKey) {
	if len(keys) == 0 {
		return
	}
	set := make(map[hostKey]bool, len(keys))
	for _, k := range keys {
		set[k] = true
	}
	m.HostHealthy.DeleteFunc(func(v []string) bool { return set[hostKey{v[0], v[1]}] })
	m.BreakerState.DeleteFunc(func(v []string) bool { return v[1] != "" && set[hostKey{v[0], v[1]}] })
}

func (m *Metrics) setHostHealthy(upstream, host string, v float64) {
	m.Hosts.mu.Lock()
	defer m.Hosts.mu.Unlock()
	if m.Hosts.refs[hostKey{upstream, host}] > 0 {
		m.HostHealthy.With(upstream, host).Set(v)
	}
}

func (m *Metrics) setBreakerState(upstream, host string, v float64) {
	if host == "" {
		m.BreakerState.With(upstream, "").Set(v)
		return
	}
	m.Hosts.mu.Lock()
	defer m.Hosts.mu.Unlock()
	if m.Hosts.refs[hostKey{upstream, host}] > 0 {
		m.BreakerState.With(upstream, host).Set(v)
	}
}
