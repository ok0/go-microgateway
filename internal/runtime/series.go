package runtime

import (
	"github.com/ok0/lychgate/internal/metrics"
	"github.com/ok0/lychgate/internal/upstream"
)

// Series are the request and upstream metric families recorded by a snapshot.
type Series struct {
	Requests *metrics.CounterVec
	Duration *metrics.HistogramVec
	Attempts *metrics.CounterVec
	Upstream *upstream.Metrics
}

// RegisterSeries registers (or, for an identical earlier registration, reuses) the series in r.
func RegisterSeries(r *metrics.Registry) (*Series, error) {
	requests, err := r.Counter("lychgate_requests_total", "Requests handled by the gateway, by route, method and status code.", "route", "method", "code")
	if err != nil {
		return nil, err
	}
	duration, err := r.Histogram("lychgate_request_duration_seconds", "Time from request receipt to handler completion, by route.", metrics.DefaultBuckets(), "route")
	if err != nil {
		return nil, err
	}
	attempts, err := r.Counter("lychgate_upstream_attempts_total", "Upstream attempts, by upstream, host and outcome.", "upstream", "host", "outcome")
	if err != nil {
		return nil, err
	}
	um := &upstream.Metrics{Attempts: attempts}
	if um.Ejections, err = r.Counter("lychgate_upstream_ejections_total", "Upstream host ejections.", "upstream", "host"); err != nil {
		return nil, err
	}
	if um.EjectionsRefused, err = r.Counter("lychgate_upstream_ejections_refused_total", "Ejections refused by max_ejection_percent.", "upstream"); err != nil {
		return nil, err
	}
	if um.DNSRefresh, err = r.Counter("lychgate_upstream_dns_refresh_total", "Upstream DNS refreshes by result.", "upstream", "result"); err != nil {
		return nil, err
	}
	if um.ActiveChecks, err = r.Counter("lychgate_upstream_active_checks_total", "Upstream active health probes by result.", "upstream", "host", "result"); err != nil {
		return nil, err
	}
	if um.HostHealthy, err = r.Gauge("lychgate_upstream_host_healthy", "Whether an upstream host is currently healthy.", "upstream", "host"); err != nil {
		return nil, err
	}
	if um.PanicMode, err = r.Gauge("lychgate_upstream_panic_mode", "Whether an upstream is in panic mode.", "upstream"); err != nil {
		return nil, err
	}
	return &Series{Requests: requests, Duration: duration, Attempts: attempts, Upstream: um}, nil
}
