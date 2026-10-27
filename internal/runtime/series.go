package runtime

import "github.com/ok0/lychgate/internal/metrics"

// Series are the request and upstream metric families recorded by a snapshot.
type Series struct {
	Requests *metrics.CounterVec
	Duration *metrics.HistogramVec
	Attempts *metrics.CounterVec
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
	return &Series{Requests: requests, Duration: duration, Attempts: attempts}, nil
}
