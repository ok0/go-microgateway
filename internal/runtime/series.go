package runtime

import (
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/upstream"
)

// Series are the request and upstream metric families recorded by a snapshot.
type Series struct {
	Requests             *metrics.CounterVec
	Duration             *metrics.HistogramVec
	Attempts             *metrics.CounterVec
	Upstream             *upstream.Metrics
	Retries              *metrics.CounterVec
	RetryBudgetExhausted *metrics.CounterVec
	AuthFailures         *metrics.CounterVec
	JWKSRefreshes        *metrics.CounterVec
	RateLimited          *metrics.CounterVec
	RateLimitKeys        *metrics.GaugeVec
	// StoreAvailable reports whether a store's guard admits calls.
	StoreAvailable *metrics.GaugeVec
	// StoreErrors counts store calls that failed or timed out.
	StoreErrors *metrics.CounterVec
	// StoreFallback counts rate-limit decisions made by a failure policy.
	StoreFallback *metrics.CounterVec
}

// RegisterSeries registers (or, for an identical earlier registration, reuses) the series in r.
func RegisterSeries(r *metrics.Registry) (*Series, error) {
	requests, err := r.Counter("microgateway_requests_total", "Requests handled by the gateway, by route, method and status code.", "route", "method", "code")
	if err != nil {
		return nil, err
	}
	duration, err := r.Histogram("microgateway_request_duration_seconds", "Time from request receipt to handler completion, by route.", metrics.DefaultBuckets(), "route")
	if err != nil {
		return nil, err
	}
	attempts, err := r.Counter("microgateway_upstream_attempts_total", "Upstream attempts, by upstream, host and outcome.", "upstream", "host", "outcome")
	if err != nil {
		return nil, err
	}
	um := &upstream.Metrics{Attempts: attempts}
	if um.Ejections, err = r.Counter("microgateway_upstream_ejections_total", "Upstream host ejections.", "upstream", "host"); err != nil {
		return nil, err
	}
	if um.EjectionsRefused, err = r.Counter("microgateway_upstream_ejections_refused_total", "Ejections refused by max_ejection_percent.", "upstream"); err != nil {
		return nil, err
	}
	if um.DNSRefresh, err = r.Counter("microgateway_upstream_dns_refresh_total", "Upstream DNS refreshes by result.", "upstream", "result"); err != nil {
		return nil, err
	}
	if um.ActiveChecks, err = r.Counter("microgateway_upstream_active_checks_total", "Upstream active health probes by result.", "upstream", "host", "result"); err != nil {
		return nil, err
	}
	if um.HostHealthy, err = r.Gauge("microgateway_upstream_host_healthy", "Whether an upstream host is currently healthy.", "upstream", "host"); err != nil {
		return nil, err
	}
	if um.PanicMode, err = r.Gauge("microgateway_upstream_panic_mode", "Whether an upstream is in panic mode.", "upstream"); err != nil {
		return nil, err
	}
	if um.BreakerState, err = r.Gauge("microgateway_circuit_breaker_state", "Circuit breaker state (0 closed, 1 half-open, 2 open); host is empty for the upstream aggregate.", "upstream", "host"); err != nil {
		return nil, err
	}
	if um.BreakerTransitions, err = r.Counter("microgateway_circuit_breaker_transitions_total", "Circuit breaker state transitions.", "upstream", "host", "from", "to"); err != nil {
		return nil, err
	}
	if um.BreakerRejected, err = r.Counter("microgateway_circuit_breaker_rejected_total", "Requests or attempts rejected by an open circuit breaker.", "upstream", "host"); err != nil {
		return nil, err
	}
	retries, err := r.Counter("microgateway_retries_total", "Retry attempts sent, by route.", "route")
	if err != nil {
		return nil, err
	}
	exhausted, err := r.Counter("microgateway_retry_budget_exhausted_total", "Retries skipped because the route retry budget was exhausted.", "route")
	if err != nil {
		return nil, err
	}
	authFailures, err := r.Counter("microgateway_auth_failures_total", "Requests rejected by authentication or authorization, by route and reason.", "route", "reason")
	if err != nil {
		return nil, err
	}
	jwksRefreshes, err := r.Counter("microgateway_auth_jwks_refreshes_total", "JWKS fetch attempts, by provider and result.", "provider", "result")
	if err != nil {
		return nil, err
	}
	rateLimited, err := r.Counter("microgateway_rate_limited_total", "Requests rejected with 429 by the rate_limit stage, by route.", "route")
	if err != nil {
		return nil, err
	}
	rateLimitKeys, err := r.Gauge("microgateway_ratelimit_keys", "Keys held by the route's in-memory rate-limit store.", "route")
	if err != nil {
		return nil, err
	}
	storeAvailable, err := r.Gauge("microgateway_store_available", "Whether the store's guard admits calls (1) or is open/probing (0).", "store")
	if err != nil {
		return nil, err
	}
	storeErrors, err := r.Counter("microgateway_store_errors_total", "Store calls that failed or timed out, by store and operation.", "store", "op")
	if err != nil {
		return nil, err
	}
	storeFallback, err := r.Counter("microgateway_ratelimit_store_fallback_total", "Rate-limit decisions made by the failure policy because the store was unavailable.", "route", "policy")
	if err != nil {
		return nil, err
	}
	return &Series{Requests: requests, Duration: duration, Attempts: attempts, Upstream: um, Retries: retries, RetryBudgetExhausted: exhausted, AuthFailures: authFailures, JWKSRefreshes: jwksRefreshes, RateLimited: rateLimited, RateLimitKeys: rateLimitKeys, StoreAvailable: storeAvailable, StoreErrors: storeErrors, StoreFallback: storeFallback}, nil
}
