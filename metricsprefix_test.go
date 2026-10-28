package microgateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const metricPrefix = "microgateway_"

// checkMetricPrefix fails t unless every family and series in body is named with metricPrefix.
func checkMetricPrefix(t *testing.T, body string) {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		if line == "" {
			continue
		}
		var name string
		if strings.HasPrefix(line, "# HELP ") || strings.HasPrefix(line, "# TYPE ") {
			name = strings.Fields(line)[2]
		} else {
			name = line
		}
		if !strings.HasPrefix(name, metricPrefix) {
			t.Errorf("metric line %q does not start with %q", line, metricPrefix)
		}
	}
}

func TestMetricsPrefix(t *testing.T) {
	be := backend(t, "m")
	g := gwWith(t, routeDoc(be.URL, "    rate_limit:\n      rate: 1\n      per: 1h\n"))

	for _, want := range []int{http.StatusOK, http.StatusTooManyRequests} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "192.0.2.1:1"
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Fatalf("status = %d, want %d", rec.Code, want)
		}
	}

	if err := g.ReloadYAML([]byte(routeDoc(be.URL, "    rate_limit:\n      rate: 2\n      per: 1h\n"))); err != nil {
		t.Fatal(err)
	}

	body := metricsText(t, g)
	checkMetricPrefix(t, body)

	for _, name := range []string{
		"microgateway_requests_total",
		"microgateway_request_duration_seconds",
		"microgateway_upstream_attempts_total",
		"microgateway_upstream_ejections_total",
		"microgateway_upstream_ejections_refused_total",
		"microgateway_upstream_dns_refresh_total",
		"microgateway_upstream_active_checks_total",
		"microgateway_upstream_host_healthy",
		"microgateway_upstream_panic_mode",
		"microgateway_circuit_breaker_state",
		"microgateway_circuit_breaker_transitions_total",
		"microgateway_circuit_breaker_rejected_total",
		"microgateway_retries_total",
		"microgateway_retry_budget_exhausted_total",
		"microgateway_auth_failures_total",
		"microgateway_auth_jwks_refreshes_total",
		"microgateway_rate_limited_total",
		"microgateway_ratelimit_keys",
		"microgateway_store_available",
		"microgateway_store_errors_total",
		"microgateway_ratelimit_store_fallback_total",
		"microgateway_config_reloads_total",
		"microgateway_config_last_reload_success_timestamp_seconds",
	} {
		if !strings.Contains(body, "# TYPE "+name+" ") {
			t.Errorf("metrics lack family %q", name)
		}
	}

	for _, line := range []string{
		`microgateway_requests_total{route="all",method="GET",code="200"} 1`,
		`microgateway_requests_total{route="all",method="GET",code="429"} 1`,
		`microgateway_rate_limited_total{route="all"} 1`,
		`microgateway_config_reloads_total{result="success"} 1`,
	} {
		if !metricsHasLine(body, line) {
			t.Errorf("metrics lack line %q\n%s", line, body)
		}
	}
}
