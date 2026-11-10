package admin_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/internal/admin"
	"github.com/ok0/go-microgateway/internal/metrics"
)

type probeSource struct {
	ready, metricsOn bool
	reg              *metrics.Registry
}

func (p *probeSource) Ready() bool                { return p.ready }
func (p *probeSource) ProbeMetrics() bool         { return p.metricsOn }
func (p *probeSource) Metrics() *metrics.Registry { return p.reg }

func TestProbeLiveReady(t *testing.T) {
	src := &probeSource{ready: true, reg: newFake(t).reg}
	h := admin.ProbeHandler(src)

	rec := serve(t, h, http.MethodGet, "/livez")
	if rec.Code != http.StatusOK || decodeStatus(t, rec) != "ok" {
		t.Fatalf("/livez: status = %d body = %q", rec.Code, rec.Body.String())
	}
	rec = serve(t, h, http.MethodGet, "/readyz")
	if rec.Code != http.StatusOK || decodeStatus(t, rec) != "ready" {
		t.Fatalf("/readyz ready: status = %d body = %q", rec.Code, rec.Body.String())
	}
	src.ready = false
	rec = serve(t, h, http.MethodGet, "/readyz")
	if rec.Code != http.StatusServiceUnavailable || decodeStatus(t, rec) != "not_ready" {
		t.Fatalf("/readyz not ready: status = %d body = %q", rec.Code, rec.Body.String())
	}
	if rec := serve(t, h, http.MethodHead, "/livez"); rec.Code != http.StatusOK {
		t.Errorf("HEAD /livez status = %d, want 200", rec.Code)
	}
}

func TestProbeMetricsOptIn(t *testing.T) {
	src := &probeSource{ready: true, reg: newFake(t).reg}
	h := admin.ProbeHandler(src)

	rec := serve(t, h, http.MethodGet, "/metrics")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("/metrics with metrics off: status = %d, want 404", rec.Code)
	}
	if code, _ := errorBody(t, rec); code != "not_found" {
		t.Errorf("/metrics with metrics off: code = %q, want not_found", code)
	}

	src.metricsOn = true
	rec = serve(t, h, http.MethodGet, "/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics with metrics on: status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != metrics.ContentType {
		t.Errorf("/metrics Content-Type = %q, want %q", ct, metrics.ContentType)
	}
	if !strings.Contains(rec.Body.String(), "admin_test_total") {
		t.Errorf("/metrics body lacks counter: %s", rec.Body.String())
	}

	src.metricsOn = false
	if rec := serve(t, h, http.MethodGet, "/metrics"); rec.Code != http.StatusNotFound {
		t.Errorf("/metrics after metrics off: status = %d, want 404", rec.Code)
	}
}

func TestProbeNoManagementRoutes(t *testing.T) {
	paths := []string{"/config", "/upstreams", "/cache/purge", "/", "/livez/x"}
	methods := []string{http.MethodGet, http.MethodPost}
	for _, metricsOn := range []bool{false, true} {
		src := &probeSource{ready: true, metricsOn: metricsOn, reg: newFake(t).reg}
		h := admin.ProbeHandler(src)
		for _, path := range paths {
			for _, method := range methods {
				for _, auth := range []string{"", "Bearer x"} {
					rec := serveAuth(t, h, method, path, auth)
					if rec.Code != http.StatusNotFound {
						t.Errorf("metrics=%v %s %s auth=%q: status = %d, want 404", metricsOn, method, path, auth, rec.Code)
						continue
					}
					if code, _ := errorBody(t, rec); code != "not_found" {
						t.Errorf("metrics=%v %s %s: code = %q, want not_found", metricsOn, method, path, code)
					}
				}
			}
		}
	}
}

func TestProbeMethods(t *testing.T) {
	h := admin.ProbeHandler(&probeSource{ready: true, reg: newFake(t).reg})
	for _, path := range []string{"/livez", "/readyz"} {
		rec := serve(t, h, http.MethodPost, path)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s status = %d, want 405", path, rec.Code)
		}
		if allow := rec.Header().Get("Allow"); allow != "GET, HEAD" {
			t.Errorf("POST %s Allow = %q, want %q", path, allow, "GET, HEAD")
		}
	}
}
