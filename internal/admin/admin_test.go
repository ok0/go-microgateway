package admin_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/admin"
	"github.com/ok0/go-microgateway/internal/cache"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/upstream"
)

const (
	testSecret  = "s3cret"
	testKeyHash = "sha256:" + "0000000000000000000000000000000000000000000000000000000000000000"
)

type fakeSource struct {
	ready    bool
	cfg      *config.Config
	statuses []upstream.Status
	reg      *metrics.Registry
}

func (f *fakeSource) Ready() bool                    { return f.ready }
func (f *fakeSource) RedactedConfig() *config.Config { return f.cfg.Redacted() }
func (f *fakeSource) Upstreams() []upstream.Status   { return f.statuses }
func (f *fakeSource) Metrics() *metrics.Registry     { return f.reg }
func (f *fakeSource) PurgeAccess() admin.PurgeAccess { return admin.PurgeAccess{} }
func (f *fakeSource) Purge(ctx context.Context, req cache.PurgeRequest, keyID string) (cache.PurgeResult, error) {
	return cache.PurgeResult{}, nil
}

func newFake(t *testing.T) *fakeSource {
	t.Helper()
	reg := metrics.NewRegistry()
	c, err := reg.Counter("admin_test_total", "A test counter.")
	if err != nil {
		t.Fatal(err)
	}
	c.With().Inc()
	cfg := &config.Config{
		Version: 1,
		Global: config.Global{
			DrainTimeout: config.Duration(30 * time.Second),
		},
		Auth: config.Auth{
			JWT: []config.JWTProvider{{
				Keys: []config.JWTKey{{Secret: config.NewSecret(testSecret)}},
			}},
			APIKeys: []config.APIKeyProvider{{
				Keys: []config.APIKey{{Hash: config.NewSecret(testKeyHash)}},
			}},
		},
	}
	return &fakeSource{
		ready: true,
		cfg:   cfg,
		statuses: []upstream.Status{{
			Name: "api",
			Hosts: []upstream.HostStatus{{
				URL:          "http://10.0.0.1:8080",
				Weight:       2,
				Healthy:      true,
				Ejected:      false,
				BreakerState: "closed",
				InFlight:     3,
			}},
		}},
		reg: reg,
	}
}

func serve(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("%s %s: Cache-Control = %q, want no-store", method, path, got)
	}
	return rec
}

func decodeStatus(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode status body %q: %v", rec.Body.String(), err)
	}
	return body.Status
}

func TestEndpoints(t *testing.T) {
	src := newFake(t)
	h := admin.Handler(src, nil)

	if rec := serve(t, h, http.MethodGet, "/livez"); rec.Code != http.StatusOK {
		t.Fatalf("/livez status = %d, want 200", rec.Code)
	}

	rec := serve(t, h, http.MethodGet, "/readyz")
	if rec.Code != http.StatusOK || decodeStatus(t, rec) != "ready" {
		t.Fatalf("/readyz ready: status = %d body = %q", rec.Code, rec.Body.String())
	}
	src.ready = false
	rec = serve(t, h, http.MethodGet, "/readyz")
	if rec.Code != http.StatusServiceUnavailable || decodeStatus(t, rec) != "not_ready" {
		t.Fatalf("/readyz not ready: status = %d body = %q", rec.Code, rec.Body.String())
	}

	rec = serve(t, h, http.MethodGet, "/config")
	if rec.Code != http.StatusOK {
		t.Fatalf("/config status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "REDACTED") {
		t.Errorf("/config body lacks REDACTED: %s", body)
	}
	if strings.Contains(body, testSecret) {
		t.Errorf("/config body leaks JWT secret: %s", body)
	}
	if strings.Contains(body, testKeyHash) {
		t.Errorf("/config body leaks API key hash: %s", body)
	}
	if !strings.Contains(body, `"drain_timeout":"30s"`) {
		t.Errorf("/config body lacks string duration: %s", body)
	}
	if !strings.Contains(body, `"auth"`) || !strings.Contains(body, `"global"`) {
		t.Errorf("/config body lacks snake_case top-level keys: %s", body)
	}

	rec = serve(t, h, http.MethodGet, "/upstreams")
	if rec.Code != http.StatusOK {
		t.Fatalf("/upstreams status = %d, want 200", rec.Code)
	}
	var got []upstream.Status
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode /upstreams: %v", err)
	}
	if !reflect.DeepEqual(got, src.statuses) {
		t.Errorf("/upstreams = %+v, want %+v", got, src.statuses)
	}
	var raw []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode /upstreams raw: %v", err)
	}
	hosts, _ := raw[0]["hosts"].([]any)
	if len(hosts) != 1 {
		t.Fatalf("/upstreams hosts = %v, want one host", raw[0]["hosts"])
	}
	host, _ := hosts[0].(map[string]any)
	for _, key := range []string{"url", "weight", "healthy", "ejected", "breaker_state", "in_flight"} {
		if _, ok := host[key]; !ok {
			t.Errorf("/upstreams host lacks key %q: %v", key, host)
		}
	}

	rec = serve(t, h, http.MethodGet, "/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != metrics.ContentType {
		t.Errorf("/metrics Content-Type = %q, want %q", ct, metrics.ContentType)
	}
	if !strings.Contains(rec.Body.String(), "admin_test_total 1") {
		t.Errorf("/metrics body lacks counter: %s", rec.Body.String())
	}
}

func TestMethods(t *testing.T) {
	h := admin.Handler(newFake(t), nil)

	if rec := serve(t, h, http.MethodHead, "/livez"); rec.Code != http.StatusOK {
		t.Errorf("HEAD /livez status = %d, want 200", rec.Code)
	}

	rec := serve(t, h, http.MethodPost, "/metrics")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /metrics status = %d, want 405", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); allow != "GET, HEAD" {
		t.Errorf("POST /metrics Allow = %q, want %q", allow, "GET, HEAD")
	}
	var errBody struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &errBody); err != nil {
		t.Fatalf("decode 405 body: %v", err)
	}
	if errBody.Error.Code != "method_not_allowed" {
		t.Errorf("POST /metrics code = %q, want method_not_allowed", errBody.Error.Code)
	}

	rec = serve(t, h, http.MethodGet, "/nope")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /nope status = %d, want 404", rec.Code)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &errBody); err != nil {
		t.Fatalf("decode 404 body: %v", err)
	}
	if errBody.Error.Code != "not_found" {
		t.Errorf("GET /nope code = %q, want not_found", errBody.Error.Code)
	}
}
