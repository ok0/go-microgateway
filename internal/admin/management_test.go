package admin_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/admin"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/upstream"
)

type gateSource struct {
	*fakeSource
	admin  config.Admin
	hashes [][32]byte
	reads  int
}

func (g *gateSource) ManagementAuth() (bool, []string, [][32]byte) {
	return g.admin.ManagementAuthRequired(), []string{"a"}, g.hashes
}

func (g *gateSource) RedactedConfig() *config.Config {
	g.reads++
	return g.fakeSource.RedactedConfig()
}

func (g *gateSource) Upstreams() []upstream.Status {
	g.reads++
	return g.fakeSource.Upstreams()
}

func (g *gateSource) Metrics() *metrics.Registry {
	g.reads++
	return g.fakeSource.Metrics()
}

func newGate(t *testing.T, mode, address string) *gateSource {
	t.Helper()
	enabled := true
	a := sha256.Sum256([]byte(tokA))
	return &gateSource{
		fakeSource: newFake(t),
		admin: config.Admin{
			Enabled: &enabled,
			Address: address,
			Auth:    config.AdminAuth{Management: mode},
		},
		hashes: [][32]byte{a},
	}
}

func serveAuth(t *testing.T, h http.Handler, method, path, auth string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("%s %s: Cache-Control = %q, want no-store", method, path, got)
	}
	return rec
}

func TestManagementGating(t *testing.T) {
	modes := []string{"auto", "required", "none"}
	addresses := []string{"127.0.0.1:9901", "0.0.0.0:9901"}
	creds := []struct {
		name  string
		auth  string
		valid bool
	}{
		{name: "none"},
		{name: "wrong", auth: "Bearer " + tokB},
		{name: "valid", auth: "Bearer " + tokA, valid: true},
	}
	paths := []string{"/config", "/upstreams", "/metrics", "/livez", "/readyz"}

	for _, mode := range modes {
		for _, address := range addresses {
			loopback := strings.HasPrefix(address, "127.")
			gatedWant := mode == "required" || (mode == "auto" && !loopback)
			for _, cred := range creds {
				for _, path := range paths {
					name := mode + " " + address + " " + cred.name + " " + path
					t.Run(name, func(t *testing.T) {
						src := newGate(t, mode, address)
						h := admin.Handler(src, nil)
						rec := serveAuth(t, h, http.MethodGet, path, cred.auth)

						if path == "/livez" || path == "/readyz" {
							if rec.Code != http.StatusOK && rec.Code != http.StatusServiceUnavailable {
								t.Fatalf("status = %d, want probe status", rec.Code)
							}
							return
						}
						if !gatedWant {
							if rec.Code != http.StatusOK {
								t.Fatalf("status = %d, want 200", rec.Code)
							}
							return
						}
						switch {
						case cred.name == "none":
							if rec.Code != http.StatusUnauthorized {
								t.Fatalf("status = %d, want 401", rec.Code)
							}
							if got := rec.Header().Get("WWW-Authenticate"); got != "Bearer" {
								t.Errorf("WWW-Authenticate = %q, want Bearer", got)
							}
							if code, _ := errorBody(t, rec); code != "unauthorized" {
								t.Errorf("code = %q, want unauthorized", code)
							}
							if src.reads != 0 {
								t.Errorf("state reads = %d, want 0", src.reads)
							}
						case !cred.valid:
							if rec.Code != http.StatusForbidden {
								t.Fatalf("status = %d, want 403", rec.Code)
							}
							if code, _ := errorBody(t, rec); code != "forbidden" {
								t.Errorf("code = %q, want forbidden", code)
							}
							if src.reads != 0 {
								t.Errorf("state reads = %d, want 0", src.reads)
							}
						default:
							if rec.Code != http.StatusOK {
								t.Fatalf("status = %d, want 200", rec.Code)
							}
						}
					})
				}
			}
		}
	}
}

func TestManagementGatingHEADAndScheme(t *testing.T) {
	src := newGate(t, "required", "0.0.0.0:9901")
	h := admin.Handler(src, nil)

	if rec := serveAuth(t, h, http.MethodHead, "/config", ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("HEAD /config without credential status = %d, want 401", rec.Code)
	}
	if rec := serveAuth(t, h, http.MethodGet, "/config", "Basic x"); rec.Code != http.StatusUnauthorized {
		t.Errorf("GET /config with Basic scheme status = %d, want 401", rec.Code)
	}
	if rec := serveAuth(t, h, http.MethodGet, "/config", "bearer "+tokA); rec.Code != http.StatusOK {
		t.Errorf("GET /config with lowercase bearer status = %d, want 200", rec.Code)
	}
	rec := serveAuth(t, h, http.MethodPost, "/config", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /config without credential status = %d, want 405", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); allow != "GET, HEAD" {
		t.Errorf("POST /config Allow = %q, want %q", allow, "GET, HEAD")
	}
}

func TestManagementGatingReadsPerRequest(t *testing.T) {
	src := newGate(t, "none", "0.0.0.0:9901")
	h := admin.Handler(src, nil)

	if rec := serveAuth(t, h, http.MethodGet, "/config", ""); rec.Code != http.StatusOK {
		t.Fatalf("/config with mode none status = %d, want 200", rec.Code)
	}
	src.admin.Auth.Management = "required"
	if rec := serveAuth(t, h, http.MethodGet, "/config", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("/config after mode required status = %d, want 401", rec.Code)
	}
}

type logRecord struct {
	Level  string `json:"level"`
	Msg    string `json:"msg"`
	Path   string `json:"path"`
	Status int    `json:"status"`
	Reason string `json:"reason"`
}

func decodeLog(t *testing.T, buf *bytes.Buffer) []logRecord {
	t.Helper()
	var out []logRecord
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec logRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

func TestManagementRejectLog(t *testing.T) {
	src := newGate(t, "required", "0.0.0.0:9901")
	var buf bytes.Buffer
	h := admin.Handler(src, slog.New(slog.NewJSONHandler(&buf, nil)))

	serveAuth(t, h, http.MethodGet, "/config", "")
	recs := decodeLog(t, &buf)
	if len(recs) != 1 {
		t.Fatalf("log records = %d, want 1: %s", len(recs), buf.String())
	}
	if r := recs[0]; r.Msg != "admin request rejected" || r.Level != "WARN" || r.Path != "/config" || r.Status != http.StatusUnauthorized {
		t.Errorf("record = %+v, want rejected WARN /config 401", r)
	}

	buf.Reset()
	serveAuth(t, h, http.MethodGet, "/config", "Bearer "+tokB)
	if strings.Contains(buf.String(), tokB) {
		t.Errorf("log leaks the presented token: %s", buf.String())
	}
	recs = decodeLog(t, &buf)
	if len(recs) != 1 || recs[0].Status != http.StatusForbidden {
		t.Errorf("records = %+v, want one 403 record", recs)
	}
}

// requiredPurge reports management auth as required while keeping the purge access of the embedded source.
type requiredPurge struct {
	*purgeSource
}

func (requiredPurge) ManagementAuth() (bool, []string, [][32]byte) {
	return true, []string{"a"}, nil
}

func TestPurgeUnaffectedByManagement(t *testing.T) {
	t.Run("no keys with required gate", func(t *testing.T) {
		h := admin.Handler(requiredPurge{newPurgeSource(t, admin.PurgeAccess{})}, nil)
		rec := postPurge(h, tokA, `{"all":true}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", rec.Code)
		}
		if code, _ := errorBody(t, rec); code != "not_found" {
			t.Errorf("code = %q, want not_found", code)
		}
	})

	t.Run("keys with mode none", func(t *testing.T) {
		access := admin.PurgeAccess{KeyIDs: []string{"a"}, Hashes: [][32]byte{sha256.Sum256([]byte(tokA))}, Cache: true}
		src := newPurgeSource(t, access)
		h := admin.Handler(src, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, purgeReq(http.MethodPost, "", strings.NewReader(`{"all":true}`)))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("no credential status = %d, want 401", rec.Code)
		}
		if got := rec.Header().Get("WWW-Authenticate"); got != "Bearer" {
			t.Errorf("WWW-Authenticate = %q, want Bearer", got)
		}
		if rec := postPurge(h, tokB, `{"all":true}`); rec.Code != http.StatusForbidden {
			t.Errorf("unknown key status = %d, want 403", rec.Code)
		}
		if rec := postPurge(h, tokA, `{"all":true}`); rec.Code != http.StatusOK {
			t.Errorf("valid key status = %d, want 200", rec.Code)
		}
	})

	t.Run("keys with required gate", func(t *testing.T) {
		h := admin.Handler(requiredPurge{newPurgeSource(t, keyedAccess(true))}, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, purgeReq(http.MethodGet, "Bearer "+tokA, nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("GET /cache/purge status = %d, want 405", rec.Code)
		}
		if allow := rec.Header().Get("Allow"); allow != "POST" {
			t.Errorf("Allow = %q, want POST", allow)
		}
	})
}
