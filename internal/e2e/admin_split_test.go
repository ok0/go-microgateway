package e2e

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/httperr"
)

const (
	splitTokA = "as-A-9fQ2xR"
	splitTokB = "as-B-4kT7wM"
	warnMsg   = "admin management endpoints are unauthenticated on a non-loopback address"
)

func splitDoc(admin string) string {
	return "version: 1\n" + listenersYAML + "upstreams:\n  - name: up\n    hosts:\n      - url: \"http://127.0.0.1:9001\"\nroutes:\n  - name: all\n    upstream: up\n" + admin
}

func splitAdmin(address, mode string, metrics bool, tokens ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "admin:\n  address: \"%s\"\n  probes:\n    metrics: %t\n  auth:\n    management: %s\n", address, metrics, mode)
	if len(tokens) > 0 {
		b.WriteString("    keys:\n")
	}
	for i, tok := range tokens {
		fmt.Fprintf(&b, "      - id: key-%d\n        hash: \"%s\"\n", i, auHash(tok))
	}
	return b.String()
}

func splitGet(h http.Handler, method, path, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// splitExpect serves a GET for path through h with token, fails t unless the status is want, and checks that the
// response is no-store.
func splitExpect(t *testing.T, h http.Handler, path, token string, want int) *httptest.ResponseRecorder {
	t.Helper()
	rec := splitGet(h, http.MethodGet, path, token)
	if rec.Code != want {
		t.Fatalf("GET %s: status = %d, want %d; body %s", path, rec.Code, want, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("GET %s: Cache-Control = %q, want no-store", path, got)
	}
	return rec
}

// splitFieldErr returns the field error at path in err, failing t unless err is a *config.ValidationError holding one.
func splitFieldErr(t *testing.T, err error, path string) config.FieldError {
	t.Helper()
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error = %v (%T), want *config.ValidationError", err, err)
	}
	for _, fe := range ve.Errors {
		if fe.Path == path {
			return fe
		}
	}
	t.Fatalf("no error at %s in %v", path, err)
	return config.FieldError{}
}

func TestAdminSplitManagementGating(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"auto", "required", "none"} {
		for _, addr := range []string{"127.0.0.1:9901", "0.0.0.0:9901"} {
			t.Run(mode+" "+addr, func(t *testing.T) {
				t.Parallel()
				g := gwWith(t, splitDoc(splitAdmin(addr, mode, false, splitTokA)))
				h := g.AdminHandler()
				nonLoopback := addr == "0.0.0.0:9901"
				gated := mode == "required" || (mode == "auto" && nonLoopback)
				for _, path := range []string{"/livez", "/readyz"} {
					splitExpect(t, h, path, "", http.StatusOK)
				}
				for _, path := range []string{"/config", "/upstreams", "/metrics"} {
					if !gated {
						for _, tok := range []string{"", splitTokB, splitTokA} {
							splitExpect(t, h, path, tok, http.StatusOK)
						}
						continue
					}
					rec := splitExpect(t, h, path, "", http.StatusUnauthorized)
					if got := rec.Header().Get("WWW-Authenticate"); got != "Bearer" {
						t.Errorf("%s: WWW-Authenticate = %q, want Bearer", path, got)
					}
					if e := auErrorBody(t, rec); e.Code != httperr.CodeUnauthorized {
						t.Errorf("%s: code = %q, want %q", path, e.Code, httperr.CodeUnauthorized)
					}
					rec = splitExpect(t, h, path, splitTokB, http.StatusForbidden)
					if e := auErrorBody(t, rec); e.Code != httperr.CodeForbidden {
						t.Errorf("%s: code = %q, want %q", path, e.Code, httperr.CodeForbidden)
					}
					splitExpect(t, h, path, splitTokA, http.StatusOK)
				}
				p := g.ProbeHandler()
				splitExpect(t, p, "/livez", "", http.StatusOK)
				for _, path := range []string{"/config", "/upstreams", "/cache/purge", "/metrics"} {
					rec := splitExpect(t, p, path, splitTokA, http.StatusNotFound)
					if e := auErrorBody(t, rec); e.Code != httperr.CodeNotFound {
						t.Errorf("%s: code = %q, want %q", path, e.Code, httperr.CodeNotFound)
					}
				}
			})
		}
	}
}

func TestAdminSplitReload(t *testing.T) {
	t.Parallel()
	g := gwWith(t, splitDoc(splitAdmin("0.0.0.0:9901", "auto", false, splitTokA)))
	h := g.AdminHandler()
	p := g.ProbeHandler()

	splitExpect(t, h, "/config", "", http.StatusUnauthorized)
	splitExpect(t, h, "/config", splitTokB, http.StatusForbidden)
	splitExpect(t, h, "/config", splitTokA, http.StatusOK)
	splitExpect(t, p, "/metrics", "", http.StatusNotFound)

	if err := g.ReloadYAML([]byte(splitDoc(splitAdmin("0.0.0.0:9901", "auto", true, splitTokB)))); err != nil {
		t.Fatalf("reload to key B and probe metrics: %v", err)
	}
	splitExpect(t, h, "/config", splitTokA, http.StatusForbidden)
	splitExpect(t, h, "/config", splitTokB, http.StatusOK)
	adminMetrics := splitExpect(t, h, "/metrics", splitTokB, http.StatusOK)
	probeMetrics := splitExpect(t, p, "/metrics", "", http.StatusOK)
	if got, want := probeMetrics.Header().Get("Content-Type"), adminMetrics.Header().Get("Content-Type"); got != want {
		t.Fatalf("probe /metrics Content-Type = %q, want %q", got, want)
	}
	if !strings.Contains(probeMetrics.Body.String(), "microgateway_") {
		t.Fatal("probe /metrics body has no microgateway_ series")
	}

	cfg := g.Config().Clone()
	cfg.Admin.Auth.Management = "none"
	if err := g.Reload(cfg); err != nil {
		t.Fatalf("reload to management none: %v", err)
	}
	splitExpect(t, h, "/config", "", http.StatusOK)

	openErr := g.ReloadYAML([]byte(splitDoc(splitAdmin("0.0.0.0:9901", "open", true, splitTokB))))
	splitFieldErr(t, openErr, "admin.auth.management")
	splitExpect(t, h, "/config", "", http.StatusOK)
	splitExpect(t, p, "/metrics", "", http.StatusOK)

	path := writeConfig(t, t.TempDir(), splitDoc(splitAdmin("0.0.0.0:9901", "auto", false, splitTokB)))
	if err := g.ReloadFile(path); err != nil {
		t.Fatalf("reload file to management auto with key B: %v", err)
	}
	splitExpect(t, h, "/config", "", http.StatusUnauthorized)
	splitExpect(t, h, "/config", splitTokB, http.StatusOK)
	splitExpect(t, p, "/metrics", "", http.StatusNotFound)

	g.SetReady(false)
	for _, hd := range []http.Handler{h, p} {
		rec := splitExpect(t, hd, "/readyz", "", http.StatusServiceUnavailable)
		if !strings.Contains(rec.Body.String(), "not_ready") {
			t.Errorf("/readyz body = %s, want not_ready", rec.Body.String())
		}
		splitExpect(t, hd, "/livez", "", http.StatusOK)
	}
}

func TestAdminSplitBoundAddress(t *testing.T) {
	t.Parallel()
	g := gwWith(t, splitDoc(splitAdmin("0.0.0.0:9901", "auto", false, splitTokA)))
	if err := g.ReloadYAML([]byte(splitDoc(splitAdmin("127.0.0.1:9901", "auto", false)))); err != nil {
		t.Fatalf("reload to loopback address: %v", err)
	}
	h := g.AdminHandler()
	splitExpect(t, h, "/config", "", http.StatusUnauthorized)
	splitExpect(t, h, "/config", splitTokA, http.StatusForbidden)
}

func TestAdminSplitOpenWarning(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, admin, address string
		want                 bool
	}{
		{"none wildcard", splitAdmin("0.0.0.0:9901", "none", false), "0.0.0.0:9901", true},
		{"none empty host", splitAdmin(":9901", "none", false), ":9901", true},
		{"none unspecified v6", splitAdmin("[::]:9901", "none", false), "[::]:9901", true},
		{"none loopback", splitAdmin("127.0.0.1:9901", "none", false), "127.0.0.1:9901", false},
		{"none localhost", splitAdmin("localhost:9901", "none", false), "localhost:9901", false},
		{"auto with key", splitAdmin("0.0.0.0:9901", "auto", false, splitTokA), "0.0.0.0:9901", false},
		{"required with key", splitAdmin("0.0.0.0:9901", "required", false, splitTokA), "0.0.0.0:9901", false},
		{"required loopback with key", splitAdmin("127.0.0.1:9901", "required", false, splitTokA), "127.0.0.1:9901", false},
		{"none disabled", strings.Replace(splitAdmin("0.0.0.0:9901", "none", false), "admin:\n", "admin:\n  enabled: false\n", 1), "0.0.0.0:9901", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc := splitDoc(tc.admin)
			var buf logBuf
			g := gwWith(t, doc, microgateway.WithLogger(newJSONLogger(&buf)))
			warnings := func() int { return len(buf.records(t, warnMsg)) }
			w := 0
			if tc.want {
				w = 1
			}
			check := func(step string, want int) {
				t.Helper()
				if got := warnings(); got != want {
					t.Fatalf("%s: warning records = %d, want %d", step, got, want)
				}
			}

			check("after New", w)
			if err := g.ReloadYAML([]byte(doc)); err != nil {
				t.Fatalf("unchanged ReloadYAML: %v", err)
			}
			check("unchanged ReloadYAML", 2*w)
			if err := g.Reload(g.Config()); err != nil {
				t.Fatalf("unchanged Reload: %v", err)
			}
			check("unchanged Reload", 3*w)
			changed := writeConfig(t, t.TempDir(), strings.Replace(doc, "metrics: false", "metrics: true", 1))
			if err := g.ReloadFile(changed); err != nil {
				t.Fatalf("changed ReloadFile: %v", err)
			}
			check("changed ReloadFile", 4*w)
			if err := g.ReloadYAML([]byte("version: 2\n")); err == nil {
				t.Fatal("ReloadYAML of version 2 succeeded, want an error")
			}
			check("failed ReloadYAML", 4*w)

			for _, r := range buf.records(t, warnMsg) {
				if r["level"] != "WARN" || r["field"] != "admin.auth.management" || r["address"] != tc.address {
					t.Errorf("warning record = %v, want level WARN, field admin.auth.management, address %q", r, tc.address)
				}
			}
			if s := buf.String(); strings.Contains(s, splitTokA) || strings.Contains(s, auHash(splitTokA)) {
				t.Error("logs contain the management key or its hash")
			}
		})
	}
}

func TestAdminSplitValidationEntryPoints(t *testing.T) {
	t.Parallel()
	bad := splitDoc(splitAdmin("0.0.0.0:9901", "auto", false))

	_, err := microgateway.NewFromYAML([]byte(bad))
	if fe := splitFieldErr(t, err, "admin.auth.keys"); fe.Line <= 0 {
		t.Errorf("NewFromYAML error line = %d, want > 0", fe.Line)
	}

	cfg, err := config.Parse([]byte(splitDoc("")))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Admin.Address = "0.0.0.0:9901"
	_, err = microgateway.New(cfg)
	splitFieldErr(t, err, "admin.auth.keys")

	g := gw(t, splitDoc(""))
	reloadCfgErr := g.Reload(cfg)
	splitFieldErr(t, reloadCfgErr, "admin.auth.keys")
	reloadBadErr := g.ReloadYAML([]byte(bad))
	if fe := splitFieldErr(t, reloadBadErr, "admin.auth.keys"); fe.Line <= 0 {
		t.Errorf("ReloadYAML error line = %d, want > 0", fe.Line)
	}
	reloadRequiredErr := g.ReloadYAML([]byte(splitDoc(splitAdmin("127.0.0.1:9901", "required", false))))
	splitFieldErr(t, reloadRequiredErr, "admin.auth.keys")

	for _, e := range []error{reloadCfgErr, reloadBadErr, reloadRequiredErr} {
		if strings.Contains(e.Error(), "sha256:") {
			t.Error("a validation error contains a key hash")
		}
	}
	splitExpect(t, g.AdminHandler(), "/config", "", http.StatusOK)
}
