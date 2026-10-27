package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func runCmd(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(context.Background(), args, &out, &errb)
	return code, out.String(), errb.String()
}

func startRun(t *testing.T, cfgPath string) (map[string]net.Addr, func() int) {
	t.Helper()
	return startRunArgs(t, []string{"-config", cfgPath}, &bytes.Buffer{})
}

func startRunArgs(t *testing.T, args []string, errb *bytes.Buffer) (map[string]net.Addr, func() int) {
	t.Helper()
	ctx, cancelCtx := context.WithCancel(context.Background())
	t.Cleanup(cancelCtx)
	ready := make(chan map[string]net.Addr, 1)
	done := make(chan int, 1)
	var out bytes.Buffer
	go func() {
		done <- runServe(ctx, args, &out, errb, func(a map[string]net.Addr) { ready <- a })
	}()
	stop := func() int {
		cancelCtx()
		select {
		case code := <-done:
			return code
		case <-time.After(10 * time.Second):
			t.Fatalf("run did not return within 10s of cancel; stderr:\n%s", errb.String())
			return -1
		}
	}
	select {
	case addrs := <-ready:
		return addrs, stop
	case code := <-done:
		t.Fatalf("run exited with code %d before listening; stderr:\n%s", code, errb.String())
	case <-time.After(10 * time.Second):
		t.Fatalf("run not ready within 10s; stderr:\n%s", errb.String())
	}
	return nil, nil
}

type echoBody struct {
	Backend string      `json:"backend"`
	Path    string      `json:"path"`
	Headers http.Header `json:"headers"`
}

func echoBackend(t *testing.T, name string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Internal", "secret")
		w.Header().Set("Cache-Control", "max-age=60")
		_ = json.NewEncoder(w).Encode(echoBody{Backend: name, Path: r.URL.Path, Headers: r.Header})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func hostPort(s *httptest.Server) string {
	return strings.TrimPrefix(s.URL, "http://")
}

func doGet(t *testing.T, client *http.Client, addr, path, host string, hdr http.Header) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if host != "" {
		req.Host = host
	}
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

func decodeEcho(t *testing.T, body []byte) echoBody {
	t.Helper()
	var e echoBody
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("decode echo body %q: %v", body, err)
	}
	return e
}

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lychgate.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const minimalConfig = `version: 1
listeners:
  - name: public
    address: "127.0.0.1:0"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
routes:
  - name: all
    match:
      path: /
      path_type: prefix
    upstream: users
`

func TestValidateExample(t *testing.T) {
	code, out, errs := runCmd(t, "validate", "-config", "../../examples/basic.yaml")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr:\n%s", code, errs)
	}
	if !strings.Contains(out, "configuration is valid") {
		t.Errorf("stdout = %q, want it to contain %q", out, "configuration is valid")
	}
	if errs != "" {
		t.Errorf("stderr = %q, want empty", errs)
	}
}

func TestValidateInvalid(t *testing.T) {
	code, _, errs := runCmd(t, "validate", "-config", "../../config/testdata/invalid_multi.yaml")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	wantPrefixes := []string{
		"listeners[0].bogus_key (line 5): ",
		"upstreams[1].name (line 10): ",
		"routes[0].upstream (line 17): ",
		"routes[1].match.path (line 20): ",
		"routes[1].timeout (line 23): ",
	}
	for _, p := range wantPrefixes {
		if !strings.Contains(errs, p) {
			t.Errorf("stderr missing line with prefix %q\nstderr:\n%s", p, errs)
		}
	}
	lineRE := regexp.MustCompile(`^\S+ \(line \d+\): .+$`)
	lines := strings.Split(strings.TrimRight(errs, "\n"), "\n")
	for _, l := range lines[:len(lines)-1] {
		if !lineRE.MatchString(l) {
			t.Errorf("error line %q does not match %s", l, lineRE)
		}
	}
}

func TestValidateMissingFile(t *testing.T) {
	code, _, errs := runCmd(t, "validate", "-config", "testdata/does-not-exist.yaml")
	if code == 0 {
		t.Fatal("exit code = 0, want nonzero")
	}
	if errs == "" {
		t.Error("stderr is empty, want an error message")
	}
}

func TestValidateMissingFlag(t *testing.T) {
	code, _, errs := runCmd(t, "validate")
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if !strings.Contains(errs, "-config is required") {
		t.Errorf("stderr = %q, want it to contain %q", errs, "-config is required")
	}
}

func TestVersion(t *testing.T) {
	code, out, _ := runCmd(t, "version")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	for _, want := range []string{"dev", "none", "unknown"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout = %q, want it to contain %q", out, want)
		}
	}
}

func TestNoCommand(t *testing.T) {
	var out, errb bytes.Buffer
	code := run(context.Background(), nil, &out, &errb)
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if !strings.Contains(errb.String(), "usage:") {
		t.Errorf("stderr = %q, want usage text", errb.String())
	}
}

func TestUnknownCommand(t *testing.T) {
	for _, cmd := range []string{"bogus"} {
		code, _, errs := runCmd(t, cmd)
		if code != 2 {
			t.Errorf("%s: exit code = %d, want 2", cmd, code)
		}
		if !strings.Contains(errs, "unknown command") {
			t.Errorf("%s: stderr = %q, want it to contain %q", cmd, errs, "unknown command")
		}
	}
}

func TestRunExample(t *testing.T) {
	users := echoBackend(t, "users")
	orders := echoBackend(t, "orders")
	raw, err := os.ReadFile("../../examples/basic.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg := string(raw)
	cfg = strings.ReplaceAll(cfg, "127.0.0.1:9001", hostPort(users))
	cfg = strings.ReplaceAll(cfg, "127.0.0.1:9002", hostPort(users))
	cfg = strings.ReplaceAll(cfg, "127.0.0.1:9101", hostPort(orders))
	cfg = strings.ReplaceAll(cfg, "127.0.0.1:9102", hostPort(orders))
	cfg = strings.ReplaceAll(cfg, `":8080"`, `"127.0.0.1:0"`)
	path := writeTemp(t, cfg)

	addrs, cancel := startRun(t, path)
	public := addrs["public"].String()
	client := &http.Client{Timeout: 5 * time.Second}

	t.Run("users api", func(t *testing.T) {
		resp, body := doGet(t, client, public, "/api/users/42", "", nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body %s", resp.StatusCode, body)
		}
		echo := decodeEcho(t, body)
		if echo.Backend != "users" || echo.Path != "/api/users/42" {
			t.Errorf("echo = %+v, want backend users path /api/users/42", echo)
		}
	})

	t.Run("strip prefix", func(t *testing.T) {
		resp, body := doGet(t, client, public, "/orders/123", "", nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body %s", resp.StatusCode, body)
		}
		echo := decodeEcho(t, body)
		if echo.Backend != "orders" || echo.Path != "/123" {
			t.Errorf("echo = %+v, want backend orders path /123", echo)
		}
	})

	t.Run("health exact", func(t *testing.T) {
		resp, body := doGet(t, client, public, "/healthz", "", nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body %s", resp.StatusCode, body)
		}
		echo := decodeEcho(t, body)
		if echo.Backend != "users" || echo.Path != "/healthz" {
			t.Errorf("echo = %+v, want backend users path /healthz", echo)
		}
	})

	t.Run("header and path rewrite", func(t *testing.T) {
		hdr := http.Header{"X-Debug": {"1"}, "X-Env": {"wrong"}}
		resp, body := doGet(t, client, public, "/v2/items?x=1", "orders.example.com", hdr)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body %s", resp.StatusCode, body)
		}
		echo := decodeEcho(t, body)
		if echo.Backend != "orders" || echo.Path != "/api/v2/items" {
			t.Errorf("echo = %+v, want backend orders path /api/v2/items", echo)
		}
		if got := echo.Headers.Get("X-Gateway"); got != "lychgate" {
			t.Errorf("backend X-Gateway = %q, want lychgate", got)
		}
		if got := echo.Headers.Get("X-Env"); got != "example" {
			t.Errorf("backend X-Env = %q, want example", got)
		}
		if _, ok := echo.Headers["X-Debug"]; ok {
			t.Error("backend received X-Debug, want it removed")
		}
		if got := resp.Header.Get("X-Served-By"); got != "lychgate" {
			t.Errorf("response X-Served-By = %q, want lychgate", got)
		}
		if got := resp.Header.Get("Cache-Control"); got != "no-cache" {
			t.Errorf("response Cache-Control = %q, want no-cache", got)
		}
		if _, ok := resp.Header["X-Internal"]; ok {
			t.Error("response carries X-Internal, want it removed")
		}
	})

	t.Run("unmatched host", func(t *testing.T) {
		resp, body := doGet(t, client, public, "/v2/items", "other.example.org", nil)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404; body %s", resp.StatusCode, body)
		}
		var e struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(body, &e); err != nil {
			t.Fatalf("decode error body %q: %v", body, err)
		}
		if e.Error.Code != "route_not_found" {
			t.Errorf("code = %q, want route_not_found", e.Error.Code)
		}
	})

	if code := cancel(); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

func TestRunInvalidConfig(t *testing.T) {
	code, _, errs := runCmd(t, "run", "-config", "../../config/testdata/invalid_multi.yaml")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	wantPrefixes := []string{
		"listeners[0].bogus_key (line 5): ",
		"upstreams[1].name (line 10): ",
		"routes[0].upstream (line 17): ",
		"routes[1].match.path (line 20): ",
		"routes[1].timeout (line 23): ",
	}
	for _, p := range wantPrefixes {
		if !strings.Contains(errs, p) {
			t.Errorf("stderr missing line with prefix %q\nstderr:\n%s", p, errs)
		}
	}
	lineRE := regexp.MustCompile(`^\S+ \(line \d+\): .+$`)
	lines := strings.Split(strings.TrimRight(errs, "\n"), "\n")
	for _, l := range lines[:len(lines)-1] {
		if !lineRE.MatchString(l) {
			t.Errorf("error line %q does not match %s", l, lineRE)
		}
	}
}

func TestRunTLSNotSupported(t *testing.T) {
	path := writeTemp(t, `version: 1
listeners:
  - name: public
    address: "127.0.0.1:0"
    tls:
      cert_file: c.pem
      key_file: k.pem
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
routes:
  - name: all
    match:
      path: /
      path_type: prefix
    upstream: users
`)
	code, _, errs := runCmd(t, "run", "-config", path)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	if !strings.Contains(errs, "not yet supported") {
		t.Errorf("stderr = %q, want it to contain %q", errs, "not yet supported")
	}
}

func TestRunDNSNotSupported(t *testing.T) {
	path := writeTemp(t, `version: 1
listeners:
  - name: public
    address: "127.0.0.1:0"
upstreams:
  - name: users
    dns:
      hostname: backend.internal
      port: 8080
routes:
  - name: all
    match:
      path: /
      path_type: prefix
    upstream: users
`)
	code, _, errs := runCmd(t, "run", "-config", path)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	if !strings.Contains(errs, "not yet supported") {
		t.Errorf("stderr = %q, want it to contain %q", errs, "not yet supported")
	}
}

func TestRunBadFlags(t *testing.T) {
	path := writeTemp(t, minimalConfig)
	tests := []struct {
		name string
		args []string
	}{
		{"missing config", []string{"run"}},
		{"bad log level", []string{"run", "-config", path, "-log-level", "loud"}},
		{"bad log format", []string{"run", "-config", path, "-log-format", "xml"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, _, _ := runCmd(t, tc.args...)
			if code != 2 {
				t.Errorf("exit code = %d, want 2", code)
			}
		})
	}
}

func TestRunLogFlagOverride(t *testing.T) {
	path := writeTemp(t, minimalConfig)
	var errb bytes.Buffer
	_, cancel := startRunArgs(t, []string{"-config", path, "-log-format", "text", "-log-level", "debug"}, &errb)
	if code := cancel(); code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr:\n%s", code, errb.String())
	}
	if !strings.Contains(errb.String(), "level=INFO msg=listening") {
		t.Errorf("stderr = %q, want it to contain %q", errb.String(), "level=INFO msg=listening")
	}
}
