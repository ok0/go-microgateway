package cli

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
)

func runCmd(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	return runWith(t, nil, args...)
}

func runWith(t *testing.T, opts []microgateway.Option, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := Main(context.Background(), Env{Args: args, Stdout: &out, Stderr: &errb}, opts...)
	return code, out.String(), errb.String()
}

type runHandle struct {
	t      *testing.T
	addrs  Addrs
	sig    chan os.Signal
	done   chan int
	cancel context.CancelFunc
	errb   interface {
		io.Writer
		String() string
	}
}

func (h *runHandle) stop() int {
	h.cancel()
	return h.wait(h.t, 10*time.Second)
}

func (h *runHandle) wait(t *testing.T, d time.Duration) int {
	t.Helper()
	select {
	case code := <-h.done:
		return code
	case <-time.After(d):
		t.Fatalf("run did not return within %s; stderr:\n%s", d, h.errb.String())
		return -1
	}
}

func startRun(t *testing.T, cfgPath string) *runHandle {
	t.Helper()
	return startRunArgs(t, []string{"-config", cfgPath}, &bytes.Buffer{})
}

func startRunArgs(t *testing.T, args []string, errb interface {
	io.Writer
	String() string
}) *runHandle {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ready := make(chan Addrs, 1)
	done := make(chan int, 1)
	sig := make(chan os.Signal, 2)
	var out bytes.Buffer
	go func() {
		done <- Main(ctx, Env{Args: append([]string{"run"}, args...), Stdout: &out, Stderr: errb, Signals: sig, Ready: func(a Addrs) { ready <- a }})
	}()
	h := &runHandle{t: t, sig: sig, done: done, cancel: cancel, errb: errb}
	select {
	case a := <-ready:
		h.addrs = a
		return h
	case code := <-done:
		t.Fatalf("run exited with code %d before listening; stderr:\n%s", code, errb.String())
	case <-time.After(10 * time.Second):
		t.Fatalf("run not ready within 10s; stderr:\n%s", errb.String())
	}
	return nil
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
	path := filepath.Join(t.TempDir(), "microgateway.yaml")
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
admin:
  address: "127.0.0.1:0"
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
	if !strings.HasPrefix(out, "microgateway dev ") {
		t.Errorf("stdout = %q, want it to start with %q", out, "microgateway dev ")
	}
}

func TestHelpNamesBinary(t *testing.T) {
	for _, arg := range []string{"help", "-h", "--help"} {
		code, out, _ := runCmd(t, arg)
		if code != 0 {
			t.Fatalf("%s: exit code = %d, want 0", arg, code)
		}
		first, _, _ := strings.Cut(out, "\n")
		if first != "usage: microgateway <command> [flags]" {
			t.Errorf("%s: first line = %q, want %q", arg, first, "usage: microgateway <command> [flags]")
		}
		for _, line := range strings.Split(out, "\n") {
			if strings.Contains(line, "usage:") && !strings.Contains(line, "usage: microgateway") {
				t.Errorf("%s: usage line %q does not name microgateway", arg, line)
			}
		}
	}
}

func TestNoCommand(t *testing.T) {
	var out, errb bytes.Buffer
	code := Main(context.Background(), Env{Stdout: &out, Stderr: &errb})
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
	cfg = strings.ReplaceAll(cfg, "127.0.0.1:9901", "127.0.0.1:0")
	path := writeTemp(t, cfg)

	h := startRun(t, path)
	public := h.addrs.Listeners["public"].String()
	admin := h.addrs.Admin.String()
	client := &http.Client{Timeout: 5 * time.Second}

	t.Run("users api", func(t *testing.T) {
		resp, body := doGet(t, client, public, "/api/users/42", "", nil)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401; body %s", resp.StatusCode, body)
		}
		if got := resp.Header.Get("WWW-Authenticate"); got != "ApiKey" {
			t.Errorf("WWW-Authenticate = %q, want ApiKey", got)
		}
		resp, body = doGet(t, client, public, "/api/users/42", "", http.Header{"X-Api-Key": {"example-api-key-alice"}})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body %s", resp.StatusCode, body)
		}
		echo := decodeEcho(t, body)
		if echo.Backend != "users" || echo.Path != "/api/users/42" {
			t.Errorf("echo = %+v, want backend users path /api/users/42", echo)
		}
		if got := echo.Headers.Get("X-Consumer-Id"); got != "alice" {
			t.Errorf("backend X-Consumer-Id = %q, want alice", got)
		}
		if _, ok := echo.Headers["X-Api-Key"]; ok {
			t.Error("backend received X-Api-Key, want it removed")
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
		if got := echo.Headers.Get("X-Gateway"); got != "microgateway" {
			t.Errorf("backend X-Gateway = %q, want microgateway", got)
		}
		if got := echo.Headers.Get("X-Env"); got != "example" {
			t.Errorf("backend X-Env = %q, want example", got)
		}
		if _, ok := echo.Headers["X-Debug"]; ok {
			t.Error("backend received X-Debug, want it removed")
		}
		if got := resp.Header.Get("X-Served-By"); got != "microgateway" {
			t.Errorf("response X-Served-By = %q, want microgateway", got)
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

	t.Run("admin readyz", func(t *testing.T) {
		resp, body := doGet(t, client, admin, "/readyz", "", nil)
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"ready"`) {
			t.Errorf("status = %d body %s, want 200 with ready", resp.StatusCode, body)
		}
	})

	t.Run("admin metrics", func(t *testing.T) {
		resp, body := doGet(t, client, admin, "/metrics", "", nil)
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "microgateway_requests_total") {
			t.Errorf("status = %d, want 200 with microgateway_requests_total in body %s", resp.StatusCode, body)
		}
	})

	t.Run("admin paths not on data listener", func(t *testing.T) {
		resp, body := doGet(t, client, public, "/readyz", "", nil)
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

	if code := h.stop(); code != 0 {
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

func TestValidateDNSUpstream(t *testing.T) {
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
	code, _, errs := runCmd(t, "validate", "-config", path)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr:\n%s", code, errs)
	}
}

func TestValidateUpstreamErrors(t *testing.T) {
	src := `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: a
    hosts:
      - url: "http://a.internal:9001"
        weight: -1
    load_balancer:
      algorithm: bogus
    passive_health:
      max_ejection_percent: 120
    active_health:
      interval: 1s
      timeout: 2s
  - name: b
    hosts:
      - url: "http://b.internal:9001"
    dns:
      hostname: b.internal
      port: 9001
    load_balancer:
      algorithm: consistent_hash
  - name: c
    hosts: []
    load_balancer:
      algorithm: consistent_hash
      hash:
        source: ip
      bounded_load_factor: 0.5
      virtual_nodes: -1
`
	path := writeTemp(t, src)
	code, _, errs := runCmd(t, "validate", "-config", path)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	paths := []string{
		"upstreams[0].hosts[0].weight",
		"upstreams[0].load_balancer.algorithm",
		"upstreams[0].passive_health.max_ejection_percent",
		"upstreams[0].active_health.timeout",
		"upstreams[1]",
		"upstreams[1].load_balancer.hash.source",
		"upstreams[2]",
		"upstreams[2].load_balancer.bounded_load_factor",
		"upstreams[2].load_balancer.virtual_nodes",
	}
	for _, p := range paths {
		re := regexp.MustCompile(regexp.QuoteMeta(p) + ` \(line [1-9][0-9]*\): `)
		if !re.MatchString(errs) {
			t.Errorf("stderr lacks an error for %s with a line number\nstderr:\n%s", p, errs)
		}
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
	h := startRunArgs(t, []string{"-config", path, "-log-format", "text", "-log-level", "debug"}, &errb)
	if code := h.stop(); code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr:\n%s", code, errb.String())
	}
	if !strings.Contains(errb.String(), "level=INFO msg=listening") {
		t.Errorf("stderr = %q, want it to contain %q", errb.String(), "level=INFO msg=listening")
	}
}

func lineOf(t *testing.T, src, substr string) int {
	t.Helper()
	for i, line := range strings.Split(src, "\n") {
		if strings.Contains(line, substr) {
			return i + 1
		}
	}
	t.Fatalf("%q not found in config", substr)
	return 0
}

func TestValidateMiddlewareErrors(t *testing.T) {
	src := minimalConfig + `middlewares:
  - name: retry
    type: cors
  - name: c
    type: cors
    params:
      max_age: -1
`
	path := writeTemp(t, src)
	code, _, errs := runCmd(t, "validate", "-config", path)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	wants := []string{
		fmt.Sprintf("middlewares[0].name (line %d): ", lineOf(t, src, "- name: retry")),
		fmt.Sprintf("middlewares[1].params.max_age (line %d): ", lineOf(t, src, "max_age: -1")),
	}
	for _, w := range wants {
		if !strings.Contains(errs, w) {
			t.Errorf("stderr missing line with prefix %q\nstderr:\n%s", w, errs)
		}
	}
}

// A type the core does not know validates when the options passed to Main register it, and validate builds nothing.
func TestValidateUnregisteredType(t *testing.T) {
	path := writeTemp(t, minimalConfig+`middlewares:
  - name: x
    type: my_custom
    params:
      anything: 1
`)
	var called atomic.Int32
	factory := func(microgateway.MiddlewareParams) (microgateway.Middleware, error) {
		called.Add(1)
		return nil, errors.New("not built")
	}
	code, _, errs := runWith(t, []microgateway.Option{microgateway.WithMiddleware("my_custom", factory)}, "validate", "-config", path)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr:\n%s", code, errs)
	}
	if n := called.Load(); n != 0 {
		t.Errorf("middleware factory called %d times during validate, want 0", n)
	}
}

func TestWatchPathsIncludesKeyFiles(t *testing.T) {
	dir := t.TempDir()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pub.pem"), pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	const keys = "keys:\n  - id: file-key\n    hash: \"sha256:9771189e33ad49531f98873eb4e4458ce1c96f313fe15c6d753f4c7b6ad4eba5\"\n    consumer: alice\n"
	if err := os.WriteFile(filepath.Join(dir, "keys.yaml"), []byte(keys), 0o600); err != nil {
		t.Fatal(err)
	}
	src := minimalConfig + `auth:
  api_keys:
    - name: file
      key_file: keys.yaml
  jwt:
    - name: pk
      issuer: https://auth.example.com
      audiences: [example-api]
      keys:
        - algorithm: EdDSA
          public_key_file: pub.pem
`
	path := filepath.Join(dir, "microgateway.yaml")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := watchPaths(path, cfg)
	want := []string{path, filepath.Join(dir, "keys.yaml"), filepath.Join(dir, "pub.pem")}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("watchPaths = %q, want %q", got, want)
	}
}

func TestValidateAuthErrors(t *testing.T) {
	code, _, errs := runCmd(t, "validate", "-config", "../../config/testdata/auth_errors.yaml")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	paths := []string{
		"auth.jwt[0].issuer",
		"auth.jwt[1].audiences",
		"auth.jwt[2].keys[0].secret",
		"auth.jwt[2].max_token_bytes",
		"auth.jwt[3].keys[0].public_key",
		"auth.jwt[4].keys[0].algorithm",
		"auth.jwt[5].jwks.ttl",
		"auth.api_keys[0].keys[0].hash",
		"routes[0].auth.providers[0]",
		"routes[1].auth.providers[0]",
		"routes[2].auth.allow_anonymous",
		"routes[3].pipeline",
	}
	for _, p := range paths {
		if !strings.Contains(errs, p+" (line ") {
			t.Errorf("stderr lacks an error for %s with a line number\nstderr:\n%s", p, errs)
		}
	}
}

func TestValidateRateLimitErrors(t *testing.T) {
	code, _, errs := runCmd(t, "validate", "-config", "../../config/testdata/ratelimit_invalid.yaml")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	wants := []struct {
		path string
		line int
		msg  string
	}{
		{"routes[0].rate_limit.idle_ttl", 17, `unknown field "idle_ttl"`},
		{"routes[1].rate_limit.rate", 23, "must be >= 1"},
		{"routes[2].rate_limit.rate", 28, "is required for token_bucket"},
		{"routes[3].rate_limit.per", 36, "must be positive"},
		{"routes[4].rate_limit.burst", 43, "must be >= 1"},
		{"routes[5].rate_limit.max_keys", 50, "must be >= 1"},
		{"routes[6].rate_limit.window", 58, "must be positive"},
		{"routes[7].rate_limit.limit", 66, "is only valid with algorithm sliding_window"},
		{"routes[8].rate_limit.rate", 74, "is only valid with algorithm token_bucket"},
		{"routes[9].rate_limit.key.name", 81, "is required when key source is header"},
		{"routes[10].rate_limit.key.name", 88, "is only valid with key source header"},
		{"routes[11].rate_limit.algorithm", 94, "must be one of token_bucket, sliding_window"},
		{"routes[12].rate_limit.key.source", 101, "must be one of ip, consumer, header, route"},
		{"routes[13].rate_limit.rate", 107, "per/rate must be at least 1µs"},
		{"routes[14].rate_limit.burst", 116, "burst × per/rate must not exceed 24h"},
		{"routes[15].rate_limit.key.source", 123, "consumer requires route authentication"},
		{"routes[16].pipeline", 130, `must include "rate_limit"`},
	}
	for _, w := range wants {
		want := fmt.Sprintf("%s (line %d): %s", w.path, w.line, w.msg)
		if !strings.Contains(errs, want) {
			t.Errorf("stderr lacks %q\nstderr:\n%s", want, errs)
		}
	}
}
