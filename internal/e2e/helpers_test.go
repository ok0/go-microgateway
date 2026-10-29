package e2e

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/authkey"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/hooks"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/internal/upstream"
)

func withClock(c clock.Clock) microgateway.Option { return hooks.WithClock(c).(microgateway.Option) }

func withRand(r clock.Rand) microgateway.Option { return hooks.WithRand(r).(microgateway.Option) }

func withDialer(d func(ctx context.Context, network, addr string) (net.Conn, error)) microgateway.Option {
	return hooks.WithDialer(d).(microgateway.Option)
}

func withResolver(r hooks.Resolver) microgateway.Option {
	return hooks.WithResolver(r).(microgateway.Option)
}

func inspect(g *microgateway.Gateway) hooks.Stats { return hooks.Inspect(g) }

const listenersYAML = "listeners:\n  - name: public\n    address: \"127.0.0.1:0\"\n"

const trustedGlobal = "global:\n  trusted_proxies:\n    - \"203.0.113.0/24\"\n"

type echo struct {
	Backend string      `json:"backend"`
	Path    string      `json:"path"`
	RawPath string      `json:"raw_path"`
	Host    string      `json:"host"`
	Headers http.Header `json:"headers"`
}

type apiError struct {
	Status    int    `json:"status"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

var hexID = regexp.MustCompile(`^[0-9a-f]{32}$`)

func echoHandler(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(echo{
			Backend: name,
			Path:    r.URL.Path,
			RawPath: r.URL.RawPath,
			Host:    r.Host,
			Headers: r.Header,
		})
	})
}

func backend(t *testing.T, name string) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(echoHandler(name))
	t.Cleanup(s.Close)
	return s
}

func gw(t *testing.T, doc string) *microgateway.Gateway {
	t.Helper()
	g, err := microgateway.NewFromYAML([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func oneRouteDoc(global, backendURL string) string {
	return "version: 1\n" + global + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: all
    upstream: up
`, backendURL)
}

func decodeErr(t *testing.T, resp *http.Response) apiError {
	t.Helper()
	defer resp.Body.Close()
	var body struct {
		Error apiError `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body.Error
}

func serve(t *testing.T, g *microgateway.Gateway, req *http.Request) (*http.Response, echo) {
	t.Helper()
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	resp := rec.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var e echo
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp, e
}

// eventually waits until cond holds; what and state (may be nil) describe the wait if the hang guard expires.
func eventually(t *testing.T, what string, cond func() bool, state func() string) {
	t.Helper()
	testwait.Until(t, what, cond, state)
}

func gwWith(t *testing.T, doc string, opts ...microgateway.Option) *microgateway.Gateway {
	t.Helper()
	g, err := microgateway.NewFromYAML([]byte(doc), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func routeDoc(backendURL, routeExtra string) string {
	return "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: all
    upstream: up
`, backendURL) + routeExtra
}

type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func newJSONLogger(l *logBuf) *slog.Logger {
	return slog.New(slog.NewJSONHandler(l, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func (l *logBuf) records(t *testing.T, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(l.String(), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if m["msg"] == msg {
			out = append(out, m)
		}
	}
	return out
}

func writeConfig(t *testing.T, dir, doc string) string {
	t.Helper()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func counters(g *microgateway.Gateway) (success, failure, unchanged float64) {
	s := inspect(g)
	return s.ReloadSuccess, s.ReloadFailure, s.ReloadUnchanged
}

func countersState(g *microgateway.Gateway) func() string {
	return func() string {
		s, f, u := counters(g)
		return fmt.Sprintf("reloads success=%v failure=%v unchanged=%v", s, f, u)
	}
}

func metricsText(t *testing.T, g *microgateway.Gateway) string {
	t.Helper()
	rec := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	checkMetricPrefix(t, body)
	return body
}

func adminBody(t *testing.T, g *microgateway.Gateway, path string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("%s status = %d, want 200", path, rec.Code)
	}
	return rec.Body.String()
}

func statusOf(client *http.Client, url string) int {
	resp, err := client.Get(url)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	testwait.Done(t, what, ch, nil)
}

func awaitWatch(t *testing.T, done <-chan error) error {
	t.Helper()
	err, _ := testwait.Recv(t, "WatchFile to return", done, nil)
	return err
}

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

type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *captureHandler) WithGroup(string) slog.Handler { return h }

func (h *captureHandler) requests() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []slog.Record
	for _, r := range h.records {
		if r.Message == "request" {
			out = append(out, r)
		}
	}
	return out
}

func recordAttrs(r slog.Record) map[string]slog.Value {
	m := make(map[string]slog.Value)
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value
		return true
	})
	return m
}

func wsEchoHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			http.Error(w, "upgrade required", http.StatusBadRequest)
			return
		}
		conn, brw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := brw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"); err != nil {
			return
		}
		if err := brw.Flush(); err != nil {
			return
		}
		_, _ = io.Copy(conn, brw)
	})
}

func metricsHasLine(body, line string) bool {
	for _, l := range strings.Split(body, "\n") {
		if l == line {
			return true
		}
	}
	return false
}

func recvLine(t *testing.T, lines <-chan string) string {
	t.Helper()
	line, ok := testwait.Recv(t, "next stream line", lines, nil)
	if !ok {
		t.Fatal("stream ended before the expected line")
	}
	return line
}

type getResult struct {
	status int
	gen    string
	err    error
}

func startGet(client *http.Client, url string) <-chan getResult {
	out := make(chan getResult, 1)
	go func() {
		resp, err := client.Get(url)
		if err != nil {
			out <- getResult{err: err}
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		out <- getResult{status: resp.StatusCode, gen: resp.Header.Get("X-Gen")}
	}()
	return out
}

func awaitGet(t *testing.T, ch <-chan getResult) getResult {
	t.Helper()
	r, _ := testwait.Recv(t, "response", ch, nil)
	if r.err != nil {
		t.Fatal(r.err)
	}
	return r
}

func series(name string, labels ...string) string {
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i := 0; i+1 < len(labels); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `%s="%s"`, labels[i], labels[i+1])
	}
	b.WriteByte('}')
	return b.String()
}

func metricValue(t *testing.T, body, name string) float64 {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		if rest, ok := strings.CutPrefix(line, name+" "); ok {
			v, err := strconv.ParseFloat(rest, 64)
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

func balDo(g *microgateway.Gateway, req *http.Request) (int, string) {
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	var e echo
	_ = json.NewDecoder(rec.Body).Decode(&e)
	return rec.Code, e.Backend
}

func bkCircuitOpen(t *testing.T, body []byte) {
	t.Helper()
	var e struct {
		Error apiError `json:"error"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatal(err)
	}
	if e.Error.Status != http.StatusServiceUnavailable || e.Error.Code != "circuit_open" || !hexID.MatchString(e.Error.RequestID) {
		t.Fatalf("body = %s, want 503 circuit_open with request_id", body)
	}
}

const (
	miTimeouts = "    timeouts:\n      connect: 1h\n      response_header: 1h\n"
	miRR       = "    load_balancer:\n      algorithm: round_robin\n"
	miPassive  = "    passive_health:\n      consecutive_failures: 2\n      base_ejection_time: 30s\n      max_ejection_time: 120s\n"
)

const (
	auIssuer   = "https://idp.example"
	auAudience = "api"
)

var auStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type auKeys struct {
	rsa1, rsa2 *rsa.PrivateKey
	ec256      *ecdsa.PrivateKey
	ec384      *ecdsa.PrivateKey
	ec521      *ecdsa.PrivateKey
	edPriv     ed25519.PrivateKey
}

var (
	auKeysOnce sync.Once
	auKeysVal  *auKeys
)

// auTestKeys returns the shared test key set, generated once per test binary.
func auTestKeys(t *testing.T) *auKeys {
	t.Helper()
	auKeysOnce.Do(func() {
		k := &auKeys{}
		var err error
		if k.rsa1, err = rsa.GenerateKey(rand.Reader, 2048); err != nil {
			panic(err)
		}
		if k.rsa2, err = rsa.GenerateKey(rand.Reader, 2048); err != nil {
			panic(err)
		}
		if k.ec256, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader); err != nil {
			panic(err)
		}
		if k.ec384, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader); err != nil {
			panic(err)
		}
		if k.ec521, err = ecdsa.GenerateKey(elliptic.P521(), rand.Reader); err != nil {
			panic(err)
		}
		if _, k.edPriv, err = ed25519.GenerateKey(rand.Reader); err != nil {
			panic(err)
		}
		auKeysVal = k
	})
	return auKeysVal
}

// auHMAC returns a deterministic shared secret for alg, exactly the minimum length for that algorithm.
func auHMAC(alg string) string {
	base := alg + ":microgateway-auth-test-secret:0123456789abcdef0123456789abcdef"
	return strings.Repeat(base, 2)[:authkey.MinSecretBytes(alg)]
}

// auEnv is the per-test environment of the root auth tests: an in-memory network, a fake clock and a log sink whose
// contents are checked against the registered secrets when the test ends.
type auEnv struct {
	t       *testing.T
	n       *memNetwork
	clk     *clock.Fake
	mu      sync.Mutex
	logs    bytes.Buffer
	secrets []string
}

func newAuEnv(t *testing.T) *auEnv {
	t.Helper()
	e := &auEnv{t: t, n: newMemNetwork(), clk: clock.NewFake(auStart)}
	t.Cleanup(e.checkLogs)
	return e
}

// checkLogs fails the test when a registered secret appears in the captured logs. The secret value is not printed.
func (e *auEnv) checkLogs() {
	e.mu.Lock()
	defer e.mu.Unlock()
	logs := e.logs.String()
	for _, s := range e.secrets {
		if s != "" && strings.Contains(logs, s) {
			e.t.Errorf("a registered secret appears in the logs")
		}
	}
}

func (e *auEnv) Write(p []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.logs.Write(p)
}

func (e *auEnv) logger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(e, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func (e *auEnv) gateway(doc string, opts ...microgateway.Option) *microgateway.Gateway {
	e.t.Helper()
	all := append([]microgateway.Option{withClock(e.clk), microgateway.WithLogger(e.logger())}, opts...)
	return e.n.gateway(e.t, doc, all...)
}

// secret registers s so that the log check fails when it is logged, and returns it.
func (e *auEnv) secret(s string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.secrets = append(e.secrets, s)
	return s
}

// claims returns standard claims relative to the fake clock; a nil value in extra removes the claim.
func (e *auEnv) claims(extra jwt.MapClaims) jwt.MapClaims {
	c := jwt.MapClaims{
		"iss": auIssuer,
		"aud": auAudience,
		"sub": "user-1",
		"exp": e.clk.Now().Add(time.Hour).Unix(),
	}
	for k, v := range extra {
		if v == nil {
			delete(c, k)
			continue
		}
		c[k] = v
	}
	return c
}

// mint signs c with alg and key, sets kid when non-empty, and registers the token as a secret.
func (e *auEnv) mint(alg, kid string, key any, c jwt.MapClaims) string {
	e.t.Helper()
	method := jwt.GetSigningMethod(alg)
	if method == nil {
		e.t.Fatalf("mint: unknown signing method %q", alg)
	}
	tok := jwt.NewWithClaims(method, c)
	if kid != "" {
		tok.Header["kid"] = kid
	}
	if alg == "none" {
		key = jwt.UnsafeAllowNoneSignatureType
	}
	s, err := tok.SignedString(key)
	if err != nil {
		e.t.Fatal(err)
	}
	return e.secret(s)
}

// get serves a GET request for target with hdr through g and returns the recorded response.
func (e *auEnv) get(g *microgateway.Gateway, target string, hdr http.Header) *httptest.ResponseRecorder {
	e.t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	for name, vals := range hdr {
		for _, v := range vals {
			req.Header.Add(name, v)
		}
	}
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	return rec
}

// auRecorder is an upstream that records the headers and raw query of every request it receives.
type auRecorder struct {
	mu      sync.Mutex
	headers []http.Header
	queries []string
}

func (r *auRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	r.headers = append(r.headers, req.Header.Clone())
	r.queries = append(r.queries, req.URL.RawQuery)
	r.mu.Unlock()
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

// Count returns the number of requests received.
func (r *auRecorder) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.headers)
}

// Last returns the headers and raw query of the most recent request, or nil and "" when none was received.
func (r *auRecorder) Last() (http.Header, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.headers) == 0 {
		return nil, ""
	}
	return r.headers[len(r.headers)-1], r.queries[len(r.queries)-1]
}

// auBackend runs a recording upstream on the environment's network.
func auBackend(e *auEnv) (*testServer, *auRecorder) {
	rec := &auRecorder{}
	return e.n.serve(e.t, rec), rec
}

// auJWK returns the JWK of pub (RSA, ECDSA or Ed25519) with the given kid and alg; an empty alg is omitted.
func auJWK(kid, alg string, pub any) map[string]any {
	b64 := func(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
	m := map[string]any{"kid": kid}
	if alg != "" {
		m["alg"] = alg
	}
	switch k := pub.(type) {
	case *rsa.PublicKey:
		m["kty"] = "RSA"
		m["n"] = b64(k.N.Bytes())
		m["e"] = b64(big.NewInt(int64(k.E)).Bytes())
	case *ecdsa.PublicKey:
		size := (k.Curve.Params().BitSize + 7) / 8
		m["kty"] = "EC"
		m["crv"] = k.Curve.Params().Name
		m["x"] = b64(k.X.FillBytes(make([]byte, size)))
		m["y"] = b64(k.Y.FillBytes(make([]byte, size)))
	case ed25519.PublicKey:
		m["kty"] = "OKP"
		m["crv"] = "Ed25519"
		m["x"] = b64(k)
	}
	return m
}

// auJWKS returns a JWKS document containing keys.
func auJWKS(keys ...map[string]any) []byte {
	b, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		panic(err)
	}
	return b
}

// auHash returns the configuration form of an API key: "sha256:" followed by the hex digest of key.
func auHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// auDoc returns a configuration with one listener, one upstream named be at backendURL, the top-level auth block
// authYAML (which must end with a newline or be empty) and the routes list body routesYAML.
func auDoc(backendURL, authYAML, routesYAML string) string {
	return "version: 1\n" + listenersYAML +
		fmt.Sprintf("upstreams:\n  - name: be\n    hosts:\n      - url: %q\n", backendURL) +
		authYAML + "routes:\n" + routesYAML
}

func auErrorBody(t *testing.T, rec *httptest.ResponseRecorder) apiError {
	t.Helper()
	var body struct {
		Error apiError `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body.Error
}

// auAssert401 checks a 401 response with the unauthorized error body and the given WWW-Authenticate challenge.
func auAssert401(t *testing.T, rec *httptest.ResponseRecorder, challenge string) {
	t.Helper()
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	e := auErrorBody(t, rec)
	if e.Code != httperr.CodeUnauthorized {
		t.Errorf("code = %q, want %q", e.Code, httperr.CodeUnauthorized)
	}
	if e.Message != httperr.Message(httperr.CodeUnauthorized) {
		t.Errorf("message = %q, want %q", e.Message, httperr.Message(httperr.CodeUnauthorized))
	}
	if e.RequestID == "" {
		t.Error("request_id is empty")
	}
	if got := rec.Header().Get("WWW-Authenticate"); got != challenge {
		t.Errorf("WWW-Authenticate = %q, want %q", got, challenge)
	}
}

func amdRoute(name, path, body string) string {
	return fmt.Sprintf("  - name: %s\n    match:\n      path: %s\n    upstream: be\n", name, path) + body
}

func amdAuthYAML(secret, hash string) string {
	return fmt.Sprintf(`auth:
  jwt:
    - name: jwt1
      issuer: https://idp.example
      audiences: [api]
      keys:
        - kid: h1
          algorithm: HS256
          secret: %q
  api_keys:
    - name: keys1
      keys:
        - id: k1
          hash: %q
          consumer: alice
          scopes: [read]
          roles: [user]
          metadata:
            tier: gold
`, secret, hash)
}

const rlStart = 1_700_000_040

func rlFake() *clock.Fake {
	return clock.NewFake(time.Unix(rlStart, 0))
}

func rlDoc(backendURL, global, routesYAML string) string {
	return "version: 1\n" + global + listenersYAML +
		fmt.Sprintf("upstreams:\n  - name: up\n    hosts:\n      - url: %q\n", backendURL) +
		"routes:\n" + routesYAML
}

func rlRoute(name, body string) string {
	return fmt.Sprintf("  - name: %s\n    match:\n      path: /%s\n    upstream: up\n", name, name) + body
}

func rlBackend(t *testing.T, n *memNetwork) (*testServer, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	h := echoHandler("rl")
	s := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		h.ServeHTTP(w, r)
	}))
	return s, &hits
}

func rlGet(t *testing.T, g *microgateway.Gateway, target, remote string, hdr http.Header) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.RemoteAddr = remote
	for name, vals := range hdr {
		for _, v := range vals {
			req.Header.Add(name, v)
		}
	}
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	return rec
}

func rlExpectRemaining(t *testing.T, rec *httptest.ResponseRecorder, remaining string) {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("RateLimit-Remaining"); got != remaining {
		t.Fatalf("RateLimit-Remaining = %q, want %q", got, remaining)
	}
}

func rlExpectLimited(t *testing.T, rec *httptest.ResponseRecorder, retryAfter string) {
	t.Helper()
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != retryAfter {
		t.Fatalf("Retry-After = %q, want %q", got, retryAfter)
	}
	e := decodeErr(t, rec.Result())
	if e.Code != "rate_limited" {
		t.Fatalf("code = %q, want rate_limited", e.Code)
	}
	if e.RequestID == "" {
		t.Fatal("request_id is empty")
	}
}

func rlExhaust(t *testing.T, g *microgateway.Gateway, path, remote string, hdr http.Header) {
	t.Helper()
	for i := range 3 {
		rlExpectRemaining(t, rlGet(t, g, path, remote, hdr), strconv.Itoa(2-i))
	}
	rlExpectLimited(t, rlGet(t, g, path, remote, hdr), "1200")
}

type rsRoute struct{ name, path, extra string }

type rsDoc struct {
	hosts                          []string
	lb, breaker, passive, timeouts string
	routes                         []rsRoute
}

const rsBreaker = "      window: 10s\n      buckets: 10\n      min_requests: 100\n      failure_ratio: 1\n      consecutive_failures: 3\n      open_timeout: 30s\n      half_open_probes: 2\n      success_threshold: 2\n"

func rsWriteBlock(b *strings.Builder, body string) {
	for _, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			b.WriteString("      " + line + "\n")
		}
	}
}

func (d rsDoc) yaml() string {
	var b strings.Builder
	b.WriteString("version: 1\n")
	b.WriteString(listenersYAML)
	b.WriteString("upstreams:\n  - name: up\n    load_balancer:\n")
	lb := d.lb
	if lb == "" {
		lb = "round_robin"
	}
	fmt.Fprintf(&b, "      algorithm: %s\n", lb)
	b.WriteString("    timeouts:\n")
	timeouts := d.timeouts
	if timeouts == "" {
		timeouts = "connect: 1h\nresponse_header: 1h\n"
	}
	rsWriteBlock(&b, timeouts)
	b.WriteString("    hosts:\n")
	for _, h := range d.hosts {
		fmt.Fprintf(&b, "      - url: %q\n", h)
	}
	if d.breaker != "" {
		b.WriteString("    circuit_breaker:\n")
		rsWriteBlock(&b, d.breaker)
	}
	if d.passive != "" {
		b.WriteString("    passive_health:\n")
		rsWriteBlock(&b, d.passive)
	}
	b.WriteString("routes:\n")
	routes := d.routes
	if len(routes) == 0 {
		routes = []rsRoute{{name: "all", path: "/"}}
	}
	for _, r := range routes {
		path := r.path
		if path == "" {
			path = "/"
		}
		fmt.Fprintf(&b, "  - name: %s\n    match:\n      path: %s\n    upstream: up\n", r.name, path)
		if r.extra != "" {
			extra := r.extra
			if !strings.HasSuffix(extra, "\n") {
				extra += "\n"
			}
			b.WriteString("    " + extra)
		}
	}
	return b.String()
}

type rsBackend struct {
	URL    string
	srv    *testServer
	hits   atomic.Int64
	mu     sync.Mutex
	bodies [][]byte
	script func(n int64, w http.ResponseWriter, r *http.Request)
	hitSig chan struct{}
}

func newRSBackend(t *testing.T, n testNetwork, script func(n int64, w http.ResponseWriter, r *http.Request)) *rsBackend {
	t.Helper()
	b := &rsBackend{script: script, hitSig: make(chan struct{})}
	b.srv = n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		b.mu.Lock()
		b.bodies = append(b.bodies, data)
		seq := b.hits.Add(1)
		close(b.hitSig)
		b.hitSig = make(chan struct{})
		b.mu.Unlock()
		if b.script == nil {
			_, _ = io.WriteString(w, "ok")
			return
		}
		b.script(seq, w, r)
	}))
	b.URL = b.srv.URL
	return b
}

func (b *rsBackend) hitCount() int64 {
	return b.hits.Load()
}

func (b *rsBackend) hitSignal() (int64, <-chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.hits.Load(), b.hitSig
}

func (b *rsBackend) body(i int) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bodies[i]
}

func rsStatusSeq(codes ...int) func(n int64, w http.ResponseWriter, r *http.Request) {
	return func(n int64, w http.ResponseWriter, _ *http.Request) {
		code := codes[min(int(n)-1, len(codes)-1)]
		w.Header().Set("X-Attempt", strconv.FormatInt(n, 10))
		w.WriteHeader(code)
		_, _ = fmt.Fprintf(w, "r%d", n)
	}
}

func rsReset(_ int64, w http.ResponseWriter, _ *http.Request) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		return
	}
	_ = conn.Close()
}

func rsHoldUntil(ch <-chan struct{}) func(n int64, w http.ResponseWriter, r *http.Request) {
	return func(_ int64, w http.ResponseWriter, r *http.Request) {
		select {
		case <-ch:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "ok")
	}
}

func rsGet(t *testing.T, client *http.Client, url, method string, body io.Reader, hdr http.Header) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header[k] = v
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, data
}

func rsMetric(t *testing.T, g *microgateway.Gateway, name string, labels ...string) float64 {
	t.Helper()
	return metricValue(t, metricsText(t, g), series(name, labels...))
}

func rsMetricOr0(t *testing.T, g *microgateway.Gateway, name string, labels ...string) float64 {
	t.Helper()
	prefix := series(name, labels...) + " "
	for _, line := range strings.Split(metricsText(t, g), "\n") {
		if rest, ok := strings.CutPrefix(line, prefix); ok {
			v, err := strconv.ParseFloat(rest, 64)
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
	}
	return 0
}

func rsStatus(t *testing.T, g *microgateway.Gateway) upstream.Status {
	t.Helper()
	return upstreamStatus(t, g, "up")
}

func rsHost(t *testing.T, g *microgateway.Gateway, url string) upstream.HostStatus {
	t.Helper()
	return hostByURL(rsStatus(t, g), url)
}

func rsTransitions(h *captureHandler) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, r := range h.records {
		if r.Message != "circuit breaker state changed" {
			continue
		}
		attrs := recordAttrs(r)
		scope := attrs["scope"].String()
		host := attrs["host"].String()
		from := attrs["from"].String()
		to := attrs["to"].String()
		reason := attrs["reason"].String()
		want := slog.LevelInfo
		if to == "open" {
			want = slog.LevelWarn
		}
		if r.Level != want {
			panic(fmt.Sprintf("circuit breaker log for to=%s has level %v, want %v", to, r.Level, want))
		}
		out = append(out, fmt.Sprintf("%s:%s:%s>%s:%s", scope, host, from, to, reason))
	}
	return out
}

var rsExpositionLine = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*(\{([a-zA-Z_][a-zA-Z0-9_]*="([^"\\]|\\.)*",?)*\})? (-?[0-9.eE+-]+|NaN|\+Inf)$`)

func rsCheckExposition(t *testing.T, text string) {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if line == "" || strings.HasPrefix(line, "# HELP ") || strings.HasPrefix(line, "# TYPE ") {
			continue
		}
		if !rsExpositionLine.MatchString(line) {
			t.Errorf("malformed exposition line: %q", line)
		}
	}
}

func rsAdvanceBackoff(t *testing.T, fake *clock.Fake, max time.Duration) {
	t.Helper()
	fake.BlockUntilDue(1, max)
	fake.Advance(max)
}

type fakeResolver struct {
	mu      sync.Mutex
	answers []string
	err     error
}

func (r *fakeResolver) set(answers []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.answers = slices.Clone(answers)
	r.err = nil
}

func (r *fakeResolver) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func (r *fakeResolver) LookupHost(_ context.Context, _ string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return nil, r.err
	}
	return slices.Clone(r.answers), nil
}

func upstreamStatus(t *testing.T, g *microgateway.Gateway, name string) upstream.Status {
	t.Helper()
	rec := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/upstreams", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/upstreams status = %d, want 200", rec.Code)
	}
	var list []upstream.Status
	if err := json.NewDecoder(rec.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	for _, st := range list {
		if st.Name == name {
			return st
		}
	}
	t.Fatalf("upstream %q not in /upstreams", name)
	return upstream.Status{}
}

func hostByURL(st upstream.Status, rawURL string) upstream.HostStatus {
	for _, h := range st.Hosts {
		if h.URL == rawURL {
			return h
		}
	}
	return upstream.HostStatus{}
}

type backendHandle struct {
	URL    string
	Status atomic.Int32
	Health atomic.Int32
	Hits   atomic.Int64
	Probes atomic.Int64
}

func failingBackend(t *testing.T, n testNetwork, name string) *backendHandle {
	t.Helper()
	b := &backendHandle{}
	b.Status.Store(http.StatusOK)
	b.Health.Store(http.StatusOK)
	srv := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/health" {
			b.Probes.Add(1)
			w.WriteHeader(int(b.Health.Load()))
		} else {
			b.Hits.Add(1)
			w.WriteHeader(int(b.Status.Load()))
		}
		_ = json.NewEncoder(w).Encode(echo{Backend: name, Path: r.URL.Path})
	}))
	b.URL = srv.URL
	return b
}

func waitProbes(t *testing.T, fake *clock.Fake, completed func() float64, want float64, armed int) {
	t.Helper()
	eventually(t, fmt.Sprintf("%v completed probes", want), func() bool { return completed() == want }, func() string {
		return fmt.Sprintf("completed probes = %v", completed())
	})
	fake.BlockUntil(armed)
}

// goroutineInfo is one live goroutine: its kind (creating go statement, fixed for its lifetime) and its stack.
type goroutineInfo struct{ kind, stack string }

// goroutineSnap holds the goroutines alive at one moment, by goroutine ID.
type goroutineSnap map[int64]goroutineInfo

func snapGoroutines() goroutineSnap {
	buf := make([]byte, 1<<16)
	var n int
	for {
		n = runtime.Stack(buf, true)
		if n < len(buf) {
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	snap := goroutineSnap{}
	for _, block := range strings.Split(string(buf[:n]), "\n\n") {
		lines := strings.Split(block, "\n")
		fields := strings.Fields(lines[0])
		if len(fields) < 2 || fields[0] != "goroutine" {
			continue
		}
		id, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			continue
		}
		entry, creator, site := "", "", false
		for _, line := range lines[1:] {
			if rest, ok := strings.CutPrefix(line, "created by "); ok {
				creator, _, _ = strings.Cut(rest, " in goroutine ")
				site = true
				continue
			}
			if site && strings.HasPrefix(line, "\t") {
				loc, _, _ := strings.Cut(strings.TrimSpace(line), " +0x")
				creator += " " + loc
				site = false
				continue
			}
			if line == "" || strings.HasPrefix(line, "\t") || line == "...additional frames elided..." {
				continue
			}
			entry = line
		}
		kind := creator
		if kind == "" {
			if strings.HasSuffix(entry, ")") {
				entry = entry[:strings.LastIndex(entry, "(")]
			}
			kind = entry
		}
		snap[id] = goroutineInfo{kind: kind, stack: block}
	}
	return snap
}

// goroutinesBeyond describes each kind of goroutine started after pre whose live count exceeds its count in base.
func goroutinesBeyond(pre, base goroutineSnap) []string {
	baseline := map[string]int{}
	for id, g := range base {
		if _, ok := pre[id]; !ok {
			baseline[g.kind]++
		}
	}
	now := snapGoroutines()
	counts := map[string]int{}
	examples := map[string]int64{}
	for id, g := range now {
		if _, ok := pre[id]; ok {
			continue
		}
		counts[g.kind]++
		if ex, seen := examples[g.kind]; !seen || id < ex {
			examples[g.kind] = id
		}
	}
	var out []string
	for kind, count := range counts {
		if count > baseline[kind] {
			out = append(out, fmt.Sprintf("%d goroutines of kind %s (baseline %d), for example:\n%s",
				count, kind, baseline[kind], now[examples[kind]].stack))
		}
	}
	sort.Strings(out)
	return out
}

// awaitGoroutinesSettled waits until no kind of goroutine started after pre is more numerous than at base: goroutines
// that predate pre (earlier tests) are ignored, transient ones only delay the wait, and a leaked one is named on the
// hang guard.
func awaitGoroutinesSettled(t *testing.T, what string, pre, base goroutineSnap) {
	t.Helper()
	eventually(t, what, func() bool { return len(goroutinesBeyond(pre, base)) == 0 }, func() string {
		return strings.Join(goroutinesBeyond(pre, base), "\n\n")
	})
}
