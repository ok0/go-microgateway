package e2e

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/upstream"
)

func wsEchoBackend(t *testing.T) *httptest.Server {
	t.Helper()
	be := httptest.NewServer(wsEchoHandler())
	t.Cleanup(be.Close)
	return be
}

func TestAccessLogRecords(t *testing.T) {
	good := backend(t, "good")
	doc := "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: api
    match:
      path: /api
    upstream: up
`, good.URL)
	logs := &captureHandler{}
	g := gwWith(t, doc, microgateway.WithLogger(slog.New(logs)))

	serve(t, g, httptest.NewRequest(http.MethodGet, "/api/x?secret=1", nil))
	nf := httptest.NewRecorder()
	g.ServeHTTP(nf, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if nf.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", nf.Code)
	}

	ws := wsEchoBackend(t)
	wsDoc := strings.Replace(doc, good.URL, ws.URL, 1)
	wsGW := gwWith(t, wsDoc, microgateway.WithLogger(slog.New(logs)))
	wsTS := httptest.NewServer(wsGW)
	t.Cleanup(wsTS.Close)
	conn, err := net.Dial("tcp", strings.TrimPrefix(wsTS.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.WriteString(conn, "GET /api/ws HTTP/1.1\r\nHost: gw.test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status = %d, want 101", resp.StatusCode)
	}
	if _, err := io.WriteString(conn, "hello\n"); err != nil {
		t.Fatal(err)
	}
	if line, err := br.ReadString('\n'); err != nil || line != "hello\n" {
		t.Fatalf("echo = %q, %v", line, err)
	}
	_ = conn.Close()

	eventually(t, func() bool { return len(logs.requests()) == 3 })
	recs := logs.requests()
	if len(recs) != 3 {
		t.Fatalf("request records = %d, want 3", len(recs))
	}

	proxied := recordAttrs(recs[0])
	for _, k := range []string{"request_id", "method", "path", "route", "upstream", "status", "bytes", "latency_ms", "client_ip"} {
		if _, ok := proxied[k]; !ok {
			t.Fatalf("proxied record lacks %q", k)
		}
	}
	if got := proxied["path"].String(); got != "/api/x" {
		t.Fatalf("path = %q, want /api/x", got)
	}
	if got := proxied["route"].String(); got != "api" {
		t.Fatalf("route = %q, want api", got)
	}
	if got := proxied["upstream"].String(); got != "up" {
		t.Fatalf("upstream = %q, want up", got)
	}
	if got := proxied["status"].Int64(); got != 200 {
		t.Fatalf("status = %d, want 200", got)
	}
	for _, v := range proxied {
		if strings.Contains(v.String(), "secret") {
			t.Fatalf("record leaks the query string: %q", v.String())
		}
	}

	notFound := recordAttrs(recs[1])
	if got := notFound["route"].String(); got != "" {
		t.Fatalf("404 route = %q, want empty", got)
	}
	if got := notFound["status"].Int64(); got != 404 {
		t.Fatalf("404 status = %d, want 404", got)
	}
	if _, ok := notFound["upstream"]; ok {
		t.Fatal("404 record has an upstream attribute")
	}

	upgrade := recordAttrs(recs[2])
	if got := upgrade["status"].Int64(); got != 101 {
		t.Fatalf("upgrade status = %d, want 101", got)
	}
}

func TestRequestMetrics(t *testing.T) {
	good := backend(t, "good")
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	badAddr := closed.Addr().String()
	_ = closed.Close()

	doc := "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: good
    hosts:
      - url: %q
  - name: bad
    hosts:
      - url: "http://%s"
routes:
  - name: api
    match:
      path: /api
    upstream: good
  - name: down
    match:
      path: /down
    upstream: bad
`, good.URL, badAddr)
	g := gwWith(t, doc)

	for _, req := range []struct{ method, path string }{
		{http.MethodGet, "/api"},
		{http.MethodGet, "/nope"},
		{http.MethodGet, "/down"},
		{"PURGE", "/api"},
	} {
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, httptest.NewRequest(req.method, req.path, nil))
	}

	rec := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != metrics.ContentType {
		t.Fatalf("Content-Type = %q, want %q", ct, metrics.ContentType)
	}
	body := rec.Body.String()
	checkMetricPrefix(t, body)

	goodHost := strings.TrimPrefix(good.URL, "http://")
	for _, line := range []string{
		`microgateway_requests_total{route="api",method="GET",code="200"} 1`,
		`microgateway_requests_total{route="unmatched",method="GET",code="404"} 1`,
		`microgateway_requests_total{route="down",method="GET",code="502"} 1`,
		`microgateway_requests_total{route="api",method="OTHER",code="200"} 1`,
		`microgateway_request_duration_seconds_count{route="api"} 2`,
		`microgateway_request_duration_seconds_bucket{route="api",le="+Inf"} 2`,
		`microgateway_upstream_attempts_total{upstream="good",host="` + goodHost + `",outcome="success"} 2`,
		`microgateway_upstream_attempts_total{upstream="bad",host="` + badAddr + `",outcome="failure"} 1`,
	} {
		if !metricsHasLine(body, line) {
			t.Fatalf("metrics lack line %q\n%s", line, body)
		}
	}
	if !strings.Contains(body, `microgateway_request_duration_seconds_sum{route="api"} `) {
		t.Fatalf("metrics lack the duration sum for route api\n%s", body)
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

func adminGet(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

func TestAdminHandler(t *testing.T) {
	hold := holdBackendForAdmin(t)
	secret := "super-secret-signing-key-0123456789"
	doc := routeDoc(hold.URL, "") + fmt.Sprintf(`auth:
  jwt:
    - name: jwt1
      issuer: iss
      audiences: [aud]
      keys:
        - kid: k1
          algorithm: HS256
          secret: %s
  api_keys:
    - name: keys1
      keys:
        - id: k1
          hash: sha256:%s
          consumer: alice
`, secret, strings.Repeat("a", 64))
	g := gwWith(t, doc)
	h := g.AdminHandler()

	if rec := adminGet(t, h, http.MethodGet, "/livez"); rec.Code != http.StatusOK {
		t.Fatalf("/livez status = %d, want 200", rec.Code)
	}

	if rec := adminGet(t, h, http.MethodGet, "/readyz"); rec.Code != http.StatusOK {
		t.Fatalf("/readyz status = %d, want 200", rec.Code)
	}
	g.SetReady(false)
	rec := adminGet(t, h, http.MethodGet, "/readyz")
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "not_ready") {
		t.Fatalf("/readyz after SetReady(false) = %d %q, want 503 not_ready", rec.Code, rec.Body.String())
	}
	if g.Ready() {
		t.Fatal("Ready() = true after SetReady(false)")
	}
	g.SetReady(true)

	rec = adminGet(t, h, http.MethodGet, "/config")
	if rec.Code != http.StatusOK {
		t.Fatalf("/config status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "REDACTED") {
		t.Fatal("/config body lacks REDACTED")
	}
	if strings.Contains(rec.Body.String(), secret) {
		t.Fatal("/config body leaks the JWT secret")
	}
	if got := g.Config().Auth.JWT[0].Keys[0].Secret.Reveal(); got != secret {
		t.Fatalf("g.Config() secret = %q, want the original", got)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/hold", nil))
	}()
	var list []upstream.Status
	eventually(t, func() bool {
		rec := adminGet(t, h, http.MethodGet, "/upstreams")
		list = nil
		if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
			t.Fatal(err)
		}
		return len(list) == 1 && len(list[0].Hosts) == 1 && list[0].Hosts[0].InFlight == 1
	})
	host := list[0].Hosts[0]
	if list[0].Name != "up" || host.URL != hold.URL || !host.Healthy || host.Ejected || host.BreakerState != "closed" || host.Weight != 1 {
		t.Fatalf("upstream status = %+v", list[0])
	}
	close(hold.release)
	<-done
	eventually(t, func() bool {
		rec := adminGet(t, h, http.MethodGet, "/upstreams")
		var l []upstream.Status
		if err := json.Unmarshal(rec.Body.Bytes(), &l); err != nil {
			t.Fatal(err)
		}
		return len(l) == 1 && len(l[0].Hosts) == 1 && l[0].Hosts[0].InFlight == 0
	})

	rec = adminGet(t, h, http.MethodGet, "/metrics")
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("/metrics Content-Type = %q, want text/plain", ct)
	}

	if rec := adminGet(t, h, http.MethodPost, "/livez"); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", rec.Code)
	}
	if rec := adminGet(t, h, http.MethodGet, "/unknown"); rec.Code != http.StatusNotFound {
		t.Fatalf("/unknown status = %d, want 404", rec.Code)
	}
}

func TestAdminPathsNotOnDataHandler(t *testing.T) {
	be := backend(t, "b")
	apiOnly := "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: api
    match:
      path: /api
    upstream: up
`, be.URL)
	g := gwWith(t, apiOnly)
	for _, path := range []string{"/metrics", "/config", "/readyz"} {
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want 404", path, rec.Code)
		}
		if e := decodeErr(t, rec.Result()); e.Code != "route_not_found" {
			t.Fatalf("%s code = %q, want route_not_found", path, e.Code)
		}
	}

	catchAll := gw(t, oneRouteDoc("", be.URL))
	_, e := serve(t, catchAll, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if e.Path != "/metrics" {
		t.Fatalf("backend path = %q, want /metrics", e.Path)
	}
}

type holdServer struct {
	URL     string
	release chan struct{}
}

func holdBackendForAdmin(t *testing.T) *holdServer {
	t.Helper()
	release := make(chan struct{})
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}\n"))
	}))
	t.Cleanup(be.Close)
	return &holdServer{URL: be.URL, release: release}
}
