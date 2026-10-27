package lychgate

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ok0/lychgate/internal/metrics"
)

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

func wsEchoBackend(t *testing.T) *httptest.Server {
	t.Helper()
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}))
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
	g := gwWith(t, doc, WithLogger(slog.New(logs)))

	serve(t, g, httptest.NewRequest(http.MethodGet, "/api/x?secret=1", nil))
	nf := httptest.NewRecorder()
	g.ServeHTTP(nf, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if nf.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", nf.Code)
	}

	ws := wsEchoBackend(t)
	wsDoc := strings.Replace(doc, good.URL, ws.URL, 1)
	wsGW := gwWith(t, wsDoc, WithLogger(slog.New(logs)))
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

func metricsHasLine(body, line string) bool {
	for _, l := range strings.Split(body, "\n") {
		if l == line {
			return true
		}
	}
	return false
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

	goodHost := strings.TrimPrefix(good.URL, "http://")
	for _, line := range []string{
		`lychgate_requests_total{route="api",method="GET",code="200"} 1`,
		`lychgate_requests_total{route="unmatched",method="GET",code="404"} 1`,
		`lychgate_requests_total{route="down",method="GET",code="502"} 1`,
		`lychgate_requests_total{route="api",method="OTHER",code="200"} 1`,
		`lychgate_request_duration_seconds_count{route="api"} 2`,
		`lychgate_request_duration_seconds_bucket{route="api",le="+Inf"} 2`,
		`lychgate_upstream_attempts_total{upstream="good",host="` + goodHost + `",outcome="success"} 2`,
		`lychgate_upstream_attempts_total{upstream="bad",host="` + badAddr + `",outcome="error"} 1`,
	} {
		if !metricsHasLine(body, line) {
			t.Fatalf("metrics lack line %q\n%s", line, body)
		}
	}
	if !strings.Contains(body, `lychgate_request_duration_seconds_sum{route="api"} `) {
		t.Fatalf("metrics lack the duration sum for route api\n%s", body)
	}
}
