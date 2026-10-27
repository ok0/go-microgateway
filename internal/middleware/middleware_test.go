package middleware

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/metrics"
	"github.com/ok0/lychgate/internal/reqctx"
)

var hex32 = regexp.MustCompile(`^[0-9a-f]{32}$`)

func discard() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestChainOrder(t *testing.T) {
	var order []string
	mk := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name)
				next.ServeHTTP(w, r)
			})
		}
	}
	final := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		order = append(order, "final")
	})
	h := Chain(final, mk("a"), nil, mk("b"), mk("c"))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	want := []string{"a", "b", "c", "final"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Errorf("order = %v, want %v", order, want)
	}
}

func TestRecover(t *testing.T) {
	h := Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/boom" {
			panic("kaboom")
		}
		w.WriteHeader(http.StatusOK)
	}), RequestID("X-Request-ID"), Recover(discard()))
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/boom")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	var body struct {
		Error struct {
			Code      string `json:"code"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "internal_error" {
		t.Errorf("code = %q", body.Error.Code)
	}
	if body.Error.RequestID == "" {
		t.Error("request_id is empty")
	}

	resp2, err := http.Get(srv.URL + "/ok")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("second status = %d, want 200", resp2.StatusCode)
	}
}

func TestRecoverAbortHandler(t *testing.T) {
	var got any
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	func() {
		defer func() { got = recover() }()
		Recover(discard())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			panic(http.ErrAbortHandler)
		})).ServeHTTP(rec, req)
	}()
	if got != http.ErrAbortHandler {
		t.Errorf("recovered = %v, want http.ErrAbortHandler", got)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty", rec.Body.String())
	}
}

func TestRequestID(t *testing.T) {
	const header = "X-Request-ID"
	cases := []struct {
		name    string
		inbound string
		want    string
	}{
		{"kept", "abc-123", "abc-123"},
		{"too long", strings.Repeat("a", 129), ""},
		{"space", "a b", ""},
		{"missing", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ctxID, reqHeader string
			h := RequestID(header)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctxID = reqctx.RequestID(r.Context())
				reqHeader = r.Header.Get(header)
			}))
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.inbound != "" {
				req.Header.Set(header, tc.inbound)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			respID := rec.Header().Get(header)
			if tc.want != "" {
				if ctxID != tc.want || reqHeader != tc.want || respID != tc.want {
					t.Errorf("ids ctx=%q req=%q resp=%q, want %q", ctxID, reqHeader, respID, tc.want)
				}
				return
			}
			if !hex32.MatchString(ctxID) {
				t.Errorf("ctx id = %q, want 32 hex", ctxID)
			}
			if ctxID != reqHeader || ctxID != respID {
				t.Errorf("ids differ: ctx=%q req=%q resp=%q", ctxID, reqHeader, respID)
			}
			if ctxID == tc.inbound {
				t.Errorf("inbound %q was kept", tc.inbound)
			}
		})
	}

	h := RequestID(header)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	r1 := httptest.NewRecorder()
	r2 := httptest.NewRecorder()
	h.ServeHTTP(r1, httptest.NewRequest(http.MethodGet, "/", nil))
	h.ServeHTTP(r2, httptest.NewRequest(http.MethodGet, "/", nil))
	if r1.Header().Get(header) == r2.Header().Get(header) {
		t.Error("two generated IDs are equal")
	}
}

func TestTimeout(t *testing.T) {
	if Timeout(0) != nil {
		t.Error("Timeout(0) is not nil")
	}
	var ok bool
	h := Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, ok = r.Context().Deadline()
	}), Timeout(time.Hour))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if !ok {
		t.Error("handler context has no deadline")
	}
}

func logRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestObserve(t *testing.T) {
	reg := metrics.NewRegistry()
	counter, err := reg.Counter("requests_total", "Requests served.", "route", "method", "code")
	if err != nil {
		t.Fatal(err)
	}
	hist, err := reg.Histogram("request_duration_seconds", "Request latency.", metrics.DefaultBuckets(), "route")
	if err != nil {
		t.Fatal(err)
	}
	rm := &RequestMetrics{Requests: counter, Duration: hist}

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFake(t0)
	ip := netip.MustParseAddr("192.0.2.1")
	h := Observe(logger, clk, true, rm)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/x" {
			info := reqctx.InfoFrom(r.Context())
			info.Route = "r1"
			info.Upstream = "u1"
		}
		clk.Advance(15 * time.Millisecond)
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "hi")
	}))

	req := httptest.NewRequest(http.MethodGet, "/x?q=1", nil)
	ctx := reqctx.WithClientIP(reqctx.WithRequestID(req.Context(), "rid"), ip)
	h.ServeHTTP(httptest.NewRecorder(), req.WithContext(ctx))
	recs := logRecords(t, &buf)
	if len(recs) != 1 {
		t.Fatalf("got %d log records, want 1", len(recs))
	}
	m := recs[0]
	want := map[string]any{
		"request_id": "rid",
		"method":     "GET",
		"path":       "/x",
		"route":      "r1",
		"upstream":   "u1",
		"status":     float64(201),
		"bytes":      float64(2),
		"latency_ms": float64(15),
		"client_ip":  "192.0.2.1",
	}
	for k, v := range want {
		if m[k] != v {
			t.Errorf("%s = %v, want %v", k, m[k], v)
		}
	}
	if got := counter.With("r1", "GET", "201").Value(); got != 1 {
		t.Errorf("requests r1/GET/201 = %v, want 1", got)
	}
	if got := hist.With("r1").Count(); got != 1 {
		t.Errorf("duration r1 count = %v, want 1", got)
	}

	buf.Reset()
	req = httptest.NewRequest(http.MethodGet, "/y", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)
	recs = logRecords(t, &buf)
	if len(recs) != 1 {
		t.Fatalf("got %d log records, want 1", len(recs))
	}
	if recs[0]["route"] != "" {
		t.Errorf("route = %v, want empty", recs[0]["route"])
	}
	if _, ok := recs[0]["upstream"]; ok {
		t.Error("upstream key present for unmatched request")
	}
	if got := counter.With("unmatched", "GET", "201").Value(); got != 1 {
		t.Errorf("requests unmatched/GET/201 = %v, want 1", got)
	}

	buf.Reset()
	req = httptest.NewRequest("PURGE", "/y", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)
	if got := counter.With("unmatched", "OTHER", "201").Value(); got != 1 {
		t.Errorf("requests unmatched/OTHER/201 = %v, want 1", got)
	}

	buf.Reset()
	ph := Observe(logger, clk, true, rm)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	func() {
		defer func() {
			if got := recover(); got != http.ErrAbortHandler {
				t.Errorf("recovered = %v, want http.ErrAbortHandler", got)
			}
		}()
		ph.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/p", nil))
	}()
	recs = logRecords(t, &buf)
	if len(recs) != 1 {
		t.Fatalf("panic: got %d log records, want 1", len(recs))
	}
	if recs[0]["status"] != float64(500) {
		t.Errorf("panic: status = %v, want 500", recs[0]["status"])
	}

	if Observe(nil, clk, false, nil) != nil {
		t.Error("Observe with logging and metrics disabled is not nil")
	}
}

func TestStatusWriterUnwrap(t *testing.T) {
	sw := &statusWriter{ResponseWriter: httptest.NewRecorder()}
	if err := http.NewResponseController(sw).Flush(); err != nil {
		t.Errorf("Flush: %v", err)
	}
}
