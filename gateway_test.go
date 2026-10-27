package lychgate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/reqctx"
)

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

func backend(t *testing.T, name string) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(echo{
			Backend: name,
			Path:    r.URL.Path,
			RawPath: r.URL.RawPath,
			Host:    r.Host,
			Headers: r.Header,
		})
	}))
	t.Cleanup(s.Close)
	return s
}

func gw(t *testing.T, doc string) *Gateway {
	t.Helper()
	g, err := NewFromYAML([]byte(doc))
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

func serve(t *testing.T, g *Gateway, req *http.Request) (*http.Response, echo) {
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

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTimer(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if cond() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("condition not met within 5s")
		case <-tick.C:
			tick.Reset(5 * time.Millisecond)
		}
	}
}

func TestNewFromYAMLInvalid(t *testing.T) {
	_, err := NewFromYAML([]byte("version: 2\nlisteners: []\n"))
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want *config.ValidationError", err)
	}
}

func TestNewNilConfig(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("New(nil) returned no error")
	}
}

func TestProxyThroughHTTPTestServer(t *testing.T) {
	be := backend(t, "a")
	g := gw(t, oneRouteDoc("", be.URL))
	ts := httptest.NewServer(g)
	t.Cleanup(ts.Close)

	resp, err := ts.Client().Get(ts.URL + "/hello")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var e echo
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatal(err)
	}
	if e.Backend != "a" {
		t.Fatalf("backend = %q, want a", e.Backend)
	}
	if c := g.Config(); c == nil || c.Global.RequestIDHeader != "X-Request-ID" {
		t.Fatalf("Config() = %+v, want RequestIDHeader X-Request-ID", c)
	}
}

func TestConcurrent(t *testing.T) {
	be := backend(t, "a")
	g := gw(t, oneRouteDoc("", be.URL))
	ts := httptest.NewServer(g)
	t.Cleanup(ts.Close)
	client := ts.Client()

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 25 {
				resp, err := client.Get(ts.URL + "/c")
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Errorf("status = %d, want 200", resp.StatusCode)
				}
			}
		}()
	}
	wg.Wait()
}

func TestCloseNoLeak(t *testing.T) {
	base := runtime.NumGoroutine()

	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	g, err := NewFromYAML([]byte(oneRouteDoc("", be.URL)))
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(g)
	client := &http.Client{Transport: &http.Transport{}}
	for range 10 {
		resp, err := client.Get(ts.URL + "/l")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	client.CloseIdleConnections()
	ts.Close()
	be.Close()
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return runtime.NumGoroutine() <= base })
}

func TestErrors(t *testing.T) {
	ok := backend(t, "ok")
	blocked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(blocked.Close)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedURL := "http://" + ln.Addr().String()
	ln.Close()

	cases := []struct {
		name   string
		doc    string
		path   string
		status int
		code   string
	}{
		{
			name: "no route",
			doc: "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: api
    match:
      path: /api
    upstream: up
`, ok.URL),
			path:   "/nope",
			status: http.StatusNotFound,
			code:   "route_not_found",
		},
		{
			name:   "connection refused",
			doc:    "version: 1\n" + listenersYAML + fmt.Sprintf("upstreams:\n  - name: up\n    hosts:\n      - url: %q\nroutes:\n  - name: all\n    upstream: up\n", closedURL),
			path:   "/",
			status: http.StatusBadGateway,
			code:   "bad_gateway",
		},
		{
			name:   "response header timeout",
			doc:    "version: 1\n" + listenersYAML + fmt.Sprintf("upstreams:\n  - name: up\n    hosts:\n      - url: %q\n    timeouts:\n      response_header: 50ms\nroutes:\n  - name: all\n    upstream: up\n", blocked.URL),
			path:   "/",
			status: http.StatusGatewayTimeout,
			code:   "gateway_timeout",
		},
		{
			name:   "route timeout",
			doc:    "version: 1\n" + listenersYAML + fmt.Sprintf("upstreams:\n  - name: up\n    hosts:\n      - url: %q\nroutes:\n  - name: all\n    upstream: up\n    timeout: 50ms\n", blocked.URL),
			path:   "/",
			status: http.StatusGatewayTimeout,
			code:   "gateway_timeout",
		},
		{
			name:   "no healthy upstream",
			doc:    "version: 1\n" + listenersYAML + fmt.Sprintf("upstreams:\n  - name: up\n    hosts:\n      - url: %q\n        weight: 0\nroutes:\n  - name: all\n    upstream: up\n", ok.URL),
			path:   "/",
			status: http.StatusServiceUnavailable,
			code:   "no_healthy_upstream",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := gw(t, tc.doc)
			rec := httptest.NewRecorder()
			g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			resp := rec.Result()
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.status)
			}
			if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
				t.Fatalf("Content-Type = %q", ct)
			}
			if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
				t.Fatalf("Cache-Control = %q", cc)
			}
			reqID := resp.Header.Get("X-Request-ID")
			e := decodeErr(t, resp)
			if e.Code != tc.code {
				t.Fatalf("code = %q, want %q", e.Code, tc.code)
			}
			if e.RequestID == "" || e.RequestID != reqID {
				t.Fatalf("request_id = %q, X-Request-ID = %q", e.RequestID, reqID)
			}
		})
	}
}

func TestForwardingHeaders(t *testing.T) {
	be := backend(t, "a")
	untrusted := gw(t, oneRouteDoc("", be.URL))
	trusted := gw(t, oneRouteDoc(trustedGlobal, be.URL))

	t.Run("untrusted peer", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		req.RemoteAddr = "203.0.113.9:4000"
		req.Header.Set("X-Forwarded-For", "1.2.3.4")
		_, e := serve(t, untrusted, req)
		if got := strings.Join(e.Headers.Values("X-Forwarded-For"), ", "); got != "203.0.113.9" {
			t.Fatalf("X-Forwarded-For = %q, want 203.0.113.9", got)
		}
		if got := e.Headers.Get("X-Forwarded-Proto"); got != "http" {
			t.Fatalf("X-Forwarded-Proto = %q", got)
		}
		if got := e.Headers.Get("X-Forwarded-Host"); got != req.Host {
			t.Fatalf("X-Forwarded-Host = %q, want %q", got, req.Host)
		}
	})

	t.Run("trusted peer", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		req.RemoteAddr = "203.0.113.9:4000"
		req.Header.Set("X-Forwarded-For", "1.2.3.4")
		_, e := serve(t, trusted, req)
		if got := strings.Join(e.Headers.Values("X-Forwarded-For"), ", "); got != "1.2.3.4, 203.0.113.9" {
			t.Fatalf("X-Forwarded-For = %q, want \"1.2.3.4, 203.0.113.9\"", got)
		}
	})

	t.Run("inbound request id kept", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		req.RemoteAddr = "203.0.113.9:4000"
		req.Header.Set("X-Request-ID", "client-id-1")
		resp, e := serve(t, untrusted, req)
		if got := e.Headers.Get("X-Request-ID"); got != "client-id-1" {
			t.Fatalf("backend X-Request-ID = %q", got)
		}
		if got := resp.Header.Get("X-Request-ID"); got != "client-id-1" {
			t.Fatalf("response X-Request-ID = %q", got)
		}
	})

	invalid := []struct {
		name    string
		inbound string
	}{
		{name: "contains space", inbound: "client id"},
		{name: "too long", inbound: strings.Repeat("a", 200)},
		{name: "missing"},
	}
	for _, tc := range invalid {
		t.Run("request id "+tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/x", nil)
			req.RemoteAddr = "203.0.113.9:4000"
			if tc.inbound != "" {
				req.Header.Set("X-Request-ID", tc.inbound)
			}
			resp, e := serve(t, untrusted, req)
			got := e.Headers.Get("X-Request-ID")
			if !hexID.MatchString(got) {
				t.Fatalf("backend X-Request-ID = %q, want 32 hex digits", got)
			}
			if got != resp.Header.Get("X-Request-ID") {
				t.Fatalf("backend %q and response %q differ", got, resp.Header.Get("X-Request-ID"))
			}
			if got == tc.inbound {
				t.Fatalf("inbound request id was kept")
			}
		})
	}

	t.Run("hop-by-hop headers removed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		req.RemoteAddr = "203.0.113.9:4000"
		req.Header.Set("Connection", "X-Hop")
		req.Header.Set("X-Hop", "1")
		_, e := serve(t, untrusted, req)
		if vals := e.Headers.Values("X-Hop"); len(vals) != 0 {
			t.Fatalf("backend saw X-Hop = %q", vals)
		}
	})
}

func TestPathParamAccessor(t *testing.T) {
	ctx := reqctx.WithParams(
		reqctx.WithRequestID(context.Background(), "rid-1"),
		[]reqctx.Param{{Name: "id", Value: "7"}},
	)
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	if got := PathParam(req, "id"); got != "7" {
		t.Fatalf("PathParam = %q, want 7", got)
	}
	if got := RequestIDFromContext(ctx); got != "rid-1" {
		t.Fatalf("RequestIDFromContext = %q, want rid-1", got)
	}
}

func TestRoundRobinGateway(t *testing.T) {
	a := backend(t, "A")
	b := backend(t, "B")
	c := backend(t, "C")
	doc := "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
        weight: 1
      - url: %q
        weight: 1
      - url: %q
        weight: 0
routes:
  - name: all
    upstream: up
`, a.URL, b.URL, c.URL)
	g := gw(t, doc)

	counts := map[string]int{}
	for range 10 {
		_, e := serve(t, g, httptest.NewRequest(http.MethodGet, "/rr", nil))
		counts[e.Backend]++
	}
	if counts["A"] != 5 || counts["B"] != 5 || counts["C"] != 0 {
		t.Fatalf("counts = %v, want A=5 B=5 C=0", counts)
	}
}
