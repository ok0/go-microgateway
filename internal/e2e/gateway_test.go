package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/internal/testwait"
)

func TestNewFromYAMLInvalid(t *testing.T) {
	_, err := microgateway.NewFromYAML([]byte("version: 2\nlisteners: []\n"))
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want *config.ValidationError", err)
	}
}

func TestNewNilConfig(t *testing.T) {
	if _, err := microgateway.New(nil); err == nil {
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
	g, err := microgateway.NewFromYAML([]byte(oneRouteDoc("", be.URL)))
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
	eventually(t, "goroutines to return to the baseline", func() bool { return runtime.NumGoroutine() <= base }, func() string {
		return fmt.Sprintf("goroutines=%d base=%d", runtime.NumGoroutine(), base)
	})
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
	if got := microgateway.PathParam(req, "id"); got != "7" {
		t.Fatalf("PathParam = %q, want 7", got)
	}
	if got := microgateway.RequestIDFromContext(ctx); got != "rid-1" {
		t.Fatalf("RequestIDFromContext = %q, want rid-1", got)
	}
}

func TestRoundRobinGateway(t *testing.T) {
	a := backend(t, "A")
	b := backend(t, "B")
	c := backend(t, "C")
	doc := "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    load_balancer:
      algorithm: round_robin
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

func TestWebSocketEcho(t *testing.T) {
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

	g := gw(t, oneRouteDoc("", be.URL))
	gwTS := httptest.NewServer(g)
	t.Cleanup(gwTS.Close)

	conn, err := net.Dial("tcp", strings.TrimPrefix(gwTS.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(testwait.Guard(t))); err != nil {
		t.Fatal(err)
	}

	_, err = io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: gw.test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
	if err != nil {
		t.Fatalf("write upgrade request: %v", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("read upgrade response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}

	for _, msg := range []string{"hello\n", "again\n"} {
		if _, err := io.WriteString(conn, msg); err != nil {
			t.Fatalf("write echo: %v", err)
		}
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read echo: %v", err)
		}
		if line != msg {
			t.Fatalf("echo = %q, want %q", line, msg)
		}
	}
}

func TestSSEIncremental(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	closeRelease := func() { releaseOnce.Do(func() { close(release) }) }

	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: one\n\n")
		_ = http.NewResponseController(w).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "data: two\n\n")
	}))
	t.Cleanup(be.Close)
	t.Cleanup(closeRelease)

	g := gw(t, oneRouteDoc("", be.URL))
	gwTS := httptest.NewServer(g)
	t.Cleanup(gwTS.Close)

	resp, err := gwTS.Client().Get(gwTS.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	lines := make(chan string, 8)
	go func() {
		defer close(lines)
		reader := bufio.NewReader(resp.Body)
		for {
			line, err := reader.ReadString('\n')
			if line != "" {
				lines <- line
			}
			if err != nil {
				return
			}
		}
	}()

	if first := recvLine(t, lines); first != "data: one\n" {
		t.Fatalf("first line = %q, want %q", first, "data: one\n")
	}
	closeRelease()
	for recvLine(t, lines) != "data: two\n" {
	}
}
