package proxy

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/reqctx"
)

func TestExchangeCompleteOnEOF(t *testing.T) {
	t.Run("full body", func(t *testing.T) {
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "hello world")
		}))
		t.Cleanup(backend.Close)
		fake := clock.NewFake(time.Unix(0, 0))
		up := newUpstream(t, backend.URL, fake)

		ex := &reqctx.Exchange{}
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req = req.WithContext(reqctx.WithExchange(req.Context(), ex))
		rec := httptest.NewRecorder()
		New(RouteSpec{}, up, Deps{Clock: fake}).ServeHTTP(rec, req)

		if got := rec.Body.String(); got != "hello world" {
			t.Errorf("body = %q, want %q", got, "hello world")
		}
		if !ex.Complete() {
			t.Error("Complete() = false after the full body was read, want true")
		}
	})

	t.Run("truncated chunked body", func(t *testing.T) {
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("response writer cannot hijack")
				return
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			defer func() { _ = conn.Close() }()
			_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n")
		}))
		t.Cleanup(backend.Close)
		fake := clock.NewFake(time.Unix(0, 0))
		up := newUpstream(t, backend.URL, fake)

		ex := &reqctx.Exchange{}
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req = req.WithContext(reqctx.WithExchange(req.Context(), ex))
		rec := httptest.NewRecorder()
		New(RouteSpec{}, up, Deps{Clock: fake}).ServeHTTP(rec, req)

		if ex.Complete() {
			t.Error("Complete() = true after the upstream body was cut short, want false")
		}
	})

	t.Run("no exchange in context", func(t *testing.T) {
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "hello world")
		}))
		t.Cleanup(backend.Close)
		fake := clock.NewFake(time.Unix(0, 0))
		up := newUpstream(t, backend.URL, fake)

		rec := httptest.NewRecorder()
		New(RouteSpec{}, up, Deps{Clock: fake}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

		if got := rec.Body.String(); got != "hello world" {
			t.Errorf("body = %q, want %q", got, "hello world")
		}
	})
}

func TestExchangeSkipsUpgrade(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("response writer cannot hijack")
			return
		}
		conn, buf, err := hj.Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close() }()
		if _, err := io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n"); err != nil {
			return
		}
		for {
			line, err := buf.ReadString('\n')
			if line != "" {
				if _, werr := io.WriteString(conn, line); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}))
	t.Cleanup(backend.Close)

	fake := clock.NewFake(time.Unix(0, 0))
	up := newUpstream(t, backend.URL, fake)
	proxy := New(RouteSpec{}, up, Deps{Clock: fake})
	ex := &reqctx.Exchange{}
	ex.Finish()
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r.WithContext(reqctx.WithExchange(r.Context(), ex)))
	}))
	t.Cleanup(gw.Close)

	conn, err := net.Dial("tcp", gw.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: gateway.test\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	if _, err := io.WriteString(conn, "ping\n"); err != nil {
		t.Fatal(err)
	}
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if line != "ping\n" {
		t.Errorf("echo = %q, want %q", line, "ping\n")
	}
	if !ex.Complete() {
		t.Error("Complete() = false after the 101 upgrade, want the exchange left untouched (true)")
	}
}
