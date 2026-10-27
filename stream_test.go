package lychgate

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func recvLine(t *testing.T, lines <-chan string) string {
	t.Helper()
	select {
	case line, ok := <-lines:
		if !ok {
			t.Fatal("stream ended before the expected line")
		}
		return line
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a stream line")
	}
	return ""
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
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	_, err = io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: gw.test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
	if err != nil {
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

	for _, msg := range []string{"hello\n", "again\n"} {
		if _, err := io.WriteString(conn, msg); err != nil {
			t.Fatal(err)
		}
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
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
