package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/testcert"
)

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("condition not met within 5s")
		case <-ticker.C:
		}
	}
}

func holdBackend(t *testing.T) (*httptest.Server, chan struct{}, chan struct{}) {
	t.Helper()
	entered := make(chan struct{}, 16)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hold" {
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(echoBody{Backend: "hold", Path: r.URL.Path, Headers: r.Header})
	}))
	t.Cleanup(srv.Close)
	return srv, entered, release
}

func wsBackend(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(echoBody{Backend: "ws", Path: r.URL.Path, Headers: r.Header})
			return
		}
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "hijack unsupported", http.StatusInternalServerError)
			return
		}
		conn, brw, err := hj.Hijack()
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
		for {
			line, err := brw.ReadString('\n')
			if err != nil {
				return
			}
			if _, err := brw.WriteString(line); err != nil {
				return
			}
			if err := brw.Flush(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func wsHandshake(t *testing.T, conn net.Conn, host string) *bufio.Reader {
	t.Helper()
	req := "GET /ws HTTP/1.1\r\nHost: " + host + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
	if _, err := io.WriteString(conn, req); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("websocket status = %d, want 101", resp.StatusCode)
	}
	return br
}

func fetchURL(t *testing.T, client *http.Client, url string) (*http.Response, []byte) {
	t.Helper()
	resp, err := client.Get(url)
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

type holdResult struct {
	status int
	err    error
}

func startHold(addr string) <-chan holdResult {
	out := make(chan holdResult, 1)
	go func() {
		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Get("http://" + addr + "/hold")
		if err != nil {
			out <- holdResult{err: err}
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		out <- holdResult{status: resp.StatusCode}
	}()
	return out
}

func readyzStatus(addr string) int {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + "/readyz")
	if err != nil {
		return -1
	}
	resp.Body.Close()
	return resp.StatusCode
}

func lifecycleConfig(global, backend string) string {
	return fmt.Sprintf(`version: 1
%slisteners:
  - name: public
    address: "127.0.0.1:0"
admin:
  address: "127.0.0.1:0"
upstreams:
  - name: backend
    hosts:
      - url: %q
routes:
  - name: all
    match:
      path: /
      path_type: prefix
    upstream: backend
`, global, backend)
}

func tlsConfigYAML(backend, certFile, keyFile, secureExtra string) string {
	return fmt.Sprintf(`version: 1
listeners:
  - name: secure
    address: "127.0.0.1:0"
    tls:
      cert_file: %q
      key_file: %q
%s  - name: plain
    address: "127.0.0.1:0"
admin:
  address: "127.0.0.1:0"
upstreams:
  - name: echo
    hosts:
      - url: %q
routes:
  - name: all
    match:
      path: /
      path_type: prefix
    upstream: echo
`, certFile, keyFile, secureExtra, backend)
}

func TestRunTLS(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, pool := testcert.Write(t, dir)
	backend := wsBackend(t)
	path := writeTemp(t, tlsConfigYAML(backend.URL, certFile, keyFile, ""))

	h := startRun(t, path)
	secure := h.addrs.Listeners["secure"].String()
	plain := h.addrs.Listeners["plain"].String()

	tlsClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{RootCAs: pool},
			ForceAttemptHTTP2: true,
		},
	}
	resp, body := fetchURL(t, tlsClient, "https://"+secure+"/api")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("https status = %d, want 200; body %s", resp.StatusCode, body)
	}
	if resp.ProtoMajor != 2 {
		t.Errorf("https ProtoMajor = %d, want 2", resp.ProtoMajor)
	}
	echo := decodeEcho(t, body)
	if got := echo.Headers.Get("X-Forwarded-Proto"); got != "https" {
		t.Errorf("X-Forwarded-Proto = %q, want https", got)
	}
	if got := echo.Headers.Get("X-Forwarded-Host"); got != secure {
		t.Errorf("X-Forwarded-Host = %q, want %q", got, secure)
	}

	plainClient := &http.Client{Timeout: 5 * time.Second}
	resp, body = fetchURL(t, plainClient, "http://"+plain+"/api")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("http status = %d, want 200; body %s", resp.StatusCode, body)
	}
	echo = decodeEcho(t, body)
	if got := echo.Headers.Get("X-Forwarded-Proto"); got != "http" {
		t.Errorf("plain X-Forwarded-Proto = %q, want http", got)
	}

	conn, err := tls.Dial("tcp", secure, &tls.Config{RootCAs: pool, NextProtos: []string{"http/1.1"}})
	if err != nil {
		t.Fatal(err)
	}
	br := wsHandshake(t, conn, secure)
	if _, err := io.WriteString(conn, "hello\n"); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("hello\n"))
	if _, err := io.ReadFull(br, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello\n" {
		t.Errorf("websocket echo = %q, want %q", got, "hello\n")
	}
	conn.Close()

	if code := h.stop(); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

func TestRunTLSHTTP1Only(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, pool := testcert.Write(t, dir)
	backend := wsBackend(t)
	path := writeTemp(t, tlsConfigYAML(backend.URL, certFile, keyFile, "    http2: false\n"))

	h := startRun(t, path)
	secure := h.addrs.Listeners["secure"].String()
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{RootCAs: pool},
			ForceAttemptHTTP2: true,
		},
	}
	resp, body := fetchURL(t, client, "https://"+secure+"/api")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("https status = %d, want 200; body %s", resp.StatusCode, body)
	}
	if resp.ProtoMajor != 1 {
		t.Errorf("ProtoMajor = %d, want 1", resp.ProtoMajor)
	}

	if code := h.stop(); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

func TestRunBadCertPath(t *testing.T) {
	plain := freePort(t)
	missing := filepath.Join(t.TempDir(), "missing.pem")
	path := writeTemp(t, fmt.Sprintf(`version: 1
listeners:
  - name: plain
    address: %q
  - name: secure
    address: "127.0.0.1:0"
    tls:
      cert_file: %q
      key_file: %q
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
`, plain, missing, missing))

	code, _, errs := runCmd(t, "run", "-config", path)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	if !strings.Contains(errs, "listeners[1].tls.cert_file (line 8): cannot read certificate") {
		t.Errorf("stderr = %q, want it to contain %q", errs, "listeners[1].tls.cert_file (line 8): cannot read certificate")
	}
	if !strings.Contains(errs, "listeners[1].tls.key_file (line 9): cannot read key") {
		t.Errorf("stderr = %q, want it to contain %q", errs, "listeners[1].tls.key_file (line 9): cannot read key")
	}
	if !strings.Contains(errs, ": 2 error(s)") {
		t.Errorf("stderr = %q, want it to contain %q", errs, ": 2 error(s)")
	}
	ln, err := net.Listen("tcp", plain)
	if err != nil {
		t.Fatalf("port %s not released: %v", plain, err)
	}
	ln.Close()
}

func TestRunBindConflict(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	conflict := held.Addr().String()
	free := freePort(t)
	admin := freePort(t)
	path := writeTemp(t, fmt.Sprintf(`version: 1
listeners:
  - name: a
    address: %q
  - name: b
    address: %q
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
  address: %q
`, free, conflict, admin))

	code, _, errs := runCmd(t, "run", "-config", path)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	if !strings.Contains(errs, `listener "b"`) {
		t.Errorf("stderr = %q, want it to contain %q", errs, `listener "b"`)
	}
	for _, addr := range []string{free, admin} {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatalf("address %s not released: %v", addr, err)
		}
		ln.Close()
	}
}

func TestRunGracefulShutdown(t *testing.T) {
	backend, entered, release := holdBackend(t)
	path := writeTemp(t, lifecycleConfig("global:\n  shutdown_delay: 2s\n  drain_timeout: 10s\n", backend.URL))

	h := startRun(t, path)
	public := h.addrs.Listeners["public"].String()
	admin := h.addrs.Admin.String()

	holdRes := startHold(public)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("/hold request did not reach the backend")
	}

	h.sig <- syscall.SIGTERM
	eventually(t, func() bool { return readyzStatus(admin) == http.StatusServiceUnavailable })
	select {
	case r := <-holdRes:
		t.Fatalf("/hold finished before release: %+v", r)
	default:
	}

	fastClient := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	resp, body := fetchURL(t, fastClient, "http://"+public+"/fast")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/fast during shutdown delay: status = %d, want 200; body %s", resp.StatusCode, body)
	}

	close(release)
	select {
	case r := <-holdRes:
		if r.err != nil || r.status != http.StatusOK {
			t.Errorf("/hold result = %+v, want status 200", r)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("/hold did not complete after release")
	}

	if code := h.wait(t, 10*time.Second); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
	if conn, err := net.Dial("tcp", public); err == nil {
		conn.Close()
		t.Error("data listener still accepts connections after shutdown")
	}
}

func TestRunForcedShutdown(t *testing.T) {
	hold, entered, _ := holdBackend(t)
	ws := wsBackend(t)
	path := writeTemp(t, fmt.Sprintf(`version: 1
global:
  drain_timeout: 300ms
listeners:
  - name: public
    address: "127.0.0.1:0"
admin:
  address: "127.0.0.1:0"
upstreams:
  - name: hold
    hosts:
      - url: %q
  - name: ws
    hosts:
      - url: %q
routes:
  - name: ws
    match:
      path: /ws
      path_type: exact
    upstream: ws
  - name: all
    match:
      path: /
      path_type: prefix
    upstream: hold
`, hold.URL, ws.URL))

	h := startRun(t, path)
	public := h.addrs.Listeners["public"].String()

	holdRes := startHold(public)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("/hold request did not reach the backend")
	}

	conn, err := net.Dial("tcp", public)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := wsHandshake(t, conn, public)

	h.sig <- syscall.SIGTERM
	if code := h.wait(t, 5*time.Second); code == 0 {
		t.Fatal("exit code = 0, want nonzero after forced shutdown")
	}

	select {
	case r := <-holdRes:
		if r.err == nil && r.status == http.StatusOK {
			t.Errorf("/hold result = %+v, want an error or non-200", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("/hold client did not return after forced shutdown")
	}

	readErr := make(chan error, 1)
	go func() {
		_, err := br.ReadByte()
		readErr <- err
	}()
	select {
	case err := <-readErr:
		if err == nil {
			t.Error("websocket read returned data, want an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("websocket connection still open after forced shutdown")
	}
}

func TestRunSecondSignal(t *testing.T) {
	hold, entered, _ := holdBackend(t)
	path := writeTemp(t, lifecycleConfig("global:\n  drain_timeout: 1m\n", hold.URL))

	h := startRun(t, path)
	public := h.addrs.Listeners["public"].String()
	admin := h.addrs.Admin.String()

	_ = startHold(public)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("/hold request did not reach the backend")
	}

	h.sig <- syscall.SIGTERM
	eventually(t, func() bool { return readyzStatus(admin) == http.StatusServiceUnavailable })

	h.sig <- syscall.SIGTERM
	if code := h.wait(t, 3*time.Second); code == 0 {
		t.Error("exit code = 0, want nonzero after second signal")
	}
}
