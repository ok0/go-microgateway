package cli

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testcert"
	"github.com/ok0/go-microgateway/internal/testwait"
)

// listeningAddrs returns the local addresses of the TCP sockets this process holds bound and unconnected
// (listeners), read from its open descriptors. It first checks that it sees a listener it opens itself, so an
// empty result cannot come from a descriptor table it failed to read.
func listeningAddrs(t *testing.T) map[string]bool {
	t.Helper()
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = probe.Close() }()
	entries, err := os.ReadDir("/dev/fd")
	if err != nil {
		t.Fatal(err)
	}
	addrs := make(map[string]bool)
	for _, e := range entries {
		fd, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		typ, err := syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_TYPE)
		if err != nil || typ != syscall.SOCK_STREAM {
			continue
		}
		if _, err := syscall.Getpeername(fd); !errors.Is(err, syscall.ENOTCONN) {
			continue
		}
		sa, err := syscall.Getsockname(fd)
		if err != nil {
			continue
		}
		var a *net.TCPAddr
		switch s := sa.(type) {
		case *syscall.SockaddrInet4:
			a = &net.TCPAddr{IP: net.IP(s.Addr[:]), Port: s.Port}
		case *syscall.SockaddrInet6:
			a = &net.TCPAddr{IP: net.IP(s.Addr[:]), Port: s.Port}
		default:
			continue
		}
		if a.Port != 0 {
			addrs[a.String()] = true
		}
	}
	if !addrs[probe.Addr().String()] {
		t.Fatalf("own listener %s not found among open descriptors: %v", probe.Addr(), addrs)
	}
	delete(addrs, probe.Addr().String())
	return addrs
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	testwait.Until(t, what, cond, nil)
}

const reloadStep = 250 * time.Millisecond // the default global.reload.debounce

// awaitDue waits until n timers of fake are due within the given duration; what and state (may be nil) describe the wait if the hang guard expires.
func awaitDue(t *testing.T, fake *clock.Fake, n int, within time.Duration, what string, state func() string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	due := make(chan struct{})
	go func() {
		defer close(due)
		_ = fake.WaitDue(ctx, n, within)
	}()
	testwait.Done(t, what, due, state)
}

// eventuallyAdvancing waits until cond holds, as eventually does; whenever a timer of fake is due within step,
// it advances fake by step so that the timer fires. It never advances while no timer is due within step.
func eventuallyAdvancing(t *testing.T, what string, fake *clock.Fake, step time.Duration, cond func() bool, state func() string) {
	t.Helper()
	var advanced time.Duration
	testwait.Until(t, what, func() bool {
		if cond() {
			return true
		}
		if fake.Due(step) > 0 {
			fake.Advance(step)
			advanced += step
		}
		return false
	}, func() string {
		s := fmt.Sprintf("fake clock advanced %v; timers due within %v: %d", advanced, step, fake.Due(step))
		if state != nil {
			s = state() + "; " + s
		}
		return s
	})
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
		defer func() { _ = conn.Close() }()
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
	defer func() { _ = resp.Body.Close() }()
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

func startHold(t *testing.T, addr string) <-chan holdResult {
	out := make(chan holdResult, 1)
	client := &http.Client{Timeout: testwait.Guard(t)}
	go func() {
		resp, err := client.Get("http://" + addr + "/hold")
		if err != nil {
			out <- holdResult{err: err}
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		out <- holdResult{status: resp.StatusCode}
	}()
	return out
}

func readyzStatus(t *testing.T, addr string) int {
	client := &http.Client{Timeout: testwait.Guard(t)}
	resp, err := client.Get("http://" + addr + "/readyz")
	if err != nil {
		return -1
	}
	_ = resp.Body.Close()
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
		Timeout: testwait.Guard(t),
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

	plainClient := &http.Client{Timeout: testwait.Guard(t)}
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
	_ = conn.Close()

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
		Timeout: testwait.Guard(t),
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
	plain := "127.0.0.1:0"
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

	before := listeningAddrs(t)
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
	for addr := range listeningAddrs(t) {
		if !before[addr] {
			t.Fatalf("address %s not released: still listening after run returned", addr)
		}
	}
}

func TestRunBindConflict(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	conflict := held.Addr().String()
	free := "127.0.0.1:0"
	admin := "127.0.0.1:0"
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

	before := listeningAddrs(t)
	if !before[conflict] {
		t.Fatalf("held listener %s not seen before run: %v", conflict, before)
	}
	code, _, errs := runCmd(t, "run", "-config", path)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	if !strings.Contains(errs, `listener "b"`) {
		t.Errorf("stderr = %q, want it to contain %q", errs, `listener "b"`)
	}
	for addr := range listeningAddrs(t) {
		if !before[addr] {
			t.Fatalf("address %s not released: still listening after run returned", addr)
		}
	}
	if !listeningAddrs(t)[conflict] {
		t.Fatalf("held listener %s closed by run", conflict)
	}
}

func TestRunGracefulShutdown(t *testing.T) {
	backend, entered, release := holdBackend(t)
	path := writeTemp(t, lifecycleConfig("global:\n  shutdown_delay: 2s\n  drain_timeout: 10s\n", backend.URL))

	h := startRun(t, path)
	public := h.addrs.Listeners["public"].String()
	admin := h.addrs.Admin.String()

	holdRes := startHold(t, public)
	testwait.Recv(t, "/hold request to reach the backend", entered, nil)

	h.sig <- syscall.SIGTERM
	eventually(t, "readiness to report 503 after SIGTERM", func() bool { return readyzStatus(t, admin) == http.StatusServiceUnavailable })
	select {
	case r := <-holdRes:
		t.Fatalf("/hold finished before release: %+v", r)
	default:
	}

	fastClient := &http.Client{
		Timeout:   testwait.Guard(t),
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	resp, body := fetchURL(t, fastClient, "http://"+public+"/fast")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/fast during shutdown delay: status = %d, want 200; body %s", resp.StatusCode, body)
	}

	awaitDue(t, h.clk, 1, 2*time.Second, "shutdown delay timer armed", nil)
	h.clk.Advance(2 * time.Second)
	eventually(t, "data listener to refuse new connections once the shutdown delay ended", func() bool {
		conn, err := net.Dial("tcp", public)
		if err != nil {
			return true
		}
		_ = conn.Close()
		return false
	})
	select {
	case r := <-holdRes:
		t.Fatalf("/hold finished before release after the listener closed: %+v", r)
	default:
	}

	close(release)
	r, _ := testwait.Recv(t, "/hold to complete after release", holdRes, nil)
	if r.err != nil || r.status != http.StatusOK {
		t.Errorf("/hold result = %+v, want status 200", r)
	}

	if code := h.wait(t); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
	if conn, err := net.Dial("tcp", public); err == nil {
		_ = conn.Close()
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

	holdRes := startHold(t, public)
	testwait.Recv(t, "/hold request to reach the backend", entered, nil)

	conn, err := net.Dial("tcp", public)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	br := wsHandshake(t, conn, public)

	h.sig <- syscall.SIGTERM
	awaitDue(t, h.clk, 1, 300*time.Millisecond, "drain timer armed", nil)
	h.clk.Advance(300 * time.Millisecond)
	if code := h.wait(t); code == 0 {
		t.Fatal("exit code = 0, want nonzero after forced shutdown")
	}

	r, _ := testwait.Recv(t, "/hold client to return after forced shutdown", holdRes, nil)
	if r.err == nil && r.status == http.StatusOK {
		t.Errorf("/hold result = %+v, want an error or non-200", r)
	}

	readErr := make(chan error, 1)
	go func() {
		_, err := br.ReadByte()
		readErr <- err
	}()
	err, _ = testwait.Recv(t, "websocket read to end after forced shutdown", readErr, nil)
	if err == nil {
		t.Error("websocket read returned data, want an error")
	}
}

func TestRunSecondSignal(t *testing.T) {
	hold, entered, _ := holdBackend(t)
	path := writeTemp(t, lifecycleConfig("global:\n  drain_timeout: 1m\n", hold.URL))

	h := startRun(t, path)
	public := h.addrs.Listeners["public"].String()
	admin := h.addrs.Admin.String()

	_ = startHold(t, public)
	testwait.Recv(t, "/hold request to reach the backend", entered, nil)

	h.sig <- syscall.SIGTERM
	eventually(t, "readiness to report 503 after SIGTERM", func() bool { return readyzStatus(t, admin) == http.StatusServiceUnavailable })

	h.sig <- syscall.SIGTERM
	if code := h.wait(t); code == 0 {
		t.Error("exit code = 0, want nonzero after second signal")
	}
}
