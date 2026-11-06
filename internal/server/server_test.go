package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testcert"
	"github.com/ok0/go-microgateway/internal/testwait"
)

var serverStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func testConfig(t *testing.T, mutate func(*config.Config)) *config.Config {
	t.Helper()
	cfg := &config.Config{
		Version:   1,
		Listeners: []config.Listener{{Name: "public", Address: "127.0.0.1:0"}},
		Upstreams: []config.Upstream{{Name: "u", Hosts: []config.Host{{URL: config.NewSecretURL("http://127.0.0.1:1")}}}},
		Routes:    []config.Route{{Name: "r", Upstream: "u"}},
		Admin:     config.Admin{Address: "127.0.0.1:0"},
	}
	if mutate != nil {
		mutate(cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	testwait.Done(t, what, ch, nil)
}

// awaitDue waits until n timers of fake are due within the given duration.
func awaitDue(t *testing.T, fake *clock.Fake, n int, within time.Duration, what string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	due := make(chan struct{})
	go func() {
		defer close(due)
		_ = fake.WaitDue(ctx, n, within)
	}()
	testwait.Done(t, what, due, func() string {
		return fmt.Sprintf("fake waiters=%d due within %v=%d", fake.Waiters(), within, fake.Due(within))
	})
}

func cleanupShutdown(t *testing.T, srv *Server) {
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = srv.Shutdown(ctx)
	})
}

func shutdownAsync(srv *Server, ctx context.Context) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- srv.Shutdown(ctx) }()
	return ch
}

func newClient(t *testing.T, tr *http.Transport) *http.Client {
	t.Helper()
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr}
}

func fetch(client *http.Client, url string) (*http.Response, string, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	return resp, string(b), nil
}

func startServer(t *testing.T, cfg *config.Config, h, admin http.Handler, ready func(bool), opts ...Option) *Server {
	t.Helper()
	srv, err := New(cfg, h, admin, ready, nil, append([]Option{WithClock(clock.NewFake(serverStart))}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	cleanupShutdown(t, srv)
	return srv
}

func TestServeAndShutdown(t *testing.T) {
	cfg := testConfig(t, nil)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	})
	srv, err := New(cfg, h, nil, nil, nil, WithClock(clock.NewFake(serverStart)))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	addr, ok := srv.Addrs()["public"]
	if !ok {
		t.Fatal("no address for listener public")
	}
	_, body, err := fetch(newClient(t, &http.Transport{}), "http://"+addr.String())
	if err != nil {
		t.Fatal(err)
	}
	if body != "ok" {
		t.Fatalf("body = %q, want %q", body, "ok")
	}
	err, _ = testwait.Recv(t, "Shutdown to return", shutdownAsync(srv, context.Background()), nil)
	if err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if _, _, err := fetch(newClient(t, &http.Transport{}), "http://"+addr.String()); err == nil {
		t.Fatal("GET after Shutdown succeeded, want error")
	}
}

func TestListenerSettings(t *testing.T) {
	cfg := testConfig(t, func(c *config.Config) {
		l := &c.Listeners[0]
		l.ReadHeaderTimeout = config.Duration(3 * time.Second)
		l.ReadTimeout = config.Duration(4 * time.Second)
		l.WriteTimeout = config.Duration(5 * time.Second)
		l.IdleTimeout = config.Duration(6 * time.Second)
		l.MaxHeaderBytes = 4096
	})
	srv, err := New(cfg, http.NotFoundHandler(), nil, nil, nil, WithClock(clock.NewFake(serverStart)))
	if err != nil {
		t.Fatal(err)
	}
	hs := srv.data[0].srv
	if hs.ReadHeaderTimeout != 3*time.Second {
		t.Errorf("ReadHeaderTimeout = %v, want %v", hs.ReadHeaderTimeout, 3*time.Second)
	}
	if hs.ReadTimeout != 4*time.Second {
		t.Errorf("ReadTimeout = %v, want %v", hs.ReadTimeout, 4*time.Second)
	}
	if hs.WriteTimeout != 5*time.Second {
		t.Errorf("WriteTimeout = %v, want %v", hs.WriteTimeout, 5*time.Second)
	}
	if hs.IdleTimeout != 6*time.Second {
		t.Errorf("IdleTimeout = %v, want %v", hs.IdleTimeout, 6*time.Second)
	}
	if hs.HTTP2 == nil {
		t.Error("HTTP2 = nil, want the default HTTP/2 config")
	}
	if hs.ConnState == nil {
		t.Error("ConnState = nil, want the limit tracker")
	}
	if want := (connLimits{header: 3 * time.Second, read: 4 * time.Second, write: 5 * time.Second, idle: 6 * time.Second}); srv.data[0].lim != want {
		t.Errorf("limits = %+v, want %+v", srv.data[0].lim, want)
	}
	if hs.MaxHeaderBytes != 4096 {
		t.Errorf("MaxHeaderBytes = %d, want 4096", hs.MaxHeaderBytes)
	}
}

func TestTLSHTTP2(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	cfg := testConfig(t, func(c *config.Config) {
		c.Listeners[0].TLS = &config.TLS{CertFile: certFile, KeyFile: keyFile}
	})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, fmt.Sprint(r.ProtoMajor, r.TLS != nil))
	})
	srv := startServer(t, cfg, h, nil, nil)
	addr := srv.Addrs()["public"].String()
	client := newClient(t, &http.Transport{
		TLSClientConfig:   &tls.Config{RootCAs: pool},
		ForceAttemptHTTP2: true,
	})
	resp, body, err := fetch(client, "https://"+addr)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ProtoMajor != 2 {
		t.Fatalf("ProtoMajor = %d, want 2", resp.ProtoMajor)
	}
	if body != "2 true" {
		t.Fatalf("body = %q, want %q", body, "2 true")
	}
}

func TestTLSHTTP1Only(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	off := false
	cfg := testConfig(t, func(c *config.Config) {
		c.Listeners[0].TLS = &config.TLS{CertFile: certFile, KeyFile: keyFile}
		c.Listeners[0].HTTP2 = &off
	})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, fmt.Sprint(r.ProtoMajor, r.TLS != nil))
	})
	srv := startServer(t, cfg, h, nil, nil)
	addr := srv.Addrs()["public"].String()
	client := newClient(t, &http.Transport{
		TLSClientConfig:   &tls.Config{RootCAs: pool},
		ForceAttemptHTTP2: true,
	})
	resp, body, err := fetch(client, "https://"+addr)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ProtoMajor != 1 {
		t.Fatalf("ProtoMajor = %d, want 1", resp.ProtoMajor)
	}
	if body != "1 true" {
		t.Fatalf("body = %q, want %q", body, "1 true")
	}
}

func TestTLSMinVersion13(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	cfg := testConfig(t, func(c *config.Config) {
		c.Listeners[0].TLS = &config.TLS{CertFile: certFile, KeyFile: keyFile, MinVersion: "1.3"}
	})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	})
	srv := startServer(t, cfg, h, nil, nil)
	addr := srv.Addrs()["public"].String()
	old := newClient(t, &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, MaxVersion: tls.VersionTLS12},
	})
	if _, _, err := fetch(old, "https://"+addr); err == nil {
		t.Fatal("TLS 1.2 client connected to a listener with min_version 1.3")
	}
	modern := newClient(t, &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool},
	})
	resp, body, err := fetch(modern, "https://"+addr)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || body != "ok" {
		t.Fatalf("status = %d, body = %q; want 200 \"ok\"", resp.StatusCode, body)
	}
}

func TestH2C(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, fmt.Sprint(r.ProtoMajor))
	})
	prior := func() *http.Transport {
		var p http.Protocols
		p.SetUnencryptedHTTP2(true)
		return &http.Transport{Protocols: &p}
	}

	on := testConfig(t, func(c *config.Config) {
		c.Listeners[0].H2C = true
	})
	srv := startServer(t, on, h, nil, nil)
	addr := srv.Addrs()["public"].String()
	resp, body, err := fetch(newClient(t, prior()), "http://"+addr)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ProtoMajor != 2 || body != "2" {
		t.Fatalf("h2c: ProtoMajor = %d, body = %q; want 2 and \"2\"", resp.ProtoMajor, body)
	}
	resp, body, err = fetch(newClient(t, &http.Transport{}), "http://"+addr)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || resp.ProtoMajor != 1 || body != "1" {
		t.Fatalf("http/1.1: status = %d, ProtoMajor = %d, body = %q", resp.StatusCode, resp.ProtoMajor, body)
	}

	off := testConfig(t, nil)
	srvOff := startServer(t, off, h, nil, nil)
	resp, _, err = fetch(newClient(t, prior()), "http://"+srvOff.Addrs()["public"].String())
	if err == nil && resp.StatusCode/100 == 2 {
		t.Fatal("prior-knowledge HTTP/2 succeeded without h2c enabled")
	}
}

func TestBadCertPath(t *testing.T) {
	cfg := testConfig(t, func(c *config.Config) {
		c.Listeners[0].TLS = &config.TLS{
			CertFile: filepath.Join(t.TempDir(), "missing.pem"),
			KeyFile:  filepath.Join(t.TempDir(), "missing.key"),
		}
	})
	_, err := New(cfg, http.NotFoundHandler(), nil, nil, nil, WithClock(clock.NewFake(serverStart)))
	if err == nil {
		t.Fatal("New returned nil error, want certificate load error")
	}
	if !strings.Contains(err.Error(), `listener "public"`) || !strings.Contains(err.Error(), "load certificate") {
		t.Fatalf("error = %v", err)
	}
}

func TestStartBindError(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = taken.Close() })
	port := freePort(t)
	cfg := testConfig(t, func(c *config.Config) {
		c.Listeners = []config.Listener{
			{Name: "a", Address: port},
			{Name: "b", Address: taken.Addr().String()},
		}
	})
	srv, err := New(cfg, http.NotFoundHandler(), nil, nil, nil, WithClock(clock.NewFake(serverStart)))
	if err != nil {
		t.Fatal(err)
	}
	err = srv.Start()
	if err == nil {
		t.Fatal("Start returned nil error, want bind error")
	}
	if !strings.Contains(err.Error(), `listener "b"`) {
		t.Fatalf("error = %v, want listener \"b\"", err)
	}
	ln, err := net.Listen("tcp", port)
	if err != nil {
		t.Fatalf("port of listener a not released after failed Start: %v", err)
	}
	_ = ln.Close()
}

func TestAdminListener(t *testing.T) {
	dataH := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "data")
	})
	adminH := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "admin")
	})
	srv := startServer(t, testConfig(t, nil), dataH, adminH, nil)
	adminAddr := srv.AdminAddr()
	if adminAddr == nil {
		t.Fatal("AdminAddr returned nil with admin enabled")
	}
	_, body, err := fetch(newClient(t, &http.Transport{}), "http://"+adminAddr.String()+"/")
	if err != nil {
		t.Fatal(err)
	}
	if body != "admin" {
		t.Fatalf("admin body = %q, want %q", body, "admin")
	}
	dataAddr := srv.Addrs()["public"].String()
	_, body, err = fetch(newClient(t, &http.Transport{}), "http://"+dataAddr+"/")
	if err != nil {
		t.Fatal(err)
	}
	if body != "data" {
		t.Fatalf("data body = %q, want %q", body, "data")
	}

	off := false
	disabled := startServer(t, testConfig(t, func(c *config.Config) {
		c.Admin.Enabled = &off
	}), dataH, adminH, nil)
	if disabled.AdminAddr() != nil {
		t.Fatal("AdminAddr non-nil with admin disabled")
	}
}

func TestShutdownGraceful(t *testing.T) {
	cfg := testConfig(t, func(c *config.Config) {
		c.Global.ShutdownDelay = config.Duration(300 * time.Millisecond)
		c.Global.DrainTimeout = config.Duration(5 * time.Second)
	})
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			entered <- struct{}{}
			<-release
			_, _ = io.WriteString(w, "ok")
			return
		}
		_, _ = io.WriteString(w, "fast")
	})
	readyCh := make(chan bool, 4)
	fake := clock.NewFake(serverStart)
	srv := startServer(t, cfg, h, nil, func(v bool) { readyCh <- v }, WithClock(fake))
	addr := srv.Addrs()["public"].String()

	type result struct {
		status int
		body   string
		err    error
	}
	slowRes := make(chan result, 1)
	slowClient := &http.Client{Transport: &http.Transport{}}
	go func() {
		resp, body, err := fetch(slowClient, "http://"+addr+"/slow")
		if err != nil {
			slowRes <- result{err: err}
			return
		}
		slowRes <- result{status: resp.StatusCode, body: body}
	}()
	waitSignal(t, entered, "slow handler")

	shutdownErr := shutdownAsync(srv, context.Background())
	v, _ := testwait.Recv(t, "ready(false)", readyCh, nil)
	if v {
		t.Fatal("ready(true) reported during shutdown")
	}
	awaitDue(t, fake, 1, 300*time.Millisecond, "shutdown delay timer")

	fastClient := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	resp, body, err := fetch(fastClient, "http://"+addr+"/fast")
	if err != nil {
		t.Fatalf("request during shutdown delay: %v", err)
	}
	if resp.StatusCode != http.StatusOK || body != "fast" {
		t.Fatalf("request during shutdown delay: status = %d, body = %q", resp.StatusCode, body)
	}

	fake.Advance(300 * time.Millisecond)
	awaitDue(t, fake, 1, 5*time.Second, "drain timer")

	close(release)
	res, _ := testwait.Recv(t, "in-flight request to complete", slowRes, nil)
	if res.err != nil {
		t.Fatalf("in-flight request: %v", res.err)
	}
	if res.status != http.StatusOK || res.body != "ok" {
		t.Fatalf("in-flight request: status = %d, body = %q; want 200 \"ok\"", res.status, res.body)
	}

	err, _ = testwait.Recv(t, "Shutdown to return", shutdownErr, nil)
	if err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if _, _, err := fetch(&http.Client{Transport: &http.Transport{DisableKeepAlives: true}}, "http://"+addr+"/fast"); err == nil {
		t.Fatal("request after Shutdown succeeded, want error")
	}
}

func TestShutdownForced(t *testing.T) {
	cfg := testConfig(t, func(c *config.Config) {
		c.Global.DrainTimeout = config.Duration(200 * time.Millisecond)
	})
	hangEntered := make(chan struct{}, 1)
	hangReturned := make(chan struct{})
	hijackEntered := make(chan struct{}, 1)
	hijackReturned := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/hang":
			defer close(hangReturned)
			hangEntered <- struct{}{}
			<-r.Context().Done()
		case "/hijack":
			defer close(hijackReturned)
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			hijackEntered <- struct{}{}
			<-r.Context().Done()
			_ = conn.Close()
		}
	})
	fake := clock.NewFake(serverStart)
	srv := startServer(t, cfg, h, nil, nil, WithClock(fake))
	addr := srv.Addrs()["public"].String()

	go func() {
		_, _, _ = fetch(&http.Client{Transport: &http.Transport{}}, "http://"+addr+"/hang")
	}()

	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	if _, err := io.WriteString(raw, "GET /hijack HTTP/1.1\r\nHost: "+addr+"\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, hangEntered, "hang handler")
	waitSignal(t, hijackEntered, "hijack handler")

	errc := shutdownAsync(srv, context.Background())
	awaitDue(t, fake, 1, 200*time.Millisecond, "drain timer")
	fake.Advance(200 * time.Millisecond)
	err, _ = testwait.Recv(t, "Shutdown to return", errc, nil)
	if !errors.Is(err, ErrForcedClose) {
		t.Fatalf("Shutdown error = %v, want ErrForcedClose", err)
	}
	waitSignal(t, hangReturned, "hang handler return")
	waitSignal(t, hijackReturned, "hijack handler return")

	readErr := make(chan error, 1)
	go func() {
		_, err := raw.Read(make([]byte, 1))
		readErr <- err
	}()
	err, _ = testwait.Recv(t, "read on hijacked connection", readErr, nil)
	if err == nil {
		t.Fatal("hijacked connection still readable after forced close")
	}
}

func TestShutdownCanceledContext(t *testing.T) {
	cfg := testConfig(t, func(c *config.Config) {
		c.Global.DrainTimeout = config.Duration(time.Hour)
	})
	entered := make(chan struct{}, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-r.Context().Done()
	})
	srv := startServer(t, cfg, h, nil, nil)
	addr := srv.Addrs()["public"].String()

	go func() {
		_, _, _ = fetch(&http.Client{Transport: &http.Transport{}}, "http://"+addr+"/hang")
	}()
	waitSignal(t, entered, "hang handler")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		done <- srv.Shutdown(ctx)
	}()
	err, _ := testwait.Recv(t, "Shutdown with canceled context to return", done, nil)
	if !errors.Is(err, ErrForcedClose) {
		t.Fatalf("Shutdown error = %v, want ErrForcedClose", err)
	}
}

// TestShutdownForcedClosesBeforeCancel pins the order of a forced shutdown: the listeners' connections are closed
// before the base context of the requests is canceled. A handler that returns when its request is canceled
// without writing would otherwise send an implicit 200 over a connection that is still open, so the client
// would take a request cut off by the shutdown for a success.
func TestShutdownForcedClosesBeforeCancel(t *testing.T) {
	cfg := testConfig(t, func(c *config.Config) {
		c.Global.DrainTimeout = config.Duration(time.Hour)
	})
	entered := make(chan struct{}, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-r.Context().Done()
	})
	srv, err := New(cfg, h, nil, nil, nil, WithClock(clock.NewFake(serverStart)))
	if err != nil {
		t.Fatal(err)
	}
	// finished closes when the connection of the request has sent its response or has been closed.
	finished := make(chan struct{})
	var once sync.Once
	srv.data[0].srv.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateIdle || state == http.StateClosed {
			once.Do(func() { close(finished) })
		}
	}
	// Cancel the base context, then hold the shutdown until the request has finished one way or the other, so
	// that a response written because of the cancellation is on the wire before anything closes the connection.
	cancelBase := srv.cancel
	srv.cancel = func() {
		cancelBase()
		testwait.Done(t, "request answered or closed after base context cancel", finished, nil)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	cleanupShutdown(t, srv)
	addr := srv.Addrs()["public"].String()

	type result struct {
		status int
		err    error
	}
	got := make(chan result, 1)
	go func() {
		resp, _, err := fetch(&http.Client{Transport: &http.Transport{}}, "http://"+addr+"/hang")
		r := result{err: err}
		if resp != nil {
			r.status = resp.StatusCode
		}
		got <- r
	}()
	waitSignal(t, entered, "hang handler")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := srv.Shutdown(ctx); !errors.Is(err, ErrForcedClose) {
		t.Fatalf("Shutdown error = %v, want ErrForcedClose", err)
	}
	r, _ := testwait.Recv(t, "client request to end", got, nil)
	if r.err == nil {
		t.Fatalf("client received status %d from a request cut off by a forced shutdown, want a connection error", r.status)
	}
}
