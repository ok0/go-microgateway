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
	"testing"
	"time"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/testcert"
)

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

func closed(ch chan struct{}) func() bool {
	return func() bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
}

func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
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
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	return resp, string(b), nil
}

func startServer(t *testing.T, cfg *config.Config, h, admin http.Handler, ready func(bool)) *Server {
	t.Helper()
	srv, err := New(cfg, h, admin, ready, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})
	return srv
}

func TestServeAndShutdown(t *testing.T) {
	cfg := testConfig(t, nil)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	})
	srv, err := New(cfg, h, nil, nil, nil)
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
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
	srv, err := New(cfg, http.NotFoundHandler(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	hs := srv.data[0].srv
	if hs.ReadHeaderTimeout != 3*time.Second {
		t.Errorf("ReadHeaderTimeout = %v, want 3s", hs.ReadHeaderTimeout)
	}
	if hs.ReadTimeout != 4*time.Second {
		t.Errorf("ReadTimeout = %v, want 4s", hs.ReadTimeout)
	}
	if hs.WriteTimeout != 5*time.Second {
		t.Errorf("WriteTimeout = %v, want 5s", hs.WriteTimeout)
	}
	if hs.IdleTimeout != 6*time.Second {
		t.Errorf("IdleTimeout = %v, want 6s", hs.IdleTimeout)
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
		io.WriteString(w, fmt.Sprint(r.ProtoMajor, r.TLS != nil))
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
		io.WriteString(w, fmt.Sprint(r.ProtoMajor, r.TLS != nil))
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
		io.WriteString(w, "ok")
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
		io.WriteString(w, fmt.Sprint(r.ProtoMajor))
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
	_, err := New(cfg, http.NotFoundHandler(), nil, nil, nil)
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
	t.Cleanup(func() { taken.Close() })
	port := freePort(t)
	cfg := testConfig(t, func(c *config.Config) {
		c.Listeners = []config.Listener{
			{Name: "a", Address: port},
			{Name: "b", Address: taken.Addr().String()},
		}
	})
	srv, err := New(cfg, http.NotFoundHandler(), nil, nil, nil)
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
	ln.Close()
}

func TestAdminListener(t *testing.T) {
	dataH := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "data")
	})
	adminH := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "admin")
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
			io.WriteString(w, "ok")
			return
		}
		io.WriteString(w, "fast")
	})
	readyCh := make(chan bool, 4)
	srv := startServer(t, cfg, h, nil, func(v bool) { readyCh <- v })
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

	shutdownErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shutdownErr <- srv.Shutdown(ctx)
	}()
	select {
	case v := <-readyCh:
		if v {
			t.Fatal("ready(true) reported during shutdown")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ready(false) not reported")
	}

	fastClient := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	resp, body, err := fetch(fastClient, "http://"+addr+"/fast")
	if err != nil {
		t.Fatalf("request during shutdown delay: %v", err)
	}
	if resp.StatusCode != http.StatusOK || body != "fast" {
		t.Fatalf("request during shutdown delay: status = %d, body = %q", resp.StatusCode, body)
	}

	close(release)
	select {
	case res := <-slowRes:
		if res.err != nil {
			t.Fatalf("in-flight request: %v", res.err)
		}
		if res.status != http.StatusOK || res.body != "ok" {
			t.Fatalf("in-flight request: status = %d, body = %q; want 200 \"ok\"", res.status, res.body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request did not complete")
	}

	select {
	case err := <-shutdownErr:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return")
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
			conn.Close()
		}
	})
	srv := startServer(t, cfg, h, nil, nil)
	addr := srv.Addrs()["public"].String()

	go func() {
		fetch(&http.Client{Transport: &http.Transport{}}, "http://"+addr+"/hang")
	}()

	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	if _, err := io.WriteString(raw, "GET /hijack HTTP/1.1\r\nHost: "+addr+"\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, hangEntered, "hang handler")
	waitSignal(t, hijackEntered, "hijack handler")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(ctx)
	if !errors.Is(err, ErrForcedClose) {
		t.Fatalf("Shutdown error = %v, want ErrForcedClose", err)
	}
	eventually(t, closed(hangReturned))
	eventually(t, closed(hijackReturned))

	readErr := make(chan error, 1)
	go func() {
		_, err := raw.Read(make([]byte, 1))
		readErr <- err
	}()
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("hijacked connection still readable after forced close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read on hijacked connection did not return")
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
		fetch(&http.Client{Transport: &http.Transport{}}, "http://"+addr+"/hang")
	}()
	waitSignal(t, entered, "hang handler")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		done <- srv.Shutdown(ctx)
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrForcedClose) {
			t.Fatalf("Shutdown error = %v, want ErrForcedClose", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown with canceled context did not return within 2s")
	}
}
