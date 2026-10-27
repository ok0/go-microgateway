package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ok0/lychgate/config"
)

func httpGet(t *testing.T, url string) (string, error) {
	t.Helper()
	client := &http.Client{Transport: &http.Transport{}}
	defer client.CloseIdleConnections()
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b), nil
}

func TestServeAndShutdown(t *testing.T) {
	cfg := &config.Config{
		Version:   1,
		Listeners: []config.Listener{{Name: "public", Address: "127.0.0.1:0"}},
		Upstreams: []config.Upstream{{Name: "u", Hosts: []config.Host{{URL: "http://127.0.0.1:1"}}}},
		Routes:    []config.Route{{Name: "r", Upstream: "u"}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	})
	srv, err := New(cfg, h, nil)
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
	body, err := httpGet(t, "http://"+addr.String())
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
	if _, err := httpGet(t, "http://"+addr.String()); err == nil {
		t.Fatal("GET after Shutdown succeeded, want error")
	}
}

func TestTLSNotSupported(t *testing.T) {
	cfg := &config.Config{
		Listeners: []config.Listener{{
			Name:    "public",
			Address: "127.0.0.1:0",
			TLS:     &config.TLS{CertFile: "c", KeyFile: "k"},
		}},
	}
	_, err := New(cfg, http.NotFoundHandler(), nil)
	if err == nil {
		t.Fatal("New returned nil error, want tls not supported")
	}
	if !strings.Contains(err.Error(), "not yet supported") || !strings.Contains(err.Error(), "public") {
		t.Fatalf("error = %v", err)
	}
}

func TestStartBindError(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { taken.Close() })
	cfg := &config.Config{
		Listeners: []config.Listener{
			{Name: "free", Address: "127.0.0.1:0"},
			{Name: "taken", Address: taken.Addr().String()},
		},
	}
	srv, err := New(cfg, http.NotFoundHandler(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err == nil {
		t.Fatal("Start returned nil error, want bind error")
	}
}
