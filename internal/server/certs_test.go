package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testcert"
	"github.com/ok0/go-microgateway/internal/testwait"
)

var farFuture = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)

func writePair(t *testing.T, certFile, keyFile string, serial int64, notAfter time.Time) {
	t.Helper()
	certPEM, keyPEM := testcert.Generate(t, serial, notAfter)
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
}

func loadTLSConfig(t *testing.T, certFile, keyFile string) *config.Config {
	t.Helper()
	doc := fmt.Sprintf(`version: 1
listeners:
  - name: secure
    address: "127.0.0.1:0"
    tls:
      cert_file: %q
      key_file: %q
upstreams:
  - name: up
    hosts:
      - url: "http://127.0.0.1:1"
routes:
  - name: all
    upstream: up
`, certFile, keyFile)
	cfg, err := config.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func startTLS(t *testing.T, cfg *config.Config, c *Certificates) *Server {
	t.Helper()
	return startServer(t, cfg, http.NotFoundHandler(), nil, nil, WithCertificates(c))
}

func dialSerial(t *testing.T, addr string) (*tls.Conn, int64) {
	t.Helper()
	conn, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, conn.ConnectionState().PeerCertificates[0].SerialNumber.Int64()
}

func prepareErr(t *testing.T, c *Certificates, cfg *config.Config) *config.ValidationError {
	t.Helper()
	_, _, err := c.Prepare(cfg)
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("Prepare error = %v, want *config.ValidationError", err)
	}
	return ve
}

func TestCertificateSwap(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	writePair(t, certFile, keyFile, 1, farFuture)
	cfg := loadTLSConfig(t, certFile, keyFile)

	// Real clock: Certificates only reads it for the expiry warning; these certificates expire in 2100.
	c := NewCertificates(nil, nil)
	_, apply, err := c.Prepare(cfg)
	if err != nil {
		t.Fatal(err)
	}
	apply()
	srv := startTLS(t, cfg, c)
	addr := srv.Addrs()["secure"].String()

	old, serial := dialSerial(t, addr)
	if serial != 1 {
		t.Fatalf("serial = %d, want 1", serial)
	}

	writePair(t, certFile, keyFile, 2, farFuture)
	_, apply, err = c.Prepare(cfg)
	if err != nil {
		t.Fatal(err)
	}
	apply()

	_, serial = dialSerial(t, addr)
	if serial != 2 {
		t.Fatalf("new connection serial = %d, want 2", serial)
	}

	type reply struct {
		status int
		err    error
		write  bool
	}
	oldReply := make(chan reply, 1)
	go func() {
		if _, err := fmt.Fprint(old, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
			oldReply <- reply{err: err, write: true}
			return
		}
		resp, err := http.ReadResponse(bufio.NewReader(old), nil)
		if err != nil {
			oldReply <- reply{err: err}
			return
		}
		_ = resp.Body.Close()
		oldReply <- reply{status: resp.StatusCode}
	}()
	r, _ := testwait.Recv(t, "response on the pre-swap connection", oldReply, nil)
	if r.write {
		t.Fatal(r.err)
	}
	if r.err != nil {
		t.Fatalf("old connection: %v", r.err)
	}
	if r.status != http.StatusNotFound {
		t.Fatalf("old connection status = %d, want 404", r.status)
	}
}

func TestUnchangedNotReparsed(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	writePair(t, certFile, keyFile, 1, farFuture)
	cfg := loadTLSConfig(t, certFile, keyFile)

	// Real clock: Certificates only reads it for the expiry warning; these certificates expire in 2100.
	c := NewCertificates(nil, nil)
	d1, apply, err := c.Prepare(cfg)
	if err != nil {
		t.Fatal(err)
	}
	apply()
	d2, apply2, err := c.Prepare(cfg)
	if err != nil {
		t.Fatal(err)
	}
	apply2()
	if c.parses.Load() != 1 {
		t.Fatalf("parses = %d, want 1", c.parses.Load())
	}
	if d1 != d2 {
		t.Fatalf("digests differ for unchanged files: %q != %q", d1, d2)
	}
}

func TestBadReplacementKeepsOld(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	writePair(t, certFile, keyFile, 1, farFuture)
	cfg := loadTLSConfig(t, certFile, keyFile)

	// Real clock: Certificates only reads it for the expiry warning; these certificates expire in 2100.
	c := NewCertificates(nil, nil)
	_, apply, err := c.Prepare(cfg)
	if err != nil {
		t.Fatal(err)
	}
	apply()
	srv := startTLS(t, cfg, c)
	addr := srv.Addrs()["secure"].String()

	_, otherKey := testcert.Generate(t, 2, farFuture)
	if err := os.WriteFile(keyFile, otherKey, 0o600); err != nil {
		t.Fatal(err)
	}
	ve := prepareErr(t, c, cfg)
	if ve.Errors[0].Path != "listeners[0].tls.cert_file" || ve.Errors[0].Line <= 0 {
		t.Fatalf("mismatched key error = %+v", ve.Errors[0])
	}

	if err := os.WriteFile(certFile, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	ve = prepareErr(t, c, cfg)
	if ve.Errors[0].Path != "listeners[0].tls.cert_file" || ve.Errors[0].Line <= 0 {
		t.Fatalf("corrupt certificate error = %+v", ve.Errors[0])
	}

	if err := os.Remove(keyFile); err != nil {
		t.Fatal(err)
	}
	ve = prepareErr(t, c, cfg)
	if ve.Errors[0].Path != "listeners[0].tls.key_file" || ve.Errors[0].Line <= 0 {
		t.Fatalf("missing key error = %+v", ve.Errors[0])
	}

	_, serial := dialSerial(t, addr)
	if serial != 1 {
		t.Fatalf("serial after failed replacements = %d, want 1", serial)
	}
}

func TestCertPathChange(t *testing.T) {
	dir := t.TempDir()
	certA := filepath.Join(dir, "a.pem")
	keyA := filepath.Join(dir, "a.key")
	certB := filepath.Join(dir, "b.pem")
	keyB := filepath.Join(dir, "b.key")
	writePair(t, certA, keyA, 1, farFuture)
	writePair(t, certB, keyB, 3, farFuture)
	cfg := loadTLSConfig(t, certA, keyA)

	// Real clock: Certificates only reads it for the expiry warning; these certificates expire in 2100.
	c := NewCertificates(nil, nil)
	_, apply, err := c.Prepare(cfg)
	if err != nil {
		t.Fatal(err)
	}
	apply()
	srv := startTLS(t, cfg, c)
	addr := srv.Addrs()["secure"].String()

	cfg2 := loadTLSConfig(t, certB, keyB)
	_, apply, err = c.Prepare(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	apply()

	_, serial := dialSerial(t, addr)
	if serial != 3 {
		t.Fatalf("serial = %d, want 3", serial)
	}
}

func TestExpiredCertificateWarns(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	writePair(t, certFile, keyFile, 1, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	cfg := loadTLSConfig(t, certFile, keyFile)

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	fake := clock.NewFake(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	c := NewCertificates(logger, fake)
	if _, _, err := c.Prepare(cfg); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(buf.Bytes(), []byte("certificate expired")) {
		t.Fatalf("log = %q, want a certificate expired record", buf.String())
	}
}

func TestShutdownTimingsUpdated(t *testing.T) {
	cfg := testConfig(t, nil)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
	})
	fake := clock.NewFake(serverStart)
	srv := startServer(t, cfg, h, nil, nil, WithClock(fake))
	t.Cleanup(func() { close(release) })
	srv.SetShutdownTimings(0, 50*time.Millisecond)

	client := newClient(t, &http.Transport{})
	addr := srv.Addrs()["public"].String()
	go func() { _, _, _ = fetch(client, "http://"+addr+"/") }()
	waitSignal(t, entered, "held request")

	errc := shutdownAsync(srv, context.Background())
	awaitDue(t, fake, 1, 50*time.Millisecond, "50ms drain timer")
	if n := fake.Due(50*time.Millisecond - time.Nanosecond); n != 0 {
		t.Fatalf("drain timer due before 50ms: %d", n)
	}
	fake.Advance(50 * time.Millisecond)
	err, _ := testwait.Recv(t, "Shutdown to return", errc, nil)
	if !errors.Is(err, ErrForcedClose) {
		t.Fatalf("Shutdown error = %v, want ErrForcedClose", err)
	}
}
