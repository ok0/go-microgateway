package server

import (
	"bufio"
	"context"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testcert"
	"github.com/ok0/go-microgateway/internal/testwait"
)

// pkgOf returns the package that declares the dynamic type of w, looking through a pointer.
func pkgOf(w any) string {
	t := reflect.TypeOf(w)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.PkgPath()
}

// startServerLogged starts a server on fake whose log output goes to logs.
func startServerLogged(t *testing.T, cfg *config.Config, logs io.Writer, fake *clock.Fake) *Server {
	t.Helper()
	srv, err := New(cfg, okHandler(), okHandler(), nil, nil, slog.New(slog.NewTextHandler(logs, nil)), WithClock(fake))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	cleanupShutdown(t, srv)
	return srv
}

// requireHTTP2Off fails the test if HTTP/2 is enabled on hs.
func requireHTTP2Off(t *testing.T, hs *http.Server) {
	t.Helper()
	if hs.Protocols.HTTP2() || hs.Protocols.UnencryptedHTTP2() {
		t.Errorf("HTTP/2 protocols enabled: TLS %v, unencrypted %v; want both off", hs.Protocols.HTTP2(), hs.Protocols.UnencryptedHTTP2())
	}
	if hs.TLSNextProto != nil {
		t.Errorf("TLSNextProto = %v, want nil", hs.TLSNextProto)
	}
}

func TestHTTP2ServedByNetHTTP(t *testing.T) {
	certFile, keyFile, _ := testcert.Write(t, t.TempDir())
	for _, w := range bubbleWires(t) {
		name := "h2c"
		if w.tls {
			name = "tls-h2"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				m := newMemNet(64 << 10)
				t.Cleanup(m.close)
				srv, err := New(w.config(t), okHandler(), nil, nil, nil, nil, WithClock(clock.NewFake(serverStart)))
				if err != nil {
					t.Fatal(err)
				}
				hs := srv.data[0].srv
				if hs.TLSNextProto != nil {
					t.Fatalf("TLSNextProto = %v before serving, want nil", hs.TLSNextProto)
				}
				srv.listen = m.listen
				if err := srv.Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					_ = srv.Shutdown(ctx)
				})
				synctest.Wait()
				if len(hs.TLSNextProto) != 1 || hs.TLSNextProto["h2"] == nil {
					t.Fatalf("TLSNextProto keys = %v once serving, want exactly h2", reflect.ValueOf(hs.TLSNextProto).MapKeys())
				}
				scheme := "http://"
				if w.tls {
					scheme = "https://"
				}
				got := getVia(t, bubbleClient(t, m, w), scheme+srv.Addrs()["public"].String()+"/")
				if got.status != http.StatusOK || got.major != 2 {
					t.Fatalf("status = %d, ProtoMajor = %d; want 200, 2", got.status, got.major)
				}
				if w.tls && got.alpn != "h2" {
					t.Fatalf("negotiated protocol = %q, want h2", got.alpn)
				}
				synctest.Wait()
				if len(hs.TLSNextProto) != 1 || hs.TLSNextProto["h2"] == nil {
					t.Fatalf("TLSNextProto keys = %v, want exactly h2", reflect.ValueOf(hs.TLSNextProto).MapKeys())
				}
			})
		})
	}
	t.Run("tls-http1-only", func(t *testing.T) {
		srv := startServer(t, limitCfg(t, tlsOn(certFile, keyFile, false)), okHandler(), okHandler(), nil, WithClock(clock.NewFake(serverStart)))
		requireHTTP2Off(t, srv.data[0].srv)
	})
	t.Run("plain", func(t *testing.T) {
		srv := startServer(t, limitCfg(t), okHandler(), okHandler(), nil, WithClock(clock.NewFake(serverStart)))
		requireHTTP2Off(t, srv.data[0].srv)
	})
	t.Run("admin", func(t *testing.T) {
		srv := startServer(t, limitCfg(t), okHandler(), okHandler(), nil, WithClock(clock.NewFake(serverStart)))
		requireHTTP2Off(t, srv.admin.srv)
	})
}

func TestTLSHandshakeJoined(t *testing.T) {
	certFile, keyFile, _ := testcert.Write(t, t.TempDir())
	t.Run("bad-record", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		logs := &logBuffer{}
		srv := startServerLogged(t, limitCfg(t, tlsOn(certFile, keyFile, true), limits(10*time.Second, 0, 0, time.Hour)), logs, fake)
		base := runtime.NumGoroutine()
		conn := dialRaw(t, srv.Addrs()["public"].String())
		c := capturing(conn)
		writeString(t, conn, "\x16\x03\x01\x00\x05hello")
		awaitCut(t, c, "connection closed after the bad record")
		testwait.Until(t, "handshake error logged", func() bool { return strings.Contains(logs.String(), "http: TLS handshake error") }, nil)
		testwait.Until(t, "goroutines back to baseline", func() bool { return runtime.NumGoroutine() <= base }, nil)
		testwait.Until(t, "no fake timers pending", func() bool { return fake.Waiters() == 0 }, nil)
	})
	t.Run("stalled", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		logs := &logBuffer{}
		srv := startServerLogged(t, limitCfg(t, tlsOn(certFile, keyFile, true), limits(2*time.Second, 0, 0, time.Hour)), logs, fake)
		base := runtime.NumGoroutine()
		c := capturing(dialRaw(t, srv.Addrs()["public"].String()))
		awaitDue(t, fake, 1, 2*time.Second, "handshake limit timer")
		advanceBelow(t, fake, 2*time.Second, c)
		fake.Advance(time.Nanosecond)
		awaitCut(t, c, "handshake limit cut")
		requireBytes(t, c, 0)
		testwait.Until(t, "goroutines back to baseline", func() bool { return runtime.NumGoroutine() <= base }, nil)
		testwait.Until(t, "no fake timers pending", func() bool { return fake.Waiters() == 0 }, nil)
	})
	t.Run("plaintext", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		logs := &logBuffer{}
		srv := startServerLogged(t, limitCfg(t, tlsOn(certFile, keyFile, true), limits(10*time.Second, 0, 0, time.Hour)), logs, fake)
		base := runtime.NumGoroutine()
		conn := dialRaw(t, srv.Addrs()["public"].String())
		c := capturing(conn)
		writeString(t, conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
		testwait.Until(t, "plaintext rejection", func() bool { return strings.HasPrefix(string(c.bytes()), "HTTP/1.0 400 Bad Request") }, nil)
		awaitCut(t, c, "connection closed after the rejection")
		testwait.Until(t, "goroutines back to baseline", func() bool { return runtime.NumGoroutine() <= base }, nil)
		testwait.Until(t, "no fake timers pending", func() bool { return fake.Waiters() == 0 }, nil)
	})
}

func TestHTTP1OnTLSListenerUsesNetHTTP(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	seen := make(chan string, 4)
	srv := startServer(t, limitCfg(t, tlsOn(certFile, keyFile, true)), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- fmt.Sprintf("%d %s", r.ProtoMajor, pkgOf(w))
		_, _ = io.WriteString(w, "ok")
	}), okHandler(), nil, WithClock(clock.NewFake(serverStart)))
	tc := dialTLS(t, srv.Addrs()["public"].String(), pool, "http/1.1")
	writeString(t, tc, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	if status, body := readResponse(t, bufio.NewReader(tc)); status != http.StatusOK || body != "ok" {
		t.Fatalf("status = %d, body = %q; want 200 \"ok\"", status, body)
	}
	if got, _ := testwait.Recv(t, "handler", seen, nil); got != "1 net/http" {
		t.Fatalf("handler saw %q, want HTTP/1 served by net/http", got)
	}
}

func TestHTTP2ShutdownSendsGoAway(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	t.Run("tls", func(t *testing.T) {
		shutdownSendsGoAway(t, limitCfg(t, tlsOn(certFile, keyFile, true)), pool)
	})
	t.Run("h2c", func(t *testing.T) {
		shutdownSendsGoAway(t, limitCfg(t, h2cOn), nil)
	})
}

func shutdownSendsGoAway(t *testing.T, cfg *config.Config, pool *x509.CertPool) {
	cfg.Global.ShutdownDelay = durPtr(0)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		_, _ = io.WriteString(w, "ok")
	})
	srv := startServer(t, cfg, h, okHandler(), nil, WithClock(clock.NewFake(serverStart)))
	p := dialH2(t, srv.Addrs()["public"].String(), pool, 0)
	p.handshake(true)
	p.headers(1, http.MethodGet, "/", true)
	waitSignal(t, entered, "handler entered")
	shutdown := shutdownAsync(srv, context.Background())
	fs := p.until("GOAWAY with last stream 1", func(f h2frame) bool { return f.Type == h2GoAway && f.LastStream == 1 })
	if last := fs[len(fs)-1]; last.Code != h2ErrNo {
		t.Fatalf("GOAWAY code = %v, want NO_ERROR", last.Code)
	}
	close(release)
	p.until("stream 1 completed", func(f h2frame) bool { return f.Stream == 1 && f.EndStream })
	_ = p.conn.Close()
	if err, _ := testwait.Recv(t, "Shutdown result", shutdown, nil); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
