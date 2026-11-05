package server

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testcert"
	"github.com/ok0/go-microgateway/internal/testwait"
)

const (
	testedLimit   = 30 * time.Millisecond
	untestedLimit = time.Hour
)

type outcome struct {
	Response     string
	BodyComplete bool
	Closed       bool
	End          string
	RawBytes     int
	Handler      string
	Handshake    string
}

type logBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// probe records what the handler of one run observed.
type probe struct {
	mu      sync.Mutex
	handler string
}

func (p *probe) set(s string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handler = s
}

func (p *probe) get() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.handler
}

type scenario struct {
	name      string
	tlsOn     bool
	handshake bool
	limits    [4]time.Duration // header, read, write, idle
	handler   func(p *probe) http.Handler
	client    func(t *testing.T, addr string, pool *x509.CertPool, p *probe, logs *logBuffer) outcome
}

func endKind(err error) string {
	switch {
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, syscall.ECONNRESET):
		return "reset"
	default:
		return "other"
	}
}

// collect reads the client side until the server ends the connection and classifies what arrived.
func collect(t *testing.T, conn net.Conn) outcome {
	t.Helper()
	c := capturing(conn)
	awaitCut(t, c, "client read end")
	var o outcome
	o.End = endKind(c.readErr())
	o.Closed = o.End == "eof" || o.End == "reset"
	data := c.bytes()
	o.RawBytes = len(data)
	if line, _, ok := strings.Cut(string(data), "\r\n"); ok && strings.HasPrefix(line, "HTTP/") {
		o.Response = line
		resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(data)), nil)
		if err == nil {
			_, err = io.ReadAll(resp.Body)
			o.BodyComplete = err == nil
		}
	}
	return o
}

func awaitHandshakeLog(t *testing.T, logs *logBuffer) string {
	t.Helper()
	testwait.Until(t, "TLS handshake error logged", func() bool {
		return strings.Contains(logs.String(), "http: TLS handshake error")
	}, logs.String)
	return "failed"
}

func okProbe(*probe) http.Handler {
	return okHandler()
}

func readAllProbe(p *probe) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				p.set("read-deadline")
			} else {
				p.set("read-error")
			}
			return
		}
		io.WriteString(w, "ok")
	})
}

func writeAllProbe(p *probe) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chunk := make([]byte, 32<<10)
		for range 2048 {
			if _, err := w.Write(chunk); err != nil {
				p.set("write-error")
				return
			}
		}
		p.set("complete")
	})
}

func clientPartialHeader(t *testing.T, addr string, _ *x509.CertPool, _ *probe, _ *logBuffer) outcome {
	conn := dialRaw(t, addr)
	writeString(t, conn, "GET / HTTP/1.1\r\nHost: x\r\n")
	return collect(t, conn)
}

func clientPartialBody(t *testing.T, addr string, _ *x509.CertPool, _ *probe, _ *logBuffer) outcome {
	conn := dialRaw(t, addr)
	writeString(t, conn, "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n0123456789")
	return collect(t, conn)
}

func clientNoRead(t *testing.T, addr string, _ *x509.CertPool, p *probe, _ *logBuffer) outcome {
	conn := dialRaw(t, addr)
	writeString(t, conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	testwait.Until(t, "handler write error", func() bool { return p.get() != "" }, p.get)
	return collect(t, conn)
}

func clientIdle(t *testing.T, addr string, _ *x509.CertPool, _ *probe, _ *logBuffer) outcome {
	conn := dialRaw(t, addr)
	writeString(t, conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	return collect(t, conn)
}

func deniedProbe(*probe) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, "denied")
	})
}

func clientEarlyBody(t *testing.T, addr string, _ *x509.CertPool, _ *probe, _ *logBuffer) outcome {
	conn := dialRaw(t, addr)
	writeString(t, conn, "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4194304\r\n\r\n")
	go conn.Write(make([]byte, 4<<20))
	return collect(t, conn)
}

func clientTLSSilent(t *testing.T, addr string, _ *x509.CertPool, _ *probe, logs *logBuffer) outcome {
	conn := dialRaw(t, addr)
	o := collect(t, conn)
	o.Handshake = awaitHandshakeLog(t, logs)
	return o
}

func clientTLSPartial(t *testing.T, addr string, _ *x509.CertPool, _ *probe, logs *logBuffer) outcome {
	conn := dialRaw(t, addr)
	hello := clientHello(t)
	writeString(t, conn, string(hello[:len(hello)/2]))
	o := collect(t, conn)
	o.Handshake = awaitHandshakeLog(t, logs)
	return o
}

// stockServer serves h with a plain net/http server configured with the scenario limits.
func stockServer(t *testing.T, sc scenario, h http.Handler, logs *logBuffer, certFile, keyFile string) string {
	t.Helper()
	p := new(http.Protocols)
	p.SetHTTP1(true)
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: sc.limits[0],
		ReadTimeout:       sc.limits[1],
		WriteTimeout:      sc.limits[2],
		IdleTimeout:       sc.limits[3],
		ErrorLog:          log.New(logs, "", 0),
		Protocols:         p,
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if sc.tlsOn {
		p.SetHTTP2(true)
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			t.Fatal(err)
		}
		srv.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
		go srv.ServeTLS(ln, "", "")
	} else {
		go srv.Serve(ln)
	}
	t.Cleanup(func() { srv.Close() })
	return ln.Addr().String()
}

// gatewayServer serves h with the gateway server configured with the scenario limits.
func gatewayServer(t *testing.T, sc scenario, h http.Handler, logs *logBuffer, certFile, keyFile string) string {
	t.Helper()
	muts := []func(*config.Listener){limits(sc.limits[0], sc.limits[1], sc.limits[2], sc.limits[3])}
	if sc.tlsOn {
		muts = append(muts, tlsOn(certFile, keyFile, true))
	}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	// Real clock: compares against net/http's own timeouts, which run only on wall time; every peer here stalls.
	srv, err := New(limitCfg(t, muts...), h, nil, nil, logger, WithClock(clock.Real()))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	cleanupShutdown(t, srv)
	return srv.Addrs()["public"].String()
}

// run executes the scenario against the stock server when stock is true and against the gateway server otherwise.
func (sc scenario) run(t *testing.T, certFile, keyFile string, pool *x509.CertPool, stock bool) outcome {
	t.Helper()
	p := &probe{}
	logs := &logBuffer{}
	h := sc.handler(p)
	var addr string
	if stock {
		addr = stockServer(t, sc, h, logs, certFile, keyFile)
	} else {
		addr = gatewayServer(t, sc, h, logs, certFile, keyFile)
	}
	o := sc.client(t, addr, pool, p, logs)
	o.Handler = p.get()
	if !sc.handshake {
		o.RawBytes = 0
	}
	return o
}

func TestLimitOutcomesMatchNetHTTP(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	scenarios := []scenario{
		{
			name:    "h1/header",
			limits:  [4]time.Duration{testedLimit, untestedLimit, untestedLimit, untestedLimit},
			handler: okProbe,
			client:  clientPartialHeader,
		},
		{
			name:    "h1/read",
			limits:  [4]time.Duration{untestedLimit, testedLimit, untestedLimit, untestedLimit},
			handler: readAllProbe,
			client:  clientPartialBody,
		},
		{
			name:    "h1/write",
			limits:  [4]time.Duration{untestedLimit, untestedLimit, testedLimit, untestedLimit},
			handler: writeAllProbe,
			client:  clientNoRead,
		},
		{
			name:    "h1/idle",
			limits:  [4]time.Duration{untestedLimit, untestedLimit, untestedLimit, testedLimit},
			handler: okProbe,
			client:  clientIdle,
		},
		{
			name:    "h1/early-body-close",
			limits:  [4]time.Duration{untestedLimit, untestedLimit, untestedLimit, untestedLimit},
			handler: deniedProbe,
			client:  clientEarlyBody,
		},
		{
			name:      "tls/handshake-silent",
			tlsOn:     true,
			handshake: true,
			limits:    [4]time.Duration{testedLimit, untestedLimit, untestedLimit, untestedLimit},
			handler:   okProbe,
			client:    clientTLSSilent,
		},
		{
			name:      "tls/handshake-partial",
			tlsOn:     true,
			handshake: true,
			limits:    [4]time.Duration{testedLimit, untestedLimit, untestedLimit, untestedLimit},
			handler:   okProbe,
			client:    clientTLSPartial,
		},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			stock := sc.run(t, certFile, keyFile, pool, true)
			gateway := sc.run(t, certFile, keyFile, pool, false)
			t.Logf("stock:   %+v", stock)
			t.Logf("gateway: %+v", gateway)
			if stock != gateway {
				t.Fatalf("outcomes differ: stock %+v, gateway %+v", stock, gateway)
			}
		})
	}
}

// clientBadRecord sends a record that is not a TLS handshake record and reads until the server closes the connection.
func clientBadRecord(t *testing.T, addr string, _ *x509.CertPool, _ *probe, logs *logBuffer) outcome {
	conn := dialRaw(t, addr)
	writeString(t, conn, "\x16\x03\x01\x00\x05hello")
	o := collect(t, conn)
	o.Handshake = awaitHandshakeLog(t, logs)
	return o
}

// clientPlaintext sends an HTTP/1.1 request to the TLS listener and reads until the server closes the connection.
func clientPlaintext(t *testing.T, addr string, _ *x509.CertPool, _ *probe, logs *logBuffer) outcome {
	conn := dialRaw(t, addr)
	writeString(t, conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	o := collect(t, conn)
	o.Handshake = awaitHandshakeLog(t, logs)
	return o
}

func TestTLSHandshakeOutcomesMatchNetHTTP(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	scenarios := []scenario{
		{
			name:      "tls/handshake-bad-record",
			tlsOn:     true,
			handshake: true,
			limits:    [4]time.Duration{untestedLimit, untestedLimit, untestedLimit, untestedLimit},
			handler:   okProbe,
			client:    clientBadRecord,
		},
		{
			name:      "tls/plaintext-400",
			tlsOn:     true,
			handshake: true,
			limits:    [4]time.Duration{untestedLimit, untestedLimit, untestedLimit, untestedLimit},
			handler:   okProbe,
			client:    clientPlaintext,
		},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			stock := sc.run(t, certFile, keyFile, pool, true)
			gateway := sc.run(t, certFile, keyFile, pool, false)
			t.Logf("stock:   %+v", stock)
			t.Logf("gateway: %+v", gateway)
			if stock != gateway {
				t.Fatalf("outcomes differ: stock %+v, gateway %+v", stock, gateway)
			}
		})
	}
}
