package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/memnet"
	"github.com/ok0/go-microgateway/internal/testcert"
)

type memNet struct {
	mu     sync.Mutex
	buffer int
	lns    map[string]*memnet.Listener
	n      int
}

func newMemNet(buffer int) *memNet {
	return &memNet{buffer: buffer, lns: map[string]*memnet.Listener{}}
}

func (m *memNet) listen(network, address string) (net.Listener, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.n++
	ln := memnet.Listen(fmt.Sprintf("127.0.0.1:%d", m.n), m.buffer)
	m.lns[ln.Addr().String()] = ln
	return ln, nil
}

func (m *memNet) dial(ctx context.Context, network, address string) (net.Conn, error) {
	m.mu.Lock()
	ln, ok := m.lns[address]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("memnet: no listener at %s", address)
	}
	return ln.DialContext(ctx, network, address)
}

func (m *memNet) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ln := range m.lns {
		_ = ln.Close()
	}
}

type bubbleWire struct {
	name     string
	tls      bool
	certFile string
	keyFile  string
	pool     *x509.CertPool
}

func bubbleWires(t *testing.T) []bubbleWire {
	t.Helper()
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	return []bubbleWire{
		{name: "tls", tls: true, certFile: certFile, keyFile: keyFile, pool: pool},
		{name: "h2c"},
	}
}

func (w bubbleWire) config(t *testing.T, muts ...func(*config.Listener)) *config.Config {
	t.Helper()
	if w.tls {
		muts = append(muts, tlsOn(w.certFile, w.keyFile, true))
	} else {
		muts = append(muts, h2cOn)
	}
	return limitCfg(t, muts...)
}

func startBubbleGateway(t *testing.T, m *memNet, cfg *config.Config, h http.Handler, logs io.Writer, opts ...Option) *Server {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	srv, err := New(cfg, h, nil, nil, logger, opts...)
	if err != nil {
		t.Fatal(err)
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
	return srv
}

func startBubbleStock(t *testing.T, m *memNet, w bubbleWire, lim [4]time.Duration, h http.Handler, logs io.Writer) string {
	t.Helper()
	p := new(http.Protocols)
	p.SetHTTP1(true)
	srv := &http.Server{
		Handler:           h,
		MaxHeaderBytes:    1 << 20,
		ReadHeaderTimeout: lim[0],
		ReadTimeout:       lim[1],
		WriteTimeout:      lim[2],
		IdleTimeout:       lim[3],
		HTTP2:             &http.HTTP2Config{},
		ErrorLog:          log.New(logs, "", 0),
		Protocols:         p,
	}
	var cert tls.Certificate
	if w.tls {
		p.SetHTTP2(true)
		var err error
		cert, err = tls.LoadX509KeyPair(w.certFile, w.keyFile)
		if err != nil {
			t.Fatal(err)
		}
		srv.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h2", "http/1.1"},
			MinVersion:   tls.VersionTLS12,
		}
	} else {
		p.SetUnencryptedHTTP2(true)
	}
	ln, err := m.listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if w.tls {
		go func() { _ = srv.ServeTLS(ln, "", "") }()
	} else {
		go func() { _ = srv.Serve(ln) }()
	}
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

func bubbleClient(t *testing.T, m *memNet, w bubbleWire) *http.Client {
	t.Helper()
	tr := &http.Transport{
		DialContext:       m.dial,
		TLSClientConfig:   &tls.Config{RootCAs: w.pool, ServerName: "localhost"},
		ForceAttemptHTTP2: true,
	}
	if !w.tls {
		p := new(http.Protocols)
		p.SetUnencryptedHTTP2(true)
		tr.Protocols = p
	}
	return newClient(t, tr)
}

func bubblePeer(m *memNet, w bubbleWire, addr string) (*h2peer, error) {
	conn, err := m.dial(context.Background(), "tcp", addr)
	if err != nil {
		return nil, err
	}
	p, err := openH2Conn(conn, w.pool)
	if err != nil {
		return nil, err
	}
	p.bubble = true
	return p, nil
}

type countClock struct {
	clock.Clock
	mu   sync.Mutex
	live int
}

func (c *countClock) add(d int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.live += d
}

func (c *countClock) armed() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.live
}

func (c *countClock) AfterFunc(d time.Duration, f func()) clock.Timer {
	c.add(1)
	t := &countTimer{c: c}
	t.t = c.Clock.AfterFunc(d, func() {
		c.add(-1)
		f()
	})
	return t
}

type countTimer struct {
	c *countClock
	t clock.Timer
}

func (t *countTimer) Stop() bool {
	ok := t.t.Stop()
	if ok {
		t.c.add(-1)
	}
	return ok
}

func (t *countTimer) Reset(d time.Duration) bool {
	ok := t.t.Reset(d)
	if !ok {
		t.c.add(1)
	}
	return ok
}

func TestRawPeerHandshakeOnMemnet(t *testing.T) {
	for _, w := range bubbleWires(t) {
		t.Run(w.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				m := newMemNet(64 << 10)
				t.Cleanup(m.close)
				lim := [4]time.Duration{time.Hour, 0, 0, time.Hour}
				stockAddr := startBubbleStock(t, m, w, lim, okHandler(), &logBuffer{})
				gw := startBubbleGateway(t, m, w.config(t, limits(time.Hour, 0, 0, time.Hour)), okHandler(), &logBuffer{}, WithClock(clock.Real()))
				sides := []struct{ name, addr string }{
					{"stock", stockAddr},
					{"gateway", gw.Addrs()["public"].String()},
				}
				for _, side := range sides {
					p, err := bubblePeer(m, w, side.addr)
					if err != nil {
						t.Fatalf("%s: %v", side.name, err)
					}
					p.t = t
					t.Cleanup(p.close)
					p.handshake(true)
					p.headers(1, http.MethodGet, "/", true)
					o := outcomeOf(p.until("stream 1 ended", streamEnded(1)))
					if !slices.Contains(o.Streams[1], "HEADERS 200") {
						t.Errorf("%s: stream 1 headers %v, want HEADERS 200", side.name, o.Streams[1])
					}
					if o.Data[1] != 2 {
						t.Errorf("%s: stream 1 DATA %d bytes, want 2", side.name, o.Data[1])
					}
				}
			})
		})
	}
}
