package server

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testcert"
	"github.com/ok0/go-microgateway/internal/testwait"
)

func TestHTTP2HandshakeTimersEndAtFirstFrames(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	for _, kind := range []struct {
		name string
		tls  bool
	}{{"tls", true}, {"h2c", false}} {
		t.Run(kind.name, func(t *testing.T) {
			fake := clock.NewFake(serverStart)
			var muts []func(*config.Listener)
			var client *http.Client
			scheme := "http://"
			if kind.tls {
				muts = append(muts, tlsOn(certFile, keyFile, true))
				client = h2Client(t, pool, false)
				scheme = "https://"
			} else {
				muts = append(muts, h2cOn)
				client = h2Client(t, nil, true)
			}
			srv := startServer(t, limitCfg(t, muts...), okHandler(), nil, nil, WithClock(fake))
			url := scheme + srv.Addrs()["public"].String() + "/"
			first := getVia(t, client, url)
			if first.status != http.StatusOK || first.major != 2 {
				t.Fatalf("first request: status = %d, ProtoMajor = %d; want 200 over HTTP/2", first.status, first.major)
			}
			// Real stall: 2.5s is longer than the HTTP/2 server's 2s first-SETTINGS wait
			time.Sleep(2500 * time.Millisecond)
			second := getVia(t, client, url)
			if second.status != http.StatusOK || second.body != "ok" || !second.reused {
				t.Fatalf("second request: status = %d, body = %q, reused = %v; want 200, \"ok\", reused", second.status, second.body, second.reused)
			}
		})
	}
}

// recordConn keeps a copy of every write made to it.
type recordConn struct {
	net.Conn
	mu     sync.Mutex
	writes [][]byte
}

func (c *recordConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	c.writes = append(c.writes, append([]byte(nil), b...))
	c.mu.Unlock()
	return c.Conn.Write(b)
}

func TestHTTP2FirstFlightCarriesSettings(t *testing.T) {
	fake := clock.NewFake(serverStart)
	srv := startServer(t, limitCfg(t, h2cOn), okHandler(), nil, nil, WithClock(fake))
	addr := srv.Addrs()["public"].String()

	var mu sync.Mutex
	var conns []*recordConn
	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	tr := &http.Transport{
		Protocols: &p,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			c, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			rc := &recordConn{Conn: c}
			mu.Lock()
			conns = append(conns, rc)
			mu.Unlock()
			return rc, nil
		},
	}
	got := getVia(t, newClient(t, tr), "http://"+addr+"/")
	if got.status != http.StatusOK || got.major != 2 {
		t.Fatalf("status = %d, ProtoMajor = %d; want 200 over HTTP/2", got.status, got.major)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(conns) != 1 {
		t.Fatalf("dialed %d connections, want 1", len(conns))
	}
	conns[0].mu.Lock()
	defer conns[0].mu.Unlock()
	if len(conns[0].writes) == 0 {
		t.Fatal("no writes recorded")
	}
	w := conns[0].writes[0]
	if len(w) < len(h2Preface)+9 || string(w[:len(h2Preface)]) != h2Preface {
		t.Fatalf("first write does not begin with the client preface: %q", w[:min(len(w), 40)])
	}
	hdr := w[len(h2Preface):]
	n := int(hdr[0])<<16 | int(hdr[1])<<8 | int(hdr[2])
	if hdr[3] != 0x4 || hdr[4] != 0 || binary.BigEndian.Uint32(hdr[5:9])&0x7fffffff != 0 {
		t.Fatalf("frame after the preface has type %#x flags %#x, want SETTINGS on stream 0", hdr[3], hdr[4])
	}
	if len(hdr) < 9+n {
		t.Fatalf("SETTINGS payload incomplete in the first write: have %d bytes, declared %d", len(hdr)-9, n)
	}
}

func TestEarlyBodyCloseResponseComplete(t *testing.T) {
	fake := clock.NewFake(serverStart)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, "denied")
	})
	srv := startServer(t, limitCfg(t), h, nil, nil, WithClock(fake))
	conn := dialRaw(t, srv.Addrs()["public"].String())
	const size = 4 << 20
	go func() {
		writeString(t, conn, "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4194304\r\n\r\n")
		_, _ = conn.Write(make([]byte, size))
	}()
	br := bufio.NewReader(conn)
	status, body := readResponse(t, br)
	if status != http.StatusUnauthorized || body != "denied" {
		t.Fatalf("status = %d, body = %q; want 401 \"denied\"", status, body)
	}
	end := make(chan error, 1)
	go func() {
		_, err := br.ReadByte()
		end <- err
	}()
	err, _ := testwait.Recv(t, "end of stream after the response", end, nil)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("read after the response = %v, want io.EOF", err)
	}
}

func TestTLSCloseNotifyAfterResponse(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	fake := clock.NewFake(serverStart)
	srv := startServer(t, limitCfg(t, tlsOn(certFile, keyFile, false)), okHandler(), nil, nil, WithClock(fake))
	conn := dialTLS(t, srv.Addrs()["public"].String(), pool)
	writeString(t, conn, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	// Real stall: 20ms before reading
	time.Sleep(20 * time.Millisecond)
	br := bufio.NewReader(conn)
	if status, body := readResponse(t, br); status != http.StatusOK || body != "ok" {
		t.Fatalf("status = %d, body = %q; want 200 \"ok\"", status, body)
	}
	end := make(chan error, 1)
	go func() {
		_, err := br.ReadByte()
		end <- err
	}()
	err, _ := testwait.Recv(t, "close_notify", end, nil)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("read after the response = %v, want io.EOF from close_notify", err)
	}
}

func TestHTTP2GracefulShutdownCompletesStreams(t *testing.T) {
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	fake := clock.NewFake(serverStart)
	body := strings.Repeat("0123456789abcdef", 4096)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		_, _ = io.WriteString(w, body)
	})
	srv := startServer(t, limitCfg(t, tlsOn(certFile, keyFile, true)), h, nil, nil, WithClock(fake))
	addr := srv.Addrs()["public"].String()
	client := h2Client(t, pool, false)

	type result struct {
		status int
		body   string
		err    error
	}
	got := make(chan result, 1)
	go func() {
		resp, err := client.Get("https://" + addr + "/slow")
		if err != nil {
			got <- result{err: err}
			return
		}
		b, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		got <- result{status: resp.StatusCode, body: string(b), err: err}
	}()
	testwait.Done(t, "slow handler entered", entered, nil)

	shutdown := shutdownAsync(srv, context.Background())
	close(release)
	r, _ := testwait.Recv(t, "in-flight stream response", got, nil)
	if r.err != nil {
		t.Fatalf("in-flight stream: %v", r.err)
	}
	if r.status != http.StatusOK || r.body != body {
		t.Fatalf("in-flight stream: status = %d, body length = %d; want 200 and the full body", r.status, len(r.body))
	}
	err, _ := testwait.Recv(t, "Shutdown to return", shutdown, nil)
	if err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestShutdownKeepsYoungSilentNewConn(t *testing.T) {
	fake := clock.NewFake(serverStart)
	cfg := limitCfg(t, limits(time.Hour, time.Hour, time.Hour, time.Hour))
	cfg.Global.ShutdownDelay = 0
	cfg.Global.DrainTimeout = config.Duration(10 * time.Second)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		_, _ = io.WriteString(w, "ok")
	})
	srv := startServer(t, cfg, h, nil, nil, WithClock(fake))
	addr := srv.Addrs()["public"].String()

	silent := dialRaw(t, addr)
	c := capturing(silent)
	// Real stall: lets the server accept the silent connection before Shutdown starts.
	time.Sleep(100 * time.Millisecond)
	hang := dialRaw(t, addr)
	writeString(t, hang, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	testwait.Done(t, "handler entered", entered, nil)

	shutdown := shutdownAsync(srv, context.Background())
	awaitDue(t, fake, 1, 10*time.Second, "drain timer")
	fake.Advance(9 * time.Second)
	stillOpen(t, c)
	select {
	case err := <-shutdown:
		t.Fatalf("Shutdown returned before the drain limit: %v", err)
	default:
	}

	close(release)
	if status, body := readResponse(t, bufio.NewReader(hang)); status != http.StatusOK || body != "ok" {
		t.Fatalf("in-flight request: status = %d, body = %q; want 200 \"ok\"", status, body)
	}
	stillOpen(t, c)

	fake.Advance(time.Second)
	err, _ := testwait.Recv(t, "Shutdown to return", shutdown, nil)
	if !errors.Is(err, ErrForcedClose) {
		t.Fatalf("Shutdown error = %v, want ErrForcedClose", err)
	}
	awaitCut(t, c, "silent connection closed at the drain limit")
	if !errors.Is(c.readErr(), io.EOF) {
		t.Fatalf("silent connection read ended with %v, want io.EOF", c.readErr())
	}
}

func TestShutdownClosesSilentNewConnAfterFiveSeconds(t *testing.T) {
	if testing.Short() {
		t.Skip("waits 5 s of wall time for net/http's StateNew rule")
	}
	cfg := limitCfg(t, limits(time.Hour, time.Hour, time.Hour, time.Hour))
	cfg.Global.ShutdownDelay = 0
	cfg.Global.DrainTimeout = config.Duration(30 * time.Second)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		_, _ = io.WriteString(w, "ok")
	})
	// Real clock: net/http's Shutdown counts a StateNew connection as idle only after 5 s of wall time.
	srv := startServer(t, cfg, h, nil, nil, WithClock(clock.Real()))
	addr := srv.Addrs()["public"].String()

	silent := dialRaw(t, addr)
	c := capturing(silent)
	hang := dialRaw(t, addr)
	writeString(t, hang, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	testwait.Done(t, "handler entered", entered, nil)

	// Real stall: net/http closes a connection still in StateNew only once it is older than 5 s of wall time.
	time.Sleep(5100 * time.Millisecond)
	shutdown := shutdownAsync(srv, context.Background())
	awaitCut(t, c, "silent connection closed by Shutdown")
	if !errors.Is(c.readErr(), io.EOF) {
		t.Fatalf("silent connection read ended with %v, want io.EOF", c.readErr())
	}

	close(release)
	if status, body := readResponse(t, bufio.NewReader(hang)); status != http.StatusOK || body != "ok" {
		t.Fatalf("request started before Shutdown: status = %d, body = %q; want 200 \"ok\"", status, body)
	}
	err, _ := testwait.Recv(t, "Shutdown to return", shutdown, nil)
	if err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
