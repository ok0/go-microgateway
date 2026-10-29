package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
)

// testNetwork is the network a gateway test runs on: in memory (hermetic) or loopback TCP.
type testNetwork interface {
	// serve runs h on a new listener until the test ends.
	serve(t *testing.T, h http.Handler) *testServer
	// gateway builds a gateway from doc whose upstream connections use this network.
	gateway(t *testing.T, doc string, opts ...microgateway.Option) *microgateway.Gateway
	// client returns a client on this network; tune (may be nil) adjusts its transport. Idle connections are closed at test end.
	client(t *testing.T, tune func(*http.Transport)) *http.Client
	// dial opens a raw connection to s; it is closed at test end.
	dial(t *testing.T, s *testServer) net.Conn
	// openConns reports the open connection ends on the network; ok is false when it cannot be observed.
	openConns() (n int64, ok bool)
	refuse(t *testing.T, s *testServer)
	blackhole(t *testing.T) string
}

// testServer is a running test server and its server-side connection counts.
type testServer struct {
	URL     string       // "http://host:port"
	Addr    string       // "host:port"
	New     atomic.Int64 // connections accepted
	Open    atomic.Int64 // accepted connections not yet closed or hijacked
	Peak    atomic.Int64
	closeFn func()
}

func (s *testServer) track(_ net.Conn, st http.ConnState) {
	switch st {
	case http.StateNew:
		s.New.Add(1)
		open := s.Open.Add(1)
		for {
			peak := s.Peak.Load()
			if open <= peak || s.Peak.CompareAndSwap(peak, open) {
				break
			}
		}
	case http.StateClosed, http.StateHijacked:
		s.Open.Add(-1)
	}
}

type memNetwork struct {
	mu        sync.Mutex
	listeners map[string]*memListener
	refused   map[string]bool
	holes     map[string]bool
	failNext  map[string]int
	dials     map[string]int
	seq       int
	open      atomic.Int64
}

func newMemNetwork() *memNetwork {
	return &memNetwork{
		listeners: make(map[string]*memListener),
		refused:   make(map[string]bool),
		holes:     make(map[string]bool),
		failNext:  make(map[string]int),
		dials:     make(map[string]int),
	}
}

type memListener struct {
	addr string
	n    *memNetwork
	ch   chan net.Conn
	done chan struct{}
	once sync.Once
}

func (n *memNetwork) listen() *memListener {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.seq++
	l := &memListener{
		addr: fmt.Sprintf("srv%d.mem:80", n.seq),
		n:    n,
		ch:   make(chan net.Conn),
		done: make(chan struct{}),
	}
	n.listeners[l.addr] = l
	return l
}

func (n *memNetwork) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	refused := &net.OpError{Op: "dial", Net: network, Err: errors.New("connection refused")}
	n.mu.Lock()
	n.dials[addr]++
	if n.failNext[addr] > 0 {
		n.failNext[addr]--
		n.mu.Unlock()
		return nil, refused
	}
	l, ok := n.listeners[addr]
	isRefused := n.refused[addr]
	isHole := n.holes[addr]
	n.mu.Unlock()
	if isRefused {
		return nil, refused
	}
	if isHole {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if !ok {
		return nil, refused
	}
	c, s := net.Pipe()
	n.mu.Lock()
	n.seq++
	p := 10000 + n.seq
	n.mu.Unlock()
	client := &memConn{
		Conn:   c,
		local:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: p},
		remote: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 80},
		n:      n,
	}
	server := &memConn{
		Conn:   s,
		local:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 80},
		remote: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: p},
		n:      n,
	}
	n.open.Add(2)
	select {
	case l.ch <- server:
		return client, nil
	case <-l.done:
		_ = client.Close()
		_ = server.Close()
		return nil, refused
	case <-ctx.Done():
		_ = client.Close()
		_ = server.Close()
		return nil, ctx.Err()
	}
}

type memConn struct {
	net.Conn
	local  net.Addr
	remote net.Addr
	n      *memNetwork
	once   sync.Once
}

func (c *memConn) Close() error {
	c.once.Do(func() { c.n.open.Add(-1) })
	return c.Conn.Close()
}

func (c *memConn) LocalAddr() net.Addr { return c.local }

func (c *memConn) RemoteAddr() net.Addr { return c.remote }

func (l *memListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *memListener) Close() error {
	l.once.Do(func() {
		close(l.done)
		l.n.mu.Lock()
		delete(l.n.listeners, l.addr)
		l.n.mu.Unlock()
	})
	return nil
}

func (l *memListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 80}
}

func (n *memNetwork) serve(t *testing.T, h http.Handler) *testServer {
	t.Helper()
	return n.serveListener(t, n.listen(), h)
}

func (n *memNetwork) serveAt(t *testing.T, addr string, h http.Handler) *testServer {
	t.Helper()
	n.mu.Lock()
	l := &memListener{addr: addr, n: n, ch: make(chan net.Conn), done: make(chan struct{})}
	n.listeners[addr] = l
	n.mu.Unlock()
	return n.serveListener(t, l, h)
}

func (n *memNetwork) serveListener(t *testing.T, l *memListener, h http.Handler) *testServer {
	s := &testServer{URL: "http://" + l.addr, Addr: l.addr}
	srv := &http.Server{Handler: h, ConnState: s.track}
	served := make(chan struct{})
	go func() {
		_ = srv.Serve(l)
		close(served)
	}()
	t.Cleanup(func() {
		_ = srv.Close()
		<-served
	})
	return s
}

func (n *memNetwork) refuse(t *testing.T, s *testServer) {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	n.refused[s.Addr] = true
}

func (n *memNetwork) failDials(t *testing.T, s *testServer, k int) {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	n.failNext[s.Addr] = k
}

func (n *memNetwork) unrefuse(t *testing.T, s *testServer) {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.refused, s.Addr)
}

func (n *memNetwork) dialCount(addr string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.dials[addr]
}

func (n *memNetwork) blackhole(t *testing.T) string {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	n.seq++
	addr := fmt.Sprintf("blackhole%d.mem:80", n.seq)
	n.holes[addr] = true
	return addr
}

func (n *memNetwork) gateway(t *testing.T, doc string, opts ...microgateway.Option) *microgateway.Gateway {
	t.Helper()
	return gwWith(t, doc, append(opts, withDialer(n.DialContext))...)
}

func (n *memNetwork) client(t *testing.T, tune func(*http.Transport)) *http.Client {
	t.Helper()
	tr := &http.Transport{DialContext: n.DialContext}
	if tune != nil {
		tune(tr)
	}
	c := &http.Client{Transport: tr}
	t.Cleanup(c.CloseIdleConnections)
	return c
}

func (n *memNetwork) dial(t *testing.T, s *testServer) net.Conn {
	t.Helper()
	conn, err := n.DialContext(context.Background(), "tcp", s.Addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func (n *memNetwork) openConns() (int64, bool) {
	return n.open.Load(), true
}

type tcpNetwork struct{}

func (tcpNetwork) serve(t *testing.T, h http.Handler) *testServer {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	s := &testServer{}
	srv.Config.ConnState = s.track
	srv.Start()
	s.URL = srv.URL
	s.Addr = srv.Listener.Addr().String()
	s.closeFn = srv.Close
	t.Cleanup(srv.Close)
	return s
}

func (tcpNetwork) refuse(t *testing.T, s *testServer) {
	t.Helper()
	s.closeFn()
}

func (tcpNetwork) blackhole(t *testing.T) string {
	t.Helper()
	return "10.255.255.1:81"
}

func (tcpNetwork) gateway(t *testing.T, doc string, opts ...microgateway.Option) *microgateway.Gateway {
	t.Helper()
	return gwWith(t, doc, opts...)
}

func (tcpNetwork) client(t *testing.T, tune func(*http.Transport)) *http.Client {
	t.Helper()
	tr := &http.Transport{}
	if tune != nil {
		tune(tr)
	}
	c := &http.Client{Transport: tr}
	t.Cleanup(c.CloseIdleConnections)
	return c
}

func (tcpNetwork) dial(t *testing.T, s *testServer) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", s.Addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func (tcpNetwork) openConns() (int64, bool) {
	return 0, false
}

func delayed(clk *clock.Fake, d time.Duration, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-clk.After(d):
		case <-r.Context().Done():
			return
		}
		h.ServeHTTP(w, r)
	})
}

type heldBackend struct {
	URL     string
	arrived chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *heldBackend) Release() {
	h.once.Do(func() { close(h.release) })
}

func holdBackend(t *testing.T, n testNetwork) *heldBackend {
	t.Helper()
	h := &heldBackend{arrived: make(chan struct{}, 16), release: make(chan struct{})}
	be := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case h.arrived <- struct{}{}:
		default:
		}
		select {
		case <-h.release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}\n"))
	}))
	h.URL = be.URL
	t.Cleanup(h.Release)
	return h
}

type sseBackend struct {
	URL     string
	release chan struct{}
	once    sync.Once
}

func (b *sseBackend) Release() {
	b.once.Do(func() { close(b.release) })
}

func newSSEBackend(t *testing.T, n testNetwork) *sseBackend {
	t.Helper()
	b := &sseBackend{release: make(chan struct{})}
	s := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: one\n\n")
		_ = http.NewResponseController(w).Flush()
		select {
		case <-b.release:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "data: two\n\n")
	}))
	b.URL = s.URL
	t.Cleanup(b.Release)
	return b
}

type countedBackend struct {
	URL     string
	srv     *testServer
	arrived chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *countedBackend) releaseHold() {
	b.once.Do(func() { close(b.release) })
}

func countedBackendFor(t *testing.T, n testNetwork, name string) *countedBackend {
	t.Helper()
	b := &countedBackend{arrived: make(chan struct{}, 16), release: make(chan struct{})}
	b.srv = n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hold" {
			select {
			case b.arrived <- struct{}{}:
			default:
			}
			select {
			case <-b.release:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(echo{Backend: name, Path: r.URL.Path})
	}))
	b.URL = b.srv.URL
	t.Cleanup(b.releaseHold)
	return b
}

func TestMemNetwork(t *testing.T) {
	n := newMemNetwork()
	s := n.serve(t, echoHandler("mem"))
	client := n.client(t, nil)
	resp, err := client.Get(s.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := s.New.Load(); got != 1 {
		t.Fatalf("connections = %d, want 1", got)
	}
	if _, err := n.DialContext(context.Background(), "tcp", "unknown.mem:80"); err == nil {
		t.Fatal("dial to unknown address succeeded")
	}
	client.CloseIdleConnections()
	eventually(t, func() bool { return s.Open.Load() == 0 })
	eventually(t, func() bool {
		open, _ := n.openConns()
		return open == 0
	})

	refusedSrv := n.serve(t, echoHandler("refused"))
	n.refuse(t, refusedSrv)
	if _, err := n.DialContext(context.Background(), "tcp", refusedSrv.Addr); err == nil {
		t.Fatal("dial to refused address succeeded")
	}

	hole := n.blackhole(t)
	holeCtx, holeCancel := context.WithCancel(context.Background())
	holeDone := make(chan error, 1)
	go func() {
		_, err := n.DialContext(holeCtx, "tcp", hole)
		holeDone <- err
	}()
	holeCancel()
	select {
	case err := <-holeDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blackhole dial error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blackhole dial did not return after cancel")
	}

	gate := make(chan struct{})
	peakSrv := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-gate
		w.WriteHeader(http.StatusOK)
	}))
	peakClient := n.client(t, nil)
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := peakClient.Get(peakSrv.URL + "/")
			if err == nil {
				resp.Body.Close()
			}
		}()
	}
	eventually(t, func() bool { return peakSrv.Open.Load() == 3 })
	close(gate)
	wg.Wait()
	if got := peakSrv.Peak.Load(); got < 3 {
		t.Fatalf("peak connections = %d, want at least 3", got)
	}

	t.Run("failDials", func(t *testing.T) {
		failSrv := n.serve(t, echoHandler("fail"))
		n.failDials(t, failSrv, 1)
		_, err := n.DialContext(context.Background(), "tcp", failSrv.Addr)
		var oe *net.OpError
		if !errors.As(err, &oe) || oe.Op != "dial" {
			t.Fatalf("first dial error = %v, want a dial *net.OpError", err)
		}
		conn, err := n.DialContext(context.Background(), "tcp", failSrv.Addr)
		if err != nil {
			t.Fatalf("second dial error = %v, want nil", err)
		}
		_ = conn.Close()
		if got := n.dialCount(failSrv.Addr); got != 2 {
			t.Fatalf("dialCount = %d, want 2", got)
		}
	})
}
