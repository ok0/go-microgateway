package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testcert"
	"github.com/ok0/go-microgateway/internal/testwait"
)

// h2MaxAttempts is the number of attempts a comparison makes. Attempt k scales the tested limits by 2^k, so the last
// attempt allows 3.84 s where the first allows 30 ms, and a server that never settles fails instead of rerunning
// without end.
const h2MaxAttempts = 8

// errUnsettled marks an attempt whose precondition was lost to the real-time limits. It is rerun with longer limits and
// never compared.
var errUnsettled = errors.New("attempt did not settle")

// unsettled returns an error that marks the attempt as unsettled, with a reason.
func unsettled(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errUnsettled, fmt.Sprintf(format, args...))
}

// startStockH2 serves h with a plain net/http server whose limits are lim (header, read, write, idle), over TLS with
// HTTP/2 when tr is TLS and over prior-knowledge h2c otherwise. It returns the address and a function that stops it.
func startStockH2(tr h2Transport, lim [4]time.Duration, h http.Handler, logs *logBuffer) (string, func(), error) {
	p := new(http.Protocols)
	p.SetHTTP1(true)
	srv := &http.Server{
		Handler:           h,
		MaxHeaderBytes:    1 << 20,
		ReadHeaderTimeout: lim[0],
		ReadTimeout:       lim[1],
		WriteTimeout:      lim[2],
		IdleTimeout:       lim[3],
		ErrorLog:          log.New(logs, "", 0),
		Protocols:         p,
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	if tr.tls {
		p.SetHTTP2(true)
		cert, err := tls.LoadX509KeyPair(tr.certFile, tr.keyFile)
		if err != nil {
			ln.Close()
			return "", nil, err
		}
		srv.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
		go srv.ServeTLS(ln, "", "")
	} else {
		p.SetUnencryptedHTTP2(true)
		go srv.Serve(ln)
	}
	return ln.Addr().String(), func() { srv.Close() }, nil
}

// startGatewayH2 serves h with the gateway server configured by cfg. It returns the public address and a function that
// stops the server.
func startGatewayH2(cfg *config.Config, h http.Handler, logs *logBuffer) (string, func(), error) {
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	// Real clock: compares against net/http's own timeouts, which run only on wall time; every peer here stalls.
	srv, err := New(cfg, h, nil, nil, logger, WithClock(clock.Real()))
	if err != nil {
		return "", nil, err
	}
	if err := srv.Start(); err != nil {
		return "", nil, err
	}
	return srv.Addrs()["public"].String(), func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		srv.Shutdown(ctx)
	}, nil
}

// h2scenario is one HTTP/2 limit comparison. drive runs the client of one attempt and reports its outcome, or an error
// that is unsettled when the attempt's precondition was lost to the real-time limits.
type h2scenario struct {
	name    string
	limits  [4]time.Duration
	handler func(p *probe) http.Handler
	drive   func(r *h2run) (h2outcome, error)
	// check, when set, is an extra requirement on each side's outcome.
	check func(t *testing.T, side string, o h2outcome)
}

// scaleLimits returns lim with every tested limit scaled by 2^k. Entries that are neither testedLimit nor 3*testedLimit
// are unchanged.
func scaleLimits(lim [4]time.Duration, k int) [4]time.Duration {
	for i, d := range lim {
		switch d {
		case testedLimit, 3 * testedLimit:
			lim[i] = d << k
		}
	}
	return lim
}

// h2run is one side of one attempt. It runs off the test goroutine and never calls testing.T.
type h2run struct {
	tr    h2Transport
	addr  string
	k     int
	lim   [4]time.Duration
	probe *probe
	mu    sync.Mutex
	ret   map[string]chan struct{}
	peers []*h2peer
}

// retLocked returns the channel that closes when the handler for path returns. The caller holds mu.
func (r *h2run) retLocked(path string) chan struct{} {
	ch, ok := r.ret[path]
	if !ok {
		ch = make(chan struct{})
		r.ret[path] = ch
	}
	return ch
}

// returned returns the channel that closes when the handler for path returns.
func (r *h2run) returned(path string) <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.retLocked(path)
}

// wrap returns a handler that runs h and then closes the channel of its path, unless it is already closed.
func (r *h2run) wrap(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			ch := r.retLocked(req.URL.Path)
			select {
			case <-ch:
			default:
				close(ch)
			}
		}()
		h.ServeHTTP(w, req)
	})
}

// awaitHandler waits until the handler for path returns, or the connection of c ends first, which is unsettled.
func (r *h2run) awaitHandler(c *h2peer, path string) error {
	select {
	case <-r.returned(path):
		return nil
	case <-c.done:
	}
	select {
	case <-r.returned(path):
		return nil
	default:
		return unsettled("connection ended before the %s handler returned", path)
	}
}

// dial connects a peer to the server of r, with a receive buffer of rcvBuf when it is above zero. A TLS handshake the
// server cut is unsettled.
func (r *h2run) dial(rcvBuf int) (*h2peer, error) {
	p, err := openH2(r.addr, r.tr.pool, rcvBuf)
	if err != nil {
		if errors.Is(err, errTLSHandshake) {
			return nil, unsettled("%v", err)
		}
		return nil, err
	}
	r.peers = append(r.peers, p)
	return p, nil
}

// handshake sends the preface with settings, starts the reader of c and waits for the server SETTINGS. The comparison
// counts no timers, so it sends no barrier.
func (r *h2run) handshake(c *h2peer, ackPings bool, gate uint32, settings ...h2Setting) (h2frame, error) {
	if err := c.writePreface(settings...); err != nil {
		return h2frame{}, unsettled("preface: %v", err)
	}
	c.start(ackPings, gate)
	fs, ev := c.await(nil, func(f h2frame) bool { return f.Type == h2Settings && !f.Ack })
	if ev != evFrame {
		return h2frame{}, unsettled("connection ended before the server SETTINGS: %v", c.endErr())
	}
	return fs[len(fs)-1], nil
}

// close closes every peer of r.
func (r *h2run) close() {
	for _, p := range r.peers {
		p.close()
	}
}

func driveSettings(r *h2run) (h2outcome, error) {
	c, err := r.dial(0)
	if err != nil {
		return h2outcome{}, err
	}
	s, err := r.handshake(c, true, 0)
	if err != nil {
		return h2outcome{}, err
	}
	wu, ev := c.await(nil, connWindowUpdate)
	if ev != evFrame {
		return h2outcome{}, fmt.Errorf("connection ended before the connection WINDOW_UPDATE: %v", c.endErr())
	}
	o := outcomeOf(nil)
	o.Settings = settingsLine(s.Settings, wu[len(wu)-1].Incr)
	o.End = peerEnd(c)
	return o, nil
}

func driveRead(r *h2run) (h2outcome, error) {
	c, err := r.dial(0)
	if err != nil {
		return h2outcome{}, err
	}
	if _, err := r.handshake(c, true, 0); err != nil {
		return h2outcome{}, err
	}
	if err := c.writeHeaders(1, http.MethodPost, "/", false); err != nil {
		return h2outcome{}, unsettled("HEADERS: %v", err)
	}
	if err := c.writeData(1, []byte("0123456789"), false); err != nil {
		return h2outcome{}, unsettled("DATA: %v", err)
	}
	fs, ev := c.await(nil, streamEnded(1))
	if ev == evEnd {
		return h2outcome{}, unsettled("connection ended before stream 1 ended")
	}
	if err := r.awaitHandler(c, "/"); err != nil {
		return h2outcome{}, err
	}
	return finish(c, fs), nil
}

// awaitTailData sends a GET on stream 1 and collects frames until 65535 DATA bytes arrive on it. It is unsettled when the
// connection ends first, or when the write limit ends stream 1 before the window is filled.
func awaitTailData(r *h2run, c *h2peer) ([]h2frame, error) {
	if err := c.writeHeaders(1, http.MethodGet, "/", true); err != nil {
		return nil, unsettled("HEADERS: %v", err)
	}
	fs, ev := c.await(nil, dataOrEnd(1, 65535))
	if ev == evEnd {
		return nil, unsettled("connection ended after %d DATA bytes on stream 1", dataBytes(fs, 1))
	}
	if dataBytes(fs, 1) < 65535 {
		return nil, unsettled("stream 1 ended after %d of 65535 DATA bytes", dataBytes(fs, 1))
	}
	return fs, nil
}

func driveWrite(r *h2run) (h2outcome, error) {
	c, err := r.dial(0)
	if err != nil {
		return h2outcome{}, err
	}
	if _, err := r.handshake(c, true, 0); err != nil {
		return h2outcome{}, err
	}
	fs, err := awaitTailData(r, c)
	if err != nil {
		return h2outcome{}, err
	}
	if !streamEnded(1)(fs[len(fs)-1]) {
		more, ev := c.await(nil, streamEnded(1))
		fs = append(fs, more...)
		if ev == evEnd {
			return h2outcome{}, unsettled("connection ended before stream 1 ended")
		}
	}
	if err := r.awaitHandler(c, "/"); err != nil {
		return h2outcome{}, err
	}
	return finish(c, fs), nil
}

func driveTail(r *h2run) (h2outcome, error) {
	c, err := r.dial(0)
	if err != nil {
		return h2outcome{}, err
	}
	if _, err := r.handshake(c, true, 0); err != nil {
		return h2outcome{}, err
	}
	fs, err := awaitTailData(r, c)
	if err != nil {
		return h2outcome{}, err
	}
	if streamEnded(1)(fs[len(fs)-1]) {
		return h2outcome{}, unsettled("stream 1 ended before its handler returned")
	}
	more, ev := c.await(r.returned("/"), streamEnded(1))
	fs = append(fs, more...)
	switch ev {
	case evEnd:
		return h2outcome{}, unsettled("connection ended before the handler returned")
	case evFrame:
		return h2outcome{}, unsettled("stream 1 ended before its handler returned")
	}
	more, ev = c.await(nil, streamEnded(1))
	fs = append(fs, more...)
	if ev == evEnd {
		return h2outcome{}, unsettled("connection ended before stream 1 ended")
	}
	return finish(c, fs), nil
}

// driveBlocked holds stream 1 inside a 16 MiB DATA write with a 4 KiB receive buffer, lets stream 3 time out behind
// it, then releases the peer and sends a PING. The attempt settles when stream 1, whose write deadline the handler clears, ends without a reset and RST_STREAM on stream 3 precedes the PING ACK.
func driveBlocked(r *h2run) (h2outcome, error) {
	c, err := r.dial(4096)
	if err != nil {
		return h2outcome{}, err
	}
	if _, err := r.handshake(c, true, 1,
		h2Setting{ID: h2SettingInitialWindowSize, Val: 1<<31 - 1},
		h2Setting{ID: h2SettingMaxFrameSize, Val: 1<<24 - 1},
	); err != nil {
		return h2outcome{}, err
	}
	if err := c.writeWindowUpdate(0, 1<<31-1-65535); err != nil {
		return h2outcome{}, unsettled("WINDOW_UPDATE: %v", err)
	}
	if err := c.writeHeaders(1, http.MethodGet, "/a", true); err != nil {
		return h2outcome{}, unsettled("HEADERS: %v", err)
	}
	fs, ev := c.await(c.gated, streamEnded(1))
	if ev == evEnd {
		return h2outcome{}, unsettled("connection ended before stream 1 DATA")
	}
	if ev == evFrame {
		return h2outcome{}, unsettled("stream 1 ended before its DATA write")
	}
	if err := c.writeHeaders(3, http.MethodGet, "/b", true); err != nil {
		return h2outcome{}, unsettled("HEADERS: %v", err)
	}
	// The reader is held at the gate, and the server runs /b once it reads HEADERS 3, which it does while its stream 1
	// write is blocked.
	<-r.returned("/b")
	// Real stall: twice the write limit, so stream 3's limit expires while stream 1's write holds the socket; the slack
	// grows with each attempt.
	time.Sleep(2 * r.lim[2])
	var token [8]byte
	binary.BigEndian.PutUint64(token[:], uint64(r.k)+1)
	if err := c.writePing(token); err != nil {
		return h2outcome{}, unsettled("PING: %v", err)
	}
	close(c.release)
	ended, acked := false, false
	more, ev := c.await(nil, func(f h2frame) bool {
		ended = ended || streamEnded(1)(f)
		acked = acked || (f.Type == h2Ping && f.Ack && f.Ping == token)
		return ended && acked
	})
	fs = append(fs, more...)
	if ev == evEnd {
		return h2outcome{}, unsettled("connection ended before stream 1 ended and the PING ACK")
	}
	rst, ack := -1, -1
	for i, f := range fs {
		if rst < 0 && f.Stream == 3 && f.Type == h2RSTStream {
			rst = i
		}
		if ack < 0 && f.Type == h2Ping && f.Ack && f.Ping == token {
			ack = i
		}
	}
	if !(rst >= 0 && ack >= 0 && rst < ack) {
		return h2outcome{}, unsettled("RST_STREAM on stream 3 did not precede the PING ACK")
	}
	// Stream 1 clears its write deadline through the serve loop. When the limit expired first, the stream was reset
	// behind the blocked write, which says nothing about the sides' handling of a cleared deadline.
	if i := slices.IndexFunc(fs, func(f h2frame) bool { return f.Stream == 1 && f.Type == h2RSTStream }); i >= 0 {
		return h2outcome{}, unsettled("stream 1 was reset before its write deadline was cleared")
	}
	if err := r.awaitHandler(c, "/a"); err != nil {
		return h2outcome{}, err
	}
	return finish(c, fs), nil
}

func driveIdle(r *h2run) (h2outcome, error) {
	c, err := r.dial(0)
	if err != nil {
		return h2outcome{}, err
	}
	if _, err := r.handshake(c, true, 0); err != nil {
		return h2outcome{}, err
	}
	fs, _ := c.await(nil, goAwayFrame)
	return finishEnded(c, fs), nil
}

func driveIdleAfterStream(r *h2run) (h2outcome, error) {
	c, err := r.dial(0)
	if err != nil {
		return h2outcome{}, err
	}
	if _, err := r.handshake(c, true, 0); err != nil {
		return h2outcome{}, err
	}
	if err := c.writeHeaders(1, http.MethodGet, "/", true); err != nil {
		return h2outcome{}, unsettled("HEADERS: %v", err)
	}
	fs, ev := c.await(nil, func(f h2frame) bool { return streamEnded(1)(f) || goAwayFrame(f) })
	if ev == evEnd || goAwayFrame(fs[len(fs)-1]) {
		return h2outcome{}, unsettled("idle limit expired before stream 1 was served")
	}
	more, _ := c.await(nil, goAwayFrame)
	fs = append(fs, more...)
	return finishEnded(c, fs), nil
}

// driveIdleNoPing keeps an idle connection under a read limit, with a peer that acknowledges PING only when ackPings is
// set, and records the GOAWAY, the end and whether the server sent a PING.
func driveIdleNoPing(ackPings bool) func(r *h2run) (h2outcome, error) {
	return func(r *h2run) (h2outcome, error) {
		c, err := r.dial(0)
		if err != nil {
			return h2outcome{}, err
		}
		if _, err := r.handshake(c, ackPings, 0); err != nil {
			return h2outcome{}, err
		}
		fs, _ := c.await(nil, goAwayFrame)
		<-c.done
		end := endKind(c.endErr())
		fs = append(fs, drain(c)...)
		o := outcomeOf(fs)
		o.End = end
		o.Pinged = slices.ContainsFunc(fs, pingFrame)
		return o, nil
	}
}

// requireNoPing fails t if the server sent a PING while idle under a read limit.
func requireNoPing(t *testing.T, side string, o h2outcome) {
	t.Helper()
	if o.Pinged {
		t.Errorf("%s: server sent a PING while idle under a read limit: %+v", side, o)
	}
}

// h2Scenarios returns the HTTP/2 limit comparisons. Each runs once per transport. The tested limits are 30 ms and the
// untested ones 1 h, as in the HTTP/1 comparison.
func h2Scenarios() []h2scenario {
	return []h2scenario{
		{
			name:    "h2/settings",
			limits:  [4]time.Duration{untestedLimit, 0, 0, untestedLimit},
			handler: okProbe,
			drive:   driveSettings,
		},
		{
			name:    "h2/read",
			limits:  [4]time.Duration{untestedLimit, testedLimit, 0, untestedLimit},
			handler: readAllProbe,
			drive:   driveRead,
		},
		{
			name:    "h2/write",
			limits:  [4]time.Duration{untestedLimit, 0, testedLimit, untestedLimit},
			handler: h2BigProbe,
			drive:   driveWrite,
		},
		{
			name:    "h2/write-tail-uncommitted",
			limits:  [4]time.Duration{untestedLimit, 0, testedLimit, untestedLimit},
			handler: h2TailProbe,
			drive:   driveTail,
		},
		{
			name:    "h2/write-tail-blocked",
			limits:  [4]time.Duration{untestedLimit, 0, testedLimit, untestedLimit},
			handler: h2BlockedProbe,
			drive:   driveBlocked,
		},
		{
			name:    "h2/idle",
			limits:  [4]time.Duration{untestedLimit, 0, 0, testedLimit},
			handler: okProbe,
			drive:   driveIdle,
		},
		{
			name:    "h2/idle-after-stream",
			limits:  [4]time.Duration{untestedLimit, 0, 0, testedLimit},
			handler: okProbe,
			drive:   driveIdleAfterStream,
		},
		{
			name:    "h2/read-no-ping",
			limits:  [4]time.Duration{untestedLimit, testedLimit, 0, 3 * testedLimit},
			handler: okProbe,
			drive:   driveIdleNoPing(true),
			check:   requireNoPing,
		},
	}
}

// h2combo is one scenario on one transport and what its attempts produced.
type h2combo struct {
	name    string
	sc      h2scenario
	tr      h2Transport
	cfgs    [h2MaxAttempts]*config.Config
	stock   h2outcome
	gateway h2outcome
	settled bool
	err     error
	tries   []string
}

// h2Combos returns one combo per scenario of scs on each transport. The gateway configurations are built on the test
// goroutine.
func h2Combos(t *testing.T, scs []h2scenario) []*h2combo {
	var combos []*h2combo
	for _, tr := range h2Transports(t) {
		for _, sc := range scs {
			c := &h2combo{name: sc.name + "/" + tr.name, sc: sc, tr: tr}
			for k := range h2MaxAttempts {
				lim := scaleLimits(sc.limits, k)
				c.cfgs[k] = tr.config(t, limits(lim[0], lim[1], lim[2], lim[3]))
			}
			combos = append(combos, c)
		}
	}
	return combos
}

// attempt runs the scenario once at attempt k against the stock server when stock is set and against the gateway
// otherwise.
func (c *h2combo) attempt(stock bool, k int) (h2outcome, error) {
	r := &h2run{tr: c.tr, k: k, lim: scaleLimits(c.sc.limits, k), probe: &probe{}, ret: map[string]chan struct{}{}}
	logs := &logBuffer{}
	h := r.wrap(c.sc.handler(r.probe))
	var stop func()
	var err error
	if stock {
		r.addr, stop, err = startStockH2(c.tr, r.lim, h, logs)
	} else {
		r.addr, stop, err = startGatewayH2(c.cfgs[k], h, logs)
	}
	if err != nil {
		return h2outcome{}, err
	}
	defer stop()
	defer r.close()
	o, err := c.sc.drive(r)
	if err == nil {
		o.Handler = r.probe.get()
	}
	return o, err
}

// run makes attempts in turn, each with stock and gateway at the same time, until one settles. An error that is not
// unsettled ends the combo.
func (c *h2combo) run() {
	for k := range h2MaxAttempts {
		var wg sync.WaitGroup
		var so, gw h2outcome
		var serr, gerr error
		wg.Go(func() { so, serr = c.attempt(true, k) })
		wg.Go(func() { gw, gerr = c.attempt(false, k) })
		wg.Wait()
		if serr != nil && !errors.Is(serr, errUnsettled) {
			c.err = fmt.Errorf("stock, attempt %d: %w", k, serr)
			return
		}
		if gerr != nil && !errors.Is(gerr, errUnsettled) {
			c.err = fmt.Errorf("gateway, attempt %d: %w", k, gerr)
			return
		}
		if serr != nil || gerr != nil {
			c.tries = append(c.tries, fmt.Sprintf("attempt %d: stock %v; gateway %v", k, serr, gerr))
			continue
		}
		c.stock, c.gateway, c.settled = so, gw, true
		return
	}
}

// compareH2 runs every combo off the test goroutine, so that their real-time waits overlap, and checks each combo in its
// own subtest. Only the test goroutine calls testing.T: it collects one result per combo under the hang guard.
func compareH2(t *testing.T, combos []*h2combo) {
	done := make(chan *h2combo, len(combos))
	for _, c := range combos {
		go func() {
			c.run()
			done <- c
		}()
	}
	finished := map[string]bool{}
	state := func() string {
		var pending []string
		for _, c := range combos {
			if !finished[c.name] {
				pending = append(pending, c.name)
			}
		}
		return "running: " + strings.Join(pending, ", ")
	}
	for range len(combos) {
		c, _ := testwait.Recv(t, "HTTP/2 comparisons", done, state)
		finished[c.name] = true
	}
	for _, c := range combos {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if c.err != nil {
				t.Fatal(c.err)
			}
			if !c.settled {
				t.Fatalf("no settled attempt in %d: %s", h2MaxAttempts, strings.Join(c.tries, "; "))
			}
			t.Logf("%s stock: %+v", c.name, c.stock)
			t.Logf("%s gateway: %+v", c.name, c.gateway)
			if len(c.tries) > 0 {
				t.Logf("%s unsettled attempts: %s", c.name, strings.Join(c.tries, "; "))
			}
			if c.sc.check != nil {
				c.sc.check(t, "stock", c.stock)
				c.sc.check(t, "gateway", c.gateway)
			}
			if !reflect.DeepEqual(c.stock, c.gateway) {
				t.Fatalf("outcomes differ: stock %+v, gateway %+v", c.stock, c.gateway)
			}
		})
	}
}

func TestHTTP2LimitOutcomesMatchNetHTTP(t *testing.T) {
	compareH2(t, h2Combos(t, h2Scenarios()))
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
