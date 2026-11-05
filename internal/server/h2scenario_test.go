package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ok0/go-microgateway/internal/memnet"
)

type h2record struct {
	Outcome h2outcome
	At      map[string]time.Duration
	Seq     []string
	Raw     string
	Log     bool
}

type h2bench struct {
	m       *memNet
	w       bubbleWire
	addr    string
	zero    time.Time
	lim     [4]time.Duration
	probe   *probe
	logs    *logBuffer
	mu      sync.Mutex
	ret     map[string]chan struct{}
	release chan struct{}
	marks   map[string]time.Duration
	peers   []*h2peer
	conns   []net.Conn
}

type holdConn struct {
	net.Conn
	mu     sync.Mutex
	open   chan struct{}
	closed chan struct{}
	once   sync.Once
}

const holdChunk = 16

// newHoldConn wraps c without holding it: reads pass through until hold is called.
func newHoldConn(c net.Conn) *holdConn {
	open := make(chan struct{})
	close(open)
	return &holdConn{Conn: c, open: open, closed: make(chan struct{})}
}

// hold makes reads wait until resume is called. While held, the server's writes stall once memnet's buffer is full.
func (c *holdConn) hold() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.open:
		c.open = make(chan struct{})
	default:
	}
}

// resume lets reads through again.
func (c *holdConn) resume() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.open:
	default:
		close(c.open)
	}
}

// Read takes at most holdChunk bytes per call, so a read already running when the hold begins takes at most that many.
func (c *holdConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	open := c.open
	c.mu.Unlock()
	select {
	case <-open:
	case <-c.closed:
		return 0, net.ErrClosed
	}
	return c.Conn.Read(p[:min(len(p), holdChunk)])
}

func (c *holdConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func pingToken(n uint64) [8]byte {
	var token [8]byte
	binary.BigEndian.PutUint64(token[:], n)
	return token
}

func pingAcked(tok [8]byte) func(h2frame) bool {
	return func(f h2frame) bool {
		return f.Type == h2Ping && f.Ack && f.Ping == tok
	}
}

// eventIndex returns the position of the first event in r.Seq with the given prefix, or -1.
func eventIndex(r h2record, prefix string) int {
	for i, ev := range r.Seq {
		if strings.HasPrefix(ev, prefix) {
			return i
		}
	}
	return -1
}

// wantOrder checks that every prefix occurs in r.Seq and that their first occurrences are in the given order.
func wantOrder(t *testing.T, r h2record, prefixes ...string) {
	t.Helper()
	last := -1
	for _, p := range prefixes {
		i := eventIndex(r, p)
		if i < 0 {
			t.Errorf("record has no event %q: %v", p, r.Seq)
			return
		}
		if i <= last {
			t.Errorf("event %q at position %d does not follow position %d: %v", p, i, last, r.Seq)
			return
		}
		last = i
	}
}

type h2case struct {
	name    string
	lim     [4]time.Duration
	tlsOnly bool
	handler func(b *h2bench) http.Handler
	drive   func(b *h2bench) (h2record, error)
	check   func(t *testing.T, w bubbleWire, r h2record)
}

type h2target func(t *testing.T, m *memNet, w bubbleWire, lim [4]time.Duration, h http.Handler, logs *logBuffer) string

func stockTarget(t *testing.T, m *memNet, w bubbleWire, lim [4]time.Duration, h http.Handler, logs *logBuffer) string {
	return startBubbleStock(t, m, w, lim, h, logs)
}

func (b *h2bench) retLocked(path string) chan struct{} {
	ch, ok := b.ret[path]
	if !ok {
		ch = make(chan struct{})
		b.ret[path] = ch
	}
	return ch
}

func (b *h2bench) returned(path string) <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.retLocked(path)
}

func (b *h2bench) wrap(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			ch := b.retLocked(req.URL.Path)
			select {
			case <-ch:
			default:
				close(ch)
			}
		}()
		h.ServeHTTP(w, req)
	})
}

func (b *h2bench) mark(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.marks[key]; !ok {
		b.marks[key] = time.Since(b.zero)
	}
}

func (b *h2bench) rawDial() (net.Conn, error) {
	conn, err := b.m.dial(context.Background(), "tcp", b.addr)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.conns = append(b.conns, conn)
	b.mu.Unlock()
	return conn, nil
}

func (b *h2bench) open(conn net.Conn) (*h2peer, error) {
	p, err := openH2Conn(conn, b.w.pool)
	if err != nil {
		return nil, err
	}
	p.bubble = true
	b.mu.Lock()
	b.peers = append(b.peers, p)
	b.mu.Unlock()
	return p, nil
}

func (b *h2bench) dial(rcvBuf int) (*h2peer, error) {
	conn, err := b.rawDial()
	if err != nil {
		return nil, err
	}
	if rcvBuf > 0 {
		if err := conn.(*memnet.Conn).SetReadBuffer(rcvBuf); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return b.open(conn)
}

func (b *h2bench) close() {
	b.mu.Lock()
	peers := b.peers
	conns := b.conns
	b.mu.Unlock()
	for _, p := range peers {
		p.close()
	}
	for _, conn := range conns {
		conn.Close()
	}
}

func (b *h2bench) run(c h2case) (h2record, error) {
	b.zero = time.Now()
	guard := time.AfterFunc(bubbleGuard, b.close)
	r, err := c.drive(b)
	fired := !guard.Stop()
	b.close()
	if fired {
		return h2record{}, fmt.Errorf("hang guard: case %s still running after %v of bubble time\ngoroutines:\n%s", c.name, bubbleGuard, bubbleStacks())
	}
	return r, err
}

func prepareCase(t *testing.T, w bubbleWire, c h2case, target h2target) *h2bench {
	t.Helper()
	m := newMemNet(64 << 10)
	t.Cleanup(m.close)
	b := &h2bench{
		m:       m,
		w:       w,
		lim:     c.lim,
		probe:   &probe{},
		logs:    &logBuffer{},
		ret:     map[string]chan struct{}{},
		release: make(chan struct{}),
		marks:   map[string]time.Duration{},
	}
	b.addr = target(t, m, w, c.lim, b.wrap(c.handler(b)), b.logs)
	return b
}

func handshakeWith(c *h2peer, ackPings bool, gate uint32, settings ...h2Setting) (h2frame, error) {
	if err := c.writePreface(settings...); err != nil {
		return h2frame{}, fmt.Errorf("preface: %w", err)
	}
	c.start(ackPings, gate)
	fs, ev := c.await(nil, func(f h2frame) bool { return f.Type == h2Settings && !f.Ack })
	if ev != evFrame {
		return h2frame{}, fmt.Errorf("connection ended before the server SETTINGS: %v", c.endErr())
	}
	return fs[len(fs)-1], nil
}

func (b *h2bench) record(c *h2peer, fs []h2frame) h2record {
	r := h2record{Outcome: finish(c, fs), At: map[string]time.Duration{}}
	r.Outcome.Handler = b.probe.get()
	b.mu.Lock()
	for k, d := range b.marks {
		r.At[k] = d
	}
	b.mu.Unlock()
	at := func(key string, t time.Time) {
		r.Seq = append(r.Seq, key)
		if _, ok := r.At[key]; !ok {
			r.At[key] = t.Sub(b.zero)
		}
	}
	for _, f := range fs {
		switch f.Type {
		case h2Headers:
			ev := "HEADERS " + statusOf(f)
			if f.EndStream {
				ev += " ES"
			}
			at(fmt.Sprintf("%d %s", f.Stream, ev), f.At)
		case h2Data:
			at(fmt.Sprintf("%d DATA", f.Stream), f.At)
			if f.EndStream {
				at(fmt.Sprintf("%d DATA ES", f.Stream), f.At)
			}
		case h2RSTStream:
			at(fmt.Sprintf("%d RST_STREAM %v", f.Stream, f.Code), f.At)
		case h2GoAway:
			at(fmt.Sprintf("GOAWAY %v last=%d", f.Code, f.LastStream), f.At)
		case h2Ping:
			if !f.Ack {
				at("PING", f.At)
			} else {
				r.Seq = append(r.Seq, fmt.Sprintf("PING ACK %d", binary.BigEndian.Uint64(f.Ping[:])))
			}
		}
	}
	c.mu.Lock()
	end := c.endAt
	c.mu.Unlock()
	if !end.IsZero() {
		r.At["end"] = end.Sub(b.zero)
	}
	return r
}

func (b *h2bench) endRecord(c *h2peer, fs []h2frame) h2record {
	<-c.done
	return b.record(c, append(fs, drain(c)...))
}

func (b *h2bench) awaitReturn(c *h2peer, path string) ([]h2frame, error) {
	fs, ev := c.await(b.returned(path), nil)
	if ev != evSignal {
		return fs, fmt.Errorf("connection ended before the %s handler returned", path)
	}
	return fs, nil
}

func (b *h2bench) getTail(c *h2peer, path string) ([]h2frame, error) {
	b.mark("sent 1")
	if err := c.writeHeaders(1, http.MethodGet, path, true); err != nil {
		return nil, fmt.Errorf("HEADERS: %w", err)
	}
	fs, ev := c.await(nil, dataOrEnd(1, 65535))
	if ev == evEnd {
		return nil, fmt.Errorf("connection ended after %d DATA bytes on stream 1", dataBytes(fs, 1))
	}
	if n := dataBytes(fs, 1); n < 65535 {
		return nil, fmt.Errorf("stream 1 ended after %d of 65535 DATA bytes", n)
	}
	return fs, nil
}

func awaitEnds(c *h2peer, prior []h2frame, ids ...uint32) ([]h2frame, error) {
	seen := map[uint32]bool{}
	note := func(f h2frame) bool {
		for _, id := range ids {
			if streamEnded(id)(f) {
				seen[id] = true
			}
		}
		return len(seen) == len(ids)
	}
	for _, f := range prior {
		note(f)
	}
	if len(seen) == len(ids) {
		return nil, nil
	}
	fs, ev := c.await(nil, note)
	if ev != evFrame {
		return fs, fmt.Errorf("connection ended before streams %v ended", ids)
	}
	return fs, nil
}

func (b *h2bench) drainRaw(conn net.Conn, send string) (h2record, error) {
	if send != "" {
		if _, err := io.WriteString(conn, send); err != nil {
			return h2record{}, err
		}
	}
	data, err := io.ReadAll(conn)
	synctest.Wait()
	r := h2record{
		Raw: string(data),
		Log: strings.Contains(b.logs.String(), "http: TLS handshake error"),
		At:  map[string]time.Duration{"end": time.Since(b.zero)},
	}
	r.Outcome.End = "eof"
	if err != nil {
		r.Outcome.End = endKind(err)
	}
	return r, nil
}

func scOK(*h2bench) http.Handler {
	return okHandler()
}

func scWriteHandler(b *h2bench) http.Handler {
	big := h2BigProbe(b.probe)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/small" {
			io.WriteString(w, "ok")
			return
		}
		big.ServeHTTP(w, r)
	})
}

func scCommittedHandler(b *h2bench) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(make([]byte, 3<<10))
		b.probe.set("returned")
	})
}

func scBlockedHandler(b *h2bench) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/a" {
			http.NewResponseController(w).SetWriteDeadline(time.Time{})
			if _, err := w.Write(make([]byte, 1<<20)); err != nil {
				b.probe.set("a-write-error")
				return
			}
			b.probe.set("a-complete")
			return
		}
		w.Header().Set("Trailer", "X-T")
		io.WriteString(w, "b")
		w.Header().Set("X-T", "t")
		b.probe.set("b-returned")
	})
}

func scActiveHandler(b *h2bench) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-b.release:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, "ok")
	})
}

func scSettings(b *h2bench) (h2record, error) {
	c, err := b.dial(0)
	if err != nil {
		return h2record{}, err
	}
	s, err := handshakeWith(c, true, 0)
	if err != nil {
		return h2record{}, err
	}
	wu, ev := c.await(nil, connWindowUpdate)
	if ev != evFrame {
		return h2record{}, fmt.Errorf("connection ended before the connection WINDOW_UPDATE: %v", c.endErr())
	}
	r := b.record(c, append([]h2frame{s}, wu...))
	r.Outcome.Settings = settingsLine(s.Settings, wu[len(wu)-1].Incr)
	return r, nil
}

func scRead(b *h2bench) (h2record, error) {
	c, err := b.dial(0)
	if err != nil {
		return h2record{}, err
	}
	if _, err := handshakeWith(c, true, 0); err != nil {
		return h2record{}, err
	}
	b.mark("sent 1")
	if err := c.writeHeaders(1, http.MethodPost, "/", false); err != nil {
		return h2record{}, fmt.Errorf("HEADERS: %w", err)
	}
	if err := c.writeData(1, []byte("0123456789"), false); err != nil {
		return h2record{}, fmt.Errorf("DATA: %w", err)
	}
	fs, ev := c.await(nil, streamRST(1))
	if ev != evFrame {
		return h2record{}, fmt.Errorf("connection ended before stream 1 was reset")
	}
	more, err := b.awaitReturn(c, "/")
	fs = append(fs, more...)
	if err != nil {
		return h2record{}, err
	}
	return b.record(c, fs), nil
}

func scWrite(b *h2bench) (h2record, error) {
	c, err := b.dial(0)
	if err != nil {
		return h2record{}, err
	}
	if _, err := handshakeWith(c, true, 0); err != nil {
		return h2record{}, err
	}
	fs, err := b.getTail(c, "/big")
	if err != nil {
		return h2record{}, err
	}
	if err := c.writeWindowUpdate(0, 1<<20); err != nil {
		return h2record{}, fmt.Errorf("WINDOW_UPDATE: %w", err)
	}
	if err := c.writeHeaders(3, http.MethodGet, "/small", true); err != nil {
		return h2record{}, fmt.Errorf("HEADERS: %w", err)
	}
	more, err := awaitEnds(c, fs, 1, 3)
	fs = append(fs, more...)
	if err != nil {
		return h2record{}, err
	}
	return b.record(c, fs), nil
}

func scTail(b *h2bench) (h2record, error) {
	c, err := b.dial(0)
	if err != nil {
		return h2record{}, err
	}
	if _, err := handshakeWith(c, true, 0); err != nil {
		return h2record{}, err
	}
	fs, err := b.getTail(c, "/")
	if err != nil {
		return h2record{}, err
	}
	if streamEnded(1)(fs[len(fs)-1]) {
		return h2record{}, fmt.Errorf("stream 1 ended before its handler returned")
	}
	more, ev := c.await(b.returned("/"), streamEnded(1))
	fs = append(fs, more...)
	switch ev {
	case evEnd:
		return h2record{}, fmt.Errorf("connection ended before the handler returned")
	case evFrame:
		return h2record{}, fmt.Errorf("stream 1 ended before its handler returned")
	}
	more, ev = c.await(nil, streamEnded(1))
	fs = append(fs, more...)
	if ev == evEnd {
		return h2record{}, fmt.Errorf("connection ended before stream 1 ended")
	}
	return b.record(c, fs), nil
}

func scTailCommitted(b *h2bench) (h2record, error) {
	conn, err := b.rawDial()
	if err != nil {
		return h2record{}, err
	}
	if err := conn.(*memnet.Conn).SetReadBuffer(256); err != nil {
		conn.Close()
		return h2record{}, err
	}
	hc := newHoldConn(conn)
	c, err := b.open(hc)
	if err != nil {
		return h2record{}, err
	}
	if _, err := handshakeWith(c, true, 0); err != nil {
		return h2record{}, err
	}
	if err := c.writePing(pingToken(1)); err != nil {
		return h2record{}, fmt.Errorf("PING: %w", err)
	}
	fs, ev := c.await(nil, pingAcked(pingToken(1)))
	if ev != evFrame {
		return h2record{}, fmt.Errorf("connection ended before the first PING ACK: %v", c.endErr())
	}
	hc.hold()
	t1 := time.Now()
	b.mark("sent 1")
	if err := c.writeHeaders(1, http.MethodGet, "/", true); err != nil {
		return h2record{}, fmt.Errorf("HEADERS: %w", err)
	}
	more, ev := c.await(b.returned("/"), nil)
	fs = append(fs, more...)
	if ev != evSignal {
		return h2record{}, fmt.Errorf("connection ended before the handler returned")
	}
	time.Sleep(time.Until(t1.Add(b.lim[2] + time.Nanosecond)))
	if err := c.writePing(pingToken(2)); err != nil {
		return h2record{}, fmt.Errorf("PING: %w", err)
	}
	b.mark("resumed")
	hc.resume()
	ack2 := pingAcked(pingToken(2))
	endSeen, ackSeen := false, false
	more, ev = c.await(nil, func(f h2frame) bool {
		if streamEnded(1)(f) {
			endSeen = true
		}
		if ack2(f) {
			ackSeen = true
		}
		return endSeen && ackSeen
	})
	fs = append(fs, more...)
	if ev != evFrame {
		return h2record{}, fmt.Errorf("connection ended before stream 1 ended and the PING ACK")
	}
	return b.record(c, fs), nil
}

func scBlocked(b *h2bench) (h2record, error) {
	c, err := b.dial(4096)
	if err != nil {
		return h2record{}, err
	}
	if _, err := handshakeWith(c, true, 1,
		h2Setting{ID: h2SettingInitialWindowSize, Val: 1<<31 - 1},
		h2Setting{ID: h2SettingMaxFrameSize, Val: 1<<24 - 1},
	); err != nil {
		return h2record{}, err
	}
	if err := c.writeWindowUpdate(0, 1<<31-1-65535); err != nil {
		return h2record{}, fmt.Errorf("WINDOW_UPDATE: %w", err)
	}
	if err := c.writeHeaders(1, http.MethodGet, "/a", true); err != nil {
		return h2record{}, fmt.Errorf("HEADERS: %w", err)
	}
	fs, ev := c.await(c.gated, streamEnded(1))
	if ev != evSignal {
		return h2record{}, fmt.Errorf("stream 1 or the connection ended before its DATA write")
	}
	b.mark("sent 3")
	t3 := time.Now()
	if err := c.writeHeaders(3, http.MethodGet, "/b", true); err != nil {
		return h2record{}, fmt.Errorf("HEADERS: %w", err)
	}
	more, ev := c.await(b.returned("/b"), nil)
	fs = append(fs, more...)
	if ev != evSignal {
		return h2record{}, fmt.Errorf("connection ended before the /b handler returned")
	}
	time.Sleep(time.Until(t3.Add(b.lim[2] - time.Nanosecond)))
	if err := c.writePing(pingToken(1)); err != nil {
		return h2record{}, fmt.Errorf("PING: %w", err)
	}
	time.Sleep(time.Until(t3.Add(b.lim[2] + time.Nanosecond)))
	if err := c.writePing(pingToken(2)); err != nil {
		return h2record{}, fmt.Errorf("PING: %w", err)
	}
	close(c.release)
	endSeen, ackSeen := false, false
	ack2 := pingAcked(pingToken(2))
	more, ev = c.await(nil, func(f h2frame) bool {
		if streamEnded(1)(f) {
			endSeen = true
		}
		if ack2(f) {
			ackSeen = true
		}
		return endSeen && ackSeen
	})
	fs = append(fs, more...)
	if ev == evEnd {
		return h2record{}, fmt.Errorf("connection ended before stream 1 ended and the PING ACK")
	}
	more, err = b.awaitReturn(c, "/a")
	fs = append(fs, more...)
	if err != nil {
		return h2record{}, err
	}
	return b.record(c, fs), nil
}

func scIdle(b *h2bench) (h2record, error) {
	c, err := b.dial(0)
	if err != nil {
		return h2record{}, err
	}
	if _, err := handshakeWith(c, true, 0); err != nil {
		return h2record{}, err
	}
	fs, _ := c.await(nil, goAwayFrame)
	return b.endRecord(c, fs), nil
}

func scIdleAfterStream(b *h2bench) (h2record, error) {
	c, err := b.dial(0)
	if err != nil {
		return h2record{}, err
	}
	if _, err := handshakeWith(c, true, 0); err != nil {
		return h2record{}, err
	}
	if err := c.writeHeaders(1, http.MethodGet, "/", true); err != nil {
		return h2record{}, fmt.Errorf("HEADERS: %w", err)
	}
	fs, ev := c.await(nil, func(f h2frame) bool { return streamEnded(1)(f) || goAwayFrame(f) })
	if ev == evEnd || goAwayFrame(fs[len(fs)-1]) {
		return h2record{}, fmt.Errorf("idle limit expired before stream 1 was served")
	}
	more, _ := c.await(nil, goAwayFrame)
	fs = append(fs, more...)
	return b.endRecord(c, fs), nil
}

func scIdleActive(b *h2bench) (h2record, error) {
	c, err := b.dial(0)
	if err != nil {
		return h2record{}, err
	}
	if _, err := handshakeWith(c, true, 0); err != nil {
		return h2record{}, err
	}
	if err := c.writeHeaders(1, http.MethodGet, "/", true); err != nil {
		return h2record{}, fmt.Errorf("HEADERS: %w", err)
	}
	time.Sleep(3 * time.Second)
	fs := drain(c)
	b.mark("released")
	close(b.release)
	more, _ := c.await(nil, streamEnded(1))
	fs = append(fs, more...)
	more, _ = c.await(nil, goAwayFrame)
	fs = append(fs, more...)
	return b.endRecord(c, fs), nil
}

func scIdleNoPing(ackPings bool) func(b *h2bench) (h2record, error) {
	return func(b *h2bench) (h2record, error) {
		c, err := b.dial(0)
		if err != nil {
			return h2record{}, err
		}
		if _, err := handshakeWith(c, ackPings, 0); err != nil {
			return h2record{}, err
		}
		fs, _ := c.await(nil, goAwayFrame)
		<-c.done
		fs = append(fs, drain(c)...)
		r := b.record(c, fs)
		r.Outcome.Pinged = slices.ContainsFunc(fs, pingFrame)
		return r, nil
	}
}

func scPreface(b *h2bench) (h2record, error) {
	conn, err := b.rawDial()
	if err != nil {
		return h2record{}, err
	}
	c, err := b.open(conn)
	if err != nil {
		return h2record{}, err
	}
	c.start(false, 0)
	fs, _ := c.await(nil, nil)
	return b.endRecord(c, fs), nil
}

func scFirstSettings(b *h2bench) (h2record, error) {
	conn, err := b.rawDial()
	if err != nil {
		return h2record{}, err
	}
	c, err := b.open(conn)
	if err != nil {
		return h2record{}, err
	}
	c.holdSettingsAck = true
	if err := c.send(func() error {
		_, err := c.conn.Write([]byte(h2Preface))
		return err
	}); err != nil {
		return h2record{}, fmt.Errorf("preface: %w", err)
	}
	c.start(true, 0)
	fs, _ := c.await(nil, nil)
	return b.endRecord(c, fs), nil
}

func scTLSHandshakeError(b *h2bench) (h2record, error) {
	conn, err := b.rawDial()
	if err != nil {
		return h2record{}, err
	}
	defer conn.Close()
	return b.drainRaw(conn, "\x16\x03\x01\x00\x05hello")
}

func scTLSPlaintext(b *h2bench) (h2record, error) {
	conn, err := b.rawDial()
	if err != nil {
		return h2record{}, err
	}
	defer conn.Close()
	return b.drainRaw(conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
}

func scTLSStall(b *h2bench) (h2record, error) {
	conn, err := b.rawDial()
	if err != nil {
		return h2record{}, err
	}
	defer conn.Close()
	return b.drainRaw(conn, "")
}

func atPrefix(r h2record, prefix string) (time.Duration, bool) {
	var best time.Duration
	found := false
	for k, d := range r.At {
		if strings.HasPrefix(k, prefix) && (!found || d < best) {
			best, found = d, true
		}
	}
	return best, found
}

func wantAt(t *testing.T, r h2record, prefix, origin string, d time.Duration) {
	t.Helper()
	var base time.Duration
	if origin != "" {
		o, ok := r.At[origin]
		if !ok {
			t.Errorf("record has no %q mark: %v", origin, r.At)
			return
		}
		base = o
	}
	got, ok := atPrefix(r, prefix)
	if !ok {
		t.Errorf("record has no event %q: %v", prefix, r.At)
		return
	}
	if got != base+d {
		t.Errorf("%q at %v, want %v", prefix, got, base+d)
	}
}

func scEnded(r h2record, id uint32) bool {
	for _, ev := range r.Outcome.Streams[id] {
		if strings.HasSuffix(ev, " ES") || strings.HasPrefix(ev, "RST_STREAM ") {
			return true
		}
	}
	return false
}

func h2Cases() []h2case {
	return []h2case{
		{
			name:    "settings",
			lim:     [4]time.Duration{time.Hour, 0, 0, time.Hour},
			handler: scOK,
			drive:   scSettings,
			check: func(t *testing.T, w bubbleWire, r h2record) {
				if r.Outcome.Settings == "" {
					t.Error("no server SETTINGS line in the record")
				}
			},
		},
		{
			name:    "read",
			lim:     [4]time.Duration{time.Hour, time.Second, 0, time.Hour},
			handler: func(b *h2bench) http.Handler { return readAllProbe(b.probe) },
			drive:   scRead,
			check: func(t *testing.T, w bubbleWire, r h2record) {
				if r.Outcome.Handler != "read-deadline" {
					t.Errorf("handler outcome %q, want read-deadline", r.Outcome.Handler)
				}
				wantAt(t, r, "1 HEADERS 200 ES", "sent 1", time.Second)
				wantAt(t, r, "1 RST_STREAM NO_ERROR", "sent 1", time.Second)
				wantOrder(t, r, "1 HEADERS 200 ES", "1 RST_STREAM NO_ERROR")
			},
		},
		{
			name:    "write",
			lim:     [4]time.Duration{time.Hour, 0, time.Second, time.Hour},
			handler: scWriteHandler,
			drive:   scWrite,
			check: func(t *testing.T, w bubbleWire, r h2record) {
				if !slices.Contains(r.Outcome.Streams[3], "HEADERS 200") || !slices.Contains(r.Outcome.Streams[3], "DATA ES") {
					t.Errorf("stream 3 events %v, want HEADERS 200 and DATA ES", r.Outcome.Streams[3])
				}
				if r.Outcome.Data[1] != 65535 {
					t.Errorf("stream 1 carried %d DATA bytes, want 65535", r.Outcome.Data[1])
				}
				wantAt(t, r, "1 RST_STREAM ", "sent 1", time.Second)
			},
		},
		{
			name:    "write-tail-uncommitted",
			lim:     [4]time.Duration{time.Hour, 0, time.Second, time.Hour},
			handler: func(b *h2bench) http.Handler { return h2TailProbe(b.probe) },
			drive:   scTail,
			check: func(t *testing.T, w bubbleWire, r h2record) {
				if r.Outcome.Handler != "returned" {
					t.Errorf("handler outcome %q, want returned", r.Outcome.Handler)
				}
				wantAt(t, r, "1 RST_STREAM ", "sent 1", time.Second)
			},
		},
		{
			name:    "write-tail-committed",
			lim:     [4]time.Duration{time.Hour, 0, time.Second, time.Hour},
			handler: scCommittedHandler,
			drive:   scTailCommitted,
			check: func(t *testing.T, w bubbleWire, r h2record) {
				if r.Outcome.Handler != "returned" {
					t.Errorf("handler outcome %q, want returned", r.Outcome.Handler)
				}
				if r.Outcome.Data[1] != 3<<10 {
					t.Errorf("stream 1 carried %d DATA bytes, want %d", r.Outcome.Data[1], 3<<10)
				}
				if _, ok := atPrefix(r, "1 RST_STREAM "); ok {
					t.Error("stream 1 was reset")
				}
				wantAt(t, r, "1 DATA ES", "resumed", 0)
				wantOrder(t, r, "1 HEADERS 200", "1 DATA ES", "PING ACK 2")
			},
		},
		{
			name:    "write-tail-blocked",
			lim:     [4]time.Duration{time.Hour, 0, time.Second, time.Hour},
			handler: scBlockedHandler,
			drive:   scBlocked,
			check: func(t *testing.T, w bubbleWire, r h2record) {
				if r.Outcome.Data[1] != 1<<20 {
					t.Errorf("stream 1 carried %d DATA bytes, want %d", r.Outcome.Data[1], 1<<20)
				}
				if _, ok := atPrefix(r, "1 RST_STREAM "); ok {
					t.Error("stream 1 was reset")
				}
				for _, ev := range r.Outcome.Streams[3] {
					if strings.HasSuffix(ev, " ES") {
						t.Errorf("stream 3 event %q, want no END_STREAM", ev)
					}
				}
				wantOrder(t, r, "PING ACK 1", "3 RST_STREAM ", "PING ACK 2")
			},
		},
		{
			name:    "idle",
			lim:     [4]time.Duration{time.Hour, 0, 0, time.Second},
			handler: scOK,
			drive:   scIdle,
			check: func(t *testing.T, w bubbleWire, r h2record) {
				wantAt(t, r, "GOAWAY NO_ERROR last=0", "", time.Second)
				wantAt(t, r, "end", "", 2*time.Second)
			},
		},
		{
			name:    "idle-from-read",
			lim:     [4]time.Duration{time.Hour, time.Second, 0, 0},
			handler: scOK,
			drive:   scIdle,
			check: func(t *testing.T, w bubbleWire, r h2record) {
				wantAt(t, r, "GOAWAY NO_ERROR last=0", "", time.Second)
				wantAt(t, r, "end", "", 2*time.Second)
			},
		},
		{
			name:    "idle-after-stream",
			lim:     [4]time.Duration{time.Hour, 0, 0, time.Second},
			handler: scOK,
			drive:   scIdleAfterStream,
			check: func(t *testing.T, w bubbleWire, r h2record) {
				wantAt(t, r, "GOAWAY NO_ERROR last=1", "1 DATA ES", time.Second)
			},
		},
		{
			name:    "idle-with-active-stream",
			lim:     [4]time.Duration{time.Hour, 0, 0, time.Second},
			handler: scActiveHandler,
			drive:   scIdleActive,
			check: func(t *testing.T, w bubbleWire, r h2record) {
				wantAt(t, r, "GOAWAY NO_ERROR last=1", "released", time.Second)
				if at, ok := atPrefix(r, "GOAWAY "); ok && at < r.At["released"] {
					t.Errorf("GOAWAY at %v, before released at %v", at, r.At["released"])
				}
			},
		},
		{
			name:    "send-ping",
			lim:     [4]time.Duration{time.Hour, time.Second, 0, 3 * time.Second},
			handler: scOK,
			drive:   scIdleNoPing(true),
			check: func(t *testing.T, w bubbleWire, r h2record) {
				if r.Outcome.Pinged {
					t.Error("server sent a PING while idle under a read limit")
				}
				wantAt(t, r, "GOAWAY ", "", 3*time.Second)
			},
		},
		{
			name:    "ping-ack",
			lim:     [4]time.Duration{time.Hour, time.Second, 0, 16 * time.Second},
			handler: scOK,
			drive:   scIdleNoPing(false),
			check: func(t *testing.T, w bubbleWire, r h2record) {
				if r.Outcome.Pinged {
					t.Error("server sent a PING while idle under a read limit")
				}
				wantAt(t, r, "GOAWAY ", "", 16*time.Second)
			},
		},
		{
			name:    "preface",
			lim:     [4]time.Duration{time.Hour, 0, 0, time.Hour},
			handler: scOK,
			drive:   scPreface,
			check: func(t *testing.T, w bubbleWire, r h2record) {
				d := time.Hour
				if w.tls {
					d = 10 * time.Second
				}
				wantAt(t, r, "end", "", d)
			},
		},
		{
			name:    "first-settings",
			lim:     [4]time.Duration{time.Hour, 0, 0, time.Hour},
			handler: scOK,
			drive:   scFirstSettings,
			check: func(t *testing.T, w bubbleWire, r h2record) {
				wantAt(t, r, "end", "", 2*time.Second)
			},
		},
		{
			name:    "tls-handshake-error",
			lim:     [4]time.Duration{time.Hour, time.Hour, time.Hour, time.Hour},
			tlsOnly: true,
			handler: scOK,
			drive:   scTLSHandshakeError,
			check: func(t *testing.T, w bubbleWire, r h2record) {
				if len(r.Raw) != 7 || r.Raw[0] != 0x15 || r.Raw[5] != 2 {
					t.Errorf("server wrote %q, want one fatal TLS alert record", r.Raw)
				}
				if !r.Log {
					t.Error("no TLS handshake error was logged")
				}
			},
		},
		{
			name:    "tls-plaintext",
			lim:     [4]time.Duration{time.Hour, time.Hour, time.Hour, time.Hour},
			tlsOnly: true,
			handler: scOK,
			drive:   scTLSPlaintext,
			check: func(t *testing.T, w bubbleWire, r h2record) {
				if !strings.HasPrefix(r.Raw, "HTTP/1.0 400 Bad Request") {
					t.Errorf("server wrote %q, want a 400 response", r.Raw)
				}
				if !r.Log {
					t.Error("no TLS handshake error was logged")
				}
			},
		},
		{
			name:    "tls-handshake-stall",
			lim:     [4]time.Duration{time.Second, time.Hour, time.Hour, time.Hour},
			tlsOnly: true,
			handler: scOK,
			drive:   scTLSStall,
			check: func(t *testing.T, w bubbleWire, r h2record) {
				wantAt(t, r, "end", "", time.Second)
			},
		},
	}
}

func TestHTTP2LimitScenariosOnNetHTTP(t *testing.T) {
	for _, w := range bubbleWires(t) {
		for _, c := range h2Cases() {
			if c.tlsOnly && !w.tls {
				continue
			}
			t.Run(c.name+"/"+w.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					b := prepareCase(t, w, c, stockTarget)
					r, err := b.run(c)
					if err != nil {
						t.Fatal(err)
					}
					t.Logf("%+v", r)
					c.check(t, w, r)
				})
			})
		}
	}
}
