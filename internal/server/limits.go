package server

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
)

const (
	h2Preface   = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	h2PeekShort = "PRI * HTTP/2.0"
)

// past returns a deadline that has always passed; setting it makes a blocked read or write fail at once.
func past() time.Time { return time.Unix(1, 0) }

type connLimits struct {
	header, read, write, idle time.Duration
	tls, h2, h2c              bool
}

func (l connLimits) headerLimit() time.Duration {
	if l.header != 0 {
		return l.header
	}
	return l.read
}

func (l connLimits) idleLimit() time.Duration {
	if l.idle != 0 {
		return l.idle
	}
	return l.read
}

func (l connLimits) handshakeLimit() time.Duration {
	var d time.Duration
	for _, v := range []time.Duration{l.header, l.read, l.write} {
		if v > 0 && (d == 0 || v < d) {
			d = v
		}
	}
	return d
}

type limitListener struct {
	net.Listener
	lim       connLimits
	clk       clock.Clock
	idleClk   clock.Clock
	tlsConfig *tls.Config
}

func (l *limitListener) Accept() (net.Conn, error) {
	raw, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	c := &limitConn{Conn: raw, ln: l, startSet: true}
	if l.lim.h2c {
		c.capture.Store(true)
		c.startSet = false
	}
	if l.tlsConfig == nil {
		return c, nil
	}
	tc := tls.Server(c, l.tlsConfig)
	c.tc, c.hsDone = tc, make(chan struct{})
	return tc, nil
}

type phase uint8

const (
	phaseNew phase = iota
	phaseHandshake
	phaseHeader
	phaseRequest
	phaseIdle
	phaseH2
	phaseHijacked
	phaseClosed
)

type dir uint8

const (
	dirRead dir = iota
	dirWrite
	dirBoth
)

type limitTimer struct {
	gen   uint64
	t     clock.Timer
	fired bool
}

// limitConn wraps an accepted raw conn beneath TLS. It enforces the HTTP/1 read-header, read, write, and idle limits
// and the TLS handshake limit on the server clock. After HTTP/2 entry every deadline call reaches the raw conn
// unchanged.
type limitConn struct {
	net.Conn
	ln       *limitListener
	capture  atomic.Bool
	mu       sync.Mutex
	phase    phase
	idleSets int
	start    time.Time
	startSet bool
	wArmed   bool
	rd, wr   limitTimer
	preface  [len(h2Preface)]byte
	npre     int
	tc       *tls.Conn
	hsDone   chan struct{}
	reading  atomic.Int32
	raw      [2]time.Time
	brR, brW uint8
	hdr0     bool
	wDefer   bool
}

func isPast(t time.Time) bool { return !t.IsZero() && !t.After(past()) }

func (c *limitConn) recordRawLocked(which dir, t time.Time) {
	if which != dirWrite {
		c.raw[dirRead] = t
	}
	if which != dirRead {
		c.raw[dirWrite] = t
	}
}

func (c *limitConn) rawSetLocked(which dir, t time.Time) {
	readDiff := which != dirWrite && !c.raw[dirRead].Equal(t)
	writeDiff := which != dirRead && !c.raw[dirWrite].Equal(t)
	switch {
	case readDiff && writeDiff:
		c.Conn.SetDeadline(t)
	case readDiff:
		c.Conn.SetReadDeadline(t)
	case writeDiff:
		c.Conn.SetWriteDeadline(t)
	}
	c.recordRawLocked(which, t)
}

func (c *limitConn) forwardLocked(which dir, t time.Time) error {
	var err error
	switch which {
	case dirRead:
		err = c.Conn.SetReadDeadline(t)
	case dirWrite:
		err = c.Conn.SetWriteDeadline(t)
	case dirBoth:
		err = c.Conn.SetDeadline(t)
	}
	c.recordRawLocked(which, t)
	return err
}

func stopLocked(s *limitTimer) {
	if s.t != nil {
		s.t.Stop()
		s.t = nil
	}
	s.gen++
	s.fired = false
}

func (c *limitConn) armLocked(s *limitTimer, clk clock.Clock, d time.Duration, which dir) {
	stopLocked(s)
	g := s.gen
	if d <= 0 {
		c.expireLocked(s, which)
		return
	}
	s.t = clk.AfterFunc(d, func() { c.fire(s, which, g) })
}

func (c *limitConn) expireLocked(s *limitTimer, which dir) {
	c.rawSetLocked(which, past())
	s.t = nil
	s.fired = true
}

func (c *limitConn) beginHeaderLocked() {
	c.phase = phaseHeader
	c.wArmed = false
	c.wDefer = c.ln.lim.write > 0
	if !c.capture.Load() {
		c.start = c.ln.clk.Now()
		c.startSet = true
	}
	if d := c.ln.lim.headerLimit(); d > 0 {
		c.armLocked(&c.rd, c.ln.clk, d, dirRead)
	} else {
		stopLocked(&c.rd)
	}
}

func (c *limitConn) stopAllLocked() {
	stopLocked(&c.rd)
	stopLocked(&c.wr)
	c.capture.Store(false)
}

func (c *limitConn) enterH2Locked() {
	c.stopAllLocked()
	c.phase = phaseH2
	c.brR, c.brW, c.hdr0, c.wDefer = 0, 0, false, false
}

func (c *limitConn) fire(s *limitTimer, which dir, g uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase >= phaseHijacked || s.gen != g {
		return
	}
	c.expireLocked(s, which)
}

// awaitHandshake waits until the handshake goroutine has recorded the end of the TLS handshake. Events after
// StateNew depend on that record, and crypto/tls never sets deadlines on this conn during a handshake.
func (c *limitConn) awaitHandshake() {
	if c.hsDone != nil {
		<-c.hsDone
	}
}

// handshake runs the server side of the TLS handshake and records its end. net/http's own HandshakeContext call
// waits on crypto/tls's handshake lock and returns the same stored result.
func (c *limitConn) handshake() {
	err := c.tc.HandshakeContext(context.Background())
	c.handshakeDone(err == nil, c.tc.ConnectionState().NegotiatedProtocol)
	close(c.hsDone)
}

func (c *limitConn) state(st http.ConnState) {
	if st != http.StateNew {
		c.awaitHandshake()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	lim := c.ln.lim
	switch st {
	case http.StateNew:
		if c.phase != phaseNew {
			return
		}
		if lim.tls {
			c.phase = phaseHandshake
			if d := lim.handshakeLimit(); d > 0 {
				c.armLocked(&c.rd, c.ln.clk, d, dirBoth)
				c.brR, c.brW = 2, 2
			}
			if c.tc != nil {
				go c.handshake()
			}
		} else {
			c.beginHeaderLocked()
			c.hdr0 = lim.headerLimit() > 0
		}
	case http.StateActive:
		if (c.phase == phaseHeader || c.phase == phaseRequest) && lim.write > 0 && !c.wArmed {
			c.wArmed = true
			stopLocked(&c.wr)
			c.rawSetLocked(dirWrite, time.Time{})
			c.armLocked(&c.wr, c.ln.clk, lim.write, dirWrite)
		}
	case http.StateIdle:
		if c.phase == phaseRequest {
			c.phase = phaseIdle
			c.idleSets = 0
		}
	case http.StateHijacked:
		c.phase = phaseHijacked
		c.stopAllLocked()
	case http.StateClosed:
		c.phase = phaseClosed
		c.stopAllLocked()
	}
}

func (c *limitConn) handshakeDone(ok bool, proto string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase != phaseHandshake {
		return
	}
	stopLocked(&c.rd)
	if !ok {
		return
	}
	if proto == "h2" && c.ln.lim.h2 {
		c.enterH2Locked()
	} else {
		c.beginHeaderLocked()
		c.hdr0 = c.ln.lim.headerLimit() > 0
	}
}

func (c *limitConn) SetDeadline(t time.Time) error {
	c.awaitHandshake()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase >= phaseH2 {
		return c.forwardLocked(dirBoth, t)
	}
	switch c.phase {
	case phaseHeader, phaseRequest, phaseIdle:
		stopLocked(&c.rd)
		stopLocked(&c.wr)
	}
	switch {
	case t.IsZero():
		c.rawSetLocked(dirBoth, t)
	case isPast(t):
		c.rawSetLocked(dirBoth, past())
	case c.phase == phaseHeader || c.phase == phaseRequest || c.phase == phaseIdle:
		return c.forwardLocked(dirBoth, t)
	}
	return nil
}

func (c *limitConn) SetReadDeadline(t time.Time) error {
	c.awaitHandshake()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase >= phaseH2 {
		return c.forwardLocked(dirRead, t)
	}
	if c.brR > 0 {
		if (c.brR == 2) != t.IsZero() {
			c.brR--
			return nil
		}
		c.brR = 0
	}
	if isPast(t) {
		if c.phase == phaseRequest {
			stopLocked(&c.rd)
		}
		c.rawSetLocked(dirRead, past())
		return nil
	}
	switch c.phase {
	case phaseHeader:
		if c.hdr0 && !t.IsZero() {
			c.hdr0 = false
			return nil
		}
		c.hdr0 = false
		if c.ln.lim.h2c && c.npre == len(h2Preface) && string(c.preface[:]) == h2Preface {
			c.enterH2Locked()
			return c.forwardLocked(dirRead, t)
		}
		stopLocked(&c.rd)
		c.capture.Store(false)
		if !c.startSet {
			c.start = c.ln.clk.Now()
			c.startSet = true
		}
		c.phase = phaseRequest
		if lim := c.ln.lim.read; lim > 0 {
			c.armLocked(&c.rd, c.ln.clk, c.start.Add(lim).Sub(c.ln.clk.Now()), dirRead)
		}
	case phaseRequest:
		stopLocked(&c.rd)
		if t.IsZero() {
			c.rawSetLocked(dirRead, t)
		} else {
			return c.forwardLocked(dirRead, t)
		}
	case phaseIdle:
		c.idleSets++
		if c.idleSets == 1 {
			if d := c.ln.lim.idleLimit(); d > 0 {
				c.armLocked(&c.rd, c.ln.idleClk, d, dirRead)
			}
		} else {
			c.beginHeaderLocked()
		}
	case phaseNew, phaseHandshake:
		if t.IsZero() {
			c.rawSetLocked(dirRead, t)
		}
	}
	return nil
}

func (c *limitConn) SetWriteDeadline(t time.Time) error {
	c.awaitHandshake()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase >= phaseH2 {
		return c.forwardLocked(dirWrite, t)
	}
	if c.brW > 0 {
		if (c.brW == 2) != t.IsZero() {
			c.brW--
			return nil
		}
		c.brW = 0
	}
	switch c.phase {
	case phaseHeader, phaseRequest, phaseIdle:
		if t.IsZero() {
			stopLocked(&c.wr)
			c.rawSetLocked(dirWrite, t)
			return nil
		}
		if c.wDefer && c.phase != phaseIdle && c.reading.Load() == 0 {
			c.wDefer = false
			if c.ln.lim.write > 0 && !c.wArmed {
				c.wArmed = true
				stopLocked(&c.wr)
				c.rawSetLocked(dirWrite, time.Time{})
				c.armLocked(&c.wr, c.ln.clk, c.ln.lim.write, dirWrite)
			}
			return nil
		}
		stopLocked(&c.wr)
		if isPast(t) {
			c.rawSetLocked(dirWrite, past())
			return nil
		}
		return c.forwardLocked(dirWrite, t)
	case phaseNew, phaseHandshake:
		if t.IsZero() {
			c.rawSetLocked(dirWrite, t)
		}
	}
	return nil
}

func (c *limitConn) Read(b []byte) (int, error) {
	c.reading.Add(1)
	n, err := c.Conn.Read(b)
	c.reading.Add(-1)
	if c.capture.Load() {
		c.mu.Lock()
		c.npre += copy(c.preface[c.npre:], b[:n])
		if !c.startSet && (err != nil || (c.npre >= len(h2PeekShort) && string(c.preface[:len(h2PeekShort)]) != h2PeekShort) || c.npre == len(c.preface)) {
			c.start = c.ln.clk.Now()
			c.startSet = true
		}
		if c.npre == len(c.preface) || err != nil {
			c.capture.Store(false)
		}
		c.mu.Unlock()
	}
	return n, err
}

func (c *limitConn) Close() error {
	c.mu.Lock()
	c.phase = phaseClosed
	c.stopAllLocked()
	c.mu.Unlock()
	return c.Conn.Close()
}

// CloseWrite half-closes the raw conn so net/http can send a FIN before its final close, as it does on a bare TCP conn.
func (c *limitConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}

func limitConnOf(c net.Conn) *limitConn {
	switch c := c.(type) {
	case *limitConn:
		return c
	case *tls.Conn:
		lc, _ := c.NetConn().(*limitConn)
		return lc
	}
	return nil
}

func trackConnState(c net.Conn, st http.ConnState) {
	if lc := limitConnOf(c); lc != nil {
		lc.state(st)
	}
}
