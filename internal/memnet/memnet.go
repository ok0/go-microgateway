// Package memnet provides in-memory network connections and listeners for tests, including tests that run inside testing/synctest bubbles.
package memnet

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// Addr is the address of an in-memory endpoint. It implements net.Addr with the network name "memnet".
type Addr string

// Network returns "memnet".
func (a Addr) Network() string { return "memnet" }

// String returns the address text.
func (a Addr) String() string { return string(a) }

type link struct {
	mu   sync.Mutex
	cond *sync.Cond
}

type side struct {
	buf     []byte
	ahead   int
	closed  bool
	rcount  int
	writing bool
	rdl     time.Time
	wdl     time.Time
	rtimer  *time.Timer
	wtimer  *time.Timer
}

// Conn is one end of an in-memory pipe. Every blocking wait is a sync.Cond wait or a channel operation, so it is durably blocking in a bubble; the only goroutines involved are the deadline timers that the deadline setters start.
type Conn struct {
	l             *link
	me, peer      *side
	local, remote Addr
}

// Pipe returns the two ends of a connected in-memory pipe. Each end accepts up to buffer bytes from the peer before further writes block; with buffer zero, a write returns only after the peer has read the data.
func Pipe(buffer int) (client, server *Conn) {
	return newPipe(buffer, Addr("memnet-client"), Addr("memnet-server"))
}

func newPipe(buffer int, clientAddr, serverAddr Addr) (*Conn, *Conn) {
	l := &link{}
	l.cond = sync.NewCond(&l.mu)
	a := &side{ahead: max(buffer, 0)}
	b := &side{ahead: max(buffer, 0)}
	client := &Conn{l: l, me: a, peer: b, local: clientAddr, remote: serverAddr}
	server := &Conn{l: l, me: b, peer: a, local: serverAddr, remote: clientAddr}
	return client, server
}

func expired(t time.Time) bool {
	return !t.IsZero() && !time.Now().Before(t)
}

func opErr(op string, c *Conn, err error) error {
	return &net.OpError{Op: op, Net: "memnet", Source: c.local, Addr: c.remote, Err: err}
}

// Read reads the bytes the peer has written. It blocks until data arrives, returns io.EOF once the peer has closed and the data is consumed, fails with os.ErrDeadlineExceeded when the read deadline passes, and fails with net.ErrClosed after a local Close.
func (c *Conn) Read(p []byte) (int, error) {
	l := c.l
	l.mu.Lock()
	defer l.mu.Unlock()
	me := c.me
	for {
		if me.closed {
			return 0, opErr("read", c, net.ErrClosed)
		}
		if len(p) == 0 {
			return 0, nil
		}
		if len(me.buf) > 0 {
			n := copy(p, me.buf)
			me.buf = me.buf[n:]
			if len(me.buf) == 0 {
				me.buf = nil
			}
			me.rcount += n
			l.cond.Broadcast()
			return n, nil
		}
		if c.peer.closed {
			return 0, io.EOF
		}
		if expired(me.rdl) {
			return 0, opErr("read", c, os.ErrDeadlineExceeded)
		}
		l.cond.Wait()
	}
}

// Write sends p to the peer and blocks while the peer's buffer is full. It fails with net.ErrClosed when either end is closed and with os.ErrDeadlineExceeded when the write deadline passes, and it reports the bytes accepted before that point.
func (c *Conn) Write(p []byte) (int, error) {
	l := c.l
	l.mu.Lock()
	defer l.mu.Unlock()
	me, peer := c.me, c.peer
	for me.writing {
		if me.closed {
			return 0, opErr("write", c, net.ErrClosed)
		}
		if expired(me.wdl) {
			return 0, opErr("write", c, os.ErrDeadlineExceeded)
		}
		l.cond.Wait()
	}
	me.writing = true
	defer func() {
		me.writing = false
		l.cond.Broadcast()
	}()
	written := 0
	for {
		if me.closed || peer.closed {
			return written, opErr("write", c, net.ErrClosed)
		}
		if peer.ahead > 0 {
			if room := peer.ahead - len(peer.buf); room > 0 {
				k := min(room, len(p)-written)
				peer.buf = append(peer.buf, p[written:written+k]...)
				written += k
				l.cond.Broadcast()
				if written == len(p) {
					return written, nil
				}
				continue
			}
		} else if len(peer.buf) == 0 {
			start := peer.rcount
			peer.buf = append(peer.buf, p[written:]...)
			l.cond.Broadcast()
			return c.drain(p, written, start)
		}
		if expired(me.wdl) {
			return written, opErr("write", c, os.ErrDeadlineExceeded)
		}
		l.cond.Wait()
	}
}

func (c *Conn) drain(p []byte, written, start int) (int, error) {
	l := c.l
	me, peer := c.me, c.peer
	for {
		if me.closed || peer.closed {
			consumed := peer.rcount - start
			peer.buf = nil
			return written + consumed, opErr("write", c, net.ErrClosed)
		}
		if len(peer.buf) == 0 {
			return len(p), nil
		}
		if expired(me.wdl) {
			consumed := peer.rcount - start
			peer.buf = nil
			return written + consumed, opErr("write", c, os.ErrDeadlineExceeded)
		}
		l.cond.Wait()
	}
}

// SetReadBuffer sets how many bytes the peer may buffer for this end to read before the peer's writes block. Negative values are treated as zero. It always returns nil.
func (c *Conn) SetReadBuffer(n int) error {
	c.l.mu.Lock()
	defer c.l.mu.Unlock()
	c.me.ahead = max(n, 0)
	c.l.cond.Broadcast()
	return nil
}

func (c *Conn) rearm(old *time.Timer, t time.Time) *time.Timer {
	if old != nil {
		old.Stop()
	}
	if t.IsZero() {
		return nil
	}
	d := time.Until(t)
	if d <= 0 {
		return nil
	}
	l := c.l
	return time.AfterFunc(d, func() {
		l.mu.Lock()
		l.cond.Broadcast()
		l.mu.Unlock()
	})
}

// SetReadDeadline sets the read deadline, and a zero time clears it. It fails with net.ErrClosed when this end is closed.
func (c *Conn) SetReadDeadline(t time.Time) error {
	l := c.l
	l.mu.Lock()
	defer l.mu.Unlock()
	if c.me.closed {
		return opErr("set", c, net.ErrClosed)
	}
	c.me.rdl = t
	c.me.rtimer = c.rearm(c.me.rtimer, t)
	l.cond.Broadcast()
	return nil
}

// SetWriteDeadline sets the write deadline, and a zero time clears it. It fails with net.ErrClosed when this end is closed.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	l := c.l
	l.mu.Lock()
	defer l.mu.Unlock()
	if c.me.closed {
		return opErr("set", c, net.ErrClosed)
	}
	c.me.wdl = t
	c.me.wtimer = c.rearm(c.me.wtimer, t)
	l.cond.Broadcast()
	return nil
}

// SetDeadline sets both the read and the write deadline.
func (c *Conn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

// Close closes this end and discards the data buffered for it. The peer reads the data it already has and then io.EOF. A second Close returns an error wrapping net.ErrClosed.
func (c *Conn) Close() error {
	l := c.l
	l.mu.Lock()
	defer l.mu.Unlock()
	if c.me.closed {
		return opErr("close", c, net.ErrClosed)
	}
	c.me.closed = true
	c.me.buf = nil
	c.me.rtimer = c.rearm(c.me.rtimer, time.Time{})
	c.me.wtimer = c.rearm(c.me.wtimer, time.Time{})
	l.cond.Broadcast()
	return nil
}

// LocalAddr returns the address of this end.
func (c *Conn) LocalAddr() net.Addr {
	return c.local
}

// RemoteAddr returns the address of the peer.
func (c *Conn) RemoteAddr() net.Addr {
	return c.remote
}

// Listener accepts in-memory connections. Every blocking wait is a sync.Cond wait or a channel operation, so it is durably blocking in a bubble; the only goroutines involved are the deadline timers that the deadline setters start.
type Listener struct {
	addr   Addr
	buffer int
	mu     sync.Mutex
	cond   *sync.Cond
	queue  []*dial
	closed bool
	done   chan struct{}
	n      int
}

type dial struct {
	client, server *Conn
	taken          chan struct{}
}

// Listen returns a listener at addr. Each connection it accepts is a pipe with buffer bytes of buffering in each direction.
func Listen(addr string, buffer int) *Listener {
	l := &Listener{addr: Addr(addr), buffer: buffer, done: make(chan struct{})}
	l.cond = sync.NewCond(&l.mu)
	return l
}

// Accept returns the server end of the next queued connection, blocking until one is dialed. It fails with a net.ErrClosed error after Close.
func (l *Listener) Accept() (net.Conn, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for len(l.queue) == 0 && !l.closed {
		l.cond.Wait()
	}
	if l.closed {
		return nil, &net.OpError{Op: "accept", Net: "memnet", Addr: l.addr, Err: net.ErrClosed}
	}
	d := l.queue[0]
	l.queue = l.queue[1:]
	close(d.taken)
	return d.server, nil
}

// DialContext queues a connection to the listener and waits until Accept takes it. The network and address arguments are ignored. It returns the context error if ctx ends first, and a dial error wrapping net.ErrClosed when the listener is closed.
func (l *Listener) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, &net.OpError{Op: "dial", Net: "memnet", Addr: l.addr, Err: net.ErrClosed}
	}
	l.n++
	client, server := newPipe(l.buffer, Addr(fmt.Sprintf("memnet-client-%d", l.n)), l.addr)
	d := &dial{client: client, server: server, taken: make(chan struct{})}
	l.queue = append(l.queue, d)
	l.cond.Broadcast()
	l.mu.Unlock()
	select {
	case <-d.taken:
		return client, nil
	case <-ctx.Done():
		return l.withdraw(d, ctx.Err())
	case <-l.done:
		return l.withdraw(d, &net.OpError{Op: "dial", Net: "memnet", Addr: l.addr, Err: net.ErrClosed})
	}
}

func (l *Listener) withdraw(d *dial, err error) (net.Conn, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case <-d.taken:
		return d.client, nil
	default:
	}
	for i, q := range l.queue {
		if q == d {
			l.queue = append(l.queue[:i], l.queue[i+1:]...)
			break
		}
	}
	// Closing a pipe end its peer already closed reports net.ErrClosed; both ends are closed either way.
	_ = d.client.Close()
	_ = d.server.Close()
	return nil, err
}

// Close stops the listener, closes every queued connection that Accept has not taken, and wakes blocked Accept and DialContext calls. Closing a closed listener returns nil.
func (l *Listener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	close(l.done)
	for _, d := range l.queue {
		// Closing a pipe end its peer already closed reports net.ErrClosed; both ends are closed either way.
		_ = d.client.Close()
		_ = d.server.Close()
	}
	l.queue = nil
	l.cond.Broadcast()
	return nil
}

// Addr returns the address of the listener.
func (l *Listener) Addr() net.Addr {
	return l.addr
}
