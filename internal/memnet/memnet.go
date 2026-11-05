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

type Addr string

func (a Addr) Network() string { return "memnet" }

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

func (c *Conn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

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

func (c *Conn) LocalAddr() net.Addr {
	return c.local
}

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

func Listen(addr string, buffer int) *Listener {
	l := &Listener{addr: Addr(addr), buffer: buffer, done: make(chan struct{})}
	l.cond = sync.NewCond(&l.mu)
	return l
}

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
	d.client.Close()
	d.server.Close()
	return nil, err
}

func (l *Listener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	close(l.done)
	for _, d := range l.queue {
		d.client.Close()
		d.server.Close()
	}
	l.queue = nil
	l.cond.Broadcast()
	return nil
}

func (l *Listener) Addr() net.Addr {
	return l.addr
}
