package server

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/ok0/go-microgateway/internal/h2server"
	"github.com/ok0/go-microgateway/internal/h2server/hpack"
	"github.com/ok0/go-microgateway/internal/testwait"
)

// errTLSHandshake reports that a server cut the TLS handshake, which it may do at a real-time limit.
var errTLSHandshake = errors.New("TLS handshake")

// errNotH2 reports that ALPN did not select h2.
var errNotH2 = errors.New("ALPN did not select h2")

// errPeerClosed reports that the test closed the peer while the reader was held.
var errPeerClosed = errors.New("peer closed by the test")

// h2frame is the summary of one frame received by an h2peer.
type h2frame struct {
	Type       h2server.FrameType
	Stream     uint32
	EndStream  bool
	Ack        bool
	Code       h2server.ErrCode
	LastStream uint32
	DataLen    int
	Header     [][2]string
	Settings   map[h2server.SettingID]uint32
	Incr       uint32
	Ping       [8]byte
}

// h2peer is a raw HTTP/2 client that sends frames by hand and records what the server sends.
type h2peer struct {
	// t is nil for a peer from openH2; such a peer is used only through the methods that return errors (write*, start,
	// await, endErr, close).
	t         *testing.T
	raw       net.Conn
	conn      net.Conn
	wmu       sync.Mutex
	fr        *h2server.Framer
	enc       *hpack.Encoder
	encBuf    bytes.Buffer
	br        *bufio.Reader
	frames    chan h2frame
	done      chan struct{}
	gated     chan struct{}
	release   chan struct{}
	quit      chan struct{}
	closeOnce sync.Once
	// holdSettingsAck, when set, makes the reader leave the server's SETTINGS unacknowledged, so the connection stays
	// silent after the preface. It is set before start.
	holdSettingsAck bool
	// pending holds frames that barrier put back for the next wait. It is used by the test goroutine only.
	pending []h2frame
	// syncs numbers the barriers of the peer, so that each PING carries a distinct payload.
	syncs uint64
	mu    sync.Mutex
	err   error
}

// openH2 connects to addr, over TLS trusting pool when pool is not nil and over plain TCP otherwise. A rcvBuf above
// zero sets the receive buffer of the socket before any I/O. It reports failures instead of failing a test, so it can
// run off the test goroutine; the caller closes the peer.
func openH2(addr string, pool *x509.CertPool, rcvBuf int) (*h2peer, error) {
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := raw.(*net.TCPConn); ok && rcvBuf > 0 {
		if err := tc.SetReadBuffer(rcvBuf); err != nil {
			raw.Close()
			return nil, err
		}
	}
	var conn net.Conn = raw
	if pool != nil {
		tc := tls.Client(raw, &tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{"h2"}})
		if err := tc.Handshake(); err != nil {
			raw.Close()
			return nil, fmt.Errorf("%w: %w", errTLSHandshake, err)
		}
		if proto := tc.ConnectionState().NegotiatedProtocol; proto != "h2" {
			raw.Close()
			return nil, fmt.Errorf("%w: got %q", errNotH2, proto)
		}
		conn = tc
	}
	br := bufio.NewReader(conn)
	p := &h2peer{
		raw:     raw,
		conn:    conn,
		fr:      h2server.NewFramer(conn, br),
		br:      br,
		frames:  make(chan h2frame, 4096),
		done:    make(chan struct{}),
		gated:   make(chan struct{}),
		release: make(chan struct{}),
		quit:    make(chan struct{}),
	}
	p.fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	p.enc = hpack.NewEncoder(&p.encBuf)
	return p, nil
}

// dialH2 connects to addr, over TLS trusting pool when pool is not nil and over plain TCP otherwise. A rcvBuf above
// zero sets the receive buffer of the socket before any I/O.
func dialH2(t *testing.T, addr string, pool *x509.CertPool, rcvBuf int) *h2peer {
	t.Helper()
	p, err := openH2(addr, pool, rcvBuf)
	if err != nil {
		t.Fatal(err)
	}
	p.t = t
	t.Cleanup(p.close)
	return p
}

// close stops the reader, also when it is held at the gate or blocked on a full frames, and closes the connection.
func (p *h2peer) close() {
	p.closeOnce.Do(func() {
		close(p.quit)
		p.raw.Close()
	})
}

// send runs f with the write side of the connection held.
func (p *h2peer) send(f func() error) error {
	p.wmu.Lock()
	defer p.wmu.Unlock()
	return f()
}

func (p *h2peer) mustSend(f func() error) {
	p.t.Helper()
	if err := p.send(f); err != nil {
		p.t.Fatal(err)
	}
}

// writePreface writes the client preface and a SETTINGS frame carrying settings.
func (p *h2peer) writePreface(settings ...h2server.Setting) error {
	return p.send(func() error {
		if _, err := p.conn.Write([]byte(h2server.ClientPreface)); err != nil {
			return err
		}
		return p.fr.WriteSettings(settings...)
	})
}

// preface writes the client preface and a SETTINGS frame carrying settings.
func (p *h2peer) preface(settings ...h2server.Setting) {
	p.t.Helper()
	if err := p.writePreface(settings...); err != nil {
		p.t.Fatal(err)
	}
}

// start begins reading frames in the background. It acknowledges SETTINGS, and PING when ackPings is set. When gate
// is not zero, it stops before reading the first DATA frame of that stream, closes gated, and waits for release.
func (p *h2peer) start(ackPings bool, gate uint32) {
	go func() {
		var err error
		defer func() {
			p.mu.Lock()
			p.err = err
			p.mu.Unlock()
			close(p.done)
			close(p.frames)
		}()
		gateHit := false
		for {
			if hdr, err := p.br.Peek(9); err == nil {
				if !gateHit && gate != 0 && h2server.FrameType(hdr[3]) == h2server.FrameData && binary.BigEndian.Uint32(hdr[5:9])&(1<<31-1) == gate {
					gateHit = true
					close(p.gated)
					select {
					case <-p.release:
					case <-p.quit:
						err = errPeerClosed
						return
					}
				}
			}
			f, rerr := p.fr.ReadFrame()
			if rerr != nil {
				err = rerr
				return
			}
			switch f := f.(type) {
			case *h2server.SettingsFrame:
				if !f.IsAck() && !p.holdSettingsAck {
					_ = p.send(p.fr.WriteSettingsAck)
				}
			case *h2server.PingFrame:
				if ackPings && !f.IsAck() {
					data := f.Data
					_ = p.send(func() error { return p.fr.WritePing(true, data) })
				}
			}
			select {
			case p.frames <- summarise(f):
			case <-p.quit:
				err = errPeerClosed
				return
			}
		}
	}()
}

// summarise copies the fields the tests read from f. The copy stays valid after the next ReadFrame.
func summarise(f h2server.Frame) h2frame {
	out := h2frame{Type: f.Header().Type, Stream: f.Header().StreamID}
	switch f := f.(type) {
	case *h2server.DataFrame:
		out.EndStream = f.StreamEnded()
		out.DataLen = len(f.Data())
	case *h2server.MetaHeadersFrame:
		out.EndStream = f.StreamEnded()
		for _, hf := range f.Fields {
			out.Header = append(out.Header, [2]string{hf.Name, hf.Value})
		}
	case *h2server.SettingsFrame:
		out.Ack = f.IsAck()
		out.Settings = make(map[h2server.SettingID]uint32)
		_ = f.ForeachSetting(func(s h2server.Setting) error {
			out.Settings[s.ID] = s.Val
			return nil
		})
	case *h2server.PingFrame:
		out.Ack = f.IsAck()
		out.Ping = f.Data
	case *h2server.GoAwayFrame:
		out.Code = f.ErrCode
		out.LastStream = f.LastStreamID
	case *h2server.RSTStreamFrame:
		out.Code = f.ErrCode
	case *h2server.WindowUpdateFrame:
		out.Incr = f.Increment
	}
	return out
}

// handshake sends the preface, starts the reader and returns the server's first SETTINGS frame that is not an ACK.
// It ends with a barrier: when it returns, the first-SETTINGS timer has stopped, and frames that arrived meanwhile stay
// queued.
func (p *h2peer) handshake(ackPings bool) h2frame {
	p.t.Helper()
	p.preface()
	p.start(ackPings, 0)
	fs := p.until("server SETTINGS", func(f h2frame) bool { return f.Type == h2server.FrameSettings && !f.Ack })
	p.barrier("server processed the client SETTINGS")
	return fs[len(fs)-1]
}

// writeHeaders writes a HEADERS frame on stream id for method and path.
func (p *h2peer) writeHeaders(id uint32, method, path string, endStream bool) error {
	p.encBuf.Reset()
	for _, f := range []hpack.HeaderField{
		{Name: ":method", Value: method},
		{Name: ":scheme", Value: "https"},
		{Name: ":authority", Value: "localhost"},
		{Name: ":path", Value: path},
	} {
		if err := p.enc.WriteField(f); err != nil {
			return err
		}
	}
	block := append([]byte(nil), p.encBuf.Bytes()...)
	return p.send(func() error {
		return p.fr.WriteHeaders(h2server.HeadersFrameParam{StreamID: id, BlockFragment: block, EndStream: endStream, EndHeaders: true})
	})
}

// headers sends a HEADERS frame on stream id for method and path.
func (p *h2peer) headers(id uint32, method, path string, endStream bool) {
	p.t.Helper()
	if err := p.writeHeaders(id, method, path, endStream); err != nil {
		p.t.Fatal(err)
	}
}

// writeData writes a DATA frame on stream id.
func (p *h2peer) writeData(id uint32, b []byte, endStream bool) error {
	return p.send(func() error { return p.fr.WriteData(id, endStream, b) })
}

// data sends a DATA frame on stream id.
func (p *h2peer) data(id uint32, b []byte, endStream bool) {
	p.t.Helper()
	if err := p.writeData(id, b, endStream); err != nil {
		p.t.Fatal(err)
	}
}

// writeWindowUpdate writes a WINDOW_UPDATE frame for stream id (0 for the connection).
func (p *h2peer) writeWindowUpdate(id, incr uint32) error {
	return p.send(func() error { return p.fr.WriteWindowUpdate(id, incr) })
}

// windowUpdate sends a WINDOW_UPDATE frame for stream id (0 for the connection).
func (p *h2peer) windowUpdate(id, incr uint32) {
	p.t.Helper()
	if err := p.writeWindowUpdate(id, incr); err != nil {
		p.t.Fatal(err)
	}
}

// writePing writes a PING frame carrying d.
func (p *h2peer) writePing(d [8]byte) error {
	return p.send(func() error { return p.fr.WritePing(false, d) })
}

// ping sends a PING frame carrying d.
func (p *h2peer) ping(d [8]byte) {
	p.t.Helper()
	if err := p.writePing(d); err != nil {
		p.t.Fatal(err)
	}
}

// status describes the state of the reader for a hang-guard diagnostic.
func (p *h2peer) status() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return fmt.Sprintf("reader ended: %v", p.err)
	}
	return "reader running"
}

// next waits for the next frame from the server. It fails the test when the connection ends first.
func (p *h2peer) next(what string) h2frame {
	p.t.Helper()
	if len(p.pending) > 0 {
		f := p.pending[0]
		p.pending = p.pending[1:]
		return f
	}
	f, ok := testwait.Recv(p.t, what, p.frames, p.status)
	if !ok {
		p.t.Fatalf("%s: connection ended first (%s)", what, p.status())
	}
	return f
}

// barrier sends a PING and waits for its ACK. The server processes the peer's frames in order and writes one frame at
// a time, so when the ACK arrives it has processed every frame sent before the PING and has finished writing every
// frame it sent before the ACK. Frames that arrive before the ACK stay queued for the next wait.
func (p *h2peer) barrier(what string) {
	p.t.Helper()
	p.syncs++
	var d [8]byte
	binary.BigEndian.PutUint64(d[:], 1<<63|p.syncs)
	p.ping(d)
	var held []h2frame
	for {
		f := p.next(what)
		if f.Type == h2server.FramePing && f.Ack && f.Ping == d {
			p.pending = held
			return
		}
		held = append(held, f)
	}
}

// until collects frames until one matches, and returns them with the matching frame last.
func (p *h2peer) until(what string, match func(h2frame) bool) []h2frame {
	p.t.Helper()
	var got []h2frame
	for {
		f := p.next(what)
		got = append(got, f)
		if match(f) {
			return got
		}
	}
}

// h2event is what await stopped on.
type h2event int

const (
	evFrame h2event = iota
	evSignal
	evEnd
)

// await collects frames until match accepts one (evFrame), sig is closed (evSignal) or the reader stopped (evEnd). A
// nil sig or match never fires. It never waits on real time; it is for runs off the test goroutine.
func (p *h2peer) await(sig <-chan struct{}, match func(h2frame) bool) ([]h2frame, h2event) {
	var fs []h2frame
	for len(p.pending) > 0 {
		f := p.pending[0]
		p.pending = p.pending[1:]
		fs = append(fs, f)
		if match != nil && match(f) {
			return fs, evFrame
		}
	}
	for {
		select {
		case <-sig:
			return fs, evSignal
		case f, ok := <-p.frames:
			if !ok {
				return fs, evEnd
			}
			fs = append(fs, f)
			if match != nil && match(f) {
				return fs, evFrame
			}
		}
	}
}

// awaitEnd waits until the reader has stopped and returns the error that stopped it.
func (p *h2peer) awaitEnd(what string) error {
	p.t.Helper()
	testwait.Done(p.t, what, p.done, p.status)
	return p.endErr()
}

// endErr returns the error that stopped the reader, or nil while it runs.
func (p *h2peer) endErr() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// open fails the test if the server has already ended the connection.
func (p *h2peer) open() {
	p.t.Helper()
	select {
	case <-p.done:
		p.t.Fatalf("connection ended before expected: %v", p.status())
	default:
	}
}
