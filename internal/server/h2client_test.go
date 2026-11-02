package server

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/ok0/go-microgateway/internal/h2server"
	"github.com/ok0/go-microgateway/internal/h2server/hpack"
	"github.com/ok0/go-microgateway/internal/testwait"
)

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
	t       *testing.T
	conn    net.Conn
	wmu     sync.Mutex
	fr      *h2server.Framer
	enc     *hpack.Encoder
	encBuf  bytes.Buffer
	br      *bufio.Reader
	frames  chan h2frame
	done    chan struct{}
	gated   chan struct{}
	release chan struct{}
	mu      sync.Mutex
	err     error
}

// dialH2 connects to addr, over TLS trusting pool when pool is not nil and over plain TCP otherwise. A rcvBuf above
// zero sets the receive buffer of the socket before any I/O.
func dialH2(t *testing.T, addr string, pool *x509.CertPool, rcvBuf int) *h2peer {
	t.Helper()
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	if tc, ok := raw.(*net.TCPConn); ok && rcvBuf > 0 {
		if err := tc.SetReadBuffer(rcvBuf); err != nil {
			t.Fatal(err)
		}
	}
	var conn net.Conn = raw
	if pool != nil {
		tc := tls.Client(raw, &tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{"h2"}})
		if err := tc.Handshake(); err != nil {
			t.Fatalf("TLS handshake: %v", err)
		}
		if p := tc.ConnectionState().NegotiatedProtocol; p != "h2" {
			t.Fatalf("ALPN = %q, want h2", p)
		}
		conn = tc
	}
	br := bufio.NewReader(conn)
	p := &h2peer{
		t:       t,
		conn:    conn,
		fr:      h2server.NewFramer(conn, br),
		br:      br,
		frames:  make(chan h2frame, 4096),
		done:    make(chan struct{}),
		gated:   make(chan struct{}),
		release: make(chan struct{}),
	}
	p.fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	p.enc = hpack.NewEncoder(&p.encBuf)
	return p
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

// preface writes the client preface and a SETTINGS frame carrying settings.
func (p *h2peer) preface(settings ...h2server.Setting) {
	p.t.Helper()
	p.mustSend(func() error {
		if _, err := p.conn.Write([]byte(h2server.ClientPreface)); err != nil {
			return err
		}
		return p.fr.WriteSettings(settings...)
	})
}

// start begins reading frames in the background. It acknowledges SETTINGS, and PING when ackPings is set. When gate
// is not zero, it stops before reading the first DATA frame of that stream, closes gated, and waits for release.
func (p *h2peer) start(ackPings bool, gate uint32) {
	go func() {
		defer close(p.done)
		gateHit := false
		for {
			if hdr, err := p.br.Peek(9); err == nil {
				if !gateHit && gate != 0 && h2server.FrameType(hdr[3]) == h2server.FrameData && binary.BigEndian.Uint32(hdr[5:9])&(1<<31-1) == gate {
					gateHit = true
					close(p.gated)
					<-p.release
				}
			}
			f, err := p.fr.ReadFrame()
			if err != nil {
				p.mu.Lock()
				p.err = err
				p.mu.Unlock()
				return
			}
			switch f := f.(type) {
			case *h2server.SettingsFrame:
				if !f.IsAck() {
					_ = p.send(p.fr.WriteSettingsAck)
				}
			case *h2server.PingFrame:
				if ackPings && !f.IsAck() {
					data := f.Data
					_ = p.send(func() error { return p.fr.WritePing(true, data) })
				}
			}
			p.frames <- summarise(f)
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
func (p *h2peer) handshake(ackPings bool) h2frame {
	p.t.Helper()
	p.preface()
	p.start(ackPings, 0)
	fs := p.until("server SETTINGS", func(f h2frame) bool { return f.Type == h2server.FrameSettings && !f.Ack })
	return fs[len(fs)-1]
}

// headers sends a HEADERS frame on stream id for method and path.
func (p *h2peer) headers(id uint32, method, path string, endStream bool) {
	p.t.Helper()
	p.encBuf.Reset()
	for _, f := range []hpack.HeaderField{
		{Name: ":method", Value: method},
		{Name: ":scheme", Value: "https"},
		{Name: ":authority", Value: "localhost"},
		{Name: ":path", Value: path},
	} {
		if err := p.enc.WriteField(f); err != nil {
			p.t.Fatal(err)
		}
	}
	block := append([]byte(nil), p.encBuf.Bytes()...)
	p.mustSend(func() error {
		return p.fr.WriteHeaders(h2server.HeadersFrameParam{StreamID: id, BlockFragment: block, EndStream: endStream, EndHeaders: true})
	})
}

// data sends a DATA frame on stream id.
func (p *h2peer) data(id uint32, b []byte, endStream bool) {
	p.t.Helper()
	p.mustSend(func() error { return p.fr.WriteData(id, endStream, b) })
}

// windowUpdate sends a WINDOW_UPDATE frame for stream id (0 for the connection).
func (p *h2peer) windowUpdate(id, incr uint32) {
	p.t.Helper()
	p.mustSend(func() error { return p.fr.WriteWindowUpdate(id, incr) })
}

// ping sends a PING frame carrying d.
func (p *h2peer) ping(d [8]byte) {
	p.t.Helper()
	p.mustSend(func() error { return p.fr.WritePing(false, d) })
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

// next waits for the next frame from the server.
func (p *h2peer) next(what string) h2frame {
	p.t.Helper()
	f, _ := testwait.Recv(p.t, what, p.frames, p.status)
	return f
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

// awaitEnd waits until the reader has stopped and returns the error that stopped it.
func (p *h2peer) awaitEnd(what string) error {
	p.t.Helper()
	testwait.Done(p.t, what, p.done, p.status)
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
