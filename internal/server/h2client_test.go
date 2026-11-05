package server

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/testwait"
)

// errTLSHandshake reports that a server cut the TLS handshake, which it may do at a real-time limit.
var errTLSHandshake = errors.New("TLS handshake")

// errNotH2 reports that ALPN did not select h2.
var errNotH2 = errors.New("ALPN did not select h2")

// errPeerClosed reports that the test closed the peer while the reader was held.
var errPeerClosed = errors.New("peer closed by the test")

// h2FrameType is the type octet of an HTTP/2 frame.
type h2FrameType uint8

const (
	h2Data         h2FrameType = 0x0
	h2Headers      h2FrameType = 0x1
	h2Priority     h2FrameType = 0x2
	h2RSTStream    h2FrameType = 0x3
	h2Settings     h2FrameType = 0x4
	h2PushPromise  h2FrameType = 0x5
	h2Ping         h2FrameType = 0x6
	h2GoAway       h2FrameType = 0x7
	h2WindowUpdate h2FrameType = 0x8
	h2Continuation h2FrameType = 0x9
)

// String returns the RFC 9113 name of the frame type, or FRAME_ and its number for an unknown type.
func (t h2FrameType) String() string {
	switch t {
	case h2Data:
		return "DATA"
	case h2Headers:
		return "HEADERS"
	case h2Priority:
		return "PRIORITY"
	case h2RSTStream:
		return "RST_STREAM"
	case h2Settings:
		return "SETTINGS"
	case h2PushPromise:
		return "PUSH_PROMISE"
	case h2Ping:
		return "PING"
	case h2GoAway:
		return "GOAWAY"
	case h2WindowUpdate:
		return "WINDOW_UPDATE"
	case h2Continuation:
		return "CONTINUATION"
	}
	return fmt.Sprintf("FRAME_%d", uint8(t))
}

// h2ErrCode is an HTTP/2 error code.
type h2ErrCode uint32

const (
	h2ErrNo                 h2ErrCode = 0x0
	h2ErrProtocol           h2ErrCode = 0x1
	h2ErrInternal           h2ErrCode = 0x2
	h2ErrFlowControl        h2ErrCode = 0x3
	h2ErrSettingsTimeout    h2ErrCode = 0x4
	h2ErrStreamClosed       h2ErrCode = 0x5
	h2ErrFrameSize          h2ErrCode = 0x6
	h2ErrRefusedStream      h2ErrCode = 0x7
	h2ErrCancel             h2ErrCode = 0x8
	h2ErrCompression        h2ErrCode = 0x9
	h2ErrConnect            h2ErrCode = 0xa
	h2ErrEnhanceYourCalm    h2ErrCode = 0xb
	h2ErrInadequateSecurity h2ErrCode = 0xc
	h2ErrHTTP11Required     h2ErrCode = 0xd
)

// String returns the RFC 9113 name of the error code, or ERR_ and its number for an unknown code.
func (c h2ErrCode) String() string {
	switch c {
	case h2ErrNo:
		return "NO_ERROR"
	case h2ErrProtocol:
		return "PROTOCOL_ERROR"
	case h2ErrInternal:
		return "INTERNAL_ERROR"
	case h2ErrFlowControl:
		return "FLOW_CONTROL_ERROR"
	case h2ErrSettingsTimeout:
		return "SETTINGS_TIMEOUT"
	case h2ErrStreamClosed:
		return "STREAM_CLOSED"
	case h2ErrFrameSize:
		return "FRAME_SIZE_ERROR"
	case h2ErrRefusedStream:
		return "REFUSED_STREAM"
	case h2ErrCancel:
		return "CANCEL"
	case h2ErrCompression:
		return "COMPRESSION_ERROR"
	case h2ErrConnect:
		return "CONNECT_ERROR"
	case h2ErrEnhanceYourCalm:
		return "ENHANCE_YOUR_CALM"
	case h2ErrInadequateSecurity:
		return "INADEQUATE_SECURITY"
	case h2ErrHTTP11Required:
		return "HTTP_1_1_REQUIRED"
	}
	return fmt.Sprintf("ERR_%d", uint32(c))
}

// h2SettingID identifies a SETTINGS parameter.
type h2SettingID uint16

const (
	h2SettingHeaderTableSize      h2SettingID = 1
	h2SettingEnablePush           h2SettingID = 2
	h2SettingMaxConcurrentStreams h2SettingID = 3
	h2SettingInitialWindowSize    h2SettingID = 4
	h2SettingMaxFrameSize         h2SettingID = 5
	h2SettingMaxHeaderListSize    h2SettingID = 6
	h2SettingNoRFC7540Priorities  h2SettingID = 9
)

// h2Setting is one SETTINGS parameter sent by the peer.
type h2Setting struct {
	ID  h2SettingID
	Val uint32
}

// Frame flags used by the peer.
const (
	h2FlagEndStream  = 0x1
	h2FlagAck        = 0x1
	h2FlagEndHeaders = 0x4
	h2FlagPadded     = 0x8
	h2FlagPriority   = 0x20
)

// h2frame is the summary of one frame received by an h2peer.
type h2frame struct {
	Type       h2FrameType
	Stream     uint32
	EndStream  bool
	Ack        bool
	Code       h2ErrCode
	LastStream uint32
	DataLen    int
	Header     [][2]string
	Settings   map[h2SettingID]uint32
	Incr       uint32
	Ping       [8]byte
	At         time.Time
}

// writeFrame writes one frame, its nine-byte header and payload, with a single Write call.
func writeFrame(w io.Writer, typ h2FrameType, flags uint8, stream uint32, payload []byte) error {
	buf := make([]byte, 9, 9+len(payload))
	buf[0] = byte(len(payload) >> 16)
	buf[1] = byte(len(payload) >> 8)
	buf[2] = byte(len(payload))
	buf[3] = byte(typ)
	buf[4] = flags
	binary.BigEndian.PutUint32(buf[5:9], stream&(1<<31-1))
	buf = append(buf, payload...)
	_, err := w.Write(buf)
	return err
}

// readFrame reads one frame and returns its header fields and payload.
func readFrame(r io.Reader) (typ h2FrameType, flags uint8, stream uint32, payload []byte, err error) {
	var hdr [9]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return 0, 0, 0, nil, err
	}
	n := int(hdr[0])<<16 | int(hdr[1])<<8 | int(hdr[2])
	payload = make([]byte, n)
	if _, err = io.ReadFull(r, payload); err != nil {
		return 0, 0, 0, nil, err
	}
	return h2FrameType(hdr[3]), hdr[4], binary.BigEndian.Uint32(hdr[5:9]) & (1<<31 - 1), payload, nil
}

// hpackInt appends v to dst as an HPACK integer with a prefixBits-bit prefix, OR-ing first into the prefix octet.
func hpackInt(dst []byte, prefixBits uint, first byte, v uint64) []byte {
	limit := uint64(1)<<prefixBits - 1
	if v < limit {
		return append(dst, first|byte(v))
	}
	dst = append(dst, first|byte(limit))
	v -= limit
	for v >= 128 {
		dst = append(dst, byte(v%128)|128)
		v /= 128
	}
	return append(dst, byte(v))
}

// hpackString appends s as a string that is not Huffman coded.
func hpackString(dst []byte, s string) []byte {
	dst = hpackInt(dst, 7, 0, uint64(len(s)))
	return append(dst, s...)
}

// hpackLiteral encodes fields as literal header fields without indexing and with new names.
func hpackLiteral(fields [][2]string) []byte {
	var out []byte
	for _, f := range fields {
		out = append(out, 0x00)
		out = hpackString(out, f[0])
		out = hpackString(out, f[1])
	}
	return out
}

// hpackDecodeInt reads an HPACK integer with a prefixBits-bit prefix from b.
func hpackDecodeInt(b []byte, prefixBits uint) (uint64, []byte, error) {
	if len(b) == 0 {
		return 0, nil, fmt.Errorf("HPACK integer: no octets")
	}
	limit := uint64(1)<<prefixBits - 1
	v := uint64(b[0]) & limit
	b = b[1:]
	if v < limit {
		return v, b, nil
	}
	for shift := uint(0); ; shift += 7 {
		if len(b) == 0 {
			return 0, nil, fmt.Errorf("HPACK integer: truncated")
		}
		if shift > 56 {
			return 0, nil, fmt.Errorf("HPACK integer: too large")
		}
		c := b[0]
		b = b[1:]
		v += uint64(c&0x7f) << shift
		if c&0x80 == 0 {
			return v, b, nil
		}
	}
}

// hpackDecodeString reads a string literal from b. A Huffman-coded string is returned as "h:" and its octets in hex.
func hpackDecodeString(b []byte) (string, []byte, error) {
	if len(b) == 0 {
		return "", nil, fmt.Errorf("HPACK string: no octets")
	}
	huffman := b[0]&0x80 != 0
	n, rest, err := hpackDecodeInt(b, 7)
	if err != nil {
		return "", nil, err
	}
	if uint64(len(rest)) < n {
		return "", nil, fmt.Errorf("HPACK string: %d octets declared, %d left", n, len(rest))
	}
	raw := rest[:n]
	rest = rest[n:]
	if huffman {
		return "h:" + hex.EncodeToString(raw), rest, nil
	}
	return string(raw), rest, nil
}

// hpackStatic returns the name and value of static table entry i. Entries 8 to 14 are the :status values; the other
// entries from 1 to 61 are named static-<i> with an empty value.
func hpackStatic(i uint64) (string, string, error) {
	statuses := [...]string{"200", "204", "206", "304", "400", "404", "500"}
	switch {
	case i >= 8 && i <= 14:
		return ":status", statuses[i-8], nil
	case i >= 1 && i <= 61:
		return fmt.Sprintf("static-%d", i), "", nil
	}
	return "", "", fmt.Errorf("HPACK index %d is not in the static table", i)
}

// hpackIndexed decodes an indexed header field from the start of b.
func hpackIndexed(b []byte) ([2]string, []byte, error) {
	i, rest, err := hpackDecodeInt(b, 7)
	if err != nil {
		return [2]string{}, nil, err
	}
	name, value, err := hpackStatic(i)
	if err != nil {
		return [2]string{}, nil, err
	}
	return [2]string{name, value}, rest, nil
}

// hpackLiteralField decodes a literal header field whose name index has prefixBits bits, from the start of b.
func hpackLiteralField(b []byte, prefixBits uint) ([2]string, []byte, error) {
	i, rest, err := hpackDecodeInt(b, prefixBits)
	if err != nil {
		return [2]string{}, nil, err
	}
	var name string
	if i == 0 {
		name, rest, err = hpackDecodeString(rest)
	} else {
		name, _, err = hpackStatic(i)
	}
	if err != nil {
		return [2]string{}, nil, err
	}
	value, rest, err := hpackDecodeString(rest)
	if err != nil {
		return [2]string{}, nil, err
	}
	return [2]string{name, value}, rest, nil
}

// hpackFields decodes a complete header block. Dynamic table size updates are skipped; the peer advertises a header
// table size of zero, so the server never references its dynamic table.
func hpackFields(block []byte) ([][2]string, error) {
	var out [][2]string
	for len(block) > 0 {
		b := block[0]
		var f [2]string
		var err error
		switch {
		case b&0x80 != 0:
			f, block, err = hpackIndexed(block)
		case b&0xc0 == 0x40:
			f, block, err = hpackLiteralField(block, 6)
		case b&0xe0 == 0x20:
			_, block, err = hpackDecodeInt(block, 5)
			if err != nil {
				return nil, err
			}
			continue
		default:
			f, block, err = hpackLiteralField(block, 4)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, nil
}

// unpad removes the padding of a frame whose PADDED flag is set.
func unpad(flags uint8, payload []byte) ([]byte, error) {
	if flags&h2FlagPadded == 0 {
		return payload, nil
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("PADDED frame without a pad length")
	}
	pad := int(payload[0])
	payload = payload[1:]
	if pad > len(payload) {
		return nil, fmt.Errorf("padding of %d octets in a frame of %d", pad, len(payload))
	}
	return payload[:len(payload)-pad], nil
}

// headerBlock returns the header block fragment of a HEADERS frame without its padding and priority fields.
func headerBlock(flags uint8, payload []byte) ([]byte, error) {
	body, err := unpad(flags, payload)
	if err != nil {
		return nil, err
	}
	if flags&h2FlagPriority != 0 {
		if len(body) < 5 {
			return nil, fmt.Errorf("HEADERS priority fields truncated")
		}
		body = body[5:]
	}
	return body, nil
}

// h2peer is a raw HTTP/2 client that sends frames by hand and records what the server sends.
type h2peer struct {
	// t is nil for a peer from openH2; such a peer is used only through the methods that return errors (write*, start,
	// await, endErr, close).
	t         *testing.T
	raw       net.Conn
	conn      net.Conn
	wmu       sync.Mutex
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
	endAt time.Time
	// bubble is set for a peer inside a synctest bubble; its waits then use the bubble-time hang guard instead of
	// internal/testwait.
	bubble bool
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
	return openH2Conn(raw, pool)
}

// openH2Conn starts an HTTP/2 peer on conn, over TLS trusting pool when pool is not nil and over conn as it is
// otherwise. It closes conn when it fails.
func openH2Conn(raw net.Conn, pool *x509.CertPool) (*h2peer, error) {
	conn := raw
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
	return &h2peer{
		raw:     raw,
		conn:    conn,
		br:      bufio.NewReader(conn),
		frames:  make(chan h2frame, 4096),
		done:    make(chan struct{}),
		gated:   make(chan struct{}),
		release: make(chan struct{}),
		quit:    make(chan struct{}),
	}, nil
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

// writePreface writes the client preface and a SETTINGS frame. The frame's first entry sets the header table size to
// zero, and settings follow it.
func (p *h2peer) writePreface(settings ...h2Setting) error {
	return p.send(func() error {
		if _, err := p.conn.Write([]byte(h2Preface)); err != nil {
			return err
		}
		var payload []byte
		for _, s := range append([]h2Setting{{ID: h2SettingHeaderTableSize, Val: 0}}, settings...) {
			payload = binary.BigEndian.AppendUint16(payload, uint16(s.ID))
			payload = binary.BigEndian.AppendUint32(payload, s.Val)
		}
		return writeFrame(p.conn, h2Settings, 0, 0, payload)
	})
}

// preface writes the client preface and a SETTINGS frame carrying settings.
func (p *h2peer) preface(settings ...h2Setting) {
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
			p.endAt = time.Now()
			p.mu.Unlock()
			close(p.done)
			close(p.frames)
		}()
		gateHit := false
		for {
			if hdr, perr := p.br.Peek(9); perr == nil {
				if !gateHit && gate != 0 && h2FrameType(hdr[3]) == h2Data && binary.BigEndian.Uint32(hdr[5:9])&(1<<31-1) == gate {
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
			f, rerr := p.readOne(ackPings)
			if rerr != nil {
				err = rerr
				return
			}
			select {
			case p.frames <- f:
			case <-p.quit:
				err = errPeerClosed
				return
			}
		}
	}()
}

// readOne reads the next frame, acknowledges SETTINGS and PING as configured, and summarises the frame.
func (p *h2peer) readOne(ackPings bool) (h2frame, error) {
	typ, flags, stream, payload, err := readFrame(p.br)
	if err != nil {
		return h2frame{}, err
	}
	f := h2frame{Type: typ, Stream: stream, At: time.Now()}
	switch typ {
	case h2Headers:
		f.EndStream = flags&h2FlagEndStream != 0
		block, err := headerBlock(flags, payload)
		if err != nil {
			return h2frame{}, err
		}
		for flags&h2FlagEndHeaders == 0 {
			typ, flags, _, payload, err = readFrame(p.br)
			if err != nil {
				return h2frame{}, err
			}
			if typ != h2Continuation {
				return h2frame{}, fmt.Errorf("%v frame inside a header block", typ)
			}
			block = append(block, payload...)
		}
		fields, err := hpackFields(block)
		if err != nil {
			return h2frame{}, err
		}
		f.Header = fields
	case h2Data:
		body, err := unpad(flags, payload)
		if err != nil {
			return h2frame{}, err
		}
		f.EndStream = flags&h2FlagEndStream != 0
		f.DataLen = len(body)
	case h2Settings:
		f.Ack = flags&h2FlagAck != 0
		if len(payload)%6 != 0 {
			return h2frame{}, fmt.Errorf("SETTINGS payload of %d octets", len(payload))
		}
		f.Settings = make(map[h2SettingID]uint32)
		for i := 0; i < len(payload); i += 6 {
			f.Settings[h2SettingID(binary.BigEndian.Uint16(payload[i:]))] = binary.BigEndian.Uint32(payload[i+2:])
		}
		if !f.Ack && !p.holdSettingsAck {
			_ = p.send(func() error { return writeFrame(p.conn, h2Settings, h2FlagAck, 0, nil) })
		}
	case h2Ping:
		if len(payload) != 8 {
			return h2frame{}, fmt.Errorf("PING payload of %d octets", len(payload))
		}
		f.Ack = flags&h2FlagAck != 0
		copy(f.Ping[:], payload)
		if ackPings && !f.Ack {
			_ = p.send(func() error { return writeFrame(p.conn, h2Ping, h2FlagAck, 0, payload) })
		}
	case h2GoAway:
		if len(payload) < 8 {
			return h2frame{}, fmt.Errorf("GOAWAY payload of %d octets", len(payload))
		}
		f.LastStream = binary.BigEndian.Uint32(payload[:4]) & (1<<31 - 1)
		f.Code = h2ErrCode(binary.BigEndian.Uint32(payload[4:8]))
	case h2RSTStream:
		if len(payload) != 4 {
			return h2frame{}, fmt.Errorf("RST_STREAM payload of %d octets", len(payload))
		}
		f.Code = h2ErrCode(binary.BigEndian.Uint32(payload))
	case h2WindowUpdate:
		if len(payload) != 4 {
			return h2frame{}, fmt.Errorf("WINDOW_UPDATE payload of %d octets", len(payload))
		}
		f.Incr = binary.BigEndian.Uint32(payload) & (1<<31 - 1)
	}
	return f, nil
}

// handshake sends the preface, starts the reader and returns the server's first SETTINGS frame that is not an ACK.
// It ends with a barrier: when it returns, the first-SETTINGS timer has stopped, and frames that arrived meanwhile stay
// queued.
func (p *h2peer) handshake(ackPings bool) h2frame {
	p.t.Helper()
	p.preface()
	p.start(ackPings, 0)
	fs := p.until("server SETTINGS", func(f h2frame) bool { return f.Type == h2Settings && !f.Ack })
	p.barrier("server processed the client SETTINGS")
	return fs[len(fs)-1]
}

// writeHeaders writes a HEADERS frame on stream id for method and path.
func (p *h2peer) writeHeaders(id uint32, method, path string, endStream bool) error {
	block := hpackLiteral([][2]string{
		{":method", method},
		{":scheme", "https"},
		{":authority", "localhost"},
		{":path", path},
	})
	var flags uint8 = h2FlagEndHeaders
	if endStream {
		flags |= h2FlagEndStream
	}
	return p.send(func() error { return writeFrame(p.conn, h2Headers, flags, id, block) })
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
	var flags uint8
	if endStream {
		flags = h2FlagEndStream
	}
	return p.send(func() error { return writeFrame(p.conn, h2Data, flags, id, b) })
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
	var payload [4]byte
	binary.BigEndian.PutUint32(payload[:], incr&(1<<31-1))
	return p.send(func() error { return writeFrame(p.conn, h2WindowUpdate, 0, id, payload[:]) })
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
	return p.send(func() error { return writeFrame(p.conn, h2Ping, 0, 0, d[:]) })
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
	var f h2frame
	var ok bool
	if p.bubble {
		f, ok = bubbleRecv(p.t, what, p.frames, p.status)
	} else {
		f, ok = testwait.Recv(p.t, what, p.frames, p.status)
	}
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
		if f.Type == h2Ping && f.Ack && f.Ping == d {
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
	if p.bubble {
		bubbleDone(p.t, what, p.done, p.status)
	} else {
		testwait.Done(p.t, what, p.done, p.status)
	}
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

// streamEnded matches the frame that ends stream id, by END_STREAM or by RST_STREAM.
func streamEnded(id uint32) func(h2frame) bool {
	return func(f h2frame) bool {
		return f.Stream == id && (f.EndStream || f.Type == h2RSTStream)
	}
}

// streamRST matches the RST_STREAM frame of stream id.
func streamRST(id uint32) func(h2frame) bool {
	return func(f h2frame) bool { return f.Stream == id && f.Type == h2RSTStream }
}

// goAwayFrame matches any GOAWAY frame.
func goAwayFrame(f h2frame) bool { return f.Type == h2GoAway }

// onStream returns the frames of fs that belong to stream id, in order.
func onStream(fs []h2frame, id uint32) []h2frame {
	var out []h2frame
	for _, f := range fs {
		if f.Stream == id {
			out = append(out, f)
		}
	}
	return out
}

// dataBytes returns the DATA bytes that fs carries on stream id.
func dataBytes(fs []h2frame, id uint32) int {
	n := 0
	for _, f := range fs {
		if f.Stream == id && f.Type == h2Data {
			n += f.DataLen
		}
	}
	return n
}

// statusOf returns the :status of a response HEADERS frame, or "trailer" for a frame without one.
func statusOf(f h2frame) string {
	for _, hf := range f.Header {
		if hf[0] == ":status" {
			return hf[1]
		}
	}
	return "trailer"
}

// drain returns the frames p has already received, without waiting.
func drain(p *h2peer) []h2frame {
	out := p.pending
	p.pending = nil
	for {
		select {
		case f, ok := <-p.frames:
			if !ok {
				return out
			}
			out = append(out, f)
		default:
			return out
		}
	}
}

// h2outcome is what one side of an HTTP/2 limit comparison did, in the terms the comparison checks. Stream events
// are "HEADERS <status|trailer> [ES]", "DATA" once per stream, "DATA ES" once per stream when a DATA frame ends it,
// and "RST_STREAM <code>". PING, SETTINGS and WINDOW_UPDATE frames are left out unless a scenario records them.
type h2outcome struct {
	Streams  map[uint32][]string
	Data     map[uint32]int
	GoAway   []string
	Settings string
	Pinged   bool
	End      string
	Handler  string
}

// outcomeOf summarises frames in arrival order.
func outcomeOf(fs []h2frame) h2outcome {
	o := h2outcome{Streams: map[uint32][]string{}, Data: map[uint32]int{}}
	dataSeen := map[uint32]bool{}
	dataEnded := map[uint32]bool{}
	for _, f := range fs {
		switch f.Type {
		case h2Headers:
			ev := "HEADERS " + statusOf(f)
			if f.EndStream {
				ev += " ES"
			}
			o.Streams[f.Stream] = append(o.Streams[f.Stream], ev)
		case h2Data:
			o.Data[f.Stream] += f.DataLen
			if !dataSeen[f.Stream] {
				dataSeen[f.Stream] = true
				o.Streams[f.Stream] = append(o.Streams[f.Stream], "DATA")
			}
			if f.EndStream && !dataEnded[f.Stream] {
				dataEnded[f.Stream] = true
				o.Streams[f.Stream] = append(o.Streams[f.Stream], "DATA ES")
			}
		case h2RSTStream:
			o.Streams[f.Stream] = append(o.Streams[f.Stream], "RST_STREAM "+f.Code.String())
		case h2GoAway:
			o.GoAway = append(o.GoAway, fmt.Sprintf("%v last=%d", f.Code, f.LastStream))
		}
	}
	return o
}

// peerEnd reports how the connection of c ended, or "open" while the reader is still running.
func peerEnd(c *h2peer) string {
	select {
	case <-c.done:
		return endKind(c.endErr())
	default:
		return "open"
	}
}

// finish summarises fs and records whether the connection of c is still open.
func finish(c *h2peer, fs []h2frame) h2outcome {
	o := outcomeOf(fs)
	o.End = peerEnd(c)
	return o
}

// finishEnded waits for the server to close the connection of c, then summarises every frame received.
func finishEnded(c *h2peer, fs []h2frame) h2outcome {
	// The server closes the connection after its GOAWAY linger.
	<-c.done
	end := endKind(c.endErr())
	o := outcomeOf(append(fs, drain(c)...))
	o.End = end
	return o
}

// settingsLine renders the server SETTINGS in ID order followed by the connection WINDOW_UPDATE increment.
func settingsLine(s map[h2SettingID]uint32, incr uint32) string {
	var parts []string
	for _, id := range slices.Sorted(maps.Keys(s)) {
		parts = append(parts, fmt.Sprintf("%d=%d", id, s[id]))
	}
	return strings.Join(append(parts, fmt.Sprintf("WINDOW_UPDATE=%d", incr)), " ")
}

// pingFrame matches a PING frame that is not an acknowledgement.
func pingFrame(f h2frame) bool { return f.Type == h2Ping && !f.Ack }

// dataOrEnd matches the frame that gives stream id n DATA bytes, or the frame that ends stream id.
func dataOrEnd(id uint32, n int) func(h2frame) bool {
	total := 0
	return func(f h2frame) bool {
		if f.Stream == id && f.Type == h2Data {
			total += f.DataLen
		}
		return total >= n || streamEnded(id)(f)
	}
}

// connWindowUpdate matches a WINDOW_UPDATE frame on stream 0.
func connWindowUpdate(f h2frame) bool {
	return f.Type == h2WindowUpdate && f.Stream == 0
}

// h2BigProbe writes one 1 MiB response and records whether the write completed.
func h2BigProbe(p *probe) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write(make([]byte, 1<<20)); err != nil {
			p.set("write-error")
			return
		}
		p.set("complete")
	})
}

// h2TailProbe writes 65535 bytes and then 100 bytes, and records that the handler returned.
func h2TailProbe(p *probe) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(make([]byte, 65535))
		w.Write(make([]byte, 100))
		p.set("returned")
	})
}

// h2BlockedProbe serves /a, a 32 MiB response whose write deadline is cleared, and /b, a small response with a
// trailer. It records when /b has returned and when /a has completed.
func h2BlockedProbe(p *probe) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/a" {
			http.NewResponseController(w).SetWriteDeadline(time.Time{})
			if _, err := w.Write(make([]byte, 32<<20)); err != nil {
				p.set("a-write-error")
				return
			}
			p.set("a-complete")
			return
		}
		w.Header().Set("Trailer", "X-T")
		io.WriteString(w, "b")
		w.Header().Set("X-T", "t")
		p.set("b-returned")
	})
}
