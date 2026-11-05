package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ok0/go-microgateway/internal/testcert"
)

const (
	seqH = time.Second
	seqR = 2 * time.Second
	seqW = 3 * time.Second
	seqI = 4 * time.Second
)

type seqLimits struct {
	h, r, w, i bool
}

type seqTrace struct {
	mu   sync.Mutex
	toks []string
}

func (tr *seqTrace) add(tok string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if n := len(tr.toks); tok == "read" && n > 0 && tr.toks[n-1] == "read" {
		return
	}
	tr.toks = append(tr.toks, tok)
}

func (tr *seqTrace) list() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return slices.Clone(tr.toks)
}

type seqConn struct {
	net.Conn
	tr *seqTrace
}

func (c *seqConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	switch {
	case n > 0:
		c.tr.add("read")
	case errors.Is(err, io.EOF):
		c.tr.add("eof")
	case err != nil:
		c.tr.add("rerr")
	}
	return n, err
}

func (c *seqConn) Close() error {
	c.tr.add("close")
	return c.Conn.Close()
}

func (c *seqConn) SetReadDeadline(t time.Time) error {
	c.tr.add("rd" + seqValue(t))
	return c.Conn.SetReadDeadline(t)
}

func (c *seqConn) SetWriteDeadline(t time.Time) error {
	c.tr.add("wr" + seqValue(t))
	return c.Conn.SetWriteDeadline(t)
}

func (c *seqConn) SetDeadline(t time.Time) error {
	c.tr.add("dl" + seqValue(t))
	return c.Conn.SetDeadline(t)
}

func seqValue(t time.Time) string {
	switch {
	case t.IsZero():
		return "0"
	case !t.After(time.Unix(1, 0)):
		return "-"
	}
	d := t.Sub(time.Now())
	switch {
	case d > 0:
		return "+" + d.String()
	case d == 0:
		return "="
	default:
		return "<"
	}
}

type seqListener struct {
	net.Listener
	pool   *x509.CertPool
	mu     sync.Mutex
	traces []*seqTrace
}

func (l *seqListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tr := &seqTrace{}
	l.mu.Lock()
	l.traces = append(l.traces, tr)
	l.mu.Unlock()
	return &seqConn{Conn: c, tr: tr}, nil
}

func (l *seqListener) last() *seqTrace {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.traces) == 0 {
		return nil
	}
	return l.traces[len(l.traces)-1]
}

func seqState(c net.Conn, st http.ConnState) {
	if tc, ok := c.(*tls.Conn); ok {
		c = tc.NetConn()
	}
	if sc, ok := c.(*seqConn); ok {
		sc.tr.add(st.String())
	}
}

func seqOn(on bool, d time.Duration) time.Duration {
	if on {
		return d
	}
	return 0
}

func seqFirst(a, b time.Duration) time.Duration {
	if a > 0 {
		return a
	}
	return b
}

func seqSmallest(vals ...time.Duration) time.Duration {
	var d time.Duration
	for _, v := range vals {
		if v > 0 && (d == 0 || v < d) {
			d = v
		}
	}
	return d
}

func seqV(d time.Duration) string {
	if d > 0 {
		return "+" + d.String()
	}
	return "0"
}

func seqOnOff(on bool) string {
	if on {
		return "on"
	}
	return "off"
}

func seqName(lim seqLimits) string {
	return fmt.Sprintf("h-%s_r-%s_w-%s_i-%s", seqOnOff(lim.h), seqOnOff(lim.r), seqOnOff(lim.w), seqOnOff(lim.i))
}

func seqMerge(toks []string) []string {
	var out []string
	for _, tok := range toks {
		if tok == "read" && len(out) > 0 && out[len(out)-1] == "read" {
			continue
		}
		out = append(out, tok)
	}
	return out
}

func seqWant(kind string, lim seqLimits, script string) []string {
	rv := seqOn(lim.r, seqR)
	wv := seqOn(lim.w, seqW)
	hdr := seqFirst(seqOn(lim.h, seqH), rv)
	idl := seqFirst(seqOn(lim.i, seqI), rv)
	hs := seqSmallest(seqOn(lim.h, seqH), rv, wv)
	var want []string
	add := func(toks ...string) { want = append(want, toks...) }

	if script == "entry" {
		add("new")
		if kind == "tls" {
			if hs > 0 {
				add("rd"+seqV(hs), "wr"+seqV(hs))
			}
			add("read")
			if hs > 0 {
				add("rd0", "wr0")
			}
			add("rd0", "wr0")
		} else {
			if hdr > 0 {
				add("rd" + seqV(hdr))
			}
			add("read", "rd0", "wr0")
		}
		return seqMerge(want)
	}

	add("new")
	if kind == "tls" {
		if hs > 0 {
			add("rd"+seqV(hs), "wr"+seqV(hs))
		}
		add("read")
		if hs > 0 {
			add("rd0", "wr0")
		}
	}
	if hdr > 0 {
		add("rd" + seqV(hdr))
	}
	switch script {
	case "keepalive":
		add("read", "rd"+seqV(rv))
		if wv > 0 {
			add("wr" + seqV(wv))
		}
		add("active", "rd0", "rd-", "rerr", "rd0", "wr0", "idle", "rd"+seqV(idl))
		add("read", "rd"+seqV(hdr), "rd"+seqV(rv))
		if wv > 0 {
			add("wr" + seqV(wv))
		}
		add("active", "rd0", "rd-", "rerr", "rd0", "wr0", "idle", "rd"+seqV(idl))
		if kind == "tls" {
			add("read", "wr+5s", "wr=", "close", "closed")
		} else {
			add("eof", "close", "closed")
		}
	case "hijack":
		add("read", "rd"+seqV(rv))
		if wv > 0 {
			add("wr" + seqV(wv))
		}
		add("active", "rd0", "rd-", "rerr", "rd0", "dl0", "hijacked")
		if kind == "tls" {
			add("wr+5s", "wr=", "close")
		} else {
			add("close")
		}
	}
	return seqMerge(want)
}

func seqTLS(t *testing.T) (*tls.Config, *x509.CertPool) {
	t.Helper()
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{pair},
		NextProtos:   []string{"h2", "http/1.1"},
		MinVersion:   tls.VersionTLS12,
	}
	return cfg, pool
}

func seqStock(t *testing.T, kind string, lim seqLimits, h http.Handler) (string, *seqListener, *memNet) {
	t.Helper()
	m := newMemNet(64 << 10)
	t.Cleanup(m.close)
	raw, err := m.listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := &seqListener{Listener: raw}
	var protos http.Protocols
	protos.SetHTTP1(true)
	srv := &http.Server{
		Handler:           h,
		ConnState:         seqState,
		HTTP2:             &http.HTTP2Config{},
		ReadHeaderTimeout: seqOn(lim.h, seqH),
		ReadTimeout:       seqOn(lim.r, seqR),
		WriteTimeout:      seqOn(lim.w, seqW),
		IdleTimeout:       seqOn(lim.i, seqI),
	}
	var srvLn net.Listener = ln
	switch kind {
	case "h2c":
		protos.SetUnencryptedHTTP2(true)
	case "tls":
		protos.SetHTTP2(true)
		cfg, pool := seqTLS(t)
		ln.pool = pool
		srv.TLSConfig = cfg
		srvLn = tls.NewListener(ln, cfg)
	}
	srv.Protocols = &protos
	go srv.Serve(srvLn)
	t.Cleanup(func() { srv.Close() })
	return raw.Addr().String(), ln, m
}

func seqHandler(script string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if script == "hijack" {
			conn, buf, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			io.WriteString(buf, "HTTP/1.1 101 Switching Protocols\r\n\r\n")
			buf.Flush()
			conn.Close()
			return
		}
		io.ReadAll(r.Body)
		io.WriteString(w, "ok")
	})
}

func seqClient(raw net.Conn, kind string, pool *x509.CertPool, proto string) (net.Conn, error) {
	if kind != "tls" {
		return raw, nil
	}
	tc := tls.Client(raw, &tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{proto}})
	if err := tc.Handshake(); err != nil {
		return nil, err
	}
	// Waiting after the handshake lets the server finish its handshake read before any request byte exists,
	// so the request arrives in a read of its own.
	synctest.Wait()
	return tc, nil
}

func seqReadOK(br *bufio.Reader) error {
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		return err
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK || string(body) != "ok" {
		return fmt.Errorf("response %d %q", resp.StatusCode, body)
	}
	return nil
}

func seqKeepalive(conn net.Conn) error {
	br := bufio.NewReader(conn)
	if _, err := io.WriteString(conn, "POST /a HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\nhi"); err != nil {
		return err
	}
	if err := seqReadOK(br); err != nil {
		return err
	}
	synctest.Wait()
	if _, err := io.WriteString(conn, "GET /b HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
		return err
	}
	if err := seqReadOK(br); err != nil {
		return err
	}
	synctest.Wait()
	return conn.Close()
}

func seqHijack(conn net.Conn) error {
	if _, err := io.WriteString(conn, "GET /h HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
		return err
	}
	b, err := io.ReadAll(conn)
	if err != nil {
		return err
	}
	if string(b) != "HTTP/1.1 101 Switching Protocols\r\n\r\n" {
		return fmt.Errorf("hijacked response %q", b)
	}
	return nil
}

func seqEntry(t *testing.T, raw net.Conn, pool *x509.CertPool) error {
	p, err := openH2Conn(raw, pool)
	if err != nil {
		return err
	}
	p.t = t
	p.bubble = true
	defer p.close()
	p.handshake(true)
	p.headers(1, "GET", "/", true)
	p.until("stream 1 ended", streamEnded(1))
	p.close()
	return nil
}

func seqCase(t *testing.T, kind, script string, lim seqLimits) {
	addr, ln, m := seqStock(t, kind, lim, seqHandler(script))
	raw, err := m.dial(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	guard := time.AfterFunc(bubbleGuard, func() { raw.Close() })
	switch script {
	case "keepalive":
		var conn net.Conn
		if conn, err = seqClient(raw, kind, ln.pool, "http/1.1"); err == nil {
			err = seqKeepalive(conn)
		}
	case "hijack":
		var conn net.Conn
		if conn, err = seqClient(raw, kind, ln.pool, "http/1.1"); err == nil {
			err = seqHijack(conn)
		}
	case "entry":
		var pool *x509.CertPool
		if kind == "tls" {
			pool = ln.pool
		}
		err = seqEntry(t, raw, pool)
	}
	if !guard.Stop() {
		t.Fatalf("hang guard: %s/%s script still running after %v of bubble time\ngoroutines:\n%s", kind, script, bubbleGuard, bubbleStacks())
	}
	if err != nil {
		t.Fatal(err)
	}
	synctest.Wait()
	tr := ln.last()
	if tr == nil {
		t.Fatal("no connection was accepted")
	}
	got := tr.list()
	want := seqWant(kind, lim, script)
	t.Logf("trace: %q", got)
	if script == "entry" && len(got) > len(want) {
		got = got[:len(want)]
	}
	if !slices.Equal(got, want) {
		t.Errorf("trace mismatch\nwant %q\ngot  %q", want, got)
	}
}

func TestHTTP1DeadlineCallSequence(t *testing.T) {
	if !strings.HasPrefix(runtime.Version(), "go1.27") {
		t.Fatalf("the call-sequence pins need go1.27, running %s", runtime.Version())
	}
	pairs := []struct{ kind, script string }{
		{"plain", "keepalive"},
		{"h2c", "keepalive"},
		{"tls", "keepalive"},
		{"plain", "hijack"},
		{"tls", "hijack"},
		{"h2c", "entry"},
		{"tls", "entry"},
	}
	for _, p := range pairs {
		for bits := 0; bits < 16; bits++ {
			lim := seqLimits{h: bits&1 != 0, r: bits&2 != 0, w: bits&4 != 0, i: bits&8 != 0}
			t.Run(p.kind+"/"+p.script+"/"+seqName(lim), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					seqCase(t, p.kind, p.script, lim)
				})
			})
		}
	}
}
