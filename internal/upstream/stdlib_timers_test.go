package upstream

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
)

func TestCallerTraceGetsDialHooks(t *testing.T) {
	t.Run("connect", func(t *testing.T) {
		base, _ := backend(t, false, okHandler())
		hostPort := strings.TrimPrefix(base, "http://")

		var mu sync.Mutex
		var starts []string
		var dones []error
		outer := &httptrace.ClientTrace{
			ConnectStart: func(network, addr string) {
				mu.Lock()
				starts = append(starts, network+" "+addr)
				mu.Unlock()
			},
			ConnectDone: func(network, addr string, err error) {
				mu.Lock()
				dones = append(dones, err)
				mu.Unlock()
			},
		}
		tr := NewTransport(idleCfg(false), nil, clock.NewFake(testStart))
		t.Cleanup(func() { _ = tr.Close() })

		req, err := http.NewRequestWithContext(httptrace.WithClientTrace(context.Background(), outer), "GET", base+"/", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := tr.RoundTrip(req)
		if err != nil {
			t.Fatalf("RoundTrip: %v", err)
		}
		if got := readClose(t, resp); got != "ok" {
			t.Fatalf("body = %q, want %q", got, "ok")
		}

		mu.Lock()
		defer mu.Unlock()
		if len(starts) != 1 || starts[0] != "tcp "+hostPort {
			t.Fatalf("ConnectStart calls = %q, want one for %q", starts, "tcp "+hostPort)
		}
		if len(dones) != 1 || dones[0] != nil {
			t.Fatalf("ConnectDone calls = %v, want one with nil error", dones)
		}
	})

	t.Run("dns", func(t *testing.T) {
		var mu sync.Mutex
		var hosts []string
		var dones int
		outer := &httptrace.ClientTrace{
			DNSStart: func(info httptrace.DNSStartInfo) {
				mu.Lock()
				hosts = append(hosts, info.Host)
				mu.Unlock()
			},
			DNSDone: func(httptrace.DNSDoneInfo) {
				mu.Lock()
				dones++
				mu.Unlock()
			},
		}
		tr := NewTransport(idleCfg(false), nil, clock.NewFake(testStart))
		t.Cleanup(func() { _ = tr.Close() })

		req, err := http.NewRequestWithContext(httptrace.WithClientTrace(context.Background(), outer), "GET", "http://localhost/", nil)
		if err != nil {
			t.Fatal(err)
		}
		seen := false
		a := tr.newAttempt(req, outer, &seen)
		if _, err := net.DefaultResolver.LookupIPAddr(a.req.Context(), "localhost"); err != nil {
			t.Fatalf("LookupIPAddr: %v", err)
		}

		mu.Lock()
		defer mu.Unlock()
		if len(hosts) != 1 || hosts[0] != "localhost" {
			t.Fatalf("DNSStart hosts = %q, want [localhost]", hosts)
		}
		if dones != 1 {
			t.Fatalf("DNSDone calls = %d, want 1", dones)
		}
	})
}

func TestInnerTransportHasNoStdlibTimers(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		name := "http1"
		if h2 {
			name = "http2"
		}
		t.Run(name, func(t *testing.T) {
			tr := NewTransport(idleCfg(h2), nil, clock.NewFake(testStart))
			t.Cleanup(func() { _ = tr.Close() })
			in := tr.inner
			if in.TLSHandshakeTimeout != 0 {
				t.Errorf("TLSHandshakeTimeout = %v, want 0", in.TLSHandshakeTimeout)
			}
			if in.IdleConnTimeout != 0 {
				t.Errorf("IdleConnTimeout = %v, want 0", in.IdleConnTimeout)
			}
			if in.ResponseHeaderTimeout != 0 {
				t.Errorf("ResponseHeaderTimeout = %v, want 0", in.ResponseHeaderTimeout)
			}
			if in.ExpectContinueTimeout != 0 {
				t.Errorf("ExpectContinueTimeout = %v, want 0", in.ExpectContinueTimeout)
			}
			if in.Proxy != nil {
				t.Error("Proxy is set, want nil")
			}
			if in.HTTP2 != nil && (in.HTTP2.SendPingTimeout != 0 || in.HTTP2.WriteByteTimeout != 0) {
				t.Errorf("HTTP2 ping/write-byte timeouts = %v/%v, want 0/0", in.HTTP2.SendPingTimeout, in.HTTP2.WriteByteTimeout)
			}
		})
	}
}

// goAwayBackend starts a TLS HTTP/2 server that sends GOAWAY after each connection's stream.
func goAwayBackend(t *testing.T) (string, *tls.Config) {
	t.Helper()
	srv := httptest.NewUnstartedServer(okHandler())
	srv.EnableHTTP2 = true
	srv.Config.SetKeepAlivesEnabled(false)
	srv.TLS = &tls.Config{NextProtos: []string{"h2", "http/1.1"}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	return srv.URL, &tls.Config{RootCAs: pool}
}

func TestH2GoAwayNewRequestsWithoutBackoff(t *testing.T) {
	base, tlsCfg := goAwayBackend(t)
	conns := &connLog{}
	tr := newTransport(idleCfg(true), conns.dial, clock.NewFake(testStart), tlsCfg)
	t.Cleanup(func() { _ = tr.Close() })
	for i := range 5 {
		resp, err := doReq(t, tr, context.Background(), "GET", base+"/", nil, 0)
		if err != nil {
			t.Fatalf("request %d: RoundTrip: %v", i, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i, resp.StatusCode)
		}
		if got := readClose(t, resp); got != "ok" {
			t.Fatalf("request %d: body = %q, want %q", i, got, "ok")
		}
	}
}

func TestReuseTimersDecideOnlyReuse(t *testing.T) {
	t.Run("early-response", func(t *testing.T) {
		base, _ := backend(t, false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			_, _ = io.WriteString(w, "too large")
		}))
		tr := NewTransport(idleCfg(false), nil, clock.NewFake(testStart))
		t.Cleanup(func() { _ = tr.Close() })

		const size = 8 << 20
		resp, err := doReq(t, tr, context.Background(), "POST", base+"/", bytes.NewReader(make([]byte, size)), size)
		if err != nil {
			t.Fatalf("RoundTrip: %v", err)
		}
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413", resp.StatusCode)
		}
		if got := readClose(t, resp); got != "too large" {
			t.Fatalf("body = %q, want %q", got, "too large")
		}
	})

	t.Run("early-close", func(t *testing.T) {
		const size = 1 << 20
		payload := strings.Repeat("x", size)
		base, _ := backend(t, false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, payload)
		}))
		tr := NewTransport(idleCfg(false), nil, clock.NewFake(testStart))
		t.Cleanup(func() { _ = tr.Close() })

		resp, err := doReq(t, tr, context.Background(), "GET", base+"/", nil, 0)
		if err != nil {
			t.Fatalf("first RoundTrip: %v", err)
		}
		if _, err := io.ReadFull(resp.Body, make([]byte, 10)); err != nil {
			t.Fatalf("first read: %v", err)
		}
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("first body close: %v", err)
		}

		resp, err = doReq(t, tr, context.Background(), "GET", base+"/", nil, 0)
		if err != nil {
			t.Fatalf("second RoundTrip: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("second status = %d, want 200", resp.StatusCode)
		}
		if got := readClose(t, resp); got != payload {
			t.Fatalf("second body length = %d, want %d", len(got), len(payload))
		}
	})
}

// gatedBody yields data after its first Read waits for gate to be closed.
type gatedBody struct {
	gate <-chan struct{}
	data []byte
}

func (b *gatedBody) Read(p []byte) (int, error) {
	if b.gate != nil {
		<-b.gate
		b.gate = nil
	}
	if len(b.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, nil
}

func (b *gatedBody) Close() error { return nil }

func TestUnknownLengthBodyIndependentOfProbe(t *testing.T) {
	echo := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(w, r.Body)
	})
	for _, c := range []struct {
		name  string
		stall bool
	}{{"stalled", true}, {"prompt", false}} {
		t.Run(c.name, func(t *testing.T) {
			base, _ := backend(t, false, echo)
			tr := NewTransport(idleCfg(false), nil, clock.NewFake(testStart))
			t.Cleanup(func() { _ = tr.Close() })

			gate := make(chan struct{})
			if c.stall {
				go func() {
					// Real stall: 250ms exceeds the transport's 200ms body probe
					time.Sleep(250 * time.Millisecond)
					close(gate)
				}()
			} else {
				close(gate)
			}
			req, err := http.NewRequestWithContext(context.Background(), "GET", base+"/", &gatedBody{gate: gate, data: []byte("abc")})
			if err != nil {
				t.Fatal(err)
			}
			req.ContentLength = -1
			resp, err := tr.RoundTrip(req)
			if err != nil {
				t.Fatalf("RoundTrip: %v", err)
			}
			if got := readClose(t, resp); got != "abc" {
				t.Fatalf("echo = %q, want %q", got, "abc")
			}
		})
	}
}

// dnsAnswer answers one DNS query: type A gets 127.0.0.1, any other type gets an empty NOERROR answer.
func dnsAnswer(q []byte) []byte {
	end := 12
	for end < len(q) && q[end] != 0 {
		end += 1 + int(q[end])
	}
	qEnd := end + 5
	if qEnd > len(q) {
		return nil
	}
	qtype := binary.BigEndian.Uint16(q[end+1:])
	resp := make([]byte, 0, 64)
	resp = append(resp, q[0], q[1])
	resp = binary.BigEndian.AppendUint16(resp, 0x8180)
	resp = binary.BigEndian.AppendUint16(resp, 1)
	ancount := uint16(0)
	if qtype == 1 {
		ancount = 1
	}
	resp = binary.BigEndian.AppendUint16(resp, ancount)
	resp = append(resp, 0, 0, 0, 0)
	resp = append(resp, q[12:qEnd]...)
	if qtype == 1 {
		resp = append(resp, 0xC0, 0x0C)
		resp = binary.BigEndian.AppendUint16(resp, 1)
		resp = binary.BigEndian.AppendUint16(resp, 1)
		resp = binary.BigEndian.AppendUint32(resp, 60)
		resp = binary.BigEndian.AppendUint16(resp, 4)
		resp = append(resp, 127, 0, 0, 1)
	}
	return resp
}

// serveDNS answers length-prefixed queries on c until the peer closes it.
func serveDNS(c net.Conn) {
	defer func() { _ = c.Close() }()
	for {
		var lenBuf [2]byte
		if _, err := io.ReadFull(c, lenBuf[:]); err != nil {
			return
		}
		msg := make([]byte, binary.BigEndian.Uint16(lenBuf[:]))
		if _, err := io.ReadFull(c, msg); err != nil {
			return
		}
		// Real stall: 20ms before the reply
		time.Sleep(20 * time.Millisecond)
		resp := dnsAnswer(msg)
		out := binary.BigEndian.AppendUint16(nil, uint16(len(resp)))
		if _, err := c.Write(append(out, resp...)); err != nil {
			return
		}
	}
}

func TestResolverExchangeDeadlineHealthyServer(t *testing.T) {
	base, _ := backend(t, false, okHandler())
	_, port, err := net.SplitHostPort(strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}

	res := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			client, server := net.Pipe()
			go serveDNS(server)
			return client, nil
		},
	}

	var mu sync.Mutex
	var dialed []string
	recordingDial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		mu.Lock()
		dialed = append(dialed, addr)
		mu.Unlock()
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}

	conn, err := dialAddrs(context.Background(), res, recordingDial, clock.NewFake(testStart), "tcp", "upstream.test:"+port)
	if err != nil {
		t.Fatalf("dialAddrs: %v", err)
	}
	_ = conn.Close()

	mu.Lock()
	defer mu.Unlock()
	if want := "127.0.0.1:" + port; len(dialed) != 1 || dialed[0] != want {
		t.Fatalf("dialed = %q, want [%s]", dialed, want)
	}
}
