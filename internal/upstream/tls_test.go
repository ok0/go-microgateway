package upstream

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/health"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/testwait"
)

// blackHole listens on loopback and accepts connections that never receive a byte. It returns the address.
func blackHole(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range held {
			_ = c.Close()
		}
	})
	return ln.Addr().String()
}

func handshakeCfg(limit time.Duration) config.Upstream {
	return config.Upstream{
		Name: "u",
		Timeouts: config.Timeouts{
			Connect:        dur(time.Hour),
			TLSHandshake:   dur(limit),
			ResponseHeader: dur(time.Hour),
		},
	}
}

func checkHandshakeTimeout(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, errHandshakeTimeout) {
		t.Fatalf("err = %v, want errHandshakeTimeout", err)
	}
	var oe *net.OpError
	if errors.As(err, &oe) && oe.Op == "dial" {
		t.Fatalf("err = %v, must not be a dial error", err)
	}
	if status, _ := httperr.FromError(err); status != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", status)
	}
	if got := health.Classify(context.Background(), nil, err); got != health.Failure {
		t.Fatalf("health outcome = %v, want failure", got)
	}
}

func TestTLSHandshakeTimeoutOnClock(t *testing.T) {
	t.Run("fake", func(t *testing.T) {
		const limit = 5 * time.Second
		addr := blackHole(t)
		fake := clock.NewFake(testStart)
		tr := newTransport(handshakeCfg(limit), nil, fake, nil)
		t.Cleanup(func() { _ = tr.Close() })

		ch := startReq(t, tr, context.Background(), "GET", "https://"+addr+"/", nil, 0)
		dueWithin(t, fake, 1, limit, "handshake timer armed")
		if n := fake.Waiters(); n != 1 {
			t.Fatalf("Waiters() = %d, want 1 (only the handshake timer)", n)
		}
		if n := fake.Due(limit - time.Nanosecond); n != 0 {
			t.Fatalf("Due(limit-1ns) = %d, want 0", n)
		}

		fake.Advance(limit - time.Nanosecond)
		select {
		case r := <-ch:
			t.Fatalf("RoundTrip returned before the handshake limit: %v", r.err)
		default:
		}

		fake.Advance(time.Nanosecond)
		r, _ := testwait.Recv(t, "RoundTrip to return at the handshake limit", ch, nil)
		checkHandshakeTimeout(t, r.err)
		if n := fake.Waiters(); n != 0 {
			t.Fatalf("Waiters() = %d after the limit, want 0", n)
		}
	})

	t.Run("real", func(t *testing.T) {
		addr := blackHole(t)
		tr := newTransport(handshakeCfg(20*time.Millisecond), nil, clock.Real(), nil)
		t.Cleanup(func() { _ = tr.Close() })

		// Real clock: a peer that never answers the handshake must hit the 20ms limit on the production clock.
		ch := startReq(t, tr, context.Background(), "GET", "https://"+addr+"/", nil, 0)
		r, _ := testwait.Recv(t, "RoundTrip to return after the real handshake limit", ch, nil)
		checkHandshakeTimeout(t, r.err)
	})
}

func TestTLSHandshakeCompletesWithoutArmedTimer(t *testing.T) {
	fake := clock.NewFake(testStart)
	url, tc := tlsBackend(t, okHandler())
	tr := newTransport(idleCfg(false), nil, fake, tc)
	t.Cleanup(func() { _ = tr.Close() })

	resp, err := doReq(t, tr, context.Background(), "GET", url+"/", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := readClose(t, resp); got != "ok" {
		t.Fatalf("body = %q, want ok", got)
	}
	checkTimers(t, fake, 0, 1)

	_ = tr.Close()
	if n := fake.HousekeepingWaiters(); n != 0 {
		t.Fatalf("HousekeepingWaiters() after Close = %d, want 0", n)
	}
}

func TestTLSNegotiatesProtocol(t *testing.T) {
	for _, c := range []struct {
		name string
		h2   bool
	}{{"h2", true}, {"http1", false}} {
		t.Run(c.name, func(t *testing.T) {
			url, tc := tlsBackend(t, okHandler())
			tr := newTransport(idleCfg(c.h2), nil, clock.NewFake(testStart), tc)
			t.Cleanup(func() { _ = tr.Close() })

			resp, err := doReq(t, tr, context.Background(), "GET", url+"/", nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			if got := readClose(t, resp); got != "ok" {
				t.Fatalf("body = %q, want ok", got)
			}
			if resp.TLS == nil {
				t.Fatal("resp.TLS = nil, want the TLS connection state")
			}
			if c.h2 {
				if resp.ProtoMajor != 2 || resp.TLS.NegotiatedProtocol != "h2" {
					t.Fatalf("proto = %d, ALPN = %q; want HTTP/2 over h2", resp.ProtoMajor, resp.TLS.NegotiatedProtocol)
				}
			} else {
				if resp.ProtoMajor != 1 || resp.TLS.NegotiatedProtocol != "http/1.1" {
					t.Fatalf("proto = %d, ALPN = %q; want HTTP/1.1 over http/1.1", resp.ProtoMajor, resp.TLS.NegotiatedProtocol)
				}
			}
		})
	}
}
