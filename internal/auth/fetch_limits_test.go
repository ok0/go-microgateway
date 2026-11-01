package auth

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testwait"
)

type lookupResult struct {
	key Key
	err error
}

func startLookup(t *testing.T, j *JWKS, kid string) <-chan lookupResult {
	t.Helper()
	ch := make(chan lookupResult, 1)
	go func() {
		k, err := j.Lookup(t.Context(), kid)
		ch <- lookupResult{key: k, err: err}
	}()
	return ch
}

func assertNoLookupResult(t *testing.T, ch <-chan lookupResult) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("Lookup returned before the timeout: err=%v", r.err)
	default:
	}
}

func newFetchLimitJWKS(t *testing.T, f Fetcher, url string, clk clock.Clock, logs *syncBuffer) *JWKS {
	t.Helper()
	cfg := testJWKSConfig()
	cfg.URL = url
	deps := JWKSDeps{Fetcher: f, Clock: clk}
	if logs != nil {
		deps.Logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	j, err := NewJWKS(cfg, deps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	return j
}

func stallServer(t *testing.T, head func(http.ResponseWriter)) (string, <-chan struct{}) {
	t.Helper()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if head != nil {
			head(w)
		}
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	return srv.URL + "/jwks", entered
}

func awaitFetchTimeout(t *testing.T, clk *clock.Fake, res <-chan lookupResult, entered <-chan struct{}, logs *syncBuffer) {
	t.Helper()
	testwait.Done(t, "fetch reached its stall point", entered, nil)
	testwait.Until(t, "key set timeout timer armed", func() bool { return clk.Waiters() == 1 }, nil)
	clk.Advance(testFetchTimeout - time.Nanosecond)
	if n := clk.Waiters(); n != 1 {
		t.Fatalf("waiters before the timeout = %d, want 1", n)
	}
	assertNoLookupResult(t, res)
	clk.Advance(time.Nanosecond)
	r, _ := testwait.Recv(t, "Lookup result after the timeout", res, nil)
	if !errors.Is(r.err, ErrKeysUnavailable) {
		t.Fatalf("Lookup err = %v, want ErrKeysUnavailable", r.err)
	}
	if !strings.Contains(logs.String(), "key set fetch timed out after 5s") {
		t.Fatalf("logs do not record the timeout: %q", logs.String())
	}
}

func TestDefaultFetcherTransportHasNoTimers(t *testing.T) {
	f := NewHTTPFetcher(nil).(*httpFetcher)
	if f.client.Timeout != 0 {
		t.Fatalf("client.Timeout = %v, want 0", f.client.Timeout)
	}
	tr, ok := f.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T, want *http.Transport", f.client.Transport)
	}
	if tr.TLSHandshakeTimeout != 0 || tr.IdleConnTimeout != 0 || tr.ResponseHeaderTimeout != 0 || tr.ExpectContinueTimeout != 0 {
		t.Fatalf("transport timeouts = %v %v %v %v, want all 0",
			tr.TLSHandshakeTimeout, tr.IdleConnTimeout, tr.ResponseHeaderTimeout, tr.ExpectContinueTimeout)
	}
	if !tr.DisableKeepAlives {
		t.Fatal("DisableKeepAlives = false, want true")
	}
	if f.dialer == nil || f.dialer.Timeout != 0 || !f.dialer.Deadline.IsZero() {
		t.Fatalf("dialer = %+v, want a dialer without timeout or deadline", f.dialer)
	}
}

func TestDefaultFetcherBoundedByJWKSTimeout(t *testing.T) {
	t.Run("connect", func(t *testing.T) {
		clk := clock.NewFake(testNow)
		logs := &syncBuffer{}
		f := NewHTTPFetcher(nil).(*httpFetcher)
		var hasDeadline atomic.Bool
		entered := make(chan struct{}, 1)
		f.client.Transport.(*http.Transport).DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			_, ok := ctx.Deadline()
			hasDeadline.Store(ok)
			entered <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		j := newFetchLimitJWKS(t, f, "https://idp.example/jwks", clk, logs)
		res := startLookup(t, j, "kid1")
		awaitFetchTimeout(t, clk, res, entered, logs)
		if hasDeadline.Load() {
			t.Fatal("dial context has a deadline, want none")
		}
	})

	t.Run("handshake", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		accepted := make(chan net.Conn, 1)
		entered := make(chan struct{}, 1)
		go func() {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- c
			entered <- struct{}{}
		}()
		t.Cleanup(func() {
			select {
			case c := <-accepted:
				_ = c.Close()
			default:
			}
		})
		clk := clock.NewFake(testNow)
		logs := &syncBuffer{}
		j := newFetchLimitJWKS(t, NewHTTPFetcher(nil), "https://"+ln.Addr().String()+"/jwks", clk, logs)
		res := startLookup(t, j, "kid1")
		awaitFetchTimeout(t, clk, res, entered, logs)
	})

	t.Run("headers", func(t *testing.T) {
		url, entered := stallServer(t, nil)
		clk := clock.NewFake(testNow)
		logs := &syncBuffer{}
		j := newFetchLimitJWKS(t, NewHTTPFetcher(nil), url, clk, logs)
		res := startLookup(t, j, "kid1")
		awaitFetchTimeout(t, clk, res, entered, logs)
	})

	t.Run("body", func(t *testing.T) {
		url, entered := stallServer(t, func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"keys":`)
			w.(http.Flusher).Flush()
		})
		clk := clock.NewFake(testNow)
		logs := &syncBuffer{}
		j := newFetchLimitJWKS(t, NewHTTPFetcher(nil), url, clk, logs)
		res := startLookup(t, j, "kid1")
		awaitFetchTimeout(t, clk, res, entered, logs)
	})
}

func TestDefaultFetcherHealthyAfterStall(t *testing.T) {
	ks := loadKeyset()
	body := jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey))
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	clk := clock.NewFake(testNow)
	j := newFetchLimitJWKS(t, NewHTTPFetcher(nil), srv.URL+"/jwks", clk, nil)
	res := startLookup(t, j, "rs256")
	testwait.Done(t, "key set request entered", entered, nil)
	close(release)
	r, _ := testwait.Recv(t, "Lookup result", res, nil)
	if r.err != nil {
		t.Fatalf("Lookup: %v", r.err)
	}
	if r.key.Algorithm != "RS256" {
		t.Fatalf("algorithm = %q, want RS256", r.key.Algorithm)
	}
}

func TestCustomFetcherClientUnchanged(t *testing.T) {
	c := &http.Client{}
	f := NewHTTPFetcher(c).(*httpFetcher)
	if f.client != c || f.dialer != nil {
		t.Fatalf("custom client was altered: client same=%v dialer=%v", f.client == c, f.dialer)
	}
}
