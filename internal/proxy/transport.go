package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/health"
	"github.com/ok0/lychgate/internal/metrics"
	"github.com/ok0/lychgate/internal/upstream"
)

type headerTimeoutError struct{}

func (headerTimeoutError) Error() string   { return "upstream response header timeout" }
func (headerTimeoutError) Timeout() bool   { return true }
func (headerTimeoutError) Temporary() bool { return true }

var ErrResponseHeaderTimeout error = headerTimeoutError{}

const (
	headerWaiting int32 = iota
	headerReceived
	headerFired
)

type AttemptTransport struct {
	Upstream *upstream.Upstream
	Attempts *metrics.CounterVec
	Clock    clock.Clock
}

func (t *AttemptTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	h, err := t.Upstream.Acquire(req, nil)
	if err != nil {
		return nil, err
	}
	clk := t.Clock
	if clk == nil {
		clk = clock.Real()
	}
	out := attemptRequest(req, h)
	ctx, cancel := context.WithCancelCause(req.Context())

	var (
		state atomic.Int32
		mu    sync.Mutex
		timer clock.Timer
	)
	fire := func() {
		if state.CompareAndSwap(headerWaiting, headerFired) {
			cancel(ErrResponseHeaderTimeout)
		}
	}
	if rh := t.Upstream.ResponseHeaderTimeout; rh > 0 {
		trace := &httptrace.ClientTrace{
			WroteRequest: func(httptrace.WroteRequestInfo) {
				mu.Lock()
				defer mu.Unlock()
				if state.Load() != headerWaiting {
					return
				}
				if timer != nil {
					timer.Stop()
				}
				timer = clk.AfterFunc(rh, fire)
			},
		}
		ctx = httptrace.WithClientTrace(ctx, trace)
	}

	h.Begin()
	resp, err := t.Upstream.Transport.RoundTrip(out.WithContext(ctx))

	mu.Lock()
	if timer != nil {
		timer.Stop()
	}
	fired := !state.CompareAndSwap(headerWaiting, headerReceived)
	mu.Unlock()
	if fired && req.Context().Err() == nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		resp, err = nil, ErrResponseHeaderTimeout
	}

	o := health.Classify(req.Context(), resp, err)
	t.Upstream.Record(h, o)
	t.record(h, o.String())

	done := func() {
		cancel(nil)
		h.Done()
	}
	if err != nil {
		done()
		return nil, err
	}
	if resp.Body == nil {
		done()
		return resp, nil
	}
	once := new(sync.Once)
	if rwc, ok := resp.Body.(io.ReadWriteCloser); ok && resp.StatusCode == http.StatusSwitchingProtocols {
		resp.Body = &rwcBody{ReadWriteCloser: rwc, once: once, done: done}
	} else {
		resp.Body = &body{ReadCloser: resp.Body, once: once, done: done}
	}
	return resp, nil
}

func (t *AttemptTransport) record(h *upstream.Host, outcome string) {
	if t.Attempts != nil {
		t.Attempts.With(t.Upstream.Name, h.URL.Host, outcome).Inc()
	}
}

func attemptRequest(req *http.Request, h *upstream.Host) *http.Request {
	out := *req
	u := *req.URL
	u.Scheme = h.URL.Scheme
	u.Host = h.URL.Host
	if p := h.URL.Path; p != "" && p != "/" {
		u.Path = strings.TrimSuffix(p, "/") + u.Path
		if u.RawPath != "" {
			u.RawPath = strings.TrimSuffix(h.URL.EscapedPath(), "/") + u.RawPath
		}
	}
	out.URL = &u
	return &out
}

type body struct {
	io.ReadCloser
	once *sync.Once
	done func()
}

func (b *body) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.done)
	return err
}

type rwcBody struct {
	io.ReadWriteCloser
	once *sync.Once
	done func()
}

func (b *rwcBody) Close() error {
	err := b.ReadWriteCloser.Close()
	b.once.Do(b.done)
	return err
}
