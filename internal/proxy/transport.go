package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ok0/go-microgateway/internal/breaker"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/health"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/retry"
	"github.com/ok0/go-microgateway/internal/upstream"
)

type headerTimeoutError struct{}

func (headerTimeoutError) Error() string   { return "upstream response header timeout" }
func (headerTimeoutError) Timeout() bool   { return true }
func (headerTimeoutError) Temporary() bool { return true }

// ErrResponseHeaderTimeout is the error of an attempt whose response headers did not arrive within the upstream response header timeout.
var ErrResponseHeaderTimeout error = headerTimeoutError{}

const (
	headerWaiting int32 = iota
	headerReceived
	headerFired
)

// AttemptTransport is the http.RoundTripper of a proxied route. It acquires an upstream host for each request, makes the attempts that the route's retry policy allows, and records the outcome of every attempt. A nil Clock selects the real clock, and a nil Rand is replaced by a source with a fixed seed.
type AttemptTransport struct {
	Upstream     *upstream.Upstream
	Attempts     *metrics.CounterVec
	Clock        clock.Clock
	Rand         clock.Rand
	HostBreakers bool
}

// RoundTrip performs the request through the upstream host selected for it and retries as the retry settings in the request context allow. It returns the response or error of the final attempt.
func (t *AttemptTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clk := t.Clock
	if clk == nil {
		clk = clock.Real()
	}
	rr := retry.FromContext(req.Context())
	h, tok, err := t.Upstream.Acquire(req, nil, t.HostBreakers)
	if err != nil {
		return nil, err
	}
	if rr == nil {
		res := t.attempt(req, h, tok, clk, nil)
		return res.resp, res.err
	}
	rnd := t.Rand
	if rnd == nil {
		rnd = clock.NewRand(1)
	}
	var tried map[*upstream.Host]bool
	for n := 1; ; n++ {
		res := t.attempt(req, h, tok, clk, rr)
		if tried == nil {
			tried = make(map[*upstream.Host]bool)
		}
		tried[h] = true
		status := 0
		if res.resp != nil {
			status = res.resp.StatusCode
		}
		if !rr.Policy.ShouldRetry(retry.Attempt{
			N:             n,
			Method:        req.Method,
			Status:        status,
			HasIdemKey:    rr.HasIdemKey,
			Replayable:    rr.Replayable,
			Sent:          res.sent,
			ConnectErr:    res.connectErr,
			HeaderTimeout: res.headerTimeout,
			Canceled:      res.reqEnded || req.Context().Err() != nil,
		}) {
			return res.resp, res.err
		}
		d := rr.Policy.Backoff(n+1, rnd)
		if !retry.Fits(req.Context(), clk, d) {
			return res.resp, res.err
		}
		slot, ok := rr.Budget.Withdraw()
		if !ok {
			rr.Exhausted()
			return res.resp, res.err
		}
		if err := retry.Sleep(req.Context(), clk, d); err != nil {
			rr.Budget.Refund(slot)
			discard(res.resp)
			return nil, err
		}
		next, nextTok, err := t.Upstream.Acquire(req, tried, t.HostBreakers)
		if err != nil {
			rr.Budget.Refund(slot)
			return res.resp, res.err
		}
		discard(res.resp)
		rr.Retried()
		h, tok = next, nextTok
	}
}

type attemptResult struct {
	resp          *http.Response
	err           error
	sent          bool
	connectErr    bool
	headerTimeout bool
	reqEnded      bool
}

// requestEnded reports how the request context has ended: its own error once it is done, or
// context.DeadlineExceeded once clk has reached its deadline before the context's own timer ran.
func requestEnded(ctx context.Context, clk clock.Clock) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d, ok := ctx.Deadline(); ok && !clk.Now().Before(d) {
		return context.DeadlineExceeded
	}
	return nil
}

type countingBody struct {
	r io.ReadCloser
	n atomic.Int64
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n.Add(int64(n))
	return n, err
}

func (b *countingBody) Close() error { return nil }

func (t *AttemptTransport) attempt(req *http.Request, h *upstream.Host, tok breaker.Token, clk clock.Clock, rr *retry.Request) attemptResult {
	out := attemptRequest(req, h)
	var cb *countingBody
	if rr != nil && req.Body != nil {
		if rr.Replayable {
			if body := rr.NewBody(); body != nil {
				out.Body = body
			}
		} else {
			cb = &countingBody{r: req.Body}
			out.Body = cb
		}
	}
	ctx, cancel := context.WithCancelCause(req.Context())

	var (
		state atomic.Int32
		mu    sync.Mutex
		timer clock.Timer
		wrote atomic.Bool
	)
	fire := func() {
		if state.CompareAndSwap(headerWaiting, headerFired) {
			cancel(ErrResponseHeaderTimeout)
		}
	}
	rh := t.Upstream.ResponseHeaderTimeout
	if rh > 0 || rr != nil {
		trace := &httptrace.ClientTrace{}
		if rr != nil {
			trace.WroteHeaders = func() { wrote.Store(true) }
		}
		if rh > 0 {
			trace.WroteRequest = func(httptrace.WroteRequestInfo) {
				mu.Lock()
				defer mu.Unlock()
				if state.Load() != headerWaiting {
					return
				}
				if timer != nil {
					timer.Stop()
				}
				timer = clk.AfterFunc(rh, fire)
			}
		}
		ctx = httptrace.WithClientTrace(ctx, trace)
	}

	h.Begin()
	resp, err := t.Upstream.Transport.RoundTrip(out.WithContext(ctx))

	mu.Lock()
	if timer != nil {
		timer.Stop()
	}
	// The outcome is fixed by state, not by which error RoundTrip saw first: headers returned before the timer fired win; otherwise the request's own end (client cancel or route deadline) wins over the header timeout.
	received := state.CompareAndSwap(headerWaiting, headerReceived)
	mu.Unlock()
	var ended error
	if !received {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		resp, err = nil, ErrResponseHeaderTimeout
		if ended = requestEnded(req.Context(), clk); ended != nil {
			err = ended
		}
	}

	res := attemptResult{
		sent:          wrote.Load() || (cb != nil && cb.n.Load() > 0),
		headerTimeout: err == ErrResponseHeaderTimeout,
		reqEnded:      ended != nil,
	}
	var oe *net.OpError
	res.connectErr = errors.As(err, &oe) && oe.Op == "dial"

	o := health.Classify(req.Context(), resp, err)
	t.Upstream.Record(h, tok, o)
	t.record(h, o.String())

	done := func() {
		cancel(nil)
		h.Done()
	}
	if err != nil {
		done()
		res.err = err
		return res
	}
	if resp.Body == nil {
		done()
		res.resp = resp
		return res
	}
	once := new(sync.Once)
	if rwc, ok := resp.Body.(io.ReadWriteCloser); ok && resp.StatusCode == http.StatusSwitchingProtocols {
		resp.Body = &rwcBody{ReadWriteCloser: rwc, once: once, done: done}
	} else {
		resp.Body = &body{ReadCloser: resp.Body, once: once, done: done}
	}
	res.resp = resp
	return res
}

func discard(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	if 0 <= resp.ContentLength && resp.ContentLength <= 4096 {
		_, _ = io.CopyN(io.Discard, resp.Body, 4096)
	}
	_ = resp.Body.Close()
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
