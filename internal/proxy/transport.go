package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/ok0/lychgate/internal/metrics"
	"github.com/ok0/lychgate/internal/upstream"
)

type AttemptTransport struct {
	Upstream *upstream.Upstream
	Attempts *metrics.CounterVec
}

func (t *AttemptTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	h, err := t.Upstream.Acquire(req, nil)
	if err != nil {
		return nil, err
	}
	out := attemptRequest(req, h)
	h.Begin()
	resp, err := t.Upstream.Transport.RoundTrip(out)
	t.record(h, attemptOutcome(req, resp, err))
	if err != nil {
		h.Done()
		return nil, err
	}
	if resp.Body == nil {
		h.Done()
		return resp, nil
	}
	once := new(sync.Once)
	if rwc, ok := resp.Body.(io.ReadWriteCloser); ok && resp.StatusCode == http.StatusSwitchingProtocols {
		resp.Body = &rwcBody{ReadWriteCloser: rwc, once: once, done: h.Done}
	} else {
		resp.Body = &body{ReadCloser: resp.Body, once: once, done: h.Done}
	}
	return resp, nil
}

func (t *AttemptTransport) record(h *upstream.Host, outcome string) {
	if t.Attempts != nil {
		t.Attempts.With(t.Upstream.Name, h.URL.Host, outcome).Inc()
	}
}

func attemptOutcome(req *http.Request, resp *http.Response, err error) string {
	if err == nil {
		if resp.StatusCode >= 500 {
			return "status_5xx"
		}
		return "success"
	}
	switch req.Context().Err() {
	case context.Canceled:
		return "canceled"
	case context.DeadlineExceeded:
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	return "error"
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
