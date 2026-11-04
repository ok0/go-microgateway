package cache

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/coalesce"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/reqctx"
)

// errNotShared completes a call whose response cannot be shared; its waiters forward on their own.
var errNotShared = errors.New("cache: response not shared")

// sharedResponse is the response a leader shares with its waiters. header is the leader's response header without
// hop-by-hop fields and without fields set by outer stages. merged is non-nil when a 304 was merged into the stored
// entry, served with age. secondary is the leader's secondary key under the response's Vary. result is the request
// result, resultMiss or resultRevalidated. st is the leader's Cache-Status member, without Name.
type sharedResponse struct {
	status    int
	header    http.Header
	body      []byte
	merged    *httpcache.Entry
	age       time.Duration
	secondary string
	result    string
	st        httpcache.Status
}

// fetch serves a request through the coalescing group when one is set, and forwards it on its own otherwise.
func (s *stage) fetch(next http.Handler, w http.ResponseWriter, r *http.Request, in fillInput, ckey string) {
	if s.group == nil {
		s.forwardAlone(next, w, r, in)
		return
	}
	c, leader, ok := s.group.Acquire(ckey)
	switch {
	case !ok:
		s.logger.Debug("cache coalescing bound reached; request forwarded on its own", "route", s.route)
		s.forwardAlone(next, w, r, in)
	case leader:
		s.lead(c, next, w, r, in)
	default:
		s.await(c, next, w, r, in)
	}
}

// forwardAlone forwards the request under its own context and counts its result.
func (s *stage) forwardAlone(next http.Handler, w http.ResponseWriter, r *http.Request, in fillInput) {
	result := resultMiss
	defer func() { s.count(result) }()
	o := s.forward(next, w, r, in, true, &fetch{ctx: r.Context()})
	result = o.result
}

// detach returns a context that keeps the values of parent and drops its cancellation. It re-applies the deadline
// of parent on the stage clock and ends when stop ends.
func (s *stage) detach(parent, stop context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	unhook := func() bool { return true }
	if stop != nil {
		unhook = context.AfterFunc(stop, cancel)
	}
	if dl, ok := parent.Deadline(); ok {
		tctx, tcancel := clock.WithTimeout(ctx, s.clk, dl.Sub(s.clk.Now()))
		return tctx, func() {
			tcancel()
			unhook()
			cancel()
		}
	}
	return ctx, func() {
		unhook()
		cancel()
	}
}

// lead forwards the request as the leader of its call. The upstream request is detached from the client, so the
// response is stored and shared even when the client leaves.
func (s *stage) lead(c *coalesce.Call, next http.Handler, w http.ResponseWriter, r *http.Request, in fillInput) {
	ctx, cancel := s.detach(r.Context(), nil)
	defer cancel()
	f := &fetch{ctx: ctx, lead: true, call: c}
	result := resultMiss
	defer func() { s.count(result) }()
	pe := c.Lead(func() (any, error) {
		o := s.forward(next, w, r, in, true, f)
		result = o.result
		return o.completion()
	})
	if pe != nil {
		s.leaderPanic(w, r, in, f, pe)
	}
}

// leaderPanic answers a recovered leader panic. An abort is propagated as before; otherwise the panic is logged and,
// when no final header was sent, answered with 500.
func (s *stage) leaderPanic(w http.ResponseWriter, r *http.Request, in fillInput, f *fetch, pe *coalesce.PanicError) {
	if pe.Value == http.ErrAbortHandler {
		panic(http.ErrAbortHandler)
	}
	s.logger.Error("panic recovered", "panic", fmt.Sprint(pe.Value), "request_id", reqctx.RequestID(r.Context()), "stack", string(pe.Stack))
	if f.sent {
		panic(http.ErrAbortHandler)
	}
	appendCacheStatus(w.Header(), s.member(httpcache.Status{Fwd: in.fwd, Key: in.key}))
	httperr.Write(w, r, http.StatusInternalServerError, httperr.CodeInternal, httperr.Message(httperr.CodeInternal))
}

// await serves the request from the call it joined, or forwards it on its own when the call cannot be shared.
func (s *stage) await(c *coalesce.Call, next http.Handler, w http.ResponseWriter, r *http.Request, in fillInput) {
	v, err := c.Wait(r.Context(), s.waitTimeout)
	var pe *coalesce.PanicError
	switch {
	case err == nil:
		sh := v.(*sharedResponse)
		if s.serveShared(w, r, in, sh) {
			s.coalesced.Inc()
			s.count(sh.result)
			return
		}
		s.forwardAlone(next, w, r, in)
	case errors.Is(err, coalesce.ErrTimeout):
		in.forced, in.detail = true, detailCoalesceTimeout
		s.forwardAlone(next, w, r, in)
	case r.Context().Err() != nil && errors.Is(err, context.Canceled):
		s.count(resultMiss)
	case r.Context().Err() != nil && errors.Is(err, context.DeadlineExceeded):
		appendCacheStatus(w.Header(), s.member(httpcache.Status{Fwd: in.fwd, Key: in.key}))
		httperr.Write(w, r, http.StatusGatewayTimeout, httperr.CodeGatewayTimeout, httperr.Message(httperr.CodeGatewayTimeout))
		s.count(resultMiss)
	case errors.As(err, &pe) && pe.Value != http.ErrAbortHandler:
		appendCacheStatus(w.Header(), s.member(httpcache.Status{Fwd: in.fwd, Key: in.key, Collapsed: true}))
		httperr.Write(w, r, http.StatusInternalServerError, httperr.CodeInternal, httperr.Message(httperr.CodeInternal))
		s.count(resultMiss)
	default:
		s.forwardAlone(next, w, r, in)
	}
}

// completion returns the value and the error a leader publishes to its waiters.
func (o outcome) completion() (any, error) {
	if o.shared != nil {
		return o.shared, nil
	}
	return nil, errNotShared
}

// sharedHeader returns the header a waiter receives: the leader's header without the fields set by outer stages,
// keeping the origin's Age.
func sharedHeader(h, outer http.Header) http.Header {
	out := httpcache.StoredHeader(h)
	if vs := h.Values("Age"); len(vs) > 0 {
		out["Age"] = slices.Clone(vs)
	}
	dropOuter(out, outer)
	return out
}

// dropOuter removes fields that outer stages set before the forward and the origin left unchanged (for example the
// request ID header), so that they are neither stored nor shared.
func dropOuter(h, outer http.Header) {
	for k, vs := range outer {
		if slices.Equal(h[k], vs) {
			delete(h, k)
		}
	}
}

// share returns sh when its response can be shared among requests with the variant of r, or nil when it cannot.
func (s *stage) share(r *http.Request, sh *sharedResponse) *sharedResponse {
	h := sh.header
	if sh.merged != nil {
		h = sh.merged.Header
	}
	names, star := httpcache.VaryNames(h)
	if star {
		return nil
	}
	sec, ok := httpcache.SecondaryKey(names, r)
	if !ok {
		return nil
	}
	sh.secondary = sec
	return sh
}
