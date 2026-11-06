// Package proxy forwards a matched request to its upstream through httputil.ReverseProxy.
package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/internal/upstream"
)

// RouteSpec holds the path rewrite, header, host and forwarding settings applied to each request and response of a route.
type RouteSpec struct {
	Name            string
	StripPrefix     string
	ReplaceFrom     string
	ReplaceTo       string
	RequestHeaders  config.HeaderOps
	ResponseHeaders config.HeaderOps
	PreserveHost    bool
	RequestIDHeader string
	Trusted         *reqctx.TrustedProxies
}

// Deps supplies the collaborators of a proxy handler. A nil Logger discards logs, and HostBreakers enables the per-host circuit breakers.
type Deps struct {
	Logger       *slog.Logger
	Attempts     *metrics.CounterVec
	Clock        clock.Clock
	Rand         clock.Rand
	HostBreakers bool
}

// New returns the reverse-proxy handler of a route that forwards to up. Each request is rewritten according to spec, each upstream attempt goes through an AttemptTransport, and upstream errors are answered with the gateway error codes.
func New(spec RouteSpec, up *upstream.Upstream, deps Deps) http.Handler {
	logger := deps.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &httputil.ReverseProxy{
		Rewrite: rewrite(spec),
		Transport: &AttemptTransport{
			Upstream:     up,
			Attempts:     deps.Attempts,
			Clock:        deps.Clock,
			Rand:         deps.Rand,
			HostBreakers: deps.HostBreakers,
		},
		ModifyResponse: modifyResponse(spec),
		ErrorHandler:   errorHandler(logger, up.Name),
		ErrorLog:       slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
		FlushInterval:  0,
	}
}

func rewrite(spec RouteSpec) func(pr *httputil.ProxyRequest) {
	return func(pr *httputil.ProxyRequest) {
		switch {
		case spec.StripPrefix != "":
			rewritePath(pr.Out.URL, spec.StripPrefix, "")
		case spec.ReplaceFrom != "":
			rewritePath(pr.Out.URL, spec.ReplaceFrom, spec.ReplaceTo)
		}

		trusted := spec.Trusted.Contains(reqctx.PeerAddr(pr.In))
		if trusted {
			for _, v := range pr.In.Header.Values("X-Forwarded-For") {
				pr.Out.Header.Add("X-Forwarded-For", v)
			}
		}
		pr.SetXForwarded()
		if trusted {
			for _, k := range []string{"X-Forwarded-Proto", "X-Forwarded-Host"} {
				if v := pr.In.Header.Get(k); v != "" {
					pr.Out.Header.Set(k, v)
				}
			}
		}

		if id := reqctx.RequestID(pr.In.Context()); spec.RequestIDHeader != "" && id != "" {
			pr.Out.Header.Set(spec.RequestIDHeader, id)
		}

		applyHeaderOps(pr.Out.Header, spec.RequestHeaders)

		if spec.PreserveHost {
			pr.Out.Host = pr.In.Host
		} else {
			pr.Out.Host = ""
		}

		pr.Out.URL.Scheme = "http"
		pr.Out.URL.Host = "upstream.invalid"
	}
}

func applyHeaderOps(h http.Header, ops config.HeaderOps) {
	for _, k := range ops.Remove {
		h.Del(k)
	}
	for _, k := range slices.Sorted(maps.Keys(ops.Set)) {
		h.Set(k, ops.Set[k].Reveal())
	}
	for _, k := range slices.Sorted(maps.Keys(ops.Add)) {
		h.Add(k, ops.Add[k].Reveal())
	}
}

func rewritePath(u *url.URL, from, to string) {
	from = strings.TrimSuffix(from, "/")
	matches := func(p string) bool {
		return from == "" || p == from || strings.HasPrefix(p, from+"/")
	}
	if !matches(u.Path) {
		return
	}
	if u.RawPath == "" {
		u.Path = join(to, u.Path[len(from):])
		return
	}
	escFrom := ""
	if from != "" {
		escFrom = (&url.URL{Path: from}).EscapedPath()
	}
	esc := u.EscapedPath()
	if esc != escFrom && !strings.HasPrefix(esc, escFrom+"/") {
		u.Path = join(to, u.Path[len(from):])
		u.RawPath = ""
		return
	}
	raw := join((&url.URL{Path: to}).EscapedPath(), esc[len(escFrom):])
	p, err := url.PathUnescape(raw)
	if err != nil {
		u.Path = join(to, u.Path[len(from):])
		u.RawPath = ""
		return
	}
	u.Path = p
	u.RawPath = raw
}

func join(to, rest string) string {
	p := strings.TrimSuffix(to, "/") + rest
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

func modifyResponse(spec RouteSpec) func(resp *http.Response) error {
	return func(resp *http.Response) error {
		resp.Header.Del(spec.RequestIDHeader)
		applyHeaderOps(resp.Header, spec.ResponseHeaders)
		if ex := reqctx.ExchangeFrom(resp.Request.Context()); ex != nil && resp.StatusCode != http.StatusSwitchingProtocols {
			ex.Begin()
			if resp.Body == nil || resp.Body == http.NoBody {
				ex.Finish()
			} else {
				resp.Body = &watchedBody{rc: resp.Body, ex: ex}
			}
		}
		return nil
	}
}

type watchedBody struct {
	rc io.ReadCloser
	ex *reqctx.Exchange
}

func (b *watchedBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if err == io.EOF {
		b.ex.Finish()
	}
	return n, err
}

func (b *watchedBody) Close() error {
	return b.rc.Close()
}

func errorHandler(logger *slog.Logger, upstreamName string) func(w http.ResponseWriter, r *http.Request, err error) {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		if errors.Is(err, upstream.ErrAllOpen) {
			markCircuitOpen(r.Context())
		}
		status, code := httperr.FromError(err)
		switch r.Context().Err() {
		case context.Canceled:
			status, code = httperr.StatusClientClosed, httperr.CodeClientClosed
		case context.DeadlineExceeded:
			status, code = http.StatusGatewayTimeout, httperr.CodeGatewayTimeout
		}
		ctx := r.Context()
		if status == httperr.StatusClientClosed {
			logger.Debug("client canceled request",
				"request_id", reqctx.RequestID(ctx),
				"route", reqctx.Route(ctx),
				"upstream", upstreamName,
				"error", err.Error())
			return
		}
		logger.Warn("upstream error",
			"request_id", reqctx.RequestID(ctx),
			"route", reqctx.Route(ctx),
			"upstream", upstreamName,
			"status", status,
			"error", err.Error())
		httperr.Write(w, r, status, code, httperr.Message(code))
	}
}
