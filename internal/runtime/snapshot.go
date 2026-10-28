// Package runtime builds the immutable per-config request handling snapshot.
package runtime

import (
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"slices"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/httperr"
	"github.com/ok0/lychgate/internal/metrics"
	"github.com/ok0/lychgate/internal/middleware"
	"github.com/ok0/lychgate/internal/proxy"
	"github.com/ok0/lychgate/internal/reqctx"
	"github.com/ok0/lychgate/internal/retry"
	"github.com/ok0/lychgate/internal/router"
	"github.com/ok0/lychgate/internal/state"
	"github.com/ok0/lychgate/internal/upstream"
)

type Deps struct {
	Logger      *slog.Logger
	Clock       clock.Clock
	Rand        clock.Rand
	Metrics     *metrics.Registry
	Middlewares *middleware.Registry
	Dial        upstream.DialFunc // nil dials TCP
	Resolver    upstream.Resolver
}

type Snapshot struct {
	Config      *config.Config
	Fingerprint string
	Router      *router.Router
	Upstreams   map[string]*upstream.Upstream

	handler     http.Handler
	notFound    http.Handler
	series      *Series
	retryRoutes []string
}

type routeEntry struct {
	name    string
	handler http.Handler
}

// Build ... Components that hold resources or carry state are obtained through tx; when Build fails the caller aborts tx, which releases everything this build created.
func Build(cfg *config.Config, d Deps, tx *state.Tx) (*Snapshot, error) {
	logger := d.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	clk := d.Clock
	if clk == nil {
		clk = clock.Real()
	}
	rnd := d.Rand
	if rnd == nil {
		rnd = clock.NewRand(1)
	}

	mreg := d.Metrics
	if mreg == nil {
		mreg = metrics.NewRegistry()
	}
	reg := d.Middlewares
	if reg == nil {
		reg = middleware.NewRegistry()
	}

	trusted, err := reqctx.ParseTrusted(cfg.Global.TrustedProxies)
	if err != nil {
		return nil, err
	}

	series, err := RegisterSeries(mreg)
	if err != nil {
		return nil, err
	}

	ups := make(map[string]*upstream.Upstream, len(cfg.Upstreams))
	for i := range cfg.Upstreams {
		u, err := upstream.Build(cfg.Upstreams[i], upstream.Deps{
			State:    tx,
			Dial:     d.Dial,
			Clock:    clk,
			Rand:     rnd,
			Logger:   logger,
			Metrics:  series.Upstream,
			Resolver: d.Resolver,
		})
		if err != nil {
			return nil, err
		}
		ups[cfg.Upstreams[i].Name] = u
	}

	named := make(map[string]middleware.Middleware, len(cfg.Middlewares))
	var mwErrs []config.FieldError
	for i, def := range cfg.Middlewares {
		if !reg.Known(def.Type) {
			path := fmt.Sprintf("middlewares[%d].type", i)
			mwErrs = append(mwErrs, config.FieldError{Path: path, Line: cfg.Line(path), Msg: fmt.Sprintf("unknown middleware type %q", def.Type)})
			continue
		}
		mw, err := reg.Build(def)
		if err != nil {
			path := fmt.Sprintf("middlewares[%d]", i)
			mwErrs = append(mwErrs, config.FieldError{Path: path, Line: cfg.Line(path), Msg: fmt.Sprintf("%q: %v", def.Name, err)})
			continue
		}
		named[def.Name] = mw
	}
	if len(mwErrs) > 0 {
		return nil, &config.ValidationError{Errors: mwErrs}
	}

	accessLog := cfg.Global.Log.AccessLog == nil || *cfg.Global.Log.AccessLog

	entries := make([]router.Entry, 0, len(cfg.Routes))
	var retryRoutes []string
	for i := range cfg.Routes {
		r := &cfg.Routes[i]
		up := ups[r.Upstream]
		order := middleware.Order(r.Middlewares, r.Pipeline)
		cb := breakerEnabled(r, up, order)
		spec := proxy.RouteSpec{
			Name:            r.Name,
			StripPrefix:     r.Rewrite.StripPrefix,
			ReplaceFrom:     r.Rewrite.ReplacePrefix.From,
			ReplaceTo:       r.Rewrite.ReplacePrefix.To,
			RequestHeaders:  r.RequestHeaders,
			ResponseHeaders: r.ResponseHeaders,
			PreserveHost:    r.PreserveHost,
			RequestIDHeader: cfg.Global.RequestIDHeader,
			Trusted:         trusted,
		}
		px := proxy.New(spec, up, proxy.Deps{Logger: logger, Attempts: series.Attempts, Clock: clk, Rand: rnd, HostBreakers: cb})
		terminal := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if in := reqctx.InfoFrom(req.Context()); in != nil {
				in.Upstream = r.Upstream
			}
			px.ServeHTTP(w, req)
		})
		stages := maps.Clone(named)
		if t := middleware.Timeout(r.Timeout.Std()); t != nil {
			stages[middleware.StageTimeout] = t
		}
		if cb {
			stages[middleware.StageCircuitBreaker] = proxy.BreakerStage(up)
		}
		if r.Retry != nil && slices.Contains(order, middleware.StageRetry) {
			b, err := retryBudget(tx, r.Name, *r.Retry, clk)
			if err != nil {
				return nil, err
			}
			name := r.Name
			stages[middleware.StageRetry] = retry.Middleware(retryPolicy(*r.Retry), b, retry.Hooks{
				OnRetry:     func() { series.Retries.With(name).Inc() },
				OnExhausted: func() { series.RetryBudgetExhausted.With(name).Inc() },
			})
			retryRoutes = append(retryRoutes, name)
		}
		h := middleware.Chain(
			terminal,
			middleware.Pipeline(order, stages)...,
		)
		entries = append(entries, router.Entry{
			Matcher: router.Matcher{
				Hosts:    r.Match.Hosts,
				Path:     r.Match.Path,
				PathType: router.PathType(r.Match.PathType),
				Methods:  r.Match.Methods,
				Headers:  r.Match.Headers,
				Priority: r.Priority,
				Order:    i,
			},
			Value: &routeEntry{name: r.Name, handler: h},
		})
	}

	rt, err := router.New(entries)
	if err != nil {
		return nil, err
	}

	notFound := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httperr.Write(w, r, http.StatusNotFound, httperr.CodeRouteNotFound, httperr.Message(httperr.CodeRouteNotFound))
	})

	observe := middleware.Observe(logger, clk, accessLog, &middleware.RequestMetrics{
		Requests: series.Requests,
		Duration: series.Duration,
	})

	s := &Snapshot{
		Config:      cfg,
		Fingerprint: cfg.Fingerprint(),
		Router:      rt,
		Upstreams:   ups,
		notFound:    notFound,
		series:      series,
		retryRoutes: retryRoutes,
	}
	s.handler = middleware.Chain(
		http.HandlerFunc(s.dispatch),
		middleware.RequestID(cfg.Global.RequestIDHeader),
		middleware.ClientIP(trusted),
		observe,
		middleware.Recover(logger),
	)
	return s, nil
}

func (s *Snapshot) dispatch(w http.ResponseWriter, r *http.Request) {
	v, params, ok := s.Router.Lookup(r)
	if !ok {
		s.notFound.ServeHTTP(w, r)
		return
	}
	re := v.(*routeEntry)
	if in := reqctx.InfoFrom(r.Context()); in != nil {
		in.Route = re.name
	}
	ctx := reqctx.WithRoute(r.Context(), re.name)
	if len(params) > 0 {
		ctx = reqctx.WithParams(ctx, params)
	}
	re.handler.ServeHTTP(w, r.WithContext(ctx))
}

// Activate publishes the host sets of the upstreams. Call it once, after the state transaction
// that built the snapshot commits and before the snapshot serves requests.
func (s *Snapshot) Activate() {
	for i := range s.Config.Upstreams {
		s.Upstreams[s.Config.Upstreams[i].Name].Activate()
	}
	for _, n := range s.retryRoutes {
		s.series.Retries.With(n)
		s.series.RetryBudgetExhausted.With(n)
	}
}

func (s *Snapshot) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}
