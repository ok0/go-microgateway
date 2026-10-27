// Package runtime builds the immutable per-config request handling snapshot.
package runtime

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/httperr"
	"github.com/ok0/lychgate/internal/metrics"
	"github.com/ok0/lychgate/internal/middleware"
	"github.com/ok0/lychgate/internal/proxy"
	"github.com/ok0/lychgate/internal/reqctx"
	"github.com/ok0/lychgate/internal/router"
	"github.com/ok0/lychgate/internal/upstream"
)

type Deps struct {
	Logger      *slog.Logger
	Clock       clock.Clock
	Metrics     *metrics.Registry
	Middlewares *middleware.Registry
}

type Snapshot struct {
	Config    *config.Config
	Router    *router.Router
	Upstreams map[string]*upstream.Upstream

	handler  http.Handler
	notFound http.Handler
}

type routeEntry struct {
	name    string
	handler http.Handler
}

func Build(cfg *config.Config, d Deps) (*Snapshot, error) {
	logger := d.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	clk := d.Clock
	if clk == nil {
		clk = clock.Real()
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
		u, err := upstream.Build(cfg.Upstreams[i])
		if err != nil {
			_ = closeAll(ups)
			return nil, err
		}
		ups[cfg.Upstreams[i].Name] = u
	}

	named := make(map[string]middleware.Middleware, len(cfg.Middlewares))
	for i, def := range cfg.Middlewares {
		if !reg.Known(def.Type) {
			_ = closeAll(ups)
			return nil, fmt.Errorf("middlewares[%d].type: unknown middleware type %q", i, def.Type)
		}
		mw, err := reg.Build(def)
		if err != nil {
			_ = closeAll(ups)
			return nil, fmt.Errorf("middlewares[%d] (%q): %w", i, def.Name, err)
		}
		named[def.Name] = mw
	}

	accessLog := cfg.Global.Log.AccessLog == nil || *cfg.Global.Log.AccessLog

	entries := make([]router.Entry, 0, len(cfg.Routes))
	for i := range cfg.Routes {
		r := &cfg.Routes[i]
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
		px := proxy.New(spec, ups[r.Upstream], proxy.Deps{Logger: logger, Attempts: series.Attempts})
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
		h := middleware.Chain(
			terminal,
			middleware.Pipeline(middleware.Order(r.Middlewares, r.Pipeline), stages)...,
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
		_ = closeAll(ups)
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
		Config:    cfg,
		Router:    rt,
		Upstreams: ups,
		notFound:  notFound,
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

func (s *Snapshot) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func (s *Snapshot) Close() error {
	return closeAll(s.Upstreams)
}

func closeAll(ups map[string]*upstream.Upstream) error {
	var errs []error
	for _, u := range ups {
		errs = append(errs, u.Close())
	}
	return errors.Join(errs...)
}
