// Package runtime builds the immutable per-config request handling snapshot.
package runtime

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/httperr"
	"github.com/ok0/lychgate/internal/middleware"
	"github.com/ok0/lychgate/internal/proxy"
	"github.com/ok0/lychgate/internal/reqctx"
	"github.com/ok0/lychgate/internal/router"
	"github.com/ok0/lychgate/internal/upstream"
)

type Deps struct {
	Logger *slog.Logger
	Clock  clock.Clock
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

	trusted, err := reqctx.ParseTrusted(cfg.Global.TrustedProxies)
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

	access := middleware.AccessLog(logger, clk, cfg.Global.Log.AccessLog == nil || *cfg.Global.Log.AccessLog)

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
		h := middleware.Chain(
			proxy.New(spec, ups[r.Upstream], proxy.Deps{Logger: logger}),
			access,
			middleware.Timeout(r.Timeout.Std()),
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

	notFound := middleware.Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httperr.Write(w, r, http.StatusNotFound, httperr.CodeRouteNotFound, httperr.Message(httperr.CodeRouteNotFound))
	}), access)

	s := &Snapshot{
		Config:    cfg,
		Router:    rt,
		Upstreams: ups,
		notFound:  notFound,
	}
	s.handler = middleware.Chain(
		http.HandlerFunc(s.dispatch),
		middleware.RequestID(cfg.Global.RequestIDHeader),
		middleware.Recover(logger),
		middleware.ClientIP(trusted),
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
