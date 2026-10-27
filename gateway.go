// Package lychgate is an embeddable HTTP API gateway. A Gateway is an http.Handler built from a validated configuration.
package lychgate

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/admin"
	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/metrics"
	"github.com/ok0/lychgate/internal/middleware"
	"github.com/ok0/lychgate/internal/runtime"
)

type Gateway struct {
	snap      atomic.Pointer[runtime.Snapshot]
	closeOnce sync.Once
	closeErr  error
	metrics   *metrics.Registry
	ready     atomic.Bool
	admin     http.Handler
}

type Option func(*options)

type options struct {
	logger      *slog.Logger
	lookup      func(string) (string, bool)
	clk         clock.Clock
	middlewares []registration
}

func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		o.logger = l
	}
}

func WithEnvLookup(f func(string) (string, bool)) Option {
	return func(o *options) {
		o.lookup = f
	}
}

func newOptions(opts []Option) options {
	o := options{logger: slog.Default(), clk: clock.Real()}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

func New(cfg *config.Config, opts ...Option) (*Gateway, error) {
	if cfg == nil {
		return nil, errors.New("lychgate: nil config")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	o := newOptions(opts)
	reg := middleware.NewRegistry()
	for _, r := range o.middlewares {
		err := reg.Register(r.typeName, func(def config.Middleware) (middleware.Middleware, error) {
			return r.f(MiddlewareParams{Name: def.Name, Type: def.Type, params: def.Params})
		})
		if err != nil {
			return nil, fmt.Errorf("lychgate: WithMiddleware(%q): %w", r.typeName, err)
		}
	}
	mreg := metrics.NewRegistry()
	snap, err := runtime.Build(cfg, runtime.Deps{Logger: o.logger, Clock: o.clk, Metrics: mreg, Middlewares: reg})
	if err != nil {
		return nil, err
	}
	g := &Gateway{metrics: mreg}
	g.snap.Store(snap)
	g.ready.Store(true)
	g.admin = admin.Handler(adminSource{g})
	return g, nil
}

func NewFromYAML(data []byte, opts ...Option) (*Gateway, error) {
	o := newOptions(opts)
	var cfg *config.Config
	var err error
	if o.lookup != nil {
		cfg, err = config.Parse(data, config.WithEnvLookup(o.lookup))
	} else {
		cfg, err = config.Parse(data)
	}
	if err != nil {
		return nil, err
	}
	return New(cfg, opts...)
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.snap.Load().ServeHTTP(w, r)
}

// Config returns the configuration of the current snapshot. Treat it as read-only.
func (g *Gateway) Config() *config.Config {
	return g.snap.Load().Config
}

// SetReady sets whether the readiness endpoint of the admin handler reports the gateway as ready.
func (g *Gateway) SetReady(ready bool) {
	g.ready.Store(ready)
}

// Ready reports whether the gateway is ready.
func (g *Gateway) Ready() bool {
	return g.ready.Load()
}

// AdminHandler returns the administrative handler. Serve it on a separate listener only; the data handler never serves admin paths.
func (g *Gateway) AdminHandler() http.Handler {
	return g.admin
}

func (g *Gateway) Close() error {
	g.closeOnce.Do(func() {
		g.closeErr = g.snap.Load().Close()
	})
	return g.closeErr
}
