// Package lychgate is an embeddable HTTP API gateway. A Gateway is an http.Handler built from a validated configuration.
package lychgate

import (
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/runtime"
)

type Gateway struct {
	snap      atomic.Pointer[runtime.Snapshot]
	closeOnce sync.Once
	closeErr  error
}

type Option func(*options)

type options struct {
	logger *slog.Logger
	lookup func(string) (string, bool)
	clk    clock.Clock
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
	snap, err := runtime.Build(cfg, runtime.Deps{Logger: o.logger, Clock: o.clk})
	if err != nil {
		return nil, err
	}
	g := &Gateway{}
	g.snap.Store(snap)
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

func (g *Gateway) Close() error {
	g.closeOnce.Do(func() {
		g.closeErr = g.snap.Load().Close()
	})
	return g.closeErr
}
