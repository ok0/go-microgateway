// Package microgateway is an embeddable HTTP API gateway. A Gateway is an http.Handler built from a validated configuration.
package microgateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/admin"
	"github.com/ok0/go-microgateway/internal/auth"
	"github.com/ok0/go-microgateway/internal/cache"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/hooks"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/middleware"
	"github.com/ok0/go-microgateway/internal/runtime"
	"github.com/ok0/go-microgateway/internal/state"
	"github.com/ok0/go-microgateway/internal/upstream"
)

// ErrClosed is returned by Reload, ReloadYAML, ReloadFile, WatchFile and Purge after Close.
var ErrClosed = errors.New("microgateway: gateway closed")

// Gateway is an http.Handler built from a validated configuration. It is safe for concurrent use.
type Gateway struct {
	cur      atomic.Pointer[generation]
	mu       sync.Mutex // serializes reloads and Close
	closed   bool       // guarded by mu
	isClosed atomic.Bool
	closeErr error
	state    *state.Registry
	deps     runtime.Deps
	parts    []hooks.Participant
	lookup   func(string) (string, bool)
	logger   *slog.Logger
	clk      clock.Clock
	metrics  *metrics.Registry
	reloads  *reloadSeries
	ready    atomic.Bool
	admin    http.Handler
	live     atomic.Int64 // generations whose resources are not yet released; observed by tests
	seriesMu sync.Mutex
	liveGens map[*generation]struct{}
}

// Option configures a Gateway built by New or NewFromYAML.
type Option func(*options)

type options struct {
	logger       *slog.Logger
	lookup       func(string) (string, bool)
	clk          clock.Clock
	middlewares  []registration
	participants []hooks.Participant
	dial         upstream.DialFunc
	rnd          clock.Rand
	resolver     upstream.Resolver
	fetcher      KeySetFetcher
	httpClient   *http.Client
	stores       []storeRegistration
	err          error
}

func init() {
	hooks.WithParticipant = func(p hooks.Participant) any {
		return Option(func(o *options) {
			o.participants = append(o.participants, p)
		})
	}
	hooks.WithClock = func(c clock.Clock) any {
		return withClock(c)
	}
	hooks.WithRand = func(r clock.Rand) any {
		return withRand(r)
	}
	hooks.WithDialer = func(d func(ctx context.Context, network, addr string) (net.Conn, error)) any {
		return withDialer(d)
	}
	hooks.WithResolver = func(r hooks.Resolver) any {
		return withResolver(r)
	}
	hooks.Inspect = func(gw any) hooks.Stats {
		g := gw.(*Gateway)
		st := hooks.Stats{
			StateEntries:    g.state.Len(),
			LiveGenerations: g.live.Load(),
			Generation:      g.cur.Load(),
			ReloadSuccess:   g.reloads.success.Value(),
			ReloadFailure:   g.reloads.failure.Value(),
			ReloadUnchanged: g.reloads.unchanged.Value(),
		}
		if mc, ok := g.cur.Load().snap.MemCacheStats(); ok {
			st.MemCache = &mc
		}
		if cs, ok := g.cur.Load().snap.CoalesceStats(); ok {
			st.Coalesce = &cs
		}
		st.Background = g.cur.Load().snap.BackgroundRunning()
		return st
	}
	hooks.StoreClock = func(p any) clock.Clock {
		if c := p.(StoreParams).clk; c != nil {
			return c
		}
		return clock.Real()
	}
}

// WithLogger sets the logger the gateway writes to. Without it, the process default logger is used.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		o.logger = l
	}
}

// WithEnvLookup replaces the lookup used for ${VAR} substitution in the configuration. Without it, substitution reads the process environment.
func WithEnvLookup(f func(string) (string, bool)) Option {
	return func(o *options) {
		o.lookup = f
	}
}

func withClock(c clock.Clock) Option {
	return func(o *options) {
		o.clk = c
	}
}

// withDialer makes upstream transports open connections through d instead of TCP.
func withDialer(d upstream.DialFunc) Option {
	return func(o *options) { o.dial = d }
}

func withRand(r clock.Rand) Option {
	return func(o *options) { o.rnd = r }
}

func withResolver(r upstream.Resolver) Option {
	return func(o *options) { o.resolver = r }
}

func newOptions(opts []Option) options {
	o := options{logger: slog.Default(), clk: clock.Real()}
	for _, opt := range opts {
		opt(&o)
	}
	if o.err == nil {
		o.err = checkStoreRegistrations(o.stores)
	}
	return o
}

func (o options) middlewareRegistry() (*middleware.Registry, error) {
	reg := middleware.NewRegistry()
	for _, r := range o.middlewares {
		err := reg.Register(r.typeName, func(def config.Middleware) (middleware.Middleware, error) {
			mw, err := r.f(MiddlewareParams{Name: def.Name, Type: def.Type, params: def.Params})
			if err != nil {
				return nil, errors.New(runtime.ScrubText(err.Error(), def.Params.StringLeaves()))
			}
			return mw, nil
		})
		if err != nil {
			return nil, fmt.Errorf("microgateway: WithMiddleware(%q): %w", r.typeName, err)
		}
	}
	return reg, nil
}

// New builds a gateway serving cfg. cfg is cloned and validated before use; New returns an error when cfg is nil or invalid.
func New(cfg *config.Config, opts ...Option) (*Gateway, error) {
	if cfg == nil {
		return nil, errors.New("microgateway: nil config")
	}
	o := newOptions(opts)
	if o.err != nil {
		return nil, o.err
	}
	reg, err := o.middlewareRegistry()
	if err != nil {
		return nil, err
	}
	set := storeSet(o.stores, o.clk)
	cfg = cfg.Clone()
	if err := cfg.Validate(); err != nil {
		return nil, withResolution(err, cfg, set, reg)
	}
	mreg := metrics.NewRegistry()
	rs, err := registerReloadSeries(mreg)
	if err != nil {
		return nil, err
	}
	rnd := o.rnd
	if rnd == nil {
		rnd = clock.NewRand(rand.Uint64())
	}
	var fetcher auth.Fetcher
	if o.fetcher != nil {
		fetcher = o.fetcher
	} else {
		fetcher = auth.NewHTTPFetcher(o.httpClient)
	}
	g := &Gateway{
		state:    state.NewRegistry(o.logger),
		deps:     runtime.Deps{Logger: o.logger, Clock: o.clk, Rand: rnd, Metrics: mreg, Middlewares: reg, Dial: o.dial, Resolver: o.resolver, KeySetFetcher: fetcher, Stores: set, Retention: new(cache.Retention)},
		parts:    o.participants,
		lookup:   o.lookup,
		logger:   o.logger,
		clk:      o.clk,
		metrics:  mreg,
		reloads:  rs,
		liveGens: make(map[*generation]struct{}),
	}
	digest, applies, err := g.prepare(cfg)
	if err != nil {
		return nil, err
	}
	gen, err := g.build(cfg, combine(cfg.Fingerprint(), digest))
	if err != nil {
		return nil, err
	}
	g.install(gen, applies)
	rs.lastSuccess.Set(float64(g.clk.Now().Unix()))
	g.ready.Store(true)
	g.admin = admin.Handler(adminSource{g}, o.logger)
	return g, nil
}

// NewFromYAML parses data as a configuration document and returns a gateway serving it, like New. When the document
// is not valid it returns one *config.ValidationError listing every problem: schema errors together with the store
// and middleware types that opts do not register, the same report microgateway validate prints.
func NewFromYAML(data []byte, opts ...Option) (*Gateway, error) {
	o := newOptions(opts)
	if o.err != nil {
		return nil, o.err
	}
	reg, err := o.middlewareRegistry()
	if err != nil {
		return nil, err
	}
	cfg, err := config.Parse(data, loadOptions(o.lookup, storeSet(o.stores, o.clk), reg)...)
	if err != nil {
		return nil, err
	}
	return New(cfg, opts...)
}

// ServeHTTP serves r with the current configuration generation. Requests that arrive after Close receive 503.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for {
		gen := g.cur.Load()
		if gen.enter() {
			defer gen.leave()
			gen.snap.ServeHTTP(w, r)
			return
		}
		if g.isClosed.Load() && g.cur.Load() == gen {
			httperr.Write(w, r, http.StatusServiceUnavailable, httperr.CodeUnavailable, httperr.Message(httperr.CodeUnavailable))
			return
		}
	}
}

// Config returns the configuration of the current snapshot. Treat it as read-only.
func (g *Gateway) Config() *config.Config {
	return g.cur.Load().snap.Config
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

// Close stops reloads, cancels background cache refreshes and releases the gateway's resources once requests in flight and those refreshes finish. Requests that arrive after Close receive 503. Close is idempotent.
func (g *Gateway) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return g.closeErr
	}
	g.closed = true
	g.isClosed.Store(true)
	g.state.Close()
	g.seriesMu.Lock()
	for gen := range g.liveGens {
		gen.snap.CancelBackground()
	}
	g.seriesMu.Unlock()
	g.closeErr = g.cur.Load().retire()
	return g.closeErr
}
