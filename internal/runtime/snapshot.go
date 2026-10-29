// Package runtime builds the immutable per-config request handling snapshot.
package runtime

import (
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/auth"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/middleware"
	"github.com/ok0/go-microgateway/internal/proxy"
	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/internal/retry"
	"github.com/ok0/go-microgateway/internal/router"
	"github.com/ok0/go-microgateway/internal/state"
	"github.com/ok0/go-microgateway/internal/upstream"
)

type Deps struct {
	Logger        *slog.Logger
	Clock         clock.Clock
	Rand          clock.Rand
	Metrics       *metrics.Registry
	Middlewares   *middleware.Registry
	Dial          upstream.DialFunc // nil dials TCP
	Resolver      upstream.Resolver
	KeySetFetcher auth.Fetcher // nil uses auth.NewHTTPFetcher(nil)
	Stores        *StoreSet    // nil registers no store types
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
	rateLimits  []rateLimitRoute
	stores      *StoreSet
	built       []*builtStore
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

	stores := d.Stores
	if stores == nil {
		stores = NewStoreSet(nil)
	}
	errs := stores.Resolve(cfg)
	errs = append(errs, ResolveMiddlewares(cfg, reg)...)
	built, berrs := buildStores(cfg, stores, tx, clk, logger, series)
	errs = append(errs, berrs...)

	named := make(map[string]middleware.Middleware, len(cfg.Middlewares))
	for i, def := range cfg.Middlewares {
		if !reg.Known(def.Type) {
			continue
		}
		mw, err := reg.Build(def)
		if err != nil {
			path := fmt.Sprintf("middlewares[%d]", i)
			errs = append(errs, config.FieldError{Path: path, Line: cfg.Line(path), Msg: fmt.Sprintf("%q: %v", def.Name, err)})
			continue
		}
		named[def.Name] = mw
	}
	if len(errs) > 0 {
		return nil, config.NewValidationError(errs)
	}

	fetcher := d.KeySetFetcher
	if fetcher == nil {
		fetcher = auth.NewHTTPFetcher(nil)
	}
	providers, strip, err := buildAuth(cfg, tx, fetcher, clk, logger, series)
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

	accessLog := cfg.Global.Log.AccessLog == nil || *cfg.Global.Log.AccessLog

	entries := make([]router.Entry, 0, len(cfg.Routes))
	var retryRoutes []string
	var rateLimits []rateLimitRoute
	storePolicies := make(map[string][]string)
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
		if t := middleware.Timeout(r.Timeout.Std(), clk); t != nil {
			stages[middleware.StageTimeout] = t
		}
		if cb {
			stages[middleware.StageCircuitBreaker] = proxy.BreakerStage(up)
		}
		if a := r.Auth; a != nil && a.Mode != "none" {
			ps := make([]auth.Authenticator, 0, len(a.Providers))
			for _, pn := range a.Providers {
				ps = append(ps, providers[pn])
			}
			name := r.Name
			stages[middleware.StageAuth] = auth.Middleware(auth.RouteConfig{
				Route:          r.Name,
				Providers:      ps,
				AllowAnonymous: a.AllowAnonymous,
				Require:        auth.Requirements{Scopes: a.Require.Scopes, Roles: a.Require.Roles, Claims: a.Require.Claims},
				Logger:         logger,
				OnFailure:      func(reason auth.Reason) { series.AuthFailures.With(name, string(reason)).Inc() },
			})
		}
		if r.RateLimit != nil {
			mw, rl, err := buildRateLimit(tx, r, clk, logger, series, built)
			if err != nil {
				return nil, err
			}
			stages[middleware.StageRateLimit] = mw
			rateLimits = append(rateLimits, rl)
			if rl.storeName != "" {
				storePolicies[rl.storeName] = append(storePolicies[rl.storeName], rl.name+"="+rl.policy)
			}
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
				Headers:  revealHeaders(r.Match.Headers),
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

	builtList := make([]*builtStore, 0, len(built))
	for i := range cfg.Stores {
		b, ok := built[cfg.Stores[i].Name]
		if !ok || slices.Contains(builtList, b) {
			continue
		}
		slices.Sort(storePolicies[b.name])
		b.policies = strings.Join(storePolicies[b.name], ",")
		builtList = append(builtList, b)
	}

	s := &Snapshot{
		Config:      cfg,
		Fingerprint: cfg.Fingerprint(),
		Router:      rt,
		Upstreams:   ups,
		notFound:    notFound,
		series:      series,
		retryRoutes: retryRoutes,
		rateLimits:  rateLimits,
		stores:      stores,
		built:       builtList,
	}
	root := []middleware.Middleware{
		middleware.RequestID(cfg.Global.RequestIDHeader),
		middleware.ClientIP(trusted),
		observe,
		middleware.Recover(logger),
	}
	if len(strip) > 0 {
		root = append(root, auth.Strip(strip))
	}
	s.handler = middleware.Chain(http.HandlerFunc(s.dispatch), root...)
	return s, nil
}

// ResolveMiddlewares reports the middleware definitions whose type is not registered in reg. It constructs nothing.
func ResolveMiddlewares(cfg *config.Config, reg *middleware.Registry) []config.FieldError {
	var errs []config.FieldError
	for i, def := range cfg.Middlewares {
		if !reg.Known(def.Type) {
			path := fmt.Sprintf("middlewares[%d].type", i)
			errs = append(errs, config.FieldError{Path: path, Line: cfg.Line(path), Msg: fmt.Sprintf("unknown middleware type %q", def.Type)})
		}
	}
	return errs
}

func revealHeaders(m map[string]config.Secret) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v.Reveal()
	}
	return out
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
	for _, b := range s.built {
		s.stores.own(b.guard, b.policies)
		s.series.StoreErrors.With(b.name, "take")
	}
	for _, rl := range s.rateLimits {
		s.series.RateLimited.With(rl.name)
		if rl.store != nil {
			s.series.RateLimitKeys.With(rl.name).Set(float64(rl.store.Len()))
		}
		if rl.storeName != "" {
			s.series.StoreFallback.With(rl.name, rl.policy)
		}
	}
}

// RefreshRateLimitKeys sets the key-count gauge of every rate-limited route that has an in-memory store.
func (s *Snapshot) RefreshRateLimitKeys() {
	for _, rl := range s.rateLimits {
		if rl.store == nil {
			continue
		}
		s.series.RateLimitKeys.With(rl.name).Set(float64(rl.store.Len()))
	}
}

// PruneGauges deletes the gauge series of rate-limited routes, upstreams and stores that no snapshot in live
// owns. Counters and histograms are never deleted: deleting a counter would lose increments not yet scraped,
// so cumulative series of removed names are kept.
func PruneGauges(live []*Snapshot) {
	if len(live) == 0 {
		return
	}
	routes := make(map[string]bool)
	upstreams := make(map[string]bool)
	stores := make(map[string]bool)
	for _, s := range live {
		for _, rl := range s.rateLimits {
			if rl.store != nil {
				routes[rl.name] = true
			}
		}
		for name := range s.Upstreams {
			upstreams[name] = true
		}
		for _, b := range s.built {
			stores[b.name] = true
		}
	}
	head := live[0]
	sr := head.series
	sr.RateLimitKeys.DeleteFunc(func(v []string) bool { return !routes[v[0]] })
	sr.Upstream.HostHealthy.DeleteFunc(func(v []string) bool { return !upstreams[v[0]] })
	sr.Upstream.PanicMode.DeleteFunc(func(v []string) bool { return !upstreams[v[0]] })
	sr.Upstream.BreakerState.DeleteFunc(func(v []string) bool { return !upstreams[v[0]] })
	head.stores.prune(stores, sr.StoreAvailable)
}

func (s *Snapshot) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}
