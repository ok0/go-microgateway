// Package runtime builds the immutable per-config request handling snapshot.
package runtime

import (
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/auth"
	"github.com/ok0/go-microgateway/internal/cache"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/coalesce"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/memstore"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/middleware"
	"github.com/ok0/go-microgateway/internal/proxy"
	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/internal/reqpath"
	"github.com/ok0/go-microgateway/internal/retry"
	"github.com/ok0/go-microgateway/internal/router"
	"github.com/ok0/go-microgateway/internal/state"
	"github.com/ok0/go-microgateway/internal/upstream"
)

// Deps holds the dependencies a gateway passes to Build.
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
	// Retention is the purge marker high-water shared by every snapshot of one gateway. Nil gives each snapshot its own.
	Retention *cache.Retention
}

// Snapshot is one built configuration generation: the parsed configuration, its router and upstreams, and the handler that serves requests with them.
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
	cacheSeries *CacheSeries
	cacheRoutes []string
	memCache    *memstore.Cache
	coalesce    *coalesce.Group
	runner      *cache.Runner
	background  *cache.Scope
	purger      *cache.Purger
	retention   *cache.Retention
	retainFloor time.Duration
	adminIDs    []string
	adminHashes [][32]byte
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
	var cs *CacheSeries
	if cacheEnabled(cfg) || mreg.Registered("microgateway_cache_requests_total") {
		cs, err = RegisterCacheSeries(mreg)
		if err != nil {
			return nil, err
		}
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

	adminIDs, adminHashes, err := adminKeys(cfg)
	if err != nil {
		return nil, err
	}

	retention := d.Retention
	if retention == nil {
		retention = new(cache.Retention)
	}
	var cw *cacheWiring
	if cacheEnabled(cfg) {
		cw, err = buildCacheWiring(cfg, tx, clk, logger, cs, series, built, retention)
		if err != nil {
			return nil, err
		}
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
	var cacheRoutes []string
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
		if r.Cache != nil && r.Cache.Enabled {
			mw, err := buildCacheStage(r, cfg, cw, providers, clk, logger)
			if err != nil {
				return nil, err
			}
			stages[middleware.StageCache] = mw
			cacheRoutes = append(cacheRoutes, r.Name)
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

	var memCache *memstore.Cache
	var group *coalesce.Group
	var runner *cache.Runner
	var background *cache.Scope
	var purger *cache.Purger
	var retainFloor time.Duration
	if cw != nil {
		memCache = cw.mem
		group = cw.group
		runner = cw.runner
		background = cw.scope
		purger = cw.purger
		retainFloor = cw.retain
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
		cacheSeries: cs,
		cacheRoutes: cacheRoutes,
		memCache:    memCache,
		coalesce:    group,
		runner:      runner,
		background:  background,
		purger:      purger,
		retention:   retention,
		retainFloor: retainFloor,
		adminIDs:    adminIDs,
		adminHashes: adminHashes,
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
	p, err := reqpath.Canonicalize(r.URL)
	if err != nil {
		if in := reqctx.InfoFrom(r.Context()); in != nil {
			in.Error = httperr.CodeInvalidPath
		}
		httperr.Write(w, r, http.StatusBadRequest, httperr.CodeInvalidPath, httperr.Message(httperr.CodeInvalidPath))
		return
	}
	r = withPath(r, p)
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

// withPath returns a shallow copy of r whose URL is a copy carrying the canonical path p. r and r.URL are not modified.
func withPath(r *http.Request, p reqpath.Path) *http.Request {
	c := r.WithContext(r.Context())
	u := *r.URL
	u.Path, u.RawPath = p.Decoded, p.Raw
	c.URL = &u
	return c
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
		s.stores.own(b.guard, b.policies, b.uses())
		if b.rateLimit {
			s.series.StoreErrors.With(b.name, "take")
		}
		if b.cache {
			for _, op := range []string{"get", "put", "purge", "delete"} {
				s.series.StoreErrors.With(b.name, op)
			}
		}
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
	if s.cacheSeries != nil {
		for _, n := range s.cacheRoutes {
			for _, res := range []string{"hit", "stale", "revalidated", "miss", "bypass"} {
				s.cacheSeries.Requests.With(n, res)
			}
			for _, res := range []string{"stored", "not_storable", "too_large", "aborted", "refused", "error"} {
				s.cacheSeries.Stores.With(n, res)
			}
		}
		if len(s.cacheRoutes) > 0 {
			for _, op := range []string{"get", "put", "purge"} {
				s.cacheSeries.StoreErrors.With(op)
			}
		}
		for _, t := range []string{cache.PurgeTypeURL, cache.PurgeTypeTag, cache.PurgeTypeAll} {
			for _, m := range []string{"hard", "soft"} {
				s.cacheSeries.Purges.With(t, m)
			}
		}
	}
	if s.purger != nil {
		s.retention.Raise(s.retainFloor)
	}
	s.RefreshCacheGauges()
}

// Purger returns the purge service of the snapshot's cache, or nil when no route enables caching.
func (s *Snapshot) Purger() *cache.Purger {
	return s.purger
}

// AdminKeys returns the ids and sha256 digests of the admin keys of the snapshot's configuration, index-aligned.
func (s *Snapshot) AdminKeys() (ids []string, hashes [][32]byte) {
	return s.adminIDs, s.adminHashes
}

// RefreshCacheGauges sets the size gauges of the built-in memory cache. It does nothing when the snapshot has none.
func (s *Snapshot) RefreshCacheGauges() {
	if s.memCache == nil {
		return
	}
	st := s.memCache.Stats()
	s.cacheSeries.Bytes.With().Set(float64(st.Bytes))
	s.cacheSeries.Entries.With().Set(float64(st.Entries))
}

// MemCacheStats returns the statistics of the built-in memory cache. It reports false when the snapshot has none.
func (s *Snapshot) MemCacheStats() (memstore.CacheStats, bool) {
	if s.memCache == nil {
		return memstore.CacheStats{}, false
	}
	return s.memCache.Stats(), true
}

// CoalesceStats reports the in-flight coalescing state of the snapshot's group. It reports false when the snapshot has none.
func (s *Snapshot) CoalesceStats() (coalesce.Stats, bool) {
	if s.coalesce == nil {
		return coalesce.Stats{}, false
	}
	return s.coalesce.Stats(), true
}

// BackgroundRunning reports the background refreshes running in the snapshot's runner. It is 0 when the snapshot has none.
func (s *Snapshot) BackgroundRunning() int {
	if s.runner == nil {
		return 0
	}
	return s.runner.Running()
}

// CancelBackground ends the background refreshes started by this snapshot; later ones are not started.
func (s *Snapshot) CancelBackground() {
	if s.background != nil {
		s.background.Cancel()
	}
}

// RetireBackground stops new background refreshes of the snapshot. It reports true when some still run, and then calls
// onIdle once after the last one ended. It reports false, without calling onIdle, when none runs.
func (s *Snapshot) RetireBackground(onIdle func()) bool {
	if s.background == nil {
		return false
	}
	return s.background.Retire(onIdle)
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
// so cumulative series of removed names are kept. It also deletes the per-host gauge series of hosts that no live
// generation's upstream holds.
func PruneGauges(live []*Snapshot) {
	if len(live) == 0 {
		return
	}
	routes := make(map[string]bool)
	upstreams := make(map[string]bool)
	stores := make(map[string]bool)
	var ups []*upstream.Upstream
	var cs *CacheSeries
	memLive := false
	for _, s := range live {
		for _, rl := range s.rateLimits {
			if rl.store != nil {
				routes[rl.name] = true
			}
		}
		for name, u := range s.Upstreams {
			upstreams[name] = true
			ups = append(ups, u)
		}
		for _, b := range s.built {
			stores[b.name] = true
		}
		if s.cacheSeries != nil && cs == nil {
			cs = s.cacheSeries
		}
		if s.memCache != nil {
			memLive = true
		}
	}
	head := live[0]
	sr := head.series
	sr.Upstream.RetainHosts(ups)
	sr.RateLimitKeys.DeleteFunc(func(v []string) bool { return !routes[v[0]] })
	sr.Upstream.HostHealthy.DeleteFunc(func(v []string) bool { return !upstreams[v[0]] })
	sr.Upstream.PanicMode.DeleteFunc(func(v []string) bool { return !upstreams[v[0]] })
	sr.Upstream.BreakerState.DeleteFunc(func(v []string) bool { return !upstreams[v[0]] })
	head.stores.prune(stores, sr.StoreAvailable)
	if cs != nil && !memLive {
		cs.Bytes.DeleteFunc(func([]string) bool { return true })
		cs.Entries.DeleteFunc(func([]string) bool { return true })
	}
}

// ServeHTTP serves r with the snapshot's handler.
func (s *Snapshot) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}
