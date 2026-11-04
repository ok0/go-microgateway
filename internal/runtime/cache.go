package runtime

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/auth"
	"github.com/ok0/go-microgateway/internal/cache"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/coalesce"
	"github.com/ok0/go-microgateway/internal/memstore"
	"github.com/ok0/go-microgateway/internal/middleware"
	"github.com/ok0/go-microgateway/internal/state"
	"github.com/ok0/go-microgateway/internal/storeguard"
	"github.com/ok0/go-microgateway/store"
)

// KindCache is the state kind of the built-in memory cache.
const KindCache = "cache"

func cacheEnabled(cfg *config.Config) bool {
	for i := range cfg.Routes {
		if rc := cfg.Routes[i].Cache; rc != nil && rc.Enabled {
			return true
		}
	}
	return false
}

// cacheWiring holds the store and collaborators shared by the cache stages of one generation.
type cacheWiring struct {
	store        store.Cache
	guard        *storeguard.Guard
	storeName    string
	mem          *memstore.Cache
	series       *CacheSeries
	errs         *cache.ErrorLog
	retain       time.Duration
	onStoreError func(op string)
	group        *coalesce.Group
	runner       *cache.Runner
	scope        *cache.Scope
}

func buildCacheWiring(cfg *config.Config, tx *state.Tx, clk clock.Clock, logger *slog.Logger, cs *CacheSeries, s *Series, built map[string]*builtStore) (*cacheWiring, error) {
	w := &cacheWiring{
		series: cs,
		errs:   cache.NewErrorLog(logger, clk),
		retain: cfg.Cache.MaxStorageTTL(cfg.Routes),
	}
	if *cfg.Cache.Coalescing.Enabled {
		co := cfg.Cache.Coalescing
		g, err := state.Get(tx, KindCache, "coalescing", fp([]int{*co.MaxKeys, *co.MaxWaiters}), func() (*coalesce.Group, error) {
			return coalesce.New(*co.MaxKeys, *co.MaxWaiters, clk), nil
		})
		if err != nil {
			return nil, err
		}
		w.group = g
	}
	rn, err := state.Get(tx, KindCache, "background", fp(*cfg.Cache.MaxBackground), func() (*cache.Runner, error) {
		return cache.NewRunner(*cfg.Cache.MaxBackground), nil
	})
	if err != nil {
		return nil, err
	}
	w.runner = rn
	w.scope = rn.Scope()
	if cfg.Cache.Store == "" {
		mc := cfg.Cache.Memory
		m, err := state.Get(tx, KindCache, "memory", fp(mc), func() (*memstore.Cache, error) {
			return memstore.NewCache(memstore.CacheOptions{
				MaxBytes: *mc.MaxBytes,
				Shards:   *mc.Shards,
				Clock:    clk,
				OnEvict:  func(n int, _ int64) { cs.Evictions.With().Add(float64(n)) },
			}), nil
		})
		if err != nil {
			return nil, err
		}
		w.store = m
		w.mem = m
		return w, nil
	}
	name := cfg.Cache.Store
	b, ok := built[name]
	if !ok {
		return nil, fmt.Errorf("cache store %q is not built", name)
	}
	w.store = b.backend
	w.guard = b.guard.g
	w.storeName = name
	w.onStoreError = func(op string) { s.StoreErrors.With(name, op).Inc() }
	return w, nil
}

func buildCacheStage(r *config.Route, cfg *config.Config, w *cacheWiring, providers map[string]auth.Authenticator, clk clock.Clock, logger *slog.Logger) (middleware.Middleware, error) {
	var ps []auth.Authenticator
	if a := r.Auth; a != nil && a.Mode != "none" {
		ps = make([]auth.Authenticator, 0, len(a.Providers))
		for _, pn := range a.Providers {
			ps = append(ps, providers[pn])
		}
	}
	return cache.NewStage(*r.Cache, cfg.Cache, cache.Deps{
		Route:        r.Name,
		Store:        w.store,
		Guard:        w.guard,
		StoreName:    w.storeName,
		Credential:   func(req *http.Request) bool { return auth.Presented(req, ps) },
		Retain:       w.retain,
		OnStoreError: w.onStoreError,
		Errors:       w.errs,
		Clock:        clk,
		Group:        w.group,
		Background:   w.scope,
		Metrics:      &cache.Metrics{Requests: w.series.Requests, Stores: w.series.Stores, StoreErrors: w.series.StoreErrors, Coalesced: w.series.Coalesced, Background: w.series.Background},
		Logger:       logger,
	})
}
