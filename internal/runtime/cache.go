package runtime

import (
	"errors"
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
	retention    *cache.Retention
	onStoreError func(op string)
	group        *coalesce.Group
	runner       *cache.Runner
	scope        *cache.Scope
	purger       *cache.Purger
}

// checkCacheDefaults reports the first defaulted field of c that the cache wiring needs and finds unset. Build
// takes a validated configuration, whose defaults are applied; this keeps any other caller from a nil dereference.
func checkCacheDefaults(c *config.Cache) error {
	if c.Coalescing.Enabled == nil {
		return errors.New("cache: coalescing.enabled is not set")
	}
	if *c.Coalescing.Enabled {
		if c.Coalescing.MaxKeys == nil {
			return errors.New("cache: coalescing.max_keys is not set")
		}
		if c.Coalescing.MaxWaiters == nil {
			return errors.New("cache: coalescing.max_waiters is not set")
		}
	}
	if c.MaxBackground == nil {
		return errors.New("cache: max_background is not set")
	}
	if c.StoreTimeout == nil {
		return errors.New("cache: store_timeout is not set")
	}
	if c.Store == "" {
		if c.Memory.MaxBytes == nil {
			return errors.New("cache: memory.max_bytes is not set")
		}
		if c.Memory.Shards == nil {
			return errors.New("cache: memory.shards is not set")
		}
	}
	return nil
}

// storeErrorCounter returns the hook that counts one failed call of op on the store called name.
func storeErrorCounter(s *Series, name string) func(op string) {
	return func(op string) { s.StoreErrors.With(name, op).Inc() }
}

func buildCacheWiring(cfg *config.Config, tx *state.Tx, clk clock.Clock, logger *slog.Logger, cs *CacheSeries, s *Series, built map[string]*builtStore, retention *cache.Retention) (*cacheWiring, error) {
	if err := checkCacheDefaults(&cfg.Cache); err != nil {
		return nil, err
	}
	w := &cacheWiring{
		series:    cs,
		errs:      cache.NewErrorLog(logger, clk),
		retain:    cfg.Cache.MaxStorageTTL(cfg.Routes),
		retention: retention,
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
	} else {
		name := cfg.Cache.Store
		b, ok := built[name]
		if !ok {
			return nil, fmt.Errorf("cache store %q is not built", name)
		}
		w.store = b.backend
		w.guard = b.guard.g
		w.storeName = name
		w.onStoreError = storeErrorCounter(s, name)
	}
	p, err := cache.NewPurger(cache.PurgerDeps{Store: w.store, Guard: w.guard, StoreName: w.storeName, Timeout: cfg.Cache.StoreTimeout.Std(),
		Retain: w.retain, Retention: retention, OnStoreError: w.onStoreError,
		Metrics: &cache.PurgeMetrics{Purges: cs.Purges, StoreErrors: cs.StoreErrors}, Logger: logger})
	if err != nil {
		return nil, err
	}
	w.purger = p
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
		Retention:    w.retention,
		OnStoreError: w.onStoreError,
		Errors:       w.errs,
		Clock:        clk,
		Group:        w.group,
		Background:   w.scope,
		Metrics:      &cache.Metrics{Requests: w.series.Requests, Stores: w.series.Stores, StoreErrors: w.series.StoreErrors, Coalesced: w.series.Coalesced, Background: w.series.Background},
		Logger:       logger,
	})
}
