package runtime

import (
	"fmt"
	"log/slog"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/memstore"
	"github.com/ok0/go-microgateway/internal/middleware"
	"github.com/ok0/go-microgateway/internal/ratelimit"
	"github.com/ok0/go-microgateway/internal/state"
)

// KindRateLimit is the state kind of a route rate-limit store.
const KindRateLimit = "ratelimit"

type rateLimitRoute struct {
	name      string
	storeName string
	policy    string
	store     *memstore.RateLimiter
}

func buildRateLimit(tx *state.Tx, r *config.Route, clk clock.Clock, logger *slog.Logger, series *Series, stores map[string]*builtStore) (middleware.Middleware, rateLimitRoute, error) {
	lim, ok := r.RateLimit.StoreLimit()
	if !ok {
		return nil, rateLimitRoute{}, fmt.Errorf("route %q: invalid rate_limit", r.Name)
	}
	rl := r.RateLimit
	name := r.Name

	var ms *memstore.RateLimiter
	if rl.Store == "" || rl.OnStoreError == "local" {
		m, err := state.Get(tx, KindRateLimit, name, fp(*rl), func() (*memstore.RateLimiter, error) {
			return memstore.NewRateLimiter(memstore.RateLimiterOptions{MaxKeys: *rl.MaxKeys, Clock: clk}), nil
		})
		if err != nil {
			return nil, rateLimitRoute{}, err
		}
		ms = m
	}
	kf, err := ratelimit.NewKeyFunc(rl.Key.Source, rl.Key.Name)
	if err != nil {
		return nil, rateLimitRoute{}, err
	}
	id := ratelimit.PolicyID(name, lim)

	if rl.Store == "" {
		l := ratelimit.New(ratelimit.Policy{ID: id, Limit: lim, Store: ms, Logger: logger, Clock: clk})
		return ratelimit.Middleware(l, kf, func() { series.RateLimited.With(name).Inc() }), rateLimitRoute{name: name, store: ms}, nil
	}

	bs, ok := stores[rl.Store]
	if !ok {
		return nil, rateLimitRoute{}, fmt.Errorf("route %q: store %q is not built", name, rl.Store)
	}
	onError, err := ratelimit.ParseOnError(rl.OnStoreError)
	if err != nil {
		return nil, rateLimitRoute{}, fmt.Errorf("route %q: %w", name, err)
	}
	storeName := rl.Store
	pol := ratelimit.Policy{
		ID:             id,
		Limit:          lim,
		Store:          bs.backend,
		Logger:         logger,
		Clock:          clk,
		Guard:          bs.guard.g,
		Timeout:        rl.StoreTimeout.Std(),
		OnError:        onError,
		DenyRetryAfter: bs.openTimeout,
		StoreName:      storeName,
		OnStoreError:   func() { series.StoreErrors.With(storeName, "take").Inc() },
		OnFallback:     func(p ratelimit.OnError) { series.StoreFallback.With(name, p.String()).Inc() },
	}
	if ms != nil {
		pol.Local = ms
	}
	l := ratelimit.New(pol)
	return ratelimit.Middleware(l, kf, func() { series.RateLimited.With(name).Inc() }), rateLimitRoute{name: name, storeName: storeName, policy: rl.OnStoreError, store: ms}, nil
}
