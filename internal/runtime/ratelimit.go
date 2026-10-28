package runtime

import (
	"fmt"
	"log/slog"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/memstore"
	"github.com/ok0/lychgate/internal/middleware"
	"github.com/ok0/lychgate/internal/ratelimit"
	"github.com/ok0/lychgate/internal/state"
)

// KindRateLimit is the state kind of a route rate-limit store.
const KindRateLimit = "ratelimit"

type rateLimitRoute struct {
	name  string
	store *memstore.RateLimiter
}

func buildRateLimit(tx *state.Tx, r *config.Route, clk clock.Clock, logger *slog.Logger, series *Series) (middleware.Middleware, rateLimitRoute, error) {
	lim, ok := r.RateLimit.StoreLimit()
	if !ok {
		return nil, rateLimitRoute{}, fmt.Errorf("route %q: invalid rate_limit", r.Name)
	}
	ms, err := state.Get(tx, KindRateLimit, r.Name, fp(*r.RateLimit), func() (*memstore.RateLimiter, error) {
		return memstore.NewRateLimiter(memstore.RateLimiterOptions{MaxKeys: *r.RateLimit.MaxKeys, Clock: clk}), nil
	})
	if err != nil {
		return nil, rateLimitRoute{}, err
	}
	kf, err := ratelimit.NewKeyFunc(r.RateLimit.Key.Source, r.RateLimit.Key.Name)
	if err != nil {
		return nil, rateLimitRoute{}, err
	}
	l := ratelimit.New(ratelimit.Policy{ID: ratelimit.PolicyID(r.Name, lim), Limit: lim, Store: ms, Logger: logger, Clock: clk})
	name := r.Name
	return ratelimit.Middleware(l, kf, func() { series.RateLimited.With(name).Inc() }), rateLimitRoute{name: name, store: ms}, nil
}
