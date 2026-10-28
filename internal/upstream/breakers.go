package upstream

import (
	"maps"
	"sync"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/breaker"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/health"
	"github.com/ok0/go-microgateway/internal/httperr"
)

const (
	KindBreaker      = "upstream.breaker"
	KindBreakerHosts = "upstream.breaker.hosts"
)

var ErrAllOpen = httperr.ErrCircuitOpen

func orInt(p *int, d int) int {
	if p == nil {
		return d
	}
	return *p
}

func orFloat(p *float64, d float64) float64 {
	if p == nil {
		return d
	}
	return *p
}

func breakerSettings(cb config.CircuitBreaker) breaker.Settings {
	return breaker.Settings{
		Window:              cb.Window.Std(),
		Buckets:             orInt(cb.Buckets, 10),
		MinRequests:         orInt(cb.MinRequests, 20),
		FailureRatio:        orFloat(cb.FailureRatio, 0.5),
		ConsecutiveFailures: orInt(cb.ConsecutiveFailures, 5),
		OpenTimeout:         cb.OpenTimeout.Std(),
		HalfOpenProbes:      orInt(cb.HalfOpenProbes, 3),
		SuccessThreshold:    orInt(cb.SuccessThreshold, 2),
	}
}

func BreakerOutcome(o health.Outcome) breaker.Outcome {
	switch o {
	case health.Success:
		return breaker.Success
	case health.Failure:
		return breaker.Failure
	}
	return breaker.Ignored
}

type hostBreakers struct {
	name     string
	settings breaker.Settings
	clk      clock.Clock
	pool     *pool
	mu       sync.Mutex
	byKey    map[string]*breaker.Breaker
}

func newHostBreakers(name string, cb config.CircuitBreaker, clk clock.Clock, pool *pool) (*hostBreakers, error) {
	s := breakerSettings(cb)
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return &hostBreakers{
		name:     name,
		settings: s,
		clk:      clk,
		pool:     pool,
		byKey:    map[string]*breaker.Breaker{},
	}, nil
}

func (hb *hostBreakers) sync(keys []string) map[string]*breaker.Breaker {
	hb.mu.Lock()
	defer hb.mu.Unlock()
	next := make(map[string]*breaker.Breaker, len(keys))
	for _, key := range keys {
		b, ok := hb.byKey[key]
		if !ok {
			s := hb.settings
			s.OnStateChange = func(from, to breaker.State, reason breaker.Reason) {
				hb.pool.onHostBreaker(hb, key, from, to, reason)
			}
			nb, err := breaker.New(s, hb.clk)
			if err != nil {
				// Settings were validated by newHostBreakers, so New cannot fail.
				panic(err)
			}
			b = nb
		}
		next[key] = b
	}
	hb.byKey = next
	return maps.Clone(next)
}

func newAggregate(name string, cb config.CircuitBreaker, clk clock.Clock, pool *pool) (*breaker.Breaker, error) {
	var b *breaker.Breaker
	s := breakerSettings(cb)
	s.OnStateChange = func(from, to breaker.State, reason breaker.Reason) {
		pool.onAggregateBreaker(b, from, to, reason)
	}
	var err error
	b, err = breaker.New(s, clk)
	return b, err
}
