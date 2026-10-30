package redis

import (
	"errors"

	microgateway "github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/hooks"
	"github.com/ok0/go-microgateway/store"
)

const capabilities = store.CapRateLimit

func factory(p microgateway.StoreParams) (store.Backend, error) {
	if p.Redis == nil {
		return nil, errors.New("redis: store entry has no redis block")
	}
	s, err := newFromConfig(p.Redis, p.Logger, hooks.StoreClock(p))
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Option returns a microgateway.Option that registers the store type redis for rate limiting. The gateway builds one
// Store per referenced store entry and closes it when the entry is no longer used. The dial, read and write limits of
// each Store run on the gateway's clock.
func Option() microgateway.Option {
	return microgateway.WithStoreType("redis", capabilities, factory)
}
