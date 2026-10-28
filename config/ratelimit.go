package config

import "github.com/ok0/go-microgateway/store"

// StoreLimit returns the store.Limit described by rl. It is valid only after defaults have been applied. A
// validated configuration always yields ok, and the returned Limit validates with store.Limit.Validate.
func (rl *RateLimit) StoreLimit() (store.Limit, bool) {
	switch {
	case rl.Algorithm == "token_bucket" && rl.Rate != nil && rl.Per != nil && rl.Burst != nil:
		return store.Limit{Algorithm: store.TokenBucket, Count: *rl.Rate, Period: rl.Per.Std(), Burst: *rl.Burst}, true
	case rl.Algorithm == "sliding_window" && rl.Limit != nil && rl.Window != nil:
		return store.Limit{Algorithm: store.SlidingWindow, Count: *rl.Limit, Period: rl.Window.Std()}, true
	}
	return store.Limit{}, false
}
