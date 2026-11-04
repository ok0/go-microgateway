package microgateway

import (
	"context"

	"github.com/ok0/go-microgateway/internal/cache"
)

// PurgeRequest selects the cached responses a purge covers: exactly one of URLs, Tags or All. See Gateway.Purge.
type PurgeRequest = cache.PurgeRequest

// PurgeResult reports a purge the cache store acknowledged.
type PurgeResult = cache.PurgeResult

// ErrCacheDisabled is returned by Purge when no route of the active configuration enables caching.
var ErrCacheDisabled = cache.ErrCacheDisabled

// ErrInvalidPurgeRequest is wrapped by the error Purge returns for a malformed request; nothing was purged.
var ErrInvalidPurgeRequest = cache.ErrInvalidRequest

// Purge removes the cached responses that req selects from the active configuration's cache. It runs the same service
// as the admin purge endpoint, without a credential. The built-in memory cache is per instance: call Purge on every
// instance, and the result's Scope reports that reach. A named store's purge is shared by every instance that uses it.
// The errors, checked in this order, are ErrClosed after Close, ErrCacheDisabled, an error wrapping
// ErrInvalidPurgeRequest, an error wrapping store.ErrUnavailable, and the error of ctx. A failure part-way may leave
// earlier batches applied; repeating the purge is safe.
func (g *Gateway) Purge(ctx context.Context, req PurgeRequest) (PurgeResult, error) {
	return g.purge(ctx, req, "")
}

func (g *Gateway) purge(ctx context.Context, req PurgeRequest, keyID string) (PurgeResult, error) {
	for {
		gen := g.cur.Load()
		if gen.enter() {
			defer gen.leave()
			p := gen.snap.Purger()
			if p == nil {
				return PurgeResult{}, ErrCacheDisabled
			}
			return p.Purge(ctx, req, keyID)
		}
		if g.isClosed.Load() && g.cur.Load() == gen {
			return PurgeResult{}, ErrClosed
		}
	}
}
