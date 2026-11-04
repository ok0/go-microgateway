// Package hooks exposes gateway extension points to the microgateway binary and to tests outside the root package without making them public API.
package hooks

import (
	"context"
	"net"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/coalesce"
	"github.com/ok0/go-microgateway/internal/memstore"
)

// Participant takes part in every configuration load of a gateway, the initial one and every reload.
type Participant interface {
	// Prepare reads material that cfg refers to outside the configuration document. It returns a digest of that
	// material, which takes part in deciding whether a reload changes anything, and a function that makes the
	// material current. Prepare must not change observable state. The gateway calls apply only after the new
	// configuration is installed and never when the load fails or changes nothing. An error fails the load.
	Prepare(cfg *config.Config) (digest string, apply func(), err error)
}

// WithParticipant returns a microgateway.Option, typed as any, that adds p to a gateway. Package microgateway sets it
// when it is initialized; it is nil until then.
var WithParticipant func(p Participant) any

// WithClock returns a microgateway.Option, typed as any, that makes a gateway use c for every timer and
// timestamp. Package microgateway sets it when it is initialized; it is nil until then. It lets tests outside the
// root package drive a gateway with a fake clock.
var WithClock func(c clock.Clock) any

// WithRand returns a microgateway.Option, typed as any, that makes a gateway draw load-balancing and retry-backoff
// randomness from r. Package microgateway sets it when it is initialized; it is nil until then.
var WithRand func(r clock.Rand) any

// WithDialer returns a microgateway.Option, typed as any, that makes upstream transports open connections through d
// instead of TCP. Package microgateway sets it when it is initialized; it is nil until then.
var WithDialer func(d func(ctx context.Context, network, addr string) (net.Conn, error)) any

// Resolver resolves upstream host names for DNS discovery.
type Resolver interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// WithResolver returns a microgateway.Option, typed as any, that makes DNS discovery resolve through r. Package
// microgateway sets it when it is initialized; it is nil until then.
var WithResolver func(r Resolver) any

// Stats is a view of a gateway's internal bookkeeping for tests.
type Stats struct {
	StateEntries                                  int     // entries in the state registry
	LiveGenerations                               int64   // generations whose resources are not yet released
	Generation                                    any     // identity of the active generation; equal values mean the same generation
	ReloadSuccess, ReloadFailure, ReloadUnchanged float64 // configuration reload attempts, by result
	MemCache                                      *memstore.CacheStats
	Coalesce                                      *coalesce.Stats // in-flight coalescing state of the active generation; nil without coalescing
	Background                                    int             // background refreshes running in the active generation's runner
}

// Inspect returns the Stats of gw, which must be a *microgateway.Gateway; it panics for any other value. Package
// microgateway sets it when it is initialized; it is nil until then.
var Inspect func(gw any) Stats

// StoreClock returns the clock of the gateway that builds the store described by p, which must be the
// microgateway.StoreParams passed to a store factory; it panics for any other value. It is the real clock unless
// the gateway was given another one. Store factories use it so that the time limits of a store run on the clock of
// its gateway. Package microgateway sets it when it is initialized; it is nil until then.
var StoreClock func(p any) clock.Clock
