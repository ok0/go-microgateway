// Package hooks exposes gateway extension points to the microgateway binary without making them public API.
package hooks

import (
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
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
