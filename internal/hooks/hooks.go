// Package hooks exposes gateway extension points to the lychgate binary without making them public API.
package hooks

import "github.com/ok0/lychgate/config"

// Participant takes part in every configuration load of a gateway, the initial one and every reload.
type Participant interface {
	// Prepare reads material that cfg refers to outside the configuration document. It returns a digest of that
	// material, which takes part in deciding whether a reload changes anything, and a function that makes the
	// material current. Prepare must not change observable state. The gateway calls apply only after the new
	// configuration is installed and never when the load fails or changes nothing. An error fails the load.
	Prepare(cfg *config.Config) (digest string, apply func(), err error)
}

// WithParticipant returns a lychgate.Option, typed as any, that adds p to a gateway. Package lychgate sets it
// when it is initialized; it is nil until then.
var WithParticipant func(p Participant) any
