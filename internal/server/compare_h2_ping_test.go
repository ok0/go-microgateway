//go:build realnet

package server

import (
	"testing"
	"time"
)

// The idle limit here exceeds net/http's 15 s ping-ack wait, so this comparison runs only with the realnet tag.
func TestHTTP2ReadLimitNoPingMatchesNetHTTP(t *testing.T) {
	compareH2(t, h2Combos(t, []h2scenario{{
		name:    "h2/read-no-ping-unacked",
		limits:  [4]time.Duration{untestedLimit, testedLimit, 0, 16 * time.Second},
		handler: okProbe,
		drive:   driveIdleNoPing(false),
		check:   requireNoPing,
	}}))
}
