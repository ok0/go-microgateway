// Package retry implements the retry policy, full-jitter backoff, per-route
// retry budget and request body replay. Budgets, randomness and waits are per
// gateway instance: each instance's budget is about ratio of its own traffic,
// so aggregate retry load stays about ratio of aggregate traffic.
package retry

import (
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
)

// Policy describes when and how a request may be retried.
type Policy struct {
	Attempts          int
	Base, Max         time.Duration
	Statuses          map[int]bool
	Methods           map[string]bool
	IdempotencyHeader string
	MaxBody           int64
}

// Attempt describes the outcome of one upstream attempt.
type Attempt struct {
	N             int
	Method        string
	Status        int
	HasIdemKey    bool
	Replayable    bool
	Sent          bool
	ConnectErr    bool
	HeaderTimeout bool
	Canceled      bool
}

// ShouldRetry reports whether the attempt described by a may be followed by another attempt.
func (p *Policy) ShouldRetry(a Attempt) bool {
	if a.Canceled || a.N >= p.Attempts {
		return false
	}
	trigger := a.ConnectErr || (a.Status >= 400 && p.Statuses[a.Status]) || (a.HeaderTimeout && p.Statuses[504])
	if !trigger {
		return false
	}
	if !a.Replayable {
		return a.ConnectErr && !a.Sent
	}
	if p.Methods[a.Method] {
		return true
	}
	if a.ConnectErr && !a.Sent {
		return true
	}
	return p.IdempotencyHeader != "" && a.HasIdemKey
}

// Backoff returns a full-jitter delay before the attempt numbered attempt (1-based, at least 2).
// The delay is drawn uniformly from [0, min(Max, Base·2^(attempt-1))).
func (p *Policy) Backoff(attempt int, rnd clock.Rand) time.Duration {
	w := p.Max
	if shift := attempt - 1; shift < 62 && p.Base <= p.Max>>shift {
		w = min(p.Max, p.Base<<shift)
	}
	if w <= 0 {
		return 0
	}
	return time.Duration(rnd.Int64N(int64(w)))
}

// Buffers reports whether a request with the given method and idempotency key state
// must have its body buffered so that it can be replayed.
func (p *Policy) Buffers(method string, hasIdemKey bool) bool {
	return p.Methods[method] || (p.IdempotencyHeader != "" && hasIdemKey)
}
