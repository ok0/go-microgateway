package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/middleware"
	"github.com/ok0/lychgate/internal/retry"
	"github.com/ok0/lychgate/internal/state"
	"github.com/ok0/lychgate/internal/upstream"
)

// KindRetryBudget is the state kind of a route retry budget.
const KindRetryBudget = "retry.budget"

func retryPolicy(rt config.Retry) *retry.Policy {
	statuses := make(map[int]bool, len(rt.OnStatuses))
	for _, s := range rt.OnStatuses {
		statuses[s] = true
	}
	methods := make(map[string]bool, len(rt.Methods))
	for _, m := range rt.Methods {
		methods[m] = true
	}
	attempts := 3
	if rt.Attempts != nil {
		attempts = *rt.Attempts
	}
	maxBody := int64(65536)
	if rt.MaxBodyBytes != nil {
		maxBody = *rt.MaxBodyBytes
	}
	p := &retry.Policy{
		Attempts: attempts,
		Base:     rt.BaseDelay.Std(),
		Max:      rt.MaxDelay.Std(),
		Statuses: statuses,
		Methods:  methods,
		MaxBody:  maxBody,
	}
	if rt.IdempotencyKey.Enabled {
		p.IdempotencyHeader = rt.IdempotencyKey.Header
	}
	return p
}

func retryBudget(tx *state.Tx, route string, rt config.Retry, clk clock.Clock) (*retry.Budget, error) {
	ratio := 0.1
	if rt.Budget.Ratio != nil {
		ratio = *rt.Budget.Ratio
	}
	minPerSecond := 10.0
	if rt.Budget.MinPerSecond != nil {
		minPerSecond = *rt.Budget.MinPerSecond
	}
	return state.Get(tx, KindRetryBudget, route, fp(rt), func() (*retry.Budget, error) {
		return retry.NewBudget(ratio, minPerSecond, rt.Budget.Window.Std(), clk), nil
	})
}

func breakerEnabled(r *config.Route, up *upstream.Upstream, order []string) bool {
	return up.HasBreaker() &&
		slices.Contains(order, middleware.StageCircuitBreaker) &&
		(r.CircuitBreaker == nil || r.CircuitBreaker.Enabled == nil || *r.CircuitBreaker.Enabled)
}

func fp(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
