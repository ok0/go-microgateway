// Package ratelimit binds a route policy to any store.RateLimiter.
//
// It holds no algorithm or instance-local state of its own. With the built-in
// memory store each gateway instance enforces its own limit, so N instances
// admit N times the configured rate. With a shared store the limit is global.
//
// When a Policy carries a Guard, every store call goes through the guard and
// a failed call is decided by the policy's OnError setting.
package ratelimit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/storeguard"
	"github.com/ok0/lychgate/store"
)

const MaxKeyBytes = 128

// OnError selects the failure policy applied when a guarded store call fails.
type OnError uint8

const (
	// NoFallback means the configured store decides every request. It is the
	// zero value and is not valid for a Policy that has a Guard.
	NoFallback OnError = iota
	// Local decides the request with the per-instance Policy.Local limiter.
	Local
	// Allow admits the request without counting it.
	Allow
	// Deny rejects the request with 503 Service Unavailable.
	Deny
)

// String returns the configuration name of the failure policy, or a numeric
// form for unknown values. NoFallback has the empty name.
func (o OnError) String() string {
	switch o {
	case NoFallback:
		return ""
	case Local:
		return "local"
	case Allow:
		return "allow"
	case Deny:
		return "deny"
	}
	return "OnError(" + strconv.Itoa(int(o)) + ")"
}

// ParseOnError returns the failure policy named by s, which must be "local",
// "allow" or "deny".
func ParseOnError(s string) (OnError, error) {
	switch s {
	case "local":
		return Local, nil
	case "allow":
		return Allow, nil
	case "deny":
		return Deny, nil
	}
	return NoFallback, fmt.Errorf("ratelimit: unknown on_error %q", s)
}

// Policy configures a Limiter.
type Policy struct {
	// ID namespaces the keys sent to the store.
	ID string
	// Limit is the limit applied to every key.
	Limit store.Limit
	// Store is the backend that decides requests. It is called directly when
	// Guard is nil and through Guard otherwise.
	Store store.RateLimiter
	// Logger receives store failure records. A nil Logger discards them.
	Logger *slog.Logger
	// Clock drives the log rate limiter. A nil Clock uses the real clock.
	Clock clock.Clock
	// Guard, when non-nil, wraps every call to Store.
	Guard *storeguard.Guard
	// Timeout bounds each store call made through Guard. It must be positive
	// when Guard is set.
	Timeout time.Duration
	// OnError selects the failure policy used when Guard is set. It must not
	// be NoFallback when Guard is set.
	OnError OnError
	// Local is the per-instance limiter used when OnError is Local. It must be
	// set when OnError is Local.
	Local store.RateLimiter
	// DenyRetryAfter is the Retry-After value sent with a Deny decision.
	DenyRetryAfter time.Duration
	// StoreName identifies the store in log records.
	StoreName string
	// OnStoreError is called each time a store call fails or times out. It is
	// not called for calls rejected by an open guard.
	OnStoreError func()
	// OnFallback is called each time the failure policy decides a request. It
	// receives the policy that decided.
	OnFallback func(OnError)
}

// Result is the outcome of Limiter.Take.
type Result struct {
	// Decision is the store decision, or the Local decision when Policy is Local.
	Decision store.Decision
	// Degraded reports that the request was admitted without a decision and
	// without rate limit headers.
	Degraded bool
	// Policy is the failure policy that decided the request, or NoFallback
	// when the configured store decided it.
	Policy OnError
}

// Limiter applies a Policy to requests.
type Limiter struct {
	p          Policy
	lastLog    atomic.Int64
	suppressed atomic.Int64
}

func PolicyID(route string, l store.Limit) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%d|%d|%d", l.Algorithm, l.Count, int64(l.Period), l.Burst)))
	return route + ":" + hex.EncodeToString(sum[:])[:8]
}

// New returns a Limiter for p. It panics when Guard is set and OnError is
// NoFallback, when Timeout is not positive, or when OnError is Local and Local
// is nil.
func New(p Policy) *Limiter {
	if p.Guard != nil && (p.OnError == NoFallback || p.Timeout <= 0 || (p.OnError == Local && p.Local == nil)) {
		panic("ratelimit: guarded policy needs a failure policy, a positive timeout and a local limiter for on_error local")
	}
	if p.Logger == nil {
		p.Logger = slog.New(slog.DiscardHandler)
	}
	if p.Clock == nil {
		p.Clock = clock.Real()
	}
	return &Limiter{p: p}
}

// Take decides one request for key. Without a Guard the configured Store
// decides directly. With a Guard the call is bounded by Timeout and a failure
// is decided by OnError.
func (l *Limiter) Take(ctx context.Context, key string) Result {
	if len(key) > MaxKeyBytes {
		sum := sha256.Sum256([]byte(key))
		key = hex.EncodeToString(sum[:])
	}
	k := l.p.ID + ":" + key
	if l.p.Guard != nil {
		return l.takeGuarded(ctx, k)
	}
	d, err := l.p.Store.Take(ctx, k, l.p.Limit)
	if err == nil {
		return Result{Decision: d}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return Result{Degraded: true}
	}
	l.logError(err)
	return Result{Degraded: true}
}

func (l *Limiter) takeGuarded(ctx context.Context, k string) Result {
	var d store.Decision
	err := l.p.Guard.Do(ctx, l.p.Timeout, func(c context.Context) error {
		var e error
		d, e = l.p.Store.Take(c, k, l.p.Limit)
		return e
	})
	switch {
	case err == nil:
		return Result{Decision: d}
	case ctx.Err() != nil:
		return Result{Degraded: true}
	case errors.Is(err, store.ErrUnavailable):
		if !storeguard.ShortCircuit(err) {
			if l.p.OnStoreError != nil {
				l.p.OnStoreError()
			}
			l.logStoreFailure(err)
		}
		return l.fallback(ctx, k)
	default:
		l.logError(err)
		return Result{Degraded: true}
	}
}

func (l *Limiter) fallback(ctx context.Context, k string) Result {
	switch l.p.OnError {
	case Local:
		d, err := l.p.Local.Take(ctx, k, l.p.Limit)
		if err != nil {
			return Result{Degraded: true}
		}
		if l.p.OnFallback != nil {
			l.p.OnFallback(Local)
		}
		return Result{Decision: d, Policy: Local}
	case Allow:
		if l.p.OnFallback != nil {
			l.p.OnFallback(Allow)
		}
		return Result{Degraded: true, Policy: Allow}
	default:
		if l.p.OnFallback != nil {
			l.p.OnFallback(Deny)
		}
		return Result{Policy: Deny}
	}
}

// claimLog reports whether a log record may be written now. It returns the
// number of records suppressed since the last one written.
func (l *Limiter) claimLog() (int64, bool) {
	now := l.p.Clock.Now().UnixNano()
	last := l.lastLog.Load()
	if last != 0 && now-last < int64(time.Second) {
		l.suppressed.Add(1)
		return 0, false
	}
	if !l.lastLog.CompareAndSwap(last, now) {
		l.suppressed.Add(1)
		return 0, false
	}
	return l.suppressed.Swap(0), true
}

func (l *Limiter) logError(err error) {
	n, ok := l.claimLog()
	if !ok {
		return
	}
	if l.p.Guard == nil {
		l.p.Logger.Error("rate limit store failed; request admitted", "policy", l.p.ID, "error", err.Error(), "suppressed", n)
		return
	}
	l.p.Logger.Error("rate limit store failed; request admitted", "policy", l.p.ID, "store", l.p.StoreName, "error", err.Error(), "suppressed", n)
}

func (l *Limiter) logStoreFailure(err error) {
	n, ok := l.claimLog()
	if !ok {
		return
	}
	l.p.Logger.Warn("rate limit store call failed; failure policy applied", "policy", l.p.ID, "store", l.p.StoreName, "on_store_error", l.p.OnError.String(), "error", err.Error(), "suppressed", n)
}
