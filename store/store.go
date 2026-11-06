// Package store defines the shared-state contracts for rate limiting and response caching.
//
// Every backend implements identical semantics. For the same sequence of
// rate-limit Take calls and the same backend clock readings, every backend
// must return the same Decision, bit for bit, and must persist the same state.
// Backends that
// need to interoperate, or that are checked against each other, follow the
// definitions in this package documentation exactly.
//
// The built-in memory backend is per instance: each gateway instance keeps its
// own state, so N instances enforce N times the configured limit. A shared
// backend gives one global limit across all instances that use it.
//
// # Limit validation
//
// Limit.Validate checks the rules below in order and reports the first one
// that fails. Every error wraps ErrInvalidLimit.
//
//  1. Algorithm must be TokenBucket or SlidingWindow ("unknown algorithm N").
//  2. Count must be >= 1 ("count must be >= 1").
//  3. Period must be > 0 ("period must be positive").
//  4. For TokenBucket, Burst must be >= 1 ("burst must be >= 1").
//  5. T = int64(Period) / Count must be >= int64(MinInterval)
//     ("emission interval below 1µs"). This applies to both algorithms.
//  6. For TokenBucket, Burst must be <= int64(MaxSpan) / T, the overflow-safe
//     form of T * Burst <= MaxSpan ("period/count × burst exceeds 24h").
//  7. For SlidingWindow, Period must be <= MaxSpan ("period exceeds 24h").
//
// # Take order
//
// Every backend processes Take in this order:
//
//  1. If ctx.Err() is non-nil, return that error unwrapped. Nothing is consumed.
//  2. Validate the Limit. A failure returns an error wrapping ErrInvalidLimit.
//  3. If the backend is closed, return an error wrapping ErrUnavailable.
//  4. Decide and persist atomically.
//
// # Arithmetic
//
// All arithmetic is int64 nanoseconds unless stated otherwise. now is the
// backend clock time as Unix nanoseconds. floorDiv(a, b) is the mathematical
// floor of a / b for b > 0.
//
// # Token bucket (GCRA)
//
// The state of a key is tat, an int64. An absent key is the initial state,
// which is equivalent to tat <= now.
//
//	T   = int64(Period) / Count            // integer division
//	tau = T * Burst
//	base   = max(tat, now)
//	newTAT = base + T
//	allowed = newTAT - now <= tau
//	if allowed { stored = newTAT; persist stored } else { stored = tat; nothing is written }
//	x = tau - (max(stored, now) - now)
//	Remaining  = x < 0 ? 0 : x / T
//	ResetAfter = max(stored - now, 0)
//	RetryAfter = allowed ? 0 : newTAT - now - tau
//	Limit      = Burst
//	expired(now) = tat <= now
//
// # Sliding window
//
// The state of a key is {win, prev, curr}, all int64. win is the index of the
// window that holds curr. An absent key is the initial state.
//
//	P = int64(Period)
//	idx = floorDiv(now, P)
//	elapsed = now - idx*P
//
//	view(st, idx):
//	    st.win == idx     -> (prev, curr) = (st.prev, st.curr)
//	    st.win == idx - 1 -> (prev, curr) = (st.curr, 0)
//	    otherwise         -> (prev, curr) = (0, 0)
//
//	estimate(prev, curr, elapsed, P) float64:
//	    e   := float64(elapsed) / float64(P)
//	    w   := 1 - e
//	    pw  := float64(float64(prev) * w)   // explicit conversion forbids fused multiply-add
//	    est := pw + float64(curr)
//
//	(prev, curr) = view(st, idx)
//	est = estimate(prev, curr, elapsed, P)
//	allowed = est + 1 <= float64(Count)
//	if allowed {
//	    curr++
//	    est = estimate(prev, curr, elapsed, P)
//	    persist {win: idx, prev, curr}
//	} else {
//	    nothing is written
//	}
//	Remaining  = max(0, int64(math.Floor(float64(Count) - est)))
//	ResetAfter = curr > 0 ? (idx+2)*P - now : prev > 0 ? (idx+1)*P - now : 0
//	RetryAfter = allowed ? 0 : k * 1ms
//	    where k is the smallest integer in [1, hi] such that allowedAt(now + k*1ms),
//	    hi = ceilDiv((idx+2)*P - now, 1ms), and allowedAt(t) evaluates
//	    view({idx, prev, curr}, floorDiv(t, P)), then the estimate at t, and
//	    tests est + 1 <= Count. k is found by binary search:
//	        lo = 1
//	        hi = hi
//	        while lo < hi {
//	            mid = lo + (hi - lo) / 2
//	            if allowedAt(mid) { hi = mid } else { lo = mid + 1 }
//	        }
//	    allowedAt is monotone because every floating-point operation is correctly
//	    rounded and the estimate is continuous across window boundaries.
//	Limit   = Count
//	expired(now) = floorDiv(now, P) >= st.win + 2
//
// Backends that reproduce these definitions must follow the same floating-point
// evaluation order, including the explicit float64 conversion of the product.
//
// # Response cache
//
// Cache stores HTTP responses. Every method runs in this order: a canceled
// context is returned unwrapped with nothing changed; invalid arguments return
// an error wrapping ErrInvalidEntry or ErrInvalidPurge; a closed handle returns
// an error wrapping ErrUnavailable; then one atomic operation runs.
//
// The store clock is the only time source. Entry.StoredAt and Lookup.Now are
// readings of that clock, and the caller computes age as InitialAge +
// Now.Sub(StoredAt). An entry is present while now < StoredAt + ttl. A ttl of
// zero or less stores nothing and reports stored=false without error. A ttl
// greater than MaxTTL is an invalid entry. Reads and purges never extend a
// deadline.
//
// Each primary key has one Vary record. A Put whose canonical Vary names differ
// from the record discards the primary's variants. A primary holds at most
// MaxVariants variants, and the variant with the oldest StoredAt is removed at
// the cap.
//
// Hard purges make covered variants misses. Soft purges keep them readable with
// Stale set. A later hard purge wins over a soft one. PurgeTags matches exact
// tags per variant. Keys and tags purges report exact affected counts.
//
// Delete removes one variant, selected by its primary and secondary keys, on
// every handle of the store. It reports false without error when the variant
// is not stored. It is not a purge: it records no marker, refuses no fill
// token, and a later Put may store the variant again.
//
// A fill token is issued by Get. A Put is refused, with stored=false and no
// error, when a covering purge began after its token was issued. A backend may
// also refuse a fill whose token was issued within PurgeTolerance after a
// covering purge. Each purge leaves a marker for its Retain, and callers pass
// a Retain of at least their largest storage TTL.
//
// The memory backend is per instance: a purge reaches only the instance that
// receives it. A shared backend gives one cluster-wide purge.
package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// ErrUnavailable reports that a backend cannot serve a request, for example
// because it is closed or its shared state cannot be reached.
var ErrUnavailable = errors.New("store: unavailable")

// ErrInvalidLimit reports that a Limit fails validation. Errors returned by
// Limit.Validate wrap it.
var ErrInvalidLimit = errors.New("store: invalid limit")

// Backend is the lifecycle contract shared by all store backends.
type Backend interface {
	// Close releases the resources held by the backend handle.
	Close() error
}

// Capabilities is a bit set of optional features a backend provides.
type Capabilities uint8

const (
	// CapRateLimit indicates that a backend implements RateLimiter.
	CapRateLimit Capabilities = 1 << iota
	// CapCache indicates that a backend implements Cache.
	CapCache
)

// Algorithm selects the rate-limiting algorithm applied to a Limit.
type Algorithm uint8

// Supported rate-limiting algorithms.
const (
	// TokenBucket is a generic cell rate algorithm (GCRA) that admits bursts
	// of up to Burst requests and refills at Count requests per Period.
	TokenBucket Algorithm = iota + 1
	// SlidingWindow approximates Count requests per Period using the weighted
	// counts of the current and previous fixed windows.
	SlidingWindow
)

// String returns the configuration name of the algorithm, or a numeric form
// for unknown values.
func (a Algorithm) String() string {
	switch a {
	case TokenBucket:
		return "token_bucket"
	case SlidingWindow:
		return "sliding_window"
	}
	return "Algorithm(" + strconv.Itoa(int(a)) + ")"
}

// Limits on the timing parameters of a Limit.
const (
	// MinInterval is the smallest permitted emission interval, period/count.
	MinInterval = time.Microsecond
	// MaxSpan is the largest permitted period, and the largest permitted
	// product of emission interval and burst.
	MaxSpan = 24 * time.Hour
)

// Limit describes a rate limit applied to one key.
type Limit struct {
	// Algorithm selects the rate-limiting algorithm.
	Algorithm Algorithm
	// Count is the number of requests admitted per Period.
	Count int64
	// Period is the time span over which Count requests are admitted.
	Period time.Duration
	// Burst is the maximum number of requests admitted at once. It is used by
	// TokenBucket only and must be zero for SlidingWindow.
	Burst int64
}

// Validate reports whether l is a valid limit. It returns nil for a valid
// limit and an error wrapping ErrInvalidLimit otherwise. The rules are listed
// in the package documentation.
func (l Limit) Validate() error {
	switch l.Algorithm {
	case TokenBucket, SlidingWindow:
	default:
		return fmt.Errorf("%w: unknown algorithm %d", ErrInvalidLimit, uint8(l.Algorithm))
	}
	if l.Count < 1 {
		return fmt.Errorf("%w: count must be >= 1", ErrInvalidLimit)
	}
	if l.Period <= 0 {
		return fmt.Errorf("%w: period must be positive", ErrInvalidLimit)
	}
	if l.Algorithm == TokenBucket && l.Burst < 1 {
		return fmt.Errorf("%w: burst must be >= 1", ErrInvalidLimit)
	}
	t := int64(l.Period) / l.Count
	if t < int64(MinInterval) {
		return fmt.Errorf("%w: emission interval below 1µs", ErrInvalidLimit)
	}
	if l.Algorithm == TokenBucket && l.Burst > int64(MaxSpan)/t {
		return fmt.Errorf("%w: period/count × burst exceeds 24h", ErrInvalidLimit)
	}
	if l.Algorithm == SlidingWindow && l.Period > MaxSpan {
		return fmt.Errorf("%w: period exceeds 24h", ErrInvalidLimit)
	}
	return nil
}

// Decision is the result of a Take call.
type Decision struct {
	// Allowed reports whether the request was admitted.
	Allowed bool
	// Limit is the configured limit, as Limit.Count for SlidingWindow or
	// Limit.Burst for TokenBucket.
	Limit int64
	// Remaining is the number of requests that would still be admitted.
	Remaining int64
	// RetryAfter is the time until the next request would be admitted. It is
	// zero when Allowed is true.
	RetryAfter time.Duration
	// ResetAfter is the time until the key returns to its initial state.
	ResetAfter time.Duration
}

// RateLimiter is a backend that enforces rate limits.
type RateLimiter interface {
	Backend
	// Take consumes one request for key under l if it is admitted, and
	// reports the decision. A denied request consumes nothing. Take processes
	// requests in the order described in the package documentation.
	Take(ctx context.Context, key string, l Limit) (Decision, error)
}
