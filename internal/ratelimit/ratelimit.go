// Package ratelimit binds a route policy to any store.RateLimiter.
//
// It holds no algorithm or instance-local state of its own. With the built-in
// memory store each gateway instance enforces its own limit, so N instances
// admit N times the configured rate. With a shared store the limit is global.
package ratelimit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/store"
)

const MaxKeyBytes = 128

type Policy struct {
	ID     string
	Limit  store.Limit
	Store  store.RateLimiter
	Logger *slog.Logger
	Clock  clock.Clock
}

type Result struct {
	Decision store.Decision
	Degraded bool // store failed, request admitted, no headers
}

type Limiter struct {
	p          Policy
	lastLog    atomic.Int64
	suppressed atomic.Int64
}

func PolicyID(route string, l store.Limit) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%d|%d|%d", l.Algorithm, l.Count, int64(l.Period), l.Burst)))
	return route + ":" + hex.EncodeToString(sum[:])[:8]
}

func New(p Policy) *Limiter {
	if p.Logger == nil {
		p.Logger = slog.New(slog.DiscardHandler)
	}
	if p.Clock == nil {
		p.Clock = clock.Real()
	}
	return &Limiter{p: p}
}

func (l *Limiter) Take(ctx context.Context, key string) Result {
	if len(key) > MaxKeyBytes {
		sum := sha256.Sum256([]byte(key))
		key = hex.EncodeToString(sum[:])
	}
	d, err := l.p.Store.Take(ctx, l.p.ID+":"+key, l.p.Limit)
	if err == nil {
		return Result{Decision: d}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return Result{Degraded: true}
	}
	l.logError(err)
	return Result{Degraded: true}
}

func (l *Limiter) logError(err error) {
	now := l.p.Clock.Now().UnixNano()
	last := l.lastLog.Load()
	if last != 0 && now-last < int64(time.Second) {
		l.suppressed.Add(1)
		return
	}
	if !l.lastLog.CompareAndSwap(last, now) {
		l.suppressed.Add(1)
		return
	}
	l.p.Logger.Error("rate limit store failed; request admitted", "policy", l.p.ID, "error", err.Error(), "suppressed", l.suppressed.Swap(0))
}
