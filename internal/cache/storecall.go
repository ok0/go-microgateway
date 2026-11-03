package cache

import (
	"context"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/storeguard"
	"github.com/ok0/go-microgateway/store"
)

// get looks up primary through the store, bounded by the guard when one is set. sel is called at most once, and
// only on success is its result meaningful.
func (s *stage) get(ctx context.Context, primary string, sel func([]string) (string, bool)) (store.Lookup, error) {
	if s.guard == nil {
		return s.store.Get(ctx, primary, sel)
	}
	var lk store.Lookup
	err := s.guard.Do(ctx, s.timeout, func(cctx context.Context) error {
		l, err := s.store.Get(cctx, primary, sel)
		if err != nil {
			return err
		}
		lk = l
		return nil
	})
	if err != nil {
		return store.Lookup{}, err
	}
	return lk, nil
}

func (s *stage) put(ctx context.Context, tok store.FillToken, primary, secondary string, names []string, e *store.Entry, ttl time.Duration) (bool, error) {
	if s.guard == nil {
		return s.store.Put(ctx, tok, primary, secondary, names, e, ttl)
	}
	var stored bool
	err := s.guard.Do(ctx, s.timeout, func(cctx context.Context) error {
		ok, err := s.store.Put(cctx, tok, primary, secondary, names, e, ttl)
		if err != nil {
			return err
		}
		stored = ok
		return nil
	})
	if err != nil {
		return false, err
	}
	return stored, nil
}

func (s *stage) purge(ctx context.Context, p store.Purge) (store.PurgeResult, error) {
	if s.guard == nil {
		return s.store.Purge(ctx, p)
	}
	var res store.PurgeResult
	err := s.guard.Do(ctx, s.timeout, func(cctx context.Context) error {
		r, err := s.store.Purge(cctx, p)
		if err != nil {
			return err
		}
		res = r
		return nil
	})
	if err != nil {
		return store.PurgeResult{}, err
	}
	return res, nil
}

// fail accounts for a failed store call made with ctx. Calls refused by an open or overloaded guard are only
// logged. A call that ended because ctx was canceled is not counted. Every other failure is counted and logged.
func (s *stage) fail(ctx context.Context, op string, err error) {
	if storeguard.ShortCircuit(err) {
		s.errs.Log(s.storeName, op, err)
		return
	}
	if ctx.Err() != nil {
		return
	}
	s.storeErrs[op].Inc()
	if s.onStoreError != nil {
		s.onStoreError(op)
	}
	s.errs.Log(s.storeName, op, err)
}

// ErrorLog writes store failure records at most once per second of its clock. Records suppressed in between are
// counted and reported with the next record.
type ErrorLog struct {
	logger     *slog.Logger
	clk        clock.Clock
	last       atomic.Int64
	suppressed atomic.Int64
}

const noRecord = math.MinInt64

// NewErrorLog returns an ErrorLog that writes to logger and reads time from clk.
func NewErrorLog(logger *slog.Logger, clk clock.Clock) *ErrorLog {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	l := &ErrorLog{logger: logger, clk: clk}
	l.last.Store(noRecord)
	return l
}

// Log records one failed call of op on the store called name. An empty name denotes the built-in memory cache.
func (l *ErrorLog) Log(store, op string, err error) {
	n, ok := l.claim()
	if !ok {
		return
	}
	name := store
	if name == "" {
		name = memoryStoreLabel
	}
	l.logger.Warn("cache store call failed; request served without the cache",
		"store", name, "op", op, "error", err.Error(), "suppressed", n)
}

func (l *ErrorLog) claim() (int64, bool) {
	now := l.clk.Now().UnixNano()
	for {
		last := l.last.Load()
		if last != noRecord && now >= last && now-last < int64(time.Second) {
			l.suppressed.Add(1)
			return 0, false
		}
		if l.last.CompareAndSwap(last, now) {
			return l.suppressed.Swap(0), true
		}
	}
}
