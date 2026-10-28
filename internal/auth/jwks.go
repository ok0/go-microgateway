package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
)

const KindJWKS = "auth.jwks"

type JWKSConfig struct {
	Provider           string
	URL                string
	DisplayURL         string
	Algorithms         []string
	TTL                time.Duration
	MinRefreshInterval time.Duration
	Timeout            time.Duration
}

type JWKSDeps struct {
	Fetcher   Fetcher
	Clock     clock.Clock
	Logger    *slog.Logger
	OnRefresh func(ok bool)
}

type JWKS struct {
	cfg  JWKSConfig
	deps JWKSDeps

	ctx    context.Context
	cancel context.CancelFunc

	mu        sync.Mutex
	keys      []Key
	byID      map[string]Key
	have      bool
	goodAt    time.Time
	attemptAt time.Time
	failed    bool
	flight    *flight
	closed    bool
}

type flight struct {
	done    chan struct{}
	waiters int
}

func NewJWKS(c JWKSConfig, d JWKSDeps) (*JWKS, error) {
	switch {
	case d.Fetcher == nil:
		return nil, errors.New("jwks: fetcher is required")
	case c.Provider == "":
		return nil, errors.New("jwks: provider is required")
	case c.URL == "":
		return nil, errors.New("jwks: url is required")
	case len(c.Algorithms) == 0:
		return nil, errors.New("jwks: at least one algorithm is required")
	case c.TTL <= 0:
		return nil, errors.New("jwks: ttl must be positive")
	case c.MinRefreshInterval <= 0:
		return nil, errors.New("jwks: min refresh interval must be positive")
	case c.Timeout <= 0:
		return nil, errors.New("jwks: timeout must be positive")
	}
	c.Algorithms = slices.Clone(c.Algorithms)
	if d.Clock == nil {
		d.Clock = clock.Real()
	}
	if d.Logger == nil {
		d.Logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &JWKS{cfg: c, deps: d, ctx: ctx, cancel: cancel}, nil
}

func (j *JWKS) Lookup(ctx context.Context, kid string) (Key, error) {
	if err := j.ensure(ctx, false); err != nil {
		return Key{}, err
	}
	if kid == "" {
		return j.single()
	}
	if k, ok := j.byKID(kid); ok {
		return k, nil
	}
	if err := j.ensure(ctx, true); err != nil {
		return Key{}, err
	}
	if k, ok := j.byKID(kid); ok {
		return k, nil
	}
	if !j.hasKeys() {
		return Key{}, ErrKeysUnavailable
	}
	return Key{}, ErrUnknownKID
}

func (j *JWKS) Keys(ctx context.Context) ([]Key, error) {
	if err := j.ensure(ctx, false); err != nil {
		return nil, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.have {
		return nil, ErrKeysUnavailable
	}
	return slices.Clone(j.keys), nil
}

func (j *JWKS) Close() error {
	j.mu.Lock()
	j.closed = true
	j.mu.Unlock()
	j.cancel()
	return nil
}

func (j *JWKS) single() (Key, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.have {
		return Key{}, ErrKeysUnavailable
	}
	if len(j.keys) == 1 {
		return j.keys[0], nil
	}
	return Key{}, ErrUnknownKID
}

func (j *JWKS) byKID(kid string) (Key, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	k, ok := j.byID[kid]
	return k, ok
}

func (j *JWKS) hasKeys() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.have
}

func (j *JWKS) ensure(ctx context.Context, force bool) error {
	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		return ErrKeysUnavailable
	}
	if j.flight == nil {
		now := j.deps.Clock.Now()
		cooled := j.attemptAt.IsZero() || now.Sub(j.attemptAt) >= j.cfg.MinRefreshInterval
		expired := !j.have || now.Sub(j.goodAt) >= j.cfg.TTL
		if (expired && (cooled || !j.failed)) || (force && cooled) {
			j.startLocked(now)
		}
	}
	if j.flight == nil {
		j.mu.Unlock()
		return nil
	}
	return j.wait(ctx, j.flight)
}

func (j *JWKS) wait(ctx context.Context, f *flight) error {
	f.waiters++
	j.mu.Unlock()

	var err error
	select {
	case <-f.done:
	case <-ctx.Done():
		err = fmt.Errorf("%w: %w", ErrKeysUnavailable, ctx.Err())
	}

	j.mu.Lock()
	f.waiters--
	j.mu.Unlock()
	return err
}

func (j *JWKS) startLocked(now time.Time) {
	j.attemptAt = now
	f := &flight{done: make(chan struct{})}
	j.flight = f
	go j.run(f)
}

type fetchResult struct {
	data []byte
	err  error
}

func (j *JWKS) run(f *flight) {
	fctx, cancel := context.WithCancel(j.ctx)
	timer := j.deps.Clock.AfterFunc(j.cfg.Timeout, cancel)

	res := make(chan fetchResult, 1)
	go func() {
		defer func() {
			if recover() != nil {
				res <- fetchResult{err: errors.New("key set fetcher panicked")}
			}
		}()
		data, err := j.deps.Fetcher.FetchKeySet(fctx, j.cfg.Provider, j.cfg.URL)
		res <- fetchResult{data: data, err: err}
	}()

	var r fetchResult
	select {
	case r = <-res:
	case <-fctx.Done():
		if j.ctx.Err() != nil {
			r.err = errors.New("key set fetch cancelled")
		} else {
			r.err = fmt.Errorf("key set fetch timed out after %s", j.cfg.Timeout)
		}
	}
	timer.Stop()
	cancel()

	err := r.err
	var keys []Key
	if err == nil {
		keys, err = ParseJWKS(r.data, j.cfg.Algorithms)
	}

	j.mu.Lock()
	if err == nil {
		j.keys = keys
		j.byID = make(map[string]Key, len(keys))
		for _, k := range keys {
			if k.ID != "" {
				j.byID[k.ID] = k
			}
		}
		j.have = true
		j.goodAt = j.deps.Clock.Now()
		j.failed = false
	} else {
		j.failed = true
	}
	have := j.have
	j.mu.Unlock()

	if j.deps.OnRefresh != nil {
		j.deps.OnRefresh(err == nil)
	}
	if err == nil {
		j.deps.Logger.Debug("jwks refreshed", "provider", j.cfg.Provider, "keys", len(keys))
	} else {
		j.deps.Logger.Warn("jwks refresh failed",
			"provider", j.cfg.Provider,
			"url", j.cfg.DisplayURL,
			"error", redact(err.Error(), j.cfg.URL),
			"keeping_last_good", have,
		)
	}

	j.mu.Lock()
	j.flight = nil
	close(f.done)
	j.mu.Unlock()
}
