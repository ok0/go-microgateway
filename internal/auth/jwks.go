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

// KindJWKS is the kind name under which the runtime keeps a JWKS key source.
const KindJWKS = "auth.jwks"

// JWKSConfig configures a JWKS key source: the provider and key set URL, the algorithms accepted, the cache TTL, the minimum interval between failed attempts and the fetch timeout.
type JWKSConfig struct {
	Provider           string
	URL                string
	DisplayURL         string
	Algorithms         []string
	TTL                time.Duration
	MinRefreshInterval time.Duration
	Timeout            time.Duration
}

// JWKSDeps are the collaborators of a JWKS. A nil Clock means the real clock, a nil Logger discards log output, and OnRefresh, when set, is called with the outcome of each refresh.
type JWKSDeps struct {
	Fetcher   Fetcher
	Clock     clock.Clock
	Logger    *slog.Logger
	OnRefresh func(ok bool)
}

// JWKS is a KeySource over a provider's key set. It fetches the set on first use and refreshes it when the cache expires or a key id is unknown. Once a set is loaded, keys in it are served at once: a due refresh runs in the background, and only a lookup of an id not in the set waits for a refresh. A failed attempt delays the next one by MinRefreshInterval, and a failed refresh keeps the last good keys.
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
	// awaitFetch observes the end of a fetch; tests replace it to force one observation order.
	awaitFetch func(fctx context.Context, res <-chan fetchResult) (fetchResult, bool)
}

type flight struct {
	done    chan struct{}
	waiters int
}

// NewJWKS returns a JWKS for c and d. It errors when the fetcher or a required configuration value is missing or not positive.
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
	return &JWKS{cfg: c, deps: d, ctx: ctx, cancel: cancel, awaitFetch: awaitFetchResult}, nil
}

// Lookup returns the key with id kid. With a loaded key set, a key in the set is returned at once and a refresh that is due is started without being awaited. An id not in the set waits for the refresh in flight, or for a refresh it starts once MinRefreshInterval has passed since the last attempt; the wait ends with the refresh, which Timeout bounds, or with ctx. Before the first successful fetch every lookup waits for the fetch in flight. An empty kid selects the key only when the set holds exactly one key. It returns ErrKeysUnavailable when no key set was ever loaded and ErrUnknownKID when the id is not in the set.
func (j *JWKS) Lookup(ctx context.Context, kid string) (Key, error) {
	if err := j.load(ctx); err != nil {
		return Key{}, err
	}
	if kid == "" {
		return j.single()
	}
	if k, ok := j.byKID(kid); ok {
		return k, nil
	}
	if err := j.awaitUnknown(ctx); err != nil {
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

// Keys returns a copy of the current key set. With a loaded set it returns at once and a refresh that is due is started without being awaited; before the first successful fetch it waits for the fetch in flight. It returns ErrKeysUnavailable when no key set was ever loaded.
func (j *JWKS) Keys(ctx context.Context) ([]Key, error) {
	if err := j.load(ctx); err != nil {
		return nil, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.have {
		return nil, ErrKeysUnavailable
	}
	return slices.Clone(j.keys), nil
}

// Close stops the key source: a refresh in progress is cancelled and later lookups return ErrKeysUnavailable. It always returns nil.
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

// load starts a refresh when one is due. Before the first successful fetch it waits for the fetch in flight; once a
// set is loaded it never waits.
func (j *JWKS) load(ctx context.Context) error {
	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		return ErrKeysUnavailable
	}
	j.startDueLocked(false)
	if j.have || j.flight == nil {
		j.mu.Unlock()
		return nil
	}
	return j.wait(ctx, j.flight)
}

// awaitUnknown waits for the refresh in flight, or starts a refresh when one may be forced and waits for it. It
// returns at once when no refresh runs and none may start.
func (j *JWKS) awaitUnknown(ctx context.Context) error {
	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		return ErrKeysUnavailable
	}
	j.startDueLocked(true)
	if j.flight == nil {
		j.mu.Unlock()
		return nil
	}
	return j.wait(ctx, j.flight)
}

// startDueLocked starts a refresh when none is in flight and one is due: the set is missing or older than TTL and
// the last attempt succeeded or MinRefreshInterval has passed since it, or force is set and MinRefreshInterval has
// passed since the last attempt.
func (j *JWKS) startDueLocked(force bool) {
	if j.flight != nil {
		return
	}
	now := j.deps.Clock.Now()
	cooled := j.attemptAt.IsZero() || now.Sub(j.attemptAt) >= j.cfg.MinRefreshInterval
	expired := !j.have || now.Sub(j.goodAt) >= j.cfg.TTL
	if (expired && (cooled || !j.failed)) || (force && cooled) {
		j.startLocked(now)
	}
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

// errFetchTimedOut is the cause the fetch timer sets on the fetch context.
var errFetchTimedOut = errors.New("key set fetch timed out")

// awaitFetchResult returns the fetch result, or reports true when the fetch context ended first.
func awaitFetchResult(fctx context.Context, res <-chan fetchResult) (fetchResult, bool) {
	select {
	case r := <-res:
		return r, false
	case <-fctx.Done():
		return fetchResult{}, true
	}
}

func (j *JWKS) run(f *flight) {
	fctx, cancel := context.WithCancelCause(j.ctx)
	timer := j.deps.Clock.AfterFunc(j.cfg.Timeout, func() { cancel(errFetchTimedOut) })

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

	// The timer records the timeout as the cause of the fetch context, so a failed fetch is classified by what
	// ended it, whichever of the result and the end of the context is observed first.
	r, ended := j.awaitFetch(fctx, res)
	timer.Stop()
	if ended || r.err != nil {
		switch cause := context.Cause(fctx); {
		case errors.Is(cause, errFetchTimedOut):
			r.err = fmt.Errorf("key set fetch timed out after %s", j.cfg.Timeout)
		case cause != nil:
			r.err = errors.New("key set fetch cancelled")
		}
	}
	cancel(nil)

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
	// The flight ends with the swap, so a reported refresh never holds back the next one.
	j.flight = nil
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

	// Waiters are released after the outcome has been reported and logged.
	close(f.done)
}
