package cache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/storeguard"
	"github.com/ok0/go-microgateway/store"
)

// Purge types and scopes reported in PurgeResult.
const (
	PurgeTypeURL  = "url"
	PurgeTypeTag  = "tag"
	PurgeTypeAll  = "all"
	ScopeInstance = "instance" // the built-in memory cache: only the instance that ran the purge is affected
	ScopeStore    = "store"    // a named store: the effect is the store's, shared by every instance using it
)

const (
	modeHard = "hard"
	modeSoft = "soft"
)

// ErrInvalidRequest is wrapped by every RequestError. Nothing was purged.
var ErrInvalidRequest = errors.New("cache: invalid purge request")

// ErrCacheDisabled reports that no route of the configuration enables caching, so there is nothing to purge.
var ErrCacheDisabled = errors.New("cache: no route enables caching")

// RequestError reports why a purge request was rejected; nothing was purged. Reason names fields and indexes only,
// never values.
type RequestError struct{ Reason string }

// Error returns the message of the rejected request.
func (e *RequestError) Error() string {
	return "cache: invalid purge request: " + e.Reason
}

// Unwrap returns ErrInvalidRequest.
func (e *RequestError) Unwrap() error {
	return ErrInvalidRequest
}

// PurgeRequest selects the cached responses a purge covers: exactly one of URLs (absolute http or https URLs, every
// Vary variant of the GET and HEAD entries), Tags (surrogate keys) or All. Soft marks entries invalid instead of
// removing them.
type PurgeRequest struct {
	URLs []string
	Tags []string
	All  bool
	Soft bool
}

// PurgeResult reports a purge the store acknowledged.
type PurgeResult struct {
	// Type is PurgeTypeURL, PurgeTypeTag or PurgeTypeAll.
	Type string
	// Soft reports whether the purge was soft.
	Soft bool
	// Accepted is the number of distinct selectors after canonicalization, or 1 for all.
	Accepted int
	// Affected is the store's count of changed entries, summed over its calls. It is 0 when AffectedKnown is false.
	Affected int
	// AffectedKnown reports whether Affected is exact.
	AffectedKnown bool
	// Scope is ScopeInstance or ScopeStore.
	Scope string
}

// Retention is the purge marker lifetime of a gateway's cache: the longest storage TTL of every configuration applied
// since it was created, clamped to store.MaxTTL. Its zero value is ready; it is safe for concurrent use.
type Retention struct{ hw atomic.Int64 }

// Raise lifts the high-water mark to d, clamped to store.MaxTTL. A d that is not positive changes nothing.
func (r *Retention) Raise(d time.Duration) {
	if d <= 0 {
		return
	}
	d = min(d, store.MaxTTL)
	for {
		cur := r.hw.Load()
		if int64(d) <= cur || r.hw.CompareAndSwap(cur, int64(d)) {
			return
		}
	}
}

// At returns the marker lifetime of a purge whose own floor is floor: the greater of the high-water mark and floor,
// both clamped to store.MaxTTL. A nil Retention gives the clamped floor alone.
func (r *Retention) At(floor time.Duration) time.Duration {
	f := min(floor, store.MaxTTL)
	if r == nil {
		return f
	}
	return max(time.Duration(r.hw.Load()), f)
}

// PurgeMetrics holds the families the purge service reports to. Each is required.
type PurgeMetrics struct {
	// Purges counts successful purge calls by type and mode, hard or soft.
	Purges *metrics.CounterVec
	// StoreErrors counts failed store calls by operation. The service uses "purge".
	StoreErrors *metrics.CounterVec
}

// PurgerDeps holds the collaborators of a Purger.
type PurgerDeps struct {
	// Store is the cache the purges are applied to.
	Store store.Cache
	// Guard bounds each store call. Nil calls Store directly.
	Guard *storeguard.Guard
	// StoreName names a configured store. It is empty for the built-in memory cache.
	StoreName string
	// Timeout is cache.store_timeout. It bounds each guarded call.
	Timeout time.Duration
	// Retain is the MaxStorageTTL of the generation's configuration, the floor of the marker lifetime.
	Retain time.Duration
	// Retention, if set, raises the marker lifetime to the high-water shared by the gateway. Nil uses Retain alone.
	Retention *Retention
	// OnStoreError, if set, is called once for every counted store failure, with "purge".
	OnStoreError func(op string)
	// Metrics receives the purge counters.
	Metrics *PurgeMetrics
	// Logger receives the purge records. Nil discards them.
	Logger *slog.Logger
}

// purgeKey identifies one purge counter series.
type purgeKey struct{ typ, mode string }

// Purger applies purge requests to the cache of one generation. It is safe for concurrent use.
type Purger struct {
	store        store.Cache
	guard        *storeguard.Guard
	timeout      time.Duration
	retain       time.Duration
	retention    *Retention
	label        string
	scope        string
	onStoreError func(op string)
	logger       *slog.Logger
	purges       map[purgeKey]*metrics.Counter
	storeErrs    *metrics.Counter
}

// unavailableError reports a purge the store did not apply. Its text names the store and the reason only.
type unavailableError struct {
	store  string
	reason string
}

func (e *unavailableError) Error() string {
	return fmt.Sprintf("cache: purge failed: store %q unavailable (%s)", e.store, e.reason)
}

func (e *unavailableError) Unwrap() error {
	return store.ErrUnavailable
}

// NewPurger returns the purge service over d.Store. It returns an error when a required collaborator is missing.
func NewPurger(d PurgerDeps) (*Purger, error) {
	switch {
	case d.Store == nil:
		return nil, errors.New("cache: store is nil")
	case d.Metrics == nil || d.Metrics.Purges == nil || d.Metrics.StoreErrors == nil:
		return nil, errors.New("cache: purge metrics are incomplete")
	case d.Retain <= 0:
		return nil, errors.New("cache: retain must be positive")
	case d.Guard != nil && d.Timeout <= 0:
		return nil, errors.New("cache: store timeout must be positive")
	}
	logger := d.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	p := &Purger{
		store:        d.Store,
		guard:        d.Guard,
		timeout:      d.Timeout,
		retain:       d.Retain,
		retention:    d.Retention,
		label:        memoryStoreLabel,
		scope:        ScopeInstance,
		onStoreError: d.OnStoreError,
		logger:       logger,
		purges:       make(map[purgeKey]*metrics.Counter, 6),
		storeErrs:    d.Metrics.StoreErrors.With(opPurge),
	}
	if d.StoreName != "" {
		p.label = d.StoreName
		p.scope = ScopeStore
	}
	for _, typ := range []string{PurgeTypeURL, PurgeTypeTag, PurgeTypeAll} {
		for _, mode := range []string{modeHard, modeSoft} {
			p.purges[purgeKey{typ, mode}] = d.Metrics.Purges.With(typ, mode)
		}
	}
	return p, nil
}

// Scope returns ScopeInstance for the built-in memory cache and ScopeStore for a named store.
func (p *Purger) Scope() string {
	return p.scope
}

// Purge applies req and reports the result the store acknowledged. keyID names the admin key that asked for the
// purge, or is empty for a library call; it is only logged.
//
// A URL request is sent as calls of at most store.MaxPurgeSelectors keys, each URL's GET and HEAD keys in the same
// call. A tag request and an all request are one call each. Every call is bounded by the guard and the store timeout,
// and the affected counts of the calls are summed. Calls are never retried. A failure part-way leaves the earlier
// calls applied and sends no later call; hard and soft purges are idempotent, so repeating the request is safe.
//
// When ctx ends before the purge completes, Purge returns ctx's error and counts and logs nothing; earlier calls may
// have been applied. Purge reads no clock and passes no timestamp. The marker lifetime is the greater of the Retain
// of the configuration and the gateway's high-water mark. A purge of the built-in memory cache reaches only this
// instance (ScopeInstance); a named store's purge has that store's reach (ScopeStore).
func (p *Purger) Purge(ctx context.Context, req PurgeRequest, keyID string) (PurgeResult, error) {
	typ, calls, accepted, err := plan(req)
	if err != nil {
		return PurgeResult{}, err
	}
	mode := modeHard
	if req.Soft {
		mode = modeSoft
	}
	retain := p.retention.At(p.retain)
	for i := range calls {
		calls[i].Soft = req.Soft
		calls[i].Retain = retain
		if calls[i].Validate() != nil {
			return PurgeResult{}, &RequestError{Reason: "selector rejected by the cache store"}
		}
	}
	affected, known := 0, true
	for _, c := range calls {
		r, err := callPurge(ctx, p.store, p.guard, p.timeout, c)
		if err != nil {
			return PurgeResult{}, p.fail(ctx, typ, mode, keyID, err)
		}
		affected += r.Affected
		known = known && r.AffectedKnown
	}
	if err := ctx.Err(); err != nil {
		return PurgeResult{}, err
	}
	res := PurgeResult{Type: typ, Soft: req.Soft, Accepted: accepted, AffectedKnown: known, Scope: p.scope}
	var loggedAffected any = "unknown"
	if known {
		res.Affected = affected
		loggedAffected = affected
	}
	p.purges[purgeKey{typ, mode}].Inc()
	p.logger.Info("cache purged", "key_id", keyID, "type", typ, "mode", mode, "accepted", accepted,
		"affected", loggedAffected, "scope", p.scope, "store", p.label)
	return res, nil
}

// fail accounts for a failed purge call and returns the error Purge reports. A call that ended because ctx was
// canceled is not counted or logged. Refusals by an open or overloaded guard are logged but not counted as store
// failures. The store's own error text is not logged: a backend may echo keys in it.
func (p *Purger) fail(ctx context.Context, typ, mode, keyID string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	reason := "error"
	var ge *storeguard.Error
	if errors.As(err, &ge) {
		reason = string(ge.Reason)
	}
	if !storeguard.ShortCircuit(err) {
		p.storeErrs.Inc()
		if p.onStoreError != nil {
			p.onStoreError(opPurge)
		}
	}
	p.logger.Warn("cache purge failed; store unavailable", "key_id", keyID, "type", typ, "mode", mode,
		"store", p.label, "reason", reason)
	return &unavailableError{store: p.label, reason: reason}
}

// plan validates req and splits it into store calls. It returns the purge type, the calls and the accepted count.
func plan(req PurgeRequest) (string, []store.Purge, int, error) {
	selectors := 0
	if len(req.URLs) > 0 {
		selectors++
	}
	if len(req.Tags) > 0 {
		selectors++
	}
	if req.All {
		selectors++
	}
	switch {
	case selectors == 0:
		return "", nil, 0, &RequestError{Reason: "no selector: set urls, tags or all"}
	case selectors > 1:
		return "", nil, 0, &RequestError{Reason: "more than one selector"}
	case len(req.URLs) > 0:
		calls, accepted, err := planURLs(req.URLs)
		return PurgeTypeURL, calls, accepted, err
	case len(req.Tags) > 0:
		calls, accepted, err := planTags(req.Tags)
		return PurgeTypeTag, calls, accepted, err
	}
	return PurgeTypeAll, []store.Purge{{Kind: store.PurgeAll}}, 1, nil
}

// planURLs returns the key calls of urls, de-duplicated by GET key, and the number of distinct URLs.
func planURLs(urls []string) ([]store.Purge, int, error) {
	if len(urls) > store.MaxPurgeSelectors {
		return nil, 0, &RequestError{Reason: fmt.Sprintf("more than %d urls", store.MaxPurgeSelectors)}
	}
	seen := make(map[string]bool, len(urls))
	var keys []string
	for i, u := range urls {
		ks, err := httpcache.URLKeys(u)
		if err != nil {
			var ue *httpcache.URLError
			if errors.As(err, &ue) {
				return nil, 0, &RequestError{Reason: fmt.Sprintf("urls[%d]: %s", i, ue.Reason)}
			}
			return nil, 0, &RequestError{Reason: fmt.Sprintf("urls[%d]: invalid URL", i)}
		}
		for _, k := range ks {
			if len(k) > store.MaxKeyBytes {
				return nil, 0, &RequestError{Reason: fmt.Sprintf("urls[%d]: key longer than %d bytes", i, store.MaxKeyBytes)}
			}
		}
		if seen[ks[0]] {
			continue
		}
		seen[ks[0]] = true
		keys = append(keys, ks...)
	}
	var calls []store.Purge
	for len(keys) > 0 {
		n := min(len(keys), store.MaxPurgeSelectors)
		calls = append(calls, store.Purge{Kind: store.PurgeKeys, Keys: keys[:n:n]})
		keys = keys[n:]
	}
	return calls, len(seen), nil
}

// planTags returns the single tag call of tags, de-duplicated in order, and the number of distinct tags.
func planTags(tags []string) ([]store.Purge, int, error) {
	if len(tags) > store.MaxPurgeSelectors {
		return nil, 0, &RequestError{Reason: fmt.Sprintf("more than %d tags", store.MaxPurgeSelectors)}
	}
	seen := make(map[string]bool, len(tags))
	var out []string
	for i, t := range tags {
		switch {
		case t == "":
			return nil, 0, &RequestError{Reason: fmt.Sprintf("tags[%d]: is empty", i)}
		case len(t) > store.MaxTagBytes:
			return nil, 0, &RequestError{Reason: fmt.Sprintf("tags[%d]: longer than %d bytes", i, store.MaxTagBytes)}
		case strings.ContainsFunc(t, func(r rune) bool { return r < 0x20 || r == 0x7f }):
			return nil, 0, &RequestError{Reason: fmt.Sprintf("tags[%d]: contains a control character", i)}
		}
		if seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	return []store.Purge{{Kind: store.PurgeTags, Tags: out}}, len(out), nil
}
