package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/state"
	"github.com/ok0/go-microgateway/internal/storeguard"
	"github.com/ok0/go-microgateway/store"
)

// KindStore is the state kind of a store backend.
const KindStore = "store"

// KindStoreGuard is the state kind of a store guard.
const KindStoreGuard = "storeguard"

// StoreType describes a registered store type. Caps lists the optional features its backends provide,
// and Build creates the backend of one configured entry of the type.
type StoreType struct {
	Caps  store.Capabilities
	Build func(e *config.Store, logger *slog.Logger) (store.Backend, error)
}

// StoreSet holds the registered store types and records which guard entry owns each store name, so that
// only the newest guard of a name drives its availability gauge.
type StoreSet struct {
	types  map[string]StoreType
	mu     sync.Mutex
	owners map[string]*guardEntry
}

// NewStoreSet returns a StoreSet registering a copy of types. A nil map registers no types.
func NewStoreSet(types map[string]StoreType) *StoreSet {
	return &StoreSet{types: maps.Clone(types), owners: make(map[string]*guardEntry)}
}

func (s *StoreSet) lookup(typ string) (StoreType, bool) {
	if s == nil {
		return StoreType{}, false
	}
	t, ok := s.types[typ]
	return t, ok
}

// Resolve reports store types that are not registered and rate-limit routes whose store type does not
// support rate limiting. It constructs nothing. A nil StoreSet registers no types.
func (s *StoreSet) Resolve(cfg *config.Config) []config.FieldError {
	var errs []config.FieldError
	first := make(map[string]*config.Store)
	for i := range cfg.Stores {
		e := &cfg.Stores[i]
		if _, ok := first[e.Name]; !ok {
			first[e.Name] = e
		}
		if e.Type == "" {
			continue
		}
		if _, ok := s.lookup(e.Type); !ok {
			path := fmt.Sprintf("stores[%d].type", i)
			errs = append(errs, config.FieldError{Path: path, Line: cfg.Line(path), Msg: fmt.Sprintf("store type %q is not registered", e.Type)})
		}
	}
	for j := range cfg.Routes {
		rl := cfg.Routes[j].RateLimit
		if rl == nil || rl.Store == "" {
			continue
		}
		e, ok := first[rl.Store]
		if !ok {
			continue
		}
		t, ok := s.lookup(e.Type)
		if !ok || t.Caps&store.CapRateLimit != 0 {
			continue
		}
		path := fmt.Sprintf("routes[%d].rate_limit.store", j)
		errs = append(errs, config.FieldError{Path: path, Line: cfg.Line(path), Msg: fmt.Sprintf("store %q (type %q) does not support rate limiting", rl.Store, e.Type)})
	}
	if cfg.Cache.Store != "" {
		if e, ok := first[cfg.Cache.Store]; ok {
			if t, ok := s.lookup(e.Type); ok && t.Caps&store.CapCache == 0 {
				path := "cache.store"
				errs = append(errs, config.FieldError{Path: path, Line: cfg.Line(path), Msg: fmt.Sprintf("store %q (type %q) does not support caching", cfg.Cache.Store, e.Type)})
			}
		}
	}
	return errs
}

// storeError is an error that carries scrubbed text. The raw error of a backend or factory is never retained.
type storeError struct {
	msg  string
	kind error
}

func (e *storeError) Error() string { return e.msg }

func (e *storeError) Unwrap() error { return e.kind }

var storeErrorKinds = []error{store.ErrInvalidLimit, store.ErrInvalidEntry, store.ErrInvalidPurge, store.ErrUnavailable, context.Canceled, context.DeadlineExceeded}

func classify(err error) error {
	for _, k := range storeErrorKinds {
		if errors.Is(err, k) {
			return k
		}
	}
	return nil
}

// storeBackend wraps a store backend so that its errors and panics reach the runtime only as scrubbed storeErrors.
type storeBackend struct {
	name string
	b    store.Backend
	rl   store.RateLimiter
	c    store.Cache
	sc   *scrubber
}

func (s *storeBackend) Take(ctx context.Context, key string, l store.Limit) (d store.Decision, err error) {
	defer func() {
		if p := recover(); p != nil {
			d = store.Decision{}
			err = &storeError{msg: fmt.Sprintf("store %q: take panicked: %s", s.name, s.sc.text(fmt.Sprint(p))), kind: store.ErrUnavailable}
		}
	}()
	d, err = s.rl.Take(ctx, key, l)
	if err != nil {
		return store.Decision{}, s.wrap("take", err)
	}
	return d, nil
}

func (s *storeBackend) Get(ctx context.Context, primary string, sel func(varyNames []string) (string, bool)) (lk store.Lookup, err error) {
	defer func() {
		if p := recover(); p != nil {
			lk = store.Lookup{}
			err = s.panicError("get", p)
		}
	}()
	lk, err = s.c.Get(ctx, primary, sel)
	if err != nil {
		return store.Lookup{}, s.wrap("get", err)
	}
	return lk, nil
}

func (s *storeBackend) Put(ctx context.Context, tok store.FillToken, primary, secondary string, varyNames []string, e *store.Entry, ttl time.Duration) (stored bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			stored = false
			err = s.panicError("put", p)
		}
	}()
	stored, err = s.c.Put(ctx, tok, primary, secondary, varyNames, e, ttl)
	if err != nil {
		return false, s.wrap("put", err)
	}
	return stored, nil
}

func (s *storeBackend) Purge(ctx context.Context, p store.Purge) (res store.PurgeResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			res = store.PurgeResult{}
			err = s.panicError("purge", r)
		}
	}()
	res, err = s.c.Purge(ctx, p)
	if err != nil {
		return store.PurgeResult{}, s.wrap("purge", err)
	}
	return res, nil
}

func (s *storeBackend) Delete(ctx context.Context, primary, secondary string) (deleted bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			deleted = false
			err = s.panicError("delete", p)
		}
	}()
	deleted, err = s.c.Delete(ctx, primary, secondary)
	if err != nil {
		return false, s.wrap("delete", err)
	}
	return deleted, nil
}

func (s *storeBackend) panicError(op string, p any) error {
	return &storeError{msg: fmt.Sprintf("store %q: %s panicked: %s", s.name, op, s.sc.text(fmt.Sprint(p))), kind: store.ErrUnavailable}
}

func (s *storeBackend) Close() error {
	if err := s.b.Close(); err != nil {
		return s.wrap("close", err)
	}
	return nil
}

func (s *storeBackend) wrap(op string, err error) error {
	kind := classify(err)
	text := s.sc.text(err.Error())
	if kind == context.Canceled || kind == context.DeadlineExceeded {
		text = kind.Error()
	}
	return &storeError{msg: fmt.Sprintf("store %q: %s: %s", s.name, op, text), kind: kind}
}

func newStoreBackend(e *config.Store, t StoreType, logger *slog.Logger) (*storeBackend, error) {
	sc := newScrubber(e)
	b, err := callFactory(e, t, logger, sc)
	if err != nil {
		return nil, err
	}
	if isNilBackend(b) {
		return nil, &storeError{msg: "store factory returned a nil backend"}
	}
	sb := &storeBackend{name: e.Name, b: b, sc: sc}
	if t.Caps&store.CapRateLimit != 0 {
		rl, ok := b.(store.RateLimiter)
		if !ok {
			_ = b.Close()
			return nil, &storeError{msg: fmt.Sprintf("store type %q declares rate limiting but the backend (%T) does not implement store.RateLimiter", e.Type, b)}
		}
		sb.rl = rl
	}
	if t.Caps&store.CapCache != 0 {
		c, ok := b.(store.Cache)
		if !ok {
			_ = b.Close()
			return nil, &storeError{msg: fmt.Sprintf("store type %q declares caching but the backend (%T) does not implement store.Cache", e.Type, b)}
		}
		sb.c = c
	}
	return sb, nil
}

func callFactory(e *config.Store, t StoreType, logger *slog.Logger, sc *scrubber) (b store.Backend, err error) {
	defer func() {
		if p := recover(); p != nil {
			b = nil
			err = &storeError{msg: "store factory panicked: " + sc.text(fmt.Sprint(p))}
		}
	}()
	b, err = t.Build(e, logger)
	if err != nil {
		return nil, &storeError{msg: "store factory failed: " + sc.text(err.Error())}
	}
	return b, nil
}

func isNilBackend(b store.Backend) bool {
	if b == nil {
		return true
	}
	v := reflect.ValueOf(b)
	return v.Kind() == reflect.Pointer && v.IsNil()
}

// Store uses recorded on a guard entry: useRateLimit for rate-limit routes and useCache for the response cache.
const (
	useRateLimit uint32 = 1 << iota
	useCache
)

// guardEntry is the guard of one store name together with the state it reports to.
type guardEntry struct {
	name     string
	g        *storeguard.Guard
	set      *StoreSet
	avail    *metrics.GaugeVec
	logger   *slog.Logger
	policies atomic.Pointer[string]
	uses     atomic.Uint32
}

func newGuardEntry(e *config.Store, set *StoreSet, clk clock.Clock, logger *slog.Logger, avail *metrics.GaugeVec) (*guardEntry, error) {
	ge := &guardEntry{name: e.Name, set: set, avail: avail, logger: logger}
	g, err := storeguard.New(e.Name, storeguard.Settings{
		ConsecutiveFailures: *e.Guard.ConsecutiveFailures,
		OpenTimeout:         e.Guard.OpenTimeout.Std(),
		MaxInflight:         *e.Guard.MaxInflight,
	}, clk, ge.changed)
	if err != nil {
		return nil, err
	}
	ge.g = g
	return ge, nil
}

func (ge *guardEntry) changed(avail bool) {
	ge.set.mu.Lock()
	defer ge.set.mu.Unlock()
	if ge.set.owners[ge.name] != ge {
		return
	}
	v := 0.0
	if avail {
		v = 1
	}
	ge.avail.With(ge.name).Set(v)
	uses := ge.uses.Load()
	if avail {
		if uses&useRateLimit != 0 {
			ge.logger.Info("rate limit store recovered; shared limits resumed", "store", ge.name, "policies", ge.policyList())
		}
		if uses&useCache != 0 {
			ge.logger.Info("cache store recovered; cache resumed", "store", ge.name)
		}
		return
	}
	if uses&useRateLimit != 0 {
		ge.logger.Warn("rate limit store unavailable; failure policies active", "store", ge.name, "policies", ge.policyList())
	}
	if uses&useCache != 0 {
		ge.logger.Warn("cache store unavailable; requests bypass the cache", "store", ge.name)
	}
}

func (ge *guardEntry) policyList() string {
	if p := ge.policies.Load(); p != nil {
		return *p
	}
	return ""
}

// own makes ge the owner of its store name and sets the availability gauge from its current state. uses is the
// combination of useRateLimit and useCache for the features built on the store.
func (s *StoreSet) own(ge *guardEntry, policies string, uses uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.owners[ge.name] = ge
	ge.policies.Store(&policies)
	ge.uses.Store(uses)
	v := 0.0
	if ge.g.Available() {
		v = 1
	}
	ge.avail.With(ge.name).Set(v)
}

// prune forgets the owners of store names not in keep and deletes their availability series.
func (s *StoreSet) prune(keep map[string]bool, avail *metrics.GaugeVec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name := range s.owners {
		if !keep[name] {
			delete(s.owners, name)
		}
	}
	avail.DeleteFunc(func(v []string) bool { return !keep[v[0]] })
}

// builtStore is a store backend and guard built for one generation.
type builtStore struct {
	name        string
	backend     *storeBackend
	guard       *guardEntry
	openTimeout time.Duration
	policies    string
	rateLimit   bool
	cache       bool
}

func (b *builtStore) uses() uint32 {
	var u uint32
	if b.rateLimit {
		u |= useRateLimit
	}
	if b.cache {
		u |= useCache
	}
	return u
}

// buildStores builds the backend and guard of every store that a rate-limit route references and whose
// type supports rate limiting, and of the cache store when caching is enabled and its type supports caching.
// Each entry is built at most once and is reused by state when unchanged.
func buildStores(cfg *config.Config, set *StoreSet, tx *state.Tx, clk clock.Clock, logger *slog.Logger, series *Series) (map[string]*builtStore, []config.FieldError) {
	referenced := make(map[string]bool)
	for i := range cfg.Routes {
		if rl := cfg.Routes[i].RateLimit; rl != nil && rl.Store != "" {
			referenced[rl.Store] = true
		}
	}
	cacheRef := cacheEnabled(cfg) && cfg.Cache.Store != ""
	built := make(map[string]*builtStore)
	seen := make(map[string]bool)
	var errs []config.FieldError
	for i := range cfg.Stores {
		e := &cfg.Stores[i]
		if seen[e.Name] {
			continue
		}
		seen[e.Name] = true
		rateLimit := referenced[e.Name]
		cache := cacheRef && e.Name == cfg.Cache.Store
		if !rateLimit && !cache {
			continue
		}
		t, ok := set.lookup(e.Type)
		if !ok {
			continue
		}
		rateLimit = rateLimit && t.Caps&store.CapRateLimit != 0
		cache = cache && t.Caps&store.CapCache != 0
		if !rateLimit && !cache {
			continue
		}
		path := fmt.Sprintf("stores[%d]", i)
		backendFP := e.BackendFingerprint()
		sb, err := state.Get(tx, KindStore, e.Name, backendFP, func() (*storeBackend, error) {
			return newStoreBackend(e, t, logger.With("store", e.Name))
		})
		if err != nil {
			errs = append(errs, config.FieldError{Path: path, Line: cfg.Line(path), Msg: err.Error()})
			continue
		}
		guardFP := fp(struct {
			Guard   config.StoreGuard
			Backend string
		}{e.Guard, backendFP})
		ge, err := state.Get(tx, KindStoreGuard, e.Name, guardFP, func() (*guardEntry, error) {
			return newGuardEntry(e, set, clk, logger, series.StoreAvailable)
		})
		if err != nil {
			errs = append(errs, config.FieldError{Path: path, Line: cfg.Line(path), Msg: err.Error()})
			continue
		}
		built[e.Name] = &builtStore{name: e.Name, backend: sb, guard: ge, openTimeout: e.Guard.OpenTimeout.Std(), rateLimit: rateLimit, cache: cache}
	}
	return built, errs
}
