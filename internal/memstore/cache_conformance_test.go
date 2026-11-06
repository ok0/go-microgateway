package memstore

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/store"
	"github.com/ok0/go-microgateway/store/storetest"
)

const brokenCacheEnv = "MICROGATEWAY_STORETEST_BROKEN_CACHE"

type cacheView struct {
	c      *Cache
	closed atomic.Bool
}

func (v *cacheView) Get(ctx context.Context, primary string, sel func(varyNames []string) (string, bool)) (store.Lookup, error) {
	if err := ctx.Err(); err != nil {
		return store.Lookup{}, err
	}
	if sel == nil {
		return store.Lookup{}, fmt.Errorf("%w: nil selector", store.ErrInvalidEntry)
	}
	if v.closed.Load() {
		return store.Lookup{}, fmt.Errorf("memstore cache view closed: %w", store.ErrUnavailable)
	}
	return v.c.Get(ctx, primary, sel)
}

func (v *cacheView) Put(ctx context.Context, tok store.FillToken, primary, secondary string, varyNames []string, e *store.Entry, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, err := store.ValidateFill(tok, primary, secondary, varyNames, e, ttl); err != nil {
		return false, err
	}
	if v.closed.Load() {
		return false, fmt.Errorf("memstore cache view closed: %w", store.ErrUnavailable)
	}
	return v.c.Put(ctx, tok, primary, secondary, varyNames, e, ttl)
}

func (v *cacheView) Purge(ctx context.Context, p store.Purge) (store.PurgeResult, error) {
	if err := ctx.Err(); err != nil {
		return store.PurgeResult{}, err
	}
	if err := p.Validate(); err != nil {
		return store.PurgeResult{}, err
	}
	if v.closed.Load() {
		return store.PurgeResult{}, fmt.Errorf("memstore cache view closed: %w", store.ErrUnavailable)
	}
	return v.c.Purge(ctx, p)
}

func (v *cacheView) Delete(ctx context.Context, primary, secondary string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := store.ValidateDelete(primary, secondary); err != nil {
		return false, err
	}
	if v.closed.Load() {
		return false, fmt.Errorf("memstore cache view closed: %w", store.ErrUnavailable)
	}
	return v.c.Delete(ctx, primary, secondary)
}

func (v *cacheView) Close() error {
	v.closed.Store(true)
	return nil
}

type cachePairBuilder func(t *testing.T, clk *clock.Fake) (a, b store.Cache, sweep func())

func newCacheHarness(build cachePairBuilder) storetest.Harness[store.Cache] {
	var clk *clock.Fake
	var sweep func()
	newAt := func(t *testing.T, start time.Time) (store.Cache, store.Cache) {
		clk = clock.NewFake(start)
		a, b, s := build(t, clk)
		sweep = s
		return a, b
	}
	return storetest.Harness[store.Cache]{
		New: func(t *testing.T) (store.Cache, store.Cache) {
			return newAt(t, epoch)
		},
		NewAt: newAt,
		Advance: func(t *testing.T, d time.Duration) {
			clk.Advance(d)
			sweep()
		},
		Resolution: time.Nanosecond,
		Horizon:    time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
	}
}

func sharedCachePair(t *testing.T, clk *clock.Fake) (store.Cache, store.Cache, func()) {
	t.Helper()
	c := NewCache(CacheOptions{MaxBytes: 1 << 27, Shards: 4, Clock: quiet(clk)})
	t.Cleanup(func() { _ = c.Close() })
	return &cacheView{c: c}, &cacheView{c: c}, func() { c.Sweep(clk.Now()) }
}

func perHandleCachePair(t *testing.T, clk *clock.Fake) (store.Cache, store.Cache, func()) {
	t.Helper()
	x := NewCache(CacheOptions{MaxBytes: 1 << 27, Shards: 4, Clock: quiet(clk)})
	y := NewCache(CacheOptions{MaxBytes: 1 << 27, Shards: 4, Clock: quiet(clk)})
	t.Cleanup(func() { _ = x.Close() })
	t.Cleanup(func() { _ = y.Close() })
	return x, y, func() {
		x.Sweep(clk.Now())
		y.Sweep(clk.Now())
	}
}

func fixed(sec string) func([]string) (string, bool) {
	return func([]string) (string, bool) { return sec, true }
}

type hardPurgeNoop struct{ store.Cache }

func (w hardPurgeNoop) Purge(ctx context.Context, p store.Purge) (store.PurgeResult, error) {
	if p.Soft {
		return w.Cache.Purge(ctx, p)
	}
	if err := p.Validate(); err != nil {
		return store.PurgeResult{}, err
	}
	return store.PurgeResult{Accepted: 1, AffectedKnown: true}, nil
}

type softAsHard struct{ store.Cache }

func (w softAsHard) Purge(ctx context.Context, p store.Purge) (store.PurgeResult, error) {
	p.Soft = false
	return w.Cache.Purge(ctx, p)
}

type hardAsSoft struct{ store.Cache }

func (w hardAsSoft) Purge(ctx context.Context, p store.Purge) (store.PurgeResult, error) {
	p.Soft = true
	return w.Cache.Purge(ctx, p)
}

type expiryLate struct{ store.Cache }

func (w expiryLate) Put(ctx context.Context, tok store.FillToken, primary, secondary string, varyNames []string, e *store.Entry, ttl time.Duration) (bool, error) {
	if ttl > 0 {
		ttl += time.Nanosecond
	}
	return w.Cache.Put(ctx, tok, primary, secondary, varyNames, e, ttl)
}

type expiryEarly struct{ store.Cache }

func (w expiryEarly) Put(ctx context.Context, tok store.FillToken, primary, secondary string, varyNames []string, e *store.Entry, ttl time.Duration) (bool, error) {
	if ttl > 0 {
		ttl -= time.Nanosecond
	}
	return w.Cache.Put(ctx, tok, primary, secondary, varyNames, e, ttl)
}

type storedAtState struct {
	mu sync.Mutex
	at map[string]time.Time
}

type trustsCallerStoredAt struct {
	store.Cache
	st *storedAtState
}

func (w trustsCallerStoredAt) Put(ctx context.Context, tok store.FillToken, primary, secondary string, varyNames []string, e *store.Entry, ttl time.Duration) (bool, error) {
	stored, err := w.Cache.Put(ctx, tok, primary, secondary, varyNames, e, ttl)
	if err == nil && stored {
		w.st.mu.Lock()
		w.st.at[primary+"\x00"+secondary] = e.StoredAt
		w.st.mu.Unlock()
	}
	return stored, err
}

func (w trustsCallerStoredAt) Get(ctx context.Context, primary string, sel func(varyNames []string) (string, bool)) (store.Lookup, error) {
	var chosen string
	l, err := w.Cache.Get(ctx, primary, func(names []string) (string, bool) {
		s, ok := sel(names)
		chosen = s
		return s, ok
	})
	if err != nil || l.Entry == nil {
		return l, err
	}
	w.st.mu.Lock()
	at, ok := w.st.at[primary+"\x00"+chosen]
	w.st.mu.Unlock()
	if ok && !at.IsZero() {
		ce := *l.Entry
		ce.StoredAt = at
		l.Entry = &ce
	}
	return l, nil
}

type ignoresFillToken struct{ store.Cache }

func (w ignoresFillToken) Put(ctx context.Context, _ store.FillToken, primary, secondary string, varyNames []string, e *store.Entry, ttl time.Duration) (bool, error) {
	l, err := w.Get(ctx, primary, fixed(secondary))
	if err != nil {
		return false, err
	}
	return w.Cache.Put(ctx, l.Token, primary, secondary, varyNames, e, ttl)
}

type zeroTTLDefault struct{ store.Cache }

func (w zeroTTLDefault) Put(ctx context.Context, tok store.FillToken, primary, secondary string, varyNames []string, e *store.Entry, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = time.Hour
	}
	return w.Cache.Put(ctx, tok, primary, secondary, varyNames, e, ttl)
}

type negativeTTLRemoves struct{ store.Cache }

func (w negativeTTLRemoves) Put(ctx context.Context, tok store.FillToken, primary, secondary string, varyNames []string, e *store.Entry, ttl time.Duration) (bool, error) {
	if ttl < 0 {
		if _, err := w.Purge(ctx, store.Purge{Kind: store.PurgeKeys, Keys: []string{primary}, Retain: store.MaxTTL}); err != nil {
			return false, err
		}
		return false, nil
	}
	return w.Cache.Put(ctx, tok, primary, secondary, varyNames, e, ttl)
}

type tagIndex struct {
	mu        sync.Mutex
	primaries map[string]map[string]struct{}
}

type tagPurgeSiblings struct {
	store.Cache
	idx *tagIndex
}

func (w tagPurgeSiblings) Put(ctx context.Context, tok store.FillToken, primary, secondary string, varyNames []string, e *store.Entry, ttl time.Duration) (bool, error) {
	stored, err := w.Cache.Put(ctx, tok, primary, secondary, varyNames, e, ttl)
	if err == nil && stored {
		w.idx.mu.Lock()
		for _, tg := range e.Tags {
			set := w.idx.primaries[tg]
			if set == nil {
				set = make(map[string]struct{})
				w.idx.primaries[tg] = set
			}
			set[primary] = struct{}{}
		}
		w.idx.mu.Unlock()
	}
	return stored, err
}

func (w tagPurgeSiblings) Purge(ctx context.Context, p store.Purge) (store.PurgeResult, error) {
	res, err := w.Cache.Purge(ctx, p)
	if err != nil || p.Kind != store.PurgeTags {
		return res, err
	}
	var sib []string
	w.idx.mu.Lock()
	for _, tg := range p.Tags {
		for pk := range w.idx.primaries[tg] {
			sib = append(sib, pk)
		}
	}
	w.idx.mu.Unlock()
	if len(sib) == 0 {
		return res, nil
	}
	if _, err := w.Cache.Purge(ctx, store.Purge{Kind: store.PurgeKeys, Keys: sib, Retain: p.Retain}); err != nil {
		return store.PurgeResult{}, err
	}
	return res, nil
}

type markerDroppedEarly struct{ store.Cache }

func (w markerDroppedEarly) Purge(ctx context.Context, p store.Purge) (store.PurgeResult, error) {
	p.Retain /= 2
	if p.Retain < time.Nanosecond {
		p.Retain = time.Nanosecond
	}
	return w.Cache.Purge(ctx, p)
}

type deleteIgnored struct{ store.Cache }

func (w deleteIgnored) Delete(ctx context.Context, primary, secondary string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := store.ValidateDelete(primary, secondary); err != nil {
		return false, err
	}
	return true, nil
}

type deleteAsKeyPurge struct{ store.Cache }

func (w deleteAsKeyPurge) Delete(ctx context.Context, primary, secondary string) (bool, error) {
	r, err := w.Purge(ctx, store.Purge{Kind: store.PurgeKeys, Keys: []string{primary}, Retain: store.MaxTTL})
	return r.Affected > 0, err
}

type deleteAbsentTrue struct{ store.Cache }

func (w deleteAbsentTrue) Delete(ctx context.Context, primary, secondary string) (bool, error) {
	_, err := w.Cache.Delete(ctx, primary, secondary)
	return err == nil, err
}

func brokenCacheBackends() map[string]cachePairBuilder {
	wrap := func(f func(store.Cache) store.Cache) cachePairBuilder {
		return func(t *testing.T, clk *clock.Fake) (store.Cache, store.Cache, func()) {
			a, b, sweep := sharedCachePair(t, clk)
			return f(a), f(b), sweep
		}
	}
	return map[string]cachePairBuilder{
		"perHandle": perHandleCachePair,
		"hardPurgeNoop": wrap(func(c store.Cache) store.Cache {
			return hardPurgeNoop{c}
		}),
		"softAsHard": wrap(func(c store.Cache) store.Cache {
			return softAsHard{c}
		}),
		"hardAsSoft": wrap(func(c store.Cache) store.Cache {
			return hardAsSoft{c}
		}),
		"expiryLate": wrap(func(c store.Cache) store.Cache {
			return expiryLate{c}
		}),
		"expiryEarly": wrap(func(c store.Cache) store.Cache {
			return expiryEarly{c}
		}),
		"trustsCallerStoredAt": func(t *testing.T, clk *clock.Fake) (store.Cache, store.Cache, func()) {
			st := &storedAtState{at: make(map[string]time.Time)}
			a, b, sweep := sharedCachePair(t, clk)
			return trustsCallerStoredAt{Cache: a, st: st}, trustsCallerStoredAt{Cache: b, st: st}, sweep
		},
		"ignoresFillToken": wrap(func(c store.Cache) store.Cache {
			return ignoresFillToken{c}
		}),
		"zeroTTLDefault": wrap(func(c store.Cache) store.Cache {
			return zeroTTLDefault{c}
		}),
		"negativeTTLRemoves": wrap(func(c store.Cache) store.Cache {
			return negativeTTLRemoves{c}
		}),
		"tagPurgeSiblings": func(t *testing.T, clk *clock.Fake) (store.Cache, store.Cache, func()) {
			idx := &tagIndex{primaries: make(map[string]map[string]struct{})}
			a, b, sweep := sharedCachePair(t, clk)
			return tagPurgeSiblings{Cache: a, idx: idx}, tagPurgeSiblings{Cache: b, idx: idx}, sweep
		},
		"markerDroppedEarly": wrap(func(c store.Cache) store.Cache {
			return markerDroppedEarly{c}
		}),
		"deleteIgnored": wrap(func(c store.Cache) store.Cache {
			return deleteIgnored{c}
		}),
		"deleteAsKeyPurge": wrap(func(c store.Cache) store.Cache {
			return deleteAsKeyPurge{c}
		}),
		"deleteAbsentTrue": wrap(func(c store.Cache) store.Cache {
			return deleteAbsentTrue{c}
		}),
	}
}

func anchoredPath(path string) string {
	segs := strings.Split(path, "/")
	for i, s := range segs {
		segs[i] = "^" + s + "$"
	}
	return strings.Join(segs, "/")
}

func TestCacheConformance(t *testing.T) {
	t.Parallel()
	storetest.Cache(t, newCacheHarness(sharedCachePair))
}

func TestBrokenCacheBackend(t *testing.T) {
	t.Parallel()
	name := os.Getenv(brokenCacheEnv)
	if name == "" {
		t.Skipf("set %s to run a broken cache backend", brokenCacheEnv)
	}
	build, ok := brokenCacheBackends()[name]
	if !ok {
		t.Fatalf("unknown broken cache backend %q", name)
	}
	storetest.Cache(t, newCacheHarness(build))
}

func TestCacheSuiteRejectsBrokenBackends(t *testing.T) {
	cases := []struct {
		name     string
		expected string
	}{
		{"perHandle", "RoundTrip"},
		{"hardPurgeNoop", "PurgeMatrix/keys-hard"},
		{"softAsHard", "PurgeMatrix/keys-soft"},
		{"hardAsSoft", "PurgeMatrix/keys-hard"},
		{"expiryLate", "TTLBoundaries/1s"},
		{"expiryEarly", "TTLBoundaries/1s"},
		{"trustsCallerStoredAt", "ClockAuthority"},
		{"ignoresFillToken", "FillTokenRace/keys-hard"},
		{"zeroTTLDefault", "TTLNonPositive"},
		{"negativeTTLRemoves", "TTLNonPositive"},
		{"tagPurgeSiblings", "PurgeTagsExactVariants"},
		{"markerDroppedEarly", "MarkerLifetime/hard"},
		{"deleteIgnored", "DeleteOneVariant"},
		{"deleteAsKeyPurge", "DeletePutAgain"},
		{"deleteAbsentTrue", "DeleteAbsent"},
	}
	table := make(map[string]bool, len(cases))
	for _, tc := range cases {
		table[tc.name] = true
	}
	for name := range brokenCacheBackends() {
		if !table[name] {
			t.Errorf("broken cache backend %q has no rejection case", name)
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestBrokenCacheBackend$/"+anchoredPath(tc.expected), "-test.v")
			cmd.Env = append(os.Environ(), brokenCacheEnv+"="+tc.name)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("broken cache backend %s: subprocess passed, want failure\n%s", tc.name, out)
			}
			if want := "--- FAIL: TestBrokenCacheBackend/" + tc.expected; !strings.Contains(string(out), want) {
				t.Errorf("broken cache backend %s: output lacks %q\n%s", tc.name, want, out)
			}
		})
	}
}
