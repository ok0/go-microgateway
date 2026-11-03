package runtime

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/teststore"
	"github.com/ok0/go-microgateway/store"
)

const stTypeName = "test"

var stStart = time.Unix(1_700_000_000, 0)

const stHead = "version: 1\nlisteners:\n  - name: public\n    address: \":8080\"\nupstreams:\n  - name: users\n    hosts:\n      - url: \"http://127.0.0.1:9001\"\n"

func stDoc(stores, routes string) string {
	return stHead + "stores:\n" + stores + "routes:\n" + routes
}

func stStore(name, typ, params string) string {
	s := fmt.Sprintf("  - name: %s\n    type: %s\n", name, typ)
	if params != "" {
		s += "    params:\n" + params
	}
	return s
}

func stRoute(name, store string) string {
	return fmt.Sprintf("  - name: %s\n    match:\n      path: /%s\n    upstream: users\n    rate_limit:\n      rate: 5\n      per: 1s\n      key:\n        source: ip\n      store: %s\n", name, name, store)
}

type stNilBackend struct{}

func (*stNilBackend) Close() error { return nil }

type stCloseOnly struct{ closes *atomic.Int64 }

func (s *stCloseOnly) Close() error {
	s.closes.Add(1)
	return nil
}

type stScripted struct {
	take     func(ctx context.Context, key string, l store.Limit) (store.Decision, error)
	closeErr error
}

func (s *stScripted) Take(ctx context.Context, key string, l store.Limit) (store.Decision, error) {
	return s.take(ctx, key, l)
}

func (s *stScripted) Close() error { return s.closeErr }

func TestResolveStores(t *testing.T) {
	var calls atomic.Int64
	factory := func(*config.Store, *slog.Logger) (store.Backend, error) {
		calls.Add(1)
		return nil, errors.New("unexpected build")
	}
	set := NewStoreSet(map[string]StoreType{
		stTypeName: {Caps: store.CapRateLimit, Build: factory},
		"nocap":    {Caps: 1 << 1, Build: factory},
	})
	cfg := mustParse(t, stDoc(
		stStore("unused", "missing", "")+stStore("b", "nocap", "")+stStore("c", stTypeName, ""),
		stRoute("api", "b")+stRoute("good", "c"),
	))

	errs := set.Resolve(cfg)
	if len(errs) != 2 {
		t.Fatalf("errors = %v, want 2", errs)
	}
	wantPaths := []string{"stores[0].type", "routes[0].rate_limit.store"}
	for i, fe := range errs {
		if fe.Path != wantPaths[i] {
			t.Errorf("error %d: path = %q, want %q", i, fe.Path, wantPaths[i])
		}
		if fe.Line <= 0 {
			t.Errorf("error %d: line = %d, want > 0", i, fe.Line)
		}
	}
	if !strings.Contains(errs[1].Msg, "does not support rate limiting") {
		t.Errorf("message = %q, want rate limiting refusal", errs[1].Msg)
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("factory calls = %d, want 0", n)
	}

	var nilSet *StoreSet
	if got := nilSet.Resolve(cfg); len(got) != 3 {
		t.Errorf("nil StoreSet errors = %v, want 3 unregistered types", got)
	}
}

func TestBuildStoreFactoryFailures(t *testing.T) {
	const secret = "p4ss-UNIQUE-1"
	var closes atomic.Int64
	set := NewStoreSet(map[string]StoreType{
		"echo": {Caps: store.CapRateLimit, Build: func(e *config.Store, _ *slog.Logger) (store.Backend, error) {
			return nil, fmt.Errorf("auth rejected for %v", e.Params.Reveal()["token"])
		}},
		"boom": {Caps: store.CapRateLimit, Build: func(e *config.Store, _ *slog.Logger) (store.Backend, error) {
			panic("panic with " + e.Params.Reveal()["token"].(string))
		}},
		"nilb": {Caps: store.CapRateLimit, Build: func(*config.Store, *slog.Logger) (store.Backend, error) {
			return nil, nil
		}},
		"typed": {Caps: store.CapRateLimit, Build: func(*config.Store, *slog.Logger) (store.Backend, error) {
			return (*stNilBackend)(nil), nil
		}},
		"noTake": {Caps: store.CapRateLimit, Build: func(*config.Store, *slog.Logger) (store.Backend, error) {
			return &stCloseOnly{closes: &closes}, nil
		}},
	})
	token := "      token: " + secret + "\n"
	doc := "version: 1\nlisteners:\n  - name: public\n    address: \":8080\"\nupstreams:\n  - name: users\n    hosts:\n      - url: \"http://127.0.0.1:9001\"\n" +
		"middlewares:\n  - name: m\n    type: nope\n" +
		"stores:\n" +
		stStore("echo", "echo", token) +
		stStore("boom", "boom", token) +
		stStore("nilb", "nilb", "") +
		stStore("typed", "typed", "") +
		stStore("noTake", "noTake", "") +
		"routes:\n" +
		stRoute("r0", "echo") + stRoute("r1", "boom") + stRoute("r2", "nilb") + stRoute("r3", "typed") + stRoute("r4", "noTake")
	cfg := mustParse(t, doc)

	s, _, err := build(t, cfg, Deps{Stores: set})
	if s != nil {
		t.Errorf("snapshot = %v, want nil", s)
	}
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("Build error %T is not a *config.ValidationError", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("Build error leaks the secret: %v", err)
	}

	want := map[string]string{
		"stores[0]":           "store factory failed: auth rejected for REDACTED",
		"stores[1]":           "store factory panicked: panic with REDACTED",
		"stores[2]":           "store factory returned a nil backend",
		"stores[3]":           "store factory returned a nil backend",
		"stores[4]":           "does not implement store.RateLimiter",
		"middlewares[0].type": "unknown middleware type",
	}
	if len(ve.Errors) != len(want) {
		t.Fatalf("errors = %d, want %d: %v", len(ve.Errors), len(want), ve.Errors)
	}
	for _, fe := range ve.Errors {
		sub, ok := want[fe.Path]
		if !ok {
			t.Errorf("unexpected error path %q: %v", fe.Path, fe)
			continue
		}
		if fe.Line <= 0 {
			t.Errorf("%s: line = %d, want > 0", fe.Path, fe.Line)
		}
		if !strings.Contains(fe.Msg, sub) {
			t.Errorf("%s: message = %q, want it to contain %q", fe.Path, fe.Msg, sub)
		}
	}
	if n := closes.Load(); n != 1 {
		t.Errorf("noTake backend closed %d times, want 1", n)
	}
}

func TestOnlyReferencedStoresBuilt(t *testing.T) {
	fake := clock.NewFake(stStart)
	shared := teststore.NewShared(fake)
	t.Cleanup(func() { _ = shared.Close() })
	var calls atomic.Int64
	set := NewStoreSet(map[string]StoreType{
		stTypeName: {Caps: store.CapRateLimit, Build: func(*config.Store, *slog.Logger) (store.Backend, error) {
			calls.Add(1)
			return shared.Handle(), nil
		}},
	})
	cfg := mustParse(t, stDoc(stStore("used", stTypeName, "")+stStore("unused", stTypeName, ""), stRoute("api", "used")))
	s, _, err := build(t, cfg, Deps{Clock: fake, Stores: set})
	if err != nil {
		t.Fatal(err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("factory calls = %d, want 1", n)
	}
	if len(s.built) != 1 || s.built[0].name != "used" {
		t.Errorf("built stores = %d, want only \"used\"", len(s.built))
	}
}

func TestStoreBackendScrubsAndClassifies(t *testing.T) {
	const secret = "tok-SECRET-9"
	e := &config.Store{Name: "s", Type: stTypeName, Params: config.NewSecretParams(map[string]any{"token": secret})}
	cases := []struct {
		name     string
		take     func(ctx context.Context, key string, l store.Limit) (store.Decision, error)
		closeErr error
		kind     error
		exact    string
	}{
		{
			name: "unavailable with query escape",
			take: func(context.Context, string, store.Limit) (store.Decision, error) {
				return store.Decision{}, fmt.Errorf("dial %s failed: %w", url.QueryEscape(secret), store.ErrUnavailable)
			},
			kind: store.ErrUnavailable,
		},
		{
			name: "invalid limit with base64",
			take: func(context.Context, string, store.Limit) (store.Decision, error) {
				return store.Decision{}, fmt.Errorf("bad %s: %w", base64.StdEncoding.EncodeToString([]byte(secret)), store.ErrInvalidLimit)
			},
			kind: store.ErrInvalidLimit,
		},
		{
			name: "canceled",
			take: func(context.Context, string, store.Limit) (store.Decision, error) {
				return store.Decision{}, fmt.Errorf("gone %s: %w", secret, context.Canceled)
			},
			kind:  context.Canceled,
			exact: `store "s": take: context canceled`,
		},
		{
			name: "unclassified",
			take: func(context.Context, string, store.Limit) (store.Decision, error) {
				return store.Decision{}, fmt.Errorf("odd %s", secret)
			},
		},
		{
			name: "panic",
			take: func(context.Context, string, store.Limit) (store.Decision, error) {
				panic("panic " + secret)
			},
			kind: store.ErrUnavailable,
		},
		{
			name: "close",
			take: func(context.Context, string, store.Limit) (store.Decision, error) {
				return store.Decision{}, nil
			},
			closeErr: fmt.Errorf("close %s: %w", secret, store.ErrUnavailable),
			kind:     store.ErrUnavailable,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &stScripted{take: c.take, closeErr: c.closeErr}
			sb, err := newStoreBackend(e, StoreType{Caps: store.CapRateLimit, Build: func(*config.Store, *slog.Logger) (store.Backend, error) {
				return fake, nil
			}}, slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatal(err)
			}
			var got error
			if c.closeErr != nil {
				got = sb.Close()
			} else {
				_, got = sb.Take(context.Background(), "k", store.Limit{})
			}
			if got == nil {
				t.Fatal("error = nil")
			}
			if strings.Contains(got.Error(), secret) || strings.Contains(got.Error(), url.QueryEscape(secret)) || strings.Contains(got.Error(), base64.StdEncoding.EncodeToString([]byte(secret))) {
				t.Errorf("error leaks the secret: %q", got.Error())
			}
			if c.kind != nil && !errors.Is(got, c.kind) {
				t.Errorf("errors.Is(%v) = false", c.kind)
			}
			if c.kind == nil && errors.Unwrap(got) != nil {
				t.Errorf("errors.Unwrap = %v, want nil", errors.Unwrap(got))
			}
			if c.kind != nil && errors.Unwrap(got) != c.kind {
				t.Errorf("errors.Unwrap = %v, want only the sentinel %v", errors.Unwrap(got), c.kind)
			}
			if c.exact != "" && got.Error() != c.exact {
				t.Errorf("error = %q, want %q", got.Error(), c.exact)
			}
		})
	}
}

func TestGuardEntryOwnership(t *testing.T) {
	fake := clock.NewFake(stStart)
	reg := metrics.NewRegistry()
	avail, err := reg.Gauge("microgateway_store_available", "Test gauge.", "store")
	if err != nil {
		t.Fatal(err)
	}
	cfg := mustParse(t, stDoc(stStore("a", stTypeName, ""), ""))
	e := &cfg.Stores[0]
	set := NewStoreSet(nil)
	logger := slog.New(slog.DiscardHandler)

	old, err := newGuardEntry(e, set, fake, logger, avail)
	if err != nil {
		t.Fatal(err)
	}
	cur, err := newGuardEntry(e, set, fake, logger, avail)
	if err != nil {
		t.Fatal(err)
	}
	set.own(old, "r=local", useRateLimit)
	set.own(cur, "r=local", useRateLimit)
	if v := avail.With("a").Value(); v != 1 {
		t.Fatalf("gauge after own = %v, want 1", v)
	}

	trip := func(ge *guardEntry) {
		t.Helper()
		for range 3 {
			_ = ge.g.Do(context.Background(), time.Second, func(context.Context) error {
				return fmt.Errorf("x: %w", store.ErrUnavailable)
			})
		}
	}
	trip(old)
	if v := avail.With("a").Value(); v != 1 {
		t.Errorf("superseded guard changed gauge to %v, want 1", v)
	}
	trip(cur)
	if v := avail.With("a").Value(); v != 0 {
		t.Errorf("owner guard gauge = %v, want 0", v)
	}

	var before strings.Builder
	if err := reg.WriteText(&before); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(before.String(), `store="a"`) {
		t.Fatalf("series for a missing before prune:\n%s", before.String())
	}
	set.prune(map[string]bool{}, avail)
	if len(set.owners) != 0 {
		t.Errorf("owners after prune = %d, want 0", len(set.owners))
	}
	var after strings.Builder
	if err := reg.WriteText(&after); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(after.String(), `store="a"`) {
		t.Errorf("series for a still exposed after prune:\n%s", after.String())
	}
}

func stCacheDoc(typ string) string {
	return stHead + "stores:\n" + stStore("c", typ, "") + "cache:\n  store: c\nroutes:\n"
}

func TestResolveCacheStore(t *testing.T) {
	factory := func(*config.Store, *slog.Logger) (store.Backend, error) {
		return nil, errors.New("unexpected build")
	}
	set := NewStoreSet(map[string]StoreType{
		"rl":    {Caps: store.CapRateLimit, Build: factory},
		"both":  {Caps: store.CapRateLimit | store.CapCache, Build: factory},
		"cache": {Caps: store.CapCache, Build: factory},
	})
	cases := []struct {
		typ      string
		wantPath string
	}{
		{typ: "rl", wantPath: "cache.store"},
		{typ: "both"},
		{typ: "cache"},
		{typ: "missing", wantPath: "stores[0].type"},
	}
	for _, c := range cases {
		t.Run(c.typ, func(t *testing.T) {
			cfg := mustParse(t, stCacheDoc(c.typ))
			errs := set.Resolve(cfg)
			if c.wantPath == "" {
				if len(errs) != 0 {
					t.Fatalf("errors = %v, want none", errs)
				}
				return
			}
			if len(errs) != 1 || errs[0].Path != c.wantPath {
				t.Fatalf("errors = %v, want one error at %q", errs, c.wantPath)
			}
			if errs[0].Line <= 0 {
				t.Errorf("line = %d, want > 0", errs[0].Line)
			}
			if c.wantPath == "cache.store" && !strings.Contains(errs[0].Msg, "does not support caching") {
				t.Errorf("message = %q, want caching refusal", errs[0].Msg)
			}
		})
	}
}

type stCache struct {
	get   func(ctx context.Context, primary string, sel func([]string) (string, bool)) (store.Lookup, error)
	put   func(ctx context.Context, tok store.FillToken, primary, secondary string, varyNames []string, e *store.Entry, ttl time.Duration) (bool, error)
	purge func(ctx context.Context, p store.Purge) (store.PurgeResult, error)
}

func (c *stCache) Get(ctx context.Context, primary string, sel func([]string) (string, bool)) (store.Lookup, error) {
	return c.get(ctx, primary, sel)
}

func (c *stCache) Put(ctx context.Context, tok store.FillToken, primary, secondary string, varyNames []string, e *store.Entry, ttl time.Duration) (bool, error) {
	return c.put(ctx, tok, primary, secondary, varyNames, e, ttl)
}

func (c *stCache) Purge(ctx context.Context, p store.Purge) (store.PurgeResult, error) {
	return c.purge(ctx, p)
}

func (c *stCache) Close() error { return nil }

func TestStoreBackendCacheScrubsAndClassifies(t *testing.T) {
	const secret = "cache-SECRET-7"
	e := &config.Store{Name: "c", Type: stTypeName, Params: config.NewSecretParams(map[string]any{"token": secret})}
	panicGet := func(context.Context, string, func([]string) (string, bool)) (store.Lookup, error) {
		panic("get " + secret)
	}
	errGet := func(context.Context, string, func([]string) (string, bool)) (store.Lookup, error) {
		return store.Lookup{}, fmt.Errorf("dial %s: %w", secret, store.ErrUnavailable)
	}
	panicPut := func(context.Context, store.FillToken, string, string, []string, *store.Entry, time.Duration) (bool, error) {
		panic("put " + secret)
	}
	errPut := func(context.Context, store.FillToken, string, string, []string, *store.Entry, time.Duration) (bool, error) {
		return false, fmt.Errorf("write %s: %w", secret, store.ErrInvalidEntry)
	}
	panicPurge := func(context.Context, store.Purge) (store.PurgeResult, error) {
		panic("purge " + secret)
	}
	errPurge := func(context.Context, store.Purge) (store.PurgeResult, error) {
		return store.PurgeResult{}, fmt.Errorf("purge %s: %w", secret, store.ErrInvalidPurge)
	}
	cases := []struct {
		name  string
		c     *stCache
		call  func(sb *storeBackend) error
		kind  error
		exact string
	}{
		{name: "get panic", c: &stCache{get: panicGet}, call: func(sb *storeBackend) error {
			_, err := sb.Get(context.Background(), "k", nil)
			return err
		}, kind: store.ErrUnavailable, exact: `store "c": get panicked: get REDACTED`},
		{name: "get error", c: &stCache{get: errGet}, call: func(sb *storeBackend) error {
			_, err := sb.Get(context.Background(), "k", nil)
			return err
		}, kind: store.ErrUnavailable},
		{name: "put panic", c: &stCache{put: panicPut}, call: func(sb *storeBackend) error {
			_, err := sb.Put(context.Background(), store.FillToken{}, "k", "", nil, nil, time.Second)
			return err
		}, kind: store.ErrUnavailable, exact: `store "c": put panicked: put REDACTED`},
		{name: "put error", c: &stCache{put: errPut}, call: func(sb *storeBackend) error {
			_, err := sb.Put(context.Background(), store.FillToken{}, "k", "", nil, nil, time.Second)
			return err
		}, kind: store.ErrInvalidEntry},
		{name: "purge panic", c: &stCache{purge: panicPurge}, call: func(sb *storeBackend) error {
			_, err := sb.Purge(context.Background(), store.Purge{})
			return err
		}, kind: store.ErrUnavailable, exact: `store "c": purge panicked: purge REDACTED`},
		{name: "purge error", c: &stCache{purge: errPurge}, call: func(sb *storeBackend) error {
			_, err := sb.Purge(context.Background(), store.Purge{})
			return err
		}, kind: store.ErrInvalidPurge},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sb, err := newStoreBackend(e, StoreType{Caps: store.CapCache, Build: func(*config.Store, *slog.Logger) (store.Backend, error) {
				return c.c, nil
			}}, slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatal(err)
			}
			got := c.call(sb)
			if got == nil {
				t.Fatal("error = nil")
			}
			if strings.Contains(got.Error(), secret) {
				t.Errorf("error leaks the secret: %q", got.Error())
			}
			if !errors.Is(got, c.kind) {
				t.Errorf("errors.Is(%v) = false for %q", c.kind, got.Error())
			}
			if c.exact != "" && got.Error() != c.exact {
				t.Errorf("error = %q, want %q", got.Error(), c.exact)
			}
		})
	}
}

func TestBuildCacheStoreNeedsCacheBackend(t *testing.T) {
	var closes atomic.Int64
	set := NewStoreSet(map[string]StoreType{
		"nocache": {Caps: store.CapCache, Build: func(*config.Store, *slog.Logger) (store.Backend, error) {
			return &stCloseOnly{closes: &closes}, nil
		}},
	})
	doc := stCacheDoc("nocache") + "  - name: api\n    match:\n      path: /api\n    upstream: users\n    cache:\n      enabled: true\n"
	cfg := mustParse(t, doc)
	s, _, err := build(t, cfg, Deps{Stores: set, Clock: clock.NewFake(stStart)})
	if s != nil {
		t.Errorf("snapshot = %v, want nil", s)
	}
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("Build error %T is not a *config.ValidationError", err)
	}
	if len(ve.Errors) != 1 || ve.Errors[0].Path != "stores[0]" {
		t.Fatalf("errors = %v, want one error at stores[0]", ve.Errors)
	}
	if !strings.Contains(ve.Errors[0].Msg, "does not implement store.Cache") {
		t.Errorf("message = %q, want the store.Cache refusal", ve.Errors[0].Msg)
	}
	if n := closes.Load(); n != 1 {
		t.Errorf("backend closed %d times, want 1", n)
	}
}
