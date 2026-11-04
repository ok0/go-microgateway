package e2e

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/ratelimit"
	"github.com/ok0/go-microgateway/internal/teststore"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

const (
	stSecret         = "p4ss-UNIQUE-1"
	stSentinelSecret = "s3nt-UNIQUE-2"
	stParamSecret    = "par4m-UNIQUE-3"
)

func stShared(t *testing.T, fake *clock.Fake) *teststore.Shared {
	t.Helper()
	s := teststore.NewShared(fake)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func stOption(shared *teststore.Shared, calls *atomic.Int64) microgateway.Option {
	return microgateway.WithStoreType("test", store.CapRateLimit, func(p microgateway.StoreParams) (store.Backend, error) {
		calls.Add(1)
		return shared.Handle(), nil
	})
}

func stDoc(backendURL, stores, routes string) string {
	if stores != "" {
		stores = "stores:\n" + stores
	}
	return rlDoc(backendURL, stores, routes)
}

func stStore(name, extra string) string {
	return fmt.Sprintf("  - name: %s\n    type: test\n", name) + extra
}

func stRoute(name, rl string) string {
	return rlRoute(name, rl)
}

func stDo(t *testing.T, g *microgateway.Gateway, path, remote string) *httptest.ResponseRecorder {
	t.Helper()
	return rlGet(t, g, path, remote, nil)
}

func stAsync(t *testing.T, g *microgateway.Gateway, path, remote string) <-chan *httptest.ResponseRecorder {
	t.Helper()
	ch := make(chan *httptest.ResponseRecorder, 1)
	go func() { ch <- rlGet(t, g, path, remote, nil) }()
	return ch
}

func stGauge(t *testing.T, g *microgateway.Gateway, name string, labels ...string) float64 {
	t.Helper()
	return rsMetricOr0(t, g, name, labels...)
}

func stCounter(t *testing.T, g *microgateway.Gateway, name string, labels ...string) float64 {
	t.Helper()
	return rsMetricOr0(t, g, name, labels...)
}

type sapiCloseOnly struct{}

func (sapiCloseOnly) Close() error { return nil }

type sapiLimits struct {
	Max int `yaml:"max"`
}

type sapiParams struct {
	Endpoint string     `yaml:"endpoint"`
	Size     int        `yaml:"size"`
	Limits   sapiLimits `yaml:"limits"`
}

func sapiFake() *clock.Fake {
	return clock.NewFake(time.Unix(1_700_000_000, 0))
}

func sapiNew(t *testing.T, doc string, opts ...microgateway.Option) (*microgateway.Gateway, error) {
	t.Helper()
	cfg, err := config.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	g, err := newGateway(cfg, opts...)
	if err == nil {
		t.Cleanup(func() { _ = g.Close() })
	}
	return g, err
}

func sapiValidationErrors(t *testing.T, err error) []config.FieldError {
	t.Helper()
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error = %T (%v), want *config.ValidationError", err, err)
	}
	return ve.Errors
}

func sapiWant(t *testing.T, errs []config.FieldError, want map[string]string) {
	t.Helper()
	if len(errs) != len(want) {
		t.Fatalf("got %d errors, want %d: %v", len(errs), len(want), errs)
	}
	for _, fe := range errs {
		msg, ok := want[fe.Path]
		if !ok {
			t.Errorf("unexpected error at %q: %s", fe.Path, fe.Msg)
			continue
		}
		if fe.Line <= 0 {
			t.Errorf("%s has no line", fe.Path)
		}
		if !strings.Contains(fe.Msg, msg) {
			t.Errorf("%s: message %q does not contain %q", fe.Path, fe.Msg, msg)
		}
	}
}

func TestStoreResolutionErrors(t *testing.T) {
	fake := sapiFake()
	n := newMemNetwork()
	be, _ := rlBackend(t, n)
	var factoryCalls atomic.Int64
	opts := []microgateway.Option{
		withClock(fake),
		microgateway.WithStoreType("failf", store.CapRateLimit, func(microgateway.StoreParams) (store.Backend, error) {
			factoryCalls.Add(1)
			return nil, errors.New("connect refused")
		}),
		microgateway.WithStoreType("nilb", store.CapRateLimit, func(microgateway.StoreParams) (store.Backend, error) {
			factoryCalls.Add(1)
			return nil, nil
		}),
		microgateway.WithStoreType("boom", store.CapRateLimit, func(microgateway.StoreParams) (store.Backend, error) {
			factoryCalls.Add(1)
			panic("boom")
		}),
		microgateway.WithStoreType("plainb", store.CapRateLimit, func(microgateway.StoreParams) (store.Backend, error) {
			factoryCalls.Add(1)
			return sapiCloseOnly{}, nil
		}),
	}

	buildStores := "  - name: unknown\n    type: nothere\n" +
		"  - name: fail-open\n    type: failf\n" +
		"  - name: nil-open\n    type: nilb\n" +
		"  - name: panic-open\n    type: boom\n" +
		"  - name: no-iface\n    type: plainb\n"
	buildRoutes := stRoute("r-fail", "    rate_limit:\n      rate: 5\n      per: 1s\n      store: fail-open\n") +
		stRoute("r-nil", "    rate_limit:\n      rate: 5\n      per: 1s\n      store: nil-open\n") +
		stRoute("r-boom", "    rate_limit:\n      rate: 5\n      per: 1s\n      store: panic-open\n") +
		stRoute("r-iface", "    rate_limit:\n      rate: 5\n      per: 1s\n      store: no-iface\n")
	buildDoc := stDoc(be.URL, buildStores, buildRoutes)
	buildWant := map[string]string{
		"stores[0].type": `store type "nothere" is not registered`,
		"stores[1]":      "store factory failed",
		"stores[2]":      "store factory returned a nil backend",
		"stores[3]":      "store factory panicked",
		"stores[4]":      "does not implement store.RateLimiter",
	}

	_, err := sapiNew(t, buildDoc, opts...)
	if err == nil {
		t.Fatal("New() = nil error, want resolution errors")
	}
	sapiWant(t, sapiValidationErrors(t, err), buildWant)
	if got := factoryCalls.Load(); got != 4 {
		t.Fatalf("factory calls after New = %d, want 4", got)
	}

	cfg, err := config.Parse([]byte(buildDoc))
	if err != nil {
		t.Fatal(err)
	}
	fp := cfg.Fingerprint()
	before := cfg.Clone()
	err = microgateway.Validate(cfg, opts...)
	if err == nil {
		t.Fatal("Validate() = nil error, want the unregistered type")
	}
	sapiWant(t, sapiValidationErrors(t, err), map[string]string{
		"stores[0].type": `store type "nothere" is not registered`,
	})
	if got := factoryCalls.Load(); got != 4 {
		t.Fatalf("factory calls after Validate = %d, want 4", got)
	}
	if cfg.Fingerprint() != fp {
		t.Fatal("Validate changed the fingerprint of cfg")
	}
	if !reflect.DeepEqual(cfg, before) {
		t.Fatal("Validate changed cfg")
	}

	missingDoc := stDoc(be.URL,
		"  - name: unknown\n    type: nothere\n",
		stRoute("r-missing", "    rate_limit:\n      rate: 5\n      per: 1s\n      store: unknown\n"))
	missingWant := map[string]string{
		"stores[0].type":             `store type "nothere" is not registered`,
		"routes[0].rate_limit.store": `store "nowhere" is not defined in stores`,
	}
	missingCfg, err := config.Parse([]byte(missingDoc))
	if err != nil {
		t.Fatal(err)
	}
	missingCfg.Routes[0].RateLimit.Store = "nowhere"
	_, err = newGateway(missingCfg, opts...)
	if err == nil {
		t.Fatal("New() = nil error, want resolution errors")
	}
	sapiWant(t, sapiValidationErrors(t, err), missingWant)

	err = microgateway.Validate(missingCfg, opts...)
	if err == nil {
		t.Fatal("Validate() = nil error, want resolution errors")
	}
	sapiWant(t, sapiValidationErrors(t, err), missingWant)
	if got := factoryCalls.Load(); got != 4 {
		t.Fatalf("factory calls after second Validate = %d, want 4", got)
	}

	g := n.gateway(t, stDoc(be.URL, "", stRoute("plain", "")), opts...)
	gfp := g.Config().Fingerprint()
	if got := stCounter(t, g, "microgateway_config_reloads_total", "result", "failure"); got != 0 {
		t.Fatalf("failure reloads before Reload = %v, want 0", got)
	}
	rerr := g.Reload(mustParseSapi(t, buildDoc))
	if rerr == nil {
		t.Fatal("Reload() = nil error, want resolution errors")
	}
	sapiWant(t, sapiValidationErrors(t, rerr), buildWant)
	if got := stCounter(t, g, "microgateway_config_reloads_total", "result", "failure"); got != 1 {
		t.Fatalf("failure reloads after Reload = %v, want 1", got)
	}
	if g.Config().Fingerprint() != gfp {
		t.Fatal("failed Reload changed the active configuration")
	}
	if rec := stDo(t, g, "/plain", "192.0.2.1:1000"); rec.Code != 200 {
		t.Fatalf("status after failed Reload = %d, want 200", rec.Code)
	}
}

func mustParseSapi(t *testing.T, doc string) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestStoreRegistrationErrors(t *testing.T) {
	f := func(microgateway.StoreParams) (store.Backend, error) { return sapiCloseOnly{}, nil }
	const doc = "version: 1\n" + listenersYAML + "upstreams:\n  - name: up\n    hosts:\n      - url: \"http://127.0.0.1:1\"\nroutes:\n  - name: plain\n    match:\n      path: /plain\n    upstream: up\n"
	cases := []struct {
		name string
		opts []microgateway.Option
		want string
	}{
		{name: "empty name", opts: []microgateway.Option{microgateway.WithStoreType("", store.CapRateLimit, f)}, want: "empty type name"},
		{name: "nil factory", opts: []microgateway.Option{microgateway.WithStoreType("nilf", store.CapRateLimit, nil)}, want: `WithStoreType("nilf"): nil factory`},
		{name: "zero caps", opts: []microgateway.Option{microgateway.WithStoreType("zero", 0, f)}, want: `WithStoreType("zero"): capabilities must not be zero`},
		{name: "unknown caps", opts: []microgateway.Option{microgateway.WithStoreType("unk", store.CapRateLimit|1<<2, f)}, want: `WithStoreType("unk"): unknown capability bits`},
		{name: "duplicate", opts: []microgateway.Option{microgateway.WithStoreType("dup", store.CapRateLimit, f), microgateway.WithStoreType("dup", store.CapRateLimit, f)}, want: `WithStoreType("dup"): type registered twice`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.Parse([]byte(doc))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := newGateway(cfg, tc.opts...); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("New() error = %v, want it to contain %q", err, tc.want)
			}
			if _, err := newGatewayFromYAML([]byte(doc), tc.opts...); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("NewFromYAML() error = %v, want it to contain %q", err, tc.want)
			}
			if err := microgateway.Validate(cfg, tc.opts...); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate() error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestStoreRegistrationAcceptsCache(t *testing.T) {
	shared := teststore.NewSharedCache(clock.NewFake(cacheStart), 1<<20)
	t.Cleanup(func() { _ = shared.Close() })
	opt := microgateway.WithStoreType("cachestore", store.CapCache, func(microgateway.StoreParams) (store.Backend, error) {
		return shared.Handle(), nil
	})
	const doc = "version: 1\n" + listenersYAML + "upstreams:\n  - name: up\n    hosts:\n      - url: \"http://127.0.0.1:1\"\nstores:\n  - name: sc\n    type: cachestore\ncache:\n  store: sc\nroutes:\n  - name: cached\n    match:\n      path: /cached\n    upstream: up\n    cache:\n      enabled: true\n"
	cfg, err := config.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	g, err := newGateway(cfg, opt)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	g2, err := newGatewayFromYAML([]byte(doc), opt)
	if err != nil {
		t.Fatalf("NewFromYAML() error = %v", err)
	}
	t.Cleanup(func() { _ = g2.Close() })
	if err := microgateway.Validate(cfg, opt); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestStoreParamsAPI(t *testing.T) {
	fake := sapiFake()
	n := newMemNetwork()
	be, _ := rlBackend(t, n)
	shared := stShared(t, fake)
	route := stRoute("r", "    rate_limit:\n      rate: 5\n      per: 1s\n      store: decoder\n")

	t.Run("decode", func(t *testing.T) {
		const secretValue = "sapi-value-endpoint-1"
		cases := []struct {
			name    string
			params  string
			wantErr []string
			banned  []string
		}{
			{
				name:   "valid",
				params: "      endpoint: " + secretValue + "\n      size: 3\n      limits:\n        max: 7\n",
			},
			{
				name:    "unknown key",
				params:  "      endpoint: " + secretValue + "\n      bogus: sapi-value-bogus\n",
				wantErr: []string{"bogus"},
				banned:  []string{"sapi-value-bogus", secretValue},
			},
			{
				name:    "wrong type",
				params:  "      size: sapi-value-str\n",
				wantErr: []string{"int"},
				banned:  []string{"sapi-value-str"},
			},
			{
				name:    "dotted nested path",
				params:  "      limits.max: 9\n",
				wantErr: []string{"limits.max"},
			},
			{
				name:    "unknown nested key",
				params:  "      limits:\n        nope: 1\n",
				wantErr: []string{"nope"},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var decoded sapiParams
				var decodeErr error
				var calls int
				opt := microgateway.WithStoreType("custom", store.CapRateLimit, func(p microgateway.StoreParams) (store.Backend, error) {
					calls++
					decodeErr = p.Decode(&decoded)
					return shared.Handle(), nil
				})
				doc := stDoc(be.URL, "  - name: decoder\n    type: custom\n    params:\n"+tc.params, route)
				if _, err := sapiNew(t, doc, withClock(fake), opt); err != nil {
					t.Fatalf("New() = %v", err)
				}
				if calls != 1 {
					t.Fatalf("factory calls = %d, want 1", calls)
				}
				if len(tc.wantErr) == 0 {
					if decodeErr != nil {
						t.Fatalf("Decode() = %v, want nil", decodeErr)
					}
					want := sapiParams{Endpoint: secretValue, Size: 3, Limits: sapiLimits{Max: 7}}
					if decoded != want {
						t.Fatalf("decoded = %+v, want %+v", decoded, want)
					}
					return
				}
				if decodeErr == nil {
					t.Fatal("Decode() = nil, want error")
				}
				for _, w := range tc.wantErr {
					if !strings.Contains(decodeErr.Error(), w) {
						t.Errorf("Decode() error %q does not name %q", decodeErr, w)
					}
				}
				for _, b := range tc.banned {
					if strings.Contains(decodeErr.Error(), b) {
						t.Errorf("Decode() error %q contains a parameter value", decodeErr)
					}
				}
			})
		}
	})

	t.Run("redis clone", func(t *testing.T) {
		opt := microgateway.WithStoreType("redis", store.CapRateLimit, func(p microgateway.StoreParams) (store.Backend, error) {
			p.Redis.Addresses[0] = "mutated.example:1"
			p.Redis.Addresses = append(p.Redis.Addresses, "extra.example:2")
			return shared.Handle(), nil
		})
		doc := stDoc(be.URL, "  - name: cache\n    type: redis\n    redis:\n      addresses: [\"10.1.1.1:6379\"]\n",
			stRoute("c", "    rate_limit:\n      rate: 5\n      per: 1s\n      store: cache\n"))
		g, err := sapiNew(t, doc, withClock(fake), opt)
		if err != nil {
			t.Fatalf("New() = %v", err)
		}
		var found bool
		for _, s := range g.Config().Stores {
			if s.Name != "cache" {
				continue
			}
			found = true
			if !reflect.DeepEqual(s.Redis.Addresses, []string{"10.1.1.1:6379"}) {
				t.Fatalf("Config() addresses = %v, want the configured address", s.Redis.Addresses)
			}
		}
		if !found {
			t.Fatal("Config() has no store cache")
		}
	})

	t.Run("logger and selection by name", func(t *testing.T) {
		var logBuf bytes.Buffer
		var seen microgateway.StoreParams
		var calls int
		opt := microgateway.WithStoreType("custom", store.CapRateLimit, func(p microgateway.StoreParams) (store.Backend, error) {
			calls++
			seen = p
			p.Logger.Info("sapi-probe")
			return shared.Handle(), nil
		})
		doc := stDoc(be.URL, "  - name: anything-goes\n    type: custom\n",
			stRoute("l", "    rate_limit:\n      rate: 5\n      per: 1s\n      store: anything-goes\n"))
		if _, err := sapiNew(t, doc, withClock(fake), microgateway.WithLogger(slog.New(slog.NewJSONHandler(&logBuf, nil))), opt); err != nil {
			t.Fatalf("New() = %v", err)
		}
		if calls != 1 {
			t.Fatalf("factory calls = %d, want 1", calls)
		}
		if seen.Name != "anything-goes" || seen.Type != "custom" {
			t.Fatalf("params name/type = %q/%q, want anything-goes/custom", seen.Name, seen.Type)
		}
		var got any
		found := false
		for line := range strings.SplitSeq(logBuf.String(), "\n") {
			if !strings.Contains(line, `"msg":"sapi-probe"`) {
				continue
			}
			var rec map[string]any
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				t.Fatal(err)
			}
			got, found = rec["store"], true
		}
		if !found {
			t.Fatal("no sapi-probe record")
		}
		if got != "anything-goes" {
			t.Fatalf("store attribute = %v, want anything-goes", got)
		}
	})
}

func TestStoreNotBuiltWhenUnreferenced(t *testing.T) {
	fake := sapiFake()
	n := newMemNetwork()
	be, _ := rlBackend(t, n)
	shared := stShared(t, fake)
	var built []string
	opt := microgateway.WithStoreType("custom", store.CapRateLimit, func(p microgateway.StoreParams) (store.Backend, error) {
		built = append(built, p.Name)
		if p.Name == "unused" {
			return nil, errors.New("unreferenced store must not be built")
		}
		return shared.Handle(), nil
	})
	doc := stDoc(be.URL,
		"  - name: used\n    type: custom\n  - name: unused\n    type: custom\n",
		stRoute("u", "    rate_limit:\n      rate: 5\n      per: 1s\n      store: used\n")+stRoute("p", ""))
	g, err := sapiNew(t, doc, withClock(fake), opt)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	if !reflect.DeepEqual(built, []string{"used"}) {
		t.Fatalf("factory calls = %v, want [used]", built)
	}
	if strings.Contains(metricsText(t, g), `store="unused"`) {
		t.Fatal("metrics contain a series for the unreferenced store")
	}
}

func TestStoresSeededViaNewFromYAML(t *testing.T) {
	data, err := os.ReadFile("../../config/testdata/stores_invalid.yaml")
	if err != nil {
		t.Fatal(err)
	}
	_, err = newGatewayFromYAML(data)
	if err == nil {
		t.Fatal("NewFromYAML() = nil error, want the seeded errors")
	}
	if strings.Contains(err.Error(), stSecret) {
		t.Fatal("NewFromYAML() error contains a secret value")
	}
	sapiWant(t, sapiValidationErrors(t, err), map[string]string{
		"stores[1].name":                        `duplicate name "dup"`,
		"stores[2].name":                        "must match ^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$",
		"stores[3].type":                        "is required",
		"stores[4].redis":                       "is only valid with type redis",
		"stores[5].redis":                       "is required for type redis",
		"stores[6].params":                      "is not valid for type redis",
		"stores[7].redis.mode":                  "must be one of standalone, sentinel, cluster",
		"stores[8].redis.addresses":             "standalone mode takes exactly one address",
		"stores[9].redis.addresses[1]":          "duplicate address",
		"stores[10].redis.addresses[0]":         "must be host:port without a scheme",
		"stores[11].redis.addresses[0]":         "must not contain userinfo",
		"stores[12].redis.addresses[0]":         "port must be between 1 and 65535",
		"stores[13].redis.master_name":          "is required in sentinel mode",
		"stores[14].redis.master_name":          "is only valid in sentinel mode",
		"stores[15].redis.db":                   "must be 0 in cluster mode",
		"stores[16].redis.tls":                  "cert_file and key_file must be set together",
		"stores[17].redis.tls.ca_file":          "is only valid when tls.enabled is true",
		"stores[18].redis.dial_timeout":         "must be positive",
		"stores[19].redis.pool_size":            "must be >= 0",
		"stores[20].redis.min_idle_conns":       "must not exceed pool_size",
		"stores[21].redis.key_prefix":           "must not contain { or }",
		"stores[22].guard.consecutive_failures": "must be >= 1",
		"stores[23].guard.open_timeout":         "must be positive",
		"stores[24].guard.max_inflight":         "must be >= 1",
		"stores[25].guard.max_inflight":         "cannot unmarshal !!float `1e3` into int",
		"stores[26].redis.db":                   "cannot unmarshal !!float `1.5` into int",
		"stores[27].redis.pool_size":            "cannot unmarshal !!float `2.0` into int",
		"stores[28].redis.db":                   "cannot unmarshal !!float `9999999...` into int",
		"routes[0].rate_limit.store_timeout":    "must be positive",
		"routes[0].rate_limit.on_store_error":   "must be one of local, allow, deny",
		"routes[1].rate_limit.on_store_error":   "is only valid with store",
		"routes[1].rate_limit.store_timeout":    "is only valid with store",
		"routes[2].rate_limit.store":            `store "nothing" is not defined in stores`,
		"stores[0].type":                        `store type "custom" is not registered`,
		"stores[1].type":                        `store type "custom" is not registered`,
		"stores[2].type":                        `store type "custom" is not registered`,
		"stores[4].type":                        `store type "memcache" is not registered`,
		"stores[5].type":                        `store type "redis" is not registered`,
		"stores[6].type":                        `store type "redis" is not registered`,
		"stores[7].type":                        `store type "redis" is not registered`,
		"stores[8].type":                        `store type "redis" is not registered`,
		"stores[9].type":                        `store type "redis" is not registered`,
		"stores[10].type":                       `store type "redis" is not registered`,
		"stores[11].type":                       `store type "redis" is not registered`,
		"stores[12].type":                       `store type "redis" is not registered`,
		"stores[13].type":                       `store type "redis" is not registered`,
		"stores[14].type":                       `store type "redis" is not registered`,
		"stores[15].type":                       `store type "redis" is not registered`,
		"stores[16].type":                       `store type "redis" is not registered`,
		"stores[17].type":                       `store type "redis" is not registered`,
		"stores[18].type":                       `store type "redis" is not registered`,
		"stores[19].type":                       `store type "redis" is not registered`,
		"stores[20].type":                       `store type "redis" is not registered`,
		"stores[21].type":                       `store type "redis" is not registered`,
		"stores[22].type":                       `store type "custom" is not registered`,
		"stores[23].type":                       `store type "custom" is not registered`,
		"stores[24].type":                       `store type "custom" is not registered`,
		"stores[25].type":                       `store type "custom" is not registered`,
		"stores[26].type":                       `store type "redis" is not registered`,
		"stores[27].type":                       `store type "redis" is not registered`,
		"stores[28].type":                       `store type "redis" is not registered`,
	})
}

func sapiCarrierStores() string {
	return fmt.Sprintf(`  - name: dup
    type: test
  - name: dup2
    type: test
  - name: ok2
    type: test
  - name: s3
    type: test
  - name: s4
    type: redis
    redis:
      addresses: ["h:1"]
      password: %s
  - name: s5
    type: redis
    redis:
      addresses: ["h:1"]
  - name: s6
    type: redis
    redis:
      addresses: ["h:1"]
  - name: s7
    type: redis
    redis:
      mode: standalone
      addresses: ["h:1"]
  - name: s8
    type: redis
    redis:
      addresses: ["h:1"]
  - name: s9
    type: redis
    redis:
      mode: cluster
      addresses: ["h:1", "h:2"]
  - name: s10
    type: redis
    redis:
      addresses: ["h:1"]
  - name: s11
    type: redis
    redis:
      addresses: ["h:1"]
  - name: s12
    type: redis
    redis:
      addresses: ["h:1"]
  - name: s13
    type: redis
    redis:
      mode: sentinel
      master_name: m
      addresses: ["h:1"]
  - name: s14
    type: redis
    redis:
      addresses: ["h:1"]
  - name: s15
    type: redis
    redis:
      mode: cluster
      db: 0
      addresses: ["h:1"]
  - name: s16
    type: redis
    redis:
      addresses: ["h:1"]
      tls: {enabled: true, cert_file: c, key_file: k}
  - name: s17
    type: redis
    redis:
      addresses: ["h:1"]
      tls: {enabled: true, ca_file: a}
  - name: s18
    type: redis
    redis:
      addresses: ["h:1"]
      dial_timeout: 1s
  - name: s19
    type: redis
    redis:
      addresses: ["h:1"]
      pool_size: 1
  - name: s20
    type: redis
    redis:
      addresses: ["h:1"]
      pool_size: 2
      min_idle_conns: 1
  - name: s21
    type: redis
    redis:
      addresses: ["h:1"]
      key_prefix: a
  - name: s22
    type: test
    guard: {consecutive_failures: 3, open_timeout: 2s, max_inflight: 4}
  - name: s23
    type: test
    guard: {consecutive_failures: 3, open_timeout: 2s, max_inflight: 4}
  - name: s24
    type: test
    guard: {consecutive_failures: 3, open_timeout: 2s, max_inflight: 4}
  - name: s25
    type: nothere
`, stSecret)
}

func sapiCarrierRoutes() string {
	return stRoute("r0", "    rate_limit:\n      rate: 5\n      per: 1s\n      store: dup\n      on_store_error: local\n      store_timeout: 50ms\n") +
		stRoute("r1", "    rate_limit:\n      rate: 5\n      per: 1s\n") +
		stRoute("r2", "    rate_limit:\n      rate: 5\n      per: 1s\n      store: dup\n")
}

func sapiWantAt(t *testing.T, cfg *config.Config, err error, want map[string]string) {
	t.Helper()
	errs := sapiValidationErrors(t, err)
	sapiWant(t, errs, want)
	for _, fe := range errs {
		if line := cfg.Line(fe.Path); fe.Line != line {
			t.Errorf("%s: line = %d, want cfg.Line = %d", fe.Path, fe.Line, line)
		}
	}
}

func TestStoresSeededViaValidate(t *testing.T) {
	n := newMemNetwork()
	be, _ := rlBackend(t, n)
	fake := sapiFake()
	shared := stShared(t, fake)
	var testCalls, redisCalls atomic.Int64
	opts := []microgateway.Option{
		withClock(fake),
		stOption(shared, &testCalls),
		microgateway.WithStoreType("redis", store.CapRateLimit, func(microgateway.StoreParams) (store.Backend, error) {
			redisCalls.Add(1)
			return shared.Handle(), nil
		}),
	}
	cfg, err := config.Parse([]byte(stDoc(be.URL, sapiCarrierStores(), sapiCarrierRoutes())))
	if err != nil {
		t.Fatalf("Parse() = %v, want the carrier document to parse", err)
	}

	cfg.Stores[1].Name = "dup"
	cfg.Stores[2].Name = "-bad"
	cfg.Stores[3].Type = ""
	cfg.Stores[4].Type = "memcache"
	cfg.Stores[5].Redis = nil
	cfg.Stores[6].Params = config.NewSecretParams(map[string]any{"k": "v"})
	cfg.Stores[7].Redis.Mode = "ring"
	cfg.Stores[8].Redis.Addresses = append(cfg.Stores[8].Redis.Addresses, "h:2")
	cfg.Stores[9].Redis.Addresses[1] = "h:1"
	cfg.Stores[10].Redis.Addresses[0] = "redis://h:1"
	cfg.Stores[11].Redis.Addresses[0] = "u@h:1"
	cfg.Stores[12].Redis.Addresses[0] = "h:0"
	cfg.Stores[13].Redis.MasterName = ""
	cfg.Stores[14].Redis.MasterName = "m"
	cfg.Stores[15].Redis.DB = 1
	cfg.Stores[16].Redis.TLS.KeyFile = ""
	cfg.Stores[17].Redis.TLS.Enabled = false
	*cfg.Stores[18].Redis.DialTimeout = 0
	cfg.Stores[19].Redis.PoolSize = -1
	cfg.Stores[20].Redis.MinIdleConns = 5
	*cfg.Stores[21].Redis.KeyPrefix = "a{b"
	*cfg.Stores[22].Guard.ConsecutiveFailures = 0
	*cfg.Stores[23].Guard.OpenTimeout = 0
	*cfg.Stores[24].Guard.MaxInflight = 0
	*cfg.Routes[0].RateLimit.StoreTimeout = 0
	cfg.Routes[0].RateLimit.OnStoreError = "maybe"
	cfg.Routes[1].RateLimit.OnStoreError = "allow"
	routeTimeout := config.Duration(10 * time.Millisecond)
	cfg.Routes[1].RateLimit.StoreTimeout = &routeTimeout
	cfg.Routes[2].RateLimit.Store = "nothing"

	want := map[string]string{
		"stores[1].name":                        `duplicate name "dup"`,
		"stores[2].name":                        "must match ^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$",
		"stores[3].type":                        "is required",
		"stores[4].redis":                       "is only valid with type redis",
		"stores[5].redis":                       "is required for type redis",
		"stores[6].params":                      "is not valid for type redis",
		"stores[7].redis.mode":                  "must be one of standalone, sentinel, cluster",
		"stores[8].redis.addresses":             "standalone mode takes exactly one address",
		"stores[9].redis.addresses[1]":          "duplicate address",
		"stores[10].redis.addresses[0]":         "must be host:port without a scheme",
		"stores[11].redis.addresses[0]":         "must not contain userinfo",
		"stores[12].redis.addresses[0]":         "port must be between 1 and 65535",
		"stores[13].redis.master_name":          "is required in sentinel mode",
		"stores[14].redis.master_name":          "is only valid in sentinel mode",
		"stores[15].redis.db":                   "must be 0 in cluster mode",
		"stores[16].redis.tls":                  "cert_file and key_file must be set together",
		"stores[17].redis.tls.ca_file":          "is only valid when tls.enabled is true",
		"stores[18].redis.dial_timeout":         "must be positive",
		"stores[19].redis.pool_size":            "must be >= 0",
		"stores[20].redis.min_idle_conns":       "must not exceed pool_size",
		"stores[21].redis.key_prefix":           "must not contain { or }",
		"stores[22].guard.consecutive_failures": "must be >= 1",
		"stores[23].guard.open_timeout":         "must be positive",
		"stores[24].guard.max_inflight":         "must be >= 1",
		"routes[0].rate_limit.store_timeout":    "must be positive",
		"routes[0].rate_limit.on_store_error":   "must be one of local, allow, deny",
		"routes[1].rate_limit.on_store_error":   "is only valid with store",
		"routes[1].rate_limit.store_timeout":    "is only valid with store",
		"routes[2].rate_limit.store":            `store "nothing" is not defined in stores`,
		"stores[4].type":                        `store type "memcache" is not registered`,
		"stores[25].type":                       `store type "nothere" is not registered`,
	}

	before := cfg.Clone()
	fp := cfg.Fingerprint()
	err = microgateway.Validate(cfg, opts...)
	sapiWantAt(t, cfg, err, want)
	for _, f := range srdForms(stSecret) {
		if strings.Contains(err.Error(), f.text) {
			t.Errorf("Validate() error contains the secret in %s form", f.label)
		}
	}
	if got := testCalls.Load() + redisCalls.Load(); got != 0 {
		t.Fatalf("store factory calls after Validate = %d, want 0", got)
	}
	if cfg.Fingerprint() != fp {
		t.Fatal("Validate changed the fingerprint of cfg")
	}
	if !reflect.DeepEqual(cfg, before) {
		t.Fatal("Validate changed cfg")
	}

	_, err = newGateway(cfg, opts...)
	sapiWantAt(t, cfg, err, want)
	if got := testCalls.Load() + redisCalls.Load(); got != 0 {
		t.Fatalf("store factory calls after New = %d, want 0", got)
	}

	g := n.gateway(t, stDoc(be.URL, stStore("ok", ""), stRoute("live", "")), opts...)
	rerr := g.Reload(cfg)
	sapiWantAt(t, cfg, rerr, want)
	if got := stCounter(t, g, "microgateway_config_reloads_total", "result", "failure"); got != 1 {
		t.Fatalf("failure reloads after Reload = %v, want 1", got)
	}
	if got := testCalls.Load() + redisCalls.Load(); got != 0 {
		t.Fatalf("store factory calls after Reload = %d, want 0", got)
	}
	if rec := stDo(t, g, "/live", "192.0.2.1:1000"); rec.Code != http.StatusOK {
		t.Fatalf("status after failed Reload = %d, want 200", rec.Code)
	}
}

func TestExamplesValidate(t *testing.T) {
	paths, err := filepath.Glob("../../examples/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no examples/*.yaml files")
	}
	for _, path := range paths {
		cfg, err := config.LoadFile(path)
		if err != nil {
			t.Fatalf("LoadFile(%s) = %v", path, err)
		}
		if err := microgateway.Validate(cfg); err != nil {
			t.Errorf("Validate(%s) = %v", path, err)
		}
	}
}

const smClient = "192.0.2.10:4000"

func smRate(ref string, rate, burst int) string {
	var b strings.Builder
	b.WriteString("    rate_limit:\n")
	fmt.Fprintf(&b, "      rate: %d\n      per: 1s\n      burst: %d\n", rate, burst)
	if ref != "" {
		fmt.Fprintf(&b, "      store: %s\n", ref)
	}
	return b.String()
}

func smDoc(stores, upstreamBody string, hosts []string, routes string) string {
	var b strings.Builder
	b.WriteString("version: 1\n")
	if stores != "" {
		b.WriteString("stores:\n" + stores)
	}
	b.WriteString(listenersYAML + "upstreams:\n  - name: up\n" + upstreamBody)
	if len(hosts) > 0 {
		b.WriteString("    hosts:\n")
		for _, h := range hosts {
			fmt.Fprintf(&b, "      - url: %q\n", h)
		}
	}
	b.WriteString("routes:\n" + routes)
	return b.String()
}

func TestStoreSharedAcrossGateways(t *testing.T) {
	n := newMemNetwork()
	be, _ := rlBackend(t, n)
	shared := stShared(t, rlFake())
	var factoryCalls atomic.Int64
	opt := stOption(shared, &factoryCalls)
	doc := stDoc(be.URL, stStore("shared", ""), stRoute("rl", smRate("shared", 5, 3)))
	gA := n.gateway(t, doc, withClock(rlFake()), opt)
	gB := n.gateway(t, doc, withClock(rlFake()), opt)
	if got := factoryCalls.Load(); got != 2 {
		t.Fatalf("store factory calls = %d, want 2", got)
	}

	rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "2")
	rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "1")
	rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "0")
	rlExpectLimited(t, stDo(t, gB, "/rl", smClient), "1")
	rlExpectLimited(t, stDo(t, gA, "/rl", smClient), "1")

	lim := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
	key := ratelimit.PolicyID("rl", lim) + ":ip:192.0.2.10"
	calls := shared.Calls()
	if len(calls) != 5 {
		t.Fatalf("shared store calls = %d, want 5", len(calls))
	}
	for i, c := range calls {
		if c.Key != key || c.Limit != lim {
			t.Fatalf("call %d = {key %q, limit %+v}, want {key %q, limit %+v}", i, c.Key, c.Limit, key, lim)
		}
	}

	memDoc := stDoc(be.URL, "", stRoute("rl", smRate("", 5, 3)))
	gC := n.gateway(t, memDoc, withClock(rlFake()), opt)
	gD := n.gateway(t, memDoc, withClock(rlFake()), opt)
	for _, g := range []*microgateway.Gateway{gC, gD} {
		rlExpectRemaining(t, stDo(t, g, "/rl", smClient), "2")
		rlExpectRemaining(t, stDo(t, g, "/rl", smClient), "1")
		rlExpectRemaining(t, stDo(t, g, "/rl", smClient), "0")
		rlExpectLimited(t, stDo(t, g, "/rl", smClient), "1")
	}
	if got := len(shared.Calls()); got != 5 {
		t.Fatalf("shared store calls after memory gateways = %d, want 5", got)
	}

	changed := stDoc(be.URL, stStore("shared", ""), stRoute("rl", smRate("shared", 5, 4)))
	if err := gA.ReloadYAML([]byte(changed)); err != nil {
		t.Fatal(err)
	}
	rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "3")
	lim4 := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 4}
	key4 := ratelimit.PolicyID("rl", lim4) + ":ip:192.0.2.10"
	calls = shared.Calls()
	if len(calls) != 6 {
		t.Fatalf("shared store calls after reload = %d, want 6", len(calls))
	}
	if last := calls[5]; last.Key != key4 || last.Limit != lim4 {
		t.Fatalf("call after reload = {key %q, limit %+v}, want {key %q, limit %+v}", last.Key, last.Limit, key4, lim4)
	}
	if key4 == key {
		t.Fatal("changed burst kept the key namespace")
	}
	rlExpectLimited(t, stDo(t, gB, "/rl", smClient), "1")
}

func TestStoreMultiInstanceIndependence(t *testing.T) {
	t.Run("breaker", func(t *testing.T) {
		n := newMemNetwork()
		be := newRSBackend(t, n, rsStatusSeq(http.StatusServiceUnavailable))
		shared := stShared(t, rlFake())
		var factoryCalls atomic.Int64
		opt := stOption(shared, &factoryCalls)
		doc := smDoc(stStore("shared", ""), miTimeouts+miRR+"    circuit_breaker:\n"+rsBreaker, []string{be.URL}, stRoute("rl", smRate("shared", 1000, 1000)))
		gA := n.gateway(t, doc, withClock(rlFake()), opt)
		gB := n.gateway(t, doc, withClock(rlFake()), opt)

		for range 3 {
			if rec := stDo(t, gA, "/rl", smClient); rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("A status = %d, want 503", rec.Code)
			}
		}
		if got := be.hitCount(); got != 3 {
			t.Fatalf("backend hits after A failures = %d, want 3", got)
		}
		bkCircuitOpen(t, stDo(t, gA, "/rl", smClient).Body.Bytes())
		if got := be.hitCount(); got != 3 {
			t.Fatalf("backend hits after short-circuit = %d, want 3", got)
		}
		if st := hostByURL(upstreamStatus(t, gA, "up"), be.URL); st.BreakerState != "open" {
			t.Fatalf("A breaker_state = %q, want open", st.BreakerState)
		}

		if st := hostByURL(upstreamStatus(t, gB, "up"), be.URL); st.BreakerState != "closed" {
			t.Fatalf("B breaker_state = %q, want closed", st.BreakerState)
		}
		if rec := stDo(t, gB, "/rl", smClient); rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("B status = %d, want 503 from backend", rec.Code)
		}
		if got := be.hitCount(); got != 4 {
			t.Fatalf("backend hits after B request = %d, want 4", got)
		}
	})

	t.Run("passive ejection", func(t *testing.T) {
		n := newMemNetwork()
		x := failingBackend(t, n, "x")
		x.Status.Store(http.StatusServiceUnavailable)
		y := failingBackend(t, n, "y")
		shared := stShared(t, rlFake())
		var factoryCalls atomic.Int64
		opt := stOption(shared, &factoryCalls)
		doc := smDoc(stStore("shared", ""), miTimeouts+miRR+miPassive, []string{x.URL, y.URL}, stRoute("rl", smRate("shared", 1000, 1000)))
		gA := n.gateway(t, doc, withClock(rlFake()), opt)
		gB := n.gateway(t, doc, withClock(rlFake()), opt)

		for range 4 {
			stDo(t, gA, "/rl", smClient)
		}
		if h := hostByURL(upstreamStatus(t, gA, "up"), x.URL); !h.Ejected {
			t.Fatal("host x not ejected on A")
		}
		if h := hostByURL(upstreamStatus(t, gB, "up"), x.URL); h.Ejected || h.EjectionCount != 0 {
			t.Fatalf("host x on B ejected=%v ejection_count=%d, want eligible", h.Ejected, h.EjectionCount)
		}
		before := x.Hits.Load()
		for range 4 {
			stDo(t, gB, "/rl", smClient)
		}
		if got := x.Hits.Load() - before; got != 2 {
			t.Fatalf("requests reaching x on B = %d, want 2 of 4", got)
		}
	})

	t.Run("reload", func(t *testing.T) {
		n := newMemNetwork()
		be, _ := rlBackend(t, n)
		shared := stShared(t, rlFake())
		var factoryCalls atomic.Int64
		opt := stOption(shared, &factoryCalls)
		doc := stDoc(be.URL, stStore("shared", ""), stRoute("rl", smRate("shared", 5, 3)))
		gA := n.gateway(t, doc, withClock(rlFake()), opt)
		gB := n.gateway(t, doc, withClock(rlFake()), opt)
		fpA := gA.Config().Fingerprint()
		fpB := gB.Config().Fingerprint()

		if err := gA.ReloadYAML([]byte(stDoc(be.URL, stStore("shared", ""), stRoute("rl", smRate("shared", 7, 3))))); err != nil {
			t.Fatal(err)
		}
		if got := gA.Config().Fingerprint(); got == fpA {
			t.Fatal("A fingerprint unchanged after reload")
		}
		if got := gB.Config().Fingerprint(); got != fpB {
			t.Fatalf("B fingerprint = %q after A reload, want %q", got, fpB)
		}
		rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "2")
	})

	t.Run("guard and local limiter", func(t *testing.T) {
		n := newMemNetwork()
		be, _ := rlBackend(t, n)
		shared := stShared(t, rlFake())
		var factoryCalls atomic.Int64
		opt := stOption(shared, &factoryCalls)
		stores := stStore("shared", "    guard:\n      consecutive_failures: 2\n      open_timeout: 30s\n")
		doc := stDoc(be.URL, stores, stRoute("rl", smRate("shared", 5, 3)))
		gA := n.gateway(t, doc, withClock(rlFake()), opt)
		gB := n.gateway(t, doc, withClock(rlFake()), opt)
		handles := shared.Handles()
		if len(handles) != 2 {
			t.Fatalf("store handles = %d, want 2", len(handles))
		}
		hA, hB := handles[0], handles[1]
		down := fmt.Errorf("shared store down: %w", store.ErrUnavailable)

		hA.SetMode(teststore.Fail, down)
		rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "2")
		rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "1")
		rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "0")
		rlExpectLimited(t, stDo(t, gA, "/rl", smClient), "1")
		if got := hA.Takes(); got != 2 {
			t.Fatalf("A store calls = %d, want 2 before the guard short-circuits", got)
		}
		if v := stGauge(t, gA, "microgateway_store_available", "store", "shared"); v != 0 {
			t.Fatalf("A store_available = %v, want 0", v)
		}
		if v := stCounter(t, gA, "microgateway_store_errors_total", "store", "shared", "op", "take"); v != 2 {
			t.Fatalf("A store_errors_total = %v, want 2", v)
		}

		if v := stGauge(t, gB, "microgateway_store_available", "store", "shared"); v != 1 {
			t.Fatalf("B store_available = %v, want 1", v)
		}
		rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "2")
		rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "1")
		rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "0")
		rlExpectLimited(t, stDo(t, gB, "/rl", smClient), "1")
		if got := hB.Takes(); got != 4 {
			t.Fatalf("B store calls = %d, want 4", got)
		}
		if got := hA.Takes(); got != 2 {
			t.Fatalf("A store calls after B traffic = %d, want 2", got)
		}

		hB.SetMode(teststore.Fail, down)
		rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "2")
		if v := stGauge(t, gB, "microgateway_store_available", "store", "shared"); v != 1 {
			t.Fatalf("B store_available after one failure = %v, want 1", v)
		}
		if v := stCounter(t, gB, "microgateway_store_errors_total", "store", "shared", "op", "take"); v != 1 {
			t.Fatalf("B store_errors_total = %v, want 1", v)
		}
		if v := stGauge(t, gA, "microgateway_store_available", "store", "shared"); v != 0 {
			t.Fatalf("A store_available after B failure = %v, want 0", v)
		}
		rlExpectLimited(t, stDo(t, gA, "/rl", smClient), "1")
	})
}

var spDown = fmt.Errorf("store unreachable: %w", store.ErrUnavailable)

const spStoreTimeout = 200 * time.Millisecond

func spSeries(t *testing.T, g *microgateway.Gateway, name string, labels ...string) (float64, bool) {
	t.Helper()
	prefix := series(name, labels...) + " "
	for _, line := range strings.Split(metricsText(t, g), "\n") {
		if rest, ok := strings.CutPrefix(line, prefix); ok {
			v, err := strconv.ParseFloat(rest, 64)
			if err != nil {
				t.Fatal(err)
			}
			return v, true
		}
	}
	return 0, false
}

func spMustSeries(t *testing.T, g *microgateway.Gateway, name string, labels ...string) float64 {
	t.Helper()
	v, ok := spSeries(t, g, name, labels...)
	if !ok {
		t.Fatalf("series %s is missing", series(name, labels...))
	}
	return v
}

func spExpectValue(t *testing.T, g *microgateway.Gateway, want float64, name string, labels ...string) {
	t.Helper()
	if got := spMustSeries(t, g, name, labels...); got != want {
		t.Fatalf("%s = %v, want %v", series(name, labels...), got, want)
	}
}

func spRateLimit(policy string, perHour int, timeout time.Duration) string {
	return fmt.Sprintf("    rate_limit:\n      rate: %d\n      per: 1h\n      store: shared\n      on_store_error: %s\n      store_timeout: %s\n",
		perHour, policy, timeout)
}

func spCanceledGet(t *testing.T, g *microgateway.Gateway, path, remote string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
	req.RemoteAddr = remote
	g.ServeHTTP(httptest.NewRecorder(), req)
}

func spHang(t *testing.T, fake *clock.Fake, h *teststore.Handle, g *microgateway.Gateway, path, remote string, timeout time.Duration) *httptest.ResponseRecorder {
	t.Helper()
	ch := stAsync(t, g, path, remote)
	h.WaitBlocked(1)
	fake.BlockUntilDue(1, timeout)
	before := fake.Now()
	fake.Advance(timeout)
	if elapsed := fake.Now().Sub(before); elapsed > timeout+100*time.Millisecond {
		t.Fatalf("request elapsed %v of fake time, want <= %v", elapsed, timeout+100*time.Millisecond)
	}
	return <-ch
}

func spExpectDegraded(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("RateLimit-Limit"); got != "" {
		t.Fatalf("RateLimit-Limit = %q, want no rate limit headers", got)
	}
}

func spExpectUnavailable(t *testing.T, rec *httptest.ResponseRecorder, retryAfter string) {
	t.Helper()
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != retryAfter {
		t.Fatalf("Retry-After = %q, want %q", got, retryAfter)
	}
	if e := decodeErr(t, rec.Result()); e.Code != httperr.CodeRateLimitUnavailable {
		t.Fatalf("code = %q, want %q", e.Code, httperr.CodeRateLimitUnavailable)
	}
}

func TestStorePolicyTable(t *testing.T) {
	modes := []struct {
		name string
		mode teststore.Mode
		hang bool
	}{
		{"fail", teststore.Fail, false},
		{"hang", teststore.Hang, true},
		{"hang_ignore_context", teststore.HangIgnoreContext, true},
	}
	for _, m := range modes {
		for _, policy := range []string{"allow", "deny", "local"} {
			t.Run(m.name+"/"+policy, func(t *testing.T) {
				const client = "192.0.2.30:4000"
				fake := rlFake()
				shared := stShared(t, fake)
				shared.SetMode(m.mode, spDown)
				n := newMemNetwork()
				be, hits := rlBackend(t, n)
				var factoryCalls atomic.Int64
				doc := stDoc(be.URL,
					stStore("shared", "    guard:\n      consecutive_failures: 1\n"),
					stRoute("rl", spRateLimit(policy, 3, spStoreTimeout)))
				g := n.gateway(t, doc, withClock(fake), stOption(shared, &factoryCalls))
				handles := shared.Handles()
				if len(handles) != 1 {
					t.Fatalf("handles = %d, want 1", len(handles))
				}
				if factoryCalls.Load() != 1 {
					t.Fatalf("factory calls = %d, want 1", factoryCalls.Load())
				}
				t.Cleanup(handles[0].Release)

				var first *httptest.ResponseRecorder
				if m.hang {
					first = spHang(t, fake, handles[0], g, "/rl", client, spStoreTimeout)
				} else {
					first = stDo(t, g, "/rl", client)
				}
				if got := len(shared.Calls()); got != 1 {
					t.Fatalf("store calls after first request = %d, want 1", got)
				}
				second := stDo(t, g, "/rl", client)
				third := stDo(t, g, "/rl", client)
				if got := len(shared.Calls()); got != 1 {
					t.Fatalf("store calls with guard open = %d, want 1", got)
				}

				switch policy {
				case "allow":
					spExpectDegraded(t, first)
					spExpectDegraded(t, second)
					spExpectDegraded(t, third)
					if got := hits.Load(); got != 3 {
						t.Fatalf("backend requests = %d, want 3", got)
					}
				case "deny":
					spExpectUnavailable(t, first, "2")
					spExpectUnavailable(t, second, "2")
					spExpectUnavailable(t, third, "2")
					if got := hits.Load(); got != 0 {
						t.Fatalf("backend requests = %d, want 0", got)
					}
				case "local":
					rlExpectRemaining(t, first, "2")
					rlExpectRemaining(t, second, "1")
					rlExpectRemaining(t, third, "0")
					rlExpectLimited(t, stDo(t, g, "/rl", client), "1200")
					if got := hits.Load(); got != 3 {
						t.Fatalf("backend requests = %d, want 3", got)
					}
				}

				wantFallbacks := 3
				if policy == "local" {
					wantFallbacks = 4
				}
				spExpectValue(t, g, 0, "microgateway_store_available", "store", "shared")
				spExpectValue(t, g, 1, "microgateway_store_errors_total", "store", "shared", "op", "take")
				spExpectValue(t, g, float64(wantFallbacks), "microgateway_ratelimit_store_fallback_total", "route", "rl", "policy", policy)
				rsCheckExposition(t, metricsText(t, g))
			})
		}
	}
}

func TestStoreRecovery(t *testing.T) {
	const client = "192.0.2.40:4000"
	fake := rlFake()
	shared := stShared(t, fake)
	n := newMemNetwork()
	be, _ := rlBackend(t, n)
	var factoryCalls atomic.Int64
	doc := stDoc(be.URL,
		stStore("shared", "    guard:\n      consecutive_failures: 3\n      open_timeout: 2s\n"),
		stRoute("rl", "    rate_limit:\n      rate: 10\n      per: 1h\n      store: shared\n      on_store_error: local\n      store_timeout: 200ms\n"))
	g := n.gateway(t, doc, withClock(fake), stOption(shared, &factoryCalls))
	handles := shared.Handles()
	if len(handles) != 1 {
		t.Fatalf("handles = %d, want 1", len(handles))
	}

	spExpectValue(t, g, 1, "microgateway_store_available", "store", "shared")
	spExpectValue(t, g, 0, "microgateway_store_errors_total", "store", "shared", "op", "take")
	spExpectValue(t, g, 0, "microgateway_ratelimit_store_fallback_total", "route", "rl", "policy", "local")
	rsCheckExposition(t, metricsText(t, g))

	shared.SetMode(teststore.Fail, spDown)
	for i := range 3 {
		rlExpectRemaining(t, stDo(t, g, "/rl", client), strconv.Itoa(9-i))
	}
	spExpectValue(t, g, 0, "microgateway_store_available", "store", "shared")
	spExpectValue(t, g, 3, "microgateway_store_errors_total", "store", "shared", "op", "take")
	spExpectValue(t, g, 3, "microgateway_ratelimit_store_fallback_total", "route", "rl", "policy", "local")
	if got := len(shared.Calls()); got != 3 {
		t.Fatalf("store calls = %d, want 3", got)
	}

	spCanceledGet(t, g, "/rl", client)
	spExpectValue(t, g, 0, "microgateway_store_available", "store", "shared")
	spExpectValue(t, g, 3, "microgateway_store_errors_total", "store", "shared", "op", "take")
	spExpectValue(t, g, 3, "microgateway_ratelimit_store_fallback_total", "route", "rl", "policy", "local")
	if got := len(shared.Calls()); got != 3 {
		t.Fatalf("store calls after canceled request = %d, want 3", got)
	}

	for i := range 2 {
		rlExpectRemaining(t, stDo(t, g, "/rl", client), strconv.Itoa(6-i))
	}
	spExpectValue(t, g, 5, "microgateway_ratelimit_store_fallback_total", "route", "rl", "policy", "local")
	spExpectValue(t, g, 3, "microgateway_store_errors_total", "store", "shared", "op", "take")
	if got := len(shared.Calls()); got != 3 {
		t.Fatalf("store calls while open = %d, want 3", got)
	}

	fake.Advance(2 * time.Second)
	shared.SetMode(teststore.Healthy, nil)
	rlExpectRemaining(t, stDo(t, g, "/rl", client), "9")
	if got := len(shared.Calls()); got != 4 {
		t.Fatalf("store calls after one probe = %d, want 4", got)
	}
	spExpectValue(t, g, 1, "microgateway_store_available", "store", "shared")
	spExpectValue(t, g, 3, "microgateway_store_errors_total", "store", "shared", "op", "take")
	spExpectValue(t, g, 5, "microgateway_ratelimit_store_fallback_total", "route", "rl", "policy", "local")

	rlExpectRemaining(t, stDo(t, g, "/rl", client), "8")
	if got := len(shared.Calls()); got != 5 {
		t.Fatalf("store calls after recovery = %d, want 5", got)
	}
	rsCheckExposition(t, metricsText(t, g))
}

func TestStoreSharedGuard(t *testing.T) {
	const client = "192.0.2.50:4000"
	fake := rlFake()
	shared := stShared(t, fake)
	n := newMemNetwork()
	be, _ := rlBackend(t, n)
	var factoryCalls atomic.Int64
	routes := stRoute("a", spRateLimit("deny", 10, 100*time.Millisecond)) +
		stRoute("b", spRateLimit("allow", 10, 300*time.Millisecond)) +
		stRoute("c", "    rate_limit:\n      rate: 10\n      per: 1h\n")
	doc := stDoc(be.URL, stStore("shared", ""), routes)
	g := n.gateway(t, doc, withClock(fake), stOption(shared, &factoryCalls))
	handles := shared.Handles()
	if len(handles) != 1 {
		t.Fatalf("handles = %d, want 1", len(handles))
	}
	h := handles[0]

	shared.SetMode(teststore.Fail, spDown)
	for range 3 {
		spExpectUnavailable(t, stDo(t, g, "/a", client), "2")
	}
	spExpectValue(t, g, 0, "microgateway_store_available", "store", "shared")
	if got := len(shared.Calls()); got != 3 {
		t.Fatalf("store calls = %d, want 3", got)
	}

	spExpectDegraded(t, stDo(t, g, "/b", client))
	if got := len(shared.Calls()); got != 3 {
		t.Fatalf("store calls with guard open = %d, want 3", got)
	}
	rlExpectRemaining(t, stDo(t, g, "/c", client), "9")
	if strings.Contains(metricsText(t, g), `microgateway_ratelimit_store_fallback_total{route="c"`) {
		t.Fatal("memory-store route c has a store fallback series")
	}

	fake.Advance(2 * time.Second)
	shared.SetMode(teststore.Hang, nil)
	probe := stAsync(t, g, "/a", client)
	h.WaitBlocked(1)
	if got := len(shared.Calls()); got != 4 {
		t.Fatalf("store calls with probe in flight = %d, want 4", got)
	}
	spExpectDegraded(t, stDo(t, g, "/b", client))
	rlExpectRemaining(t, stDo(t, g, "/c", client), "8")
	if got := len(shared.Calls()); got != 4 {
		t.Fatalf("store calls while probe in flight = %d, want 4", got)
	}

	h.Release()
	rlExpectRemaining(t, <-probe, "9")
	spExpectValue(t, g, 1, "microgateway_store_available", "store", "shared")

	shared.SetMode(teststore.Healthy, nil)
	rlExpectRemaining(t, stDo(t, g, "/b", client), "9")
	if got := len(shared.Calls()); got != 5 {
		t.Fatalf("store calls after recovery = %d, want 5", got)
	}
	rsCheckExposition(t, metricsText(t, g))
}

type srdBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *srdBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *srdBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type srdTee struct{ hs []slog.Handler }

func (t srdTee) Enabled(ctx context.Context, l slog.Level) bool {
	for _, h := range t.hs {
		if h.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (t srdTee) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, h := range t.hs {
		if h.Enabled(ctx, r.Level) {
			errs = append(errs, h.Handle(ctx, r.Clone()))
		}
	}
	return errors.Join(errs...)
}

func (t srdTee) WithAttrs(attrs []slog.Attr) slog.Handler {
	hs := make([]slog.Handler, len(t.hs))
	for i, h := range t.hs {
		hs[i] = h.WithAttrs(attrs)
	}
	return srdTee{hs: hs}
}

func (t srdTee) WithGroup(name string) slog.Handler {
	hs := make([]slog.Handler, len(t.hs))
	for i, h := range t.hs {
		hs[i] = h.WithGroup(name)
	}
	return srdTee{hs: hs}
}

func srdMessages(h *captureHandler) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, r := range h.records {
		out = append(out, r.Message)
	}
	return out
}

type srdForm struct{ label, text string }

func srdForms(v string) []srdForm {
	return []srdForm{
		{"raw", v},
		{"query-escaped", url.QueryEscape(v)},
		{"path-escaped", url.PathEscape(v)},
		{"base64", base64.StdEncoding.EncodeToString([]byte(v))},
		{"raw base64", base64.RawStdEncoding.EncodeToString([]byte(v))},
		{"url base64", base64.URLEncoding.EncodeToString([]byte(v))},
		{"raw url base64", base64.RawURLEncoding.EncodeToString([]byte(v))},
		{"hex", hex.EncodeToString([]byte(v))},
	}
}

func srdLeakCheck(t *testing.T, outputs map[string]string) {
	t.Helper()
	secrets := []struct{ kind, value string }{
		{"redis password", stSecret},
		{"sentinel password", stSentinelSecret},
		{"param token", stParamSecret},
	}
	for _, name := range slices.Sorted(maps.Keys(outputs)) {
		for _, s := range secrets {
			for _, f := range srdForms(s.value) {
				if strings.Contains(outputs[name], f.text) {
					t.Errorf("output %s contains the %s in %s form", name, s.kind, f.label)
				}
			}
		}
	}
}

func srdRender(t *testing.T, v any) string {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "%v|%+v|%#v|%s|%q|%x|%X|%10s\n", v, v, v, v, v, v, v, v)
	fmt.Fprintln(&b, fmt.Sprint(v))
	js, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	b.Write(js)
	b.WriteByte('\n')
	ys, err := yaml.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	b.Write(ys)
	slog.New(slog.NewTextHandler(&b, nil)).Info("render", "value", v)
	slog.New(slog.NewJSONHandler(&b, nil)).Info("render", "value", v)
	return b.String()
}

func srdStore(t *testing.T, cfg *config.Config, name string) *config.Store {
	t.Helper()
	for i := range cfg.Stores {
		if cfg.Stores[i].Name == name {
			return &cfg.Stores[i]
		}
	}
	t.Fatalf("store %q not found in config", name)
	return nil
}

func srdStores() string {
	return fmt.Sprintf(`  - name: cache
    type: redis
    redis:
      mode: sentinel
      master_name: mymaster
      addresses:
        - "sentinel.internal:26379"
      username: app
      password: %q
      sentinel_username: watcher
      sentinel_password: %q
  - name: open
    type: redis
    redis:
      addresses:
        - "open.internal:6379"
      password: ""
  - name: vault
    type: vault
    params:
      token: %q
      empty: ""
      retries: 3
      nested:
        key: %q
`, stSecret, stSentinelSecret, stParamSecret, stParamSecret)
}

func srdRoutes() string {
	return stRoute("cached", "    rate_limit:\n      rate: 2\n      per: 1h\n      burst: 2\n      store: cache\n      on_store_error: local\n") +
		stRoute("vaulted", "    rate_limit:\n      rate: 2\n      per: 1h\n      burst: 2\n      store: vault\n      on_store_error: deny\n")
}

func srdDoc(backend string) string {
	return stDoc(backend, srdStores(), srdRoutes())
}

func srdBadDoc(backend string) string {
	stores := fmt.Sprintf(`  - name: cache
    type: redis
    redis:
      mode: standalone
      addresses:
        - "redis://:%s@cache.internal:6379"
      password: %q
      sentinel_password: %q
      key_prefix: "a{b"
  - name: vault
    type: vault
    params:
      token: %q
`, stSecret, stSecret, stSentinelSecret, stParamSecret)
	return stDoc(backend, stores, stRoute("cached", "    rate_limit:\n      rate: 2\n      per: 1h\n      store: cache\n"))
}

func srdMismatchDoc(backend string) string {
	stores := fmt.Sprintf("  - name: cache\n    type: redis\n    redis:\n      addresses:\n        - \"cache.internal:6379\"\n      db: %q\n", stSecret)
	return stDoc(backend, stores, stRoute("cached", "    rate_limit:\n      rate: 2\n      per: 1h\n      store: cache\n"))
}

func srdEnvDoc(backend, password, onErr string) string {
	stores := fmt.Sprintf("  - name: cache\n    type: redis\n    redis:\n      addresses:\n        - \"cache.internal:6379\"\n      password: %s\n", password)
	route := stRoute("envd", "    rate_limit:\n      rate: 2\n      per: 1h\n      store: cache\n      on_store_error: "+onErr+"\n")
	return stDoc(backend, stores, route)
}

func srdFailed(t *testing.T, err error, what string) string {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected an error", what)
	}
	return err.Error()
}

func TestStoreSecretsNeverSurface(t *testing.T) {
	fake := clock.NewFake(time.Unix(1_700_000_000, 0))
	shared := stShared(t, fake)
	n := newMemNetwork()
	be, _ := rlBackend(t, n)

	jsonBuf, textBuf := &srdBuf{}, &srdBuf{}
	capture := &captureHandler{}
	logger := slog.New(srdTee{hs: []slog.Handler{
		slog.NewJSONHandler(jsonBuf, &slog.HandlerOptions{Level: slog.LevelDebug}),
		slog.NewTextHandler(textBuf, &slog.HandlerOptions{Level: slog.LevelDebug}),
		capture,
	}})

	var rendered []string
	opts := []microgateway.Option{
		withClock(fake),
		microgateway.WithLogger(logger),
		microgateway.WithStoreType("redis", store.CapRateLimit, func(p microgateway.StoreParams) (store.Backend, error) {
			p.Logger.Info("redis store entry built", "params", p, "redis", p.Redis)
			rendered = append(rendered, srdRender(t, p), srdRender(t, &p), srdRender(t, p.Redis))
			h := shared.Handle()
			h.SetMode(teststore.Fail, fmt.Errorf("auth %s sentinel %s rejected: %w", stSecret, stSentinelSecret, store.ErrUnavailable))
			return h, nil
		}),
		microgateway.WithStoreType("vault", store.CapRateLimit, func(p microgateway.StoreParams) (store.Backend, error) {
			p.Logger.Info("vault store entry built", "params", p)
			rendered = append(rendered, srdRender(t, p), srdRender(t, &p))
			h := shared.Handle()
			h.SetMode(teststore.Fail, fmt.Errorf("token %s rejected: %w", stParamSecret, store.ErrUnavailable))
			return h, nil
		}),
	}

	g := n.gateway(t, srdDoc(be.URL), opts...)

	const client = "192.0.2.10:4000"
	var statuses []int
	var limited string
	for range 5 {
		rec := stDo(t, g, "/cached", client)
		statuses = append(statuses, rec.Code)
		if rec.Code == http.StatusTooManyRequests {
			limited = rec.Body.String()
		}
	}
	if want := []int{200, 200, 429, 429, 429}; !slices.Equal(statuses, want) {
		t.Fatalf("cached statuses = %v, want %v", statuses, want)
	}

	var unavailable string
	for range 4 {
		rec := stDo(t, g, "/vaulted", client)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("vaulted status = %d, want 503", rec.Code)
		}
		unavailable = rec.Body.String()
	}
	if limited == "" || unavailable == "" {
		t.Fatal("expected a 429 body and a 503 body")
	}

	msgs := srdMessages(capture)
	for _, want := range []string{
		"rate limit store unavailable; failure policies active",
		"rate limit store call failed; failure policy applied",
	} {
		if !slices.Contains(msgs, want) {
			t.Errorf("missing log record %q", want)
		}
	}
	if got := stGauge(t, g, "microgateway_store_available", "store", "cache"); got != 0 {
		t.Errorf("cache store_available = %v, want 0", got)
	}
	if got := stCounter(t, g, "microgateway_store_errors_total", "store", "cache", "op", "take"); got != 3 {
		t.Errorf("cache store_errors_total = %v, want 3", got)
	}
	if got := stCounter(t, g, "microgateway_store_errors_total", "store", "vault", "op", "take"); got != 3 {
		t.Errorf("vault store_errors_total = %v, want 3", got)
	}

	metricsBody := metricsText(t, g)
	configBody := adminBody(t, g, "/config")

	cfg := g.Config()
	cache := srdStore(t, cfg, "cache")
	vault := srdStore(t, cfg, "vault")
	stored := srdRender(t, cache) + srdRender(t, cache.Redis) + srdRender(t, cache.Redis.Password) + srdRender(t, cache.Redis.SentinelPassword)
	stored += srdRender(t, vault) + srdRender(t, vault.Params) + srdRender(t, &vault.Params)

	var cfgJSON struct {
		Stores []struct {
			Name  string `json:"name"`
			Redis *struct {
				Password         string `json:"password"`
				SentinelPassword string `json:"sentinel_password"`
			} `json:"redis"`
			Params map[string]any `json:"params"`
		} `json:"stores"`
	}
	if err := json.Unmarshal([]byte(configBody), &cfgJSON); err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]int)
	for i, s := range cfgJSON.Stores {
		byName[s.Name] = i
	}
	redisOf := func(name string) (pw, spw string) {
		i, ok := byName[name]
		if !ok || cfgJSON.Stores[i].Redis == nil {
			t.Fatalf("store %q with redis block missing from /config", name)
		}
		return cfgJSON.Stores[i].Redis.Password, cfgJSON.Stores[i].Redis.SentinelPassword
	}
	if pw, spw := redisOf("cache"); pw != config.Redacted || spw != config.Redacted {
		t.Errorf("/config cache passwords = (%q, %q), want REDACTED for both", pw, spw)
	}
	if pw, spw := redisOf("open"); pw != "" || spw != "" {
		t.Errorf("/config open passwords = (%q, %q), want empty for both", pw, spw)
	}
	if i, ok := byName["vault"]; !ok {
		t.Fatal("store vault missing from /config")
	} else {
		params := cfgJSON.Stores[i].Params
		if params["token"] != config.Redacted || params["retries"] != config.Redacted {
			t.Errorf("/config vault params = %v, want token and retries REDACTED", params)
		}
		if params["empty"] != "" {
			t.Errorf("/config vault empty param = %v, want empty", params["empty"])
		}
		nested, _ := params["nested"].(map[string]any)
		if nested["key"] != config.Redacted {
			t.Errorf("/config vault nested key = %v, want REDACTED", nested["key"])
		}
	}

	if got := cache.Redis.Password.Reveal(); got != stSecret {
		t.Error("source password no longer reveals after /config")
	}
	if got := cache.Redis.SentinelPassword.Reveal(); got != stSentinelSecret {
		t.Error("source sentinel password no longer reveals after /config")
	}
	if !slices.Contains(vault.Params.StringLeaves(), stParamSecret) {
		t.Error("source params no longer reveal their token after /config")
	}

	_, errBad := newGatewayFromYAML([]byte(srdBadDoc(be.URL)), opts...)
	badText := srdFailed(t, errBad, "New with invalid store fields")
	_, errBare := newGatewayFromYAML([]byte(srdBadDoc(be.URL)))
	bareText := srdFailed(t, errBare, "New without registered types")
	_, errMismatch := newGatewayFromYAML([]byte(srdMismatchDoc(be.URL)), opts...)
	mismatchText := srdFailed(t, errMismatch, "New with a type mismatch")

	_, errFactory := newGatewayFromYAML([]byte(srdDoc(be.URL)),
		withClock(fake),
		microgateway.WithStoreType("redis", store.CapRateLimit, func(p microgateway.StoreParams) (store.Backend, error) {
			return nil, fmt.Errorf("dial %s with password %s refused", p.Redis.Addresses[0], p.Redis.Password.Reveal())
		}),
		microgateway.WithStoreType("vault", store.CapRateLimit, func(p microgateway.StoreParams) (store.Backend, error) {
			return shared.Handle(), nil
		}),
	)
	factoryText := srdFailed(t, errFactory, "New with a failing factory")

	valid, err := config.Parse([]byte(srdDoc(be.URL)))
	if err != nil {
		t.Fatal(err)
	}
	valid.Stores[0].Redis.Addresses = []string{"redis://:" + stSecret + "@cache.internal:6379"}
	errValidateBare := microgateway.Validate(valid)
	validateBareText := srdFailed(t, errValidateBare, "Validate without registered types")
	errValidate := microgateway.Validate(valid, opts...)
	validateText := srdFailed(t, errValidate, "Validate with registered types")

	unsetLookup := func(string) (string, bool) { return "", false }
	_, errUnset := newGatewayFromYAML([]byte(srdEnvDoc(be.URL, "${LG_UNSET_PW}", "local")), microgateway.WithEnvLookup(unsetLookup))
	unsetText := srdFailed(t, errUnset, "New with an unset env reference")
	if !strings.Contains(unsetText, "LG_UNSET_PW") {
		t.Errorf("unset env error does not name LG_UNSET_PW")
	}
	setLookup := func(k string) (string, bool) {
		if k == "LG_SET_PW" {
			return stSecret, true
		}
		return "", false
	}
	_, errSet := newGatewayFromYAML([]byte(srdEnvDoc(be.URL, "${LG_SET_PW}", "maybe")), withClock(fake), microgateway.WithEnvLookup(setLookup),
		microgateway.WithStoreType("redis", store.CapRateLimit, func(p microgateway.StoreParams) (store.Backend, error) {
			return shared.Handle(), nil
		}))
	setText := srdFailed(t, errSet, "New with a set env reference and an invalid field")

	outputs := map[string]string{
		"log.json":        jsonBuf.String(),
		"log.text":        textBuf.String(),
		"metrics":         metricsBody,
		"config":          configBody,
		"response.429":    limited,
		"response.503":    unavailable,
		"factory":         strings.Join(rendered, "\n"),
		"format.store":    stored,
		"new.invalid":     badText,
		"new.bare":        bareText,
		"new.mismatch":    mismatchText,
		"new.factory":     factoryText,
		"validate.bare":   validateBareText,
		"validate":        validateText,
		"env.unset":       unsetText,
		"env.set.invalid": setText,
	}
	srdLeakCheck(t, outputs)

	if !strings.Contains(outputs["log.json"], config.Redacted) {
		t.Error("log output does not carry the scrubbed backend error")
	}
	if !strings.Contains(outputs["factory"], config.Redacted) {
		t.Error("factory renderings do not show the redacted secrets")
	}
}

const srlGlobalID = "global:\n  request_id_header: X-Trace-Id\n"

func srlType(shared *teststore.Shared, calls *atomic.Int64) microgateway.Option {
	return microgateway.WithStoreType("test", store.CapRateLimit, func(p microgateway.StoreParams) (store.Backend, error) {
		calls.Add(1)
		if p.Name == "bad" {
			return nil, errors.New("cannot open bad store")
		}
		return shared.Handle(), nil
	})
}

func srlDoc(backend, global, stores, extraUpstreams, routes string) string {
	if stores != "" {
		stores = "stores:\n" + stores
	}
	return "version: 1\n" + global + listenersYAML + stores +
		fmt.Sprintf("upstreams:\n  - name: up\n    hosts:\n      - url: %q\n", backend) +
		extraUpstreams + "routes:\n" + routes
}

func srlStore(name string, failures int, endpoint string) string {
	return stStore(name, fmt.Sprintf("    guard:\n      consecutive_failures: %d\n    params:\n      endpoint: %s\n", failures, endpoint))
}

func srlRateLimit(storeName string) string {
	body := "    rate_limit:\n      rate: 3\n      per: 1h\n      max_keys: 100\n      key:\n        source: ip\n"
	if storeName != "" {
		body += "      store: " + storeName + "\n"
	}
	return body
}

func srlSample(t *testing.T, g *microgateway.Gateway, s string) (float64, bool) {
	t.Helper()
	for _, line := range strings.Split(metricsText(t, g), "\n") {
		if rest, ok := strings.CutPrefix(line, s+" "); ok {
			v, err := strconv.ParseFloat(rest, 64)
			if err != nil {
				t.Fatal(err)
			}
			return v, true
		}
	}
	return 0, false
}

func srlExpectSample(t *testing.T, g *microgateway.Gateway, s string, want float64) {
	t.Helper()
	got, ok := srlSample(t, g, s)
	if !ok {
		t.Fatalf("%s is absent, want %v", s, want)
	}
	if got != want {
		t.Fatalf("%s = %v, want %v", s, got, want)
	}
}

func srlExpectAbsent(t *testing.T, g *microgateway.Gateway, s string) {
	t.Helper()
	if v, ok := srlSample(t, g, s); ok {
		t.Fatalf("%s = %v is present, want absent", s, v)
	}
}

func TestStoreReloadRetention(t *testing.T) {
	n := newMemNetwork()
	fake := rlFake()
	be, _ := rlBackend(t, n)
	shared := stShared(t, fake)
	shared.SetMode(teststore.Fail, fmt.Errorf("store down: %w", store.ErrUnavailable))
	var calls atomic.Int64
	const client = "192.0.2.1:1000"

	g := n.gateway(t, srlDoc(be.URL, "", srlStore("s1", 2, "v0"), "", stRoute("rl", srlRateLimit("s1"))), withClock(fake), srlType(shared, &calls))
	reload := func(doc string) {
		t.Helper()
		if err := g.ReloadYAML([]byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	available := func(name string) string {
		return fmt.Sprintf("microgateway_store_available{store=%q}", name)
	}
	routes := stRoute("rl", srlRateLimit("s1")) + stRoute("other", "")

	rlExhaust(t, g, "/rl", client, nil)
	if got := calls.Load(); got != 1 {
		t.Fatalf("factory calls = %d, want 1", got)
	}
	if got := len(shared.Calls()); got != 2 {
		t.Fatalf("store calls = %d, want 2", got)
	}
	srlExpectSample(t, g, available("s1"), 0)
	srlExpectSample(t, g, `microgateway_store_errors_total{store="s1",op="take"}`, 2)

	reload(srlDoc(be.URL, "", srlStore("s1", 2, "v0"), "", routes))
	reload(srlDoc(be.URL, srlGlobalID, srlStore("s1", 2, "v0"), "", routes))
	reload(srlDoc(be.URL, srlGlobalID, srlStore("s1", 2, "v0"), "", routes))
	srlExpectSample(t, g, `microgateway_config_reloads_total{result="unchanged"}`, 1)
	if got := calls.Load(); got != 1 {
		t.Fatalf("factory calls after retained reloads = %d, want 1", got)
	}
	if got := len(shared.Handles()); got != 1 {
		t.Fatalf("handles after retained reloads = %d, want 1", got)
	}
	rlExpectLimited(t, stDo(t, g, "/rl", client), "1200")
	if got := len(shared.Calls()); got != 2 {
		t.Fatalf("store calls with the guard open = %d, want 2", got)
	}
	srlExpectSample(t, g, available("s1"), 0)

	reload(srlDoc(be.URL, srlGlobalID, srlStore("s1", 3, "v0"), "", routes))
	srlExpectSample(t, g, available("s1"), 1)
	rlExpectLimited(t, stDo(t, g, "/rl", client), "1200")
	if got := shared.Calls(); len(got) != 3 || got[2].Handle != 0 {
		t.Fatalf("store calls after guard-only change = %+v, want a third call on handle 0", got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("factory calls after guard-only change = %d, want 1", got)
	}
	srlExpectSample(t, g, available("s1"), 1)
	srlExpectSample(t, g, `microgateway_store_errors_total{store="s1",op="take"}`, 3)

	h0 := shared.Handles()[0]
	h0.SetMode(teststore.Hang, nil)
	ch := stAsync(t, g, "/rl", client)
	h0.WaitBlocked(1)
	reload(srlDoc(be.URL, srlGlobalID, srlStore("s1", 3, "v1"), "", routes))
	if got := calls.Load(); got != 2 {
		t.Fatalf("factory calls after params change = %d, want 2", got)
	}
	if got := len(shared.Handles()); got != 2 {
		t.Fatalf("handles after params change = %d, want 2", got)
	}
	if got := h0.Closes(); got != 0 {
		t.Fatalf("old handle closed %d times while its request is in flight, want 0", got)
	}
	h0.Release()
	if rec := <-ch; rec.Code != http.StatusOK {
		t.Fatalf("in-flight request status = %d, want 200", rec.Code)
	}
	if got := h0.Closes(); got != 1 {
		t.Fatalf("old handle closed %d times after drain, want 1", got)
	}

	renamed := stRoute("rl", srlRateLimit("s1b")) + stRoute("other", "")
	reload(srlDoc(be.URL, srlGlobalID, srlStore("s1b", 3, "v1"), "", renamed))
	if got := calls.Load(); got != 3 {
		t.Fatalf("factory calls after rename = %d, want 3", got)
	}
	if got := len(shared.Handles()); got != 3 {
		t.Fatalf("handles after rename = %d, want 3", got)
	}
	if got := shared.Handles()[1].Closes(); got != 1 {
		t.Fatalf("renamed-away handle closed %d times, want 1", got)
	}
	srlExpectAbsent(t, g, available("s1"))
	srlExpectSample(t, g, available("s1b"), 1)

	reload(srlDoc(be.URL, srlGlobalID, srlStore("s1b", 3, "v1")+srlStore("aux", 2, "v0"), "", renamed+stRoute("aux", srlRateLimit("aux"))))
	if got := calls.Load(); got != 4 {
		t.Fatalf("factory calls after adding aux = %d, want 4", got)
	}
	if got := shared.Handles()[3].Closes(); got != 0 {
		t.Fatalf("aux handle closed %d times while referenced, want 0", got)
	}
	srlExpectSample(t, g, available("aux"), 1)

	reload(srlDoc(be.URL, srlGlobalID, srlStore("s1b", 3, "v1"), "", renamed))
	if got := calls.Load(); got != 4 {
		t.Fatalf("factory calls after unreferencing aux = %d, want 4", got)
	}
	if got := shared.Handles()[3].Closes(); got != 1 {
		t.Fatalf("unreferenced aux handle closed %d times, want 1", got)
	}
	srlExpectAbsent(t, g, available("aux"))

	failing := srlDoc(be.URL, srlGlobalID,
		srlStore("s1b", 3, "v1")+srlStore("fresh", 2, "v0")+srlStore("bad", 2, "v0"), "",
		renamed+stRoute("fresh", srlRateLimit("fresh"))+stRoute("bad", srlRateLimit("bad")))
	if err := g.ReloadYAML([]byte(failing)); err == nil {
		t.Fatal("reload with a failing store factory succeeded")
	}
	if got := calls.Load(); got != 6 {
		t.Fatalf("factory calls after failing reload = %d, want 6", got)
	}
	if got := len(shared.Handles()); got != 5 {
		t.Fatalf("handles after failing reload = %d, want 5", got)
	}
	if got := shared.Handles()[4].Closes(); got != 1 {
		t.Fatalf("handle created by the failed build closed %d times, want 1", got)
	}
	if got := shared.Handles()[2].Closes(); got != 0 {
		t.Fatalf("active handle closed %d times after failing reload, want 0", got)
	}
	for _, r := range g.Config().Routes {
		if r.Name == "bad" || r.Name == "fresh" {
			t.Fatalf("route %q from the failed reload is active", r.Name)
		}
	}

	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	for i, h := range shared.Handles() {
		if got := h.Closes(); got != 1 {
			t.Fatalf("handle %d closed %d times after Close, want 1", i, got)
		}
	}
}

func TestStoreReloadNoGoroutineLeak(t *testing.T) {
	pre := snapGoroutines()
	n := newMemNetwork()
	fake := rlFake()
	be, _ := rlBackend(t, n)
	shared := stShared(t, fake)
	var calls atomic.Int64
	doc := func(failures int) string {
		return srlDoc(be.URL, "", srlStore("s1", failures, "v0"), "", stRoute("rl", srlRateLimit("s1")))
	}
	g := n.gateway(t, doc(3), withClock(fake), srlType(shared, &calls))
	t.Cleanup(func() { _ = g.Close() })

	base := snapGoroutines()
	for i := range 100 {
		failures := 4
		if i%2 == 1 {
			failures = 3
		}
		if err := g.ReloadYAML([]byte(doc(failures))); err != nil {
			t.Fatal(err)
		}
	}
	awaitGoroutinesSettled(t, "goroutines of replaced stores to exit after 100 reloads", pre, base)
}

func TestStoreGaugePrunedAfterDrain(t *testing.T) {
	n := newMemNetwork()
	fake := rlFake()
	be, _ := rlBackend(t, n)
	shared := stShared(t, fake)
	var calls atomic.Int64
	const client = "192.0.2.1:1000"

	extraUps := fmt.Sprintf("  - name: up2\n    hosts:\n      - url: %q\n", be.URL)
	side := "  - name: side\n    match:\n      path: /side\n    upstream: up2\n"
	initial := srlDoc(be.URL, "", srlStore("s1", 2, "v0"), extraUps,
		stRoute("rl", srlRateLimit("s1"))+stRoute("mem", srlRateLimit(""))+side+stRoute("keep", ""))
	g := n.gateway(t, initial, withClock(fake), srlType(shared, &calls))

	h0 := shared.Handles()[0]
	h0.SetMode(teststore.Hang, nil)
	ch := stAsync(t, g, "/rl", client)
	h0.WaitBlocked(1)

	if err := g.ReloadYAML([]byte(srlDoc(be.URL, "", "", "", stRoute("keep", "")))); err != nil {
		t.Fatal(err)
	}
	srlExpectSample(t, g, `microgateway_store_available{store="s1"}`, 1)
	srlExpectSample(t, g, `microgateway_store_errors_total{store="s1",op="take"}`, 0)
	srlExpectSample(t, g, `microgateway_ratelimit_keys{route="rl"}`, 0)
	srlExpectSample(t, g, `microgateway_ratelimit_keys{route="mem"}`, 0)
	srlExpectSample(t, g, `microgateway_upstream_panic_mode{upstream="up2"}`, 0)

	h0.Release()
	if rec := <-ch; rec.Code != http.StatusOK {
		t.Fatalf("in-flight request status = %d, want 200", rec.Code)
	}

	srlExpectAbsent(t, g, `microgateway_store_available{store="s1"}`)
	srlExpectSample(t, g, `microgateway_store_errors_total{store="s1",op="take"}`, 0)
	srlExpectAbsent(t, g, `microgateway_ratelimit_keys{route="rl"}`)
	srlExpectAbsent(t, g, `microgateway_ratelimit_keys{route="mem"}`)
	srlExpectAbsent(t, g, `microgateway_upstream_panic_mode{upstream="up2"}`)
	srlExpectSample(t, g, `microgateway_upstream_panic_mode{upstream="up"}`, 0)
}

func TestInstancesShareRateLimitState(t *testing.T) {
	n := newMemNetwork()
	be, _ := rlBackend(t, n)
	storeClk := rlFake()
	shared := stShared(t, storeClk)
	spy := teststore.NewSpy()
	var factoryCalls atomic.Int64
	opt := miSpyOption(spy, store.CapRateLimit, shared, nil, &factoryCalls)
	doc := stDoc(be.URL, stStore("spy", ""), stRoute("rl", smRate("spy", 1, 3)))
	gws, fakes := miInstances(t, n, 3, doc, time.Unix(rlStart, 0), time.Hour, opt)
	gA, gB, gC := gws[0], gws[1], gws[2]
	if got := factoryCalls.Load(); got != 3 {
		t.Fatalf("store factory calls = %d, want 3", got)
	}

	rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "2")
	rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "1")
	rlExpectRemaining(t, stDo(t, gC, "/rl", smClient), "0")
	rlExpectLimited(t, stDo(t, gA, "/rl", smClient), "1")

	fakes[0].Advance(5 * time.Hour)
	rlExpectLimited(t, stDo(t, gA, "/rl", smClient), "1")

	storeClk.Advance(time.Second - time.Nanosecond)
	rlExpectLimited(t, stDo(t, gB, "/rl", smClient), "1")
	storeClk.Advance(time.Nanosecond)
	rlExpectRemaining(t, stDo(t, gC, "/rl", smClient), "0")
	rlExpectLimited(t, stDo(t, gA, "/rl", smClient), "1")

	storeClk.Advance(time.Second + time.Nanosecond)
	rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "0")
	rlExpectLimited(t, stDo(t, gC, "/rl", smClient), "1")

	if got := spy.Count("take"); got != 10 {
		t.Fatalf("take calls = %d, want 10", got)
	}

	memDoc := stDoc(be.URL, "", stRoute("rl", smRate("", 1, 3)))
	memGws, _ := miInstances(t, n, 3, memDoc, time.Unix(rlStart, 0), time.Hour, opt)
	for _, g := range memGws {
		rlExpectRemaining(t, stDo(t, g, "/rl", smClient), "2")
		rlExpectRemaining(t, stDo(t, g, "/rl", smClient), "1")
		rlExpectRemaining(t, stDo(t, g, "/rl", smClient), "0")
		rlExpectLimited(t, stDo(t, g, "/rl", smClient), "1")
	}
	if got := spy.Count("take"); got != 10 {
		t.Fatalf("take calls after memory gateways = %d, want 10", got)
	}
	if got := factoryCalls.Load(); got != 3 {
		t.Fatalf("store factory calls after memory gateways = %d, want 3", got)
	}
}

func TestCustomStoreTypeAcrossInstances(t *testing.T) {
	n := newMemNetwork()
	origin := newFlightOrigin()
	origin.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "body")
	be := n.serve(t, origin)
	storeClk := rlFake()
	shared := stShared(t, storeClk)
	sc := teststore.NewSharedCache(storeClk, 1<<20)
	t.Cleanup(func() { _ = sc.Close() })
	spy := teststore.NewSpy()
	var mu sync.Mutex
	made := map[string][]*teststore.SpyHandle{}
	var factoryCalls atomic.Int64
	opt := microgateway.WithStoreType("custom", store.CapRateLimit|store.CapCache, func(p microgateway.StoreParams) (store.Backend, error) {
		factoryCalls.Add(1)
		h := spy.Wrap(shared.Handle(), sc.Handle())
		mu.Lock()
		defer mu.Unlock()
		made[p.Name] = append(made[p.Name], h)
		return h, nil
	})

	const stores = "stores:\n  - name: s1\n    type: custom\n  - name: s2\n    type: custom\ncache:\n  store: s2\n"
	routes := rlRoute("rl", "    rate_limit:\n      rate: 1\n      per: 1h\n      burst: 2\n      store: s1\n") +
		rlRoute("c", "    cache:\n      enabled: true\n")
	doc := cacheGatewayDoc(be.URL, stores, routes)
	gA := n.gateway(t, doc, withClock(clock.NewFake(time.Unix(rlStart, 0))), opt)
	gB := n.gateway(t, doc, withClock(clock.NewFake(time.Unix(rlStart, 0))), opt)
	if got := factoryCalls.Load(); got != 4 {
		t.Fatalf("store factory calls = %d, want 4", got)
	}
	if len(made) != 2 || len(made["s1"]) != 2 || len(made["s2"]) != 2 {
		t.Fatalf("handles by store = %d names, s1 %d, s2 %d; want 2 names with 2 handles each",
			len(made), len(made["s1"]), len(made["s2"]))
	}

	rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "1")
	rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "0")
	rlExpectLimited(t, stDo(t, gA, "/rl", smClient), "3600")

	const target = "http://h/c"
	if st := purgeLast(t, purgeGet(t, gA, target, nil)); st.Hit {
		t.Fatalf("A first /c %+v, want miss", st)
	}
	if st := purgeLast(t, purgeGet(t, gB, target, nil)); !st.Hit {
		t.Fatalf("B /c %+v, want hit", st)
	}

	rlOnly := microgateway.WithStoreType("rlonly", store.CapRateLimit, func(microgateway.StoreParams) (store.Backend, error) {
		return nil, errors.New("rlonly store must not be built")
	})
	badDoc := cacheGatewayDoc(be.URL, "stores:\n  - name: rlonly\n    type: rlonly\ncache:\n  store: rlonly\n",
		rlRoute("c", "    cache:\n      enabled: true\n"))
	wantLine := 0
	for i, line := range strings.Split(badDoc, "\n") {
		if strings.TrimSpace(line) == "store: rlonly" {
			wantLine = i + 1
		}
	}
	_, err := microgateway.NewFromYAML([]byte(badDoc), rlOnly)
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("NewFromYAML error = %T (%v), want *config.ValidationError", err, err)
	}
	found := false
	for _, fe := range ve.Errors {
		if fe.Path == "cache.store" {
			found = true
			if fe.Line != wantLine {
				t.Fatalf("cache.store error line = %d, want %d", fe.Line, wantLine)
			}
		}
	}
	if !found {
		t.Fatalf("no cache.store error in %v", ve.Errors)
	}

	s1A, s1B := made["s1"][0], made["s1"][1]
	s2A, s2B := made["s2"][0], made["s2"][1]
	noCache := cacheGatewayDoc(be.URL, "stores:\n  - name: s1\n    type: custom\n",
		rlRoute("rl", "    rate_limit:\n      rate: 1\n      per: 1h\n      burst: 2\n      store: s1\n"))
	if err := gA.ReloadYAML([]byte(noCache)); err != nil {
		t.Fatal(err)
	}
	if got := s2A.Closes(); got != 1 {
		t.Fatalf("A s2 handle closed %d times after removal, want 1", got)
	}
	if got := s2B.Closes(); got != 0 {
		t.Fatalf("B s2 handle closed %d times after A removal, want 0", got)
	}
	if err := gA.ReloadYAML([]byte(noCache)); err != nil {
		t.Fatal(err)
	}
	if got := s2A.Closes(); got != 1 {
		t.Fatalf("A s2 handle closed %d times after repeated reload, want 1", got)
	}
	if err := gA.Close(); err != nil {
		t.Fatal(err)
	}
	if got := s1A.Closes(); got != 1 {
		t.Fatalf("A s1 handle closed %d times after Close, want 1", got)
	}
	if got := s1B.Closes(); got != 0 {
		t.Fatalf("B s1 handle closed %d times after A Close, want 0", got)
	}
	if got := factoryCalls.Load(); got != 4 {
		t.Fatalf("store factory calls after reloads and Close = %d, want 4", got)
	}
}

func TestInstancesMixedSharedStore(t *testing.T) {
	pre := snapGoroutines()
	n := newMemNetwork()
	storeClk := rlFake()
	shared := stShared(t, storeClk)
	sc := teststore.NewSharedCache(storeClk, 1<<20)
	t.Cleanup(func() { _ = sc.Close() })
	spy := teststore.NewSpy()
	var factoryCalls atomic.Int64
	opt := miSpyOption(spy, store.CapRateLimit|store.CapCache, shared, sc, &factoryCalls)
	origin := newFlightOrigin()
	origin.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Surrogate-Key": {"tg"}}, "body")
	be := n.serve(t, origin)
	top := "stores:\n" + stStore("s", "") + "cache:\n  store: s\n" + purgeAdminYAML(purgeTokA)
	routes := rlRoute("rl", "    rate_limit:\n      rate: 1\n      per: 1h\n      burst: 6\n      store: s\n") +
		rlRoute("c", "    cache:\n      enabled: true\n") +
		rlRoute("crl", "    rate_limit:\n      rate: 1\n      per: 1h\n      burst: 1\n      store: s\n    cache:\n      enabled: true\n")
	gs, _ := miInstances(t, n, 3, cacheGatewayDoc(be.URL, top, routes), time.Unix(rlStart, 0), time.Hour, opt)
	gA, gB, gC := gs[0], gs[1], gs[2]
	if got := factoryCalls.Load(); got != 3 {
		t.Fatalf("store factory calls = %d, want 3", got)
	}
	base := snapGoroutines()
	const target = "http://h/c"

	if st := purgeLast(t, purgeGet(t, gA, target, nil)); st.Hit {
		t.Fatalf("A first /c %+v, want miss", st)
	}
	for _, g := range []*microgateway.Gateway{gB, gC} {
		if st := purgeLast(t, purgeGet(t, g, target, nil)); !st.Hit {
			t.Fatalf("/c %+v, want hit", st)
		}
	}
	if calls, _, _ := origin.counts(); calls != 1 {
		t.Fatalf("origin calls after fill = %d, want 1", calls)
	}

	var pending []<-chan *httptest.ResponseRecorder
	for _, g := range gs {
		for range 6 {
			pending = append(pending, stAsync(t, g, "/rl", smClient))
		}
	}
	admitted := 0
	for _, ch := range pending {
		rec, _ := testwait.Recv(t, "rate-limited response", ch, nil)
		if rec.Code == http.StatusOK {
			admitted++
			continue
		}
		rlExpectLimited(t, rec, "3600")
	}
	if admitted != 6 {
		t.Fatalf("admitted /rl requests = %d, want 6", admitted)
	}
	if got := spy.Count("take"); got != 18 {
		t.Fatalf("take calls = %d, want 18", got)
	}
	rlCalls := len(shared.Calls())

	drain := func() {
		for range len(origin.arrived) {
			<-origin.arrived
		}
	}
	fetchHeld := func(what string, held ...*microgateway.Gateway) {
		t.Helper()
		before, _, _ := origin.counts()
		drain()
		origin.hold()
		chans := make([]<-chan *http.Response, 0, len(held))
		for _, g := range held {
			ch := make(chan *http.Response, 1)
			go func() { ch <- purgeGet(t, g, target, nil) }()
			chans = append(chans, ch)
		}
		for range held {
			awaitSignal(t, origin.arrived, what+": origin request")
		}
		origin.release()
		for _, ch := range chans {
			resp, _ := testwait.Recv(t, what+": response", ch, nil)
			if st := purgeLast(t, resp); st.Hit {
				t.Fatalf("%s: response %+v, want no hit", what, st)
			}
		}
		if after, _, _ := origin.counts(); after-before != len(held) {
			t.Fatalf("%s: origin calls = %d, want %d", what, after-before, len(held))
		}
	}

	purgeOK(t, purgeDo(t, gA, purgeTokA, `{"urls":["`+target+`"]}`))
	fetchHeld("after URL purge", gB, gC)

	if _, err := gC.Purge(context.Background(), microgateway.PurgeRequest{Tags: []string{"tg"}}); err != nil {
		t.Fatal(err)
	}
	fetchHeld("after tag purge", gA, gB)

	if _, err := gB.Purge(context.Background(), microgateway.PurgeRequest{All: true}); err != nil {
		t.Fatal(err)
	}
	fetchHeld("after purge all", gA, gC)

	if got := len(shared.Calls()); got != rlCalls {
		t.Fatalf("rate-limit store calls after purges = %d, want %d", got, rlCalls)
	}
	for _, g := range gs {
		rlExpectLimited(t, stDo(t, g, "/rl", smClient), "3600")
	}

	const crl = "http://h/crl"
	first := purgeGet(t, gA, crl, nil)
	if first.StatusCode != http.StatusOK || !purgeLast(t, first).Stored {
		t.Fatalf("A /crl status = %d, want 200 stored", first.StatusCode)
	}
	purgeOK(t, purgeDo(t, gA, purgeTokA, `{"all":true}`))
	mark := len(spy.Calls())
	if denied := purgeGet(t, gB, crl, nil); denied.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("B /crl status = %d, want 429", denied.StatusCode)
	}
	for _, c := range miSpyDelta(spy, mark) {
		if c.Op == "get" || c.Op == "put" {
			t.Fatalf("B denied request made a %s call, want none", c.Op)
		}
	}
	storeClk.Advance(time.Hour)
	third := purgeGet(t, gC, crl, nil)
	if third.StatusCode != http.StatusOK || purgeLast(t, third).Hit {
		t.Fatalf("C /crl status = %d, want 200 and no hit", third.StatusCode)
	}

	for _, g := range gs {
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
	}
	awaitGoroutinesSettled(t, "mixed run", pre, base)
}

func TestInstancesStoreFailureIndependent(t *testing.T) {
	n := newMemNetwork()
	be, hits := rlBackend(t, n)
	storeClk := rlFake()
	shared := stShared(t, storeClk)
	sc := teststore.NewSharedCache(storeClk, 1<<20)
	t.Cleanup(func() { _ = sc.Close() })
	spy := teststore.NewSpy()
	opt := miSpyOption(spy, store.CapRateLimit|store.CapCache, shared, sc, nil)
	top := "stores:\n" + stStore("s", "    guard:\n      consecutive_failures: 2\n      open_timeout: 30s\n") + "cache:\n  store: s\n"
	cacheRoute := rlRoute("c", "    cache:\n      enabled: true\n")
	docFor := func(policy string) string {
		return cacheGatewayDoc(be.URL, top, rlRoute("rl", fmt.Sprintf(
			"    rate_limit:\n      rate: 1\n      per: 1h\n      burst: 5\n      store: s\n      on_store_error: %s\n", policy))+cacheRoute)
	}
	fA, fB, fC := rlFake(), rlFake(), rlFake()
	gA := n.gateway(t, docFor("local"), withClock(fA), opt)
	gB := n.gateway(t, docFor("allow"), withClock(fB), opt)
	gC := n.gateway(t, docFor("deny"), withClock(fC), opt)
	handles := spy.Handles()
	if len(handles) != 3 {
		t.Fatalf("store handles = %d, want 3", len(handles))
	}
	idA, idB, idC := handles[0].ID(), handles[1].ID(), handles[2].ID()
	avail := func(g *microgateway.Gateway) float64 {
		return stGauge(t, g, "microgateway_store_available", "store", "s")
	}
	expectAvail := func(what string, g *microgateway.Gateway, want float64) {
		t.Helper()
		if got := avail(g); got != want {
			t.Fatalf("%s store_available = %v, want %v", what, got, want)
		}
	}

	rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "4")
	rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "3")
	rlExpectRemaining(t, stDo(t, gC, "/rl", smClient), "2")

	spy.SetMode("", teststore.Fail, spDown)

	takesA := spy.CountHandle(idA, "take")
	rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "4")
	expectAvail("A after one failure", gA, 1)
	rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "3")
	expectAvail("A after two failures", gA, 0)
	if got := spy.CountHandle(idA, "take") - takesA; got != 2 {
		t.Fatalf("A store calls = %d, want 2", got)
	}
	rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "2")
	if got := spy.CountHandle(idA, "take") - takesA; got != 2 {
		t.Fatalf("A store calls with guard open = %d, want 2", got)
	}

	takesB := spy.CountHandle(idB, "take")
	spExpectDegraded(t, stDo(t, gB, "/rl", smClient))
	expectAvail("B after one failure", gB, 1)
	spExpectDegraded(t, stDo(t, gB, "/rl", smClient))
	expectAvail("B after two failures", gB, 0)
	if got := spy.CountHandle(idB, "take") - takesB; got != 2 {
		t.Fatalf("B store calls = %d, want 2", got)
	}
	spExpectDegraded(t, stDo(t, gB, "/rl", smClient))
	if got := spy.CountHandle(idB, "take") - takesB; got != 2 {
		t.Fatalf("B store calls with guard open = %d, want 2", got)
	}

	takesC := spy.CountHandle(idC, "take")
	spExpectUnavailable(t, stDo(t, gC, "/rl", smClient), "30")
	expectAvail("C after one failure", gC, 1)
	spExpectUnavailable(t, stDo(t, gC, "/rl", smClient), "30")
	expectAvail("C after two failures", gC, 0)
	if got := spy.CountHandle(idC, "take") - takesC; got != 2 {
		t.Fatalf("C store calls = %d, want 2", got)
	}
	spExpectUnavailable(t, stDo(t, gC, "/rl", smClient), "30")
	if got := spy.CountHandle(idC, "take") - takesC; got != 2 {
		t.Fatalf("C store calls with guard open = %d, want 2", got)
	}

	owned := []struct {
		g      *microgateway.Gateway
		policy string
	}{{gA, "local"}, {gB, "allow"}, {gC, "deny"}}
	for _, o := range owned {
		for _, p := range []string{"local", "allow", "deny"} {
			want := 0.0
			if p == o.policy {
				want = 3
			}
			if got := stCounter(t, o.g, "microgateway_ratelimit_store_fallback_total", "route", "rl", "policy", p); got != want {
				t.Fatalf("fallback total policy %s = %v, want %v", p, got, want)
			}
		}
	}

	spyCalls := len(spy.Calls())
	originHits := hits.Load()
	cacheResp := purgeGet(t, gA, "http://h/c", nil)
	if cacheResp.StatusCode != http.StatusOK {
		t.Fatalf("A /c status = %d, want 200", cacheResp.StatusCode)
	}
	if st := purgeLast(t, cacheResp); st.Fwd != httpcache.FwdBypass {
		t.Fatalf("A /c %+v, want bypass", st)
	}
	if got := hits.Load() - originHits; got != 1 {
		t.Fatalf("origin requests for A /c = %d, want 1", got)
	}
	if got := len(spy.Calls()); got != spyCalls {
		t.Fatalf("spy calls for A /c = %d, want none", got-spyCalls)
	}

	spy.SetMode("", teststore.Inherit, nil)

	fA.Advance(30*time.Second - time.Nanosecond)
	takesA = spy.CountHandle(idA, "")
	if rec := stDo(t, gA, "/rl", smClient); rec.Code != http.StatusOK {
		t.Fatalf("A status before open timeout = %d, want 200", rec.Code)
	}
	if got := spy.CountHandle(idA, ""); got != takesA {
		t.Fatalf("A store calls before open timeout = %d, want none", got-takesA)
	}
	fA.Advance(time.Nanosecond)
	rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "1")
	if got := spy.CountHandle(idA, "") - takesA; got != 1 {
		t.Fatalf("A store calls at probe = %d, want 1", got)
	}
	expectAvail("A after probe", gA, 1)
	expectAvail("B before probe", gB, 0)
	expectAvail("C before probe", gC, 0)

	takesB = spy.CountHandle(idB, "")
	spExpectDegraded(t, stDo(t, gB, "/rl", smClient))
	if got := spy.CountHandle(idB, ""); got != takesB {
		t.Fatalf("B store calls before open timeout = %d, want none", got-takesB)
	}
	takesC = spy.CountHandle(idC, "")
	spExpectUnavailable(t, stDo(t, gC, "/rl", smClient), "30")
	if got := spy.CountHandle(idC, ""); got != takesC {
		t.Fatalf("C store calls before open timeout = %d, want none", got-takesC)
	}

	fB.Advance(30*time.Second + time.Nanosecond)
	rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "0")
	if got := spy.CountHandle(idB, "") - takesB; got != 1 {
		t.Fatalf("B store calls at probe = %d, want 1", got)
	}
	expectAvail("B after probe", gB, 1)
	rlExpectLimited(t, stDo(t, gB, "/rl", smClient), "3600")
	spExpectUnavailable(t, stDo(t, gC, "/rl", smClient), "30")
	if got := spy.CountHandle(idC, ""); got != takesC {
		t.Fatalf("C store calls after B probe = %d, want none", got-takesC)
	}
}

func TestInstancesMixedLoad(t *testing.T) {
	if os.Getenv("MICROGATEWAY_LOAD") == "" {
		t.Skip("load check: set MICROGATEWAY_LOAD=1 to run")
	}
	const (
		instances = 10
		perLoop   = 10000
		burst     = 500
		seed      = 19
	)
	pre := snapGoroutines()
	n := newMemNetwork()
	storeClk := rlFake()
	shared := stShared(t, storeClk)
	sc := teststore.NewSharedCache(storeClk, 1<<20)
	t.Cleanup(func() { _ = sc.Close() })
	spy := teststore.NewSpy()
	opt := miSpyOption(spy, store.CapRateLimit|store.CapCache, shared, sc, nil)

	var version, floor atomic.Int64
	be := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		fmt.Fprint(w, version.Load())
	}))
	top := "stores:\n" + stStore("s", "") + "cache:\n  store: s\n"
	routes := rlRoute("rl", fmt.Sprintf("    rate_limit:\n      rate: 1\n      per: 1h\n      burst: %d\n      store: s\n", burst)) +
		rlRoute("c", "    cache:\n      enabled: true\n")
	gs, _ := miInstances(t, n, instances, cacheGatewayDoc(be.URL, top, routes), time.Unix(rlStart, 0), time.Hour, opt)
	base := snapGoroutines()

	raise := func(v int64) {
		for {
			cur := floor.Load()
			if v <= cur || floor.CompareAndSwap(cur, v) {
				return
			}
		}
	}
	var admitted, stale atomic.Int64
	var wg sync.WaitGroup
	for i, g := range gs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(i)))
			for range perLoop {
				switch roll := rng.IntN(100); {
				case roll < 70:
					rec := stDo(t, g, "/rl", smClient)
					switch rec.Code {
					case http.StatusOK:
						admitted.Add(1)
					case http.StatusTooManyRequests:
					default:
						t.Errorf("/rl status = %d, want 200 or 429", rec.Code)
					}
				case roll < 95:
					seen := floor.Load()
					resp := purgeGet(t, g, "http://h/c", nil)
					body, err := io.ReadAll(resp.Body)
					if err != nil {
						t.Error(err)
						break
					}
					ver, err := strconv.ParseInt(string(body), 10, 64)
					if err != nil {
						t.Error(err)
						break
					}
					if ver < seen {
						stale.Add(1)
					}
				default:
					v := version.Add(1)
					if _, err := gs[rng.IntN(instances)].Purge(context.Background(), microgateway.PurgeRequest{All: true}); err != nil {
						t.Error(err)
					}
					raise(v)
				}
			}
		}()
	}
	wg.Wait()

	if got := admitted.Load(); got != burst {
		t.Fatalf("admitted /rl requests = %d, want %d", got, burst)
	}
	if got := stale.Load(); got != 0 {
		t.Fatalf("/c responses older than a completed purge = %d, want 0", got)
	}
	for _, g := range gs {
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
	}
	awaitGoroutinesSettled(t, "mixed load run", pre, base)
}

func TestStoreClosedAfterInFlightDrain(t *testing.T) {
	const (
		client = "192.0.2.50:1000"
		fresh  = "192.0.2.51:1000"
	)
	routes := stRoute("rl", srlRateLimit("s1")) + stRoute("other", "")
	cases := []struct {
		name   string
		reload func(backend string) string
	}{
		{"rename", func(b string) string {
			return srlDoc(b, "", srlStore("s2", 2, "v0"), "", stRoute("rl", srlRateLimit("s2"))+stRoute("other", ""))
		}},
		{"unreference", func(b string) string {
			return srlDoc(b, "", srlStore("s1", 2, "v0"), "", stRoute("rl", srlRateLimit(""))+stRoute("other", ""))
		}},
		{"remove", func(b string) string {
			return srlDoc(b, "", "", "", stRoute("rl", srlRateLimit(""))+stRoute("other", ""))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := newMemNetwork()
			fake := rlFake()
			be, _ := rlBackend(t, n)
			shared := stShared(t, fake)
			var calls atomic.Int64
			g := n.gateway(t, srlDoc(be.URL, "", srlStore("s1", 2, "v0"), "", routes), withClock(fake), srlType(shared, &calls))

			h := shared.Handles()[0]
			h.SetMode(teststore.Hang, nil)
			ch := stAsync(t, g, "/rl", client)
			h.WaitBlocked(1)

			if err := g.ReloadYAML([]byte(tc.reload(be.URL))); err != nil {
				t.Fatal(err)
			}
			if got := h.Closes(); got != 0 {
				t.Fatalf("handle closed %d times while its request is in flight, want 0", got)
			}
			if _, ok := srlSample(t, g, `microgateway_store_available{store="s1"}`); !ok {
				t.Fatal(`microgateway_store_available{store="s1"} is absent while its request is in flight, want present`)
			}
			takes := h.Takes()
			if rec := stDo(t, g, "/rl", fresh); rec.Code != http.StatusOK {
				t.Fatalf("request on the new generation status = %d, want 200", rec.Code)
			}
			if got := h.Takes(); got != takes {
				t.Fatalf("old handle takes = %d after a request on the new generation, want %d", got, takes)
			}

			h.SetMode(teststore.Healthy, nil)
			h.Release()
			if rec := <-ch; rec.Code != http.StatusOK {
				t.Fatalf("in-flight request status = %d, want 200", rec.Code)
			}
			if got := h.Closes(); got != 1 {
				t.Fatalf("handle closed %d times after drain, want 1", got)
			}
			srlExpectAbsent(t, g, `microgateway_store_available{store="s1"}`)

			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			if got := h.Closes(); got != 1 {
				t.Fatalf("handle closed %d times after Close, want 1", got)
			}
			for i, other := range shared.Handles() {
				if got := other.Closes(); got != 1 {
					t.Fatalf("handle %d closed %d times after Close, want 1", i, got)
				}
			}
		})
	}
}
