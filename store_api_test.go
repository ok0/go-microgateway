package lychgate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/store"
)

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

func sapiNew(t *testing.T, doc string, opts ...Option) (*Gateway, error) {
	t.Helper()
	cfg, err := config.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(cfg, opts...)
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
	opts := []Option{
		withClock(fake),
		WithStoreType("failf", store.CapRateLimit, func(StoreParams) (store.Backend, error) {
			factoryCalls.Add(1)
			return nil, errors.New("connect refused")
		}),
		WithStoreType("nilb", store.CapRateLimit, func(StoreParams) (store.Backend, error) {
			factoryCalls.Add(1)
			return nil, nil
		}),
		WithStoreType("boom", store.CapRateLimit, func(StoreParams) (store.Backend, error) {
			factoryCalls.Add(1)
			panic("boom")
		}),
		WithStoreType("plainb", store.CapRateLimit, func(StoreParams) (store.Backend, error) {
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
	err = Validate(cfg, opts...)
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
	_, err = New(missingCfg, opts...)
	if err == nil {
		t.Fatal("New() = nil error, want resolution errors")
	}
	sapiWant(t, sapiValidationErrors(t, err), missingWant)

	err = Validate(missingCfg, opts...)
	if err == nil {
		t.Fatal("Validate() = nil error, want resolution errors")
	}
	sapiWant(t, sapiValidationErrors(t, err), missingWant)
	if got := factoryCalls.Load(); got != 4 {
		t.Fatalf("factory calls after second Validate = %d, want 4", got)
	}

	g := n.gateway(t, stDoc(be.URL, "", stRoute("plain", "")), opts...)
	gfp := g.Config().Fingerprint()
	if got := stCounter(t, g, "lychgate_config_reloads_total", "result", "failure"); got != 0 {
		t.Fatalf("failure reloads before Reload = %v, want 0", got)
	}
	rerr := g.Reload(mustParseSapi(t, buildDoc))
	if rerr == nil {
		t.Fatal("Reload() = nil error, want resolution errors")
	}
	sapiWant(t, sapiValidationErrors(t, rerr), buildWant)
	if got := stCounter(t, g, "lychgate_config_reloads_total", "result", "failure"); got != 1 {
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
	f := func(StoreParams) (store.Backend, error) { return sapiCloseOnly{}, nil }
	const doc = "version: 1\n" + listenersYAML + "upstreams:\n  - name: up\n    hosts:\n      - url: \"http://127.0.0.1:1\"\nroutes:\n  - name: plain\n    match:\n      path: /plain\n    upstream: up\n"
	cases := []struct {
		name string
		opts []Option
		want string
	}{
		{name: "empty name", opts: []Option{WithStoreType("", store.CapRateLimit, f)}, want: "empty type name"},
		{name: "nil factory", opts: []Option{WithStoreType("nilf", store.CapRateLimit, nil)}, want: `WithStoreType("nilf"): nil factory`},
		{name: "zero caps", opts: []Option{WithStoreType("zero", 0, f)}, want: `WithStoreType("zero"): capabilities must not be zero`},
		{name: "unknown caps", opts: []Option{WithStoreType("unk", store.CapRateLimit|1<<1, f)}, want: `WithStoreType("unk"): unknown capability bits`},
		{name: "duplicate", opts: []Option{WithStoreType("dup", store.CapRateLimit, f), WithStoreType("dup", store.CapRateLimit, f)}, want: `WithStoreType("dup"): type registered twice`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.Parse([]byte(doc))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := New(cfg, tc.opts...); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("New() error = %v, want it to contain %q", err, tc.want)
			}
			if _, err := NewFromYAML([]byte(doc), tc.opts...); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("NewFromYAML() error = %v, want it to contain %q", err, tc.want)
			}
			if err := Validate(cfg, tc.opts...); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate() error = %v, want it to contain %q", err, tc.want)
			}
		})
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
				opt := WithStoreType("custom", store.CapRateLimit, func(p StoreParams) (store.Backend, error) {
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
		opt := WithStoreType("redis", store.CapRateLimit, func(p StoreParams) (store.Backend, error) {
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
		var seen StoreParams
		var calls int
		opt := WithStoreType("custom", store.CapRateLimit, func(p StoreParams) (store.Backend, error) {
			calls++
			seen = p
			p.Logger.Info("sapi-probe")
			return shared.Handle(), nil
		})
		doc := stDoc(be.URL, "  - name: anything-goes\n    type: custom\n",
			stRoute("l", "    rate_limit:\n      rate: 5\n      per: 1s\n      store: anything-goes\n"))
		if _, err := sapiNew(t, doc, withClock(fake), WithLogger(slog.New(slog.NewJSONHandler(&logBuf, nil))), opt); err != nil {
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
	opt := WithStoreType("custom", store.CapRateLimit, func(p StoreParams) (store.Backend, error) {
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
	data, err := os.ReadFile("config/testdata/stores_invalid.yaml")
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewFromYAML(data)
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
	opts := []Option{
		withClock(fake),
		stOption(shared, &testCalls),
		WithStoreType("redis", store.CapRateLimit, func(StoreParams) (store.Backend, error) {
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
	err = Validate(cfg, opts...)
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

	_, err = New(cfg, opts...)
	sapiWantAt(t, cfg, err, want)
	if got := testCalls.Load() + redisCalls.Load(); got != 0 {
		t.Fatalf("store factory calls after New = %d, want 0", got)
	}

	g := n.gateway(t, stDoc(be.URL, stStore("ok", ""), stRoute("live", "")), opts...)
	rerr := g.Reload(cfg)
	sapiWantAt(t, cfg, rerr, want)
	if got := stCounter(t, g, "lychgate_config_reloads_total", "result", "failure"); got != 1 {
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
	paths, err := filepath.Glob("examples/*.yaml")
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
		if err := Validate(cfg); err != nil {
			t.Errorf("Validate(%s) = %v", path, err)
		}
	}
}
