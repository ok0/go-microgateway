package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

const cacheDocHead = `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
stores:
  - name: shared
    type: custom
    params: {}
routes:
  - name: users-api
    upstream: users
`

func indentBlock(s, pad string) string {
	if s == "" {
		return ""
	}
	return pad + strings.ReplaceAll(strings.TrimSuffix(s, "\n"), "\n", "\n"+pad) + "\n"
}

func cacheDoc(global, route string) string {
	return cacheDocHead + indentBlock(route, "    ") + global
}

func cacheLine(t *testing.T, doc, substr string) int {
	t.Helper()
	n := lineOf(doc, substr)
	if n == 0 {
		t.Fatalf("substring %q not found in document", substr)
	}
	return n
}

func goBuilt(t *testing.T, doc string) *Config {
	t.Helper()
	var c Config
	if err := yaml.Unmarshal([]byte(doc), &c); err != nil {
		t.Fatal(err)
	}
	return &c
}

type cacheWant struct {
	path, msg, at string
}

type cacheRow struct {
	name    string
	global  string
	route   string
	extra   string
	unknown bool
	want    []cacheWant
}

func cacheErrSet(t *testing.T, err error) []string {
	t.Helper()
	if err == nil {
		return nil
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("want *ValidationError, got %T: %v", err, err)
	}
	out := make([]string, 0, len(ve.Errors))
	for _, fe := range ve.Errors {
		out = append(out, fmt.Sprintf("%s|%d|%s", fe.Path, fe.Line, fe.Msg))
	}
	slices.Sort(out)
	return out
}

func cacheWantSet(t *testing.T, doc string, want []cacheWant) []string {
	t.Helper()
	out := make([]string, 0, len(want))
	for _, w := range want {
		out = append(out, fmt.Sprintf("%s|%d|%s", w.path, cacheLine(t, doc, w.at), w.msg))
	}
	slices.Sort(out)
	return out
}

func cacheRows() []cacheRow {
	longName := strings.Repeat("a", 129)
	return []cacheRow{
		{name: "name empty", global: "cache:\n  name: \"\"\n", want: []cacheWant{
			{"cache.name", "must be 1-128 printable ASCII characters", `name: ""`},
		}},
		{name: "name 129 bytes", global: "cache:\n  name: " + longName + "\n", want: []cacheWant{
			{"cache.name", "must be 1-128 printable ASCII characters", longName},
		}},
		{name: "name non-ASCII", global: "cache:\n  name: \"caché\"\n", want: []cacheWant{
			{"cache.name", "must be 1-128 printable ASCII characters", "caché"},
		}},
		{name: "store missing", global: "cache:\n  store: missing\n", want: []cacheWant{
			{"cache.store", `store "missing" is not defined in stores`, "store: missing"},
		}},
		{name: "memory max_bytes zero", global: "cache:\n  memory:\n    max_bytes: 0\n", want: []cacheWant{
			{"cache.memory.max_bytes", "must be >= 1", "max_bytes: 0"},
		}},
		{name: "memory shards zero", global: "cache:\n  memory:\n    shards: 0\n", want: []cacheWant{
			{"cache.memory.shards", "must be between 1 and 1024", "shards: 0"},
		}},
		{name: "memory shards too large", global: "cache:\n  memory:\n    shards: 1025\n", want: []cacheWant{
			{"cache.memory.shards", "must be between 1 and 1024", "shards: 1025"},
		}},
		{name: "max_body_bytes zero", global: "cache:\n  max_body_bytes: 0\n", want: []cacheWant{
			{"cache.max_body_bytes", "must be between 1 and 67108864", "max_body_bytes: 0"},
		}},
		{name: "max_body_bytes too large", global: "cache:\n  max_body_bytes: 67108865\n", want: []cacheWant{
			{"cache.max_body_bytes", "must be between 1 and 67108864", "max_body_bytes: 67108865"},
		}},
		{name: "store_timeout zero", global: "cache:\n  store_timeout: 0s\n", want: []cacheWant{
			{"cache.store_timeout", "must be positive", "store_timeout: 0s"},
		}},
		{name: "tag_header empty", global: "cache:\n  tag_header: \"\"\n", want: []cacheWant{
			{"cache.tag_header", "must be a valid header field name", `tag_header: ""`},
		}},
		{name: "tag_header with space", global: "cache:\n  tag_header: \"bad header\"\n", want: []cacheWant{
			{"cache.tag_header", "must be a valid header field name", `tag_header: "bad header"`},
		}},
		{name: "tag_delimiter tab", global: "cache:\n  tag_delimiter: tab\n", want: []cacheWant{
			{"cache.tag_delimiter", "must be one of space, comma", "tag_delimiter: tab"},
		}},
		{name: "coalescing timeout zero", global: "cache:\n  coalescing:\n    timeout: 0s\n", want: []cacheWant{
			{"cache.coalescing.timeout", "must be positive", "timeout: 0s"},
		}},
		{name: "coalescing max_keys zero", global: "cache:\n  coalescing:\n    max_keys: 0\n", want: []cacheWant{
			{"cache.coalescing.max_keys", "must be >= 1", "max_keys: 0"},
		}},
		{name: "coalescing max_waiters zero", global: "cache:\n  coalescing:\n    max_waiters: 0\n", want: []cacheWant{
			{"cache.coalescing.max_waiters", "must be >= 1", "max_waiters: 0"},
		}},
		{name: "max_background zero", global: "cache:\n  max_background: 0\n", want: []cacheWant{
			{"cache.max_background", "must be >= 1", "max_background: 0"},
		}},
		{name: "memory ratio without store", global: "cache:\n  memory:\n    max_bytes: 1048576\n    shards: 16\n", want: []cacheWant{
			{"cache.memory.max_bytes", "divided by memory.shards must be at least max_body_bytes (1048576)", "max_bytes: 1048576"},
		}},
		{name: "memory ratio with store", global: "cache:\n  store: shared\n  memory:\n    max_bytes: 1048576\n    shards: 16\n"},
		{name: "route default_ttl negative", route: "cache:\n  enabled: true\n  default_ttl: -1s\n", want: []cacheWant{
			{"routes[0].cache.default_ttl", "must not be negative", "default_ttl: -1s"},
		}},
		{name: "route max_ttl zero", route: "cache:\n  enabled: true\n  max_ttl: 0s\n", want: []cacheWant{
			{"routes[0].cache.max_ttl", "must be positive", "max_ttl: 0s"},
		}},
		{name: "route keep too long", route: "cache:\n  enabled: true\n  keep: 8761h\n", want: []cacheWant{
			{"routes[0].cache.keep", "must not exceed 365 days", "keep: 8761h"},
		}},
		{name: "route stale_while_revalidate negative", route: "cache:\n  enabled: true\n  stale_while_revalidate: -1s\n", want: []cacheWant{
			{"routes[0].cache.stale_while_revalidate", "must not be negative", "stale_while_revalidate: -1s"},
		}},
		{name: "route stale_if_error too long", route: "cache:\n  enabled: true\n  stale_if_error: 8761h\n", want: []cacheWant{
			{"routes[0].cache.stale_if_error", "must not exceed 365 days", "stale_if_error: 8761h"},
		}},
		{name: "route default_ttl above max_ttl", route: "cache:\n  enabled: true\n  default_ttl: 2h\n  max_ttl: 1h\n", want: []cacheWant{
			{"routes[0].cache.default_ttl", "must not exceed max_ttl", "default_ttl: 2h"},
		}},
		{name: "route statuses empty", route: "cache:\n  enabled: true\n  statuses: []\n", want: []cacheWant{
			{"routes[0].cache.statuses", "must not be empty", "statuses: []"},
		}},
		{name: "route statuses invalid and duplicate", route: "cache:\n  enabled: true\n  statuses: [200, 304, 199, 600, 206, 200]\n", want: []cacheWant{
			{"routes[0].cache.statuses[1]", "must be a final status from 200 to 599 other than 206 and 304", "statuses: ["},
			{"routes[0].cache.statuses[2]", "must be a final status from 200 to 599 other than 206 and 304", "statuses: ["},
			{"routes[0].cache.statuses[3]", "must be a final status from 200 to 599 other than 206 and 304", "statuses: ["},
			{"routes[0].cache.statuses[4]", "must be a final status from 200 to 599 other than 206 and 304", "statuses: ["},
			{"routes[0].cache.statuses[5]", "duplicate status 200", "statuses: ["},
		}},
		{name: "disabled route still reports", route: "cache:\n  enabled: false\n  statuses: [304]\n", want: []cacheWant{
			{"routes[0].cache.statuses[0]", "must be a final status from 200 to 599 other than 206 and 304", "statuses: [304]"},
		}},
		{name: "second route statuses", global: "", extra: "  - name: second\n    match:\n      path: /second\n    upstream: users\n    cache:\n      enabled: true\n      statuses: [200, 203, 304]\n", want: []cacheWant{
			{"routes[1].cache.statuses[2]", "must be a final status from 200 to 599 other than 206 and 304", "statuses: [200, 203, 304]"},
		}},
		{name: "pipeline cache after circuit_breaker", route: "pipeline:\n  - circuit_breaker\n  - cache\n", want: []cacheWant{
			{"routes[0].pipeline", `"cache" must come before "circuit_breaker" and "retry"`, "pipeline:"},
		}},
		{name: "pipeline cache after retry", route: "cache:\n  enabled: true\npipeline:\n  - retry\n  - cache\n", want: []cacheWant{
			{"routes[0].pipeline[0]", `"retry" must be the last stage`, "- retry"},
			{"routes[0].pipeline", `"cache" must come before "circuit_breaker" and "retry"`, "pipeline:"},
		}},
		{name: "pipeline cache first", route: "cache:\n  enabled: true\npipeline:\n  - cache\n  - circuit_breaker\n  - retry\n"},
		{name: "pipeline cache on route without cache block", route: "pipeline:\n  - cache\n"},
		{name: "pipeline cache before auth", route: "pipeline:\n  - cache\n  - auth\n", want: []cacheWant{
			{"routes[0].pipeline[0]", msgCacheBeforeAuth, "- cache"},
		}},
		{name: "pipeline cache between stages before auth", route: "pipeline:\n  - timeout\n  - cache\n  - auth\n", want: []cacheWant{
			{"routes[0].pipeline[1]", msgCacheBeforeAuth, "- cache"},
		}},
		{name: "pipeline stages between cache and auth", route: "pipeline:\n  - cache\n  - rate_limit\n  - timeout\n  - auth\n", want: []cacheWant{
			{"routes[0].pipeline[0]", msgCacheBeforeAuth, "- cache"},
		}},
		{name: "pipeline cache after rate_limit before auth", route: "pipeline:\n  - rate_limit\n  - cache\n  - auth\n", want: []cacheWant{
			{"routes[0].pipeline[1]", msgCacheBeforeAuth, "- cache"},
		}},
		{name: "pipeline cache before auth with auth and cache blocks", route: "auth:\n  mode: none\ncache:\n  enabled: true\npipeline:\n  - cache\n  - auth\n", want: []cacheWant{
			{"routes[0].pipeline[0]", msgCacheBeforeAuth, "- cache"},
		}},
		{name: "pipeline cache before auth and after circuit_breaker", route: "pipeline:\n  - circuit_breaker\n  - cache\n  - auth\n", want: []cacheWant{
			{"routes[0].pipeline", `"cache" must come before "circuit_breaker" and "retry"`, "pipeline:"},
			{"routes[0].pipeline[1]", msgCacheBeforeAuth, "- cache"},
		}},
		{name: "pipeline duplicate cache before auth", route: "pipeline: [cache, cache, auth]\n", want: []cacheWant{
			{"routes[0].pipeline[0]", msgCacheBeforeAuth, "pipeline: ["},
			{"routes[0].pipeline[1]", `duplicate stage "cache"`, "pipeline: ["},
		}},
		{name: "pipeline cache before auth with another error", global: "cache:\n  store_timeout: 0s\n", route: "pipeline:\n  - cache\n  - auth\n", want: []cacheWant{
			{"cache.store_timeout", "must be positive", "store_timeout: 0s"},
			{"routes[0].pipeline[0]", msgCacheBeforeAuth, "- cache"},
		}},
		{name: "pipeline cache before auth on two routes", route: "pipeline:\n  - cache\n  - auth\n", extra: "  - name: second\n    match:\n      path: /second\n    upstream: users\n    pipeline: [cache, auth]\n", want: []cacheWant{
			{"routes[0].pipeline[0]", msgCacheBeforeAuth, "- cache"},
			{"routes[1].pipeline[0]", msgCacheBeforeAuth, "pipeline: [cache, auth]"},
		}},
		{name: "pipeline auth before cache", route: "pipeline:\n  - auth\n  - cache\n"},
		{name: "pipeline cache before rate_limit", route: "pipeline:\n  - cache\n  - rate_limit\n"},
		{name: "pipeline cache before rate_limit after auth", route: "pipeline:\n  - auth\n  - cache\n  - rate_limit\n"},
		{name: "pipeline auth without cache", route: "pipeline:\n  - timeout\n  - auth\n"},
		{name: "pipeline default order", route: "auth:\n  mode: none\ncache:\n  enabled: true\npipeline:\n  - timeout\n  - auth\n  - rate_limit\n  - cache\n  - circuit_breaker\n  - retry\n"},
		{name: "pipeline user middleware and timeout around cache", global: "middlewares:\n  - name: mw1\n    type: custom\n", route: "middlewares: [mw1]\npipeline:\n  - mw1\n  - auth\n  - cache\n  - timeout\n"},
		{name: "cache and auth blocks without pipeline", route: "auth:\n  mode: none\ncache:\n  enabled: true\n"},
		{name: "unknown global key", global: "cache:\n  bogus: 1\n", unknown: true, want: []cacheWant{
			{"cache.bogus", `unknown field "bogus"`, "bogus: 1"},
		}},
		{name: "unknown route key", route: "cache:\n  enabled: true\n  bogus: 1\n", unknown: true, want: []cacheWant{
			{"routes[0].cache.bogus", `unknown field "bogus"`, "bogus: 1"},
		}},
		{name: "middleware named cache", global: "middlewares:\n  - name: cache\n    type: custom\n", want: []cacheWant{
			{"middlewares[0].name", `"cache" is reserved for a built-in stage`, "- name: cache"},
		}},
	}
}

func TestCacheDefaults(t *testing.T) {
	t.Parallel()
	c, err := Parse([]byte(cacheDoc("", "cache:\n  enabled: true\n")))
	if err != nil {
		t.Fatal(err)
	}
	ca := c.Cache
	if ca.Name == nil || *ca.Name != "microgateway" {
		t.Errorf("name = %v, want microgateway", ca.Name)
	}
	if ca.Store != "" {
		t.Errorf("store = %q, want empty", ca.Store)
	}
	if ca.Memory.MaxBytes == nil || *ca.Memory.MaxBytes != 67108864 {
		t.Errorf("memory.max_bytes = %v, want 67108864", ca.Memory.MaxBytes)
	}
	if ca.Memory.Shards == nil || *ca.Memory.Shards != 16 {
		t.Errorf("memory.shards = %v, want 16", ca.Memory.Shards)
	}
	if ca.MaxBodyBytes == nil || *ca.MaxBodyBytes != 1048576 {
		t.Errorf("max_body_bytes = %v, want 1048576", ca.MaxBodyBytes)
	}
	if ca.StoreTimeout == nil || ca.StoreTimeout.Std() != 100*time.Millisecond {
		t.Errorf("store_timeout = %v, want 100ms", ca.StoreTimeout)
	}
	if ca.TagHeader == nil || *ca.TagHeader != "Surrogate-Key" {
		t.Errorf("tag_header = %v, want Surrogate-Key", ca.TagHeader)
	}
	if ca.TagDelimiter != "space" {
		t.Errorf("tag_delimiter = %q, want space", ca.TagDelimiter)
	}
	if ca.StripTagHeader == nil || !*ca.StripTagHeader {
		t.Errorf("strip_tag_header = %v, want true", ca.StripTagHeader)
	}
	if ca.StatusKey {
		t.Errorf("status_key = true, want false")
	}
	if ca.Coalescing.Enabled == nil || !*ca.Coalescing.Enabled {
		t.Errorf("coalescing.enabled = %v, want true", ca.Coalescing.Enabled)
	}
	if ca.Coalescing.Timeout == nil || ca.Coalescing.Timeout.Std() != 5*time.Second {
		t.Errorf("coalescing.timeout = %v, want 5s", ca.Coalescing.Timeout)
	}
	if ca.Coalescing.MaxKeys == nil || *ca.Coalescing.MaxKeys != 10000 {
		t.Errorf("coalescing.max_keys = %v, want 10000", ca.Coalescing.MaxKeys)
	}
	if ca.Coalescing.MaxWaiters == nil || *ca.Coalescing.MaxWaiters != 1024 {
		t.Errorf("coalescing.max_waiters = %v, want 1024", ca.Coalescing.MaxWaiters)
	}
	if ca.MaxBackground == nil || *ca.MaxBackground != 64 {
		t.Errorf("max_background = %v, want 64", ca.MaxBackground)
	}

	rc := c.Routes[0].Cache
	if rc == nil {
		t.Fatal("routes[0].cache is nil")
	}
	if rc.MaxTTL == nil || rc.MaxTTL.Std() != 24*time.Hour {
		t.Errorf("max_ttl = %v, want 24h", rc.MaxTTL)
	}
	if rc.DefaultTTL != 0 || rc.Keep != 0 || rc.StaleWhileRevalidate != 0 || rc.StaleIfError != 0 {
		t.Errorf("durations = %v %v %v %v, want all 0", rc.DefaultTTL, rc.Keep, rc.StaleWhileRevalidate, rc.StaleIfError)
	}
	wantStatuses := []int{200, 203, 204, 300, 301, 308, 404, 405, 410, 414, 501}
	if !slices.Equal(rc.Statuses, wantStatuses) {
		t.Errorf("statuses = %v, want %v", rc.Statuses, wantStatuses)
	}
}

func TestCacheDefaultsAbsentAndEmpty(t *testing.T) {
	t.Parallel()
	c, err := Parse([]byte(cacheDoc("", "")))
	if err != nil {
		t.Fatal(err)
	}
	if c.Routes[0].Cache != nil {
		t.Errorf("route without cache has Cache = %+v, want nil", c.Routes[0].Cache)
	}
	empty, err := Parse([]byte(cacheDoc("cache: {}\n", "")))
	if err != nil {
		t.Fatal(err)
	}
	omitted, err := Parse([]byte(cacheDoc("", "")))
	if err != nil {
		t.Fatal(err)
	}
	if empty.Fingerprint() != omitted.Fingerprint() {
		t.Errorf("cache: {} differs from omitted block")
	}
}

func TestCacheValidation(t *testing.T) {
	t.Parallel()
	for _, row := range cacheRows() {
		if row.unknown {
			continue
		}
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			doc := cacheDoc(row.global, row.route) + row.extra
			_, err := Parse([]byte(doc))
			got := cacheErrSet(t, err)
			want := cacheWantSet(t, doc, row.want)
			if !slices.Equal(got, want) {
				t.Errorf("errors = %q\n want %q", got, want)
			}
		})
	}
}

func TestCacheValidationUnknownKeys(t *testing.T) {
	t.Parallel()
	for _, row := range cacheRows() {
		if !row.unknown {
			continue
		}
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			doc := cacheDoc(row.global, row.route) + row.extra
			_, err := Parse([]byte(doc))
			got := cacheErrSet(t, err)
			want := cacheWantSet(t, doc, row.want)
			if !slices.Equal(got, want) {
				t.Errorf("errors = %q\n want %q", got, want)
			}
		})
	}
}

func TestCacheValidationGoBuilt(t *testing.T) {
	t.Parallel()
	for _, row := range cacheRows() {
		if row.unknown {
			continue
		}
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			doc := cacheDoc(row.global, row.route) + row.extra
			err := goBuilt(t, doc).Validate()
			got := cacheErrSet(t, err)
			want := make([]string, 0, len(row.want))
			for _, w := range row.want {
				want = append(want, fmt.Sprintf("%s|0|%s", w.path, w.msg))
			}
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Errorf("errors = %q\n want %q", got, want)
			}
		})
	}
}

func goCacheConfig(cache Cache, rc *RouteCache) *Config {
	return &Config{
		Version:   1,
		Listeners: []Listener{{Name: "public", Address: ":8080"}},
		Upstreams: []Upstream{{Name: "users", Hosts: []Host{{URL: NewSecretURL("http://127.0.0.1:9001")}}}},
		Routes:    []Route{{Name: "users-api", Upstream: "users", Cache: rc}},
		Cache:     cache,
	}
}

func TestCacheValidationLiteralGoConfigs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  *Config
		want []string
	}{
		{
			name: "memory shards",
			cfg:  goCacheConfig(Cache{Memory: CacheMemory{Shards: intPtr(0)}}, nil),
			want: []string{"cache.memory.shards|0|must be between 1 and 1024"},
		},
		{
			name: "route statuses",
			cfg:  goCacheConfig(Cache{}, &RouteCache{Statuses: []int{304}}),
			want: []string{"routes[0].cache.statuses[0]|0|must be a final status from 200 to 599 other than 206 and 304"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := cacheErrSet(t, tc.cfg.Validate())
			want := slices.Clone(tc.want)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Errorf("errors = %q\n want %q", got, want)
			}
		})
	}
}

func TestMaxStorageTTL(t *testing.T) {
	t.Parallel()
	dur := func(d time.Duration) *Duration {
		v := Duration(d)
		return &v
	}
	tests := []struct {
		name   string
		c      *Cache
		routes []Route
		want   time.Duration
	}{
		{
			name:   "none enabled",
			routes: []Route{{Cache: nil}, {Cache: &RouteCache{Enabled: false, MaxTTL: dur(time.Hour)}}},
			want:   0,
		},
		{
			name:   "one route",
			routes: []Route{{Cache: &RouteCache{Enabled: true, MaxTTL: dur(time.Hour), StaleWhileRevalidate: Duration(30 * time.Second), StaleIfError: Duration(5 * time.Minute), Keep: Duration(10 * time.Minute)}}},
			want:   time.Hour + 15*time.Minute,
		},
		{
			name: "several routes take the maximum",
			routes: []Route{
				{Cache: &RouteCache{Enabled: true, MaxTTL: dur(time.Hour), Keep: Duration(10 * time.Minute)}},
				{Cache: &RouteCache{Enabled: true, MaxTTL: dur(2 * time.Hour), StaleWhileRevalidate: Duration(time.Minute)}},
			},
			want: 2*time.Hour + time.Minute,
		},
		{
			name: "disabled route ignored",
			routes: []Route{
				{Cache: &RouteCache{Enabled: true, MaxTTL: dur(time.Hour)}},
				{Cache: &RouteCache{Enabled: false, MaxTTL: dur(10 * time.Hour)}},
			},
			want: time.Hour,
		},
		{
			name:   "nil max_ttl counts as 24h",
			routes: []Route{{Cache: &RouteCache{Enabled: true}}},
			want:   24 * time.Hour,
		},
		{
			name:   "negative components count as zero",
			routes: []Route{{Cache: &RouteCache{Enabled: true, MaxTTL: dur(time.Hour), Keep: Duration(-time.Hour), StaleIfError: Duration(-time.Minute)}}},
			want:   time.Hour,
		},
		{
			name:   "saturates",
			routes: []Route{{Cache: &RouteCache{Enabled: true, MaxTTL: dur(math.MaxInt64), Keep: Duration(time.Second)}}},
			want:   math.MaxInt64,
		},
		{
			name:   "nil receiver",
			c:      nil,
			routes: []Route{{Cache: &RouteCache{Enabled: true, MaxTTL: dur(time.Hour)}}},
			want:   time.Hour,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.c.MaxStorageTTL(tc.routes); got != tc.want {
				t.Errorf("MaxStorageTTL = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCacheCloneFingerprintJSON(t *testing.T) {
	t.Parallel()
	c, err := Parse([]byte(cacheDoc("", "cache:\n  enabled: true\n")))
	if err != nil {
		t.Fatal(err)
	}
	before := c.Fingerprint()

	cl := c.Clone()
	cl.Routes[0].Cache.Statuses[0] = 500
	*cl.Cache.Name = "changed"
	*cl.Cache.Memory.Shards = 2
	if c.Routes[0].Cache.Statuses[0] != 200 || *c.Cache.Name != "microgateway" || *c.Cache.Memory.Shards != 16 {
		t.Fatal("mutating the clone changed the original")
	}
	if c.Fingerprint() != before {
		t.Fatal("fingerprint of the original changed after mutating the clone")
	}

	for _, mutate := range []struct {
		name string
		fn   func(*Config)
	}{
		{"cache.name", func(x *Config) { *x.Cache.Name = "other" }},
		{"cache.memory.shards", func(x *Config) { *x.Cache.Memory.Shards = 8 }},
		{"routes[0].cache.max_ttl", func(x *Config) { *x.Routes[0].Cache.MaxTTL = Duration(2 * time.Hour) }},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			t.Parallel()
			x, err := Parse([]byte(cacheDoc("", "cache:\n  enabled: true\n")))
			if err != nil {
				t.Fatal(err)
			}
			mutate.fn(x)
			if x.Fingerprint() == before {
				t.Errorf("fingerprint unchanged after changing %s", mutate.name)
			}
		})
	}

	b, err := json.Marshal(c.Redacted())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"cache":{"name":"microgateway"`,
		`"statuses":[200,203,204,300,301,308,404,405,410,414,501]`,
	} {
		if !strings.Contains(string(b), want) {
			t.Errorf("redacted JSON lacks %s", want)
		}
	}
}
