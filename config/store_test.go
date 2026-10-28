package config

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

const storeTestBase = `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
`

const storeCacheRedis = "  - name: cache\n    type: redis\n    redis:\n      addresses: [\"127.0.0.1:6379\"]\n"

func storeDoc(stores, rateLimitExtra string) string {
	return storeTestBase + "stores:\n" + stores + "routes:\n  - name: all\n    upstream: users\n    rate_limit:\n      algorithm: token_bucket\n      rate: 10\n      store: cache\n" + rateLimitExtra
}

func TestStoresSeededErrors(t *testing.T) {
	_, err := LoadFile("testdata/stores_invalid.yaml")
	errs := fieldErrors(t, err)
	want := []struct {
		path, msg string
	}{
		{"stores[1].name", `duplicate name "dup"`},
		{"stores[2].name", "must match ^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$"},
		{"stores[3].type", "is required"},
		{"stores[4].redis", "is only valid with type redis"},
		{"stores[5].redis", "is required for type redis"},
		{"stores[6].params", "is not valid for type redis"},
		{"stores[7].redis.mode", "must be one of standalone, sentinel, cluster"},
		{"stores[8].redis.addresses", "standalone mode takes exactly one address"},
		{"stores[9].redis.addresses[1]", "duplicate address"},
		{"stores[10].redis.addresses[0]", "must be host:port without a scheme"},
		{"stores[11].redis.addresses[0]", "must not contain userinfo"},
		{"stores[12].redis.addresses[0]", "port must be between 1 and 65535"},
		{"stores[13].redis.master_name", "is required in sentinel mode"},
		{"stores[14].redis.master_name", "is only valid in sentinel mode"},
		{"stores[15].redis.db", "must be 0 in cluster mode"},
		{"stores[16].redis.tls", "cert_file and key_file must be set together"},
		{"stores[17].redis.tls.ca_file", "is only valid when tls.enabled is true"},
		{"stores[18].redis.dial_timeout", "must be positive"},
		{"stores[19].redis.pool_size", "must be >= 0"},
		{"stores[20].redis.min_idle_conns", "must not exceed pool_size"},
		{"stores[21].redis.key_prefix", "must not contain { or }"},
		{"stores[22].guard.consecutive_failures", "must be >= 1"},
		{"stores[23].guard.open_timeout", "must be positive"},
		{"stores[24].guard.max_inflight", "must be >= 1"},
		{"stores[25].guard.max_inflight", "cannot unmarshal !!float `1e3` into int"},
		{"stores[26].redis.db", "cannot unmarshal !!float `1.5` into int"},
		{"stores[27].redis.pool_size", "cannot unmarshal !!float `2.0` into int"},
		{"stores[28].redis.db", "cannot unmarshal !!float `9999999...` into int"},
		{"routes[0].rate_limit.store_timeout", "must be positive"},
		{"routes[0].rate_limit.on_store_error", "must be one of local, allow, deny"},
		{"routes[1].rate_limit.on_store_error", "is only valid with store"},
		{"routes[1].rate_limit.store_timeout", "is only valid with store"},
		{"routes[2].rate_limit.store", `store "nothing" is not defined in stores`},
	}
	if len(errs) != len(want) {
		t.Errorf("got %d errors, want %d: %v", len(errs), len(want), err)
	}
	for _, w := range want {
		var found *FieldError
		for i := range errs {
			if errs[i].Path == w.path {
				found = &errs[i]
				break
			}
		}
		if found == nil {
			t.Errorf("no error at %s; got %v", w.path, err)
			continue
		}
		if found.Line <= 0 {
			t.Errorf("%s has no line", w.path)
		}
		if !strings.Contains(found.Msg, w.msg) {
			t.Errorf("%s: message %q does not contain %q", w.path, found.Msg, w.msg)
		}
	}
	text := err.Error()
	for _, leak := range []string{"p4ss-UNIQUE-1", "redis://h:1", "u@h:1"} {
		if strings.Contains(text, leak) {
			t.Errorf("error text contains %q: %s", leak, text)
		}
	}
}

func TestStoreDefaults(t *testing.T) {
	c, err := Parse([]byte(storeDoc(storeCacheRedis, "")))
	if err != nil {
		t.Fatal(err)
	}
	s := c.Stores[0]
	if *s.Guard.ConsecutiveFailures != 3 || s.Guard.OpenTimeout.Std() != 2*time.Second || *s.Guard.MaxInflight != 1024 {
		t.Errorf("guard defaults = %+v", s.Guard)
	}
	r := s.Redis
	if r.Mode != "standalone" {
		t.Errorf("Mode = %q", r.Mode)
	}
	if r.DialTimeout.Std() != time.Second || r.ReadTimeout.Std() != 500*time.Millisecond || r.WriteTimeout.Std() != 500*time.Millisecond {
		t.Errorf("timeout defaults = %v %v %v", r.DialTimeout, r.ReadTimeout, r.WriteTimeout)
	}
	if *r.KeyPrefix != "microgateway" {
		t.Errorf("KeyPrefix = %q", *r.KeyPrefix)
	}
	rl := c.Routes[0].RateLimit
	if rl.OnStoreError != "local" || rl.StoreTimeout.Std() != 50*time.Millisecond {
		t.Errorf("rate limit store defaults = %q %v", rl.OnStoreError, rl.StoreTimeout)
	}

	explicit := `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
stores:
  - name: cache
    type: redis
    guard:
      consecutive_failures: 7
    redis:
      addresses: ["127.0.0.1:6379"]
      dial_timeout: 3s
      key_prefix: x
routes:
  - name: all
    upstream: users
    rate_limit:
      algorithm: token_bucket
      rate: 10
      store: cache
      on_store_error: deny
      store_timeout: 10ms
`
	c, err = Parse([]byte(explicit))
	if err != nil {
		t.Fatal(err)
	}
	if *c.Stores[0].Guard.ConsecutiveFailures != 7 {
		t.Errorf("explicit consecutive_failures not kept: %d", *c.Stores[0].Guard.ConsecutiveFailures)
	}
	if c.Stores[0].Redis.DialTimeout.Std() != 3*time.Second {
		t.Errorf("explicit dial_timeout not kept: %v", c.Stores[0].Redis.DialTimeout)
	}
	if *c.Stores[0].Redis.KeyPrefix != "x" {
		t.Errorf("explicit key_prefix not kept: %q", *c.Stores[0].Redis.KeyPrefix)
	}
	if c.Routes[0].RateLimit.OnStoreError != "deny" || c.Routes[0].RateLimit.StoreTimeout.Std() != 10*time.Millisecond {
		t.Errorf("explicit store options not kept: %q %v", c.Routes[0].RateLimit.OnStoreError, c.Routes[0].RateLimit.StoreTimeout)
	}
}

func TestStoreExplicitZeroNotDefaulted(t *testing.T) {
	cases := []struct {
		name, stores, rateLimit, path string
	}{
		{"consecutive_failures", storeCacheRedis + "    guard:\n      consecutive_failures: 0\n", "", "stores[0].guard.consecutive_failures"},
		{"open_timeout", storeCacheRedis + "    guard:\n      open_timeout: 0s\n", "", "stores[0].guard.open_timeout"},
		{"max_inflight", storeCacheRedis + "    guard:\n      max_inflight: 0\n", "", "stores[0].guard.max_inflight"},
		{"dial_timeout", storeCacheRedis + "      dial_timeout: 0s\n", "", "stores[0].redis.dial_timeout"},
		{"read_timeout", storeCacheRedis + "      read_timeout: 0s\n", "", "stores[0].redis.read_timeout"},
		{"write_timeout", storeCacheRedis + "      write_timeout: 0s\n", "", "stores[0].redis.write_timeout"},
		{"key_prefix", storeCacheRedis + "      key_prefix: \"\"\n", "", "stores[0].redis.key_prefix"},
		{"store_timeout", storeCacheRedis, "      store_timeout: 0s\n", "routes[0].rate_limit.store_timeout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(storeDoc(tc.stores, tc.rateLimit)))
			if !hasPath(fieldErrors(t, err), tc.path) {
				t.Errorf("explicit zero not reported at %s: %v", tc.path, err)
			}
		})
	}
}

func TestStoresAbsentValid(t *testing.T) {
	c, err := Parse([]byte(minimalYAML))
	if err != nil {
		t.Fatal(err)
	}
	if c.Stores != nil {
		t.Errorf("Stores = %+v, want nil", c.Stores)
	}
	if c.Routes[0].RateLimit != nil {
		t.Errorf("RateLimit = %+v, want nil", c.Routes[0].RateLimit)
	}
	withLimit := storeTestBase + "routes:\n  - name: all\n    upstream: users\n    rate_limit:\n      algorithm: token_bucket\n      rate: 10\n"
	c, err = Parse([]byte(withLimit))
	if err != nil {
		t.Fatal(err)
	}
	if rl := c.Routes[0].RateLimit; rl.Store != "" || rl.OnStoreError != "" || rl.StoreTimeout != nil {
		t.Errorf("store fields set without a store reference: %+v", rl)
	}
}

func TestStoreClone(t *testing.T) {
	c := parseFull(t)
	origAddr := c.Stores[0].Redis.Addresses[0]
	origToken := c.Stores[1].Params.Reveal()["nested"].(map[string]any)["token"]
	origMax := *c.Stores[1].Guard.MaxInflight

	cl := c.Clone()
	if !reflect.DeepEqual(c, cl) {
		t.Fatalf("Clone differs from the original")
	}
	cl.Stores[0].Redis.Addresses[0] = "changed:1"
	cloneParams := cl.Stores[1].Params.Reveal()
	cloneParams["nested"].(map[string]any)["token"] = "changed"
	*cl.Stores[1].Guard.MaxInflight = 1
	cl.Stores[0].Redis.Password = NewSecret("changed")

	if c.Stores[0].Redis.Addresses[0] != origAddr {
		t.Errorf("Addresses shared with clone")
	}
	if c.Stores[1].Params.Reveal()["nested"].(map[string]any)["token"] != origToken {
		t.Errorf("params shared with clone")
	}
	if *c.Stores[1].Guard.MaxInflight != origMax {
		t.Errorf("guard shared with clone")
	}
	if c.Stores[0].Redis.Password.Reveal() != "example-redis-password" {
		t.Errorf("password shared with clone")
	}

	rc := c.Stores[0].Redis.Clone()
	rc.Addresses[1] = "changed:2"
	rc.DB = 9
	*rc.DialTimeout = Duration(time.Hour)
	if c.Stores[0].Redis.Addresses[1] == "changed:2" || c.Stores[0].Redis.DB == 9 || c.Stores[0].Redis.DialTimeout.Std() == time.Hour {
		t.Errorf("RedisStore.Clone shares data with original")
	}
	var nilRedis *RedisStore
	if nilRedis.Clone() != nil {
		t.Errorf("Clone of nil RedisStore is not nil")
	}
}

func TestStoreFingerprint(t *testing.T) {
	base := parseFull(t)
	baseFP := base.Fingerprint()
	type mutation struct {
		name string
		mut  func(c *Config)
	}
	cases := []mutation{
		{"store name", func(c *Config) { c.Stores[0].Name = "renamed" }},
		{"store type", func(c *Config) { c.Stores[1].Type = "other" }},
		{"redis mode", func(c *Config) { c.Stores[0].Redis.Mode = "cluster" }},
		{"redis addresses", func(c *Config) { c.Stores[0].Redis.Addresses[1] = "10.0.1.99:26379" }},
		{"redis master_name", func(c *Config) { c.Stores[0].Redis.MasterName = "other" }},
		{"redis username", func(c *Config) { c.Stores[0].Redis.Username = "other" }},
		{"redis password", func(c *Config) { c.Stores[0].Redis.Password = NewSecret("other-password") }},
		{"redis sentinel_username", func(c *Config) { c.Stores[0].Redis.SentinelUsername = "other" }},
		{"redis sentinel_password", func(c *Config) { c.Stores[0].Redis.SentinelPassword = NewSecret("other-password") }},
		{"redis db", func(c *Config) { c.Stores[0].Redis.DB = 3 }},
		{"redis tls enabled", func(c *Config) { c.Stores[0].Redis.TLS.Enabled = false }},
		{"redis tls ca_file", func(c *Config) { c.Stores[0].Redis.TLS.CAFile = "other-ca.pem" }},
		{"redis tls cert_file", func(c *Config) { c.Stores[0].Redis.TLS.CertFile = "other.crt" }},
		{"redis tls key_file", func(c *Config) { c.Stores[0].Redis.TLS.KeyFile = "other.key" }},
		{"redis tls server_name", func(c *Config) { c.Stores[0].Redis.TLS.ServerName = "other.example.com" }},
		{"redis tls insecure_skip_verify", func(c *Config) { c.Stores[0].Redis.TLS.InsecureSkipVerify = true }},
		{"redis dial_timeout", func(c *Config) { *c.Stores[0].Redis.DialTimeout = Duration(9 * time.Second) }},
		{"redis read_timeout", func(c *Config) { *c.Stores[0].Redis.ReadTimeout = Duration(9 * time.Second) }},
		{"redis write_timeout", func(c *Config) { *c.Stores[0].Redis.WriteTimeout = Duration(9 * time.Second) }},
		{"redis pool_size", func(c *Config) { c.Stores[0].Redis.PoolSize = 99 }},
		{"redis min_idle_conns", func(c *Config) { c.Stores[0].Redis.MinIdleConns = 1 }},
		{"redis key_prefix", func(c *Config) { *c.Stores[0].Redis.KeyPrefix = "other" }},
		{"guard consecutive_failures", func(c *Config) { *c.Stores[1].Guard.ConsecutiveFailures = 9 }},
		{"guard open_timeout", func(c *Config) { *c.Stores[1].Guard.OpenTimeout = Duration(time.Minute) }},
		{"guard max_inflight", func(c *Config) { *c.Stores[1].Guard.MaxInflight = 7 }},
		{"params nested leaf", func(c *Config) { setNestedToken(c, "other") }},
		{"route store", func(c *Config) { c.Routes[1].RateLimit.Store = "other" }},
		{"route on_store_error", func(c *Config) { c.Routes[1].RateLimit.OnStoreError = "allow" }},
		{"route store_timeout", func(c *Config) { *c.Routes[1].RateLimit.StoreTimeout = Duration(time.Second) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := parseFull(t)
			tc.mut(c)
			if c.Fingerprint() == baseFP {
				t.Errorf("changing %s did not change Fingerprint", tc.name)
			}
		})
	}

	storeFP := func(c *Config, i int) string { return c.Stores[i].BackendFingerprint() }
	c := parseFull(t)
	if storeFP(c, 0) == storeFP(parseFull(t), 1) {
		t.Errorf("distinct stores share a BackendFingerprint")
	}
	c = parseFull(t)
	c.Stores[1].Type = "other"
	if storeFP(c, 1) == storeFP(parseFull(t), 1) {
		t.Errorf("type change did not change BackendFingerprint")
	}
	c = parseFull(t)
	c.Stores[1].Name = "renamed"
	if storeFP(c, 1) != storeFP(parseFull(t), 1) {
		t.Errorf("name change altered BackendFingerprint")
	}
	c = parseFull(t)
	c.Stores[0].Redis.Password = NewSecret("other-password")
	if storeFP(c, 0) == storeFP(parseFull(t), 0) {
		t.Errorf("password change did not change BackendFingerprint")
	}
	c = parseFull(t)
	setNestedToken(c, "other")
	if storeFP(c, 1) == storeFP(parseFull(t), 1) {
		t.Errorf("params change did not change BackendFingerprint")
	}
	c = parseFull(t)
	c.Stores[0].Redis.Addresses[1] = "10.0.1.99:26379"
	if storeFP(c, 0) == storeFP(parseFull(t), 0) {
		t.Errorf("address change did not change BackendFingerprint")
	}
	c = parseFull(t)
	c.Stores[0].Redis.SentinelPassword = NewSecret("other-password")
	if storeFP(c, 0) == storeFP(parseFull(t), 0) {
		t.Errorf("sentinel password change did not change BackendFingerprint")
	}
	c = parseFull(t)
	*c.Stores[1].Guard.MaxInflight = 7
	*c.Stores[1].Guard.ConsecutiveFailures = 9
	*c.Stores[1].Guard.OpenTimeout = Duration(time.Minute)
	if storeFP(c, 1) != storeFP(parseFull(t), 1) {
		t.Errorf("guard change altered BackendFingerprint")
	}
}

func TestStoreFingerprintDefaultsAndOrder(t *testing.T) {
	plain := storeDoc(storeCacheRedis, "")
	explicit := storeDoc(storeCacheRedis+"      dial_timeout: 1s\n    guard:\n      consecutive_failures: 3\n", "      on_store_error: local\n      store_timeout: 50ms\n")
	a, err := Parse([]byte(plain))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Parse([]byte(explicit))
	if err != nil {
		t.Fatal(err)
	}
	if a.Fingerprint() != b.Fingerprint() {
		t.Errorf("explicit defaults changed Config.Fingerprint")
	}
	if a.Stores[0].BackendFingerprint() != b.Stores[0].BackendFingerprint() {
		t.Errorf("explicit defaults changed BackendFingerprint")
	}

	ordered := storeDoc("  - name: cache\n    type: custom\n    params:\n      alpha: 1\n      beta: two\n", "")
	reversed := storeDoc("  - name: cache\n    type: custom\n    params:\n      beta: two\n      alpha: 1\n", "")
	x, err := Parse([]byte(ordered))
	if err != nil {
		t.Fatal(err)
	}
	y, err := Parse([]byte(reversed))
	if err != nil {
		t.Fatal(err)
	}
	if x.Fingerprint() != y.Fingerprint() || x.Stores[0].BackendFingerprint() != y.Stores[0].BackendFingerprint() {
		t.Errorf("params key order changed the fingerprint")
	}
}

func TestStoreRedacted(t *testing.T) {
	c := parseFull(t)
	pm := c.Stores[1].Params.Reveal()
	pm["port"] = 8080
	pm["empty"] = ""
	c.Stores[1].Params = NewSecretParams(pm)
	c.Stores[0].Redis.SentinelPassword = NewSecret("")
	c.Stores[0].Redis.Password = NewSecret("example-redis-password")

	r := c.Redacted()
	if got := r.Stores[0].Redis.Password.Reveal(); got != "REDACTED" {
		t.Errorf("Password = %q, want REDACTED", got)
	}
	if got := r.Stores[0].Redis.SentinelPassword.Reveal(); got != "" {
		t.Errorf("empty SentinelPassword = %q, want empty", got)
	}
	p := r.Stores[1].Params.Reveal()
	if p["endpoint"] != "REDACTED" || p["port"] != "REDACTED" || p["empty"] != "" {
		t.Errorf("params = %v", p)
	}
	nested := p["nested"].(map[string]any)
	if nested["token"] != "REDACTED" {
		t.Errorf("nested token = %v", nested["token"])
	}
	tiers := nested["tiers"].([]any)
	if len(tiers) != 1 || tiers[0] != "REDACTED" {
		t.Errorf("tiers = %v", tiers)
	}

	if c.Stores[0].Redis.Password.Reveal() != "example-redis-password" {
		t.Errorf("source password changed")
	}
	if c.Stores[1].Params.Reveal()["endpoint"] != "https://limits.example.com" {
		t.Errorf("source params changed: %v", c.Stores[1].Params.Reveal()["endpoint"])
	}

	for _, v := range []any{r, c.Redacted()} {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		for _, leak := range []string{"example-redis-password", "example-token", "https://limits.example.com", "example-sentinel-password"} {
			if strings.Contains(string(b), leak) {
				t.Errorf("redacted JSON contains %q", leak)
			}
		}
		if !strings.Contains(string(b), "REDACTED") {
			t.Errorf("redacted JSON has no REDACTED marker")
		}
	}
}

func TestEnvSubstitutionSecretNames(t *testing.T) {
	doc := func(password string) string {
		return storeDoc("  - name: cache\n    type: redis\n    redis:\n      addresses: [\"127.0.0.1:6379\"]\n      password: "+password+"\n", "")
	}

	_, err := Parse([]byte(doc("${LG_MISSING}")))
	if err == nil || !strings.Contains(err.Error(), "LG_MISSING") {
		t.Fatalf("missing variable error = %v", err)
	}

	lookup := func(name string) (string, bool) {
		if name == "LG_SET" {
			return "s3cr3t-env", true
		}
		return "", false
	}
	c, err := Parse([]byte(doc("${LG_SET}")), WithEnvLookup(lookup))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Stores[0].Redis.Password.Reveal(); got != "s3cr3t-env" {
		t.Errorf("Reveal() = %q", got)
	}
	tokenDoc := storeDoc("  - name: cache\n    type: custom\n    params:\n      token: ${LG_SET}\n", "")
	tc, err := Parse([]byte(tokenDoc), WithEnvLookup(lookup))
	if err != nil {
		t.Fatal(err)
	}
	if got := tc.Stores[0].Params.Reveal()["token"]; got != "s3cr3t-env" {
		t.Errorf("params token = %v", got)
	}
	b, err := json.Marshal(c.Redacted())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "s3cr3t-env") {
		t.Errorf("Redacted JSON contains the env secret")
	}

	bad := storeDoc("  - name: cache\n    type: redis\n    redis:\n      mode: ring\n      addresses: [\"127.0.0.1:6379\"]\n      password: ${LG_SET}\n", "")
	_, err = Parse([]byte(bad), WithEnvLookup(lookup))
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error = %v, want *ValidationError", err)
	}
	if strings.Contains(err.Error(), "s3cr3t-env") {
		t.Errorf("error text contains the env secret: %v", err)
	}
}

func setNestedToken(c *Config, v string) {
	m := c.Stores[1].Params.Reveal()
	m["nested"].(map[string]any)["token"] = v
	c.Stores[1].Params = NewSecretParams(m)
}

func TestSecretYAMLShapes(t *testing.T) {
	redisDoc := func(field string) string {
		return storeDoc("  - name: cache\n    type: redis\n    redis:\n      addresses: [\"127.0.0.1:6379\"]\n      "+field+"\n", "")
	}

	c, err := Parse([]byte(redisDoc("password: ~")))
	if err != nil {
		t.Fatal(err)
	}
	if !c.Stores[0].Redis.Password.IsZero() {
		t.Errorf("password: ~ is not zero")
	}

	c, err = Parse([]byte(redisDoc("password: 1234")))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Stores[0].Redis.Password.Reveal(); got != "1234" {
		t.Errorf("password: 1234 reveals %q", got)
	}

	_, err = Parse([]byte(redisDoc("password: {a: 1}")))
	if !hasPath(fieldErrors(t, err), "stores[0].redis.password") {
		t.Errorf("mapping password not reported at stores[0].redis.password: %v", err)
	}
	for _, fe := range fieldErrors(t, err) {
		if fe.Path == "stores[0].redis.password" && fe.Line <= 0 {
			t.Errorf("mapping password error has no line")
		}
		if fe.Path == "stores[0].redis.password" && fe.Msg != "must be a string" {
			t.Errorf("mapping password msg = %q, want %q", fe.Msg, "must be a string")
		}
		if strings.HasPrefix(fe.Path, "stores[0].redis.password.") {
			t.Errorf("error reported at child path %q", fe.Path)
		}
	}

	scalarParams := "  - name: cache\n    type: custom\n    params: \"p-scalar-SECRET\"\n"
	_, err = Parse([]byte(storeDoc(scalarParams, "")))
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("scalar params error = %v, want *ValidationError", err)
	}
	var found bool
	for _, fe := range ve.Errors {
		if fe.Path == "stores[0].params" {
			found = true
			if fe.Line <= 0 {
				t.Errorf("scalar params error has no line")
			}
			if fe.Msg != "must be a mapping" {
				t.Errorf("scalar params msg = %q, want %q", fe.Msg, "must be a mapping")
			}
		}
	}
	if !found {
		t.Errorf("scalar params not reported at stores[0].params: %v", err)
	}
	if strings.Contains(err.Error(), "p-scalar-SECRET") {
		t.Errorf("error text contains the scalar params value: %v", err)
	}

	c, err = Parse([]byte(storeDoc("  - name: cache\n    type: custom\n    params: {}\n", "")))
	if err != nil {
		t.Fatal(err)
	}
	if c.Stores[0].Params.IsZero() {
		t.Errorf("params: {} is zero, want non-zero empty")
	}
	if len(c.Stores[0].Params.Reveal()) != 0 {
		t.Errorf("params: {} reveals %v", c.Stores[0].Params.Reveal())
	}
}
