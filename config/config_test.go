package config

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

const minimalBody = `listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
routes:
  - name: all
    upstream: users
`

const minimalYAML = "version: 1\n" + minimalBody

var wantKeys = []string{
	"version",
	"global",
	"global.request_id_header",
	"global.trusted_proxies",
	"global.strip_identity_headers",
	"global.drain_timeout",
	"global.shutdown_delay",
	"global.log",
	"global.log.level",
	"global.log.format",
	"global.log.access_log",
	"global.reload",
	"global.reload.debounce",
	"global.reload.poll_interval",
	"listeners",
	"listeners[].name",
	"listeners[].address",
	"listeners[].tls",
	"listeners[].tls.cert_file",
	"listeners[].tls.key_file",
	"listeners[].tls.min_version",
	"listeners[].http2",
	"listeners[].h2c",
	"listeners[].read_header_timeout",
	"listeners[].read_timeout",
	"listeners[].write_timeout",
	"listeners[].idle_timeout",
	"listeners[].max_header_bytes",
	"admin",
	"admin.enabled",
	"admin.address",
	"upstreams",
	"upstreams[].name",
	"upstreams[].hosts",
	"upstreams[].hosts[].url",
	"upstreams[].hosts[].weight",
	"upstreams[].dns",
	"upstreams[].dns.hostname",
	"upstreams[].dns.port",
	"upstreams[].dns.scheme",
	"upstreams[].dns.refresh_interval",
	"upstreams[].load_balancer",
	"upstreams[].load_balancer.algorithm",
	"upstreams[].load_balancer.hash",
	"upstreams[].load_balancer.hash.source",
	"upstreams[].load_balancer.hash.name",
	"upstreams[].load_balancer.bounded_load_factor",
	"upstreams[].load_balancer.virtual_nodes",
	"upstreams[].timeouts",
	"upstreams[].timeouts.connect",
	"upstreams[].timeouts.tls_handshake",
	"upstreams[].timeouts.response_header",
	"upstreams[].timeouts.idle",
	"upstreams[].pool",
	"upstreams[].pool.max_idle_per_host",
	"upstreams[].pool.max_conns_per_host",
	"upstreams[].http2",
	"upstreams[].passive_health",
	"upstreams[].passive_health.consecutive_failures",
	"upstreams[].passive_health.base_ejection_time",
	"upstreams[].passive_health.max_ejection_time",
	"upstreams[].passive_health.max_ejection_percent",
	"upstreams[].active_health",
	"upstreams[].active_health.path",
	"upstreams[].active_health.interval",
	"upstreams[].active_health.jitter",
	"upstreams[].active_health.timeout",
	"upstreams[].active_health.healthy_threshold",
	"upstreams[].active_health.unhealthy_threshold",
	"upstreams[].active_health.expected_statuses",
	"upstreams[].circuit_breaker",
	"upstreams[].circuit_breaker.window",
	"upstreams[].circuit_breaker.buckets",
	"upstreams[].circuit_breaker.min_requests",
	"upstreams[].circuit_breaker.failure_ratio",
	"upstreams[].circuit_breaker.consecutive_failures",
	"upstreams[].circuit_breaker.open_timeout",
	"upstreams[].circuit_breaker.half_open_probes",
	"upstreams[].circuit_breaker.success_threshold",
	"auth",
	"auth.jwt",
	"auth.jwt[].name",
	"auth.jwt[].issuer",
	"auth.jwt[].audiences",
	"auth.jwt[].leeway",
	"auth.jwt[].header",
	"auth.jwt[].keys",
	"auth.jwt[].keys[].kid",
	"auth.jwt[].keys[].algorithm",
	"auth.jwt[].keys[].secret",
	"auth.jwt[].keys[].public_key",
	"auth.jwt[].keys[].public_key_file",
	"auth.jwt[].jwks",
	"auth.jwt[].jwks.url",
	"auth.jwt[].jwks.algorithms",
	"auth.jwt[].jwks.ttl",
	"auth.jwt[].jwks.min_refresh_interval",
	"auth.jwt[].jwks.timeout",
	"auth.jwt[].scope_claim",
	"auth.jwt[].roles_claim",
	"auth.jwt[].forward_claims",
	"auth.jwt[].max_token_bytes",
	"auth.jwt[].forward_credentials",
	"auth.api_keys",
	"auth.api_keys[].name",
	"auth.api_keys[].header",
	"auth.api_keys[].query",
	"auth.api_keys[].key_file",
	"auth.api_keys[].keys",
	"auth.api_keys[].keys[].id",
	"auth.api_keys[].keys[].hash",
	"auth.api_keys[].keys[].consumer",
	"auth.api_keys[].keys[].metadata",
	"auth.api_keys[].keys[].scopes",
	"auth.api_keys[].keys[].roles",
	"auth.api_keys[].consumer_header",
	"auth.api_keys[].metadata_headers",
	"auth.api_keys[].forward_credentials",
	"middlewares",
	"middlewares[].name",
	"middlewares[].type",
	"middlewares[].params",
	"routes",
	"routes[].name",
	"routes[].match",
	"routes[].match.hosts",
	"routes[].match.path",
	"routes[].match.path_type",
	"routes[].match.methods",
	"routes[].match.headers",
	"routes[].priority",
	"routes[].upstream",
	"routes[].preserve_host",
	"routes[].rewrite",
	"routes[].rewrite.strip_prefix",
	"routes[].rewrite.replace_prefix",
	"routes[].rewrite.replace_prefix.from",
	"routes[].rewrite.replace_prefix.to",
	"routes[].request_headers",
	"routes[].request_headers.add",
	"routes[].request_headers.set",
	"routes[].request_headers.remove",
	"routes[].response_headers",
	"routes[].response_headers.add",
	"routes[].response_headers.set",
	"routes[].response_headers.remove",
	"routes[].timeout",
	"routes[].auth",
	"routes[].auth.mode",
	"routes[].auth.providers",
	"routes[].auth.allow_anonymous",
	"routes[].auth.require",
	"routes[].auth.require.scopes",
	"routes[].auth.require.roles",
	"routes[].auth.require.claims",
	"routes[].rate_limit",
	"routes[].rate_limit.algorithm",
	"routes[].rate_limit.rate",
	"routes[].rate_limit.per",
	"routes[].rate_limit.burst",
	"routes[].rate_limit.limit",
	"routes[].rate_limit.window",
	"routes[].rate_limit.key",
	"routes[].rate_limit.key.source",
	"routes[].rate_limit.key.name",
	"routes[].rate_limit.max_keys",
	"routes[].rate_limit.idle_ttl",
	"routes[].circuit_breaker",
	"routes[].circuit_breaker.enabled",
	"routes[].retry",
	"routes[].retry.attempts",
	"routes[].retry.base_delay",
	"routes[].retry.max_delay",
	"routes[].retry.on_statuses",
	"routes[].retry.methods",
	"routes[].retry.idempotency_key",
	"routes[].retry.idempotency_key.enabled",
	"routes[].retry.idempotency_key.header",
	"routes[].retry.budget",
	"routes[].retry.budget.ratio",
	"routes[].retry.budget.min_per_second",
	"routes[].retry.budget.window",
	"routes[].retry.max_body_bytes",
	"routes[].middlewares",
	"routes[].pipeline",
}

func fieldErrors(t *testing.T, err error) []FieldError {
	t.Helper()
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("want *ValidationError, got %T: %v", err, err)
	}
	return ve.Errors
}

func lineOf(src, substr string) int {
	for i, l := range strings.Split(src, "\n") {
		if strings.Contains(l, substr) {
			return i + 1
		}
	}
	return 0
}

func TestInvalidMultiCollectsAll(t *testing.T) {
	_, err := LoadFile("testdata/invalid_multi.yaml")
	errs := fieldErrors(t, err)
	want := []struct {
		path string
		line int
	}{
		{"listeners[0].bogus_key", 5},
		{"upstreams[1].name", 10},
		{"routes[0].upstream", 17},
		{"routes[1].match.path", 20},
		{"routes[1].timeout", 23},
	}
	if len(errs) != len(want) {
		t.Fatalf("got %d errors, want %d: %v", len(errs), len(want), err)
	}
	for i, w := range want {
		if errs[i].Path != w.path || errs[i].Line != w.line {
			t.Errorf("error %d: got %s (line %d), want %s (line %d)", i, errs[i].Path, errs[i].Line, w.path, w.line)
		}
		if errs[i].Msg == "" {
			t.Errorf("error %d has an empty message", i)
		}
	}
	text := err.Error()
	last := -1
	for _, w := range want {
		idx := strings.Index(text, w.path+" (line")
		if idx <= last {
			t.Errorf("%s not listed in line order in %q", w.path, text)
		}
		last = idx
	}
}

func TestFullSchemaAccepted(t *testing.T) {
	data, err := os.ReadFile("testdata/full.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Parse(data); err != nil {
		t.Fatalf("full.yaml rejected: %v", err)
	}
}

func collectSchema(tb testing.TB, t reflect.Type, prefix string, keys, mapLeaves map[string]bool) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			tag := f.Tag.Get("yaml")
			if tag == "-" {
				continue
			}
			name, opts, _ := strings.Cut(tag, ",")
			if name == "" || strings.Contains(","+opts+",", ",inline,") {
				tb.Errorf("field %s.%s has no explicit yaml key", t.Name(), f.Name)
				continue
			}
			p := name
			if prefix != "" {
				p = prefix + "." + name
			}
			keys[p] = true
			ft := f.Type
			for ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Map {
				mapLeaves[p] = true
			}
			collectSchema(tb, f.Type, p, keys, mapLeaves)
		}
	case reflect.Slice:
		collectSchema(tb, t.Elem(), prefix+"[]", keys, mapLeaves)
	}
}

func collectNodeKeys(n *yaml.Node, prefix string, mapLeaves, out map[string]bool) {
	switch n.Kind {
	case yaml.DocumentNode:
		for _, c := range n.Content {
			collectNodeKeys(c, prefix, mapLeaves, out)
		}
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			p := n.Content[i].Value
			if prefix != "" {
				p = prefix + "." + p
			}
			out[p] = true
			if mapLeaves[p] {
				continue
			}
			collectNodeKeys(n.Content[i+1], p, mapLeaves, out)
		}
	case yaml.SequenceNode:
		for _, c := range n.Content {
			collectNodeKeys(c, prefix+"[]", mapLeaves, out)
		}
	}
}

func TestSchemaKeysComplete(t *testing.T) {
	schema := map[string]bool{}
	mapLeaves := map[string]bool{}
	collectSchema(t, reflect.TypeOf(Config{}), "", schema, mapLeaves)

	want := map[string]bool{}
	for _, k := range wantKeys {
		want[k] = true
	}
	for k := range schema {
		if !want[k] {
			t.Errorf("schema key %q is missing from wantKeys", k)
		}
	}
	for k := range want {
		if !schema[k] {
			t.Errorf("wantKeys entry %q is not a schema key", k)
		}
	}

	data, err := os.ReadFile("testdata/full.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		t.Fatal(err)
	}
	nodeKeys := map[string]bool{}
	collectNodeKeys(&root, "", mapLeaves, nodeKeys)
	for _, k := range wantKeys {
		if !nodeKeys[k] {
			t.Errorf("full.yaml does not set %q", k)
		}
	}
}

func TestDefaults(t *testing.T) {
	c, err := Parse([]byte(minimalYAML))
	if err != nil {
		t.Fatal(err)
	}
	if c.Global.RequestIDHeader != "X-Request-ID" {
		t.Errorf("RequestIDHeader = %q", c.Global.RequestIDHeader)
	}
	if c.Global.DrainTimeout.Std() != 30*time.Second {
		t.Errorf("DrainTimeout = %v", c.Global.DrainTimeout)
	}
	if c.Listeners[0].HTTP2 == nil || !*c.Listeners[0].HTTP2 {
		t.Errorf("HTTP2 default not true")
	}
	if c.Listeners[0].MaxHeaderBytes != 1048576 {
		t.Errorf("MaxHeaderBytes = %d", c.Listeners[0].MaxHeaderBytes)
	}
	if c.Admin.Address != "127.0.0.1:9901" {
		t.Errorf("Admin.Address = %q", c.Admin.Address)
	}
	if c.Upstreams[0].LoadBalancer.Algorithm != "p2c" {
		t.Errorf("Algorithm = %q", c.Upstreams[0].LoadBalancer.Algorithm)
	}
	if w := c.Upstreams[0].Hosts[0].Weight; w == nil || *w != 1 {
		t.Errorf("Weight = %v", w)
	}
	if c.Routes[0].Match.Path != "/" || c.Routes[0].Match.PathType != "prefix" {
		t.Errorf("Match = %+v", c.Routes[0].Match)
	}

	c, err = Parse([]byte(minimalYAML + "    retry: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	r := c.Routes[0].Retry
	if r == nil {
		t.Fatal("retry not decoded")
	}
	if r.Attempts == nil || *r.Attempts != 3 || !reflect.DeepEqual(r.OnStatuses, []int{502, 503, 504}) {
		t.Errorf("retry defaults = %+v", r)
	}
	if r.MaxBodyBytes == nil || *r.MaxBodyBytes != 65536 {
		t.Errorf("default max_body_bytes not 65536: %v", r.MaxBodyBytes)
	}
	if r.Budget.Ratio == nil || *r.Budget.Ratio != 0.1 {
		t.Errorf("default budget ratio not 0.1: %v", r.Budget.Ratio)
	}
	if r.Budget.MinPerSecond == nil || *r.Budget.MinPerSecond != 10 {
		t.Errorf("default budget min_per_second not 10: %v", r.Budget.MinPerSecond)
	}

	c, err = Parse([]byte(strings.Replace(minimalYAML, "    hosts:\n", "    circuit_breaker: {}\n    hosts:\n", 1)))
	if err != nil {
		t.Fatal(err)
	}
	cb := c.Upstreams[0].CircuitBreaker
	if cb == nil || cb.ConsecutiveFailures == nil || *cb.ConsecutiveFailures != 5 {
		t.Errorf("default consecutive_failures not 5: %+v", cb)
	}
	if cb == nil || cb.Buckets == nil || *cb.Buckets != 10 {
		t.Errorf("default buckets not 10: %+v", cb)
	}
	if cb == nil || cb.MinRequests == nil || *cb.MinRequests != 20 {
		t.Errorf("default min_requests not 20: %+v", cb)
	}
	if cb == nil || cb.FailureRatio == nil || *cb.FailureRatio != 0.5 {
		t.Errorf("default failure_ratio not 0.5: %+v", cb)
	}
	if cb == nil || cb.HalfOpenProbes == nil || *cb.HalfOpenProbes != 3 {
		t.Errorf("default half_open_probes not 3: %+v", cb)
	}
	if cb == nil || cb.SuccessThreshold == nil || *cb.SuccessThreshold != 2 {
		t.Errorf("default success_threshold not 2: %+v", cb)
	}
}

func TestResilienceDefaultsKeepExplicitZero(t *testing.T) {
	const explicit = `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
    circuit_breaker:
      consecutive_failures: 0
routes:
  - name: all
    upstream: users
    retry:
      max_body_bytes: 0
      budget:
        ratio: 0
        min_per_second: 0
`
	c, err := Parse([]byte(explicit))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if cb := c.Upstreams[0].CircuitBreaker; cb == nil || cb.ConsecutiveFailures == nil || *cb.ConsecutiveFailures != 0 {
		t.Errorf("explicit consecutive_failures 0 not kept: %+v", cb)
	}
	rt := c.Routes[0].Retry
	if rt == nil || rt.MaxBodyBytes == nil || *rt.MaxBodyBytes != 0 {
		t.Errorf("explicit max_body_bytes 0 not kept: %+v", rt)
	}
	if rt == nil || rt.Budget.Ratio == nil || *rt.Budget.Ratio != 0 {
		t.Errorf("explicit budget ratio 0 not kept: %+v", rt)
	}
	if rt == nil || rt.Budget.MinPerSecond == nil || *rt.Budget.MinPerSecond != 0 {
		t.Errorf("explicit budget min_per_second 0 not kept: %+v", rt)
	}
}

func TestHealthDefaultsKeepExplicitZero(t *testing.T) {
	const explicit = `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
    passive_health:
      max_ejection_percent: 0
    active_health:
      jitter: 0
routes:
  - name: all
    upstream: users
`
	c, err := Parse([]byte(explicit))
	if err != nil {
		t.Fatal(err)
	}
	if ph := c.Upstreams[0].PassiveHealth; ph == nil || ph.MaxEjectionPercent == nil || *ph.MaxEjectionPercent != 0 {
		t.Errorf("explicit max_ejection_percent 0 not kept: %+v", ph)
	}
	if ah := c.Upstreams[0].ActiveHealth; ah == nil || ah.Jitter == nil || *ah.Jitter != 0 {
		t.Errorf("explicit jitter 0 not kept: %+v", ah)
	}

	const empty = `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
    passive_health: {}
    active_health: {}
routes:
  - name: all
    upstream: users
`
	c, err = Parse([]byte(empty))
	if err != nil {
		t.Fatal(err)
	}
	if ph := c.Upstreams[0].PassiveHealth; ph == nil || ph.MaxEjectionPercent == nil || *ph.MaxEjectionPercent != 50 {
		t.Errorf("default max_ejection_percent not 50: %+v", ph)
	}
	if ah := c.Upstreams[0].ActiveHealth; ah == nil || ah.Jitter == nil || *ah.Jitter != 0.1 {
		t.Errorf("default jitter not 0.1: %+v", ah)
	}
}

func TestHostWeightZero(t *testing.T) {
	src := strings.Replace(minimalYAML, "      - url: \"http://127.0.0.1:9001\"\n", "      - url: \"http://127.0.0.1:9001\"\n        weight: 0\n", 1)
	c, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if w := c.Upstreams[0].Hosts[0].Weight; w == nil || *w != 0 {
		t.Errorf("Weight = %v, want explicit 0", w)
	}

	src = strings.Replace(minimalYAML, "      - url: \"http://127.0.0.1:9001\"\n", "      - url: \"http://127.0.0.1:9001\"\n        weight: -1\n", 1)
	_, err = Parse([]byte(src))
	if !hasPath(fieldErrors(t, err), "upstreams[0].hosts[0].weight") {
		t.Errorf("negative weight not reported at upstreams[0].hosts[0].weight: %v", err)
	}
}

func TestMultiDocumentRejected(t *testing.T) {
	_, err := Parse([]byte(minimalYAML + "---\nversion: 1\n"))
	if err == nil || !strings.Contains(err.Error(), "multiple YAML documents") {
		t.Fatalf("got %v", err)
	}
}

func TestUnknownNestedKeys(t *testing.T) {
	src := `version: 1
global:
  log:
    colour: true
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
routes:
  - name: all
    upstream: users
    retry:
      budget:
        bogus: 1
`
	_, err := Parse([]byte(src))
	errs := fieldErrors(t, err)
	want := map[string]int{
		"global.log.colour":            lineOf(src, "colour: true"),
		"routes[0].retry.budget.bogus": lineOf(src, "bogus: 1"),
	}
	if len(errs) != len(want) {
		t.Fatalf("got %d errors, want %d: %v", len(errs), len(want), err)
	}
	for _, fe := range errs {
		line, ok := want[fe.Path]
		if !ok {
			t.Errorf("unexpected error %v", fe)
			continue
		}
		if fe.Line != line {
			t.Errorf("%s at line %d, want %d", fe.Path, fe.Line, line)
		}
	}
}

func TestSyntaxError(t *testing.T) {
	_, err := Parse([]byte("version: 1\nlisteners: [\n"))
	errs := fieldErrors(t, err)
	if len(errs) != 1 || errs[0].Line <= 0 {
		t.Fatalf("got %v", errs)
	}
}

func TestTypeMismatchStillValidates(t *testing.T) {
	src := `version: 1
listeners:
  - name: public
    address: ":8080"
    max_header_bytes: abc
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
routes:
  - name: all
    upstream: nope
`
	_, err := Parse([]byte(src))
	errs := fieldErrors(t, err)
	var sawType, sawUpstream bool
	for _, fe := range errs {
		if fe.Path == "listeners[0].max_header_bytes" && fe.Line == lineOf(src, "max_header_bytes: abc") {
			sawType = true
		}
		if fe.Path == "routes[0].upstream" && strings.Contains(fe.Msg, "unknown upstream") {
			sawUpstream = true
		}
	}
	if !sawType || !sawUpstream {
		t.Fatalf("type error %v, unknown upstream %v: %v", sawType, sawUpstream, err)
	}
}

func TestLoadFileMissing(t *testing.T) {
	_, err := LoadFile("testdata/nope.yaml")
	if err == nil {
		t.Fatal("want error")
	}
	var ve *ValidationError
	if errors.As(err, &ve) {
		t.Fatalf("missing file reported as ValidationError: %v", err)
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("want fs.ErrNotExist, got %v", err)
	}
}

func TestLoadFileResolvesKeyFile(t *testing.T) {
	dir := t.TempDir()
	keys, err := os.ReadFile("testdata/keys.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "keys.yaml"), keys, 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "lychgate.yaml")
	write := func(src string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	write(minimalYAML + "auth:\n  api_keys:\n    - name: file\n      key_file: keys.yaml\n")
	c, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	p := c.Auth.APIKeys[0]
	if !filepath.IsAbs(p.KeyFile) {
		t.Errorf("KeyFile %q is not absolute", p.KeyFile)
	}
	if len(p.FileKeys) != 1 || p.FileKeys[0].ID != "file-key" {
		t.Errorf("file keys = %+v", p.FileKeys)
	}

	write(minimalYAML + "auth:\n  api_keys:\n    - name: file\n      key_file: missing.yaml\n")
	_, err = LoadFile(path)
	if !hasPath(fieldErrors(t, err), "auth.api_keys[0].key_file") {
		t.Errorf("missing key file not reported at auth.api_keys[0].key_file: %v", err)
	}

	write(minimalYAML + "auth:\n  jwt:\n    - name: pk\n      issuer: https://auth.example.com\n      audiences: [example-api]\n      keys:\n        - algorithm: RS256\n          public_key_file: missing.pem\n")
	_, err = LoadFile(path)
	if !hasPath(fieldErrors(t, err), "auth.jwt[0].keys[0].public_key_file") {
		t.Errorf("missing public key file not reported: %v", err)
	}
}

func TestMiddlewareErrorLines(t *testing.T) {
	src := `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
middlewares:
  - name: retry
    type: cors
  - name: lim
    type: request_size_limit
    params:
      max_bytes: 0
  - name: c
    type: cors
    params:
      allowed_origins:
        - "*"
      allow_credentials: true
      bogus: 1
routes:
  - name: all
    upstream: users
    middlewares:
      - lim
      - lim
    pipeline:
      - timeout
      - timeout
`
	nthLine := func(substr string, n int) int {
		seen := 0
		for i, l := range strings.Split(src, "\n") {
			if strings.Contains(l, substr) {
				seen++
				if seen == n {
					return i + 1
				}
			}
		}
		return 0
	}
	_, err := Parse([]byte(src))
	errs := fieldErrors(t, err)
	want := []struct {
		path string
		line int
	}{
		{"middlewares[0].name", lineOf(src, "- name: retry")},
		{"middlewares[1].params.max_bytes", lineOf(src, "max_bytes: 0")},
		{"middlewares[2].params.allow_credentials", lineOf(src, "allow_credentials: true")},
		{"middlewares[2].params.bogus", lineOf(src, "bogus: 1")},
		{"routes[0].middlewares[1]", nthLine("- lim", 2)},
		{"routes[0].pipeline[1]", nthLine("- timeout", 2)},
	}
	for _, w := range want {
		found := false
		for _, fe := range errs {
			if fe.Path == w.path && fe.Line == w.line {
				found = true
			}
		}
		if !found {
			t.Errorf("no error at %s (line %d); got %v", w.path, w.line, err)
		}
	}
}

func hasPath(errs []FieldError, path string) bool {
	for _, fe := range errs {
		if fe.Path == path {
			return true
		}
	}
	return false
}

func goConfig(version int) *Config {
	return &Config{
		Version:   version,
		Listeners: []Listener{{Name: "public", Address: ":8080"}},
		Upstreams: []Upstream{{Name: "users", Hosts: []Host{{URL: "http://127.0.0.1:9001"}}}},
		Routes:    []Route{{Name: "all", Upstream: "users"}},
	}
}

func TestValidateGoBuilt(t *testing.T) {
	c := goConfig(1)
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.Listeners[0].MaxHeaderBytes != 1048576 {
		t.Errorf("defaults not applied: MaxHeaderBytes = %d", c.Listeners[0].MaxHeaderBytes)
	}

	err := goConfig(2).Validate()
	errs := fieldErrors(t, err)
	if len(errs) != 1 || errs[0].Line != 0 || errs[0].Path != "version" {
		t.Fatalf("got %v", errs)
	}
}
