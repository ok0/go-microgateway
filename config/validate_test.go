package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

const baseYAML = `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
    circuit_breaker:
      half_open_probes: 3
      success_threshold: 2
auth:
  jwt:
    - name: jwt1
      issuer: https://auth.example.com
      audiences:
        - example-api
      keys:
        - algorithm: HS256
          secret: example-secret-0123456789abcdef0123
  api_keys:
    - name: keys1
      keys:
        - id: alice
          hash: "sha256:9771189e33ad49531f98873eb4e4458ce1c96f313fe15c6d753f4c7b6ad4eba5"
          consumer: alice
middlewares:
  - name: cors1
    type: cors
routes:
  - name: users-api
    match:
      path: /api/users
      path_type: prefix
    upstream: users
  - name: health
    match:
      path: /healthz
      path_type: exact
    upstream: users
`

func base() *Config {
	c, err := Parse([]byte(baseYAML))
	if err != nil {
		panic(err)
	}
	return c
}

func TestSemanticChecks(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
		path   string
		substr string
	}{
		{"version 2", func(c *Config) { c.Version = 2 }, "version", "unsupported version"},
		{"duplicate listener name", func(c *Config) {
			c.Listeners = append(c.Listeners, c.Listeners[0])
		}, "listeners[1].name", "duplicate name"},
		{"duplicate upstream name", func(c *Config) {
			c.Upstreams = append(c.Upstreams, c.Upstreams[0])
		}, "upstreams[1].name", "duplicate name"},
		{"duplicate route name", func(c *Config) {
			c.Routes[1].Name = c.Routes[0].Name
		}, "routes[1].name", "duplicate name"},
		{"duplicate provider name across jwt and api_keys", func(c *Config) {
			c.Auth.APIKeys[0].Name = "jwt1"
		}, "auth.api_keys[0].name", "duplicate name"},
		{"duplicate middleware name", func(c *Config) {
			c.Middlewares = append(c.Middlewares, Middleware{Name: "cors1", Type: "cors"})
		}, "middlewares[1].name", "duplicate name"},
		{"unknown route upstream", func(c *Config) {
			c.Routes[0].Upstream = "nope"
		}, "routes[0].upstream", "unknown upstream"},
		{"unknown auth provider", func(c *Config) {
			c.Routes[0].Auth = &RouteAuth{Mode: "any", Providers: []string{"nope"}}
		}, "routes[0].auth.providers[0]", "unknown auth provider"},
		{"unknown route middleware", func(c *Config) {
			c.Routes[0].Middlewares = []string{"nope"}
		}, "routes[0].middlewares[0]", "unknown middleware"},
		{"unknown pipeline stage", func(c *Config) {
			c.Routes[0].Pipeline = []string{"bogus"}
		}, "routes[0].pipeline[0]", "unknown pipeline stage"},
		{"drain timeout negative", func(c *Config) {
			c.Global.DrainTimeout = Duration(-time.Second)
		}, "global.drain_timeout", "must be positive"},
		{"zero rate on token_bucket", func(c *Config) {
			c.Routes[0].RateLimit = &RateLimit{Algorithm: "token_bucket", Rate: int64Ptr(0)}
		}, "routes[0].rate_limit.rate", "must be >= 1"},
		{"invalid path_type", func(c *Config) {
			c.Routes[0].Match.PathType = "regex"
		}, "routes[0].match.path_type", "must be one of"},
		{"invalid auth mode", func(c *Config) {
			c.Routes[0].Auth = &RouteAuth{Mode: "bogus"}
		}, "routes[0].auth.mode", "must be one of"},
		{"invalid lb algorithm", func(c *Config) {
			c.Upstreams[0].LoadBalancer.Algorithm = "bogus"
		}, "upstreams[0].load_balancer.algorithm", "must be one of"},
		{"invalid rate-limit key source", func(c *Config) {
			c.Routes[0].RateLimit = &RateLimit{Rate: int64Ptr(1), Key: RateLimitKey{Source: "bogus"}}
		}, "routes[0].rate_limit.key.source", "must be one of"},
		{"invalid hash source with consistent_hash", func(c *Config) {
			c.Upstreams[0].LoadBalancer = LoadBalancer{Algorithm: "consistent_hash", Hash: Hash{Source: "bogus"}}
		}, "upstreams[0].load_balancer.hash.source", "must be one of"},
		{"host URL scheme", func(c *Config) {
			c.Upstreams[0].Hosts[0].URL = NewSecretURL("ftp://x")
		}, "upstreams[0].hosts[0].url", "scheme"},
		{"jwks url", func(c *Config) {
			c.Auth.JWT[0].JWKS = &JWKS{URL: NewSecretURL("not a url"), Algorithms: []string{"RS256"}}
		}, "auth.jwt[0].jwks.url", "must be an http or https URL"},
		{"trusted proxy CIDR", func(c *Config) {
			c.Global.TrustedProxies = []string{"10.0.0.0/33"}
		}, "global.trusted_proxies[0]", "invalid CIDR"},
		{"no listeners", func(c *Config) {
			c.Listeners = nil
		}, "listeners", "at least one listener is required"},
		{"upstream with neither hosts nor dns", func(c *Config) {
			c.Upstreams[0].Hosts = nil
		}, "upstreams[0]", "either hosts or dns is required"},
		{"upstream with both hosts and dns", func(c *Config) {
			c.Upstreams[0].DNS = &DNS{Hostname: "x"}
		}, "upstreams[0]", "mutually exclusive"},
		{"pattern with glob in the middle", func(c *Config) {
			c.Routes[0].Match = Match{Path: "/a/*r/b", PathType: "pattern"}
		}, "routes[0].match.path", "invalid path"},
		{"conflicting routes", func(c *Config) {
			r := c.Routes[0]
			r.Name = "copy"
			c.Routes = append(c.Routes, r)
		}, "routes[2]", "conflicts with route"},
		{"breaker probes below success threshold", func(c *Config) {
			c.Upstreams[0].CircuitBreaker.HalfOpenProbes = intPtr(1)
		}, "upstreams[0].circuit_breaker.half_open_probes", "must be >= success_threshold"},
		{"jwt leeway above 5m", func(c *Config) {
			c.Auth.JWT[0].Leeway = durPtr(6 * time.Minute)
		}, "auth.jwt[0].leeway", "must not exceed 5m"},
		{"HS256 key with public_key instead of secret", func(c *Config) {
			c.Auth.JWT[0].Keys[0].Secret = NewSecret("")
			c.Auth.JWT[0].Keys[0].PublicKey = "pem"
		}, "auth.jwt[0].keys[0].public_key", "not allowed with HS"},
		{"RS256 key with secret", func(c *Config) {
			c.Auth.JWT[0].Keys[0].Algorithm = "RS256"
		}, "auth.jwt[0].keys[0].secret", "allowed only with HS"},
		{"retry not last in pipeline", func(c *Config) {
			c.Routes[0].Pipeline = []string{"retry", "auth"}
		}, "routes[0].pipeline[0]", `"retry" must be the last stage`},
		{"api key hash form", func(c *Config) {
			c.Auth.APIKeys[0].Keys[0].Hash = NewSecret("sha256:abc")
		}, "auth.api_keys[0].keys[0].hash", "must have the form"},
		{"jwt mode with an api_keys provider", func(c *Config) {
			c.Routes[0].Auth = &RouteAuth{Mode: "jwt", Providers: []string{"keys1"}}
		}, "routes[0].auth.providers[0]", "is not a jwt provider"},
		{"middleware named retry", func(c *Config) {
			c.Middlewares[0].Name = "retry"
		}, "middlewares[0].name", "reserved"},
		{"duplicate route middleware", func(c *Config) {
			c.Routes[0].Middlewares = []string{"cors1", "cors1"}
		}, "routes[0].middlewares[1]", "duplicate middleware"},
		{"invalid cors param", func(c *Config) {
			c.Middlewares[0].Params = NewSecretParams(map[string]any{"max_age": -1})
		}, "middlewares[0].params.max_age", "must not be negative"},
		{"unknown builtin param", func(c *Config) {
			c.Middlewares[0].Params = NewSecretParams(map[string]any{"origin": "x"})
		}, "middlewares[0].params.origin", "unknown field"},
		{"request_size_limit missing max_bytes", func(c *Config) {
			c.Middlewares = append(c.Middlewares, Middleware{Name: "lim", Type: "request_size_limit"})
		}, "middlewares[1].params.max_bytes", "must be positive"},
		{"duplicate pipeline stage", func(c *Config) {
			c.Routes[0].Pipeline = []string{"timeout", "timeout"}
		}, "routes[0].pipeline[1]", "duplicate"},
		{"passive max_ejection_percent above 100", func(c *Config) {
			c.Upstreams[0].PassiveHealth = &PassiveHealth{MaxEjectionPercent: intPtr(101)}
		}, "upstreams[0].passive_health.max_ejection_percent", "between 0 and 100"},
		{"passive max_ejection_percent negative", func(c *Config) {
			c.Upstreams[0].PassiveHealth = &PassiveHealth{MaxEjectionPercent: intPtr(-1)}
		}, "upstreams[0].passive_health.max_ejection_percent", "between 0 and 100"},
		{"active jitter of 1", func(c *Config) {
			c.Upstreams[0].ActiveHealth = &ActiveHealth{Interval: Duration(10 * time.Second), Timeout: Duration(2 * time.Second), Jitter: floatPtr(1.0)}
		}, "upstreams[0].active_health.jitter", "[0, 1)"},
		{"active timeout equal to interval", func(c *Config) {
			c.Upstreams[0].ActiveHealth = &ActiveHealth{Interval: Duration(10 * time.Second), Timeout: Duration(10 * time.Second)}
		}, "upstreams[0].active_health.timeout", "less than interval"},
		{"active path without slash", func(c *Config) {
			c.Upstreams[0].ActiveHealth = &ActiveHealth{Path: "health", Interval: Duration(10 * time.Second), Timeout: Duration(2 * time.Second)}
		}, "upstreams[0].active_health.path", "must start with"},
		{"active negative healthy threshold", func(c *Config) {
			c.Upstreams[0].ActiveHealth = &ActiveHealth{HealthyThreshold: intPtr(-1), Interval: Duration(10 * time.Second), Timeout: Duration(2 * time.Second)}
		}, "upstreams[0].active_health.healthy_threshold", "must be >= 1"},
		{"active expected status class", func(c *Config) {
			c.Upstreams[0].ActiveHealth = &ActiveHealth{ExpectedStatuses: []string{"2xx"}, Interval: Duration(10 * time.Second), Timeout: Duration(2 * time.Second)}
		}, "upstreams[0].active_health.expected_statuses[0]", "invalid status range"},
		{"dns refresh interval below one second", func(c *Config) {
			c.Upstreams[0].Hosts = nil
			c.Upstreams[0].DNS = &DNS{Hostname: "x.internal", Port: 80, RefreshInterval: Duration(500 * time.Millisecond)}
		}, "upstreams[0].dns.refresh_interval", "at least 1s"},
		{"negative host weight", func(c *Config) {
			c.Upstreams[0].Hosts[0].Weight = intPtr(-1)
		}, "upstreams[0].hosts[0].weight", "must not be negative"},
		{"bounded load factor below one", func(c *Config) {
			c.Upstreams[0].LoadBalancer = LoadBalancer{Algorithm: "consistent_hash", Hash: Hash{Source: "ip"}, BoundedLoadFactor: 0.5}
		}, "upstreams[0].load_balancer.bounded_load_factor", "must be >= 1"},
		{"negative virtual nodes", func(c *Config) {
			c.Upstreams[0].LoadBalancer = LoadBalancer{Algorithm: "consistent_hash", Hash: Hash{Source: "ip"}, BoundedLoadFactor: 1.25, VirtualNodes: -1}
		}, "upstreams[0].load_balancer.virtual_nodes", "must be >= 1"},
		{"consistent hash header source without name", func(c *Config) {
			c.Upstreams[0].LoadBalancer = LoadBalancer{Algorithm: "consistent_hash", Hash: Hash{Source: "header"}, BoundedLoadFactor: 1.25, VirtualNodes: 160}
		}, "upstreams[0].load_balancer.hash.name", "is required"},
		{"host url with credentials and invalid port", func(c *Config) {
			c.Upstreams[0].Hosts[0].URL = NewSecretURL("http://user:s3cret@h:bad")
		}, "upstreams[0].hosts[0].url", "invalid URL"},
		{"breaker buckets zero", func(c *Config) {
			c.Upstreams[0].CircuitBreaker.Buckets = intPtr(0)
		}, "upstreams[0].circuit_breaker.buckets", "must be >= 1"},
		{"breaker buckets above 1000", func(c *Config) {
			c.Upstreams[0].CircuitBreaker.Buckets = intPtr(1001)
		}, "upstreams[0].circuit_breaker.buckets", "must be <= 1000"},
		{"breaker window too short for buckets", func(c *Config) {
			c.Upstreams[0].CircuitBreaker.Window = Duration(5 * time.Millisecond)
			c.Upstreams[0].CircuitBreaker.Buckets = intPtr(10)
		}, "upstreams[0].circuit_breaker.buckets", "at least 1ms"},
		{"breaker failure ratio zero", func(c *Config) {
			c.Upstreams[0].CircuitBreaker.FailureRatio = floatPtr(0)
		}, "upstreams[0].circuit_breaker.failure_ratio", "must be in (0, 1]"},
		{"breaker failure ratio above one", func(c *Config) {
			c.Upstreams[0].CircuitBreaker.FailureRatio = floatPtr(1.5)
		}, "upstreams[0].circuit_breaker.failure_ratio", "must be in (0, 1]"},
		{"breaker negative consecutive failures", func(c *Config) {
			c.Upstreams[0].CircuitBreaker.ConsecutiveFailures = intPtr(-1)
		}, "upstreams[0].circuit_breaker.consecutive_failures", "must be >= 0"},
		{"breaker negative open timeout", func(c *Config) {
			c.Upstreams[0].CircuitBreaker.OpenTimeout = Duration(-time.Second)
		}, "upstreams[0].circuit_breaker.open_timeout", "must be positive"},
		{"breaker negative window in go-built config", func(c *Config) {
			c.Upstreams[0].CircuitBreaker.Window = Duration(-1)
		}, "upstreams[0].circuit_breaker.window", "must be positive"},
		{"breaker half open probes below success threshold", func(c *Config) {
			c.Upstreams[0].CircuitBreaker.SuccessThreshold = intPtr(2)
			c.Upstreams[0].CircuitBreaker.HalfOpenProbes = intPtr(1)
		}, "upstreams[0].circuit_breaker.half_open_probes", "must be >= success_threshold"},
		{"retry attempts zero", func(c *Config) {
			c.Routes[0].Retry = &Retry{Attempts: intPtr(0)}
		}, "routes[0].retry.attempts", "must be between 1 and 10"},
		{"retry attempts above ten", func(c *Config) {
			c.Routes[0].Retry = &Retry{Attempts: intPtr(11)}
		}, "routes[0].retry.attempts", "must be between 1 and 10"},
		{"retry max delay below base delay", func(c *Config) {
			c.Routes[0].Retry = &Retry{Attempts: intPtr(3), BaseDelay: Duration(300 * time.Millisecond), MaxDelay: Duration(250 * time.Millisecond)}
		}, "routes[0].retry.max_delay", "must be >= base_delay"},
		{"retry budget ratio above one", func(c *Config) {
			c.Routes[0].Retry = &Retry{Attempts: intPtr(3), Budget: RetryBudget{Ratio: floatPtr(1.5)}}
		}, "routes[0].retry.budget.ratio", "must be in [0, 1]"},
		{"retry budget ratio negative", func(c *Config) {
			c.Routes[0].Retry = &Retry{Attempts: intPtr(3), Budget: RetryBudget{Ratio: floatPtr(-0.1)}}
		}, "routes[0].retry.budget.ratio", "must be in [0, 1]"},
		{"retry budget min per second negative", func(c *Config) {
			c.Routes[0].Retry = &Retry{Attempts: intPtr(3), Budget: RetryBudget{MinPerSecond: floatPtr(-1)}}
		}, "routes[0].retry.budget.min_per_second", "must be a finite number"},
		{"retry lowercase method", func(c *Config) {
			c.Routes[0].Retry = &Retry{Attempts: intPtr(3), Methods: []string{"get"}}
		}, "routes[0].retry.methods[0]", "invalid method"},
		{"retry on_statuses below 400", func(c *Config) {
			c.Routes[0].Retry = &Retry{Attempts: intPtr(3), OnStatuses: []int{200}}
		}, "routes[0].retry.on_statuses[0]", "must be between 400 and 599"},
		{"retry on_statuses above 599", func(c *Config) {
			c.Routes[0].Retry = &Retry{Attempts: intPtr(3), OnStatuses: []int{600}}
		}, "routes[0].retry.on_statuses[0]", "must be between 400 and 599"},
		{"retry negative max body bytes", func(c *Config) {
			c.Routes[0].Retry = &Retry{Attempts: intPtr(3), MaxBodyBytes: int64Ptr(-1)}
		}, "routes[0].retry.max_body_bytes", "must not be negative"},
		{"retry before circuit breaker in pipeline", func(c *Config) {
			c.Routes[0].Pipeline = []string{"retry", "circuit_breaker"}
		}, "routes[0].pipeline[0]", "must be the last stage"},
		{"strip prefix with dot segment", func(c *Config) {
			c.Routes[0].Rewrite = Rewrite{StripPrefix: "/a/../b"}
		}, "routes[0].rewrite.strip_prefix", "canonical path"},
		{"strip prefix with empty segment", func(c *Config) {
			c.Routes[0].Rewrite = Rewrite{StripPrefix: "/a//b"}
		}, "routes[0].rewrite.strip_prefix", "canonical path"},
		{"strip prefix with unneeded escape", func(c *Config) {
			c.Routes[0].Rewrite = Rewrite{StripPrefix: "/a%7Eb"}
		}, "routes[0].rewrite.strip_prefix", "canonical path"},
		{"strip prefix with escaped slash", func(c *Config) {
			c.Routes[0].Rewrite = Rewrite{StripPrefix: "/a%2Fb"}
		}, "routes[0].rewrite.strip_prefix", "canonical path"},
		{"replace from with escaped dot segment", func(c *Config) {
			c.Routes[0].Rewrite = Rewrite{ReplacePrefix: ReplacePrefix{From: "/v1/%2e%2e", To: "/v2"}}
		}, "routes[0].rewrite.replace_prefix.from", "canonical path"},
		{"replace to with backslash", func(c *Config) {
			c.Routes[0].Rewrite = Rewrite{ReplacePrefix: ReplacePrefix{From: "/v1", To: "/v2\\x"}}
		}, "routes[0].rewrite.replace_prefix.to", "canonical path"},
		{"replace to with dot segment", func(c *Config) {
			c.Routes[0].Rewrite = Rewrite{ReplacePrefix: ReplacePrefix{From: "/v1", To: "/v2/./x"}}
		}, "routes[0].rewrite.replace_prefix.to", "canonical path"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base()
			tc.mutate(c)
			err := c.Validate()
			found := false
			for _, fe := range fieldErrors(t, err) {
				if fe.Path == tc.path && strings.Contains(fe.Msg, tc.substr) {
					found = true
				}
			}
			if !found {
				t.Errorf("no error at %s containing %q; got %v", tc.path, tc.substr, err)
			}
		})
	}
}

func TestBaseValid(t *testing.T) {
	if err := base().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestRewritePrefixCanonicalAccepted(t *testing.T) {
	cases := []struct {
		name    string
		rewrite Rewrite
	}{
		{"strip root", Rewrite{StripPrefix: "/"}},
		{"strip without slash", Rewrite{StripPrefix: "/api"}},
		{"strip with trailing slash", Rewrite{StripPrefix: "/api/"}},
		{"strip with semicolon", Rewrite{StripPrefix: "/a;b"}},
		{"strip with escaped semicolon", Rewrite{StripPrefix: "/a%3Bb"}},
		{"strip with escaped non-ASCII", Rewrite{StripPrefix: "/caf%C3%B6"}},
		{"replace with trailing slash target", Rewrite{ReplacePrefix: ReplacePrefix{From: "/v1", To: "/api/v2/"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base()
			c.Routes[0].Rewrite = tc.rewrite
			if err := c.Validate(); err != nil {
				t.Errorf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestUnregisteredTypeAccepted(t *testing.T) {
	c := base()
	c.Middlewares = append(c.Middlewares, Middleware{Name: "x", Type: "my_custom", Params: NewSecretParams(map[string]any{"anything": 1})})
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestAllErrorsInOnePass(t *testing.T) {
	c := base()
	c.Version = 2
	c.Global.DrainTimeout = Duration(-time.Second)
	c.Listeners = nil
	err := c.Validate()
	if n := len(fieldErrors(t, err)); n < 3 {
		t.Fatalf("got %d errors, want at least 3: %v", n, err)
	}
}

func TestExplicitZeroHealthAccepted(t *testing.T) {
	c := base()
	c.Upstreams[0].PassiveHealth = &PassiveHealth{MaxEjectionPercent: intPtr(0)}
	c.Upstreams[0].ActiveHealth = &ActiveHealth{Jitter: floatPtr(0), Interval: Duration(10 * time.Second), Timeout: Duration(2 * time.Second)}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestUpstreamErrorsAllAtOnce(t *testing.T) {
	src := `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: a
    hosts:
      - url: "http://a.internal:9001"
        weight: -1
    load_balancer:
      algorithm: bogus
    passive_health:
      max_ejection_percent: 120
    active_health:
      interval: 1s
      timeout: 2s
  - name: b
    hosts:
      - url: "http://b.internal:9001"
    dns:
      hostname: b.internal
      port: 9001
    load_balancer:
      algorithm: consistent_hash
  - name: c
    hosts: []
    load_balancer:
      algorithm: consistent_hash
      hash:
        source: ip
      bounded_load_factor: 0.5
      virtual_nodes: -1
`
	_, err := Parse([]byte(src))
	errs := fieldErrors(t, err)
	want := []string{
		"upstreams[0].hosts[0].weight",
		"upstreams[0].load_balancer.algorithm",
		"upstreams[0].passive_health.max_ejection_percent",
		"upstreams[0].active_health.timeout",
		"upstreams[1]",
		"upstreams[1].load_balancer.hash.source",
		"upstreams[2]",
		"upstreams[2].load_balancer.bounded_load_factor",
		"upstreams[2].load_balancer.virtual_nodes",
	}
	for _, w := range want {
		found := false
		for _, fe := range errs {
			if fe.Path == w {
				found = true
				if fe.Line <= 0 {
					t.Errorf("%s has no line number", w)
				}
			}
		}
		if !found {
			t.Errorf("no error at %s; got %v", w, err)
		}
	}
}

func TestHostURLErrorOmitsCredentials(t *testing.T) {
	c := base()
	c.Upstreams[0].Hosts[0].URL = NewSecretURL("http://user:s3cret@h:bad")
	err := c.Validate()
	if err == nil {
		t.Fatal("no error for invalid host URL")
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Errorf("error leaks credential: %v", err)
	}
}

func TestHostURLParseErrorHidesCredentials(t *testing.T) {
	c := base()
	c.Upstreams[0].Hosts[0].URL = NewSecretURL("http://u:p%zz@h:1")
	var ve *ValidationError
	if err := c.Validate(); !errors.As(err, &ve) {
		t.Fatalf("Validate() = %v, want *ValidationError", err)
	}
	var found bool
	for _, fe := range ve.Errors {
		if fe.Path != "upstreams[0].hosts[0].url" {
			continue
		}
		found = true
		if fe.Msg != "invalid URL" {
			t.Errorf("credential URL message = %q, want %q", fe.Msg, "invalid URL")
		}
	}
	if !found {
		t.Fatalf("no error at upstreams[0].hosts[0].url: %v", ve)
	}

	c = base()
	c.Upstreams[0].Hosts[0].URL = NewSecretURL("http://h:1/%zz")
	ve = nil
	if err := c.Validate(); !errors.As(err, &ve) {
		t.Fatalf("Validate() = %v, want *ValidationError", err)
	}
	found = false
	for _, fe := range ve.Errors {
		if fe.Path != "upstreams[0].hosts[0].url" {
			continue
		}
		found = true
		if !strings.HasPrefix(fe.Msg, "invalid URL: ") {
			t.Errorf("plain URL message = %q, want prefix %q", fe.Msg, "invalid URL: ")
		}
	}
	if !found {
		t.Fatalf("no error at upstreams[0].hosts[0].url: %v", ve)
	}
}

func TestResilienceErrorsAllAtOnce(t *testing.T) {
	src := `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
    circuit_breaker:
      window: 5ms
      buckets: 10
      failure_ratio: 1.5
      consecutive_failures: -1
      open_timeout: -1s
      success_threshold: 2
      half_open_probes: 1
routes:
  - name: all
    upstream: users
    pipeline:
      - retry
      - circuit_breaker
    retry:
      attempts: 11
      base_delay: 300ms
      max_delay: 250ms
      on_statuses: [200]
      methods: [get]
      max_body_bytes: -1
      budget:
        ratio: 1.5
        min_per_second: -1
`
	withUnknown := strings.Replace(src, "      budget:\n", "      backoff: 1s\n      budget:\n", 1)
	_, err := Parse([]byte(withUnknown))
	if errs := fieldErrors(t, err); !hasPath(errs, "routes[0].retry.backoff") {
		t.Fatalf("no decode error at routes[0].retry.backoff: %v", err)
	} else {
		for _, fe := range errs {
			if fe.Path == "routes[0].retry.backoff" && fe.Line != lineOf(withUnknown, "backoff: 1s") {
				t.Errorf("unknown key at line %d, want %d", fe.Line, lineOf(withUnknown, "backoff: 1s"))
			}
		}
	}

	_, err = Parse([]byte(src))
	errs := fieldErrors(t, err)
	want := []string{
		"upstreams[0].circuit_breaker.buckets",
		"upstreams[0].circuit_breaker.failure_ratio",
		"upstreams[0].circuit_breaker.consecutive_failures",
		"upstreams[0].circuit_breaker.open_timeout",
		"upstreams[0].circuit_breaker.half_open_probes",
		"routes[0].retry.attempts",
		"routes[0].retry.max_delay",
		"routes[0].retry.budget.ratio",
		"routes[0].retry.budget.min_per_second",
		"routes[0].retry.methods[0]",
		"routes[0].retry.on_statuses[0]",
		"routes[0].retry.max_body_bytes",
		"routes[0].pipeline[0]",
	}
	for _, w := range want {
		found := false
		for _, fe := range errs {
			if fe.Path == w {
				found = true
				if fe.Line <= 0 {
					t.Errorf("%s has no line number", w)
				}
			}
		}
		if !found {
			t.Errorf("no error at %s; got %v", w, err)
		}
	}
}

func TestResilienceExplicitZeroRejected(t *testing.T) {
	src := `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
    circuit_breaker:
      buckets: 0
      min_requests: 0
      failure_ratio: 0
      half_open_probes: 0
      success_threshold: 0
routes:
  - name: all
    upstream: users
    retry:
      attempts: 0
`
	_, err := Parse([]byte(src))
	errs := fieldErrors(t, err)
	want := []struct {
		path string
		msg  string
		key  string
	}{
		{"upstreams[0].circuit_breaker.buckets", "must be >= 1", "buckets: 0"},
		{"upstreams[0].circuit_breaker.min_requests", "must be >= 1", "min_requests: 0"},
		{"upstreams[0].circuit_breaker.failure_ratio", "must be in (0, 1]", "failure_ratio: 0"},
		{"upstreams[0].circuit_breaker.half_open_probes", "must be >= 1", "half_open_probes: 0"},
		{"upstreams[0].circuit_breaker.success_threshold", "must be >= 1", "success_threshold: 0"},
		{"routes[0].retry.attempts", "must be between 1 and 10", "attempts: 0"},
	}
	if len(errs) != len(want) {
		t.Fatalf("got %d errors, want %d: %v", len(errs), len(want), err)
	}
	for _, fe := range errs {
		known := false
		for _, w := range want {
			if fe.Path == w.path {
				known = true
			}
		}
		if !known {
			t.Errorf("unexpected error at %s: %s", fe.Path, fe.Msg)
		}
	}
	for _, w := range want {
		found := false
		for _, fe := range errs {
			if fe.Path == w.path {
				found = true
				if fe.Msg != w.msg {
					t.Errorf("%s message %q, want %q", w.path, fe.Msg, w.msg)
				}
				if fe.Line != lineOf(src, w.key) {
					t.Errorf("%s at line %d, want %d", w.path, fe.Line, lineOf(src, w.key))
				}
			}
		}
		if !found {
			t.Errorf("no error at %s; got %v", w.path, err)
		}
	}
}

const lineDoc = `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
routes:
  - name: users-api
    upstream: users
    rate_limit:
      algorithm: token_bucket
      rate: 10
`

func exactLine(doc, trimmed string) int {
	for i, l := range strings.Split(doc, "\n") {
		if strings.TrimSpace(l) == trimmed {
			return i + 1
		}
	}
	return 0
}

func TestValidateAnnotatesLines(t *testing.T) {
	c, err := Parse([]byte(lineDoc))
	if err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	c.Routes[0].RateLimit.Store = "nowhere"
	c.Routes[0].RateLimit.Rate = &zero

	wantStore := exactLine(lineDoc, "rate_limit:")
	wantRate := exactLine(lineDoc, "rate: 10")
	if wantStore == 0 || wantRate == 0 {
		t.Fatalf("lines not found in document: %d %d", wantStore, wantRate)
	}

	check := func(name string, cfg *Config, wantStoreLine, wantRateLine int) {
		t.Helper()
		var ve *ValidationError
		if err := cfg.Validate(); !errors.As(err, &ve) {
			t.Fatalf("%s: Validate() = %v, want *ValidationError", name, err)
		}
		lines := map[string]int{}
		for _, fe := range ve.Errors {
			lines[fe.Path] = fe.Line
		}
		storeLine, ok := lines["routes[0].rate_limit.store"]
		if !ok {
			t.Fatalf("%s: no error at routes[0].rate_limit.store: %v", name, ve)
		}
		if storeLine != wantStoreLine {
			t.Errorf("%s: store line = %d, want %d", name, storeLine, wantStoreLine)
		}
		rateLine, ok := lines["routes[0].rate_limit.rate"]
		if !ok {
			t.Fatalf("%s: no error at routes[0].rate_limit.rate: %v", name, ve)
		}
		if rateLine != wantRateLine {
			t.Errorf("%s: rate line = %d, want %d", name, rateLine, wantRateLine)
		}
	}

	check("parsed", c.Clone(), wantStore, wantRate)

	g := c.Clone()
	g.lines = nil
	check("go-built", g, 0, 0)
}
