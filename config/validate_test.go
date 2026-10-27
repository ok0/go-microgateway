package config

import (
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
          secret: example-secret
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
			c.Routes[0].RateLimit = &RateLimit{Algorithm: "token_bucket", Rate: 0}
		}, "routes[0].rate_limit.rate", "must be a positive finite number"},
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
			c.Routes[0].RateLimit = &RateLimit{Rate: 1, Key: RateLimitKey{Source: "bogus"}}
		}, "routes[0].rate_limit.key.source", "must be one of"},
		{"invalid hash source with consistent_hash", func(c *Config) {
			c.Upstreams[0].LoadBalancer = LoadBalancer{Algorithm: "consistent_hash", Hash: Hash{Source: "bogus"}}
		}, "upstreams[0].load_balancer.hash.source", "must be one of"},
		{"host URL scheme", func(c *Config) {
			c.Upstreams[0].Hosts[0].URL = "ftp://x"
		}, "upstreams[0].hosts[0].url", "scheme"},
		{"jwks url", func(c *Config) {
			c.Auth.JWT[0].JWKS = &JWKS{URL: "not a url"}
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
			c.Upstreams[0].CircuitBreaker.HalfOpenProbes = 1
		}, "upstreams[0].circuit_breaker.half_open_probes", "must be >= success_threshold"},
		{"jwt leeway above 5m", func(c *Config) {
			c.Auth.JWT[0].Leeway = Duration(6 * time.Minute)
		}, "auth.jwt[0].leeway", "must not exceed 5m"},
		{"HS256 key with public_key instead of secret", func(c *Config) {
			c.Auth.JWT[0].Keys[0].Secret = ""
			c.Auth.JWT[0].Keys[0].PublicKey = "pem"
		}, "auth.jwt[0].keys[0].secret", "requires a secret"},
		{"RS256 key with secret", func(c *Config) {
			c.Auth.JWT[0].Keys[0].Algorithm = "RS256"
		}, "auth.jwt[0].keys[0].algorithm", "requires public_key or public_key_file"},
		{"retry not last in pipeline", func(c *Config) {
			c.Routes[0].Pipeline = []string{"retry", "auth"}
		}, "routes[0].pipeline[0]", `"retry" must be the last stage`},
		{"api key hash form", func(c *Config) {
			c.Auth.APIKeys[0].Keys[0].Hash = "sha256:abc"
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
			c.Middlewares[0].Params = map[string]any{"max_age": -1}
		}, "middlewares[0].params.max_age", "must not be negative"},
		{"unknown builtin param", func(c *Config) {
			c.Middlewares[0].Params = map[string]any{"origin": "x"}
		}, "middlewares[0].params.origin", "unknown field"},
		{"request_size_limit missing max_bytes", func(c *Config) {
			c.Middlewares = append(c.Middlewares, Middleware{Name: "lim", Type: "request_size_limit"})
		}, "middlewares[1].params.max_bytes", "must be positive"},
		{"duplicate pipeline stage", func(c *Config) {
			c.Routes[0].Pipeline = []string{"timeout", "timeout"}
		}, "routes[0].pipeline[1]", "duplicate"},
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

func TestUnregisteredTypeAccepted(t *testing.T) {
	c := base()
	c.Middlewares = append(c.Middlewares, Middleware{Name: "x", Type: "my_custom", Params: map[string]any{"anything": 1}})
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
