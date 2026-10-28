package config

import (
	"strings"
	"testing"
	"time"
)

const cloneDoc = `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
        weight: 3
routes:
  - name: first
    upstream: users
    auth:
      mode: jwt
      providers: [jwt1]
      require:
        claims:
          x: 1
  - name: second
    upstream: users
    match:
      path: /second
middlewares:
  - name: tag
    type: custom
    params:
      k: v
      list: [1, 2]
auth:
  jwt:
    - name: jwt1
      issuer: https://idp.example
      audiences: [api]
      leeway: 10s
      max_token_bytes: 2048
      jwks:
        url: https://idp.example/jwks
        algorithms: [RS256]
        ttl: 5m
  api_keys:
    - name: keys1
      forward_credentials: true
      keys:
        - id: alice
          hash: "sha256:9771189e33ad49531f98873eb4e4458ce1c96f313fe15c6d753f4c7b6ad4eba5"
          consumer: alice
          scopes: [read]
`

func parseClone(t *testing.T) *Config {
	t.Helper()
	c, err := Parse([]byte(cloneDoc))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCloneIndependent(t *testing.T) {
	c := parseClone(t)
	orig := c.Fingerprint()

	c2 := c.Clone()
	if c2.Fingerprint() != orig {
		t.Fatalf("clone fingerprint %q differs from original %q", c2.Fingerprint(), orig)
	}

	c2.Routes[0].Name = "changed"
	*c2.Upstreams[0].Hosts[0].Weight = 9
	c2.Middlewares[0].Params["k"] = "x"
	list := c2.Middlewares[0].Params["list"].([]any)
	c2.Middlewares[0].Params["list"] = append(list, 3)

	if got := c.Fingerprint(); got != orig {
		t.Fatalf("original fingerprint changed from %q to %q", orig, got)
	}
	if c.Routes[0].Name != "first" {
		t.Errorf("original route name = %q", c.Routes[0].Name)
	}
	if *c.Upstreams[0].Hosts[0].Weight != 3 {
		t.Errorf("original weight = %d", *c.Upstreams[0].Hosts[0].Weight)
	}
	if c.Middlewares[0].Params["k"] != "v" {
		t.Errorf("original param k = %v", c.Middlewares[0].Params["k"])
	}
	if l := c.Middlewares[0].Params["list"].([]any); len(l) != 2 {
		t.Errorf("original list length = %d", len(l))
	}
}

func TestCloneAuthIndependent(t *testing.T) {
	c := parseClone(t)
	c.Auth.APIKeys[0].FileKeys = []APIKey{{ID: "file-1", Hash: "sha256:" + strings.Repeat("b", 64), Consumer: "filer", Scopes: []string{"read"}}}
	orig := c.Fingerprint()

	c2 := c.Clone()
	if c2.Fingerprint() != orig {
		t.Fatalf("clone fingerprint %q differs from original %q", c2.Fingerprint(), orig)
	}

	*c2.Auth.JWT[0].Leeway = 0
	*c2.Auth.JWT[0].MaxTokenBytes = 1
	*c2.Auth.JWT[0].JWKS.TTL = Duration(time.Minute)
	c2.Auth.JWT[0].JWKS.Algorithms[0] = "ES256"
	*c2.Auth.APIKeys[0].Header = "X-Other"
	*c2.Auth.APIKeys[0].ForwardCredentials = false
	c2.Routes[0].Auth.Require.Claims["x"] = 2
	c2.Auth.APIKeys[0].FileKeys[0].ID = "changed"
	c2.Auth.APIKeys[0].Keys[0].Scopes[0] = "write"

	if got := c.Fingerprint(); got != orig {
		t.Fatalf("original fingerprint changed from %q to %q", orig, got)
	}
	if *c.Auth.JWT[0].Leeway != Duration(10*time.Second) {
		t.Errorf("original leeway = %v", *c.Auth.JWT[0].Leeway)
	}
	if *c.Auth.JWT[0].MaxTokenBytes != 2048 {
		t.Errorf("original max_token_bytes = %d", *c.Auth.JWT[0].MaxTokenBytes)
	}
	if *c.Auth.JWT[0].JWKS.TTL != Duration(5*time.Minute) || c.Auth.JWT[0].JWKS.Algorithms[0] != "RS256" {
		t.Errorf("original jwks = %+v", c.Auth.JWT[0].JWKS)
	}
	if *c.Auth.APIKeys[0].Header != "X-API-Key" || !*c.Auth.APIKeys[0].ForwardCredentials {
		t.Errorf("original api key header or forward_credentials changed")
	}
	if c.Routes[0].Auth.Require.Claims["x"] != 1 {
		t.Errorf("original claim x = %v", c.Routes[0].Auth.Require.Claims["x"])
	}
	if c.Auth.APIKeys[0].FileKeys[0].ID != "file-1" {
		t.Errorf("original file key id = %q", c.Auth.APIKeys[0].FileKeys[0].ID)
	}
	if c.Auth.APIKeys[0].Keys[0].Scopes[0] != "read" {
		t.Errorf("original scope = %q", c.Auth.APIKeys[0].Keys[0].Scopes[0])
	}
}

func TestCloneNil(t *testing.T) {
	if (*Config)(nil).Clone() != nil {
		t.Fatal("nil Clone did not return nil")
	}
}

func TestLine(t *testing.T) {
	c := parseClone(t)
	if got := c.Line("listeners[0].address"); got != 4 {
		t.Errorf("Line(listeners[0].address) = %d, want 4", got)
	}
	if got := c.Clone().Line("listeners[0].address"); got != 4 {
		t.Errorf("clone Line(listeners[0].address) = %d, want 4", got)
	}
	if got := (&Config{}).Line("listeners[0].address"); got != 0 {
		t.Errorf("unparsed Line(listeners[0].address) = %d, want 0", got)
	}
	if c.Redacted() == nil {
		t.Error("Redacted returned nil")
	}
}
