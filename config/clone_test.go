package config

import (
	"testing"
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
