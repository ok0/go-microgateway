package config

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

var hexFingerprint = regexp.MustCompile(`^[0-9a-f]{64}$`)

func parseFull(t *testing.T) *Config {
	t.Helper()
	data, err := os.ReadFile("testdata/full.yaml")
	if err != nil {
		t.Fatal(err)
	}
	c, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestRedacted(t *testing.T) {
	c := parseFull(t)
	origSecret := c.Auth.JWT[0].Keys[0].Secret
	origHash := c.Auth.APIKeys[0].Keys[0].Hash

	r := c.Redacted()
	for i := range r.Auth.JWT {
		for _, k := range r.Auth.JWT[i].Keys {
			if k.Secret != "" && k.Secret != "REDACTED" {
				t.Errorf("secret not redacted: %q", k.Secret)
			}
		}
	}
	for i := range r.Auth.APIKeys {
		for _, k := range r.Auth.APIKeys[i].Keys {
			if k.Hash != "REDACTED" {
				t.Errorf("hash not redacted: %q", k.Hash)
			}
		}
	}
	if c.Auth.JWT[0].Keys[0].Secret != origSecret || c.Auth.APIKeys[0].Keys[0].Hash != origHash {
		t.Error("original configuration was modified")
	}
	if r == c {
		t.Error("Redacted returned the same pointer")
	}

	r.Routes[0].Match.Headers["X-Tenant"] = "changed"
	r.Routes[0].Match.Hosts[0] = "changed"
	if c.Routes[0].Match.Headers["X-Tenant"] != "acme" {
		t.Error("map shared between copy and original")
	}
	if c.Routes[0].Match.Hosts[0] != "api.example.com" {
		t.Error("slice shared between copy and original")
	}
}

func TestFingerprintStable(t *testing.T) {
	a := parseFull(t).Fingerprint()
	b := parseFull(t).Fingerprint()
	if !hexFingerprint.MatchString(a) || a != b {
		t.Fatalf("fingerprints %q and %q", a, b)
	}
	c := parseFull(t)
	if c.Redacted().Fingerprint() == a {
		t.Error("redacted fingerprint equals original")
	}
}

func TestFingerprintChanges(t *testing.T) {
	base := parseFull(t).Fingerprint()
	edits := map[string]func(*Config){
		"route priority": func(c *Config) { c.Routes[0].Priority++ },
		"host weight":    func(c *Config) { *c.Upstreams[0].Hosts[0].Weight++ },
		"retry status":   func(c *Config) { c.Routes[0].Retry.OnStatuses[0] = 599 },
		"middleware param": func(c *Config) {
			c.Middlewares[0].Params["max_age"] = 601
		},
		"jwt secret":    func(c *Config) { c.Auth.JWT[0].Keys[0].Secret = "other-secret" },
		"route timeout": func(c *Config) { c.Routes[0].Timeout = Duration(time.Second) },
	}
	for name, edit := range edits {
		c := parseFull(t)
		edit(c)
		if c.Fingerprint() == base {
			t.Errorf("%s edit did not change the fingerprint", name)
		}
	}
}

func TestRedactURL(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"http://user:pw@h:80/p", "http://REDACTED@h:80/p"},
		{"http://user@h", "http://REDACTED@h"},
		{"http://h:80/a%2Fb", "http://h:80/a%2Fb"},
		{"http://u:p w@h:80/%zz", "http://REDACTED@h:80/%zz"},
		{"u:p@h/x", "REDACTED@h/x"},
		{"", ""},
		{"http://u:p@h:1/x?token=abc&v=1#f", "http://REDACTED@h:1/x?token=REDACTED&v=REDACTED#f"},
		{"http://h:1/?flag", "http://h:1/?flag"},
		{"http://h:1/x", "http://h:1/x"},
	}
	for _, tc := range cases {
		if got := RedactURL(tc.in); got != tc.want {
			t.Errorf("RedactURL(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestRedactURLUnparseableQuery(t *testing.T) {
	got := RedactURL("http://u:p@h:1/%zz?token=abc")
	if strings.Contains(got, "p@") || strings.Contains(got, "abc") {
		t.Errorf("RedactURL left a credential or query value: %q", got)
	}
}

func TestRedactedURLCredentials(t *testing.T) {
	c := parseFull(t)
	c.Upstreams[0].Hosts[0].URL = "http://user:s3cret@h:80"
	c.Auth.JWT[0].JWKS = &JWKS{URL: "https://u:s3cret@idp/keys"}
	before := c.Fingerprint()

	r := c.Redacted()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "s3cret") {
		t.Errorf("redacted configuration still contains the credential: %s", b)
	}
	if c.Upstreams[0].Hosts[0].URL != "http://user:s3cret@h:80" || c.Auth.JWT[0].JWKS.URL != "https://u:s3cret@idp/keys" {
		t.Error("original configuration was modified")
	}
	if c.Fingerprint() != before {
		t.Error("original fingerprint changed")
	}
}
