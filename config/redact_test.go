package config

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/leakcheck"
	"go.yaml.in/yaml/v3"
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
	origSecret := c.Auth.JWT[0].Keys[0].Secret.Reveal()
	origHash := c.Auth.APIKeys[0].Keys[0].Hash.Reveal()

	r := c.Redacted()
	for i := range r.Auth.JWT {
		for _, k := range r.Auth.JWT[i].Keys {
			if k.Secret.Reveal() != "" && k.Secret.Reveal() != "REDACTED" {
				t.Errorf("secret not redacted: %q", k.Secret.Reveal())
			}
		}
	}
	for i := range r.Auth.APIKeys {
		for _, k := range r.Auth.APIKeys[i].Keys {
			if k.Hash.Reveal() != "REDACTED" {
				t.Errorf("hash not redacted: %q", k.Hash.Reveal())
			}
		}
	}
	if c.Auth.JWT[0].Keys[0].Secret.Reveal() != origSecret || c.Auth.APIKeys[0].Keys[0].Hash.Reveal() != origHash {
		t.Error("original configuration was modified")
	}
	if r == c {
		t.Error("Redacted returned the same pointer")
	}

	r.Routes[0].Match.Headers["X-Tenant"] = NewSecret("changed")
	r.Routes[0].Match.Hosts[0] = "changed"
	if c.Routes[0].Match.Headers["X-Tenant"].Reveal() != "acme" {
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
			p := c.Middlewares[0].Params.Reveal()
			p["max_age"] = 601
			c.Middlewares[0].Params = NewSecretParams(p)
		},
		"jwt secret":    func(c *Config) { c.Auth.JWT[0].Keys[0].Secret = NewSecret("other-secret") },
		"route timeout": func(c *Config) { c.Routes[0].Timeout = Duration(time.Second) },
		"leeway nil": func(c *Config) {
			c.Auth.JWT[0].Leeway = nil
		},
		"leeway zero": func(c *Config) {
			z := Duration(0)
			c.Auth.JWT[0].Leeway = &z
		},
		"max_token_bytes": func(c *Config) { *c.Auth.JWT[0].MaxTokenBytes++ },
		"jwt forward_credentials": func(c *Config) {
			v := true
			c.Auth.JWT[0].ForwardCredentials = &v
		},
		"jwks.algorithms": func(c *Config) { c.Auth.JWT[1].JWKS.Algorithms = []string{"RS256"} },
		"jwks.ttl": func(c *Config) {
			d := Duration(time.Hour)
			c.Auth.JWT[1].JWKS.TTL = &d
		},
		"jwks.min_refresh_interval": func(c *Config) {
			d := Duration(time.Hour)
			c.Auth.JWT[1].JWKS.MinRefreshInterval = &d
		},
		"jwks.timeout": func(c *Config) {
			d := Duration(time.Hour)
			c.Auth.JWT[1].JWKS.Timeout = &d
		},
		"api header nil": func(c *Config) { c.Auth.APIKeys[0].Header = nil },
		"api header empty": func(c *Config) {
			s := ""
			c.Auth.APIKeys[0].Header = &s
		},
		"api forward_credentials": func(c *Config) {
			v := true
			c.Auth.APIKeys[0].ForwardCredentials = &v
		},
		"file keys entry": func(c *Config) {
			c.Auth.APIKeys[0].FileKeys = []APIKey{{ID: "file-1", Hash: NewSecret("sha256:" + strings.Repeat("a", 64)), Consumer: "file"}}
		},
		"loaded public key":   func(c *Config) { c.Auth.JWT[1].Keys[0].LoadedPublicKey = "pem" },
		"key scopes":          func(c *Config) { c.Auth.APIKeys[0].Keys[0].Scopes = []string{"admin"} },
		"key roles":           func(c *Config) { c.Auth.APIKeys[0].Keys[0].Roles = []string{"admin"} },
		"require claim 1":     func(c *Config) { c.Routes[0].Auth.Require.Claims["tier"] = 1 },
		"require claim \"1\"": func(c *Config) { c.Routes[0].Auth.Require.Claims["tier"] = "1" },
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
	c.Upstreams[0].Hosts[0].URL = NewSecretURL("http://user:s3cret@h:80")
	c.Auth.JWT[1].JWKS = &JWKS{URL: NewSecretURL("https://u:pw@idp/keys?token=s3cret"), Algorithms: []string{"RS256"}}
	before := c.Fingerprint()

	r := c.Redacted()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"s3cret", "pw@"} {
		if strings.Contains(string(b), secret) {
			t.Errorf("redacted configuration still contains %q: %s", secret, b)
		}
	}
	if c.Upstreams[0].Hosts[0].URL.Reveal() != "http://user:s3cret@h:80" || c.Auth.JWT[1].JWKS.URL.Reveal() != "https://u:pw@idp/keys?token=s3cret" {
		t.Error("original configuration was modified")
	}
	if c.Fingerprint() != before {
		t.Error("original fingerprint changed")
	}
}

func TestRedactedDropsLoadedKeyMaterial(t *testing.T) {
	c := parseFull(t)
	c.Auth.JWT[1].Keys[0].LoadedPublicKey = "-----BEGIN PUBLIC KEY-----\nloaded-pem-body\n"
	c.Auth.APIKeys[0].FileKeys = []APIKey{{ID: "file-1", Hash: NewSecret("sha256:" + strings.Repeat("c", 64)), Consumer: "filer"}}

	r := c.Redacted()
	if got := r.Auth.JWT[1].Keys[0].LoadedPublicKey; got != "" {
		t.Errorf("redacted LoadedPublicKey = %q, want empty", got)
	}
	if r.Auth.APIKeys[0].FileKeys != nil {
		t.Errorf("redacted FileKeys = %+v, want nil", r.Auth.APIKeys[0].FileKeys)
	}
	if c.Auth.JWT[1].Keys[0].LoadedPublicKey == "" || len(c.Auth.APIKeys[0].FileKeys) != 1 {
		t.Error("original key material was modified")
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"loaded-pem-body", strings.Repeat("c", 64), "9771189e33ad49531f98873eb4e4458ce1c96f313fe15c6d753f4c7b6ad4eba5", "file-1"} {
		if strings.Contains(string(b), secret) {
			t.Errorf("redacted configuration contains %q", secret)
		}
	}
}

func typedSecretBase(t *testing.T) *Config {
	t.Helper()
	c := parseFull(t)
	c.Upstreams[0].Hosts[0].URL = NewSecretURL("http://user:pw-a@127.0.0.1:9001")
	c.Auth.JWT[1].JWKS.URL = NewSecretURL("https://sso.example.com/.well-known/jwks.json?token=q-a")
	c.Auth.APIKeys[0].FileKeys = []APIKey{{ID: "file-1", Hash: NewSecret("sha256:" + strings.Repeat("a", 64)), Consumer: "file"}}
	return c
}

func TestFingerprintCoversTypedSecrets(t *testing.T) {
	base := typedSecretBase(t).Fingerprint()
	edits := map[string]func(*Config){
		"jwt key secret": func(c *Config) { c.Auth.JWT[0].Keys[0].Secret = NewSecret("rotated-jwt-secret") },
		"inline api key hash": func(c *Config) {
			c.Auth.APIKeys[0].Keys[0].Hash = NewSecret("sha256:" + strings.Repeat("c", 64))
		},
		"file key hash": func(c *Config) {
			c.Auth.APIKeys[0].FileKeys[0].Hash = NewSecret("sha256:" + strings.Repeat("b", 64))
		},
		"host url password": func(c *Config) {
			c.Upstreams[0].Hosts[0].URL = NewSecretURL("http://user:pw-b@127.0.0.1:9001")
		},
		"jwks url query value": func(c *Config) {
			c.Auth.JWT[1].JWKS.URL = NewSecretURL("https://sso.example.com/.well-known/jwks.json?token=q-b")
		},
		"redis password": func(c *Config) {
			c.Stores[0].Redis.Password = NewSecret("rotated-redis-pw")
		},
		"params leaf": func(c *Config) {
			p := c.Stores[1].Params.Reveal()
			p["nested"].(map[string]any)["token"] = "rotated-token"
			c.Stores[1].Params = NewSecretParams(p)
		},
	}
	for name, edit := range edits {
		c := typedSecretBase(t)
		edit(c)
		if c.Fingerprint() == base {
			t.Errorf("%s edit did not change the fingerprint", name)
		}
	}

	c := typedSecretBase(t)
	if c.Clone().Fingerprint() != base {
		t.Error("identical clone has a different fingerprint")
	}

	a := FingerprintOf(Upstream{Name: "u", Hosts: []Host{{URL: NewSecretURL("http://user:pw-a@h:1")}}})
	b := FingerprintOf(Upstream{Name: "u", Hosts: []Host{{URL: NewSecretURL("http://user:pw-b@h:1")}}})
	if a == b {
		t.Error("upstreams differing only in host URL password have equal FingerprintOf")
	}
}

func TestRedactedTypedSecrets(t *testing.T) {
	const (
		jwtSecret = "jwt-LEAK-3c9e"
		urlPW     = "urlpw-LEAK-41ab"
		urlQuery  = "urlq-LEAK-9d02"
	)
	hashHex := strings.Repeat("7f", 32)

	c := typedSecretBase(t)
	c.Auth.JWT[0].Keys[0].Secret = NewSecret(jwtSecret)
	c.Auth.APIKeys[0].Keys[0].Hash = NewSecret("sha256:" + hashHex)
	c.Upstreams[0].Hosts[0].URL = NewSecretURL("http://user:" + urlPW + "@127.0.0.1:9001")
	c.Auth.JWT[1].JWKS.URL = NewSecretURL("https://sso.example.com/.well-known/jwks.json?token=" + urlQuery)
	leaks := []string{jwtSecret, hashHex, urlPW, urlQuery}

	b, err := json.Marshal(c.Redacted())
	if err != nil {
		t.Fatal(err)
	}
	for _, leak := range leaks {
		if strings.Contains(string(b), leak) {
			t.Errorf("redacted JSON contains a secret value")
		}
	}
	formatted := fmt.Sprintf("%+v", *c)
	for _, leak := range leaks {
		if strings.Contains(formatted, leak) {
			t.Errorf("formatted configuration contains a secret value")
		}
	}

	if c.Auth.JWT[0].Keys[0].Secret.Reveal() != jwtSecret {
		t.Error("original JWT secret was modified")
	}
	if c.Auth.APIKeys[0].Keys[0].Hash.Reveal() != "sha256:"+hashHex {
		t.Error("original hash was modified")
	}
	if c.Upstreams[0].Hosts[0].URL.Reveal() != "http://user:"+urlPW+"@127.0.0.1:9001" {
		t.Error("original host URL was modified")
	}
	if c.Auth.JWT[1].JWKS.URL.Reveal() != "https://sso.example.com/.well-known/jwks.json?token="+urlQuery {
		t.Error("original JWKS URL was modified")
	}
}

const headerSecretDoc = `version: 1
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
    match:
      headers:
        X-Internal: mh-LEAK-5d0
    request_headers:
      add:
        X-Add: ra-base
      set:
        X-Api-Key: "${UPSTREAM_KEY}"
    response_headers:
      add:
        X-Lit: rl-LEAK-9e2
      set:
        X-Set: rs-base
middlewares:
  - name: hdr
    type: headers
    params:
      request:
        set:
          Authorization: "Bearer ${TOKEN}"
  - name: usr
    type: custom
    params:
      token: "${MW_SECRET}"
      nested:
        list:
          - nl-LEAK-6f1
`

func parseHeaderSecrets(t *testing.T) *Config {
	t.Helper()
	c, err := Parse([]byte(headerSecretDoc), WithEnvLookup(mapLookup(map[string]string{
		"UPSTREAM_KEY": "uk-LEAK-31a",
		"TOKEN":        "tk-LEAK-82b",
		"MW_SECRET":    "mw-LEAK-47c",
	})))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestRedactedHeaderValuesAndParams(t *testing.T) {
	const (
		upKey    = "uk-LEAK-31a"
		token    = "tk-LEAK-82b"
		mwSecret = "mw-LEAK-47c"
		matchH   = "mh-LEAK-5d0"
		respH    = "rl-LEAK-9e2"
		nested   = "nl-LEAK-6f1"
	)
	leaks := []string{upKey, "Bearer " + token, token, mwSecret, matchH, respH, nested}

	c := parseHeaderSecrets(t)
	if got := c.Routes[0].RequestHeaders.Set["X-Api-Key"].Reveal(); got != upKey {
		t.Errorf("X-Api-Key = %q, want the substituted value", got)
	}
	hdrParams := c.Middlewares[0].Params.Reveal()["request"].(map[string]any)["set"].(map[string]any)
	if got := hdrParams["Authorization"]; got != "Bearer "+token {
		t.Errorf("Authorization param = %v, want the substituted value", got)
	}
	if got := c.Middlewares[1].Params.Reveal()["token"]; got != mwSecret {
		t.Errorf("token param = %v, want the substituted value", got)
	}

	r := c.Redacted()
	jb, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	yb, err := yaml.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, out := range []string{string(jb), string(yb)} {
		for _, want := range []string{"X-Api-Key", "Authorization", "token", "REDACTED"} {
			if !strings.Contains(out, want) {
				t.Errorf("redacted encoding does not contain %q", want)
			}
		}
		for _, leak := range leaks {
			for _, form := range leakcheck.Forms(leak) {
				if strings.Contains(out, form) {
					t.Errorf("redacted encoding contains secret form %q", form)
				}
			}
		}
	}
	leakcheck.Check(t, "config", c, leaks...)
	leakcheck.Check(t, "redacted config", r, leaks...)
	if got := r.Routes[0].RequestHeaders.Set["X-Api-Key"].Reveal(); got != "REDACTED" {
		t.Errorf("redacted X-Api-Key = %q, want REDACTED", got)
	}
}

func TestFingerprintCoversHeaderValuesAndParams(t *testing.T) {
	base := parseHeaderSecrets(t)
	want := base.Fingerprint()
	if got := base.Clone().Fingerprint(); got != want {
		t.Fatal("unchanged clone has a different fingerprint")
	}

	edits := map[string]func(*Config){
		"match header": func(c *Config) {
			c.Routes[0].Match.Headers["X-Internal"] = NewSecret("mh-changed")
		},
		"request add": func(c *Config) {
			c.Routes[0].RequestHeaders.Add["X-Add"] = NewSecret("ra-changed")
		},
		"request set": func(c *Config) {
			c.Routes[0].RequestHeaders.Set["X-Api-Key"] = NewSecret("uk-changed")
		},
		"response add": func(c *Config) {
			c.Routes[0].ResponseHeaders.Add["X-Lit"] = NewSecret("rl-changed")
		},
		"response set": func(c *Config) {
			c.Routes[0].ResponseHeaders.Set["X-Set"] = NewSecret("rs-changed")
		},
		"headers middleware param": func(c *Config) {
			p := c.Middlewares[0].Params.Reveal()
			p["request"].(map[string]any)["set"].(map[string]any)["Authorization"] = "Bearer changed"
			c.Middlewares[0].Params = NewSecretParams(p)
		},
		"user middleware param": func(c *Config) {
			p := c.Middlewares[1].Params.Reveal()
			p["token"] = "changed"
			c.Middlewares[1].Params = NewSecretParams(p)
		},
	}
	for name, edit := range edits {
		t.Run(name, func(t *testing.T) {
			c := base.Clone()
			edit(c)
			if c.Fingerprint() == want {
				t.Errorf("%s edit did not change the fingerprint", name)
			}
		})
	}
}
