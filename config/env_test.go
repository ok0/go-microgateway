package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func mapLookup(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

var testEnv = mapLookup(map[string]string{
	"HOST":    "api.example.com",
	"PORT":    "8080",
	"EMPTY":   "",
	"YAMLISH": "x: [1, 2]\nfoo: bar",
})

func TestExpand(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"${HOST}", "api.example.com"},
		{"${MISSING:-def}", "def"},
		{"${EMPTY:-def}", "def"},
		{"$$HOST", "$HOST"},
		{"a${HOST}:${PORT}/b", "aapi.example.com:8080/b"},
		{"${MISSING:-${HOST}}", "api.example.com"},
		{"${MISSING:-${ALSO_MISSING:-x}}", "x"},
		{"cost $5", "cost $5"},
	}
	for _, c := range cases {
		got, problems := expand(c.in, testEnv)
		if len(problems) != 0 || got != c.want {
			t.Errorf("expand(%q) = %q, %v; want %q", c.in, got, problems, c.want)
		}
	}

	problemCases := []struct {
		in, substr string
	}{
		{"${MISSING}", `"MISSING" is not set`},
		{"${HOST", "unterminated"},
		{"${1BAD}", "invalid variable name"},
	}
	for _, c := range problemCases {
		_, problems := expand(c.in, testEnv)
		if len(problems) == 0 || !strings.Contains(problems[0], c.substr) {
			t.Errorf("expand(%q) problems = %v; want one containing %q", c.in, problems, c.substr)
		}
	}
}

func TestParseUnsetVariable(t *testing.T) {
	src := "version: 1\n" + strings.Replace(minimalBody, `url: "http://127.0.0.1:9001"`, `url: "http://${UPSTREAM_HOST}:9000"`, 1)
	_, err := Parse([]byte(src), WithEnvLookup(mapLookup(nil)))
	errs := fieldErrors(t, err)
	for _, fe := range errs {
		if fe.Path == "upstreams[0].hosts[0].url" {
			if fe.Line != lineOf(src, "UPSTREAM_HOST") {
				t.Errorf("line = %d, want %d", fe.Line, lineOf(src, "UPSTREAM_HOST"))
			}
			if !strings.Contains(fe.Msg, "UPSTREAM_HOST") {
				t.Errorf("msg = %q", fe.Msg)
			}
			return
		}
	}
	t.Fatalf("no error at upstreams[0].hosts[0].url: %v", err)
}

func TestSubstitutionKeepsStructure(t *testing.T) {
	src := func(upstream, reqHeader string) string {
		return "version: 1\nglobal:\n  request_id_header: " + reqHeader + "\n" +
			strings.Replace(minimalBody, "upstream: users", "upstream: "+upstream, 1)
	}

	c, err := Parse([]byte(src("users", "${YAMLISH}")), WithEnvLookup(testEnv))
	if err != nil {
		t.Fatal(err)
	}
	if c.Global.RequestIDHeader != "x: [1, 2]\nfoo: bar" {
		t.Errorf("RequestIDHeader = %q", c.Global.RequestIDHeader)
	}

	_, err = Parse([]byte(src("nope", "${YAMLISH}")), WithEnvLookup(testEnv))
	errs := fieldErrors(t, err)
	var got int
	for _, fe := range errs {
		if fe.Path == "routes[0].upstream" && strings.Contains(fe.Msg, "unknown upstream") {
			got = fe.Line
		}
	}
	plain := src("nope", "X-Request-ID")
	_, err = Parse([]byte(plain))
	var want int
	for _, fe := range fieldErrors(t, err) {
		if fe.Path == "routes[0].upstream" && strings.Contains(fe.Msg, "unknown upstream") {
			want = fe.Line
		}
	}
	if got == 0 || got != want {
		t.Errorf("unknown upstream line with substitution = %d, without = %d", got, want)
	}
}

func TestSubstitutionInParamsAndMaps(t *testing.T) {
	src := "version: 1\n" + minimalBody + `middlewares:
  - name: mw
    type: custom
    params:
      origin: "${HOST}"
      nested:
        list:
          - "${PORT}"
`
	src = strings.Replace(src, "    upstream: users\n", "    upstream: users\n    request_headers:\n      set:\n        X-Host: \"${HOST}\"\n", 1)
	c, err := Parse([]byte(src), WithEnvLookup(testEnv))
	if err != nil {
		t.Fatal(err)
	}
	params := c.Middlewares[0].Params.Reveal()
	if params["origin"] != "api.example.com" {
		t.Errorf("origin = %v", params["origin"])
	}
	nested, ok := params["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested = %T", params["nested"])
	}
	list, ok := nested["list"].([]any)
	if !ok || len(list) != 1 || list[0] != "8080" {
		t.Errorf("list = %v", nested["list"])
	}
	if c.Routes[0].RequestHeaders.Set["X-Host"].Reveal() != "api.example.com" {
		t.Errorf("X-Host = %q", c.Routes[0].RequestHeaders.Set["X-Host"].Reveal())
	}
}

func TestDefaultLookupIsOSEnv(t *testing.T) {
	t.Setenv("MICROGATEWAY_TEST_HDR", "X-Req")
	src := "version: 1\nglobal:\n  request_id_header: ${MICROGATEWAY_TEST_HDR}\n" + minimalBody
	c, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if c.Global.RequestIDHeader != "X-Req" {
		t.Errorf("RequestIDHeader = %q", c.Global.RequestIDHeader)
	}
}

func TestEnvSubstitutionTypedAuthSecrets(t *testing.T) {
	const (
		jwtVal  = "0123456789abcdef0123456789abcdef-env"
		hashVal = "sha256:" + "abababababababababababababababababababababababababababababababab"
		pwVal   = "pw-env-LEAK-5a1b"
		qVal    = "q-env-LEAK-6c2d"
	)
	const doc = `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://u:${LG_PW}@h:1"
routes:
  - name: all
    upstream: users
auth:
  jwt:
    - name: hs
      issuer: https://idp.example
      audiences: [api]
      keys:
        - algorithm: HS256
          secret: "${LG_JWT}"
    - name: remote
      issuer: https://idp.example
      audiences: [api]
      jwks:
        url: "https://h/jwks?k=${LG_Q}"
        algorithms: [RS256]
  api_keys:
    - name: keys1
      keys:
        - id: alice
          hash: "${LG_HASH}"
          consumer: alice
`
	lookup := mapLookup(map[string]string{
		"LG_PW":   pwVal,
		"LG_JWT":  jwtVal,
		"LG_Q":    qVal,
		"LG_HASH": hashVal,
	})
	c, err := Parse([]byte(doc), WithEnvLookup(lookup))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Auth.JWT[0].Keys[0].Secret.Reveal(); got != jwtVal {
		t.Errorf("JWT secret = %q, want the substituted value", got)
	}
	if got := c.Auth.APIKeys[0].Keys[0].Hash.Reveal(); got != hashVal {
		t.Errorf("API key hash = %q, want the substituted value", got)
	}
	if got := c.Upstreams[0].Hosts[0].URL.Reveal(); got != "http://u:"+pwVal+"@h:1" {
		t.Errorf("host URL = %q, want the substituted value", got)
	}
	if got := c.Auth.JWT[1].JWKS.URL.Reveal(); got != "https://h/jwks?k="+qVal {
		t.Errorf("JWKS URL = %q, want the substituted value", got)
	}

	b, err := json.Marshal(c.Redacted())
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{jwtVal, hashVal, pwVal, qVal} {
		if strings.Contains(string(b), v) {
			t.Errorf("redacted configuration contains a substituted value")
		}
	}

	missing := mapLookup(map[string]string{
		"LG_PW":   pwVal,
		"LG_Q":    qVal,
		"LG_HASH": hashVal,
	})
	_, err = Parse([]byte(doc), WithEnvLookup(missing))
	if err == nil {
		t.Fatal("no error for unset LG_JWT")
	}
	if !strings.Contains(err.Error(), "LG_JWT") {
		t.Errorf("error does not name the variable: %v", err)
	}
	for _, v := range []string{jwtVal, hashVal, pwVal, qVal} {
		if strings.Contains(err.Error(), v) {
			t.Errorf("error contains a substituted value")
		}
	}
}

func TestEnvSubstitutionHeaderValues(t *testing.T) {
	const doc = "version: 1\n" + minimalBody
	withHeader := func(value string) string {
		return strings.Replace(doc, "    upstream: users\n", "    upstream: users\n    request_headers:\n      set:\n        X-Key: \""+value+"\"\n", 1)
	}
	c, err := Parse([]byte(withHeader("${H}")), WithEnvLookup(mapLookup(map[string]string{"H": "hv-env-LEAK-7d"})))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Routes[0].RequestHeaders.Set["X-Key"].Reveal(); got != "hv-env-LEAK-7d" {
		t.Errorf("X-Key = %q, want the substituted value", got)
	}

	_, err = Parse([]byte(withHeader("${NOPE}")), WithEnvLookup(mapLookup(map[string]string{})))
	var found bool
	for _, fe := range fieldErrors(t, err) {
		if fe.Path == "routes[0].request_headers.set.X-Key" && fe.Msg == `environment variable "NOPE" is not set` {
			found = true
		}
	}
	if !found {
		t.Errorf("no field error for unset NOPE at routes[0].request_headers.set.X-Key: %v", err)
	}
}
