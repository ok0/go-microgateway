package config

import (
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
	params := c.Middlewares[0].Params
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
	if c.Routes[0].RequestHeaders.Set["X-Host"] != "api.example.com" {
		t.Errorf("X-Host = %q", c.Routes[0].RequestHeaders.Set["X-Host"])
	}
}

func TestDefaultLookupIsOSEnv(t *testing.T) {
	t.Setenv("LYCHGATE_TEST_HDR", "X-Req")
	src := "version: 1\nglobal:\n  request_id_header: ${LYCHGATE_TEST_HDR}\n" + minimalBody
	c, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if c.Global.RequestIDHeader != "X-Req" {
		t.Errorf("RequestIDHeader = %q", c.Global.RequestIDHeader)
	}
}
