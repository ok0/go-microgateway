package cli

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/store"
)

func validateLines(t *testing.T, opts []microgateway.Option, path string) []string {
	t.Helper()
	code, out, errs := runWith(t, opts, "validate", "-config", path)
	if code != 1 {
		t.Fatalf("validate exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	if out != "" {
		t.Fatalf("validate stdout = %q, want empty", out)
	}
	lines := strings.Split(strings.TrimSuffix(errs, "\n"), "\n")
	last := fmt.Sprintf("%s: %d error(s)", path, len(lines)-1)
	if lines[len(lines)-1] != last {
		t.Fatalf("validate last line = %q, want %q", lines[len(lines)-1], last)
	}
	body := slices.Clone(lines[:len(lines)-1])
	slices.Sort(body)
	return body
}

func reloadLines(t *testing.T, opts []microgateway.Option, path string) []string {
	t.Helper()
	g, err := microgateway.NewFromYAML([]byte(minimalConfig), opts...)
	if err != nil {
		t.Fatalf("NewFromYAML(minimalConfig): %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	err = g.ReloadFile(path)
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("ReloadFile(%s) error = %v, want *config.ValidationError", path, err)
	}
	out := make([]string, 0, len(ve.Errors))
	for _, fe := range ve.Errors {
		out = append(out, fe.String())
	}
	slices.Sort(out)
	return out
}

func TestReloadFileMatchesValidate(t *testing.T) {
	passthrough := func(microgateway.MiddlewareParams) (microgateway.Middleware, error) {
		return func(h http.Handler) http.Handler { return h }, nil
	}
	cases := []struct {
		name   string
		doc    string
		opts   []microgateway.Option
		count  int
		absent []string
	}{
		{
			name: "four problems",
			doc: `version: 1
global:
  drain_timeout: soon
listeners:
  - name: public
    address: "127.0.0.1:0"
upstreams:
  - name: up
    hosts:
      - url: "http://127.0.0.1:9001"
stores:
  - name: shared
    type: nosuch
middlewares:
  - name: tag
    type: mystery
routes:
  - name: limited
    match:
      path: /limited
    upstream: up
    rate_limit:
      rate: 5
      per: 1s
      store: missing
`,
			count: 4,
		},
		{
			name: "mixed",
			doc: `version: 1
listeners:
  - name: public
    address: "127.0.0.1:0"
upstreams:
  - name: up
    hosts:
      - url: "http://127.0.0.1:9001"
stores:
  - name: -bad
    type: nosuch
  - name: r
    type: redis
middlewares:
  - name: a
    type: mystery
  - 7
routes:
  - name: limited
    match:
      path: /limited
    upstream: up
    rate_limit:
      rate: 5
      per: 1s
      store: missing
`,
		},
		{
			name: "registered types",
			doc: `version: 1
global:
  drain_timeout: soon
listeners:
  - name: public
    address: "127.0.0.1:0"
upstreams:
  - name: up
    hosts:
      - url: "http://127.0.0.1:9001"
stores:
  - name: c
    type: custom
  - name: n
    type: nosuch
middlewares:
  - name: t
    type: tagger
  - name: m
    type: mystery
routes:
  - name: r
    match:
      path: /r
    upstream: up
`,
			opts: []microgateway.Option{
				microgateway.WithStoreType("custom", store.CapRateLimit, func(microgateway.StoreParams) (store.Backend, error) {
					return nil, errors.New("unused")
				}),
				microgateway.WithMiddleware("tagger", passthrough),
			},
			absent: []string{`"custom"`, `"tagger"`},
		},
		{
			name:  "seeded stores",
			count: 61,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTemp(t, tc.doc)
			if tc.name == "seeded stores" {
				path = "../../config/testdata/stores_invalid.yaml"
			}
			reload := reloadLines(t, tc.opts, path)
			validate := validateLines(t, tc.opts, path)
			if !slices.Equal(reload, validate) {
				t.Fatalf("ReloadFile lines differ from validate lines\nReloadFile:\n%s\nvalidate:\n%s",
					strings.Join(reload, "\n"), strings.Join(validate, "\n"))
			}
			if len(reload) < 2 {
				t.Fatalf("got %d errors, want at least 2:\n%s", len(reload), strings.Join(reload, "\n"))
			}
			if tc.count != 0 && len(reload) != tc.count {
				t.Fatalf("got %d errors, want %d:\n%s", len(reload), tc.count, strings.Join(reload, "\n"))
			}
			for _, a := range tc.absent {
				for _, line := range reload {
					if strings.Contains(line, a) {
						t.Errorf("line %q mentions %s, want it absent", line, a)
					}
				}
			}
		})
	}
}
