package e2e

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
)

func gbParse(t *testing.T, doc string) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(doc))
	if err != nil {
		t.Fatalf("config.Parse = %v, want nil", err)
	}
	return cfg
}

func gbPairs(t *testing.T, err error) []string {
	t.Helper()
	errs := opErrors(t, err)
	paths := map[string]bool{}
	out := make([]string, 0, len(errs))
	for _, fe := range errs {
		if paths[fe.Path] {
			t.Fatalf("path %q reported more than once: %v", fe.Path, errs)
		}
		paths[fe.Path] = true
		out = append(out, fe.Path+": "+fe.Msg)
	}
	slices.Sort(out)
	return out
}

func gbHead(backend string) string {
	return "version: 1\n" + listenersYAML + "upstreams:\n  - name: up\n    hosts:\n      - url: " + strconv.Quote(backend) + "\n"
}

func TestGoBuiltTypelessMiddlewareOnce(t *testing.T) {
	n := newMemNetwork()
	be := n.serve(t, echoHandler("A"))
	cfg := gbParse(t, gbHead(be.URL)+"middlewares:\n  - name: m\n    type: tagger\nroutes:\n  - name: r\n    match:\n      path: /r\n    upstream: up\n")
	cfg.Middlewares[0].Type = ""
	want := []string{"middlewares[0].type: is required"}

	if got := gbPairs(t, microgateway.Validate(cfg)); !slices.Equal(got, want) {
		t.Fatalf("Validate: got %v, want %v", got, want)
	}
	_, err := microgateway.New(cfg)
	if got := gbPairs(t, err); !slices.Equal(got, want) {
		t.Fatalf("New: got %v, want %v", got, want)
	}
	g := n.gateway(t, watchDoc(be.URL, "/x"))
	if got := gbPairs(t, g.Reload(cfg)); !slices.Equal(got, want) {
		t.Fatalf("Reload: got %v, want %v", got, want)
	}
}

func gbCompare(t *testing.T, n *memNetwork, backend, doc string, cfg *config.Config) {
	t.Helper()
	_, yerr := microgateway.NewFromYAML([]byte(doc))
	y := gbPairs(t, yerr)
	check := func(step string, err error) {
		t.Helper()
		if got := gbPairs(t, err); !slices.Equal(got, y) {
			t.Fatalf("%s: Go-built %v, YAML %v", step, got, y)
		}
	}
	check("Validate", microgateway.Validate(cfg))
	_, err := microgateway.New(cfg)
	check("New", err)
	g := n.gateway(t, watchDoc(backend, "/x"))
	check("Reload", g.Reload(cfg))
}

func TestGoBuiltMatchesYAML(t *testing.T) {
	n := newMemNetwork()
	be := n.serve(t, echoHandler("A"))
	route := "routes:\n  - name: r\n    match:\n      path: /r\n    upstream: up\n"

	t.Run("typeless middleware", func(t *testing.T) {
		doc := gbHead(be.URL) + "middlewares:\n  - name: m\n" + route
		cfg := gbParse(t, gbHead(be.URL)+"middlewares:\n  - name: m\n    type: tagger\n"+route)
		cfg.Middlewares[0].Type = ""
		gbCompare(t, n, be.URL, doc, cfg)
	})

	t.Run("four errors", func(t *testing.T) {
		doc := strings.Replace(opBadDoc(be.URL), "drain_timeout: soon", "drain_timeout: -1s", 1)
		goDoc := strings.Replace(doc, "  drain_timeout: -1s\n", "", 1)
		goDoc = strings.Replace(goDoc, "store: missing", "store: shared", 1)
		cfg := gbParse(t, goDoc)
		cfg.Global.DrainTimeout = config.Duration(-time.Second)
		cfg.Routes[0].RateLimit.Store = "missing"

		want := []string{
			"global.drain_timeout: must be positive",
			`middlewares[0].type: unknown middleware type "mystery"`,
			`routes[0].rate_limit.store: store "missing" is not defined in stores`,
			`stores[0].type: store type "nosuch" is not registered`,
		}
		_, yerr := microgateway.NewFromYAML([]byte(doc))
		if got := gbPairs(t, yerr); !slices.Equal(got, want) {
			t.Fatalf("YAML errors = %v, want %v", got, want)
		}
		gbCompare(t, n, be.URL, doc, cfg)
	})
}
