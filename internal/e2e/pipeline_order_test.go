package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/cli"
	"go.yaml.in/yaml/v3"
)

const poMsg = `stage "cache" must come after "auth": cache hits would be served without authentication`

// poWant returns the errors of a document with a cache-before-auth pipeline and a negative drain timeout. With line
// set, each error carries the line of its source text in doc; otherwise the lines are 0.
func poWant(t *testing.T, doc string, line bool) []config.FieldError {
	t.Helper()
	pipe, drain := 0, 0
	if line {
		pipe = opLine(t, doc, "pipeline: [cache, auth]")
		drain = opLine(t, doc, "drain_timeout: -1s")
	}
	return []config.FieldError{
		{Path: "routes[0].pipeline[0]", Line: pipe, Msg: poMsg},
		{Path: "global.drain_timeout", Line: drain, Msg: "must be positive"},
	}
}

// poBehaves checks that g still serves the authenticated cached route and rejects an unauthenticated request.
func poBehaves(t *testing.T, g *microgateway.Gateway, step string) {
	t.Helper()
	rec := cacheDo(g, http.MethodGet, "/x", caHdr(caKey))
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: authenticated GET /x status = %d, want 200", step, rec.Code)
	}
	var e echo
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatalf("%s: decode echo body: %v", step, err)
	}
	if e.Backend != "A" {
		t.Fatalf("%s: served by %q, want A", step, e.Backend)
	}
	t.Run(step+" unauthenticated", func(t *testing.T) {
		auAssert401(t, cacheDo(g, http.MethodGet, "/x", nil), "ApiKey")
	})
}

func TestPipelineOrderRejectedEverywhere(t *testing.T) {
	t.Parallel()
	n := newMemNetwork()
	be := n.serve(t, echoHandler("A"))
	bad := caDoc(be.URL, "global:\n  drain_timeout: -1s\n", "[cache, auth]")
	good := caDoc(be.URL, "", "[auth, cache]")

	_, err := newGatewayFromYAML([]byte(bad))
	opWant(t, err, poWant(t, bad, true))

	var cfg config.Config
	if err := yaml.Unmarshal([]byte(bad), &cfg); err != nil {
		t.Fatal(err)
	}
	opWant(t, microgateway.Validate(&cfg), poWant(t, bad, false))
	_, err = newGateway(&cfg)
	opWant(t, err, poWant(t, bad, false))

	g1 := n.gateway(t, good)
	g2 := n.gateway(t, good)
	fp1, fp2 := g1.Config().Fingerprint(), g2.Config().Fingerprint()
	s1, f1, u1 := counters(g1)
	s2, f2, u2 := counters(g2)
	poBehaves(t, g1, "before reload")
	poBehaves(t, g2, "before reload on second instance")

	opWant(t, g1.Reload(&cfg), poWant(t, bad, false))
	if s, f, u := counters(g1); s != s1 || f != f1+1 || u != u1 {
		t.Fatalf("counters after Reload = (%v, %v, %v), want (%v, %v, %v)", s, f, u, s1, f1+1, u1)
	}
	if g1.Config().Fingerprint() != fp1 {
		t.Fatal("Reload changed the active configuration")
	}
	poBehaves(t, g1, "after Reload")

	opWant(t, g1.ReloadYAML([]byte(bad)), poWant(t, bad, true))
	if s, f, u := counters(g1); s != s1 || f != f1+2 || u != u1 {
		t.Fatalf("counters after ReloadYAML = (%v, %v, %v), want (%v, %v, %v)", s, f, u, s1, f1+2, u1)
	}
	if g1.Config().Fingerprint() != fp1 {
		t.Fatal("ReloadYAML changed the active configuration")
	}
	poBehaves(t, g1, "after ReloadYAML")

	path := writeConfig(t, t.TempDir(), bad)
	err = g1.ReloadFile(path)
	opWant(t, err, poWant(t, bad, true))
	fe := opErrors(t, err)
	if s, f, u := counters(g1); s != s1 || f != f1+3 || u != u1 {
		t.Fatalf("counters after ReloadFile = (%v, %v, %v), want (%v, %v, %v)", s, f, u, s1, f1+3, u1)
	}
	if g1.Config().Fingerprint() != fp1 {
		t.Fatal("ReloadFile changed the active configuration")
	}
	poBehaves(t, g1, "after ReloadFile")

	var buf bytes.Buffer
	code := cli.Main(context.Background(), cli.Env{
		Args:      []string{"validate", "-config", path},
		Stderr:    &buf,
		LookupEnv: func(string) (string, bool) { return "", false },
	})
	if code != 1 {
		t.Fatalf("validate exit code = %d, want 1", code)
	}
	var want []string
	for _, e := range fe {
		want = append(want, e.String())
	}
	want = append(want, fmt.Sprintf("%s: %d error(s)", path, len(fe)))
	got := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if !slices.Equal(got, want) {
		t.Fatalf("validate stderr lines\n got: %q\nwant: %q", got, want)
	}

	if s, f, u := counters(g2); s != s2 || f != f2 || u != u2 {
		t.Fatalf("second instance counters = (%v, %v, %v), want (%v, %v, %v)", s, f, u, s2, f2, u2)
	}
	if g2.Config().Fingerprint() != fp2 {
		t.Fatal("second instance configuration changed")
	}
	poBehaves(t, g2, "second instance")
}
