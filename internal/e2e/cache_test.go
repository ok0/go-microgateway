package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/cli"
	"github.com/ok0/go-microgateway/internal/clock"
)

func cacheBadDoc(backendURL string) string {
	return fmt.Sprintf(`version: 1
global:
  drain_timeout: soon
%supstreams:
  - name: up
    hosts:
      - url: %q
cache:
  tag_delimiter: tab
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
    cache:
      enabled: true
      statuses: [200, 304]
`, listenersYAML, backendURL)
}

func cacheBadWant(t *testing.T, doc string) []config.FieldError {
	t.Helper()
	return []config.FieldError{
		{Path: "global.drain_timeout", Line: opLine(t, doc, "drain_timeout: soon"), Msg: `invalid duration "soon" (use a value such as "250ms" or "5s")`},
		{Path: "cache.tag_delimiter", Line: opLine(t, doc, "tag_delimiter: tab"), Msg: "must be one of space, comma"},
		{Path: "middlewares[0].type", Line: opLine(t, doc, "type: mystery"), Msg: `unknown middleware type "mystery"`},
		{Path: "routes[0].rate_limit.store", Line: opLine(t, doc, "store: missing"), Msg: `store "missing" is not defined in stores`},
		{Path: "routes[0].cache.statuses[1]", Line: opLine(t, doc, "statuses: [200, 304]"), Msg: "must be a final status from 200 to 599 other than 206 and 304"},
	}
}

func TestCacheConfigOnePass(t *testing.T) {
	t.Parallel()
	n := newMemNetwork()
	be := n.serve(t, echoHandler("A"))
	doc := cacheBadDoc(be.URL)
	fake := clock.NewFake(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))

	_, err := newGatewayFromYAML([]byte(doc), withClock(fake))
	opWant(t, err, cacheBadWant(t, doc))
	fe := opErrors(t, err)

	g := n.gateway(t, watchDoc(be.URL, "/x"), withClock(fake))
	fp := g.Config().Fingerprint()
	if got := servedBy(g, "/x"); got != "A" {
		t.Fatalf("served by %q before reload, want A", got)
	}
	_, f0, _ := counters(g)

	err = g.ReloadYAML([]byte(doc))
	opWant(t, err, cacheBadWant(t, doc))
	if _, f, _ := counters(g); f != f0+1 {
		t.Fatalf("failure counter after ReloadYAML = %v, want %v", f, f0+1)
	}
	if g.Config().Fingerprint() != fp {
		t.Fatal("ReloadYAML changed the active configuration")
	}
	if got := servedBy(g, "/x"); got != "A" {
		t.Fatalf("served by %q after ReloadYAML, want A", got)
	}

	var buf bytes.Buffer
	path := writeConfig(t, t.TempDir(), doc)
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
}

func TestCacheConfigDoesNotChangeTraffic(t *testing.T) {
	t.Parallel()
	n := newMemNetwork()
	var origin atomic.Int64
	be := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte("ok"))
	}))
	fake := clock.NewFake(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))

	const enabled = "    cache:\n      enabled: true\n"
	variants := []struct {
		name string
		doc  string
	}{
		{"baseline", cacheTrafficDoc(be.URL, "", "")},
		{"named", cacheTrafficDoc(be.URL, "cache:\n  name: edge\n", enabled)},
		{"enabled", cacheTrafficDoc(be.URL, "", enabled)},
		{"pipeline", cacheTrafficDoc(be.URL, "", enabled+"    pipeline: [timeout, cache]\n")},
	}

	var baseNames [2][]string
	var baseMetrics string
	for i, v := range variants {
		g := n.gateway(t, v.doc, withClock(fake))
		before := origin.Load()
		var names [2][]string
		for j := range names {
			rec := httptest.NewRecorder()
			g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
			resp := rec.Result()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%s: status = %d, want 200", v.name, resp.StatusCode)
			}
			if body := rec.Body.String(); body != "ok" {
				t.Fatalf("%s: body = %q, want ok", v.name, body)
			}
			if resp.Header.Get("Cache-Status") != "" || resp.Header.Get("Age") != "" {
				t.Fatalf("%s: Cache-Status %q or Age %q present, want neither", v.name, resp.Header.Get("Cache-Status"), resp.Header.Get("Age"))
			}
			for k := range resp.Header {
				names[j] = append(names[j], k)
			}
			slices.Sort(names[j])
		}
		if d := origin.Load() - before; d != 2 {
			t.Fatalf("%s: origin requests = %d, want 2", v.name, d)
		}
		metrics := metricsText(t, g)
		if i == 0 {
			baseNames = names
			baseMetrics = metrics
		} else {
			if !reflect.DeepEqual(names, baseNames) {
				t.Fatalf("%s: header names = %q, want %q", v.name, names, baseNames)
			}
			if metrics != baseMetrics {
				t.Fatalf("%s: metrics differ from the baseline gateway", v.name)
			}
		}
		if v.name == "named" {
			var cfg map[string]any
			if err := json.Unmarshal([]byte(adminBody(t, g, "/config")), &cfg); err != nil {
				t.Fatal(err)
			}
			top, _ := cfg["cache"].(map[string]any)
			if top["name"] != "edge" {
				t.Fatalf("/config cache.name = %v, want edge", top["name"])
			}
			routes, _ := cfg["routes"].([]any)
			if len(routes) != 1 {
				t.Fatalf("/config routes = %d, want 1", len(routes))
			}
			route, _ := routes[0].(map[string]any)
			rc, _ := route["cache"].(map[string]any)
			if rc["enabled"] != true {
				t.Fatalf("/config routes[0].cache.enabled = %v, want true", rc["enabled"])
			}
			want := []any{200.0, 203.0, 204.0, 300.0, 301.0, 308.0, 404.0, 405.0, 410.0, 414.0, 501.0}
			if !reflect.DeepEqual(rc["statuses"], want) {
				t.Fatalf("/config routes[0].cache.statuses = %v, want %v", rc["statuses"], want)
			}
		}
	}
}

func cacheTrafficDoc(backendURL, top, route string) string {
	return "version: 1\n" + listenersYAML + top + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: r
    match:
      path: /x
    upstream: up
%s`, backendURL, route)
}
