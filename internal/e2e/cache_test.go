package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/cli"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

var cacheStart = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

var cacheFamilies = []string{
	"microgateway_cache_requests_total",
	"microgateway_cache_stores_total",
	"microgateway_cache_store_errors_total",
	"microgateway_cache_evictions_total",
	"microgateway_cache_bytes",
	"microgateway_cache_entries",
}

// cacheScript is an origin whose response is set by the test and which counts and records its requests.
type cacheScript struct {
	mu     sync.Mutex
	status int
	header http.Header
	body   string
	handle http.HandlerFunc
	calls  int
	last   http.Header
}

func (s *cacheScript) set(status int, header http.Header, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status, s.header, s.body, s.handle = status, header, body, nil
}

func (s *cacheScript) setHandle(h http.HandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handle = h
}

func (s *cacheScript) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *cacheScript) lastHeader() http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

func (s *cacheScript) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.calls++
	s.last = r.Header.Clone()
	status, header, body, h := s.status, s.header, s.body, s.handle
	s.mu.Unlock()
	if h != nil {
		h(w, r)
		return
	}
	if _, ok := header["Date"]; !ok {
		w.Header()["Date"] = nil
	}
	for k, v := range header {
		w.Header()[k] = v
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

// cacheGatewayDoc returns a configuration with the upstream at backendURL, top-level blocks top and the routes.
func cacheGatewayDoc(backendURL, top, routes string) string {
	return "version: 1\n" + listenersYAML + top + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
`, backendURL) + "routes:\n" + routes
}

// cacheRouteYAML returns one route matching every path, with caching enabled and the cache block lines extra.
// Each line of extra is indented under the route's cache block.
func cacheRouteYAML(name string, extra ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "  - name: %s\n    match:\n      path: /\n    upstream: up\n    cache:\n      enabled: true\n", name)
	for _, line := range extra {
		if line != "" {
			b.WriteString("      " + line + "\n")
		}
	}
	return b.String()
}

func cacheDo(g *microgateway.Gateway, method, target string, hdr http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	for k, v := range hdr {
		req.Header[k] = v
	}
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	return rec
}

func cacheStatusOf(t *testing.T, rec *httptest.ResponseRecorder) []httpcache.Status {
	t.Helper()
	sts, err := httpcache.ParseStatus(strings.Join(rec.Header().Values("Cache-Status"), ", "))
	if err != nil {
		t.Fatalf("Cache-Status %q: %v", rec.Header().Values("Cache-Status"), err)
	}
	return sts
}

// cacheLast returns the last Cache-Status member of rec, which describes this gateway's cache stage.
func cacheLast(t *testing.T, rec *httptest.ResponseRecorder) httpcache.Status {
	t.Helper()
	sts := cacheStatusOf(t, rec)
	if len(sts) == 0 {
		t.Fatal("response has no Cache-Status")
	}
	return sts[len(sts)-1]
}

func cacheMetricFamilies(body string) []string {
	var names []string
	for _, line := range strings.Split(body, "\n") {
		if rest, ok := strings.CutPrefix(line, "# HELP "); ok {
			name, _, _ := strings.Cut(rest, " ")
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

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

func TestCacheConfigTraffic(t *testing.T) {
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
		name      string
		doc       string
		cacheName string
	}{
		{name: "baseline", doc: cacheTrafficDoc(be.URL, "", "")},
		{name: "edge", doc: cacheTrafficDoc(be.URL, "cache:\n  name: edge\n", "")},
		{name: "named", doc: cacheTrafficDoc(be.URL, "cache:\n  name: edge\n", enabled), cacheName: "edge"},
		{name: "enabled", doc: cacheTrafficDoc(be.URL, "", enabled), cacheName: "microgateway"},
		{name: "pipeline", doc: cacheTrafficDoc(be.URL, "", enabled+"    pipeline: [timeout, cache]\n"), cacheName: "microgateway"},
	}

	var baseNames []string
	var baseMetrics string
	var baseFamilies []string
	for i, v := range variants {
		g := n.gateway(t, v.doc, withClock(fake))
		before := origin.Load()
		var names []string
		var second *httptest.ResponseRecorder
		for j := range 2 {
			rec := cacheDo(g, http.MethodGet, "/x", nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s: status = %d, want 200", v.name, rec.Code)
			}
			if body := rec.Body.String(); body != "ok" {
				t.Fatalf("%s: body = %q, want ok", v.name, body)
			}
			if v.cacheName == "" && (rec.Header().Get("Cache-Status") != "" || rec.Header().Get("Age") != "") {
				t.Fatalf("%s: Cache-Status %q or Age %q present, want neither", v.name, rec.Header().Get("Cache-Status"), rec.Header().Get("Age"))
			}
			for k := range rec.Header() {
				names = append(names, k)
			}
			if j == 1 {
				second = rec
			}
		}
		slices.Sort(names)
		names = slices.Compact(names)

		wantOrigin := int64(2)
		if v.cacheName != "" {
			wantOrigin = 1
		}
		if d := origin.Load() - before; d != wantOrigin {
			t.Fatalf("%s: origin requests = %d, want %d", v.name, d, wantOrigin)
		}
		metrics := metricsText(t, g)
		families := cacheMetricFamilies(metrics)
		if i == 0 {
			baseNames, baseMetrics, baseFamilies = names, metrics, families
		}
		if v.cacheName == "" {
			if !slices.Equal(names, baseNames) {
				t.Fatalf("%s: header names = %q, want %q", v.name, names, baseNames)
			}
			if metrics != baseMetrics {
				t.Fatalf("%s: metrics differ from the baseline gateway", v.name)
			}
		} else {
			wantNames := append(slices.Clone(baseNames), "Age", "Cache-Status")
			slices.Sort(wantNames)
			if !slices.Equal(names, wantNames) {
				t.Fatalf("%s: header names = %q, want %q", v.name, names, wantNames)
			}
			wantFamilies := append(slices.Clone(baseFamilies), cacheFamilies...)
			slices.Sort(wantFamilies)
			if !slices.Equal(families, wantFamilies) {
				t.Fatalf("%s: metric families = %q, want %q", v.name, families, wantFamilies)
			}
			st := cacheLast(t, second)
			if st.Name != v.cacheName || !st.Hit {
				t.Fatalf("%s: last Cache-Status member %+v, want hit named %q", v.name, st, v.cacheName)
			}
			if second.Header().Get("Age") == "" {
				t.Fatalf("%s: second response has no Age", v.name)
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

func cacheCapDoc(backendURL string) string {
	return fmt.Sprintf(`version: 1
global:
  drain_timeout: soon
%supstreams:
  - name: up
    hosts:
      - url: %q
stores:
  - name: rl
    type: rlonly
cache:
  store: rl
  tag_delimiter: tab
middlewares:
  - name: tag
    type: mystery
routes:
  - name: limited
    match:
      path: /limited
    upstream: up
    cache:
      enabled: true
`, listenersYAML, backendURL)
}

func cacheCapWant(t *testing.T, doc string) []config.FieldError {
	t.Helper()
	return []config.FieldError{
		{Path: "global.drain_timeout", Line: opLine(t, doc, "drain_timeout: soon"), Msg: `invalid duration "soon" (use a value such as "250ms" or "5s")`},
		{Path: "cache.tag_delimiter", Line: opLine(t, doc, "tag_delimiter: tab"), Msg: "must be one of space, comma"},
		{Path: "middlewares[0].type", Line: opLine(t, doc, "type: mystery"), Msg: `unknown middleware type "mystery"`},
		{Path: "cache.store", Line: opLine(t, doc, "store: rl"), Msg: `store "rl" (type "rlonly") does not support caching`},
	}
}

// cacheCapResolveDoc is cacheCapDoc without the schema errors, which config.Parse rejects before Validate runs.
func cacheCapResolveDoc(backendURL string) string {
	return fmt.Sprintf(`version: 1
%supstreams:
  - name: up
    hosts:
      - url: %q
stores:
  - name: rl
    type: rlonly
cache:
  store: rl
middlewares:
  - name: tag
    type: mystery
routes:
  - name: limited
    match:
      path: /limited
    upstream: up
    cache:
      enabled: true
`, listenersYAML, backendURL)
}

func cacheCapResolveWant(t *testing.T, doc string) []config.FieldError {
	t.Helper()
	return []config.FieldError{
		{Path: "middlewares[0].type", Line: opLine(t, doc, "type: mystery"), Msg: `unknown middleware type "mystery"`},
		{Path: "cache.store", Line: opLine(t, doc, "store: rl"), Msg: `store "rl" (type "rlonly") does not support caching`},
	}
}

func TestCacheStoreCapabilityOnePass(t *testing.T) {
	t.Parallel()
	n := newMemNetwork()
	be := n.serve(t, echoHandler("A"))
	doc := cacheCapDoc(be.URL)
	fake := clock.NewFake(cacheStart)
	rlOnly := microgateway.WithStoreType("rlonly", store.CapRateLimit, func(microgateway.StoreParams) (store.Backend, error) {
		return sapiCloseOnly{}, nil
	})

	_, err := newGatewayFromYAML([]byte(doc), withClock(fake), rlOnly)
	opWant(t, err, cacheCapWant(t, doc))
	fe := opErrors(t, err)

	g := n.gateway(t, watchDoc(be.URL, "/x"), withClock(fake), rlOnly)
	fp := g.Config().Fingerprint()
	if got := servedBy(g, "/x"); got != "A" {
		t.Fatalf("served by %q before reload, want A", got)
	}
	_, f0, _ := counters(g)

	err = g.ReloadYAML([]byte(doc))
	opWant(t, err, cacheCapWant(t, doc))
	if _, f, _ := counters(g); f != f0+1 {
		t.Fatalf("failure counter after ReloadYAML = %v, want %v", f, f0+1)
	}
	if g.Config().Fingerprint() != fp {
		t.Fatal("ReloadYAML changed the active configuration")
	}
	if got := servedBy(g, "/x"); got != "A" {
		t.Fatalf("served by %q after ReloadYAML, want A", got)
	}

	resolveDoc := cacheCapResolveDoc(be.URL)
	resolved, err := config.Parse([]byte(resolveDoc))
	if err != nil {
		t.Fatal(err)
	}
	opWant(t, microgateway.Validate(resolved, rlOnly), cacheCapResolveWant(t, resolveDoc))

	var buf bytes.Buffer
	path := writeConfig(t, t.TempDir(), doc)
	code := cli.Main(context.Background(), cli.Env{
		Args:      []string{"validate", "-config", path},
		Stderr:    &buf,
		LookupEnv: func(string) (string, bool) { return "", false },
	}, rlOnly)
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

func cacheCollisionDoc(urlA, urlB, upstreamB string) string {
	return "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
  - name: other
    hosts:
      - url: %q
routes:
  - name: a
    match:
      path: /
      headers:
        X-Tenant: a
    upstream: up
    cache:
      enabled: true
  - name: b
    match:
      path: /
      headers:
        X-Tenant: b
    upstream: %s
    cache:
      enabled: true
`, urlA, urlB, upstreamB)
}

func TestCacheCollisionValidation(t *testing.T) {
	t.Parallel()
	n := newMemNetwork()
	fake := clock.NewFake(cacheStart)
	scr := &cacheScript{}
	scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "A")
	be := n.serve(t, scr)
	other := n.serve(t, echoHandler("B"))

	differing := cacheCollisionDoc(be.URL, other.URL, "other")
	_, err := newGatewayFromYAML([]byte(differing), withClock(fake))
	ve := opErrors(t, err)
	if len(ve) != 2 {
		t.Fatalf("errors = %v, want one per route", ve)
	}
	for _, fe := range ve {
		if !strings.Contains(fe.Msg, `"a"`) || !strings.Contains(fe.Msg, `"b"`) || !strings.Contains(fe.Msg, "upstream") {
			t.Fatalf("error %q does not name both routes and the difference", fe.Msg)
		}
	}

	identical := cacheCollisionDoc(be.URL, other.URL, "up")
	g := n.gateway(t, identical, withClock(fake))
	first := cacheDo(g, http.MethodGet, "http://example.com/c", http.Header{"X-Tenant": {"a"}})
	if st := cacheLast(t, first); st.Hit || !st.Stored {
		t.Fatalf("route a fill %+v, want stored and not a hit", st)
	}
	second := cacheDo(g, http.MethodGet, "http://example.com/c", http.Header{"X-Tenant": {"b"}})
	if st := cacheLast(t, second); !st.Hit {
		t.Fatalf("route b request %+v, want a hit on the entry route a filled", st)
	}
	if scr.count() != 1 {
		t.Fatalf("origin requests = %d, want 1", scr.count())
	}
}

func TestCacheForwardedProto(t *testing.T) {
	t.Parallel()
	n := newMemNetwork()
	scr := &cacheScript{}
	scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
	be := n.serve(t, scr)
	g := n.gateway(t, cacheGatewayDoc(be.URL, "", cacheRouteYAML("r")), withClock(clock.NewFake(cacheStart)))

	plain := cacheDo(g, http.MethodGet, "http://example.com/fp", nil)
	if st := cacheLast(t, plain); st.Hit || !st.Stored {
		t.Fatalf("plain fill %+v, want stored", st)
	}
	fwd := cacheDo(g, http.MethodGet, "http://example.com/fp", http.Header{"X-Forwarded-Proto": {"https"}})
	if st := cacheLast(t, fwd); !st.Hit {
		t.Fatalf("request with X-Forwarded-Proto %+v, want hit", st)
	}
	tls := httptest.NewRequest(http.MethodGet, "https://example.com/fp", nil)
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, tls)
	if st := cacheLast(t, rec); st.Hit {
		t.Fatalf("TLS request %+v, want a miss", st)
	}
	if scr.count() != 2 {
		t.Fatalf("origin requests = %d, want 2", scr.count())
	}
}

func TestCacheEligibilityGateway(t *testing.T) {
	t.Parallel()
	n := newMemNetwork()
	scr := &cacheScript{}
	scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "body")
	be := n.serve(t, scr)
	g := n.gateway(t, cacheGatewayDoc(be.URL, "", cacheRouteYAML("r")), withClock(clock.NewFake(cacheStart)))

	cases := []struct {
		name   string
		method string
		hdr    http.Header
		fwd    string
		detail string
	}{
		{name: "post", method: http.MethodPost, fwd: httpcache.FwdMethod},
		{name: "put", method: http.MethodPut, fwd: httpcache.FwdMethod},
		{name: "patch", method: http.MethodPatch, fwd: httpcache.FwdMethod},
		{name: "delete", method: http.MethodDelete, fwd: httpcache.FwdMethod},
		{name: "options", method: http.MethodOptions, fwd: httpcache.FwdMethod},
		{name: "range", method: http.MethodGet, fwd: httpcache.FwdBypass, detail: "range", hdr: http.Header{"Range": {"bytes=0-1"}}},
		{name: "if-range", method: http.MethodGet, fwd: httpcache.FwdBypass, detail: "range", hdr: http.Header{"If-Range": {`"v1"`}}},
		{name: "if-match", method: http.MethodGet, fwd: httpcache.FwdBypass, detail: "precondition", hdr: http.Header{"If-Match": {`"v1"`}}},
		{name: "if-unmodified-since", method: http.MethodGet, fwd: httpcache.FwdBypass, detail: "precondition",
			hdr: http.Header{"If-Unmodified-Since": {cacheStart.Format(http.TimeFormat)}}},
	}
	for i, c := range cases {
		before := scr.count()
		rec := cacheDo(g, c.method, fmt.Sprintf("http://example.com/e%d", i), c.hdr)
		st := cacheLast(t, rec)
		if st.Fwd != c.fwd || st.Detail != c.detail || st.Hit {
			t.Fatalf("%s: status %+v, want fwd=%q detail=%q and no hit", c.name, st, c.fwd, c.detail)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: code %d, want 200", c.name, rec.Code)
		}
		if d := scr.count() - before; d != 1 {
			t.Fatalf("%s: origin requests = %d, want 1", c.name, d)
		}
	}
}

func TestCacheRulesGateway(t *testing.T) {
	t.Parallel()
	type step struct {
		advance time.Duration
		query   string
		hdr     http.Header
	}
	cases := []struct {
		name   string
		route  []string
		origin http.Header
		steps  []step
		hit    bool
		fwd    string
		calls  int
	}{
		{name: "fresh hit after fill", origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []step{{}, {}}, hit: true, calls: 1},
		{name: "no-store", origin: http.Header{"Cache-Control": {"no-store, max-age=60"}},
			steps: []step{{}, {}}, fwd: "uri-miss", calls: 2},
		{name: "private", origin: http.Header{"Cache-Control": {"private, max-age=60"}},
			steps: []step{{}, {}}, fwd: "uri-miss", calls: 2},
		{name: "no-cache with validator and keep revalidates", route: []string{"keep: 1h"},
			origin: http.Header{"Cache-Control": {"no-cache"}, "Etag": {`"a"`}},
			steps:  []step{{}, {}}, fwd: "stale", calls: 2},
		{name: "no-cache with validator and no keep is not stored", origin: http.Header{"Cache-Control": {"no-cache"}, "Etag": {`"a"`}},
			steps: []step{{}, {}}, fwd: "uri-miss", calls: 2},
		{name: "s-maxage over max-age", origin: http.Header{"Cache-Control": {"max-age=0, s-maxage=60"}},
			steps: []step{{}, {}}, hit: true, calls: 1},
		{name: "expires", origin: http.Header{"Date": {cacheStart.Format(http.TimeFormat)}, "Expires": {cacheStart.Add(time.Minute).Format(http.TimeFormat)}},
			steps: []step{{}, {}}, hit: true, calls: 1},
		{name: "vary hit for same variant", origin: http.Header{"Vary": {"Accept-Language"}, "Cache-Control": {"max-age=60"}},
			steps: []step{{hdr: http.Header{"Accept-Language": {"en"}}}, {hdr: http.Header{"Accept-Language": {"en"}}}}, hit: true, calls: 1},
		{name: "vary miss for other variant", origin: http.Header{"Vary": {"Accept-Language"}, "Cache-Control": {"max-age=60"}},
			steps: []step{{hdr: http.Header{"Accept-Language": {"en"}}}, {hdr: http.Header{"Accept-Language": {"fr"}}}}, fwd: "vary-miss", calls: 2},
		{name: "vary star", origin: http.Header{"Vary": {"*"}, "Cache-Control": {"max-age=60"}},
			steps: []step{{}, {}}, fwd: "uri-miss", calls: 2},
		{name: "set-cookie", origin: http.Header{"Set-Cookie": {"a=b"}, "Cache-Control": {"max-age=60"}},
			steps: []step{{}, {}}, fwd: "uri-miss", calls: 2},
		{name: "authorization without public", origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []step{{hdr: http.Header{"Authorization": {"Bearer t"}}}, {hdr: http.Header{"Authorization": {"Bearer t"}}}}, fwd: "uri-miss", calls: 2},
		{name: "authorization with public", origin: http.Header{"Cache-Control": {"public, max-age=60"}},
			steps: []step{{hdr: http.Header{"Authorization": {"Bearer t"}}}, {hdr: http.Header{"Authorization": {"Bearer t"}}}}, hit: true, calls: 1},
		{name: "authorization with must-revalidate", origin: http.Header{"Cache-Control": {"must-revalidate, max-age=60"}},
			steps: []step{{hdr: http.Header{"Authorization": {"Bearer t"}}}, {hdr: http.Header{"Authorization": {"Bearer t"}}}}, hit: true, calls: 1},
		{name: "request no-cache revalidates", origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []step{{}, {hdr: http.Header{"Cache-Control": {"no-cache"}}}}, fwd: "stale", calls: 2},
		{name: "request max-age zero revalidates", origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []step{{}, {advance: time.Second, hdr: http.Header{"Cache-Control": {"max-age=0"}}}}, fwd: "stale", calls: 2},
		{name: "request min-fresh above remaining", origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []step{{}, {hdr: http.Header{"Cache-Control": {"min-fresh=100"}}}}, fwd: "stale", calls: 2},
		{name: "request max-stale serves stale", route: []string{"stale_if_error: 1h"}, origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []step{{}, {advance: 120 * time.Second, hdr: http.Header{"Cache-Control": {"max-stale=3600"}}}}, hit: true, calls: 1},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n := newMemNetwork()
			scr := &cacheScript{}
			scr.set(http.StatusOK, c.origin, "body")
			be := n.serve(t, scr)
			fake := clock.NewFake(cacheStart)
			g := n.gateway(t, cacheGatewayDoc(be.URL, "", cacheRouteYAML("r", c.route...)), withClock(fake))
			target := fmt.Sprintf("http://example.com/r%d", i)
			var rec *httptest.ResponseRecorder
			for _, s := range c.steps {
				if s.advance > 0 {
					fake.Advance(s.advance)
				}
				rec = cacheDo(g, http.MethodGet, target+s.query, s.hdr)
			}
			st := cacheLast(t, rec)
			if st.Hit != c.hit {
				t.Errorf("hit=%v, want %v", st.Hit, c.hit)
			}
			if st.Fwd != c.fwd {
				t.Errorf("fwd=%q, want %q", st.Fwd, c.fwd)
			}
			if rec.Code != http.StatusOK {
				t.Errorf("status %d, want 200", rec.Code)
			}
			if got := scr.count(); got != c.calls {
				t.Errorf("origin calls %d, want %d", got, c.calls)
			}
		})
	}

	t.Run("only-if-cached", func(t *testing.T) {
		n := newMemNetwork()
		scr := &cacheScript{}
		scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "body")
		be := n.serve(t, scr)
		fake := clock.NewFake(cacheStart)
		g := n.gateway(t, cacheGatewayDoc(be.URL, "", cacheRouteYAML("r")), withClock(fake))
		miss := cacheDo(g, http.MethodGet, "http://example.com/oic", http.Header{"Cache-Control": {"only-if-cached"}})
		if st := cacheLast(t, miss); st.Hit || st.Detail != "only-if-cached" {
			t.Fatalf("only-if-cached miss %+v", st)
		}
		if miss.Code != http.StatusGatewayTimeout || !strings.Contains(miss.Body.String(), httperr.CodeNotCached) {
			t.Fatalf("only-if-cached miss status %d body %q", miss.Code, miss.Body.String())
		}
		if scr.count() != 0 {
			t.Fatalf("origin requests = %d, want 0", scr.count())
		}
		cacheDo(g, http.MethodGet, "http://example.com/oic", nil)
		hit := cacheDo(g, http.MethodGet, "http://example.com/oic", http.Header{"Cache-Control": {"only-if-cached"}})
		if st := cacheLast(t, hit); !st.Hit || hit.Code != http.StatusOK {
			t.Fatalf("only-if-cached hit %+v status %d", st, hit.Code)
		}
	})

	t.Run("circuit open", func(t *testing.T) {
		n := newMemNetwork()
		scr := &cacheScript{}
		scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "cached")
		be := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/bad" {
				panic(http.ErrAbortHandler)
			}
			scr.ServeHTTP(w, r)
		}))
		fake := clock.NewFake(cacheStart)
		doc := rsDoc{hosts: []string{be.URL}, breaker: rsBreaker, routes: []rsRoute{
			{name: "r", path: "/", extra: "cache:\n      enabled: true\n"},
		}}.yaml()
		g := n.gateway(t, doc, withClock(fake))
		if st := cacheLast(t, cacheDo(g, http.MethodGet, "http://example.com/cb", nil)); !st.Stored {
			t.Fatalf("fill %+v, want stored", st)
		}
		var codes []string
		for range 4 {
			rec := cacheDo(g, http.MethodGet, "http://example.com/bad", nil)
			cacheLast(t, rec)
			codes = append(codes, decodeErr(t, rec.Result()).Code)
		}
		if codes[0] != httperr.CodeBadGateway || codes[len(codes)-1] != httperr.CodeCircuitOpen {
			t.Fatalf("error codes %q, want bad_gateway first and circuit_open last", codes)
		}
		hit := cacheDo(g, http.MethodGet, "http://example.com/cb", nil)
		if st := cacheLast(t, hit); !st.Hit || hit.Code != http.StatusOK || hit.Body.String() != "cached" {
			t.Fatalf("hit while breaker open %+v status %d body %q", st, hit.Code, hit.Body.String())
		}
	})
}

func TestCacheStreamingGateway(t *testing.T) {
	t.Parallel()
	t.Run("event observed before origin finishes", func(t *testing.T) {
		n := newMemNetwork()
		release := make(chan struct{})
		var once sync.Once
		releaseOnce := func() { once.Do(func() { close(release) }) }
		t.Cleanup(releaseOnce)
		scr := &cacheScript{}
		scr.setHandle(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "event: one\n\n")
			_ = http.NewResponseController(w).Flush()
			<-release
			_, _ = io.WriteString(w, "event: two\n\n")
		})
		be := n.serve(t, scr)
		g := n.gateway(t, cacheGatewayDoc(be.URL, "", cacheRouteYAML("r")), withClock(clock.NewFake(cacheStart)))
		srv := httptest.NewServer(g)
		t.Cleanup(srv.Close)
		resp, err := srv.Client().Get(srv.URL + "/sse")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		first := make(chan string, 1)
		go func() {
			line, _ := bufio.NewReader(resp.Body).ReadString('\n')
			first <- line
		}()
		line, ok := testwait.Recv(t, "first event before the origin finishes", first, nil)
		if !ok || line != "event: one\n" {
			t.Fatalf("first line %q", line)
		}
		releaseOnce()
		rest, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(rest), "event: two") {
			t.Fatalf("rest of stream %q", rest)
		}
	})

	t.Run("oversize body is byte-identical and too_large", func(t *testing.T) {
		n := newMemNetwork()
		body := strings.Repeat("0123456789", 4)
		scr := &cacheScript{}
		scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"40"}}, body)
		be := n.serve(t, scr)
		g := n.gateway(t, cacheGatewayDoc(be.URL, "cache:\n  max_body_bytes: 16\n", cacheRouteYAML("r")), withClock(clock.NewFake(cacheStart)))
		rec := cacheDo(g, http.MethodGet, "http://example.com/big", nil)
		if rec.Body.String() != body {
			t.Fatalf("client body %q", rec.Body.String())
		}
		if st := cacheLast(t, rec); st.Stored {
			t.Fatalf("oversize response reported as stored: %+v", st)
		}
		m := metricsText(t, g)
		if v := metricValue(t, m, series("microgateway_cache_stores_total", "route", "r", "result", "too_large")); v != 1 {
			t.Fatalf("too_large = %v, want 1", v)
		}
		if cacheDo(g, http.MethodGet, "http://example.com/big", nil).Header().Get("Cache-Status") == "" || scr.count() != 2 {
			t.Fatalf("second request must be a miss to the origin, origin calls %d", scr.count())
		}
	})

	t.Run("origin abort mid-body stores nothing", func(t *testing.T) {
		n := newMemNetwork()
		scr := &cacheScript{}
		scr.setHandle(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "max-age=60")
			w.WriteHeader(http.StatusOK)
			_ = http.NewResponseController(w).Flush()
			_, _ = io.WriteString(w, "partial")
			panic(http.ErrAbortHandler)
		})
		be := n.serve(t, scr)
		g := n.gateway(t, cacheGatewayDoc(be.URL, "", cacheRouteYAML("r")), withClock(clock.NewFake(cacheStart)))
		cacheDo(g, http.MethodGet, "http://example.com/ab", nil)
		m := metricsText(t, g)
		if v := metricValue(t, m, series("microgateway_cache_stores_total", "route", "r", "result", "aborted")); v != 1 {
			t.Fatalf("aborted = %v, want 1", v)
		}
		if v := metricValue(t, m, series("microgateway_cache_stores_total", "route", "r", "result", "stored")); v != 0 {
			t.Fatalf("stored = %v, want 0", v)
		}
	})

	t.Run("client cancel stores nothing", func(t *testing.T) {
		n := newMemNetwork()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		scr := &cacheScript{}
		scr.setHandle(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "first")
			_ = http.NewResponseController(w).Flush()
			cancel()
			_, _ = io.WriteString(w, "second")
		})
		be := n.serve(t, scr)
		g := n.gateway(t, cacheGatewayDoc(be.URL, "", cacheRouteYAML("r")), withClock(clock.NewFake(cacheStart)))
		req := httptest.NewRequest(http.MethodGet, "http://example.com/cc", nil).WithContext(ctx)
		g.ServeHTTP(httptest.NewRecorder(), req)
		m := metricsText(t, g)
		if v := metricValue(t, m, series("microgateway_cache_stores_total", "route", "r", "result", "stored")); v != 0 {
			t.Fatalf("stored = %v, want 0", v)
		}
		scr.setHandle(nil)
		scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		if st := cacheLast(t, cacheDo(g, http.MethodGet, "http://example.com/cc", nil)); st.Hit {
			t.Fatal("cancelled fill was served")
		}
	})
}

func TestCacheRevalidationGateway(t *testing.T) {
	t.Parallel()
	t.Run("etag 304 refreshes the entry", func(t *testing.T) {
		n := newMemNetwork()
		fake := clock.NewFake(cacheStart)
		scr := &cacheScript{}
		scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "old")
		be := n.serve(t, scr)
		g := n.gateway(t, cacheGatewayDoc(be.URL, "", cacheRouteYAML("r", "stale_if_error: 1h")), withClock(fake))
		cacheDo(g, http.MethodGet, "http://example.com/rv", nil)
		fake.Advance(61 * time.Second)
		scr.set(http.StatusNotModified, http.Header{"Cache-Control": {"max-age=120"}, "Etag": {`"v1"`}}, "")
		rec := cacheDo(g, http.MethodGet, "http://example.com/rv", nil)
		if got := scr.lastHeader().Get("If-None-Match"); got != `"v1"` {
			t.Fatalf("origin If-None-Match %q, want \"v1\"", got)
		}
		st := cacheLast(t, rec)
		if !st.Hit || st.Fwd != httpcache.FwdStale || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified {
			t.Fatalf("status %+v, want hit fwd=stale fwd-status=304", st)
		}
		if !st.HasTTL || st.TTL != 120 {
			t.Fatalf("ttl %d, want 120", st.TTL)
		}
		if rec.Code != http.StatusOK || rec.Body.String() != "old" {
			t.Fatalf("client response %d %q, want 200 old", rec.Code, rec.Body.String())
		}
		if age := rec.Header().Get("Age"); age != "0" {
			t.Fatalf("Age %q, want 0", age)
		}
	})

	t.Run("last-modified 304 is conditional on If-Modified-Since", func(t *testing.T) {
		n := newMemNetwork()
		fake := clock.NewFake(cacheStart)
		lm := cacheStart.Format(http.TimeFormat)
		scr := &cacheScript{}
		scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Last-Modified": {lm}}, "old")
		be := n.serve(t, scr)
		g := n.gateway(t, cacheGatewayDoc(be.URL, "", cacheRouteYAML("r", "stale_if_error: 1h")), withClock(fake))
		cacheDo(g, http.MethodGet, "http://example.com/lm", nil)
		fake.Advance(61 * time.Second)
		scr.set(http.StatusNotModified, http.Header{"Cache-Control": {"max-age=60"}, "Last-Modified": {lm}}, "")
		rec := cacheDo(g, http.MethodGet, "http://example.com/lm", nil)
		if got := scr.lastHeader().Get("If-Modified-Since"); got != lm {
			t.Fatalf("origin If-Modified-Since %q, want %q", got, lm)
		}
		if st := cacheLast(t, rec); !st.Hit || st.Fwd != httpcache.FwdStale {
			t.Fatalf("status %+v, want hit fwd=stale", st)
		}
	})

	t.Run("stale-while-revalidate window serves and revalidates", func(t *testing.T) {
		n := newMemNetwork()
		fake := clock.NewFake(cacheStart)
		scr := &cacheScript{}
		scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "old")
		be := n.serve(t, scr)
		g := n.gateway(t, cacheGatewayDoc(be.URL, "", cacheRouteYAML("r", "stale_while_revalidate: 1h")), withClock(fake))
		cacheDo(g, http.MethodGet, "http://example.com/swr", nil)
		fake.Advance(61 * time.Second)
		scr.set(http.StatusNotModified, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "")
		rec := cacheDo(g, http.MethodGet, "http://example.com/swr", nil)
		if got := scr.lastHeader().Get("If-None-Match"); got != `"v1"` {
			t.Fatalf("origin If-None-Match %q, want \"v1\"", got)
		}
		if rec.Body.String() != "old" {
			t.Fatalf("body %q, want old", rec.Body.String())
		}
		m := metricsText(t, g)
		if v := metricValue(t, m, series("microgateway_cache_requests_total", "route", "r", "result", "revalidated")); v != 1 {
			t.Fatalf("revalidated = %v, want 1", v)
		}
	})
}

func TestCacheUnsafeAndOnlyIfCachedGateway(t *testing.T) {
	t.Parallel()
	for _, scheme := range []string{"http", "https"} {
		t.Run(scheme+" POST 201 removes GET and HEAD entries", func(t *testing.T) {
			n := newMemNetwork()
			scr := &cacheScript{}
			scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
			be := n.serve(t, scr)
			g := n.gateway(t, cacheGatewayDoc(be.URL, "", cacheRouteYAML("r")), withClock(clock.NewFake(cacheStart)))
			target := scheme + "://example.com/u?q=1"
			cacheDo(g, http.MethodGet, target, nil)
			cacheDo(g, http.MethodHead, target, nil)
			if scr.count() != 2 {
				t.Fatalf("fill calls %d, want 2", scr.count())
			}
			scr.set(http.StatusCreated, nil, "")
			cacheDo(g, http.MethodPost, target, nil)
			scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
			if st := cacheLast(t, cacheDo(g, http.MethodGet, target, nil)); st.Hit {
				t.Error("GET hit after unsafe invalidation")
			}
			if st := cacheLast(t, cacheDo(g, http.MethodHead, target, nil)); st.Hit {
				t.Error("HEAD hit after unsafe invalidation")
			}
		})
	}

	t.Run("POST 404 purges nothing", func(t *testing.T) {
		n := newMemNetwork()
		scr := &cacheScript{}
		scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		be := n.serve(t, scr)
		g := n.gateway(t, cacheGatewayDoc(be.URL, "", cacheRouteYAML("r")), withClock(clock.NewFake(cacheStart)))
		cacheDo(g, http.MethodGet, "http://example.com/n404", nil)
		scr.set(http.StatusNotFound, nil, "")
		if rec := cacheDo(g, http.MethodPost, "http://example.com/n404", nil); rec.Code != http.StatusNotFound {
			t.Fatalf("POST status %d, want 404", rec.Code)
		}
		calls := scr.count()
		if st := cacheLast(t, cacheDo(g, http.MethodGet, "http://example.com/n404", nil)); !st.Hit {
			t.Fatalf("GET after POST 404 %+v, want hit", st)
		}
		if scr.count() != calls {
			t.Fatal("GET after POST 404 reached the origin")
		}
	})

	t.Run("only-if-cached", func(t *testing.T) {
		n := newMemNetwork()
		scr := &cacheScript{}
		scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "body")
		be := n.serve(t, scr)
		g := n.gateway(t, cacheGatewayDoc(be.URL, "", cacheRouteYAML("r")), withClock(clock.NewFake(cacheStart)))
		miss := cacheDo(g, http.MethodGet, "http://example.com/oic2", http.Header{"Cache-Control": {"only-if-cached"}})
		if st := cacheLast(t, miss); st.Hit || st.Detail != "only-if-cached" {
			t.Fatalf("only-if-cached miss %+v", st)
		}
		if miss.Code != http.StatusGatewayTimeout || !strings.Contains(miss.Body.String(), httperr.CodeNotCached) {
			t.Fatalf("only-if-cached miss status %d body %q", miss.Code, miss.Body.String())
		}
		if scr.count() != 0 {
			t.Fatalf("origin requests = %d, want 0", scr.count())
		}
		cacheDo(g, http.MethodGet, "http://example.com/oic2", nil)
		hit := cacheDo(g, http.MethodGet, "http://example.com/oic2", http.Header{"Cache-Control": {"only-if-cached"}})
		if st := cacheLast(t, hit); !st.Hit || hit.Code != http.StatusOK {
			t.Fatalf("only-if-cached hit %+v status %d", st, hit.Code)
		}
	})
}
