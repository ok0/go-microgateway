package e2e

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/reqpath"
)

const pathAPIKey = "path-api-key-1"

// pathRecorder is a backend that records the path of every request it receives.
type pathRecorder struct {
	name         string
	cacheControl string
	mu           sync.Mutex
	seen         []echo
}

func (p *pathRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e := echo{Backend: p.name, Path: r.URL.Path, RawPath: r.URL.RawPath}
	p.mu.Lock()
	p.seen = append(p.seen, e)
	p.mu.Unlock()
	if p.cacheControl != "" {
		w.Header().Set("Cache-Control", p.cacheControl)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(e)
}

func (p *pathRecorder) take() []echo {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.seen
	p.seen = nil
	return out
}

func (p *pathRecorder) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.seen)
}

// pathBackends is the set of recorders behind the path matrix gateway and the configuration that routes to them.
type pathBackends struct {
	public, admin, cached, rl *pathRecorder
	doc                       string
}

func newPathBackends(t testing.TB, n *memNetwork) *pathBackends {
	t.Helper()
	b := &pathBackends{
		public: &pathRecorder{name: "public"},
		admin:  &pathRecorder{name: "admin"},
		cached: &pathRecorder{name: "cached", cacheControl: "public, max-age=60"},
		rl:     &pathRecorder{name: "rl"},
	}
	pub := n.serve(t, b.public)
	adm := n.serve(t, b.admin)
	cch := n.serve(t, b.cached)
	rl := n.serve(t, b.rl)
	b.doc = pathDoc(pub.URL, adm.URL, cch.URL, rl.URL)
	return b
}

// takeAll returns and clears the requests every recorder received, in recorder order.
func (b *pathBackends) takeAll() []echo {
	var out []echo
	for _, r := range []*pathRecorder{b.public, b.admin, b.cached, b.rl} {
		out = append(out, r.take()...)
	}
	return out
}

func pathDoc(pub, adm, cch, rl string) string {
	return "version: 1\n" + listenersYAML + fmt.Sprintf(`auth:
  api_keys:
    - name: keys1
      query: api_key
      keys:
        - id: k1
          hash: %q
          consumer: alice
`, auHash(pathAPIKey)) + fmt.Sprintf(`upstreams:
  - name: public
    hosts:
      - url: %q
  - name: admin
    hosts:
      - url: %q
  - name: cached
    hosts:
      - url: %q
  - name: rl
    hosts:
      - url: %q
routes:
  - name: public
    match:
      path: /public/
    upstream: public
  - name: admin
    match:
      path: /admin/
    upstream: admin
    auth:
      mode: apikey
      providers: [keys1]
  - name: cached
    match:
      path: /cached/
    upstream: cached
    cache:
      enabled: true
  - name: rl-a
    match:
      path: /rl/a
      path_type: exact
    upstream: rl
    rate_limit:
      rate: 2
      per: 1h
      key:
        source: ip
  - name: rl-b
    match:
      path: /rl/b
      path_type: exact
    upstream: rl
    rate_limit:
      rate: 2
      per: 1h
      key:
        source: ip
`, pub, adm, cch, rl)
}

// pathExtraRoute is an unrelated route appended to pathDoc for the reload step.
const pathExtraRoute = "  - name: extra\n    match:\n      path: /extra\n    upstream: public\n"

// pathMatrixSpellings are the spellings of the path matrix; only "/admin//x" is a valid request.
var pathMatrixSpellings = []string{
	"/public/../admin/x",
	"/public/%2e%2e/admin/x",
	"/public/%2E%2E/admin/x",
	"/public/..%2fadmin/x",
	"/public/..%5cadmin/x",
	"/public/..\\admin/x",
	"/public/..;/admin/x",
	"/public//../admin/x",
	"/admin//x",
	"/admin/%2e/x",
	"/admin%2fx",
}

// outcome is what a client sees for one request: the status, the error code, and the path a backend echoed.
type outcome struct {
	Status  int
	Code    string
	Backend string
	Path    string
	Raw     string
}

// pathReq builds a GET request for target, which must parse as a request URI.
func pathReq(t testing.TB, method, target, remote string) *http.Request {
	t.Helper()
	u, err := url.ParseRequestURI(target)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(method, "http://example.com/", nil)
	req.URL.Path = u.Path
	req.URL.RawPath = u.RawPath
	req.URL.RawQuery = u.RawQuery
	req.RequestURI = target
	req.RemoteAddr = remote
	return req
}

// urlReq builds a request whose URL fields are set directly, for paths that do not parse as a request URI.
func urlReq(method, path, raw, remote string) *http.Request {
	req := httptest.NewRequest(method, "http://example.com/", nil)
	req.URL.Path = path
	req.URL.RawPath = raw
	req.RemoteAddr = remote
	return req
}

// pathTarget parses s for seeding; it fails t when s does not parse.
func pathTarget(t testing.TB, s string) *url.URL {
	t.Helper()
	u, err := url.ParseRequestURI(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// pathEscaped is the escaped form of a decoded path with the given raw form; an empty raw form is the default escaping.
func pathEscaped(decoded, raw string) string {
	if raw != "" {
		return raw
	}
	return (&url.URL{Path: decoded}).EscapedPath()
}

// escaped is the escaped form of a canonical path.
func escaped(p reqpath.Path) string {
	return pathEscaped(p.Decoded, p.Raw)
}

// pathBackend is the name of the backend that the canonical decoded path selects; empty when no route does.
func pathBackend(decoded string) string {
	switch {
	case strings.HasPrefix(decoded, "/public/"):
		return "public"
	case strings.HasPrefix(decoded, "/admin/"):
		return "admin"
	case strings.HasPrefix(decoded, "/cached/"):
		return "cached"
	case decoded == "/rl/a" || decoded == "/rl/b":
		return "rl"
	}
	return ""
}

// pathDotSegment reports whether p has a "." or ".." segment, where a segment ends at its first ";".
func pathDotSegment(p string) bool {
	for _, seg := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		seg, _, _ = strings.Cut(seg, ";")
		if seg == "." || seg == ".." {
			return true
		}
	}
	return false
}

// pathDo serves req through g and returns the outcome and the raw response body.
func pathDo(t testing.TB, g http.Handler, req *http.Request) (outcome, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	body := rec.Body.String()
	o := outcome{Status: rec.Code}
	if rec.Code == http.StatusOK {
		var e echo
		if err := json.Unmarshal([]byte(body), &e); err != nil {
			t.Fatalf("200 body %q: %v", body, err)
		}
		o.Backend, o.Path, o.Raw = e.Backend, e.Path, e.RawPath
		return o, body
	}
	var eb struct {
		Error apiError `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &eb); err != nil {
		t.Fatalf("status %d body %q: %v", rec.Code, body, err)
	}
	o.Code = eb.Error.Code
	return o, body
}

// pathCheckServed fails t unless the 200 answer o echoes the canonical form of the request URL u and came from the route that the canonical path selects.
func pathCheckServed(t testing.TB, u *url.URL, o outcome) {
	t.Helper()
	want, err := reqpath.Canonicalize(u)
	if err != nil {
		t.Fatalf("request reached a backend but its path is rejected: %v", err)
	}
	if o.Path != want.Decoded || pathEscaped(o.Path, o.Raw) != escaped(want) {
		t.Fatalf("backend saw path %q (raw %q), want canonical %q (escaped %q)", o.Path, o.Raw, want.Decoded, escaped(want))
	}
	if got := pathBackend(want.Decoded); o.Backend != got {
		t.Fatalf("backend = %q for canonical path %q, want %q", o.Backend, want.Decoded, got)
	}
}

// pathRun sends the path matrix, the cache class and the rate-limit class through g and returns the outcomes of the
// matrix and the rate-limit class. tag makes the cache paths of this run distinct; remote is the client address.
func pathRun(t *testing.T, b *pathBackends, g *microgateway.Gateway, tag, remote string) []outcome {
	t.Helper()
	b.takeAll()
	var out []outcome
	for _, sp := range pathMatrixSpellings {
		for _, key := range []string{"", pathAPIKey} {
			req := pathReq(t, http.MethodGet, sp, remote)
			if key != "" {
				req.URL.RawQuery = "api_key=" + key
			}
			_, werr := reqpath.Canonicalize(req.URL)
			before := b.admin.count()
			o, body := pathDo(t, g, req)
			if key == "" && b.admin.count() != before {
				t.Fatalf("%s without credentials reached the admin backend", sp)
			}
			if key == "" && o.Status == http.StatusUnauthorized && strings.Contains(body, "admin") {
				t.Fatalf("%s: unauthenticated body names a backend: %s", sp, body)
			}
			if o.Status == http.StatusOK {
				if werr != nil {
					t.Fatalf("%s answered 200 but its path is rejected: %v", sp, werr)
				}
				pathCheckServed(t, req.URL, o)
			}
			out = append(out, o)
		}
	}

	cacheRemote := "192.0.2.90:1000"
	p := "/cached/" + tag
	before := b.cached.count()
	var bodies []string
	for _, sp := range []string{p + "/a~x", p + "/a%7Ex", p + "/a%7ex", p + "//a~x"} {
		req := pathReq(t, http.MethodGet, sp, cacheRemote)
		o, body := pathDo(t, g, req)
		if o.Status != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", sp, o.Status)
		}
		pathCheckServed(t, req.URL, o)
		bodies = append(bodies, body)
	}
	if got := b.cached.count() - before; got != 1 {
		t.Fatalf("backend fills for the ~ spellings = %d, want 1", got)
	}
	for _, body := range bodies[1:] {
		if body != bodies[0] {
			t.Fatalf("cached bodies differ: %q and %q", bodies[0], body)
		}
	}
	before = b.cached.count()
	for _, sp := range []string{p + "/a;x", p + "/a%3Bx"} {
		req := pathReq(t, http.MethodGet, sp, cacheRemote)
		o, _ := pathDo(t, g, req)
		if o.Status != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", sp, o.Status)
		}
		pathCheckServed(t, req.URL, o)
		if strings.HasSuffix(sp, "%3Bx") && o.Raw != p+"/a%3Bx" {
			t.Fatalf("%s: backend raw path = %q, want %q", sp, o.Raw, p+"/a%3Bx")
		}
	}
	if got := b.cached.count() - before; got != 2 {
		t.Fatalf("backend fills for the ; spellings = %d, want 2", got)
	}

	for _, sp := range []string{"/rl/a", "/rl//a", "/rl/%61", "/rl/b"} {
		req := pathReq(t, http.MethodGet, sp, remote)
		o, _ := pathDo(t, g, req)
		if o.Status == http.StatusOK {
			pathCheckServed(t, req.URL, o)
		}
		out = append(out, o)
	}
	return out
}

// pathMatrixWant is the outcome table of pathRun for a fresh gateway.
func pathMatrixWant() []outcome {
	var out []outcome
	for _, sp := range pathMatrixSpellings {
		if sp == "/admin//x" {
			out = append(out,
				outcome{Status: http.StatusUnauthorized, Code: httperr.CodeUnauthorized},
				outcome{Status: http.StatusOK, Backend: "admin", Path: "/admin/x"},
			)
			continue
		}
		bad := outcome{Status: http.StatusBadRequest, Code: httperr.CodeInvalidPath}
		out = append(out, bad, bad)
	}
	return append(out,
		outcome{Status: http.StatusOK, Backend: "rl", Path: "/rl/a"},
		outcome{Status: http.StatusOK, Backend: "rl", Path: "/rl/a"},
		outcome{Status: http.StatusTooManyRequests, Code: httperr.CodeRateLimited},
		outcome{Status: http.StatusOK, Backend: "rl", Path: "/rl/b"},
	)
}

func TestPathSpellingsMatrix(t *testing.T) {
	n := newMemNetwork()
	b := newPathBackends(t, n)
	g1 := n.gateway(t, b.doc)
	g2 := n.gateway(t, b.doc)

	first := pathRun(t, b, g1, "one", "192.0.2.10:1000")
	second := pathRun(t, b, g2, "two", "192.0.2.11:1000")
	if err := g1.ReloadYAML([]byte(b.doc + pathExtraRoute)); err != nil {
		t.Fatal(err)
	}
	third := pathRun(t, b, g1, "three", "192.0.2.12:1000")

	want := pathMatrixWant()
	for _, run := range []struct {
		name string
		got  []outcome
	}{{"fresh gateway", first}, {"second gateway", second}, {"after reload", third}} {
		if !slices.Equal(run.got, want) {
			t.Fatalf("%s: outcomes = %+v, want %+v", run.name, run.got, want)
		}
	}
}

// pathRawGet sends one HTTP/1.1 request line for target to s over a new connection and returns the status and body.
func pathRawGet(t *testing.T, n *memNetwork, s *testServer, target string) (int, string) {
	t.Helper()
	conn := n.dial(t, s)
	if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: gw.test\r\nConnection: close\r\n\r\n", target); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

func TestPathOverListener(t *testing.T) {
	n := newMemNetwork()
	b := newPathBackends(t, n)
	g := n.gateway(t, b.doc)
	s := n.serve(t, g)

	for _, target := range []string{"/public/..%2fadmin/x", "/public/../admin/x", "/public/%2e%2e/admin/x"} {
		code, body := pathRawGet(t, n, s, target)
		var eb struct {
			Error apiError `json:"error"`
		}
		if err := json.Unmarshal([]byte(body), &eb); err != nil {
			t.Fatalf("%s: body %q: %v", target, body, err)
		}
		if code != http.StatusBadRequest || eb.Error.Code != httperr.CodeInvalidPath {
			t.Fatalf("%s: status = %d code = %q, want 400 %s", target, code, eb.Error.Code, httperr.CodeInvalidPath)
		}
	}
	if got := b.takeAll(); len(got) != 0 {
		t.Fatalf("rejected requests reached backends: %+v", got)
	}

	code, body := pathRawGet(t, n, s, "/admin//x?api_key="+pathAPIKey)
	if code != http.StatusOK {
		t.Fatalf("keyed /admin//x status = %d body %q, want 200", code, body)
	}
	var e echo
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		t.Fatal(err)
	}
	if e.Backend != "admin" || e.Path != "/admin/x" {
		t.Fatalf("keyed /admin//x reached %q at %q, want admin at /admin/x", e.Backend, e.Path)
	}
}

func TestPathRejectedSkipsEveryStage(t *testing.T) {
	n := newMemNetwork()
	rec := &pathRecorder{name: "rec"}
	be := n.serve(t, rec)
	var calls atomic.Int64
	count := func(microgateway.MiddlewareParams) (microgateway.Middleware, error) {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				next.ServeHTTP(w, r)
			})
		}, nil
	}
	doc := routeDoc(be.URL, "    middlewares: [m]\n") + "middlewares:\n  - name: m\n    type: count\n"
	g := n.gateway(t, doc, microgateway.WithMiddleware("count", count))

	const remote = "192.0.2.1:1000"
	rejected := []struct {
		name string
		req  *http.Request
	}{
		{"encoded percent escape", urlReq(http.MethodGet, "/x", "/x%zz", remote)},
		{"control byte", urlReq(http.MethodGet, "/x\x00", "", remote)},
		{"raw path mismatch", urlReq(http.MethodGet, "/x", "/y", remote)},
	}
	for _, c := range rejected {
		c.req.Header.Set("X-Request-ID", "pathcheck1")
		hr := httptest.NewRecorder()
		g.ServeHTTP(hr, c.req)
		if hr.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", c.name, hr.Code)
		}
		if ct := hr.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("%s: Content-Type = %q, want application/json", c.name, ct)
		}
		body := hr.Body.String()
		var eb struct {
			Error apiError `json:"error"`
		}
		if err := json.Unmarshal([]byte(body), &eb); err != nil {
			t.Fatalf("%s: body %q: %v", c.name, body, err)
		}
		if eb.Error.Code != httperr.CodeInvalidPath || eb.Error.RequestID != "pathcheck1" || eb.Error.Message != httperr.Message(httperr.CodeInvalidPath) {
			t.Fatalf("%s: error = %+v", c.name, eb.Error)
		}
		for _, bad := range []string{"admin", "public", "x%zz"} {
			if strings.Contains(body, bad) {
				t.Fatalf("%s: body names %q: %s", c.name, bad, body)
			}
		}
	}
	if calls.Load() != 0 || rec.count() != 0 {
		t.Fatalf("rejected requests ran middleware %d times and reached the backend %d times", calls.Load(), rec.count())
	}

	opt := httptest.NewRecorder()
	g.ServeHTTP(opt, urlReq(http.MethodOptions, "*", "", remote))
	if opt.Code != http.StatusNotFound {
		t.Fatalf("OPTIONS * status = %d, want 404", opt.Code)
	}
	var eb struct {
		Error apiError `json:"error"`
	}
	if err := json.Unmarshal(opt.Body.Bytes(), &eb); err != nil {
		t.Fatal(err)
	}
	if eb.Error.Code != httperr.CodeRouteNotFound {
		t.Fatalf("OPTIONS * code = %q, want %s", eb.Error.Code, httperr.CodeRouteNotFound)
	}
	if calls.Load() != 0 || rec.count() != 0 {
		t.Fatalf("OPTIONS * ran middleware %d times and reached the backend %d times", calls.Load(), rec.count())
	}

	_, _ = serve(t, g, pathReq(t, http.MethodGet, "/x", remote))
	if calls.Load() != 1 || rec.count() != 1 {
		t.Fatalf("positive control: middleware %d times, backend %d times, want 1 and 1", calls.Load(), rec.count())
	}

	metric := series("microgateway_requests_total", "route", "unmatched", "method", "GET", "code", "400")
	if got := metricValue(t, metricsText(t, g), metric); got != float64(len(rejected)) {
		t.Fatalf("%s = %v, want %d", metric, got, len(rejected))
	}
}

func TestPathRejectedAccessLog(t *testing.T) {
	n := newMemNetwork()
	be := n.serve(t, echoHandler("up"))
	h := &captureHandler{}
	g := n.gateway(t, routeDoc(be.URL, ""), microgateway.WithLogger(slog.New(h)))

	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, pathReq(t, http.MethodGet, "/public/..%2fadmin/x", "192.0.2.1:1000"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("rejected status = %d, want 400", rec.Code)
	}
	reqs := h.requests()
	if len(reqs) != 1 {
		t.Fatalf("request records = %d, want 1", len(reqs))
	}
	a := recordAttrs(reqs[0])
	if got := a["path"].String(); got != "/public/../admin/x" {
		t.Fatalf("path = %q, want the received path", got)
	}
	if got := a["status"].Int64(); got != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", got)
	}
	if got := a["route"].String(); got != "" {
		t.Fatalf("route = %q, want empty", got)
	}
	if got := a["error"].String(); got != httperr.CodeInvalidPath {
		t.Fatalf("error = %q, want %s", got, httperr.CodeInvalidPath)
	}

	rec = httptest.NewRecorder()
	g.ServeHTTP(rec, pathReq(t, http.MethodGet, "/public//x", "192.0.2.1:1000"))
	reqs = h.requests()
	if len(reqs) != 2 {
		t.Fatalf("request records = %d, want 2", len(reqs))
	}
	a = recordAttrs(reqs[1])
	if got := a["path"].String(); got != "/public//x" {
		t.Fatalf("path = %q, want the received path /public//x", got)
	}
	if _, ok := a["error"]; ok {
		t.Fatal("accepted request has an error attribute")
	}
}

func TestPathRewriteAndHashUseCanonical(t *testing.T) {
	n := newMemNetwork()
	strip := n.serve(t, &pathRecorder{name: "strip"})
	rep := n.serve(t, &pathRecorder{name: "rep"})
	h1 := n.serve(t, &pathRecorder{name: "h1"})
	h2 := n.serve(t, &pathRecorder{name: "h2"})
	doc := "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: strip
    hosts:
      - url: %q
  - name: rep
    hosts:
      - url: %q
  - name: hash
%s    hosts:
      - url: %q
      - url: %q
routes:
  - name: strip
    match:
      path: /strip/
    upstream: strip
    rewrite:
      strip_prefix: /strip
  - name: rep
    match:
      path: /v1/
    upstream: rep
    rewrite:
      replace_prefix:
        from: /v1
        to: /api/v2
  - name: hash
    match:
      path: /hash/
    upstream: hash
`, strip.URL, rep.URL, balHashLB("path"), h1.URL, h2.URL)
	g := n.gateway(t, doc)

	served := func(target string) outcome {
		t.Helper()
		req := pathReq(t, http.MethodGet, target, "192.0.2.60:1000")
		u := req.URL
		before := *req.URL
		o, _ := pathDo(t, g, req)
		if req.URL != u || *req.URL != before {
			t.Fatalf("ServeHTTP changed the request URL of %s", target)
		}
		if o.Status != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", target, o.Status)
		}
		return o
	}

	if o := served("/strip//a%7Eb"); o.Path != "/a~b" || o.Raw != "" {
		t.Fatalf("/strip//a%%7Eb reached %q (raw %q), want /a~b", o.Path, o.Raw)
	}
	if o := served("/strip/a%3bb"); o.Path != "/a;b" || o.Raw != "/a%3Bb" {
		t.Fatalf("/strip/a%%3bb reached %q (raw %q), want /a;b (raw /a%%3Bb)", o.Path, o.Raw)
	}
	if o := served("/v1//x"); o.Path != "/api/v2/x" || o.Raw != "" {
		t.Fatalf("/v1//x reached %q (raw %q), want /api/v2/x", o.Path, o.Raw)
	}

	for i := 1; i <= 8; i++ {
		key := fmt.Sprintf("k~%d", i)
		spellings := []string{
			"/hash/" + key,
			fmt.Sprintf("/hash/k%%7E%d", i),
			"/hash//" + key,
			fmt.Sprintf("/hash///k%%7e%d", i),
		}
		first := served(spellings[0])
		if first.Path != "/hash/"+key {
			t.Fatalf("%s reached %q, want /hash/%s", spellings[0], first.Path, key)
		}
		for _, sp := range spellings[1:] {
			if o := served(sp); o.Backend != first.Backend {
				t.Fatalf("%s reached %q, but %s reached %q", sp, o.Backend, spellings[0], first.Backend)
			}
		}
	}
}

func TestPathRewritePrefixValidation(t *testing.T) {
	n := newMemNetwork()
	be := n.serve(t, echoHandler("up"))
	head := gbHead(be.URL)
	routes := "routes:\n  - name: strip\n    match:\n      path: /strip\n    upstream: up\n    rewrite:\n      strip_prefix: /strip\n" +
		"  - name: other\n    match:\n      path: /other\n    upstream: up\n"
	cfg := gbParse(t, head+routes)
	cfg.Routes[0].Rewrite.StripPrefix = "/a/./b"
	cfg.Routes[1].Upstream = "missing"
	wantPaths := []string{"routes[0].rewrite.strip_prefix", "routes[1].upstream"}
	check := func(step string, err error) {
		t.Helper()
		errs := opErrors(t, err)
		for _, want := range wantPaths {
			if !slices.ContainsFunc(errs, func(fe config.FieldError) bool { return fe.Path == want }) {
				t.Fatalf("%s: errors %+v lack %s", step, errs, want)
			}
		}
	}
	check("Validate", microgateway.Validate(cfg))
	_, err := newGateway(cfg)
	check("New", err)

	valid := head + "routes:\n  - name: strip\n    match:\n      path: /strip\n    upstream: up\n    rewrite:\n      strip_prefix: /strip\n"
	bad := head + "routes:\n  - name: strip\n    match:\n      path: /strip\n    upstream: up\n    rewrite:\n      strip_prefix: /strip//x\n" +
		"  - name: other\n    match:\n      path: /other\n    upstream: missing\n"
	g := n.gateway(t, valid)
	err = g.ReloadYAML([]byte(bad))
	errs := opErrors(t, err)
	lines := map[string]int{
		"routes[0].rewrite.strip_prefix": pathLineOf(bad, "strip_prefix: /strip//x"),
		"routes[1].upstream":             pathLineOf(bad, "upstream: missing"),
	}
	for path, line := range lines {
		if !slices.ContainsFunc(errs, func(fe config.FieldError) bool { return fe.Path == path && fe.Line == line }) {
			t.Fatalf("reload errors %+v lack %s at line %d", errs, path, line)
		}
	}

	if _, e := serve(t, g, pathReq(t, http.MethodGet, "/strip/y", "192.0.2.70:1000")); e.Path != "/y" {
		t.Fatalf("old config served /strip/y at %q, want /y", e.Path)
	}
	if got := metricsHasLine(metricsText(t, g), `microgateway_config_reloads_total{result="failure"} 1`); !got {
		t.Fatal("reloads_total{result=\"failure\"} is not 1 after the rejected reload")
	}
}

// pathLineOf returns the 1-based number of the first line of src that contains substr, or 0.
func pathLineOf(src, substr string) int {
	for i, l := range strings.Split(src, "\n") {
		if strings.Contains(l, substr) {
			return i + 1
		}
	}
	return 0
}

func FuzzRoutedPathEqualsForwarded(f *testing.F) {
	n := newMemNetwork()
	b := newPathBackends(f, n)
	g := n.gateway(f, b.doc)

	seeds := append([]string{}, pathMatrixSpellings...)
	seeds = append(seeds, "/public/x", "/admin/x", "/cached/a%7Ex", "/rl/a", "/public/a%3Bb")
	for _, s := range seeds {
		u := pathTarget(f, s)
		f.Add(u.Path, u.RawPath, false)
		f.Add(u.Path, u.RawPath, true)
	}
	f.Add("*", "", false)
	f.Add("/public/\x00", "", false)
	f.Add("/public/a", "/public/%zz", false)

	f.Fuzz(func(t *testing.T, path, raw string, withKey bool) {
		b.takeAll()
		req := urlReq(http.MethodGet, path, raw, "192.0.2.50:1000")
		if withKey {
			req.URL.RawQuery = "api_key=" + pathAPIKey
		}
		want, werr := reqpath.Canonicalize(&url.URL{Path: path, RawPath: raw})
		adminBefore := b.admin.count()
		o, _ := pathDo(t, g, req)
		adminHit := b.admin.count() != adminBefore
		seen := b.takeAll()

		if werr != nil {
			if o.Status != http.StatusBadRequest || o.Code != httperr.CodeInvalidPath {
				t.Fatalf("rejected path answered %d %q, want 400 %s", o.Status, o.Code, httperr.CodeInvalidPath)
			}
			if len(seen) != 0 {
				t.Fatalf("rejected path reached a backend: %+v", seen)
			}
			return
		}
		if o.Status >= 400 && o.Status < 500 {
			if !withKey && adminHit {
				t.Fatalf("unauthenticated request reached the admin backend (status %d)", o.Status)
			}
			return
		}
		if o.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200", o.Status)
		}
		if pathDotSegment(want.Decoded) {
			t.Fatalf("accepted canonical path %q has a dot segment", want.Decoded)
		}
		if o.Path != want.Decoded || pathEscaped(o.Path, o.Raw) != escaped(want) {
			t.Fatalf("backend saw path %q (raw %q), want canonical %q (escaped %q)", o.Path, o.Raw, want.Decoded, escaped(want))
		}
		if got := pathBackend(want.Decoded); o.Backend != got {
			t.Fatalf("backend = %q, want %q for canonical path %q", o.Backend, got, want.Decoded)
		}
		if o.Backend == "admin" && !withKey {
			t.Fatal("admin backend answered a request without credentials")
		}
		if len(seen) > 1 {
			t.Fatalf("backends received %d requests for one call: %+v", len(seen), seen)
		}
		if len(seen) == 1 && (seen[0].Backend != o.Backend || seen[0].Path != o.Path || seen[0].RawPath != o.Raw) {
			t.Fatalf("backend received %+v, client got %+v", seen[0], o)
		}
	})
}
