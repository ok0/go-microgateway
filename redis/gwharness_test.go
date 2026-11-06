package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	microgateway "github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/hooks"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/testwait"
)

func upstream(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func gwDoc(stores, routes, backendURL string) string {
	var b strings.Builder
	b.WriteString("version: 1\n")
	if stores != "" {
		b.WriteString("stores:\n" + stores)
	}
	b.WriteString("listeners:\n  - name: public\n    address: \"127.0.0.1:0\"\n")
	fmt.Fprintf(&b, "upstreams:\n  - name: up\n    hosts:\n      - url: %q\n", backendURL)
	b.WriteString("routes:\n" + routes)
	return b.String()
}

func indentLines(s string, n int) string {
	pad := strings.Repeat(" ", n)
	var b strings.Builder
	for _, line := range strings.SplitAfter(s, "\n") {
		if line == "" {
			continue
		}
		b.WriteString(pad)
		b.WriteString(line)
	}
	return b.String()
}

func redisStoreYAML(name string, addrs []string, redisExtra, guardExtra string) string {
	quoted := make([]string, len(addrs))
	for i, a := range addrs {
		quoted[i] = strconv.Quote(a)
	}
	if guardExtra == "" {
		guardExtra = "consecutive_failures: 3\nopen_timeout: 2s\n"
	}
	return "  - name: " + name + "\n    type: redis\n    redis:\n      addresses: [" + strings.Join(quoted, ", ") + "]\n" +
		indentLines(redisExtra, 6) +
		"    guard:\n" + indentLines(guardExtra, 6)
}

func routeYAML(name, rateLimitBody string) string {
	return "  - name: " + name + "\n    match:\n      path: /" + name + "\n    upstream: up\n    rate_limit:\n" +
		indentLines(rateLimitBody, 6)
}

func newGateway(t *testing.T, doc string, register bool, clk clock.Clock, logger *slog.Logger, opts ...microgateway.Option) (*microgateway.Gateway, error) {
	t.Helper()
	all := append([]microgateway.Option(nil), opts...)
	if register {
		all = append(all, Option())
	}
	if clk != nil {
		all = append(all, hooks.WithClock(clk).(microgateway.Option))
	}
	if logger != nil {
		all = append(all, microgateway.WithLogger(logger))
	}
	g, err := microgateway.NewFromYAML([]byte(doc), all...)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = g.Close() })
	return g, nil
}

func mustGateway(t *testing.T, doc string, clk clock.Clock, logger *slog.Logger) *microgateway.Gateway {
	t.Helper()
	g, err := newGateway(t, doc, true, clk, logger)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func doGet(g http.Handler, path, remote string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.RemoteAddr = remote
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	return rec
}

// getGuarded serves GET path from remote on g in its own goroutine and waits for it under a hang guard.
func getGuarded(t *testing.T, g http.Handler, path, remote string) *httptest.ResponseRecorder {
	t.Helper()
	var rec *httptest.ResponseRecorder
	done := make(chan struct{})
	go func() {
		defer close(done)
		rec = doGet(g, path, remote)
	}()
	testwait.Done(t, "GET "+path+" to complete", done, nil)
	return rec
}

// getAdvancing is getGuarded for a gateway on fake: while the request is pending it advances fake by step whenever a
// timer of fake is due within step. It returns the recorder and the fake time that passed while the request ran.
func getAdvancing(t *testing.T, g http.Handler, fake *clock.Fake, step time.Duration, path, remote string) (*httptest.ResponseRecorder, time.Duration) {
	t.Helper()
	start := fake.Now()
	var rec *httptest.ResponseRecorder
	done := make(chan struct{})
	go func() {
		defer close(done)
		rec = doGet(g, path, remote)
	}()
	testwait.Until(t, "GET "+path+" to complete", func() bool {
		select {
		case <-done:
			return true
		default:
		}
		if fake.Due(step) > 0 {
			fake.Advance(step)
		}
		return false
	}, func() string {
		return fmt.Sprintf("fake clock advanced %v; timers due within %v: %d", fake.Since(start), step, fake.Due(step))
	})
	return rec, fake.Since(start)
}

func responseErrorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body is not a JSON error: %v", err)
	}
	return body.Error.Code
}

func metricsText(t *testing.T, g *microgateway.Gateway) string {
	t.Helper()
	rec := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics: status %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

func seriesValue(body, name string, labels map[string]string) (float64, bool) {
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, name+"{") {
			continue
		}
		matched := true
		for k, v := range labels {
			if !strings.Contains(line, k+"=\""+v+"\"") {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		idx := strings.LastIndexByte(line, ' ')
		if idx < 0 {
			return 0, false
		}
		v, err := strconv.ParseFloat(line[idx+1:], 64)
		if err != nil {
			return 0, false
		}
		return v, true
	}
	return 0, false
}

func checkExposition(t *testing.T, body string) {
	t.Helper()
	series := regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*(\{.*\})?$`)
	for _, line := range strings.Split(body, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.LastIndexByte(line, ' ')
		if idx < 0 {
			t.Errorf("exposition line has no value: %q", line)
			continue
		}
		if !series.MatchString(line[:idx]) {
			t.Errorf("exposition series is malformed: %q", line[:idx])
		}
		if _, err := strconv.ParseFloat(line[idx+1:], 64); err != nil {
			t.Errorf("exposition value is not a float: %q", line)
		}
	}
}

const gcClientAddr = "192.0.2.30:4000"

const gcToken = "gc-admin-7Hq2Lw"

const gcLastModified = "Fri, 02 Jan 2026 03:04:05 GMT"

type gcOrigin struct {
	url     string
	arrived chan struct{}

	mu     sync.Mutex
	status int
	header http.Header
	body   string
	tags   map[string]string
	calls  int
	cond   int
	last   http.Header
	gate   chan struct{}
}

func newGCOrigin(t *testing.T) *gcOrigin {
	t.Helper()
	o := &gcOrigin{status: http.StatusOK, body: "v1", tags: make(map[string]string), arrived: make(chan struct{}, 256)}
	srv := httptest.NewServer(o)
	t.Cleanup(srv.Close)
	o.url = srv.URL
	return o
}

func (o *gcOrigin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	o.mu.Lock()
	o.calls++
	if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
		o.cond++
	}
	o.last = r.Header.Clone()
	gate := o.gate
	o.mu.Unlock()
	if gate != nil {
		select {
		case o.arrived <- struct{}{}:
		default:
		}
		<-gate
	}
	o.mu.Lock()
	status, header, body, tags := o.status, o.header.Clone(), o.body, o.tags[r.URL.Path]
	o.mu.Unlock()
	for k, v := range header {
		w.Header()[k] = v
	}
	if tags != "" {
		w.Header().Set("Surrogate-Key", tags)
	}
	w.WriteHeader(status)
	if status != http.StatusNotModified {
		_, _ = io.WriteString(w, body+"|"+r.Header.Get("Accept-Language"))
	}
}

func (o *gcOrigin) set(status int, header http.Header, body string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.status, o.header, o.body = status, header.Clone(), body
}

func (o *gcOrigin) tag(path, tags string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.tags[path] = tags
}

func (o *gcOrigin) hold() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.gate = make(chan struct{})
}

func (o *gcOrigin) release() {
	o.mu.Lock()
	gate := o.gate
	o.gate = nil
	o.mu.Unlock()
	if gate != nil {
		close(gate)
	}
}

func (o *gcOrigin) counts() (calls, cond int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls, o.cond
}

func (o *gcOrigin) lastHeader() http.Header {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.last.Clone()
}

type gcEntry struct {
	method string
	path   string
	lang   string
}

func (e gcEntry) url() string {
	return "http://example.com" + e.path
}

func (e gcEntry) header() http.Header {
	if e.lang == "" {
		return nil
	}
	return http.Header{"Accept-Language": {e.lang}}
}

func gcRoute(name string, extra ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "  - name: %s\n    match:\n      path: /\n    upstream: up\n    cache:\n      enabled: true\n", name)
	for _, line := range extra {
		if line != "" {
			b.WriteString("      " + line + "\n")
		}
	}
	return b.String()
}

func gcDoc(stores, cacheTop, admin, routes, backendURL string) string {
	var b strings.Builder
	b.WriteString("version: 1\n")
	if stores != "" {
		b.WriteString("stores:\n" + stores)
	}
	if cacheTop != "" {
		b.WriteString("cache:\n" + cacheTop)
	}
	b.WriteString(admin)
	b.WriteString("listeners:\n  - name: public\n    address: \"127.0.0.1:0\"\n")
	fmt.Fprintf(&b, "upstreams:\n  - name: up\n    hosts:\n      - url: %q\n", backendURL)
	b.WriteString("routes:\n" + routes)
	return b.String()
}

func gcAdminYAML(tokens ...string) string {
	if len(tokens) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("admin:\n  auth:\n    keys:\n")
	for i, tok := range tokens {
		sum := sha256.Sum256([]byte(tok))
		fmt.Fprintf(&b, "      - id: key-%d\n        hash: \"sha256:%s\"\n", i, hex.EncodeToString(sum[:]))
	}
	return b.String()
}

func gcPurge(t *testing.T, g *microgateway.Gateway, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/cache/purge", strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(rec, req)
	return rec
}

type gcReply struct {
	Type     string `json:"type"`
	Soft     bool   `json:"soft"`
	Accepted int    `json:"accepted"`
	Affected *int   `json:"affected,omitempty"`
	Scope    string `json:"scope"`
}

func (r gcReply) String() string {
	affected := "omitted"
	if r.Affected != nil {
		affected = strconv.Itoa(*r.Affected)
	}
	return fmt.Sprintf("{type=%s soft=%t accepted=%d affected=%s scope=%s}", r.Type, r.Soft, r.Accepted, affected, r.Scope)
}

func gcPurgeOK(t *testing.T, rec *httptest.ResponseRecorder) gcReply {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("purge status %d, want 200: %s", rec.Code, rec.Body.String())
	}
	dec := json.NewDecoder(strings.NewReader(rec.Body.String()))
	dec.DisallowUnknownFields()
	var out gcReply
	if err := dec.Decode(&out); err != nil {
		t.Fatalf("purge reply %q: %v", rec.Body.String(), err)
	}
	return out
}

func gcStatus(t *testing.T, rec *httptest.ResponseRecorder) httpcache.Status {
	t.Helper()
	sts, err := httpcache.ParseStatus(strings.Join(rec.Header().Values("Cache-Status"), ", "))
	if err != nil {
		t.Fatalf("Cache-Status %q: %v", rec.Header().Values("Cache-Status"), err)
	}
	if len(sts) == 0 {
		t.Fatal("response has no Cache-Status")
	}
	return sts[len(sts)-1]
}

func gcRequest(h http.Handler, method, rawURL string, hdr http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, rawURL, nil)
	req.RemoteAddr = gcClientAddr
	for k, v := range hdr {
		req.Header[k] = v
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func gcFetch(t *testing.T, h http.Handler, e gcEntry) (*httptest.ResponseRecorder, httpcache.Status) {
	t.Helper()
	rec := gcRequest(h, e.method, e.url(), e.header())
	return rec, gcStatus(t, rec)
}

func gcSnapshot(t *testing.T, c goredis.UniversalClient, prefix string, expiryMs func(key string) int64) map[string]string {
	t.Helper()
	ctx := context.Background()
	out := make(map[string]string)
	for _, k := range ckKeys(t, c, prefix) {
		typ, err := c.Type(ctx, k).Result()
		if err != nil {
			t.Fatalf("TYPE %s: %v", k, err)
		}
		var val string
		switch typ {
		case "hash":
			fields, err := c.HGetAll(ctx, k).Result()
			if err != nil {
				t.Fatalf("HGETALL %s: %v", k, err)
			}
			names := make([]string, 0, len(fields))
			for f := range fields {
				names = append(names, f)
			}
			sort.Strings(names)
			var b strings.Builder
			for _, f := range names {
				fmt.Fprintf(&b, "%s=%s\n", f, fields[f])
			}
			val = b.String()
		case "string":
			v, err := c.Get(ctx, k).Result()
			if err != nil {
				t.Fatalf("GET %s: %v", k, err)
			}
			val = v
		default:
			t.Fatalf("%s has type %s; the snapshot reads hash and string keys", k, typ)
		}
		out[k] = fmt.Sprintf("%s pexpireat=%d\n%s", typ, expiryMs(k), val)
	}
	return out
}

// checkSnapshotDetects writes keys under prefix and checks that gcSnapshot reads their absolute expiries exactly
// through expiryMs, is stable on an unchanged store, and changes with any expiry, value or key-set change.
func checkSnapshotDetects(t *testing.T, c goredis.UniversalClient, prefix string, expiryMs func(key string) int64) {
	t.Helper()
	ctx := context.Background()
	now, err := c.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	at := now.Add(time.Hour).UnixMilli()
	h, s, n := prefix+":snap:h", prefix+":snap:s", prefix+":snap:n"
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(c.HSet(ctx, h, "a", "1", "b", "2").Err())
	must(c.PExpireAt(ctx, h, time.UnixMilli(at)).Err())
	must(c.Set(ctx, s, "v", 0).Err())
	must(c.PExpireAt(ctx, s, time.UnixMilli(at+1)).Err())
	must(c.Set(ctx, n, "x", 0).Err())
	snap := func() map[string]string { return gcSnapshot(t, c, prefix, expiryMs) }
	before := snap()
	want := map[string]string{
		h: fmt.Sprintf("hash pexpireat=%d\na=1\nb=2\n", at),
		s: fmt.Sprintf("string pexpireat=%d\nv", at+1),
		n: "string pexpireat=-1\nx",
	}
	if !reflect.DeepEqual(before, want) {
		t.Fatalf("snapshot = %q, want %q", before, want)
	}
	if after := snap(); !reflect.DeepEqual(after, before) {
		t.Fatalf("snapshot of an unchanged store = %q, want %q", after, before)
	}
	changed := func(what string, change, restore func() error) {
		t.Helper()
		must(change())
		if reflect.DeepEqual(snap(), before) {
			t.Fatalf("snapshot did not detect %s", what)
		}
		must(restore())
		if after := snap(); !reflect.DeepEqual(after, before) {
			t.Fatalf("snapshot after restoring %s = %q, want %q", what, after, before)
		}
	}
	changed("an expiry moved by 1 ms",
		func() error { return c.PExpireAt(ctx, s, time.UnixMilli(at+2)).Err() },
		func() error { return c.PExpireAt(ctx, s, time.UnixMilli(at+1)).Err() })
	changed("an expiry removed",
		func() error { return c.Persist(ctx, s).Err() },
		func() error { return c.PExpireAt(ctx, s, time.UnixMilli(at+1)).Err() })
	changed("an expiry added",
		func() error { return c.PExpireAt(ctx, n, time.UnixMilli(at)).Err() },
		func() error { return c.Persist(ctx, n).Err() })
	changed("a hash field value",
		func() error { return c.HSet(ctx, h, "b", "3").Err() },
		func() error { return c.HSet(ctx, h, "b", "2").Err() })
	changed("a string value",
		func() error { return c.SetRange(ctx, s, 0, "w").Err() },
		func() error { return c.SetRange(ctx, s, 0, "v").Err() })
	changed("a key added",
		func() error { return c.Set(ctx, prefix+":snap:z", "1", 0).Err() },
		func() error { return c.Del(ctx, prefix+":snap:z").Err() })
}
