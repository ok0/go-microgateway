package cache

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/coalesce"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/memstore"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/internal/storeguard"
	"github.com/ok0/go-microgateway/store"
)

// start is a whole second, so that second-granular headers and fake clock readings agree.
var start = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

const testRoute = "users"

// spyCall records one call to the spy store.
type spyCall struct {
	Op        string
	Primary   string
	Secondary string
	VaryNames []string
	Tags      []string
	Header    http.Header
	TTL       time.Duration
	Stored    bool
	Purge     store.Purge
}

// spy wraps a memstore cache, records every call and injects failures, hangs and panics per operation.
type spy struct {
	inner  *memstore.Cache
	mu     sync.Mutex
	calls  []spyCall
	errs   map[string]error
	holds  map[string]chan struct{}
	panics map[string]any
	refuse bool
}

func newSpy(tb testing.TB, clk clock.Clock) *spy {
	tb.Helper()
	inner := memstore.NewCache(memstore.CacheOptions{MaxBytes: 1 << 22, Clock: clk})
	tb.Cleanup(func() { _ = inner.Close() })
	return &spy{inner: inner, errs: map[string]error{}, holds: map[string]chan struct{}{}, panics: map[string]any{}}
}

// panicOn makes every call of op panic.
func (s *spy) panicOn(op string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.panics[op] = "secret panic text"
}

// Close satisfies store.Backend. The memory cache is closed by the test cleanup.
func (s *spy) Close() error { return nil }

func (s *spy) record(c spyCall) {
	s.mu.Lock()
	s.calls = append(s.calls, c)
	s.mu.Unlock()
}

// setErr makes every call of op fail with err. A nil err clears the failure.
func (s *spy) setErr(op string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		delete(s.errs, op)
		return
	}
	s.errs[op] = err
}

// hang parks every call of op until release is called or the call context ends.
func (s *spy) hang(op string) (release func()) {
	ch := make(chan struct{})
	s.mu.Lock()
	s.holds[op] = ch
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		delete(s.holds, op)
		s.mu.Unlock()
		close(ch)
	}
}

func (s *spy) setRefuse(v bool) {
	s.mu.Lock()
	s.refuse = v
	s.mu.Unlock()
}

func (s *spy) gate(ctx context.Context, op string) error {
	s.mu.Lock()
	hold, err, p := s.holds[op], s.errs[op], s.panics[op]
	s.mu.Unlock()
	if p != nil {
		panic(p)
	}
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (s *spy) Get(ctx context.Context, primary string, sel func([]string) (string, bool)) (store.Lookup, error) {
	s.record(spyCall{Op: opGet, Primary: primary})
	if err := s.gate(ctx, opGet); err != nil {
		return store.Lookup{}, err
	}
	return s.inner.Get(ctx, primary, sel)
}

func (s *spy) Put(ctx context.Context, tok store.FillToken, primary, secondary string, varyNames []string, e *store.Entry, ttl time.Duration) (bool, error) {
	c := spyCall{Op: opPut, Primary: primary, Secondary: secondary, VaryNames: slices.Clone(varyNames), TTL: ttl}
	if e != nil {
		c.Tags = slices.Clone(e.Tags)
		c.Header = e.Header.Clone()
	}
	if err := s.gate(ctx, opPut); err != nil {
		s.record(c)
		return false, err
	}
	s.mu.Lock()
	refuse := s.refuse
	s.mu.Unlock()
	if refuse {
		s.record(c)
		return false, nil
	}
	stored, err := s.inner.Put(ctx, tok, primary, secondary, varyNames, e, ttl)
	c.Stored = stored
	s.record(c)
	return stored, err
}

func (s *spy) Purge(ctx context.Context, p store.Purge) (store.PurgeResult, error) {
	s.record(spyCall{Op: opPurge, Purge: p})
	if err := s.gate(ctx, opPurge); err != nil {
		return store.PurgeResult{}, err
	}
	return s.inner.Purge(ctx, p)
}

// calls returns a copy of the recorded calls of op, in call order. An empty op selects every call.
func (s *spy) callsOf(op string) []spyCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []spyCall
	for _, c := range s.calls {
		if op == "" || c.Op == op {
			out = append(out, c)
		}
	}
	return out
}

// origin is a scripted upstream. Each call is counted and its request header kept.
type origin struct {
	mu         sync.Mutex
	calls      int
	seen       []http.Header
	header     http.Header
	status     int
	body       []byte
	omitFinish bool
	handle     func(w http.ResponseWriter, r *http.Request)
	gate       chan struct{}
	arrived    chan struct{}
}

func (o *origin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	o.mu.Lock()
	o.calls++
	o.seen = append(o.seen, r.Header.Clone())
	gate, arrived := o.gate, o.arrived
	h, header, status, body, omit := o.handle, o.header, o.status, o.body, o.omitFinish
	o.mu.Unlock()
	if gate != nil {
		arrived <- struct{}{}
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
	}
	if h != nil {
		h(w, r)
		return
	}
	ex := reqctx.ExchangeFrom(r.Context())
	if ex != nil {
		ex.Begin()
	}
	for k, v := range header {
		w.Header()[k] = slices.Clone(v)
	}
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	if r.Method != http.MethodHead && len(body) > 0 {
		_, _ = w.Write(body)
	}
	if ex != nil && !omit {
		ex.Finish()
	}
}

// script sets the response the origin gives from now on.
func (o *origin) script(status int, header http.Header, body string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.status = status
	o.header = header
	o.body = []byte(body)
	o.handle = nil
}

func (o *origin) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls
}

// request returns the header of the last origin request.
func (o *origin) last() http.Header {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.seen) == 0 {
		return nil
	}
	return o.seen[len(o.seen)-1]
}

// hold makes every origin call from now on wait until release is called or the request context ends. arrived
// receives one value per held call before it waits. release is idempotent; it also lifts the hold for later calls.
func (o *origin) hold() (arrived <-chan struct{}, release func()) {
	gate := make(chan struct{})
	ch := make(chan struct{}, 1024)
	o.mu.Lock()
	o.gate, o.arrived = gate, ch
	o.mu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			o.mu.Lock()
			if o.gate == gate {
				o.gate, o.arrived = nil, nil
			}
			o.mu.Unlock()
			close(gate)
		})
	}
}

// conditional returns how many origin requests carried If-None-Match or If-Modified-Since.
func (o *origin) conditional() int {
	return o.countSeen(true)
}

// unconditional returns how many origin requests carried neither If-None-Match nor If-Modified-Since.
func (o *origin) unconditional() int {
	return o.countSeen(false)
}

func (o *origin) countSeen(conditional bool) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, h := range o.seen {
		if (h.Get("If-None-Match") != "" || h.Get("If-Modified-Since") != "") == conditional {
			n++
		}
	}
	return n
}

// parallel runs f(i) for i in [0, n) in n goroutines. The returned channel is closed when all of them returned.
func parallel(n int, f func(i int)) <-chan struct{} {
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			f(i)
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	return done
}

// rigOpts selects the configuration and collaborators of a rig. wrap, when set, is applied outside the stage.
type rigOpts struct {
	route        string
	global       string
	clk          *clock.Fake
	spy          *spy
	guard        bool
	credential   func(*http.Request) bool
	onStoreError func(op string)
	errs         *ErrorLog
	logger       *slog.Logger
	wrap         func(http.Handler) http.Handler
}

// rig is one cache stage in front of a scripted origin.
type rig struct {
	tb    testing.TB
	clk   *clock.Fake
	spy   *spy
	mets  *Metrics
	orig  *origin
	guard *storeguard.Guard
	group *coalesce.Group
	h     http.Handler
}

func newRig(tb testing.TB, o rigOpts) *rig {
	tb.Helper()
	clk := o.clk
	if clk == nil {
		clk = clock.NewFake(start)
	}
	sp := o.spy
	if sp == nil {
		sp = newSpy(tb, clk)
	}
	reg := metrics.NewRegistry()
	mets := newMetrics(tb, reg)
	orig := &origin{status: http.StatusOK, header: http.Header{}}
	rc, gc := parseCache(tb, o.route, o.global)
	d := Deps{
		Route:        testRoute,
		Store:        sp,
		Retain:       30 * 24 * time.Hour,
		Credential:   o.credential,
		OnStoreError: o.onStoreError,
		Errors:       o.errs,
		Clock:        clk,
		Metrics:      mets,
		Logger:       o.logger,
	}
	if *gc.Coalescing.Enabled {
		d.Group = coalesce.New(*gc.Coalescing.MaxKeys, *gc.Coalescing.MaxWaiters, clk)
	}
	r := &rig{tb: tb, clk: clk, spy: sp, mets: mets, orig: orig, group: d.Group}
	if o.guard {
		g, err := storeguard.New("shared", storeguard.Settings{ConsecutiveFailures: 2, OpenTimeout: time.Second, MaxInflight: 16}, clk, nil)
		if err != nil {
			tb.Fatal(err)
		}
		d.Guard = g
		d.StoreName = "shared"
		r.guard = g
	}
	mw, err := NewStage(rc, gc, d)
	if err != nil {
		tb.Fatal(err)
	}
	r.h = mw(orig)
	if o.wrap != nil {
		r.h = o.wrap(r.h)
	}
	return r
}

func indentYAML(s, pad string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	for i, l := range lines {
		lines[i] = pad + l
	}
	return strings.Join(lines, "\n") + "\n"
}

// parseCache builds the route and global cache settings through config.Parse, so that defaults are applied.
func parseCache(tb testing.TB, route, global string) (config.RouteCache, config.Cache) {
	tb.Helper()
	if route == "" {
		route = "enabled: true"
	}
	doc := "version: 1\n" +
		"listeners:\n  - name: public\n    address: \":8080\"\n" +
		"upstreams:\n  - name: users\n    hosts:\n      - url: \"http://127.0.0.1:9001\"\n" +
		"routes:\n  - name: " + testRoute + "\n    upstream: users\n    cache:\n" + indentYAML(route, "      ")
	if global != "" {
		doc += "cache:\n" + indentYAML(global, "  ")
	}
	cfg, err := config.Parse([]byte(doc))
	if err != nil {
		tb.Fatalf("parse cache settings: %v", err)
	}
	rc := cfg.Routes[0].Cache
	if rc == nil {
		tb.Fatal("route has no cache settings")
	}
	return *rc, cfg.Cache
}

func newMetrics(tb testing.TB, reg *metrics.Registry) *Metrics {
	tb.Helper()
	req, err := reg.Counter("microgateway_cache_requests_total", "Cache requests.", "route", "result")
	if err != nil {
		tb.Fatal(err)
	}
	stores, err := reg.Counter("microgateway_cache_stores_total", "Cache storage outcomes.", "route", "result")
	if err != nil {
		tb.Fatal(err)
	}
	storeErrs, err := reg.Counter("microgateway_cache_store_errors_total", "Cache store call failures.", "op")
	if err != nil {
		tb.Fatal(err)
	}
	coalesced, err := reg.Counter("microgateway_cache_coalesced_total", "Cache requests served by another request's upstream fetch.", "route")
	if err != nil {
		tb.Fatal(err)
	}
	return &Metrics{Requests: req, Stores: stores, StoreErrors: storeErrs, Coalesced: coalesced}
}

// get serves a GET for path with the given request header through the rig.
func (r *rig) get(path string, hdr http.Header) *httptest.ResponseRecorder {
	return r.serve(http.MethodGet, path, hdr)
}

func (r *rig) serve(method, path string, hdr http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "http://example.com"+path, nil)
	for k, v := range hdr {
		req.Header[k] = slices.Clone(v)
	}
	return do(r.h, req)
}

func (r *rig) requests(result string) float64 {
	return r.mets.Requests.With(testRoute, result).Value()
}

func (r *rig) stores(result string) float64 {
	return r.mets.Stores.With(testRoute, result).Value()
}

func (r *rig) storeErrs(op string) float64 {
	return r.mets.StoreErrors.With(op).Value()
}

func (r *rig) coalesced() float64 {
	return r.mets.Coalesced.With(testRoute).Value()
}

func do(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// cacheStatus returns the members of the Cache-Status field of rec, failing the test when it does not parse.
func cacheStatus(tb testing.TB, rec *httptest.ResponseRecorder) []httpcache.Status {
	tb.Helper()
	members, err := httpcache.ParseStatus(strings.Join(rec.Header().Values("Cache-Status"), ", "))
	if err != nil {
		tb.Fatalf("Cache-Status does not parse: %v (%q)", err, rec.Header().Values("Cache-Status"))
	}
	return members
}

// lastStatus returns the last Cache-Status member of rec, which is the one the stage appended.
func lastStatus(tb testing.TB, rec *httptest.ResponseRecorder) httpcache.Status {
	tb.Helper()
	members := cacheStatus(tb, rec)
	if len(members) == 0 {
		tb.Fatalf("no Cache-Status in response (status %d)", rec.Code)
	}
	return members[len(members)-1]
}

// hit reports whether rec was served from the cache by the stage.
func hit(tb testing.TB, rec *httptest.ResponseRecorder) bool {
	return lastStatus(tb, rec).Hit
}

// logBuffer returns an error log whose records go to the returned buffer.
func logBuffer(clk clock.Clock) (*ErrorLog, *bytes.Buffer) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	return NewErrorLog(logger, clk), &buf
}
