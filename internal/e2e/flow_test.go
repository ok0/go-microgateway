package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
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
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/hooks"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/middleware"
	"github.com/ok0/go-microgateway/internal/proxy"
	"github.com/ok0/go-microgateway/internal/runtime"
)

func withFlowTrace(record func(step string)) microgateway.Option {
	return hooks.WithFlowTrace(record).(microgateway.Option)
}

type flowRecorder struct {
	mu    sync.Mutex
	steps []string
}

func (r *flowRecorder) record(step string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.steps = append(r.steps, step)
}

func (r *flowRecorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.steps
	r.steps = nil
	return out
}

func flowPass() microgateway.Option {
	return microgateway.WithMiddleware("pass", func(microgateway.MiddlewareParams) (microgateway.Middleware, error) {
		return func(next http.Handler) http.Handler { return next }, nil
	})
}

// flowBackend counts its own hits. When shared is set, the first call made through the shared counter answers 502
// and every other call answers 200 "ok".
type flowBackend struct {
	hits   atomic.Int64
	shared *atomic.Int64
}

func (b *flowBackend) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	b.hits.Add(1)
	w.Header()["Date"] = nil
	if b.shared != nil && b.shared.Add(1) == 1 {
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

const flowKey = "flow-key-1"

// flowDoc is the flow configuration with the given hosts for the full, plain and piped routes.
func flowDoc(a, b, c string) string {
	return flowDocPlain(a, b, c, "")
}

// flowDocPlain is flowDoc with plainExtra appended to the plain route.
func flowDocPlain(a, b, c, plainExtra string) string {
	return "version: 1\n" + listenersYAML +
		"cache:\n  memory:\n    max_bytes: 8388608\n    shards: 1\n" +
		"auth:\n  api_keys:\n    - name: keys1\n      keys:\n        - id: k1\n" +
		fmt.Sprintf("          hash: %q\n          consumer: alice\n", auHash(flowKey)) +
		"middlewares:\n  - name: m\n    type: pass\n" +
		"upstreams:\n" +
		"  - name: full\n" +
		"    load_balancer:\n      algorithm: round_robin\n" +
		"    timeouts: {connect: 1h, response_header: 1h}\n" +
		"    circuit_breaker: {}\n" +
		fmt.Sprintf("    hosts:\n      - url: %q\n      - url: %q\n", a, b) +
		"  - name: plain\n" +
		"    timeouts: {connect: 1h, response_header: 1h}\n" +
		fmt.Sprintf("    hosts:\n      - url: %q\n", c) +
		"routes:\n" +
		"  - name: full\n" +
		"    match:\n      path: /flow/\n" +
		"    upstream: full\n" +
		"    middlewares: [m]\n" +
		"    timeout: 1h\n" +
		"    auth: {mode: apikey, providers: [keys1]}\n" +
		"    rate_limit: {rate: 1000000, per: 1s}\n" +
		"    cache: {enabled: true, default_ttl: 60s}\n" +
		"    circuit_breaker: {enabled: true}\n" +
		"    retry: {attempts: 2, base_delay: 10ms, max_delay: 10ms}\n" +
		"  - name: piped\n" +
		"    match:\n      path: /piped/\n" +
		"    upstream: plain\n" +
		"    middlewares: [m]\n" +
		"    timeout: 30s\n" +
		"    rate_limit: {rate: 1000000, per: 1s}\n" +
		"    pipeline: [rate_limit]\n" +
		"  - name: plain\n" +
		"    match:\n      path: /plain/\n" +
		"    upstream: plain\n" +
		"    middlewares: [m]\n" +
		plainExtra
}

// flowEntry is the entry part of the request flow, through route_match.
var flowEntry = runtime.Flow()[:7]

// flowAttempt is the trace of one upstream attempt.
var flowAttempt = []string{proxy.StepLoadBalancer, proxy.StepUpstream}

// flowPlainTrace is the trace of a request on the plain route, which has no configured stage.
var flowPlainTrace = slices.Concat(flowEntry, []string{runtime.StepRouteMiddleware, runtime.StepProxy}, flowAttempt)

func flowReq(path string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "http://gw.test"+path, nil)
	req.RemoteAddr = "192.0.2.1:1234"
	return req
}

func flowCheck(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("flow trace = %v, want %v", got, want)
	}
}

// stampLogger returns a JSON logger that checks every record's
// handler-stamped time is wall time in [start, now] and then writes it as
// "wall", so two gateways' logs compare byte for byte apart from wall time.
func stampLogger(t *testing.T, w io.Writer, start time.Time) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) != 0 || a.Key != slog.TimeKey {
				return a
			}
			if a.Value.Kind() != slog.KindTime {
				t.Errorf("log time kind = %v, want time", a.Value.Kind())
				return a
			}
			if tm := a.Value.Time(); tm.Before(start) || tm.After(time.Now()) {
				t.Errorf("log time %v outside [%v, now]", tm, start)
			}
			return slog.String(slog.TimeKey, "wall")
		},
	}))
}

// checkStamped fails unless logs is non-empty, every line carries the
// stamped time first, and the request's access-log line is present.
func checkStamped(t *testing.T, logs string) {
	t.Helper()
	lines := strings.Split(strings.TrimRight(logs, "\n"), "\n")
	if logs == "" || len(lines) == 0 {
		t.Fatal("no log lines written")
	}
	seen := false
	for _, l := range lines {
		if !strings.HasPrefix(l, `{"time":"wall",`) {
			t.Fatalf("log line without stamped time: %s", l)
		}
		seen = seen || strings.Contains(l, `"request_id":"flowsame1"`)
	}
	if !seen {
		t.Fatalf("no log line for request flowsame1:\n%s", logs)
	}
}

func flowErrorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error apiError `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body.Error.Code
}

func flowHits(bs ...*flowBackend) int64 {
	var total int64
	for _, b := range bs {
		total += b.hits.Load()
	}
	return total
}

func TestRequestFlowTrace(t *testing.T) {
	n := newMemNetwork()
	first := new(atomic.Int64)
	a := &flowBackend{shared: first}
	b := &flowBackend{shared: first}
	c := &flowBackend{}
	sa := n.serve(t, a)
	sb := n.serve(t, b)
	sc := n.serve(t, c)
	fake := clock.NewFake(time.Now())
	fr := &flowRecorder{}
	g := n.gateway(t, flowDoc(sa.URL, sb.URL, sc.URL), withClock(fake), withFlowTrace(fr.record), flowPass())

	t.Run("full route with retry", func(t *testing.T) {
		fr.take()
		req := flowReq("/flow/x")
		req.Header.Set("X-API-Key", flowKey)
		d := reDrive(t, fake, 250*time.Millisecond)
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, req)
		d.stop()
		if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
			t.Fatalf("status = %d body = %q, want 200 ok", rec.Code, rec.Body.String())
		}
		fullTrace := []string{
			runtime.StepRequestID, runtime.StepClientIP, runtime.StepObserve, runtime.StepRecover,
			runtime.StepStripHeaders, runtime.StepCanonicalPath, runtime.StepRouteMatch, runtime.StepRouteMiddleware,
			"timeout", "auth", "rate_limit", "cache", "circuit_breaker", "retry",
			runtime.StepProxy, proxy.StepLoadBalancer, proxy.StepUpstream, proxy.StepLoadBalancer, proxy.StepUpstream,
		}
		got := fr.take()
		flowCheck(t, got, fullTrace)
		flowCheck(t, got, append(slices.Clone(runtime.Flow()), flowAttempt...))
		if a.hits.Load() != 1 || b.hits.Load() != 1 {
			t.Fatalf("hits a=%d b=%d, want 1 each (the retry reaches the other host)", a.hits.Load(), b.hits.Load())
		}
	})

	t.Run("route without stages", func(t *testing.T) {
		fr.take()
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, flowReq("/plain/x"))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		got := fr.take()
		flowCheck(t, got, flowPlainTrace)
		for _, stage := range middleware.DefaultStages {
			if slices.Contains(got, stage) {
				t.Fatalf("trace %v contains unconfigured stage %q", got, stage)
			}
		}
	})

	t.Run("explicit pipeline drops an unnamed stage", func(t *testing.T) {
		fr.take()
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, flowReq("/piped/x"))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		want := slices.Concat(flowEntry, []string{runtime.StepRouteMiddleware, "rate_limit", runtime.StepProxy}, flowAttempt)
		flowCheck(t, fr.take(), want)
	})

	t.Run("invalid path", func(t *testing.T) {
		fr.take()
		before := flowHits(a, b, c)
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, urlReq(http.MethodGet, "/flow/x", "/flow/x%zz", "192.0.2.1:1234"))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", rec.Code)
		}
		if code := flowErrorCode(t, rec); code != httperr.CodeInvalidPath {
			t.Fatalf("error code = %q, want %q", code, httperr.CodeInvalidPath)
		}
		flowCheck(t, fr.take(), runtime.Flow()[:6])
		if after := flowHits(a, b, c); after != before {
			t.Fatalf("backend hits went from %d to %d, want no backend hit", before, after)
		}
	})

	t.Run("unmatched path", func(t *testing.T) {
		fr.take()
		before := flowHits(a, b, c)
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, flowReq("/nomatch"))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", rec.Code)
		}
		if code := flowErrorCode(t, rec); code != httperr.CodeRouteNotFound {
			t.Fatalf("error code = %q, want %q", code, httperr.CodeRouteNotFound)
		}
		flowCheck(t, fr.take(), runtime.Flow()[:7])
		if after := flowHits(a, b, c); after != before {
			t.Fatalf("backend hits went from %d to %d, want no backend hit", before, after)
		}
	})
}

func TestRequestFlowTraceIsInvisible(t *testing.T) {
	n := newMemNetwork()
	c := &flowBackend{}
	sc := n.serve(t, c)
	doc := flowDoc(sc.URL, sc.URL, sc.URL)
	var lbA, lbB logBuf
	fr := &flowRecorder{}
	start := time.Now()
	gA := n.gateway(t, doc,
		withClock(clock.NewFake(time.Unix(1_700_000_000, 0))),
		withRand(clock.NewRand(1)),
		microgateway.WithLogger(stampLogger(t, &lbA, start)),
		withFlowTrace(fr.record),
		flowPass())
	gB := n.gateway(t, doc,
		withClock(clock.NewFake(time.Unix(1_700_000_000, 0))),
		withRand(clock.NewRand(1)),
		microgateway.WithLogger(stampLogger(t, &lbB, start)),
		flowPass())

	serveSame := func(g *microgateway.Gateway) *httptest.ResponseRecorder {
		req := flowReq("/plain/x")
		req.Header.Set("X-Request-ID", "flowsame1")
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, req)
		return rec
	}
	recA := serveSame(gA)
	recB := serveSame(gB)

	if recA.Code != recB.Code {
		t.Fatalf("status traced %d, untraced %d", recA.Code, recB.Code)
	}
	if recA.Body.String() != recB.Body.String() {
		t.Fatalf("body traced %q, untraced %q", recA.Body.String(), recB.Body.String())
	}
	if !reflect.DeepEqual(recA.Header(), recB.Header()) {
		t.Fatalf("headers traced %v, untraced %v", recA.Header(), recB.Header())
	}
	if metricsText(t, gA) != metricsText(t, gB) {
		t.Fatal("metrics differ between the traced and the untraced gateway")
	}
	checkStamped(t, lbA.String())
	if lbA.String() != lbB.String() {
		t.Fatalf("logs differ:\ntraced:\n%s\nuntraced:\n%s", lbA.String(), lbB.String())
	}
	flowCheck(t, fr.take(), flowPlainTrace)
}

func TestRequestFlowTracePerGateway(t *testing.T) {
	n := newMemNetwork()
	c := &flowBackend{}
	sc := n.serve(t, c)
	doc := flowDoc(sc.URL, sc.URL, sc.URL)
	frA := &flowRecorder{}
	frB := &flowRecorder{}
	gA := n.gateway(t, doc, withClock(clock.NewFake(time.Now())), withFlowTrace(frA.record), flowPass())
	gB := n.gateway(t, doc, withClock(clock.NewFake(time.Now())), withFlowTrace(frB.record), flowPass())

	rec := httptest.NewRecorder()
	gA.ServeHTTP(rec, flowReq("/plain/a"))
	if rec.Code != http.StatusOK {
		t.Fatalf("gateway A status = %d, want 200", rec.Code)
	}
	flowCheck(t, frA.take(), flowPlainTrace)
	flowCheck(t, frB.take(), nil)

	rec = httptest.NewRecorder()
	gB.ServeHTTP(rec, flowReq("/plain/b"))
	if rec.Code != http.StatusOK {
		t.Fatalf("gateway B status = %d, want 200", rec.Code)
	}
	flowCheck(t, frB.take(), flowPlainTrace)
	flowCheck(t, frA.take(), nil)

	if err := gA.ReloadYAML([]byte(flowDocPlain(sc.URL, sc.URL, sc.URL, "    timeout: 30s\n"))); err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	gA.ServeHTTP(rec, flowReq("/plain/c"))
	if rec.Code != http.StatusOK {
		t.Fatalf("gateway A after reload status = %d, want 200", rec.Code)
	}
	flowCheck(t, frA.take(), slices.Concat(flowEntry, []string{runtime.StepRouteMiddleware, "timeout", runtime.StepProxy}, flowAttempt))
	flowCheck(t, frB.take(), nil)
}

func TestRequestFlowTraceConcurrent(t *testing.T) {
	n := newMemNetwork()
	c := &flowBackend{}
	sc := n.serve(t, c)
	fr := &flowRecorder{}
	g := n.gateway(t, flowDoc(sc.URL, sc.URL, sc.URL), withClock(clock.NewFake(time.Now())), withFlowTrace(fr.record), flowPass())

	const workers = 8
	codes := make([]int, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			g.ServeHTTP(rec, flowReq("/plain/x"))
			codes[i] = rec.Code
		}()
	}
	wg.Wait()

	for i, code := range codes {
		if code != http.StatusOK {
			t.Fatalf("request %d status = %d, want 200", i, code)
		}
	}
	got := fr.take()
	var want []string
	for range workers {
		want = append(want, flowPlainTrace...)
	}
	slices.Sort(got)
	slices.Sort(want)
	flowCheck(t, got, want)
}
