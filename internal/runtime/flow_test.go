package runtime

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"testing"

	"github.com/ok0/go-microgateway/internal/middleware"
	"github.com/ok0/go-microgateway/internal/proxy"
	"github.com/ok0/go-microgateway/internal/reqctx"
)

// checkEntry reports whether rows is the entry table of Flow: one row per entryFlow id, then route_middleware.
func checkEntry(rows []flowStep) error {
	if len(rows) != len(entryFlow) {
		return fmt.Errorf("entry table has %d rows, want %d", len(rows), len(entryFlow))
	}
	flow := Flow()
	for i := range rows {
		if rows[i].id != flow[i] {
			return fmt.Errorf("row %d id = %q, want %q", i, rows[i].id, flow[i])
		}
	}
	if flow[len(rows)] != StepRouteMiddleware {
		return fmt.Errorf("Flow()[%d] = %q, want %q", len(rows), flow[len(rows)], StepRouteMiddleware)
	}
	return nil
}

type countHandler struct{ n int }

func (c *countHandler) ServeHTTP(http.ResponseWriter, *http.Request) { c.n++ }

func TestFlowOrder(t *testing.T) {
	want := []string{"request_id", "client_ip", "observe", "recover", "strip_headers", "canonical_path", "route_match", "route_middleware", "timeout", "auth", "rate_limit", "cache", "circuit_breaker", "retry", "proxy", "load_balancer", "upstream"}
	if got := Flow(); !slices.Equal(got, want) {
		t.Errorf("Flow() = %v, want %v", got, want)
	}
	exp := slices.Concat(entryFlow, []string{StepRouteMiddleware}, middleware.DefaultStages, []string{StepProxy, proxy.StepLoadBalancer, proxy.StepUpstream})
	if got := Flow(); !slices.Equal(got, exp) {
		t.Errorf("Flow() = %v, want entryFlow + route_middleware + DefaultStages + proxy steps %v", got, exp)
	}
	f := Flow()
	f[0] = "x"
	_ = append(f[:1], "y")
	if got := Flow(); got[0] != "request_id" || got[1] != "client_ip" {
		t.Errorf("Flow() after caller mutation starts %q, %q; want request_id, client_ip", got[0], got[1])
	}
}

func TestEntryTableMatchesFlow(t *testing.T) {
	trusted, _ := reqctx.ParseTrusted(nil)
	logger := slog.New(slog.DiscardHandler)
	rows := (&Snapshot{}).entryTable("X-Request-ID", trusted, nil, logger, []string{"X-User-ID"})
	if err := checkEntry(rows); err != nil {
		t.Fatal(err)
	}

	bad := slices.Clone(rows)
	bad[2].id = "x"
	if checkEntry(bad) == nil {
		t.Error("checkEntry accepted a table with a wrong id")
	}

	swapped := slices.Clone(rows)
	swapped[2], swapped[3] = swapped[3], swapped[2]
	if checkEntry(swapped) == nil {
		t.Error("checkEntry accepted a table with swapped rows")
	}

	short := slices.Clone(rows)[:len(rows)-1]
	if checkEntry(short) == nil {
		t.Error("checkEntry accepted a table without its last row")
	}

	if rows[4].mw == nil {
		t.Error("strip row is nil with a strip list")
	}
	if (&Snapshot{}).entryTable("X-Request-ID", trusted, nil, logger, nil)[4].mw != nil {
		t.Error("strip row is set without a strip list")
	}
}

func TestTracerUnset(t *testing.T) {
	tr := &tracer{}
	mw := middleware.Middleware(func(next http.Handler) http.Handler { return next })
	if reflect.ValueOf(tr.wrap("x", mw)).Pointer() != reflect.ValueOf(mw).Pointer() {
		t.Error("wrap without record did not return its input")
	}
	h := &countHandler{}
	if tr.handler("x", h) != http.Handler(h) {
		t.Error("handler without record did not return its input")
	}
	if tr.links != 0 {
		t.Errorf("links = %d without record, want 0", tr.links)
	}

	tr2 := &tracer{record: func(string) {}}
	if tr2.wrap("x", nil) != nil {
		t.Error("wrap of a nil middleware returned a middleware")
	}
	if tr2.links != 0 {
		t.Errorf("links = %d after a nil wrap, want 0", tr2.links)
	}
	tr2.handler("x", h).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if h.n != 1 {
		t.Errorf("handler calls = %d, want 1", h.n)
	}
	if tr2.links != 1 {
		t.Errorf("links = %d after one handler link, want 1", tr2.links)
	}
}

func TestBuildTraceLinks(t *testing.T) {
	cfg := mustParse(t, `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
middlewares:
  - name: m
    type: ok
routes:
  - name: api
    match:
      path: /api
    upstream: users
    middlewares: [m]
    timeout: 1s
`)
	reg := middleware.NewRegistry()
	if err := reg.Register("ok", passthroughFactory); err != nil {
		t.Fatal(err)
	}

	s, _, err := build(t, cfg, Deps{Middlewares: reg})
	if err != nil {
		t.Fatal(err)
	}
	if s.traceLinks != 0 {
		t.Errorf("traceLinks = %d without FlowTrace, want 0", s.traceLinks)
	}

	var steps []string
	s, _, err = build(t, cfg, Deps{Middlewares: reg, FlowTrace: func(id string) { steps = append(steps, id) }})
	if err != nil {
		t.Fatal(err)
	}
	if s.traceLinks != 10 {
		t.Errorf("traceLinks = %d with FlowTrace, want 10", s.traceLinks)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nomatch", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
	if want := Flow()[:7]; !slices.Equal(steps, want) {
		t.Errorf("steps = %v, want %v", steps, want)
	}
}
