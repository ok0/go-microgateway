package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/coalesce"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/middleware"
	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/internal/retry"
	"github.com/ok0/go-microgateway/internal/router"
	"github.com/ok0/go-microgateway/internal/state"
	"github.com/ok0/go-microgateway/internal/testwait"
)

var buildStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func mustParse(t *testing.T, doc string) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func build(t *testing.T, cfg *config.Config, d Deps) (*Snapshot, *state.Registry, error) {
	t.Helper()
	// a snapshot's limits and timers run on a fake clock unless the test passes its own
	if d.Clock == nil {
		d.Clock = clock.NewFake(buildStart)
	}
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	tx := reg.Begin()
	s, err := Build(cfg, d, tx)
	if err != nil {
		tx.Abort()
		return nil, reg, err
	}
	lease := tx.Commit()
	t.Cleanup(func() { _ = lease.Release() })
	s.Activate()
	return s, reg, nil
}

type fakeResolver struct {
	answers []string
}

func (f fakeResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	return f.answers, nil
}

func TestBuildDNSUpstream(t *testing.T) {
	cfg := mustParse(t, `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    dns:
      hostname: backend.internal
      port: 8080
routes:
  - name: all
    upstream: users
`)
	s, _, err := build(t, cfg, Deps{Resolver: fakeResolver{answers: []string{"10.0.0.2", "10.0.0.1"}}})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, h := range s.Upstreams["users"].Hosts() {
		got = append(got, h.URL.String())
	}
	want := []string{"http://10.0.0.1:8080", "http://10.0.0.2:8080"}
	if !slices.Equal(got, want) {
		t.Errorf("hosts = %v, want %v", got, want)
	}
}

func TestUpstreamSeriesPresentAtStartup(t *testing.T) {
	cfg := mustParse(t, `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
routes:
  - name: api
    match:
      path: /api
    upstream: users
`)
	reg := metrics.NewRegistry()
	if _, _, err := build(t, cfg, Deps{Metrics: reg}); err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	if err := reg.WriteText(&sb); err != nil {
		t.Fatal(err)
	}
	text := sb.String()
	for _, want := range []string{
		`microgateway_upstream_host_healthy{upstream="users",host="127.0.0.1:9001"} 1`,
		`microgateway_upstream_panic_mode{upstream="users"} 0`,
		`microgateway_upstream_ejections_refused_total{upstream="users"} 0`,
		`microgateway_upstream_attempts_total{upstream="users",host="127.0.0.1:9001",outcome="success"} 0`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("metrics text lacks %q", want)
		}
	}
}

func TestNotFound(t *testing.T) {
	cfg := mustParse(t, `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
routes:
  - name: api
    match:
      path: /api
    upstream: users
`)
	s, _, err := build(t, cfg, Deps{})
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	var body struct {
		Error struct {
			Status    int    `json:"status"`
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v; body = %s", err, rec.Body.String())
	}
	if body.Error.Code != "route_not_found" {
		t.Errorf("code = %q, want route_not_found", body.Error.Code)
	}
	if body.Error.RequestID == "" {
		t.Errorf("request_id is empty")
	}
	if got := rec.Header().Get("X-Request-ID"); got != body.Error.RequestID {
		t.Errorf("X-Request-ID header = %q, body request_id = %q", got, body.Error.RequestID)
	}
}

func TestPatternRouteProxied(t *testing.T) {
	seen := make(chan string, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.URL.Path
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(backend.Close)

	cfg := mustParse(t, fmt.Sprintf(`version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: %q
routes:
  - name: user
    match:
      path: /u/:id
      path_type: pattern
    upstream: users
`, backend.URL))
	s, _, err := build(t, cfg, Deps{})
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(s)
	t.Cleanup(gw.Close)

	resp, err := http.Get(gw.URL + "/u/7")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	p, _ := testwait.Recv(t, "backend request", seen, nil)
	if p != "/u/7" {
		t.Errorf("backend path = %q, want /u/7", p)
	}
}

func TestParamsInContext(t *testing.T) {
	probe := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := reqctx.Route(r.Context()); got != "u" {
			t.Errorf("route = %q, want u", got)
		}
		if got := reqctx.ParamValue(r.Context(), "id"); got != "7" {
			t.Errorf("param id = %q, want 7", got)
		}
	})
	rt, err := router.New([]router.Entry{{
		Matcher: router.Matcher{Path: "/u/:id", PathType: router.PathPattern, Order: 0},
		Value:   &routeEntry{name: "u", handler: probe},
	}})
	if err != nil {
		t.Fatal(err)
	}
	s := &Snapshot{Router: rt, notFound: http.NotFoundHandler()}

	req := httptest.NewRequest(http.MethodGet, "/u/7", nil)
	_, params, ok := rt.Lookup(req)
	if !ok {
		t.Fatal("Lookup(/u/7) not found")
	}
	if want := (router.Params{{Name: "id", Value: "7"}}); !reflect.DeepEqual(params, want) {
		t.Errorf("params = %v, want %v", params, want)
	}

	s.dispatch(httptest.NewRecorder(), req)
}

func passthroughFactory(config.Middleware) (middleware.Middleware, error) {
	return func(next http.Handler) http.Handler { return next }, nil
}

func TestBuildMiddlewareErrors(t *testing.T) {
	t.Run("unknown type", func(t *testing.T) {
		cfg := mustParse(t, `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
middlewares:
  - name: a
    type: nope
routes:
  - name: api
    match:
      path: /api
    upstream: users
`)
		s, _, err := build(t, cfg, Deps{})
		if err == nil || !strings.Contains(err.Error(), "middlewares[0].type") || !strings.Contains(err.Error(), `unknown middleware type "nope"`) {
			t.Fatalf("Build error = %v, want middlewares[0].type and unknown middleware type \"nope\"", err)
		}
		var ve *config.ValidationError
		if !errors.As(err, &ve) {
			t.Errorf("Build error %T is not a *config.ValidationError", err)
		}
		if s != nil {
			t.Errorf("snapshot = %v, want nil", s)
		}
	})

	t.Run("factory error", func(t *testing.T) {
		reg := middleware.NewRegistry()
		if err := reg.Register("ok", passthroughFactory); err != nil {
			t.Fatal(err)
		}
		if err := reg.Register("t", func(config.Middleware) (middleware.Middleware, error) {
			return nil, errors.New("boom")
		}); err != nil {
			t.Fatal(err)
		}
		cfg := mustParse(t, `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
middlewares:
  - name: a
    type: ok
  - name: b
    type: t
routes:
  - name: api
    match:
      path: /api
    upstream: users
`)
		s, _, err := build(t, cfg, Deps{Middlewares: reg})
		if err == nil || !strings.Contains(err.Error(), "middlewares[1]") || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("Build error = %v, want middlewares[1] and boom", err)
		}
		var ve *config.ValidationError
		if !errors.As(err, &ve) {
			t.Errorf("Build error %T is not a *config.ValidationError", err)
		}
		if s != nil {
			t.Errorf("snapshot = %v, want nil", s)
		}
	})
}

func TestBuildMiddlewareErrorsAll(t *testing.T) {
	reg := middleware.NewRegistry()
	if err := reg.Register("t", func(config.Middleware) (middleware.Middleware, error) {
		return nil, errors.New("boom")
	}); err != nil {
		t.Fatal(err)
	}
	cfg := mustParse(t, `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
middlewares:
  - name: a
    type: nope
  - name: b
    type: nope2
  - name: c
    type: t
routes:
  - name: api
    match:
      path: /api
    upstream: users
`)
	s, _, err := build(t, cfg, Deps{Middlewares: reg})
	if s != nil {
		t.Errorf("snapshot = %v, want nil", s)
	}
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("Build error %T is not a *config.ValidationError", err)
	}
	wantPaths := []string{"middlewares[0].type", "middlewares[1].type", "middlewares[2]"}
	if len(ve.Errors) != len(wantPaths) {
		t.Fatalf("errors = %d, want %d: %v", len(ve.Errors), len(wantPaths), ve.Errors)
	}
	for i, fe := range ve.Errors {
		if fe.Path != wantPaths[i] {
			t.Errorf("error %d: path = %q, want %q", i, fe.Path, wantPaths[i])
		}
		if fe.Line <= 0 {
			t.Errorf("error %d: line = %d, want > 0", i, fe.Line)
		}
	}
}

func TestBuildFingerprint(t *testing.T) {
	cfg := mustParse(t, `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
routes:
  - name: api
    match:
      path: /api
    upstream: users
`)
	s, _, err := build(t, cfg, Deps{})
	if err != nil {
		t.Fatal(err)
	}
	if s.Fingerprint != cfg.Fingerprint() {
		t.Errorf("Fingerprint = %q, want %q", s.Fingerprint, cfg.Fingerprint())
	}
}

func TestBuildFailureReleasesCreated(t *testing.T) {
	reg := middleware.NewRegistry()
	if err := reg.Register("t", func(config.Middleware) (middleware.Middleware, error) {
		return nil, errors.New("boom")
	}); err != nil {
		t.Fatal(err)
	}
	cfg := mustParse(t, `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
middlewares:
  - name: b
    type: t
routes:
  - name: api
    match:
      path: /api
    upstream: users
`)
	_, sreg, err := build(t, cfg, Deps{Middlewares: reg})
	if err == nil {
		t.Fatal("Build error = nil, want factory error")
	}
	if n := sreg.Len(); n != 0 {
		t.Errorf("registry Len = %d after failed build, want 0", n)
	}
}

func TestFactoryCalledOncePerDefinition(t *testing.T) {
	calls := 0
	reg := middleware.NewRegistry()
	if err := reg.Register("count", func(def config.Middleware) (middleware.Middleware, error) {
		calls++
		return passthroughFactory(def)
	}); err != nil {
		t.Fatal(err)
	}
	cfg := mustParse(t, `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
middlewares:
  - name: first
    type: count
  - name: unused
    type: count
routes:
  - name: a
    match:
      path: /a
    upstream: users
    middlewares: [first]
  - name: b
    match:
      path: /b
    upstream: users
    middlewares: [first]
`)
	if _, _, err := build(t, cfg, Deps{Middlewares: reg}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Errorf("factory calls = %d, want 2", calls)
	}
}

func TestSeriesReusedAcrossBuilds(t *testing.T) {
	reg := metrics.NewRegistry()
	cfg := mustParse(t, `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
routes:
  - name: api
    match:
      path: /api
    upstream: users
`)
	for i := range 2 {
		if _, _, err := build(t, cfg, Deps{Metrics: reg}); err != nil {
			t.Fatalf("Build %d: %v", i, err)
		}
	}
}

func TestResilienceSeriesPresentAtStartup(t *testing.T) {
	cfg := mustParse(t, `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
    circuit_breaker: {}
routes:
  - name: api
    match:
      path: /api
    upstream: users
    retry: {}
`)
	reg := metrics.NewRegistry()
	if _, _, err := build(t, cfg, Deps{Metrics: reg}); err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	if err := reg.WriteText(&sb); err != nil {
		t.Fatal(err)
	}
	text := sb.String()
	for _, want := range []string{
		`microgateway_circuit_breaker_state{upstream="users",host="127.0.0.1:9001"} 0`,
		`microgateway_circuit_breaker_state{upstream="users",host=""} 0`,
		`microgateway_circuit_breaker_transitions_total{upstream="users",host="",from="closed",to="open"} 0`,
		`microgateway_circuit_breaker_rejected_total{upstream="users",host=""} 0`,
		`microgateway_retries_total{route="api"} 0`,
		`microgateway_retry_budget_exhausted_total{route="api"} 0`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("metrics text lacks %q", want)
		}
	}

	bad := mustParse(t, `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
middlewares:
  - name: a
    type: nope
routes:
  - name: fresh
    match:
      path: /fresh
    upstream: users
    retry: {}
`)
	if _, _, err := build(t, bad, Deps{Metrics: reg}); err == nil {
		t.Fatal("Build with unknown middleware type succeeded")
	}
	sb.Reset()
	if err := reg.WriteText(&sb); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sb.String(), `route="fresh"`) {
		t.Error("failed build left a retry series for its route")
	}
}

func TestBudgetStateRetention(t *testing.T) {
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	clk := clock.NewFake(buildStart)
	budgetFor := func(t *testing.T, doc string) *retry.Budget {
		t.Helper()
		cfg := mustParse(t, doc)
		tx := reg.Begin()
		if _, err := Build(cfg, Deps{Clock: clk}, tx); err != nil {
			tx.Abort()
			t.Fatal(err)
		}
		b, err := retryBudget(tx, "api", *cfg.Routes[0].Retry, clk)
		if err != nil {
			tx.Abort()
			t.Fatal(err)
		}
		lease := tx.Commit()
		t.Cleanup(func() { _ = lease.Release() })
		return b
	}
	const doc = `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
routes:
  - name: api
    match:
      path: /api
    upstream: users
    retry:
      attempts: %d
`

	b1 := budgetFor(t, fmt.Sprintf(doc, 3))
	b1.Deposit()
	b2 := budgetFor(t, fmt.Sprintf(doc, 3))
	if b2 != b1 {
		t.Fatal("budget was rebuilt for an unchanged retry block")
	}
	if requests, _ := b2.Stats(); requests != 1 {
		t.Errorf("requests = %d after reuse, want 1", requests)
	}

	b3 := budgetFor(t, fmt.Sprintf(doc, 2))
	if b3 == b1 {
		t.Fatal("budget was reused after attempts changed")
	}
	if requests, _ := b3.Stats(); requests != 0 {
		t.Errorf("requests = %d for a new budget, want 0", requests)
	}
}

func TestSnapshotBackgroundScope(t *testing.T) {
	cached, _, err := build(t, mustParse(t, cacheHead+cacheRouteBlock), Deps{})
	if err != nil {
		t.Fatal(err)
	}
	if st, ok := cached.CoalesceStats(); !ok || st != (coalesce.Stats{}) {
		t.Fatalf("CoalesceStats = %+v, %v; want an idle group", st, ok)
	}
	if n := cached.BackgroundRunning(); n != 0 {
		t.Fatalf("BackgroundRunning = %d on an idle snapshot", n)
	}
	if cached.RetireBackground(func() { t.Error("onIdle called without a running refresh") }) {
		t.Fatal("RetireBackground reported a running refresh on an idle snapshot")
	}
	cached.CancelBackground()

	plain, _, err := build(t, mustParse(t, cacheHead+"routes:\n"+plainRouteBlock), Deps{})
	if err != nil {
		t.Fatal(err)
	}
	if plain.RetireBackground(func() { t.Error("onIdle called for a snapshot without a scope") }) {
		t.Fatal("RetireBackground reported a refresh for a snapshot without a scope")
	}
	if n := plain.BackgroundRunning(); n != 0 {
		t.Fatalf("BackgroundRunning = %d without a runner", n)
	}
	if _, ok := plain.CoalesceStats(); ok {
		t.Fatal("CoalesceStats reported a group for a snapshot without cache")
	}
	plain.CancelBackground()
}
