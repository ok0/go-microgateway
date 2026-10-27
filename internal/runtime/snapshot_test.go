package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/metrics"
	"github.com/ok0/lychgate/internal/middleware"
	"github.com/ok0/lychgate/internal/reqctx"
	"github.com/ok0/lychgate/internal/router"
)

func mustParse(t *testing.T, doc string) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestBuildDNSFails(t *testing.T) {
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
	s, err := Build(cfg, Deps{})
	if err == nil || !strings.Contains(err.Error(), "not yet supported") {
		t.Fatalf("Build error = %v, want not yet supported", err)
	}
	if s != nil {
		t.Errorf("snapshot = %v, want nil", s)
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
	s, err := Build(cfg, Deps{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

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
	s, err := Build(cfg, Deps{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
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
	select {
	case p := <-seen:
		if p != "/u/7" {
			t.Errorf("backend path = %q, want /u/7", p)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("backend was not called")
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
		s, err := Build(cfg, Deps{})
		if err == nil || !strings.Contains(err.Error(), "middlewares[0].type") || !strings.Contains(err.Error(), `unknown middleware type "nope"`) {
			t.Fatalf("Build error = %v, want middlewares[0].type and unknown middleware type \"nope\"", err)
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
		s, err := Build(cfg, Deps{Middlewares: reg})
		if err == nil || !strings.Contains(err.Error(), "middlewares[1]") || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("Build error = %v, want middlewares[1] and boom", err)
		}
		if s != nil {
			t.Errorf("snapshot = %v, want nil", s)
		}
	})
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
	s, err := Build(cfg, Deps{Middlewares: reg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
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
		s, err := Build(cfg, Deps{Metrics: reg})
		if err != nil {
			t.Fatalf("Build %d: %v", i, err)
		}
		t.Cleanup(func() { _ = s.Close() })
	}
}
