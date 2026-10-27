package lychgate

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func gwWith(t *testing.T, doc string, opts ...Option) *Gateway {
	t.Helper()
	g, err := NewFromYAML([]byte(doc), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func routeDoc(backendURL, routeExtra string) string {
	return "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: all
    upstream: up
`, backendURL) + routeExtra
}

type tagRecorder struct {
	mu   sync.Mutex
	seen []string
}

func (r *tagRecorder) add(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, s)
}

func (r *tagRecorder) get() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.seen)
}

func TestUserMiddlewareOrder(t *testing.T) {
	be := backend(t, "b")
	rec := &tagRecorder{}
	tag := func(p MiddlewareParams) (Middleware, error) {
		var c struct {
			Label string `yaml:"label"`
		}
		if err := p.Decode(&c); err != nil {
			return nil, err
		}
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				deadline := "nodeadline"
				if _, ok := r.Context().Deadline(); ok {
					deadline = "deadline"
				}
				rec.add(c.Label + " " + deadline)
				next.ServeHTTP(w, r)
			})
		}, nil
	}
	doc := routeDoc(be.URL, "    middlewares: [first, second]\n    timeout: 5s\n") + `middlewares:
  - name: first
    type: tag
    params:
      label: "1"
  - name: second
    type: tag
    params:
      label: "2"
`
	g := gwWith(t, doc, WithMiddleware("tag", tag))
	serve(t, g, httptest.NewRequest(http.MethodGet, "/x", nil))
	if got, want := rec.get(), []string{"1 nodeadline", "2 nodeadline"}; !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

func TestUserMiddlewareDecodeError(t *testing.T) {
	be := backend(t, "b")
	tag := func(p MiddlewareParams) (Middleware, error) {
		var c struct {
			Label string `yaml:"label"`
		}
		if err := p.Decode(&c); err != nil {
			return nil, err
		}
		return func(next http.Handler) http.Handler { return next }, nil
	}
	doc := routeDoc(be.URL, "    middlewares: [first]\n") + `middlewares:
  - name: first
    type: tag
    params:
      label: x
      bogus: 1
`
	_, err := NewFromYAML([]byte(doc), WithMiddleware("tag", tag))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"middlewares[0]", "params.bogus", `unknown field "bogus"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err, want)
		}
	}
}

func TestNewMiddlewareErrors(t *testing.T) {
	be := backend(t, "b")
	ok := func(p MiddlewareParams) (Middleware, error) {
		return func(next http.Handler) http.Handler { return next }, nil
	}
	failing := func(p MiddlewareParams) (Middleware, error) {
		return nil, errors.New("factory refused the params")
	}
	cases := []struct {
		name string
		doc  string
		opts []Option
		want []string
	}{
		{
			name: "unknown type",
			doc:  routeDoc(be.URL, "    middlewares: [m]\n") + "middlewares:\n  - name: m\n    type: nope\n",
			want: []string{"middlewares[0].type"},
		},
		{
			name: "factory error",
			doc:  routeDoc(be.URL, "    middlewares: [m]\n") + "middlewares:\n  - name: m\n    type: x\n",
			opts: []Option{WithMiddleware("x", failing)},
			want: []string{"middlewares[0]", "factory refused the params"},
		},
		{
			name: "duplicate registration",
			doc:  routeDoc(be.URL, ""),
			opts: []Option{WithMiddleware("x", ok), WithMiddleware("x", ok)},
			want: []string{`WithMiddleware("x")`, "already registered"},
		},
		{
			name: "built-in collision",
			doc:  routeDoc(be.URL, ""),
			opts: []Option{WithMiddleware("cors", ok)},
			want: []string{"built in"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewFromYAML([]byte(tc.doc), tc.opts...)
			if err == nil {
				t.Fatal("expected an error")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not contain %q", err, want)
				}
			}
		})
	}
}

func TestRecoverUserMiddlewarePanic(t *testing.T) {
	be := backend(t, "b")
	var fired atomic.Bool
	boom := func(p MiddlewareParams) (Middleware, error) {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if fired.CompareAndSwap(false, true) {
					panic("boom")
				}
				next.ServeHTTP(w, r)
			})
		}, nil
	}
	doc := routeDoc(be.URL, "    middlewares: [m]\n") + "middlewares:\n  - name: m\n    type: boom\n"
	g := gwWith(t, doc, WithMiddleware("boom", boom))

	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	resp := rec.Result()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	e := decodeErr(t, resp)
	if e.Code != "internal_error" {
		t.Fatalf("code = %q, want internal_error", e.Code)
	}
	id := rec.Header().Get("X-Request-ID")
	if id == "" || e.RequestID != id {
		t.Fatalf("request_id = %q, want non-empty equal to X-Request-ID %q", e.RequestID, id)
	}

	serve(t, g, httptest.NewRequest(http.MethodGet, "/x", nil))
}

func TestAbortHandlerNotSwallowed(t *testing.T) {
	be := backend(t, "b")
	var fired atomic.Bool
	abort := func(p MiddlewareParams) (Middleware, error) {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if fired.CompareAndSwap(false, true) {
					panic(http.ErrAbortHandler)
				}
				next.ServeHTTP(w, r)
			})
		}, nil
	}
	doc := routeDoc(be.URL, "    middlewares: [m]\n") + "middlewares:\n  - name: m\n    type: abort\n"
	g := gwWith(t, doc, WithMiddleware("abort", abort))
	ts := httptest.NewServer(g)
	t.Cleanup(ts.Close)

	if resp, err := ts.Client().Get(ts.URL + "/x"); err == nil {
		resp.Body.Close()
		t.Fatal("expected the connection to be aborted")
	}
	resp, err := ts.Client().Get(ts.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestRouteTimeout504(t *testing.T) {
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(be.Close)
	g := gwWith(t, routeDoc(be.URL, "    timeout: 100ms\n"))

	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	resp := rec.Result()
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", resp.StatusCode)
	}
	if e := decodeErr(t, resp); e.Code != "gateway_timeout" {
		t.Fatalf("code = %q, want gateway_timeout", e.Code)
	}
}

func TestRouteTimeoutSSE(t *testing.T) {
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 1; i <= 3; i++ {
			_, _ = fmt.Fprintf(w, "data: %d\n\n", i)
			_ = http.NewResponseController(w).Flush()
			if i < 3 {
				time.Sleep(50 * time.Millisecond)
			}
		}
	}))
	t.Cleanup(be.Close)
	g := gwWith(t, routeDoc(be.URL, "    timeout: 2s\n"))
	gwTS := httptest.NewServer(g)
	t.Cleanup(gwTS.Close)

	resp, err := gwTS.Client().Get(gwTS.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	reader := bufio.NewReader(resp.Body)
	var events []string
	for len(events) < 3 {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "data: ") {
			events = append(events, strings.TrimSpace(line))
		}
	}
	if want := []string{"data: 1", "data: 2", "data: 3"}; !slices.Equal(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	_, _ = io.Copy(io.Discard, reader)
}

func TestIdentityFromContextFalse(t *testing.T) {
	be := backend(t, "b")
	var observed []bool
	probe := func(p MiddlewareParams) (Middleware, error) {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, ok := IdentityFromContext(r.Context())
				observed = append(observed, ok)
				next.ServeHTTP(w, r)
			})
		}, nil
	}
	doc := routeDoc(be.URL, "    middlewares: [p]\n") + "middlewares:\n  - name: p\n    type: probe\n"
	g := gwWith(t, doc, WithMiddleware("probe", probe))
	serve(t, g, httptest.NewRequest(http.MethodGet, "/x", nil))
	if !slices.Equal(observed, []bool{false}) {
		t.Fatalf("identity observed = %v, want [false]", observed)
	}
}
