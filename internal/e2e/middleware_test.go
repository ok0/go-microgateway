package e2e

import (
	"bufio"
	"bytes"
	"encoding/json"
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

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/leakcheck"
	"github.com/ok0/go-microgateway/internal/testwait"
)

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
	tag := func(p microgateway.MiddlewareParams) (microgateway.Middleware, error) {
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
	g := gwWith(t, doc, microgateway.WithMiddleware("tag", tag))
	serve(t, g, httptest.NewRequest(http.MethodGet, "/x", nil))
	if got, want := rec.get(), []string{"1 nodeadline", "2 nodeadline"}; !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

func TestUserMiddlewareDecodeError(t *testing.T) {
	be := backend(t, "b")
	tag := func(p microgateway.MiddlewareParams) (microgateway.Middleware, error) {
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
	_, err := newGatewayFromYAML([]byte(doc), microgateway.WithMiddleware("tag", tag))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"middlewares[0]", "params.bogus", `unknown key "bogus"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err, want)
		}
	}
}

func TestNewMiddlewareErrors(t *testing.T) {
	be := backend(t, "b")
	ok := func(p microgateway.MiddlewareParams) (microgateway.Middleware, error) {
		return func(next http.Handler) http.Handler { return next }, nil
	}
	failing := func(p microgateway.MiddlewareParams) (microgateway.Middleware, error) {
		return nil, errors.New("factory refused the params")
	}
	cases := []struct {
		name string
		doc  string
		opts []microgateway.Option
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
			opts: []microgateway.Option{microgateway.WithMiddleware("x", failing)},
			want: []string{"middlewares[0]", "factory refused the params"},
		},
		{
			name: "duplicate registration",
			doc:  routeDoc(be.URL, ""),
			opts: []microgateway.Option{microgateway.WithMiddleware("x", ok), microgateway.WithMiddleware("x", ok)},
			want: []string{`WithMiddleware("x")`, "already registered"},
		},
		{
			name: "built-in collision",
			doc:  routeDoc(be.URL, ""),
			opts: []microgateway.Option{microgateway.WithMiddleware("cors", ok)},
			want: []string{"built in"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newGatewayFromYAML([]byte(tc.doc), tc.opts...)
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
	boom := func(p microgateway.MiddlewareParams) (microgateway.Middleware, error) {
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
	g := gwWith(t, doc, microgateway.WithMiddleware("boom", boom))

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
	abort := func(p microgateway.MiddlewareParams) (microgateway.Middleware, error) {
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
	g := gwWith(t, doc, microgateway.WithMiddleware("abort", abort))
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
	arrived := make(chan struct{}, 1)
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case arrived <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	}))
	t.Cleanup(be.Close)
	fake := clock.NewFake(time.Now())
	g := gwWith(t, routeDoc(be.URL, "    timeout: 100ms\n"), withClock(fake))

	rec := httptest.NewRecorder()
	served := make(chan struct{})
	go func() {
		defer close(served)
		g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	}()
	testwait.Recv(t, "request to reach the hung backend", arrived, nil)
	awaitDue(t, fake, 1, 100*time.Millisecond, "route deadline armed", nil)
	fake.Advance(100 * time.Millisecond)
	testwait.Done(t, "request to end at the route deadline", served, nil)
	resp := rec.Result()
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", resp.StatusCode)
	}
	if e := decodeErr(t, resp); e.Code != "gateway_timeout" {
		t.Fatalf("code = %q, want gateway_timeout", e.Code)
	}
}

func TestRouteTimeoutSSE(t *testing.T) {
	fake := clock.NewFake(time.Now())
	steps := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 1; i <= 3; i++ {
			_, _ = fmt.Fprintf(w, "data: %d\n\n", i)
			_ = http.NewResponseController(w).Flush()
			if i < 3 {
				select {
				case <-steps[i-1]:
				case <-r.Context().Done():
					return
				}
			}
		}
	}))
	t.Cleanup(be.Close)
	g := gwWith(t, routeDoc(be.URL, "    timeout: 2s\n"), withClock(fake))
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
			if n := len(events); n < 3 {
				fake.Advance(50 * time.Millisecond)
				close(steps[n-1])
			}
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
	probe := func(p microgateway.MiddlewareParams) (microgateway.Middleware, error) {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, ok := microgateway.IdentityFromContext(r.Context())
				observed = append(observed, ok)
				next.ServeHTTP(w, r)
			})
		}, nil
	}
	doc := routeDoc(be.URL, "    middlewares: [p]\n") + "middlewares:\n  - name: p\n    type: probe\n"
	g := gwWith(t, doc, microgateway.WithMiddleware("probe", probe))
	serve(t, g, httptest.NewRequest(http.MethodGet, "/x", nil))
	if !slices.Equal(observed, []bool{false}) {
		t.Fatalf("identity observed = %v, want [false]", observed)
	}
}

func assertNoSecretForms(t *testing.T, text, secret string) {
	t.Helper()
	for _, f := range leakcheck.Forms(secret) {
		if strings.Contains(text, f) {
			t.Fatalf("output reveals a secret in form %q", f)
		}
	}
}

func TestRouteHeaderSecretsNeverSurface(t *testing.T) {
	const secret = "rh-LEAK-2a4"
	be := backend(t, "b")
	env := microgateway.WithEnvLookup(func(k string) (string, bool) {
		if k == "UP_TOKEN" {
			return secret, true
		}
		return "", false
	})
	doc := routeDoc(be.URL, "    request_headers:\n      set:\n        Authorization: \"Bearer ${UP_TOKEN}\"\n")
	g := gwWith(t, doc, env)

	_, e := serve(t, g, httptest.NewRequest(http.MethodGet, "/x", nil))
	if got := e.Headers.Get("Authorization"); got != "Bearer "+secret {
		t.Fatal("backend did not receive the substituted Authorization header")
	}

	out, err := json.Marshal(g.Config().Redacted())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "Authorization") {
		t.Fatalf("redacted config %s does not contain the header name", out)
	}
	assertNoSecretForms(t, string(out), secret)
}

func counterBackend(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "{}\n")
	}))
	t.Cleanup(be.Close)
	return be, &hits
}

func builtinDoc(backendURL, mw, params string) string {
	return routeDoc(backendURL, "    middlewares: [b]\n") + "middlewares:\n  - name: b\n    type: " + mw + "\n" + params
}

func TestCORSThroughGateway(t *testing.T) {
	be, hits := counterBackend(t)
	g := gwWith(t, builtinDoc(be.URL, "cors", "    params:\n      allowed_origins: [\"https://app.example.com\"]\n"))
	ts := httptest.NewServer(g)
	t.Cleanup(ts.Close)

	preflight := func(origin string) int {
		req, err := http.NewRequest(http.MethodOptions, ts.URL+"/x", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Origin", origin)
		req.Header.Set("Access-Control-Request-Method", "GET")
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if code := preflight("https://app.example.com"); code != http.StatusNoContent {
		t.Fatalf("allowed preflight status = %d, want 204", code)
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("backend hits after allowed preflight = %d, want 0", n)
	}
	if code := preflight("https://evil.example.com"); code != http.StatusForbidden {
		t.Fatalf("denied preflight status = %d, want 403", code)
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("backend hits after denied preflight = %d, want 0", n)
	}

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", "https://app.example.com")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("actual status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Fatalf("Access-Control-Allow-Origin = %q", got)
	}
	if !strings.Contains(resp.Header.Get("Vary"), "Origin") {
		t.Fatalf("Vary = %q, want Origin", resp.Header.Get("Vary"))
	}
}

func TestCredentialsWildcardRejected(t *testing.T) {
	be, _ := counterBackend(t)
	doc := builtinDoc(be.URL, "cors", "    params:\n      allowed_origins: [\"*\"]\n      allow_credentials: true\n")
	_, err := newGatewayFromYAML([]byte(doc))
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "middlewares[0].params.allow_credentials") {
		t.Fatalf("error %q does not name middlewares[0].params.allow_credentials", err)
	}
}

func TestSizeLimitThroughGateway(t *testing.T) {
	be, hits := counterBackend(t)
	g := gwWith(t, builtinDoc(be.URL, "request_size_limit", "    params:\n      max_bytes: 16\n"))

	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(strings.Repeat("a", 17))))
	resp := rec.Result()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("content-length status = %d, want 413", resp.StatusCode)
	}
	decodeErr(t, resp)
	if n := hits.Load(); n != 0 {
		t.Fatalf("backend hits = %d, want 0", n)
	}

	ts := httptest.NewServer(g)
	t.Cleanup(ts.Close)
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write(bytes.Repeat([]byte("b"), 64<<10))
		_ = pw.Close()
	}()
	chunked, err := http.NewRequest(http.MethodPost, ts.URL+"/x", pr)
	if err != nil {
		t.Fatal(err)
	}
	chunked.ContentLength = -1
	resp, err = ts.Client().Do(chunked)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("chunked status = %d, want 413", resp.StatusCode)
	}
	if e := decodeErr(t, resp); e.Code != "payload_too_large" {
		t.Fatalf("code = %q, want payload_too_large", e.Code)
	}
	eventually(t, "the oversized chunked request to reach the backend", func() bool { return hits.Load() == 1 }, func() string {
		return fmt.Sprintf("backend hits = %d", hits.Load())
	})

	before := hits.Load()
	small := httptest.NewRecorder()
	g.ServeHTTP(small, httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("12345678")))
	if small.Code != http.StatusOK {
		t.Fatalf("small body status = %d, want 200", small.Code)
	}
	if n := hits.Load() - before; n != 1 {
		t.Fatalf("backend hits for small body = %d, want 1", n)
	}
}

func TestHeadersThroughGateway(t *testing.T) {
	be := backend(t, "b")
	doc := routeDoc(be.URL, "    middlewares: [hdrs]\n    request_headers:\n      set:\n        X-Mw: route\n") + `middlewares:
  - name: hdrs
    type: headers
    params:
      request:
        set:
          X-Mw: "1"
      response:
        set:
          X-Resp: mw
`
	g := gwWith(t, doc)
	resp, e := serve(t, g, httptest.NewRequest(http.MethodGet, "/x", nil))
	if got := e.Headers.Get("X-Mw"); got != "route" {
		t.Fatalf("backend X-Mw = %q, want route", got)
	}
	if got := resp.Header.Get("X-Resp"); got != "mw" {
		t.Fatalf("response X-Resp = %q, want mw", got)
	}
}
