package main

import (
	"context"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/store"
	"github.com/ok0/go-microgateway/store/storetest"
)

var testStart = time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)

type testClock struct {
	nanos atomic.Int64
}

func newTestClock() *testClock {
	c := &testClock{}
	c.nanos.Store(testStart.UnixNano())
	return c
}

func (c *testClock) now() time.Time {
	return time.Unix(0, c.nanos.Load())
}

func (c *testClock) advance(d time.Duration) {
	c.nanos.Add(int64(d))
}

func lookupMap(m map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := m[name]
		return v, ok
	}
}

type result struct {
	status int
	header http.Header
	body   string
}

func send(t *testing.T, client *http.Client, base, method, path string, headers map[string]string) result {
	t.Helper()
	req, err := http.NewRequest(method, base+path, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s %s: %v", method, path, err)
	}
	return result{status: resp.StatusCode, header: resp.Header, body: string(body)}
}

func TestEmbedApp(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "backend "+r.URL.Path)
	}))
	t.Cleanup(backend.Close)

	clk := newTestClock()
	a, err := newApp(configYAML, lookupMap(map[string]string{"BACKEND_URL": backend.URL}), clk.now)
	if err != nil {
		t.Fatalf("newApp: %v", err)
	}
	t.Cleanup(func() { _ = a.gateway.Close() })
	srv := httptest.NewServer(a.handler)
	t.Cleanup(srv.Close)
	client := srv.Client()
	keyed := map[string]string{"X-API-Key": "example-api-key-embed"}

	t.Run("application route", func(t *testing.T) {
		res := send(t, client, srv.URL, http.MethodGet, "/app/hello", nil)
		if res.status != http.StatusOK || res.body != "hello from the application" {
			t.Fatalf("got %d %q, want 200 hello from the application", res.status, res.body)
		}
	})

	t.Run("gateway route", func(t *testing.T) {
		res := send(t, client, srv.URL, http.MethodGet, "/api/items", keyed)
		if res.status != http.StatusOK || res.body != "backend /api/items" {
			t.Fatalf("got %d %q, want 200 backend /api/items", res.status, res.body)
		}
		if got := res.header.Get("X-Audit-Consumer"); got != "embed-client" {
			t.Errorf("X-Audit-Consumer = %q, want embed-client", got)
		}
		if res.header.Get("X-Audit-Request-ID") == "" {
			t.Errorf("X-Audit-Request-ID is empty")
		}
	})

	t.Run("order", func(t *testing.T) {
		res := send(t, client, srv.URL, http.MethodGet, "/api/items", nil)
		if res.status != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", res.status)
		}
		if _, ok := res.header["X-Audit-Consumer"]; ok {
			t.Errorf("X-Audit-Consumer present on a 401 response; auth must run before audit")
		}
	})

	t.Run("custom store limit", func(t *testing.T) {
		res := send(t, client, srv.URL, http.MethodGet, "/api/items", keyed)
		if res.status != http.StatusOK {
			t.Fatalf("second keyed request status = %d, want 200", res.status)
		}
		res = send(t, client, srv.URL, http.MethodGet, "/api/items", keyed)
		if res.status != http.StatusTooManyRequests {
			t.Fatalf("third keyed request status = %d, want 429", res.status)
		}
		if res.header.Get("Retry-After") == "" {
			t.Errorf("429 response has no Retry-After header")
		}
		if got := res.header.Get("X-Audit-Consumer"); got != "embed-client" {
			t.Errorf("X-Audit-Consumer on 429 = %q, want embed-client; audit must run before rate_limit", got)
		}
	})

	t.Run("reload", func(t *testing.T) {
		changed := strings.NewReplacer(
			"X-Audit-Consumer", "X-Audit-Client",
			"rate: 2", "rate: 4",
			"burst: 2", "burst: 4",
		).Replace(string(configYAML))
		clk.advance(time.Minute)
		if err := a.gateway.ReloadYAML([]byte(changed)); err != nil {
			t.Fatalf("ReloadYAML: %v", err)
		}
		res := send(t, client, srv.URL, http.MethodGet, "/api/items", keyed)
		if res.status != http.StatusOK {
			t.Fatalf("status after reload = %d, want 200", res.status)
		}
		if got := res.header.Get("X-Audit-Client"); got != "embed-client" {
			t.Errorf("X-Audit-Client = %q, want embed-client", got)
		}
		if _, ok := res.header["X-Audit-Consumer"]; ok {
			t.Errorf("X-Audit-Consumer still present after reload")
		}
	})

	t.Run("admin", func(t *testing.T) {
		rec := httptest.NewRecorder()
		a.gateway.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /readyz = %d, want 200", rec.Code)
		}
	})

	t.Run("close", func(t *testing.T) {
		if err := a.gateway.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		res := send(t, client, srv.URL, http.MethodGet, "/api/items", keyed)
		if res.status != http.StatusServiceUnavailable {
			t.Fatalf("request after Close = %d, want 503", res.status)
		}
	})
}

func TestExampleLimiterConformance(t *testing.T) {
	var current *testClock
	storetest.RateLimiter(t, storetest.Harness[store.RateLimiter]{
		New: func(t *testing.T) (store.RateLimiter, store.RateLimiter) {
			current = newTestClock()
			lim := newMemoryLimiter(current.now, defaultMaxKeys)
			return lim.handle(), lim.handle()
		},
		Advance: func(t *testing.T, d time.Duration) {
			current.advance(d)
		},
	})
}

func TestAllowListFetcher(t *testing.T) {
	const keySet = `{"keys":[]}`
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, keySet)
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	f := newAllowListFetcher(srv.Client(), u.Hostname())

	body, err := f.FetchKeySet(context.Background(), "example-jwt", srv.URL+"/jwks.json")
	if err != nil {
		t.Fatalf("FetchKeySet allowed host: %v", err)
	}
	if string(body) != keySet {
		t.Errorf("body = %q, want %q", body, keySet)
	}

	for _, raw := range []string{
		"https://other.example.net/jwks.json",
		"http://" + u.Host + "/jwks.json",
	} {
		_, err := f.FetchKeySet(context.Background(), "example-jwt", raw)
		if err == nil {
			t.Errorf("FetchKeySet(%q) succeeded, want an error", raw)
			continue
		}
		if strings.Contains(err.Error(), raw) {
			t.Errorf("error %q contains the URL", err)
		}
	}
}

func TestEmbedImportsArePublic(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no Go files found")
	}
	fset := token.NewFileSet()
	for _, name := range files {
		f, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, imp := range f.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatalf("%s: unquote import: %v", name, err)
			}
			for _, seg := range strings.Split(path, "/") {
				if seg == "internal" {
					t.Errorf("%s imports internal package %s", name, path)
				}
			}
		}
	}
}
