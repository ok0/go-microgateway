package lychgate

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

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
	_, err := NewFromYAML([]byte(doc))
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
