package middleware

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/httperr"
)

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", rec.Body.String(), err)
	}
	return body.Error.Code
}

func TestHeadersRequest(t *testing.T) {
	p := config.HeadersParams{Request: config.HeaderOps{
		Add:    map[string]config.Secret{"X-Add": config.NewSecret("a")},
		Set:    map[string]config.Secret{"X-Set": config.NewSecret("s")},
		Remove: []string{"X-Gone"},
	}}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Gone", "1")
	var got http.Header
	Headers(p)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
	})).ServeHTTP(httptest.NewRecorder(), req)
	if got.Get("X-Add") != "a" {
		t.Errorf("X-Add = %q, want a", got.Get("X-Add"))
	}
	if got.Get("X-Set") != "s" {
		t.Errorf("X-Set = %q, want s", got.Get("X-Set"))
	}
	if _, ok := got["X-Gone"]; ok {
		t.Error("X-Gone still present")
	}
}

func TestHeadersResponseOnWriteHeader(t *testing.T) {
	p := config.HeadersParams{Response: config.HeaderOps{
		Set:    map[string]config.Secret{"X-Up": config.NewSecret("2")},
		Add:    map[string]config.Secret{"X-A": config.NewSecret("a")},
		Remove: []string{"X-B"},
	}}
	rec := httptest.NewRecorder()
	Headers(p)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Up", "1")
		w.Header().Set("X-B", "b")
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := rec.Header().Get("X-Up"); got != "2" {
		t.Errorf("X-Up = %q, want 2", got)
	}
	if got := rec.Header().Get("X-A"); got != "a" {
		t.Errorf("X-A = %q, want a", got)
	}
	if got := rec.Header().Get("X-B"); got != "" {
		t.Errorf("X-B = %q, want removed", got)
	}
}

func TestHeadersResponseImplicit(t *testing.T) {
	p := config.HeadersParams{Response: config.HeaderOps{
		Set: map[string]config.Secret{"X-Set": config.NewSecret("s")},
	}}
	rec := httptest.NewRecorder()
	Headers(p)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "body")
	})).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := rec.Header().Get("X-Set"); got != "s" {
		t.Errorf("X-Set = %q, want s", got)
	}
	if rec.Body.String() != "body" {
		t.Errorf("body = %q", rec.Body.String())
	}
}

func TestHeadersFlushAndInformational(t *testing.T) {
	p := config.HeadersParams{Response: config.HeaderOps{
		Add: map[string]config.Secret{"X-Count": config.NewSecret("1")},
	}}
	rec := httptest.NewRecorder()
	var flushErr error
	Headers(p)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusContinue)
		flushErr = http.NewResponseController(w).Flush()
	})).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if flushErr != nil {
		t.Errorf("Flush: %v", flushErr)
	}
	if !rec.Flushed {
		t.Error("recorder not flushed")
	}
	if got := rec.Header().Values("X-Count"); len(got) != 1 || got[0] != "1" {
		t.Errorf("X-Count = %v, want [1]", got)
	}
}

func TestSizeLimitContentLength(t *testing.T) {
	called := false
	h := RequestSizeLimit(config.RequestSizeLimitParams{MaxBytes: 10})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader("12345678901")))
	if called {
		t.Error("next was called")
	}
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", rec.Code)
	}
	if got := errorCode(t, rec); got != httperr.CodePayloadTooLarge {
		t.Errorf("code = %q", got)
	}
}

func TestSizeLimitStreaming(t *testing.T) {
	h := RequestSizeLimit(config.RequestSizeLimitParams{MaxBytes: 10})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.ReadAll(r.Body)
		var mbe *http.MaxBytesError
		if !errors.As(err, &mbe) {
			t.Errorf("read error = %v, want *http.MaxBytesError", err)
		}
		httperr.Write(w, r, http.StatusBadGateway, httperr.CodeBadGateway, httperr.Message(httperr.CodeBadGateway))
	}))
	req := httptest.NewRequest(http.MethodPost, "/", io.MultiReader(strings.NewReader("0123456789"), strings.NewReader("abcdefghij")))
	req.ContentLength = -1
	rec := httptest.NewRecorder()
	rec.Header().Set("X-Request-ID", "rid")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", rec.Code)
	}
	if got := errorCode(t, rec); got != httperr.CodePayloadTooLarge {
		t.Errorf("code = %q", got)
	}
	if got := rec.Header().Get("X-Request-ID"); got != "rid" {
		t.Errorf("X-Request-ID = %q, want rid", got)
	}
}

func TestSizeLimitWithin(t *testing.T) {
	var body string
	h := RequestSizeLimit(config.RequestSizeLimitParams{MaxBytes: 10})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("ReadAll: %v", err)
		}
		body = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/", io.MultiReader(strings.NewReader("0123456789")))
	req.ContentLength = -1
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if body != "0123456789" {
		t.Errorf("body = %q", body)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

func TestSizeLimitAfterResponseStarted(t *testing.T) {
	var readErr error
	h := RequestSizeLimit(config.RequestSizeLimitParams{MaxBytes: 10})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = http.NewResponseController(w).Flush()
		_, readErr = io.ReadAll(r.Body)
	}))
	req := httptest.NewRequest(http.MethodPost, "/", io.MultiReader(strings.NewReader("0123456789abcdefghij")))
	req.ContentLength = -1
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var mbe *http.MaxBytesError
	if !errors.As(readErr, &mbe) {
		t.Errorf("read error = %v, want *http.MaxBytesError", readErr)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

func corsMiddleware(t *testing.T, params map[string]any) Middleware {
	t.Helper()
	v, err := config.Middleware{Name: "c", Type: "cors", Params: config.NewSecretParams(params)}.BuiltinParams()
	if err != nil {
		t.Fatalf("BuiltinParams: %v", err)
	}
	return CORS(*v.(*config.CORSParams))
}

func TestCORSNoOrigin(t *testing.T) {
	h := corsMiddleware(t, map[string]any{"allowed_origins": []any{"https://a.example"}})
	called := false
	rec := httptest.NewRecorder()
	h(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if !called {
		t.Error("next not called")
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin = %q, want empty", got)
	}
	if got := rec.Header().Values("Vary"); len(got) != 0 {
		t.Errorf("Vary = %v, want none", got)
	}
}

func TestCORSPreflightAllowed(t *testing.T) {
	h := corsMiddleware(t, map[string]any{
		"allowed_origins":   []any{"https://a.example"},
		"allowed_headers":   []any{"X-Token"},
		"allow_credentials": true,
		"max_age":           600,
	})
	called := false
	req := httptest.NewRequest(http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "https://a.example")
	req.Header.Set("Access-Control-Request-Method", "POST")
	rec := httptest.NewRecorder()
	h(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})).ServeHTTP(rec, req)
	if called {
		t.Error("next was called for preflight")
	}
	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rec.Code)
	}
	checks := map[string]string{
		"Access-Control-Allow-Origin":      "https://a.example",
		"Access-Control-Allow-Methods":     "GET, HEAD, POST",
		"Access-Control-Allow-Headers":     "X-Token",
		"Access-Control-Max-Age":           "600",
		"Access-Control-Allow-Credentials": "true",
	}
	for k, want := range checks {
		if got := rec.Header().Get(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	if !strings.Contains(strings.Join(rec.Header().Values("Vary"), ","), "Origin") {
		t.Errorf("Vary = %v, want Origin", rec.Header().Values("Vary"))
	}
}

func TestCORSPreflightDenied(t *testing.T) {
	h := corsMiddleware(t, map[string]any{"allowed_origins": []any{"https://a.example"}})
	called := false
	req := httptest.NewRequest(http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "https://evil.example")
	req.Header.Set("Access-Control-Request-Method", "GET")
	rec := httptest.NewRecorder()
	h(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})).ServeHTTP(rec, req)
	if called {
		t.Error("next was called for denied preflight")
	}
	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", rec.Code)
	}
	if got := errorCode(t, rec); got != httperr.CodeForbidden {
		t.Errorf("code = %q, want forbidden", got)
	}
}

func TestCORSActualAllowed(t *testing.T) {
	h := corsMiddleware(t, map[string]any{
		"allowed_origins": []any{"https://a.example"},
		"expose_headers":  []any{"X-Out"},
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://a.example")
	rec := httptest.NewRecorder()
	h(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "x")
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)
	acao := rec.Header().Values("Access-Control-Allow-Origin")
	if len(acao) != 1 || acao[0] != "https://a.example" {
		t.Errorf("Access-Control-Allow-Origin = %v, want [https://a.example]", acao)
	}
	if got := rec.Header().Get("Access-Control-Expose-Headers"); got != "X-Out" {
		t.Errorf("Access-Control-Expose-Headers = %q, want X-Out", got)
	}
	if !strings.Contains(strings.Join(rec.Header().Values("Vary"), ","), "Origin") {
		t.Errorf("Vary = %v, want Origin", rec.Header().Values("Vary"))
	}
}

func TestCORSActualDenied(t *testing.T) {
	h := corsMiddleware(t, map[string]any{"allowed_origins": []any{"https://a.example"}})
	called := false
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://evil.example")
	rec := httptest.NewRecorder()
	h(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)
	if !called {
		t.Error("next not called")
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin = %q, want empty", got)
	}
	if !strings.Contains(strings.Join(rec.Header().Values("Vary"), ","), "Origin") {
		t.Errorf("Vary = %v, want Origin", rec.Header().Values("Vary"))
	}
}

func TestCORSWildcard(t *testing.T) {
	h := corsMiddleware(t, map[string]any{"allowed_origins": []any{"*"}})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://any.example")
	rec := httptest.NewRecorder()
	h(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q, want *", got)
	}
	if got := rec.Header().Values("Vary"); len(got) != 0 {
		t.Errorf("Vary = %v, want none", got)
	}
}

func TestCORSOptionsWithoutRequestMethod(t *testing.T) {
	h := corsMiddleware(t, map[string]any{"allowed_origins": []any{"https://a.example"}})
	called := false
	req := httptest.NewRequest(http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "https://a.example")
	h(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})).ServeHTTP(httptest.NewRecorder(), req)
	if !called {
		t.Error("next not called for OPTIONS without Access-Control-Request-Method")
	}
}
