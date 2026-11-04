package admin_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/internal/admin"
	"github.com/ok0/go-microgateway/internal/cache"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/store"
)

const (
	tokA = "tok-A-5Wq"
	tokB = "tok-B-8Ld"
)

type purgeCall struct {
	Req   cache.PurgeRequest
	KeyID string
}

type purgeSource struct {
	*fakeSource
	access admin.PurgeAccess
	result cache.PurgeResult
	err    error
	calls  []purgeCall
}

func (p *purgeSource) PurgeAccess() admin.PurgeAccess { return p.access }

func (p *purgeSource) Purge(ctx context.Context, req cache.PurgeRequest, keyID string) (cache.PurgeResult, error) {
	p.calls = append(p.calls, purgeCall{Req: req, KeyID: keyID})
	return p.result, p.err
}

func newPurgeSource(t *testing.T, access admin.PurgeAccess) *purgeSource {
	t.Helper()
	return &purgeSource{fakeSource: newFake(t), access: access}
}

func keyedAccess(cacheOn bool) admin.PurgeAccess {
	a := sha256.Sum256([]byte(tokA))
	b := sha256.Sum256([]byte(tokB))
	return admin.PurgeAccess{KeyIDs: []string{"a", "b"}, Hashes: [][32]byte{a, b}, Cache: cacheOn}
}

// failReader fails the test when the handler reads the body.
type failReader struct{ t *testing.T }

func (f failReader) Read(p []byte) (int, error) {
	f.t.Errorf("body read")
	return 0, io.EOF
}

func purgeReq(method, auth string, body io.Reader) *http.Request {
	req := httptest.NewRequest(method, "/cache/purge", body)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	return req
}

func postPurge(h http.Handler, token, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, purgeReq(http.MethodPost, "Bearer "+token, strings.NewReader(body)))
	return rec
}

func errorBody(t *testing.T, rec *httptest.ResponseRecorder) (code, message string) {
	t.Helper()
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body %q: %v", rec.Body.String(), err)
	}
	return body.Error.Code, body.Error.Message
}

func padded(n int) string {
	const base = `{"all":true}`
	return base + strings.Repeat(" ", n-len(base))
}

func TestPurgeOrder(t *testing.T) {
	bearerA := "Bearer " + tokA
	tests := []struct {
		name    string
		method  string
		auth    string
		keys    bool
		cacheOn bool
		reach   bool
		status  int
		code    string
		allow   string
		www     string
	}{
		{name: "no keys POST", method: http.MethodPost, auth: bearerA, status: 404, code: "not_found"},
		{name: "no keys GET", method: http.MethodGet, auth: bearerA, status: 404, code: "not_found"},
		{name: "no keys DELETE", method: http.MethodDelete, auth: bearerA, status: 404, code: "not_found"},
		{name: "keys GET", method: http.MethodGet, auth: bearerA, keys: true, cacheOn: true, status: 405, code: "method_not_allowed", allow: "POST"},
		{name: "keys HEAD", method: http.MethodHead, auth: bearerA, keys: true, cacheOn: true, status: 405, code: "method_not_allowed", allow: "POST"},
		{name: "keys PUT", method: http.MethodPut, auth: bearerA, keys: true, cacheOn: true, status: 405, code: "method_not_allowed", allow: "POST"},
		{name: "no header", method: http.MethodPost, keys: true, cacheOn: true, status: 401, code: "unauthorized", www: "Bearer"},
		{name: "basic scheme", method: http.MethodPost, auth: "Basic dXNlcjpwdw==", keys: true, cacheOn: true, status: 401, code: "unauthorized", www: "Bearer"},
		{name: "bare scheme", method: http.MethodPost, auth: "Bearer", keys: true, cacheOn: true, status: 401, code: "unauthorized", www: "Bearer"},
		{name: "blank token", method: http.MethodPost, auth: "Bearer   ", keys: true, cacheOn: true, status: 401, code: "unauthorized", www: "Bearer"},
		{name: "wrong token", method: http.MethodPost, auth: "Bearer wrong", keys: true, cacheOn: true, status: 403, code: "forbidden"},
		{name: "cache off", method: http.MethodPost, auth: bearerA, keys: true, status: 404, code: "cache_not_configured"},
		{name: "cache off wrong token", method: http.MethodPost, auth: "Bearer wrong", keys: true, status: 403, code: "forbidden"},
		{name: "lower-case scheme", method: http.MethodPost, auth: "bearer " + tokA, keys: true, cacheOn: true, reach: true, status: http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := newPurgeSource(t, admin.PurgeAccess{})
			if tc.keys {
				src.access = keyedAccess(tc.cacheOn)
			}
			var body io.Reader = failReader{t}
			if tc.reach {
				body = strings.NewReader(`{"all":true}`)
			}
			rec := httptest.NewRecorder()
			admin.Handler(src, nil).ServeHTTP(rec, purgeReq(tc.method, tc.auth, body))
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.status, rec.Body.String())
			}
			if got := rec.Header().Get("Cache-Control"); got != "no-store" {
				t.Errorf("Cache-Control = %q, want no-store", got)
			}
			if tc.reach {
				if len(src.calls) != 1 || src.calls[0].KeyID != "a" || !src.calls[0].Req.All {
					t.Fatalf("calls = %+v, want one all purge for key a", src.calls)
				}
				return
			}
			if len(src.calls) != 0 {
				t.Errorf("Purge called %d times, want 0", len(src.calls))
			}
			code, _ := errorBody(t, rec)
			if code != tc.code {
				t.Errorf("code = %q, want %q", code, tc.code)
			}
			if got := rec.Header().Get("Allow"); got != tc.allow {
				t.Errorf("Allow = %q, want %q", got, tc.allow)
			}
			if got := rec.Header().Get("WWW-Authenticate"); got != tc.www {
				t.Errorf("WWW-Authenticate = %q, want %q", got, tc.www)
			}
		})
	}
}

func TestPurgeBody(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"not json", `not json`},
		{"two objects", `{"all":true} {}`},
		{"empty", ``},
		{"blank", `   `},
		{"null", `null`},
		{"array", `[]`},
		{"unknown field", `{"all":true,"x":1}`},
		{"urls not a list", `{"urls":"http://h/"}`},
		{"url not a string", `{"urls":[1]}`},
		{"urls null", `{"urls":null}`},
		{"tags object", `{"tags":{}}`},
		{"all string", `{"all":"true"}`},
		{"all null", `{"all":null}`},
		{"soft not bool", `{"all":true,"soft":1}`},
		{"soft without selector", `{"soft":true}`},
		{"all false", `{"all":false}`},
		{"all false and urls", `{"all":false,"urls":["http://h/"]}`},
		{"urls and tags", `{"urls":["http://h/"],"tags":["t"]}`},
		{"empty urls", `{"urls":[]}`},
		{"empty tags", `{"tags":[]}`},
		{"over limit", padded(1<<20 + 1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := newPurgeSource(t, keyedAccess(true))
			rec := postPurge(admin.Handler(src, nil), tokA, tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
			code, message := errorBody(t, rec)
			if code != "bad_request" {
				t.Errorf("code = %q, want bad_request", code)
			}
			if !strings.HasPrefix(message, "invalid purge request: ") {
				t.Errorf("message = %q, want prefix %q", message, "invalid purge request: ")
			}
			if len(src.calls) != 0 {
				t.Errorf("Purge called %d times, want 0", len(src.calls))
			}
		})
	}
}

func TestPurgeBodyValid(t *testing.T) {
	tests := []struct {
		name string
		body string
		want cache.PurgeRequest
	}{
		{"exact limit", padded(1 << 20), cache.PurgeRequest{All: true}},
		{"urls soft", `{"urls":["http://h/a"],"soft":true}`, cache.PurgeRequest{URLs: []string{"http://h/a"}, Soft: true}},
		{"tags hard", `{"tags":["t"],"soft":false}`, cache.PurgeRequest{Tags: []string{"t"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := newPurgeSource(t, keyedAccess(true))
			rec := postPurge(admin.Handler(src, nil), tokA, tc.body)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
			}
			if len(src.calls) != 1 {
				t.Fatalf("Purge called %d times, want 1", len(src.calls))
			}
			if !reflect.DeepEqual(src.calls[0].Req, tc.want) {
				t.Errorf("request = %+v, want %+v", src.calls[0].Req, tc.want)
			}
		})
	}
}

func TestPurgeErrorMapping(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		status  int
		code    string
		message string
	}{
		{"request error", &cache.RequestError{Reason: "urls[0]: empty host"}, http.StatusBadRequest, "bad_request", "invalid purge request: urls[0]: empty host"},
		{"cache disabled", cache.ErrCacheDisabled, http.StatusNotFound, "cache_not_configured", ""},
		{"store unavailable", fmt.Errorf("x: %w", store.ErrUnavailable), http.StatusServiceUnavailable, "store_unavailable", ""},
		{"other failure", errors.New("closed"), http.StatusServiceUnavailable, "service_unavailable", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := newPurgeSource(t, keyedAccess(true))
			src.err = tc.err
			rec := postPurge(admin.Handler(src, nil), tokA, `{"all":true}`)
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d", rec.Code, tc.status)
			}
			code, message := errorBody(t, rec)
			if code != tc.code {
				t.Errorf("code = %q, want %q", code, tc.code)
			}
			if !strings.Contains(message, tc.message) {
				t.Errorf("message = %q, want it to contain %q", message, tc.message)
			}
		})
	}
}

func TestPurgeCanceledClient(t *testing.T) {
	src := newPurgeSource(t, keyedAccess(true))
	src.err = context.Canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := purgeReq(http.MethodPost, "Bearer "+tokA, strings.NewReader(`{"all":true}`)).WithContext(ctx)
	rec := httptest.NewRecorder()
	admin.Handler(src, nil).ServeHTTP(rec, req)
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty", rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "" {
		t.Errorf("Content-Type = %q, want none", ct)
	}
}

func TestPurgeSuccess(t *testing.T) {
	src := newPurgeSource(t, keyedAccess(true))
	src.result = cache.PurgeResult{Type: "url", Soft: false, Accepted: 3, Affected: 2, AffectedKnown: true, Scope: "instance"}
	rec := postPurge(admin.Handler(src, nil), tokA, `{"urls":["http://h/a"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	want := `{"type":"url","soft":false,"accepted":3,"affected":2,"scope":"instance"}` + "\n"
	if got := rec.Body.String(); got != want {
		t.Errorf("body = %q, want %q", got, want)
	}

	src = newPurgeSource(t, keyedAccess(true))
	src.result = cache.PurgeResult{Type: "tag", Accepted: 1, Scope: "store"}
	rec = postPurge(admin.Handler(src, nil), tokA, `{"tags":["t"]}`)
	want = `{"type":"tag","soft":false,"accepted":1,"scope":"store"}` + "\n"
	if got := rec.Body.String(); got != want {
		t.Errorf("unknown affected body = %q, want %q", got, want)
	}

	src = newPurgeSource(t, keyedAccess(true))
	rec = postPurge(admin.Handler(src, nil), tokB, `{"all":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("tokB status = %d, want 200", rec.Code)
	}
	if len(src.calls) != 1 || src.calls[0].KeyID != "b" {
		t.Errorf("calls = %+v, want one call with key b", src.calls)
	}
}

func TestPurgeRejectLog(t *testing.T) {
	var buf bytes.Buffer
	src := newPurgeSource(t, keyedAccess(true))
	h := admin.Handler(src, slog.New(slog.NewJSONHandler(&buf, nil)))
	h.ServeHTTP(httptest.NewRecorder(), purgeReq(http.MethodPost, "Basic dXNlcjpwdw==", failReader{t}))
	h.ServeHTTP(httptest.NewRecorder(), purgeReq(http.MethodPost, "Bearer wrong-Xq9", failReader{t}))

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("log lines = %d, want 2: %q", len(lines), buf.String())
	}
	for _, line := range lines {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		if entry["level"] != "WARN" {
			t.Errorf("level = %v, want WARN", entry["level"])
		}
	}
	out := buf.String()
	for _, secret := range []string{tokA, tokB, "wrong-Xq9", "dXNlcjpwdw"} {
		if strings.Contains(out, secret) {
			t.Errorf("log contains %q", secret)
		}
	}
	for _, tok := range []string{tokA, tokB} {
		sum := sha256.Sum256([]byte(tok))
		if strings.Contains(out, hex.EncodeToString(sum[:])) {
			t.Errorf("log contains the hash of a key")
		}
	}
}

func TestOtherEndpointsUnchanged(t *testing.T) {
	src := newPurgeSource(t, keyedAccess(true))
	h := admin.Handler(src, nil)
	for _, tc := range []struct{ path, contentType string }{
		{"/livez", "application/json"},
		{"/readyz", "application/json"},
		{"/config", "application/json"},
		{"/upstreams", "application/json"},
		{"/metrics", metrics.ContentType},
	} {
		rec := serve(t, h, http.MethodGet, tc.path)
		if rec.Code != http.StatusOK {
			t.Errorf("%s status = %d, want 200", tc.path, rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); ct != tc.contentType {
			t.Errorf("%s Content-Type = %q, want %q", tc.path, ct, tc.contentType)
		}
	}
	rec := serve(t, h, http.MethodPost, "/metrics")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /metrics status = %d, want 405", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); allow != "GET, HEAD" {
		t.Errorf("POST /metrics Allow = %q, want %q", allow, "GET, HEAD")
	}
}
