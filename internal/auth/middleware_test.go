package auth

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ok0/lychgate/internal/reqctx"
)

type fakeAuth struct {
	name      string
	challenge string
	header    string
	query     string
	forward   bool
	res       Result
	err       error
}

func (f *fakeAuth) Name() string      { return f.name }
func (f *fakeAuth) Challenge() string { return f.challenge }
func (f *fakeAuth) Authenticate(*http.Request) (Result, error) {
	return f.res, f.err
}
func (f *fakeAuth) Credentials() (string, string) { return f.header, f.query }
func (f *fakeAuth) ForwardCredentials() bool      { return f.forward }

func okResult(provider string) Result {
	return Result{Identity: reqctx.Identity{Provider: provider}}
}

type outcome struct {
	rec      *httptest.ResponseRecorder
	req      *http.Request
	called   bool
	failures []Reason
}

func run(t *testing.T, c RouteConfig, r *http.Request) outcome {
	t.Helper()
	var o outcome
	c.OnFailure = func(reason Reason) { o.failures = append(o.failures, reason) }
	next := http.HandlerFunc(func(w http.ResponseWriter, rr *http.Request) {
		o.called = true
		o.req = rr
		w.WriteHeader(http.StatusOK)
	})
	o.rec = httptest.NewRecorder()
	Middleware(c)(next).ServeHTTP(o.rec, r)
	return o
}

func TestMiddlewareModes(t *testing.T) {
	t.Run("first success wins", func(t *testing.T) {
		p1 := &fakeAuth{name: "a", challenge: "Bearer", res: okResult("a")}
		p2 := &fakeAuth{name: "b", challenge: "ApiKey", res: okResult("b")}
		o := run(t, RouteConfig{Providers: []Authenticator{p1, p2}}, httptest.NewRequest(http.MethodGet, "/", nil))
		if !o.called {
			t.Fatal("next not called")
		}
		if id, _ := reqctx.IdentityFrom(o.req.Context()); id.Provider != "a" {
			t.Errorf("provider = %q, want a", id.Provider)
		}
	})

	t.Run("failed provider does not stop later providers", func(t *testing.T) {
		p1 := &fakeAuth{name: "a", challenge: "Bearer", err: Fail(ReasonInvalidKey, nil)}
		p2 := &fakeAuth{name: "b", challenge: "ApiKey", res: okResult("b")}
		o := run(t, RouteConfig{Providers: []Authenticator{p1, p2}}, httptest.NewRequest(http.MethodGet, "/", nil))
		if !o.called {
			t.Fatal("next not called")
		}
		if len(o.failures) != 0 {
			t.Errorf("failures = %v, want none", o.failures)
		}
	})

	t.Run("all fail reports first reason", func(t *testing.T) {
		p1 := &fakeAuth{name: "a", challenge: "Bearer", err: Fail(ReasonExpired, nil)}
		p2 := &fakeAuth{name: "b", challenge: "ApiKey", err: Fail(ReasonSignature, nil)}
		o := run(t, RouteConfig{Providers: []Authenticator{p1, p2}}, httptest.NewRequest(http.MethodGet, "/", nil))
		if o.called {
			t.Error("next called")
		}
		if o.rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", o.rec.Code)
		}
		if len(o.failures) != 1 || o.failures[0] != ReasonExpired {
			t.Errorf("failures = %v, want [expired]", o.failures)
		}
	})

	t.Run("none present is missing", func(t *testing.T) {
		p1 := &fakeAuth{name: "a", challenge: "Bearer", err: ErrNoCredentials}
		p2 := &fakeAuth{name: "b", challenge: "ApiKey", err: ErrNoCredentials}
		o := run(t, RouteConfig{Providers: []Authenticator{p1, p2}}, httptest.NewRequest(http.MethodGet, "/", nil))
		if o.rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", o.rec.Code)
		}
		if len(o.failures) != 1 || o.failures[0] != ReasonMissing {
			t.Errorf("failures = %v, want [missing]", o.failures)
		}
	})

	t.Run("allow anonymous with nothing presented", func(t *testing.T) {
		p1 := &fakeAuth{name: "a", challenge: "Bearer", err: ErrNoCredentials}
		o := run(t, RouteConfig{Providers: []Authenticator{p1}, AllowAnonymous: true}, httptest.NewRequest(http.MethodGet, "/", nil))
		if !o.called {
			t.Fatal("next not called")
		}
		if _, ok := reqctx.IdentityFrom(o.req.Context()); ok {
			t.Error("identity present for anonymous request")
		}
		if len(o.failures) != 0 {
			t.Errorf("failures = %v, want none", o.failures)
		}
	})

	t.Run("allow anonymous with invalid credential", func(t *testing.T) {
		p1 := &fakeAuth{name: "a", challenge: "Bearer", err: Fail(ReasonSignature, nil)}
		o := run(t, RouteConfig{Providers: []Authenticator{p1}, AllowAnonymous: true}, httptest.NewRequest(http.MethodGet, "/", nil))
		if o.called {
			t.Error("next called")
		}
		if o.rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", o.rec.Code)
		}
	})
}

func TestMiddleware401Shape(t *testing.T) {
	tests := []struct {
		name      string
		providers []Authenticator
		challenge string
	}{
		{
			name:      "bearer first",
			providers: []Authenticator{&fakeAuth{name: "jwt", challenge: "Bearer", err: ErrNoCredentials}, &fakeAuth{name: "key", challenge: "ApiKey", err: ErrNoCredentials}},
			challenge: "Bearer",
		},
		{
			name:      "apikey first",
			providers: []Authenticator{&fakeAuth{name: "key", challenge: "ApiKey", err: ErrNoCredentials}, &fakeAuth{name: "jwt", challenge: "Bearer", err: ErrNoCredentials}},
			challenge: "ApiKey",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r = r.WithContext(reqctx.WithRequestID(r.Context(), "req-123"))
			o := run(t, RouteConfig{Providers: tc.providers}, r)

			if o.called {
				t.Error("backend called")
			}
			if got := o.rec.Header().Get("WWW-Authenticate"); got != tc.challenge {
				t.Errorf("WWW-Authenticate = %q, want %q", got, tc.challenge)
			}
			var body struct {
				Error struct {
					Status    int    `json:"status"`
					Code      string `json:"code"`
					Message   string `json:"message"`
					RequestID string `json:"request_id"`
				} `json:"error"`
			}
			if err := json.Unmarshal(o.rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if body.Error.Code != "unauthorized" || body.Error.Message != "unauthorized" {
				t.Errorf("body = %+v", body.Error)
			}
			if body.Error.RequestID != "req-123" {
				t.Errorf("request_id = %q, want req-123", body.Error.RequestID)
			}
		})
	}
}

func TestMiddleware403(t *testing.T) {
	tests := []struct {
		name    string
		require Requirements
		res     Result
		wantOK  bool
	}{
		{
			name:    "missing scope",
			require: Requirements{Scopes: []string{"admin"}},
			res:     Result{Scopes: []string{"read"}},
		},
		{
			name:    "missing role",
			require: Requirements{Roles: []string{"ops"}},
			res:     Result{Roles: []string{"dev"}},
		},
		{
			name:    "wrong claim",
			require: Requirements{Claims: map[string]any{"tenant": "a"}},
			res:     Result{Values: map[string]any{"tenant": "b"}},
		},
		{
			name:    "all satisfied",
			require: Requirements{Scopes: []string{"read"}, Roles: []string{"ops"}, Claims: map[string]any{"tenant": "a"}},
			res:     Result{Scopes: []string{"read"}, Roles: []string{"ops"}, Values: map[string]any{"tenant": "a"}},
			wantOK:  true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &fakeAuth{name: "a", challenge: "Bearer", res: tc.res}
			o := run(t, RouteConfig{Providers: []Authenticator{p}, Require: tc.require}, httptest.NewRequest(http.MethodGet, "/", nil))
			if tc.wantOK {
				if !o.called {
					t.Fatalf("next not called, status %d", o.rec.Code)
				}
				return
			}
			if o.called {
				t.Error("next called")
			}
			if o.rec.Code != http.StatusForbidden {
				t.Errorf("status = %d, want 403", o.rec.Code)
			}
			if len(o.failures) != 1 || o.failures[0] != ReasonForbidden {
				t.Errorf("failures = %v, want [forbidden]", o.failures)
			}
		})
	}
}

func TestRequirementsSatisfied(t *testing.T) {
	tests := []struct {
		name string
		q    Requirements
		res  Result
		want bool
	}{
		{"scope from string", Requirements{Scopes: []string{"write"}}, Result{Scopes: stringList("read write")}, true},
		{"scope from array", Requirements{Scopes: []string{"write"}}, Result{Scopes: stringList([]any{"read", "write"})}, true},
		{"scope absent from array", Requirements{Scopes: []string{"write"}}, Result{Scopes: stringList([]any{"read"})}, false},
		{"string equal", Requirements{Claims: map[string]any{"sub": "abc"}}, Result{Values: map[string]any{"sub": "abc"}}, true},
		{"string mismatch", Requirements{Claims: map[string]any{"sub": "abd"}}, Result{Values: map[string]any{"sub": "abc"}}, false},
		{"json number equals int", Requirements{Claims: map[string]any{"n": 5}}, Result{Values: map[string]any{"n": float64(5)}}, true},
		{"json number mismatch", Requirements{Claims: map[string]any{"n": 6}}, Result{Values: map[string]any{"n": float64(5)}}, false},
		{"bool equal", Requirements{Claims: map[string]any{"b": true}}, Result{Values: map[string]any{"b": true}}, true},
		{"bool mismatch", Requirements{Claims: map[string]any{"b": true}}, Result{Values: map[string]any{"b": false}}, false},
		{"array contains value", Requirements{Claims: map[string]any{"aud": "y"}}, Result{Values: map[string]any{"aud": []any{"x", "y"}}}, true},
		{"array lacks value", Requirements{Claims: map[string]any{"aud": "z"}}, Result{Values: map[string]any{"aud": []any{"x", "y"}}}, false},
		{"metadata string vs int", Requirements{Claims: map[string]any{"count": 5}}, Result{Values: map[string]any{"count": "5"}}, true},
		{"metadata string vs int mismatch", Requirements{Claims: map[string]any{"count": 6}}, Result{Values: map[string]any{"count": "5"}}, false},
		{"metadata string vs bool", Requirements{Claims: map[string]any{"flag": true}}, Result{Values: map[string]any{"flag": "true"}}, true},
		{"missing claim", Requirements{Claims: map[string]any{"sub": "x"}}, Result{Values: map[string]any{}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.q.Satisfied(tc.res); got != tc.want {
				t.Errorf("Satisfied = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMiddlewareCredentialRemoval(t *testing.T) {
	t.Run("default removes winner credentials", func(t *testing.T) {
		p := &fakeAuth{name: "k", challenge: "ApiKey", header: "X-API-Key", query: "api_key", res: okResult("k")}
		r := httptest.NewRequest(http.MethodGet, "/x?b=2&api_key=K&a=%2F", nil)
		r.Header.Set("X-API-Key", "K")
		o := run(t, RouteConfig{Providers: []Authenticator{p}}, r)
		if !o.called {
			t.Fatal("next not called")
		}
		if got := o.req.Header.Get("X-API-Key"); got != "" {
			t.Errorf("header kept: %q", got)
		}
		if got := o.req.URL.RawQuery; got != "b=2&a=%2F" {
			t.Errorf("RawQuery = %q, want b=2&a=%%2F", got)
		}
	})

	t.Run("forward flag keeps winner credentials", func(t *testing.T) {
		p := &fakeAuth{name: "k", challenge: "ApiKey", header: "X-API-Key", query: "api_key", forward: true, res: okResult("k")}
		r := httptest.NewRequest(http.MethodGet, "/x?b=2&api_key=K&a=%2F", nil)
		r.Header.Set("X-API-Key", "K")
		o := run(t, RouteConfig{Providers: []Authenticator{p}}, r)
		if !o.called {
			t.Fatal("next not called")
		}
		if got := o.req.Header.Get("X-API-Key"); got != "K" {
			t.Errorf("header = %q, want K", got)
		}
		if got := o.req.URL.RawQuery; got != "b=2&api_key=K&a=%2F" {
			t.Errorf("RawQuery = %q", got)
		}
	})

	t.Run("non-winning credential locations are removed", func(t *testing.T) {
		winner := &fakeAuth{name: "k", challenge: "ApiKey", header: "X-API-Key", forward: true, res: okResult("k")}
		other := &fakeAuth{name: "j", challenge: "Bearer", header: "X-Other", query: "token", err: ErrNoCredentials}
		r := httptest.NewRequest(http.MethodGet, "/x?token=T&x=1", nil)
		r.Header.Set("X-API-Key", "K")
		r.Header.Set("X-Other", "T")
		o := run(t, RouteConfig{Providers: []Authenticator{other, winner}}, r)
		if !o.called {
			t.Fatal("next not called")
		}
		if got := o.req.Header.Get("X-API-Key"); got != "K" {
			t.Errorf("winner header = %q, want K", got)
		}
		if got := o.req.Header.Get("X-Other"); got != "" {
			t.Errorf("other header kept: %q", got)
		}
		if got := o.req.URL.RawQuery; got != "x=1" {
			t.Errorf("RawQuery = %q, want x=1", got)
		}
	})
}

func TestMiddlewareForwardAndIdentity(t *testing.T) {
	id := reqctx.Identity{Provider: "k", Consumer: "svc-a"}
	p := &fakeAuth{name: "k", challenge: "ApiKey", res: Result{
		Identity: id,
		Forward:  map[string]string{"X-Consumer": "svc-a"},
	}}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("X-Consumer", "spoofed")
	o := run(t, RouteConfig{Providers: []Authenticator{p}}, r)
	if !o.called {
		t.Fatal("next not called")
	}
	got, ok := reqctx.IdentityFrom(o.req.Context())
	if !ok || got.Consumer != "svc-a" || got.Provider != "k" {
		t.Errorf("identity = %+v, ok = %v", got, ok)
	}
	if v := o.req.Header.Get("X-Consumer"); v != "svc-a" {
		t.Errorf("X-Consumer = %q, want svc-a", v)
	}
}

func TestStrip(t *testing.T) {
	t.Run("removes listed headers case-insensitively", func(t *testing.T) {
		h := Strip([]string{"x-internal", "X-Secret"})
		var got *http.Request
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got = r })
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("X-Internal", "1")
		r.Header.Set("X-Secret", "2")
		r.Header.Set("X-Keep", "3")
		h(next).ServeHTTP(httptest.NewRecorder(), r)
		if got.Header.Get("X-Internal") != "" || got.Header.Get("X-Secret") != "" {
			t.Errorf("listed headers kept: %v", got.Header)
		}
		if got.Header.Get("X-Keep") != "3" {
			t.Errorf("X-Keep = %q, want 3", got.Header.Get("X-Keep"))
		}
	})

	t.Run("empty list is pass-through", func(t *testing.T) {
		h := Strip(nil)
		var got *http.Request
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got = r })
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("X-Internal", "1")
		h(next).ServeHTTP(httptest.NewRecorder(), r)
		if got.Header.Get("X-Internal") != "1" {
			t.Errorf("header removed by empty Strip")
		}
	})
}

func TestForwardValue(t *testing.T) {
	tests := []struct {
		name   string
		in     any
		want   string
		wantOK bool
	}{
		{"string", "abc", "abc", true},
		{"int", 3, "3", true},
		{"float", 2.5, "2.5", true},
		{"bool", true, "true", true},
		{"array", []any{"a", 1}, `["a",1]`, true},
		{"map", map[string]any{"k": "v"}, `{"k":"v"}`, true},
		{"nil", nil, "", false},
		{"control character", "a\nb", "", false},
		{"del character", "a\x7fb", "", false},
		{"empty string", "", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := forwardValue(tc.in)
			if got != tc.want || ok != tc.wantOK {
				t.Errorf("forwardValue = (%q, %v), want (%q, %v)", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}
