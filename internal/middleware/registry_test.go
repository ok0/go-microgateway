package middleware

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/config"
)

func TestRegisterErrors(t *testing.T) {
	r := NewRegistry()
	noop := func(config.Middleware) (Middleware, error) {
		return func(next http.Handler) http.Handler { return next }, nil
	}
	for _, name := range []string{"cors", "headers", "request_size_limit"} {
		err := r.Register(name, noop)
		if err == nil || !strings.Contains(err.Error(), "built in") {
			t.Errorf("Register(%q) = %v, want built-in error", name, err)
		}
	}
	if err := r.Register("", noop); err == nil {
		t.Error("Register with empty name succeeded")
	}
	if err := r.Register("nil_factory", nil); err == nil {
		t.Error("Register with nil factory succeeded")
	}
	if err := r.Register("x", noop); err != nil {
		t.Fatalf("Register(x): %v", err)
	}
	err := r.Register("x", noop)
	if err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Errorf("second Register(x) = %v, want already registered error", err)
	}
}

func TestBuild(t *testing.T) {
	t.Run("builtin", func(t *testing.T) {
		r := NewRegistry()
		def := config.Middleware{Name: "h", Type: "headers", Params: config.NewSecretParams(map[string]any{
			"response": map[string]any{"set": map[string]any{"X-A": "1"}},
		})}
		mw, err := r.Build(def)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		if mw == nil {
			t.Fatal("Build returned nil middleware")
		}
	})

	t.Run("registered", func(t *testing.T) {
		r := NewRegistry()
		var got config.Middleware
		var called bool
		mw := Middleware(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				called = true
				next.ServeHTTP(w, req)
			})
		})
		if err := r.Register("custom", func(def config.Middleware) (Middleware, error) {
			got = def
			return mw, nil
		}); err != nil {
			t.Fatal(err)
		}
		def := config.Middleware{Name: "n", Type: "custom", Params: config.NewSecretParams(map[string]any{"k": "v"})}
		out, err := r.Build(def)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		if got.Name != def.Name || got.Type != def.Type || !reflect.DeepEqual(got.Params, def.Params) {
			t.Errorf("factory received %+v, want %+v", got, def)
		}
		out(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		if !called {
			t.Error("returned middleware was not the registered one")
		}
	})

	t.Run("factory error", func(t *testing.T) {
		r := NewRegistry()
		errBoom := errors.New("boom")
		if err := r.Register("failing", func(config.Middleware) (Middleware, error) {
			return nil, errBoom
		}); err != nil {
			t.Fatal(err)
		}
		_, err := r.Build(config.Middleware{Name: "f", Type: "failing"})
		if !errors.Is(err, errBoom) {
			t.Errorf("Build error = %v, want %v", err, errBoom)
		}
	})

	t.Run("nil middleware", func(t *testing.T) {
		r := NewRegistry()
		if err := r.Register("nilmw", func(config.Middleware) (Middleware, error) {
			return nil, nil
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Build(config.Middleware{Name: "z", Type: "nilmw"}); err == nil {
			t.Error("Build with nil middleware succeeded")
		}
	})

	t.Run("unknown", func(t *testing.T) {
		r := NewRegistry()
		if r.Known("nope") {
			t.Error("Known(nope) = true")
		}
		_, err := r.Build(config.Middleware{Name: "u", Type: "nope"})
		if err == nil || !strings.Contains(err.Error(), "unknown middleware type") {
			t.Errorf("Build error = %v, want unknown middleware type", err)
		}
	})
}
