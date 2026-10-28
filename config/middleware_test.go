package config

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/ok0/lychgate/internal/leakcheck"
)

func TestBuiltinParamsDefaults(t *testing.T) {
	v, err := Middleware{Name: "c", Type: "cors", Params: NewSecretParams(map[string]any{"allowed_origins": []any{"https://a.example"}})}.BuiltinParams()
	if err != nil {
		t.Fatal(err)
	}
	cp, ok := v.(*CORSParams)
	if !ok {
		t.Fatalf("got %T, want *CORSParams", v)
	}
	if want := []string{"GET", "HEAD", "POST"}; !reflect.DeepEqual(cp.AllowedMethods, want) {
		t.Errorf("AllowedMethods = %v, want %v", cp.AllowedMethods, want)
	}
	if want := []string{"https://a.example"}; !reflect.DeepEqual(cp.AllowedOrigins, want) {
		t.Errorf("AllowedOrigins = %v, want %v", cp.AllowedOrigins, want)
	}

	v, err = Middleware{Name: "h", Type: "headers", Params: NewSecretParams(map[string]any{
		"request": map[string]any{"set": map[string]any{"X-Env": "prod"}},
	})}.BuiltinParams()
	if err != nil {
		t.Fatal(err)
	}
	hp, ok := v.(*HeadersParams)
	if !ok {
		t.Fatalf("got %T, want *HeadersParams", v)
	}
	if got := hp.Request.Set["X-Env"].Reveal(); got != "prod" {
		t.Errorf("Request.Set[X-Env] = %q, want prod", got)
	}

	v, err = Middleware{Name: "l", Type: "request_size_limit", Params: NewSecretParams(map[string]any{"max_bytes": 1024})}.BuiltinParams()
	if err != nil {
		t.Fatal(err)
	}
	lp, ok := v.(*RequestSizeLimitParams)
	if !ok {
		t.Fatalf("got %T, want *RequestSizeLimitParams", v)
	}
	if lp.MaxBytes != 1024 {
		t.Errorf("MaxBytes = %d, want 1024", lp.MaxBytes)
	}
}

func TestBuiltinParamsErrors(t *testing.T) {
	cases := []struct {
		name       string
		typ        string
		params     map[string]any
		wantPath   string
		wantSubstr string
	}{
		{"cors unknown field", "cors", map[string]any{"bogus": 1}, "bogus", "unknown field"},
		{"cors wrong type", "cors", map[string]any{"allowed_origins": "x"}, "allowed_origins", "cannot unmarshal"},
		{"cors origin without scheme", "cors", map[string]any{"allowed_origins": []any{"app.example.com"}}, "allowed_origins[0]", "must be"},
		{"cors wildcard with credentials", "cors", map[string]any{"allowed_origins": []any{"*"}, "allow_credentials": true}, "allow_credentials", "cannot be true"},
		{"cors negative max_age", "cors", map[string]any{"max_age": -1}, "max_age", "must not be negative"},
		{"cors lower-case method", "cors", map[string]any{"allowed_methods": []any{"get"}}, "allowed_methods[0]", "upper-case"},
		{"headers unknown field", "headers", map[string]any{"request": map[string]any{"bogus": 1}}, "request.bogus", "unknown field"},
		{"headers invalid set name", "headers", map[string]any{"response": map[string]any{"set": map[string]any{"Bad Header": "x"}}}, "response.set.Bad Header", "invalid header name"},
		{"request_size_limit missing", "request_size_limit", map[string]any{}, "max_bytes", "must be positive"},
		{"request_size_limit wrong type", "request_size_limit", map[string]any{"max_bytes": "big"}, "max_bytes", "cannot unmarshal"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Middleware{Name: "m", Type: tc.typ, Params: NewSecretParams(tc.params)}.BuiltinParams()
			var pe *ParamsError
			if !errors.As(err, &pe) {
				t.Fatalf("got %T (%v), want *ParamsError", err, err)
			}
			found := false
			for _, fe := range pe.Errors {
				if fe.Path == tc.wantPath && strings.Contains(fe.Msg, tc.wantSubstr) {
					found = true
				}
			}
			if !found {
				t.Errorf("no error at %s containing %q; got %v", tc.wantPath, tc.wantSubstr, err)
			}
		})
	}
}

func TestDecodeParamsAbsent(t *testing.T) {
	for _, params := range []map[string]any{nil, {}} {
		v := &RequestSizeLimitParams{MaxBytes: 7}
		if err := DecodeParams(params, v); err != nil {
			t.Fatalf("DecodeParams(%v): %v", params, err)
		}
		if v.MaxBytes != 7 {
			t.Errorf("value changed to %d for %v", v.MaxBytes, params)
		}
	}
}

func TestBuiltinParamsNotBuiltin(t *testing.T) {
	if _, err := (Middleware{Name: "x", Type: "my_custom"}).BuiltinParams(); err == nil {
		t.Fatal("want error for a type that is not built in")
	}
}

func TestBuiltinParamsMergedErrors(t *testing.T) {
	cases := []struct {
		name        string
		typ         string
		params      map[string]any
		wantPaths   []string
		wantLastSub string
	}{
		{"cors unknown field and semantic", "cors", map[string]any{"allowed_origins": []any{"*"}, "allow_credentials": true, "bogus": 1}, []string{"bogus", "allow_credentials"}, "cannot be true"},
		{"cors semantic and decode", "cors", map[string]any{"allowed_origins": []any{"app.example.com"}, "max_age": "x"}, []string{"max_age", "allowed_origins[0]"}, ""},
		{"headers decode and semantic", "headers", map[string]any{"request": map[string]any{"bogus": 1, "set": map[string]any{"Bad Header": "x"}}}, []string{"request.bogus", "request.set.Bad Header"}, "invalid header name"},
		{"request_size_limit wrong type only", "request_size_limit", map[string]any{"max_bytes": "big"}, []string{"max_bytes"}, "cannot unmarshal"},
		{"request_size_limit unknown field and semantic", "request_size_limit", map[string]any{"bogus": 1}, []string{"bogus", "max_bytes"}, "must be positive"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, err := Middleware{Name: "m", Type: tc.typ, Params: NewSecretParams(tc.params)}.BuiltinParams()
			if v != nil {
				t.Errorf("value = %v, want nil", v)
			}
			var pe *ParamsError
			if !errors.As(err, &pe) {
				t.Fatalf("got %T (%v), want *ParamsError", err, err)
			}
			var paths []string
			for _, fe := range pe.Errors {
				paths = append(paths, fe.Path)
			}
			if !slices.Equal(paths, tc.wantPaths) {
				t.Fatalf("paths = %v, want %v", paths, tc.wantPaths)
			}
			if tc.wantLastSub != "" {
				last := pe.Errors[len(pe.Errors)-1]
				if !strings.Contains(last.Msg, tc.wantLastSub) {
					t.Errorf("last message = %q, want it to contain %q", last.Msg, tc.wantLastSub)
				}
			}
		})
	}
}

func TestHeadersParamsSecretValues(t *testing.T) {
	v, err := Middleware{Name: "h", Type: "headers", Params: NewSecretParams(map[string]any{
		"request": map[string]any{"set": map[string]any{"Authorization": "Bearer hv-LEAK-1"}},
	})}.BuiltinParams()
	if err != nil {
		t.Fatal(err)
	}
	hp := v.(*HeadersParams)
	if got := hp.Request.Set["Authorization"].Reveal(); got != "Bearer hv-LEAK-1" {
		t.Errorf("Request.Set[Authorization].Reveal() = %q, want the configured value", got)
	}

	for _, tc := range []struct {
		name   string
		value  any
		secret string
	}{
		{"list value", []any{"hv-LEAK-2"}, "hv-LEAK-2"},
		{"mapping value", map[string]any{"a": "hv-LEAK-3"}, "hv-LEAK-3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Middleware{Name: "h", Type: "headers", Params: NewSecretParams(map[string]any{
				"request": map[string]any{"set": map[string]any{"Authorization": tc.value}},
			})}.BuiltinParams()
			var pe *ParamsError
			if !errors.As(err, &pe) {
				t.Fatalf("got %T (%v), want *ParamsError", err, err)
			}
			want := []FieldError{{Path: "request.set.Authorization", Msg: "must be a string"}}
			if len(pe.Errors) != len(want) || pe.Errors[0].Path != want[0].Path || pe.Errors[0].Msg != want[0].Msg {
				t.Errorf("errors = %v, want %v", pe.Errors, want)
			}
			for _, form := range leakcheck.Forms(tc.secret) {
				if strings.Contains(err.Error(), form) {
					t.Errorf("error text contains secret form %q: %v", form, err)
				}
			}
		})
	}
}
