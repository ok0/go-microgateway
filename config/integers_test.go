package config

import (
	"errors"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func integerSchemaPaths(t reflect.Type, path string, out *[]string) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == reflect.TypeOf(Duration(0)) || t == reflect.TypeOf(time.Duration(0)) {
		return
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		*out = append(*out, path)
	case reflect.Slice, reflect.Array:
		integerSchemaPaths(t.Elem(), path+"[]", out)
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.PkgPath != "" {
				continue
			}
			tag := f.Tag.Get("yaml")
			if tag == "-" {
				continue
			}
			name, _, _ := strings.Cut(tag, ",")
			if name == "" {
				name = strings.ToLower(f.Name)
			}
			p := name
			if path != "" {
				p = path + "." + name
			}
			integerSchemaPaths(f.Type, p, out)
		}
	}
}

func TestIntegerFieldsAudited(t *testing.T) {
	var got []string
	integerSchemaPaths(reflect.TypeOf(Config{}), "", &got)
	slices.Sort(got)
	want := []string{
		"auth.jwt[].max_token_bytes",
		"listeners[].max_header_bytes",
		"routes[].priority",
		"routes[].rate_limit.burst",
		"routes[].rate_limit.limit",
		"routes[].rate_limit.max_keys",
		"routes[].rate_limit.rate",
		"routes[].retry.attempts",
		"routes[].retry.max_body_bytes",
		"routes[].retry.on_statuses[]",
		"upstreams[].active_health.healthy_threshold",
		"upstreams[].active_health.unhealthy_threshold",
		"upstreams[].circuit_breaker.buckets",
		"upstreams[].circuit_breaker.consecutive_failures",
		"upstreams[].circuit_breaker.half_open_probes",
		"upstreams[].circuit_breaker.min_requests",
		"upstreams[].circuit_breaker.success_threshold",
		"upstreams[].dns.port",
		"upstreams[].hosts[].weight",
		"upstreams[].load_balancer.virtual_nodes",
		"upstreams[].passive_health.consecutive_failures",
		"upstreams[].passive_health.max_ejection_percent",
		"upstreams[].pool.max_conns_per_host",
		"upstreams[].pool.max_idle_per_host",
		"version",
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("integer schema paths mismatch\n got: %q\nwant: %q", got, want)
	}
}

func navigateNode(t *testing.T, n *yaml.Node, path string) *yaml.Node {
	t.Helper()
	if n.Kind == yaml.DocumentNode {
		n = n.Content[0]
	}
	for _, seg := range strings.Split(path, ".") {
		key, idx := seg, -1
		if i := strings.Index(seg, "["); i >= 0 {
			key = seg[:i]
			v, err := strconv.Atoi(seg[i+1 : len(seg)-1])
			if err != nil {
				t.Fatalf("bad index in %q: %v", path, err)
			}
			idx = v
		}
		found := false
		for i := 0; i+1 < len(n.Content); i += 2 {
			if n.Content[i].Value == key {
				n = n.Content[i+1]
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("key %q missing on path %q", key, path)
		}
		if idx >= 0 {
			if n.Kind != yaml.SequenceNode || idx >= len(n.Content) {
				t.Fatalf("index %d missing on path %q", idx, path)
			}
			n = n.Content[idx]
		}
	}
	return n
}

func TestIntegerFieldsRejectFloat(t *testing.T) {
	raw, err := os.ReadFile("testdata/full.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Parse(raw, WithBaseDir("testdata")); err != nil {
		t.Fatalf("unmodified full.yaml: %v", err)
	}

	tests := []struct {
		path, typ string
	}{
		{"version", "int"},
		{"listeners[0].max_header_bytes", "int"},
		{"upstreams[0].hosts[0].weight", "int"},
		{"upstreams[1].dns.port", "int"},
		{"upstreams[0].load_balancer.virtual_nodes", "int"},
		{"upstreams[0].pool.max_idle_per_host", "int"},
		{"upstreams[0].pool.max_conns_per_host", "int"},
		{"upstreams[0].passive_health.consecutive_failures", "int"},
		{"upstreams[0].passive_health.max_ejection_percent", "int"},
		{"upstreams[0].active_health.healthy_threshold", "int"},
		{"upstreams[0].active_health.unhealthy_threshold", "int"},
		{"upstreams[0].circuit_breaker.buckets", "int"},
		{"upstreams[0].circuit_breaker.min_requests", "int"},
		{"upstreams[0].circuit_breaker.consecutive_failures", "int"},
		{"upstreams[0].circuit_breaker.half_open_probes", "int"},
		{"upstreams[0].circuit_breaker.success_threshold", "int"},
		{"auth.jwt[0].max_token_bytes", "int"},
		{"routes[0].priority", "int"},
		{"routes[0].rate_limit.rate", "int64"},
		{"routes[0].rate_limit.burst", "int64"},
		{"routes[0].rate_limit.max_keys", "int"},
		{"routes[1].rate_limit.limit", "int64"},
		{"routes[0].retry.attempts", "int"},
		{"routes[0].retry.on_statuses[0]", "int"},
		{"routes[0].retry.max_body_bytes", "int64"},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			var doc yaml.Node
			if err := yaml.Unmarshal(raw, &doc); err != nil {
				t.Fatal(err)
			}
			target := navigateNode(t, &doc, tc.path)
			target.Value = "2.5"
			target.Tag = ""
			target.Style = 0
			out, err := yaml.Marshal(&doc)
			if err != nil {
				t.Fatal(err)
			}
			var reparsed yaml.Node
			if err := yaml.Unmarshal(out, &reparsed); err != nil {
				t.Fatal(err)
			}
			wantLine := navigateNode(t, &reparsed, tc.path).Line

			_, err = Parse(out, WithBaseDir("testdata"))
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("error = %v, want *ValidationError", err)
			}
			if len(ve.Errors) != 1 {
				t.Fatalf("got %d errors, want 1: %v", len(ve.Errors), err)
			}
			fe := ve.Errors[0]
			if fe.Path != tc.path {
				t.Errorf("Path = %q, want %q", fe.Path, tc.path)
			}
			if fe.Line != wantLine {
				t.Errorf("Line = %d, want %d", fe.Line, wantLine)
			}
			if want := "cannot unmarshal !!float `2.5` into " + tc.typ; fe.Msg != want {
				t.Errorf("Msg = %q, want %q", fe.Msg, want)
			}
		})
	}
}

func TestParamsIntegersRejectFloat(t *testing.T) {
	cases := []struct {
		name string
		mw   Middleware
		want FieldError
	}{
		{
			name: "cors max_age",
			mw:   Middleware{Type: "cors", Params: map[string]any{"max_age": 2.5}},
			want: FieldError{Path: "max_age", Msg: "cannot unmarshal !!float `2.5` into int"},
		},
		{
			name: "request_size_limit max_bytes",
			mw:   Middleware{Type: "request_size_limit", Params: map[string]any{"max_bytes": 2.5}},
			want: FieldError{Path: "max_bytes", Msg: "cannot unmarshal !!float `2.5` into int64"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.mw.BuiltinParams()
			var pe *ParamsError
			if !errors.As(err, &pe) {
				t.Fatalf("error = %v, want *ParamsError", err)
			}
			found := false
			for _, fe := range pe.Errors {
				if fe.Path == tc.want.Path && fe.Msg == tc.want.Msg {
					found = true
				}
			}
			if !found {
				t.Errorf("errors %v do not contain %+v", pe.Errors, tc.want)
			}
		})
	}

	ok := []Middleware{
		{Type: "cors", Params: map[string]any{"max_age": 600}},
		{Type: "request_size_limit", Params: map[string]any{"max_bytes": 1024}},
	}
	for _, mw := range ok {
		if _, err := mw.BuiltinParams(); err != nil {
			t.Errorf("%s: unexpected error: %v", mw.Type, err)
		}
	}
}
