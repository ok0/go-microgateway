package router

import (
	"strings"
	"testing"
)

func TestCompile(t *testing.T) {
	valid := []struct {
		name     string
		path     string
		typ      PathType
		wantSegs []Segment
	}{
		{name: "root prefix", path: "/", typ: PathPrefix, wantSegs: nil},
		{name: "exact literal", path: "/api/users", typ: PathExact, wantSegs: []Segment{
			{Kind: SegLiteral, Value: "api"},
			{Kind: SegLiteral, Value: "users"},
		}},
		{name: "prefix trailing slash", path: "/api/", typ: PathPrefix, wantSegs: []Segment{
			{Kind: SegLiteral, Value: "api"},
		}},
		{name: "colon literal in exact", path: "/v1/items:batch", typ: PathExact, wantSegs: []Segment{
			{Kind: SegLiteral, Value: "v1"},
			{Kind: SegLiteral, Value: "items:batch"},
		}},
		{name: "param", path: "/users/:id", typ: PathPattern, wantSegs: []Segment{
			{Kind: SegLiteral, Value: "users"},
			{Kind: SegParam, Value: "id"},
		}},
		{name: "glob", path: "/files/*rest", typ: PathPattern, wantSegs: []Segment{
			{Kind: SegLiteral, Value: "files"},
			{Kind: SegGlob, Value: "rest"},
		}},
		{name: "param and glob", path: "/a/:x/b/*y", typ: PathPattern, wantSegs: []Segment{
			{Kind: SegLiteral, Value: "a"},
			{Kind: SegParam, Value: "x"},
			{Kind: SegLiteral, Value: "b"},
			{Kind: SegGlob, Value: "y"},
		}},
	}
	for _, tc := range valid {
		t.Run("valid/"+tc.name, func(t *testing.T) {
			p, err := Compile(tc.path, tc.typ)
			if err != nil {
				t.Fatalf("Compile(%q, %q) error: %v", tc.path, tc.typ, err)
			}
			if p.Raw != tc.path || p.Type != tc.typ {
				t.Errorf("Raw/Type = %q/%q, want %q/%q", p.Raw, p.Type, tc.path, tc.typ)
			}
			if len(p.Segments) != len(tc.wantSegs) {
				t.Fatalf("got %d segments, want %d: %+v", len(p.Segments), len(tc.wantSegs), p.Segments)
			}
			for i, want := range tc.wantSegs {
				if p.Segments[i] != want {
					t.Errorf("segment %d = %+v, want %+v", i, p.Segments[i], want)
				}
			}
		})
	}

	invalid := []struct {
		name    string
		path    string
		typ     PathType
		wantSub string
	}{
		{name: "no leading slash", path: "api", typ: PathExact, wantSub: `path must start with "/"`},
		{name: "question mark", path: "/a?b", typ: PathPrefix, wantSub: "invalid character"},
		{name: "empty segment", path: "/a//b", typ: PathPrefix, wantSub: "empty path segment"},
		{name: "dot segment", path: "/a/../b", typ: PathExact, wantSub: "dot segment not allowed"},
		{name: "pattern trailing slash", path: "/a/", typ: PathPattern, wantSub: `pattern must not end with "/"`},
		{name: "glob not last", path: "/a/*r/b", typ: PathPattern, wantSub: "glob segment must be last"},
		{name: "param starts with digit", path: "/:1x", typ: PathPattern, wantSub: "invalid parameter name"},
		{name: "duplicate param", path: "/:id/:id", typ: PathPattern, wantSub: "duplicate parameter name"},
		{name: "colon inside literal", path: "/a:b", typ: PathPattern, wantSub: "must not contain"},
		{name: "unknown type", path: "/a", typ: "regex", wantSub: "unknown path type"},
		{name: "empty glob name", path: "/*", typ: PathPattern, wantSub: "invalid parameter name"},
	}
	for _, tc := range invalid {
		t.Run("invalid/"+tc.name, func(t *testing.T) {
			_, err := Compile(tc.path, tc.typ)
			if err == nil {
				t.Fatalf("Compile(%q, %q) succeeded, want error containing %q", tc.path, tc.typ, tc.wantSub)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantSub)
			}
		})
	}
}
