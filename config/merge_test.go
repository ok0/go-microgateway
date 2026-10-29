package config

import (
	"reflect"
	"testing"
)

func fe(path string) FieldError {
	return FieldError{Path: path, Msg: "m:" + path}
}

func rep(paths ...string) []FieldError {
	var out []FieldError
	for _, p := range paths {
		out = append(out, FieldError{Path: p, Line: 1, Msg: "m:" + p})
	}
	return out
}

func checks(paths ...string) []FieldError {
	var out []FieldError
	for _, p := range paths {
		out = append(out, fe(p))
	}
	return out
}

func TestMergeCheck(t *testing.T) {
	tests := []struct {
		name     string
		reported []FieldError
		check    []FieldError
		want     []FieldError
	}{
		{
			name:     "equal path dropped",
			reported: rep("a.b"),
			check:    checks("a.b"),
			want:     rep("a.b"),
		},
		{
			name:     "child via dot dropped",
			reported: rep("routes[0].upstream"),
			check:    checks("routes[0].upstream.x"),
			want:     rep("routes[0].upstream"),
		},
		{
			name:     "child via bracket dropped",
			reported: rep("middlewares"),
			check:    checks("middlewares[1].type"),
			want:     rep("middlewares"),
		},
		{
			name:     "sibling kept",
			reported: rep("routes[0].upstream"),
			check:    checks("routes[0].name"),
			want:     append(rep("routes[0].upstream"), checks("routes[0].name")...),
		},
		{
			name:     "string prefix not segment index",
			reported: rep("middlewares[1]"),
			check:    checks("middlewares[10].type"),
			want:     append(rep("middlewares[1]"), checks("middlewares[10].type")...),
		},
		{
			name:     "string prefix not segment name",
			reported: rep("routes[0].up"),
			check:    checks("routes[0].upstream"),
			want:     append(rep("routes[0].up"), checks("routes[0].upstream")...),
		},
		{
			name:     "parent of reported path kept",
			reported: rep("a.b.c"),
			check:    checks("a.b"),
			want:     append(rep("a.b.c"), checks("a.b")...),
		},
		{
			name:     "pathless check kept",
			reported: rep("a"),
			check:    []FieldError{{Path: "", Msg: "doc"}},
			want:     append(rep("a"), FieldError{Path: "", Msg: "doc"}),
		},
		{
			name:     "pathless reported drops nothing",
			reported: []FieldError{{Path: "", Line: 1, Msg: "x"}},
			check:    checks("a"),
			want:     append([]FieldError{{Path: "", Line: 1, Msg: "x"}}, checks("a")...),
		},
		{
			name:     "check paths are not added to reported",
			reported: nil,
			check:    checks("a", "a"),
			want:     checks("a", "a"),
		},
		{
			name: "empty nil nil",
			want: nil,
		},
		{
			name:  "empty nil reported with check",
			check: checks("a"),
			want:  checks("a"),
		},
		{
			name:     "empty reported nil check",
			reported: rep("a"),
			want:     rep("a"),
		},
		{
			name:     "order kept",
			reported: rep("z", "a"),
			check:    checks("c", "z.x", "b"),
			want:     append(rep("z", "a"), checks("c", "b")...),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MergeCheck(tc.reported, tc.check)
			if tc.want == nil {
				if len(got) != 0 {
					t.Fatalf("MergeCheck = %v, want empty", got)
				}
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("MergeCheck = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMergeCheckDoesNotAliasReported(t *testing.T) {
	backing := make([]FieldError, 2, 4)
	backing[0] = FieldError{Path: "a", Line: 1, Msg: "m:a"}
	backing[1] = FieldError{Path: "sentinel", Line: 9, Msg: "keep"}
	got := MergeCheck(backing[:1], checks("b"))
	if len(got) != 2 {
		t.Fatalf("MergeCheck length = %d, want 2", len(got))
	}
	if backing[1] != (FieldError{Path: "sentinel", Line: 9, Msg: "keep"}) {
		t.Fatalf("reported backing entry changed to %v", backing[1])
	}
}
