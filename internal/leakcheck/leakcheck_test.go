package leakcheck

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

type masked string

func (masked) String() string { return "***" }

func (masked) Format(f fmt.State, _ rune) { fmt.Fprint(f, "***") }

func (masked) MarshalJSON() ([]byte, error) { return []byte(`"***"`), nil }

type holder struct{ M masked }

type opaque struct{ b *struct{ v string } }

func (opaque) String() string { return "***" }

func (opaque) Format(f fmt.State, _ rune) { fmt.Fprint(f, "***") }

func (opaque) MarshalJSON() ([]byte, error) { return []byte(`"***"`), nil }

func (opaque) MarshalYAML() (any, error) { return "***", nil }

func stringLeaf(s string) func(reflect.Type) (reflect.Value, bool) {
	return func(t reflect.Type) (reflect.Value, bool) {
		if t.Kind() != reflect.String {
			return reflect.Value{}, false
		}
		v := reflect.New(t).Elem()
		v.SetString(s)
		return v, true
	}
}

func noLeaf(reflect.Type) (reflect.Value, bool) {
	return reflect.Value{}, false
}

func assertLeak(t *testing.T, leaks []Leak, rendering string) {
	t.Helper()
	for _, l := range leaks {
		if l.Rendering == rendering {
			return
		}
	}
	t.Errorf("no leak reported under rendering %q", rendering)
}

func wrappersFor(leaks []Leak, verb string) []string {
	var out []string
	for _, l := range leaks {
		w, v, ok := strings.Cut(l.Rendering, "|")
		if ok && v == verb {
			out = append(out, w)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func TestFindDetectsLeak(t *testing.T) {
	type plain struct{ P string }
	type outer struct{ P *plain }
	const secret = "raw-LEAK"

	allWrappers := []string{"any", "array", "field", "map", "ptr", "ptrfield", "ptrfieldptr", "ptrslice", "slice", "val"}
	plainVerbV := []string{"any", "array", "field", "map", "ptr", "slice", "val"}
	outerVerbS := []string{"any", "array", "field", "map", "ptr", "slice", "val"}

	plainVal := Fill(reflect.TypeFor[plain](), stringLeaf(secret)).Interface()
	plainLeaks := Find(plainVal, secret)
	if got := wrappersFor(plainLeaks, "%s"); !slices.Equal(got, allWrappers) {
		t.Errorf("plain %%s wrappers = %v, want %v", got, allWrappers)
	}
	if got := wrappersFor(plainLeaks, "%v"); !slices.Equal(got, plainVerbV) {
		t.Errorf("plain %%v wrappers = %v, want %v", got, plainVerbV)
	}

	outerVal := Fill(reflect.TypeFor[outer](), stringLeaf(secret)).Interface()
	outerLeaks := Find(outerVal, secret)
	if got := wrappersFor(outerLeaks, "%s"); !slices.Equal(got, outerVerbS) {
		t.Errorf("outer %%s wrappers = %v, want %v", got, outerVerbS)
	}
	if got := wrappersFor(outerLeaks, "%v"); !slices.Equal(got, nil) {
		t.Errorf("outer %%v wrappers = %v, want none", got)
	}
}

func TestFindStringKindSecretLeaks(t *testing.T) {
	leaks := Find(holder{M: masked("raw-LEAK")}, "raw-LEAK")
	assertLeak(t, leaks, "ptrfield|%s")
}

func TestFindOpaqueClean(t *testing.T) {
	leaks := Find(opaque{&struct{ v string }{"raw-LEAK"}}, "raw-LEAK")
	if len(leaks) != 0 {
		var labels []string
		for _, l := range leaks {
			labels = append(labels, l.Rendering)
		}
		t.Errorf("opaque type reported %d leaking renderings: %v", len(leaks), labels)
	}
}

func TestForms(t *testing.T) {
	got := Forms("a b/c")
	want := []string{
		"a b/c",
		"6120622f63",
		"6120622F63",
		"61 20 62 2f 63",
		"61 20 62 2F 63",
		"YSBiL2M=",
		"YSBiL2M",
		"a+b%2Fc",
		"a%20b%2Fc",
	}
	if !slices.Equal(got, want) {
		t.Errorf("Forms(%q) = %q, want %q", "a b/c", got, want)
	}
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestExportedTypes(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.go", "package p\ntype A struct{}\ntype b int\ntype (\n\tC = int\n\tD[T any] struct{ v T }\n)\nfunc E() {}\n")
	writeFile(t, dir, "a_test.go", "package p\ntype T struct{}\n")
	writeFile(t, dir, "notgo.txt", "type X int")
	got, err := ExportedTypes(dir)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"A", "C", "D"}; !slices.Equal(got, want) {
		t.Errorf("ExportedTypes = %q, want %q", got, want)
	}

	bad := t.TempDir()
	writeFile(t, bad, "bad.go", "package p\ntype (")
	if _, err := ExportedTypes(bad); err == nil {
		t.Errorf("ExportedTypes on a file with a syntax error returned no error")
	}
}

func TestHolds(t *testing.T) {
	type inner struct{ s string }
	type outer struct {
		p *inner
		m map[any]any
		b []byte
		a [1]any
	}
	const s = "h-LEAK-1"

	cases := []struct {
		name string
		v    any
	}{
		{"pointer field", outer{p: &inner{s}}},
		{"map value", outer{m: map[any]any{"k": inner{s}}}},
		{"map key", outer{m: map[any]any{s: 1}}},
		{"byte slice", outer{b: []byte("x" + s)}},
		{"array element", outer{a: [1]any{&inner{s}}}},
	}
	for _, tc := range cases {
		if !Holds(tc.v, s) {
			t.Errorf("Holds(%s) = false, want true", tc.name)
		}
	}
	if Holds(outer{p: &inner{"other"}}, s) {
		t.Errorf("Holds reported a secret in a value that does not contain it")
	}
	if Holds(nil, s) {
		t.Errorf("Holds(nil) = true, want false")
	}

	type node struct {
		next *node
		v    string
	}
	n := &node{v: "x"}
	n.next = n
	if Holds(n, s) {
		t.Errorf("Holds reported a secret in a cyclic value that does not contain it")
	}
}

func TestFillAndReaches(t *testing.T) {
	type target struct{}
	type inner struct{ S string }
	type filled struct {
		A *inner
		L []int
		M map[string]bool
		u int
	}
	type node struct{ Next *node }
	type viaMap struct{ M map[string]target }
	type viaUnexported struct{ u *target }
	type unrelated struct{ X int }

	v := Fill(reflect.TypeFor[filled](), noLeaf)
	if got := v.FieldByName("A").Elem().FieldByName("S").String(); got != "x" {
		t.Errorf("A.S = %q, want %q", got, "x")
	}
	if got := v.FieldByName("L").Len(); got != 1 {
		t.Errorf("len(L) = %d, want 1", got)
	}
	if got := v.FieldByName("M").Len(); got != 1 {
		t.Errorf("len(M) = %d, want 1", got)
	}
	if got := v.FieldByName("u").Int(); got != 0 {
		t.Errorf("unexported u = %d, want 0", got)
	}

	self := Fill(reflect.TypeFor[node](), noLeaf)
	if self.Field(0).IsNil() {
		t.Errorf("self-referential Next is nil, want a non-nil pointer")
	}

	targetType := reflect.TypeFor[target]()
	if !Reaches(reflect.TypeFor[viaMap](), targetType) {
		t.Errorf("Reaches did not find target behind a map elem")
	}
	if !Reaches(reflect.TypeFor[viaUnexported](), targetType) {
		t.Errorf("Reaches did not find target behind an unexported field")
	}
	if Reaches(reflect.TypeFor[unrelated](), targetType) {
		t.Errorf("Reaches reported target for an unrelated type")
	}
}
