package yamlx

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

type testConfig struct {
	A int `yaml:"a"`
	B struct {
		C string `yaml:"c"`
	} `yaml:"b"`
}

func TestDecodeStrictOK(t *testing.T) {
	var v testConfig
	if err := DecodeStrict([]byte("a: 1\nb:\n  c: x\n"), &v); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.A != 1 || v.B.C != "x" {
		t.Fatalf("decoded value = %+v", v)
	}
}

func TestDecodeStrictUnknownField(t *testing.T) {
	var v testConfig
	err := DecodeStrict([]byte("a: 1\nz: 2\n"), &v)
	var te *yaml.TypeError
	if !errors.As(err, &te) {
		t.Fatalf("expected *yaml.TypeError, got %v", err)
	}
	if len(te.Errors) != 1 {
		t.Fatalf("expected 1 entry, got %d: %v", len(te.Errors), te.Errors)
	}
	if !strings.Contains(te.Errors[0], "line 2") || !strings.Contains(te.Errors[0], "field z not found") {
		t.Fatalf("unexpected entry: %q", te.Errors[0])
	}
}

func TestDecodeStrictUnknownNested(t *testing.T) {
	var v testConfig
	err := DecodeStrict([]byte("b:\n  c: x\n  d: y\n"), &v)
	var te *yaml.TypeError
	if !errors.As(err, &te) {
		t.Fatalf("expected *yaml.TypeError, got %v", err)
	}
	found := false
	for _, e := range te.Errors {
		if strings.Contains(e, "line 3") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no entry contains line 3: %v", te.Errors)
	}
}

func TestDecodeStrictMultiDoc(t *testing.T) {
	var v testConfig
	err := DecodeStrict([]byte("a: 1\n---\na: 2\n"), &v)
	if !errors.Is(err, ErrMultipleDocuments) {
		t.Fatalf("expected ErrMultipleDocuments, got %v", err)
	}
}

func TestDecodeStrictEmpty(t *testing.T) {
	var v testConfig
	if err := DecodeStrict([]byte(""), &v); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDecodeStrictTypeAndMulti(t *testing.T) {
	var v testConfig
	err := DecodeStrict([]byte("a: x\n---\na: 2\n"), &v)
	var te *yaml.TypeError
	if !errors.As(err, &te) {
		t.Fatalf("expected *yaml.TypeError in chain, got %v", err)
	}
	if !errors.Is(err, ErrMultipleDocuments) {
		t.Fatalf("expected ErrMultipleDocuments in chain, got %v", err)
	}
}

type valueTarget struct {
	A int `yaml:"a"`
	B struct {
		C []string `yaml:"c"`
	} `yaml:"b"`
}

func TestDecodeValue(t *testing.T) {
	tests := []struct {
		name      string
		src       any
		wantA     int
		wantC     []string
		wantErr   bool
		wantValue bool
		wantPath  string
		wantMsg   string
	}{
		{
			name:  "valid",
			src:   map[string]any{"a": 1, "b": map[string]any{"c": []any{"x"}}},
			wantA: 1,
			wantC: []string{"x"},
		},
		{name: "nil", src: nil},
		{name: "empty map", src: map[string]any{}},
		{
			name:      "unknown top-level field",
			src:       map[string]any{"a": 1, "zz": 2},
			wantErr:   true,
			wantValue: true,
			wantPath:  "zz",
			wantMsg:   `unknown field "zz"`,
		},
		{
			name:      "unknown nested field",
			src:       map[string]any{"b": map[string]any{"q": 1}},
			wantErr:   true,
			wantValue: true,
			wantPath:  "b.q",
			wantMsg:   `unknown field "q"`,
		},
		{
			name:      "type mismatch",
			src:       map[string]any{"a": "abc"},
			wantErr:   true,
			wantValue: true,
			wantPath:  "a",
			wantMsg:   "cannot unmarshal",
		},
		{
			name:      "nested type mismatch",
			src:       map[string]any{"b": map[string]any{"c": "notalist"}},
			wantErr:   true,
			wantValue: true,
			wantPath:  "b.c",
			wantMsg:   "cannot unmarshal",
		},
		{
			name:    "unencodable value",
			src:     map[string]any{"a": make(chan int)},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var v valueTarget
			err := DecodeValue(tt.src, &v)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if v.A != tt.wantA {
					t.Fatalf("A = %d, want %d", v.A, tt.wantA)
				}
				if strings.Join(v.B.C, ",") != strings.Join(tt.wantC, ",") {
					t.Fatalf("C = %v, want %v", v.B.C, tt.wantC)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantValue {
				return
			}
			var ve *ValueError
			if !errors.As(err, &ve) {
				t.Fatalf("expected *ValueError, got %T: %v", err, err)
			}
			if len(ve.Errors) != 1 {
				t.Fatalf("expected 1 entry, got %d: %v", len(ve.Errors), ve.Errors)
			}
			if ve.Errors[0].Path != tt.wantPath {
				t.Fatalf("Path = %q, want %q", ve.Errors[0].Path, tt.wantPath)
			}
			if !strings.Contains(ve.Errors[0].Msg, tt.wantMsg) {
				t.Fatalf("Msg = %q, want substring %q", ve.Errors[0].Msg, tt.wantMsg)
			}
		})
	}
}

func TestValueErrorFormat(t *testing.T) {
	e := &ValueError{Errors: []PathError{{"a", "x"}, {"", "y"}}}
	if got := e.Error(); got != "a: x; y" {
		t.Fatalf("Error() = %q, want %q", got, "a: x; y")
	}
}

func TestDecodeStrictFloatIntoInteger(t *testing.T) {
	type inner struct {
		F int32 `yaml:"f"`
	}
	type emb struct {
		M int `yaml:"m"`
	}
	type target struct {
		A   int              `yaml:"a"`
		B   *int64           `yaml:"b"`
		C   []int            `yaml:"c"`
		D   map[string]uint8 `yaml:"d"`
		E   inner            `yaml:"e"`
		G   any              `yaml:"g"`
		H   time.Duration    `yaml:"h"`
		X   float64          `yaml:"x"`
		S   string           `yaml:"s"`
		emb `yaml:",inline"`
	}
	tests := []struct {
		name  string
		in    string
		want  []string
		count int
		check func(t *testing.T, v *target)
	}{
		{name: "int", in: "a: 3", check: func(t *testing.T, v *target) {
			if v.A != 3 {
				t.Errorf("A = %d, want 3", v.A)
			}
		}},
		{name: "hex into pointer", in: "b: 0x10", check: func(t *testing.T, v *target) {
			if v.B == nil || *v.B != 16 {
				t.Errorf("B = %v, want 16", v.B)
			}
		}},
		{name: "float into any", in: "g: 2.5", check: func(t *testing.T, v *target) {
			if v.G != 2.5 {
				t.Errorf("G = %v, want 2.5", v.G)
			}
		}},
		{name: "float into float64", in: "x: 2.5"},
		{name: "null into pointer", in: "b: ~", check: func(t *testing.T, v *target) {
			if v.B != nil {
				t.Errorf("B = %v, want nil", v.B)
			}
		}},
		{name: "quoted float into string", in: `s: "2.5"`},
		{name: "float into int", in: "a: 2.5", want: []string{"line 1: cannot unmarshal !!float `2.5` into int"}},
		{name: "float 1.0 into int", in: "a: 1.0", want: []string{"line 1: cannot unmarshal !!float `1.0` into int"}},
		{name: "float 1e3 into int", in: "a: 1e3", want: []string{"line 1: cannot unmarshal !!float `1e3` into int"}},
		{name: "float .inf into int", in: "a: .inf", want: []string{"line 1: cannot unmarshal !!float `.inf` into int"}},
		{name: "float .nan into int", in: "a: .nan", want: []string{"line 1: cannot unmarshal !!float `.nan` into int"}},
		{name: "float into int64 pointer", in: "b: 2.5", want: []string{"line 1: cannot unmarshal !!float `2.5` into int64"}},
		{name: "float in int slice", in: "c: [1, 2.5]", want: []string{"line 1: cannot unmarshal !!float `2.5` into int"}},
		{name: "float in block int slice", in: "c:\n  - 1\n  - 2.5\n", want: []string{"line 3: cannot unmarshal !!float `2.5` into int"}},
		{name: "float into uint8 map value", in: "d: {k: 0.5}", want: []string{"line 1: cannot unmarshal !!float `0.5` into uint8"}},
		{name: "float into nested int32", in: "e: {f: 7.25}", want: []string{"line 1: cannot unmarshal !!float `7.25` into int32"}},
		{name: "float into inline int", in: "m: 1.5", want: []string{"line 1: cannot unmarshal !!float `1.5` into int"}},
		{name: "long float truncated in message", in: "a: 12345678901.5", want: []string{"line 1: cannot unmarshal !!float `1234567...` into int"}},
		{name: "float through alias", in: "x: &v 2.5\na: *v\n", want: []string{"line 1: cannot unmarshal !!float `2.5` into int"}},
		{name: "float through merge", in: "d:\n  <<: {k: 1.5}\n  j: 2\n", want: []string{"line 2: cannot unmarshal !!float `1.5` into uint8"}},
		{name: "outer key wins over merge", in: "d:\n  <<: {k: 1.5}\n  k: 2\n"},
		{name: "ordered by line", in: "a: 2.5\nb: str\n", want: []string{
			"line 1: cannot unmarshal !!float `2.5` into int",
			"line 2: cannot unmarshal !!str `str` into int64",
		}},
		{name: "no duplicate of yaml overflow", in: "b: 99999999999999999999\n", count: 1},
		{name: "no duplicate of yaml duration", in: "h: 1.5\n", count: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var v target
			err := DecodeStrict([]byte(tt.in), &v)
			if tt.want == nil && tt.count == 0 {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if tt.check != nil {
					tt.check(t, &v)
				}
				return
			}
			var te *yaml.TypeError
			if !errors.As(err, &te) {
				t.Fatalf("expected *yaml.TypeError, got %v", err)
			}
			if tt.count != 0 {
				if len(te.Errors) != tt.count {
					t.Fatalf("got %d entries, want %d: %v", len(te.Errors), tt.count, te.Errors)
				}
				return
			}
			if !slices.Equal(te.Errors, tt.want) {
				t.Fatalf("Errors = %q, want %q", te.Errors, tt.want)
			}
		})
	}
}

func TestDecodeStrictFloatIntoIntegerMultiDoc(t *testing.T) {
	type target struct {
		A int `yaml:"a"`
	}
	var v target
	err := DecodeStrict([]byte("a: 2.5\n---\na: 1\n"), &v)
	var te *yaml.TypeError
	if !errors.As(err, &te) {
		t.Fatalf("expected *yaml.TypeError in chain, got %v", err)
	}
	if !slices.Equal(te.Errors, []string{"line 1: cannot unmarshal !!float `2.5` into int"}) {
		t.Fatalf("Errors = %q", te.Errors)
	}
	if !errors.Is(err, ErrMultipleDocuments) {
		t.Fatalf("expected ErrMultipleDocuments in chain, got %v", err)
	}
}

func TestDecodeValueFloatIntoInteger(t *testing.T) {
	var v struct {
		Max int64 `yaml:"max"`
	}
	err := DecodeValue(map[string]any{"max": 2.5}, &v)
	var ve *ValueError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValueError, got %T: %v", err, err)
	}
	want := []PathError{{Path: "max", Msg: "cannot unmarshal !!float `2.5` into int64"}}
	if !slices.Equal(ve.Errors, want) {
		t.Fatalf("Errors = %v, want %v", ve.Errors, want)
	}

	for _, val := range []any{3, 3.0} {
		var ok struct {
			Max int64 `yaml:"max"`
		}
		if err := DecodeValue(map[string]any{"max": val}, &ok); err != nil {
			t.Fatalf("value %v: unexpected error: %v", val, err)
		}
		if ok.Max != 3 {
			t.Fatalf("value %v: Max = %d, want 3", val, ok.Max)
		}
	}
}

func TestDecodeStrictRepeatedFloatErrors(t *testing.T) {
	var v struct {
		A int `yaml:"a"`
		B int `yaml:"b"`
	}
	err := DecodeStrict([]byte("{a: 1.5, b: 1.5}"), &v)
	var te *yaml.TypeError
	if !errors.As(err, &te) {
		t.Fatalf("DecodeStrict() = %v, want *yaml.TypeError", err)
	}
	if len(te.Errors) != 2 {
		t.Fatalf("got %d entries, want 2: %v", len(te.Errors), te.Errors)
	}
	if te.Errors[0] != te.Errors[1] {
		t.Errorf("entries differ: %q and %q", te.Errors[0], te.Errors[1])
	}
	if !strings.Contains(te.Errors[0], "into int") {
		t.Errorf("entry %q does not report the integer field", te.Errors[0])
	}
}

type positionedDecoder struct{}

func (p *positionedDecoder) UnmarshalYAML(*yaml.Node) error {
	return &yaml.TypeError{Errors: []string{"line 1: column 4: type x.T: bad"}}
}

type scalarOnly struct{}

func (*scalarOnly) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return &yaml.TypeError{Errors: []string{fmt.Sprintf("line %d: column %d: type x.S: must be a string", n.Line, n.Column)}}
	}
	return nil
}

func TestDecodeValueCustomErrorAtFailingNode(t *testing.T) {
	type target struct {
		A struct {
			B scalarOnly `yaml:"b"`
			C scalarOnly `yaml:"c"`
		} `yaml:"a"`
	}
	tests := []struct {
		name string
		src  any
		want []PathError
	}{
		{
			name: "sequence value",
			src:  map[string]any{"a": map[string]any{"b": []any{"x"}}},
			want: []PathError{{Path: "a.b", Msg: "must be a string"}},
		},
		{
			name: "mapping value",
			src:  map[string]any{"a": map[string]any{"b": map[string]any{"k": "x"}}},
			want: []PathError{{Path: "a.b", Msg: "must be a string"}},
		},
		{
			name: "sequence after scalar sibling",
			src:  map[string]any{"a": map[string]any{"b": "ok", "c": []any{"x", "y"}}},
			want: []PathError{{Path: "a.c", Msg: "must be a string"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var v target
			err := DecodeValue(tt.src, &v)
			var ve *ValueError
			if !errors.As(err, &ve) {
				t.Fatalf("expected *ValueError, got %T: %v", err, err)
			}
			if !slices.Equal(ve.Errors, tt.want) {
				t.Fatalf("Errors = %v, want %v", ve.Errors, tt.want)
			}
		})
	}
}

func TestDecodeValueHidesDecoderPosition(t *testing.T) {
	var v struct {
		H positionedDecoder `yaml:"h"`
	}
	err := DecodeValue(map[string]any{"h": "x"}, &v)
	var ve *ValueError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValueError, got %T: %v", err, err)
	}
	want := []PathError{{Path: "h", Msg: "bad"}}
	if !slices.Equal(ve.Errors, want) {
		t.Fatalf("Errors = %v, want %v", ve.Errors, want)
	}
}
