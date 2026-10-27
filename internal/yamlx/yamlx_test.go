package yamlx

import (
	"errors"
	"strings"
	"testing"

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
