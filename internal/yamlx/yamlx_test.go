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
