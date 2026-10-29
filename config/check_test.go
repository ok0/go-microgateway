package config

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const checkDoc = `version: 1
listeners:
  - name: public
    address: "127.0.0.1:0"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
routes:
  - name: all
    match:
      path: /
      path_type: prefix
    upstream: users
`

func lineIn(t *testing.T, doc, substr string) int {
	t.Helper()
	for i, line := range strings.Split(doc, "\n") {
		if strings.Contains(line, substr) {
			return i + 1
		}
	}
	t.Fatalf("%q not found in document", substr)
	return 0
}

func validationErrorOf(t *testing.T, err error) *ValidationError {
	t.Helper()
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error = %v, want *ValidationError", err)
	}
	return ve
}

func TestWithCheckValidDocument(t *testing.T) {
	check := func(*Config) []FieldError {
		return []FieldError{{Path: "routes[0].upstream", Msg: "checked"}}
	}
	_, err := Parse([]byte(checkDoc), WithCheck(check))
	ve := validationErrorOf(t, err)
	if len(ve.Errors) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(ve.Errors), ve.Errors)
	}
	want := "routes[0].upstream (line " + strconv.Itoa(lineIn(t, checkDoc, "upstream: users")) + "): checked"
	if got := ve.Errors[0].String(); got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}

	cfg, err := Parse([]byte(checkDoc), WithCheck(func(*Config) []FieldError { return nil }))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if cfg == nil {
		t.Fatal("Parse() returned nil config")
	}
}

func TestWithCheckJoinsSchemaErrors(t *testing.T) {
	doc := strings.Replace(checkDoc, "upstream: users", "upstream: missing", 1)
	var seen *Config
	check := func(c *Config) []FieldError {
		seen = c
		return []FieldError{{Path: "listeners[0].name", Msg: "checked"}}
	}
	_, err := Parse([]byte(doc), WithCheck(check))
	ve := validationErrorOf(t, err)

	var schemaHit, checkHit bool
	for _, fe := range ve.Errors {
		if fe.Path == "routes[0].upstream" {
			schemaHit = true
		}
		if fe.Path == "listeners[0].name" && fe.Msg == "checked" {
			checkHit = true
		}
	}
	if !schemaHit || !checkHit {
		t.Fatalf("errors = %v, want schema error at routes[0].upstream and checked error at listeners[0].name", ve.Errors)
	}
	if seen == nil {
		t.Fatal("check was not called")
	}
	if seen.Routes[0].Upstream != "missing" {
		t.Fatalf("check saw upstream %q, want %q", seen.Routes[0].Upstream, "missing")
	}
}

func TestWithCheckDropsReportedPaths(t *testing.T) {
	doc := strings.Replace(checkDoc, "upstream: users", "upstream: missing", 1)
	_, err := Parse([]byte(doc))
	base := len(validationErrorOf(t, err).Errors)

	check := func(*Config) []FieldError {
		return []FieldError{
			{Path: "routes[0].upstream", Msg: "dup"},
			{Path: "routes[0].upstream.x", Msg: "nested"},
			{Msg: "pathless"},
			{Path: "routes[0].name", Msg: "kept"},
		}
	}
	_, err = Parse([]byte(doc), WithCheck(check))
	ve := validationErrorOf(t, err)
	if len(ve.Errors) != base+2 {
		t.Fatalf("got %d errors, want %d: %v", len(ve.Errors), base+2, ve.Errors)
	}
	var pathless, kept bool
	for _, fe := range ve.Errors {
		switch fe.Msg {
		case "dup", "nested":
			t.Fatalf("error %v should have been dropped", fe)
		case "pathless":
			pathless = true
		case "kept":
			kept = true
		}
	}
	if !pathless || !kept {
		t.Fatalf("errors = %v, want pathless and kept entries", ve.Errors)
	}
}

func TestWithCheckNotRunOnUndecodable(t *testing.T) {
	calls := 0
	counter := func(*Config) []FieldError {
		calls++
		return nil
	}
	for _, data := range []string{"version: [1\n", "- a\n- b\n"} {
		if _, err := Parse([]byte(data), WithCheck(counter)); err == nil {
			t.Fatalf("Parse(%q) error = nil, want error", data)
		}
	}
	if calls != 0 {
		t.Fatalf("check ran %d times, want 0", calls)
	}
}

func TestWithCheckLoadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(path, []byte(checkDoc), 0o600); err != nil {
		t.Fatal(err)
	}
	check := func(*Config) []FieldError {
		return []FieldError{{Path: "routes[0].name", Msg: "checked"}}
	}
	_, err := LoadFile(path, WithCheck(check))
	ve := validationErrorOf(t, err)
	want := "routes[0].name (line " + strconv.Itoa(lineIn(t, checkDoc, "name: all")) + "): checked"
	if len(ve.Errors) != 1 || ve.Errors[0].String() != want {
		t.Fatalf("errors = %v, want one error %q", ve.Errors, want)
	}
}
