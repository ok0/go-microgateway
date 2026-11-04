package doccheck

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestAnchor(t *testing.T) {
	cases := map[string]string{
		"Shared and per-instance state": "shared-and-per-instance-state",
		"`cache` before `auth`":         "cache-before-auth",
		"Retry and budget flow":         "retry-and-budget-flow",
		"routes[].cache":                "routescache",
	}
	for in, want := range cases {
		if got := Anchor(in); got != want {
			t.Errorf("Anchor(%q) = %q, want %q", in, got, want)
		}
	}
	doc := Parse("dup.md", []byte("# x\n\n# x\n\n# x\n"))
	var got []string
	for _, h := range doc.Headings {
		got = append(got, h.Anchor)
	}
	if want := []string{"x", "x-1", "x-2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("duplicate anchors = %v, want %v", got, want)
	}
}

func TestParse(t *testing.T) {
	src := []string{
		"# Title",
		"",
		"Intro with [link](other.md#sec) and `[code](no.md)`.",
		"",
		"```go",
		"[inside](fence.md)",
		"```",
		"",
		"~~~text",
		"# not a heading",
		"~~~",
		"",
		"| Key | Value |",
		"|---|---|",
		"| a\\|b | `c` |",
	}
	doc := Parse("p.md", []byte(strings.Join(src, "\n")))
	if len(doc.Headings) != 1 || doc.Headings[0].Text != "Title" || doc.Headings[0].Line != 1 {
		t.Fatalf("headings = %+v", doc.Headings)
	}
	if len(doc.Fences) != 2 {
		t.Fatalf("fences = %+v", doc.Fences)
	}
	first := doc.Fences[0]
	if first.Lang != "go" || first.Line != 5 || first.Heading != "Title" || first.Body != "[inside](fence.md)" {
		t.Errorf("first fence = %+v", first)
	}
	second := doc.Fences[1]
	if second.Lang != "text" || second.Line != 9 || second.Heading != "Title" {
		t.Errorf("second fence = %+v", second)
	}
	intro := "Intro with [link](other.md#sec) and `[code](no.md)`."
	if second.Marker != intro || first.Marker != intro {
		t.Errorf("markers = %q, %q, want %q", first.Marker, second.Marker, intro)
	}
	if len(doc.Links) != 1 || doc.Links[0].Target != "other.md#sec" || doc.Links[0].Line != 3 {
		t.Errorf("links = %+v", doc.Links)
	}
	if len(doc.Tables) != 1 {
		t.Fatalf("tables = %+v", doc.Tables)
	}
	tb := doc.Tables[0]
	if !reflect.DeepEqual(tb.Header, []string{"Key", "Value"}) {
		t.Errorf("table header = %q", tb.Header)
	}
	if want := [][]string{{"a\\|b", "`c`"}}; !reflect.DeepEqual(tb.Rows, want) {
		t.Errorf("table rows = %q, want %q", tb.Rows, want)
	}
	if tb.Line != 13 || tb.Heading != "Title" {
		t.Errorf("table line = %d heading = %q", tb.Line, tb.Heading)
	}
}

func TestCheckLinks(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("a.md", "# self\n\n[ok](b.md#exists)\n[bad anchor](b.md#missing)\n[bad file](missing.md)\n[self](#self)\n[web](https://x)\n")
	write("b.md", "# exists\n")
	a, err := Load(filepath.Join(dir, "a.md"))
	if err != nil {
		t.Fatal(err)
	}
	problems := CheckLinks([]*Doc{a})
	if len(problems) != 2 {
		t.Fatalf("problems = %q, want two", problems)
	}
	if !strings.Contains(problems[0], "#missing") && !strings.Contains(problems[1], "#missing") {
		t.Errorf("no problem names #missing: %q", problems)
	}
	if !strings.Contains(problems[0], "missing.md") && !strings.Contains(problems[1], "missing.md") {
		t.Errorf("no problem names missing.md: %q", problems)
	}
}

func TestLintMermaid(t *testing.T) {
	valid := map[string]string{
		"flowchart": "flowchart TD\n  A[\"Start\"] --> B{Check}\n  B -->|yes| C[Done]\n",
		"sequence": "sequenceDiagram\n  participant C as Client\n  participant G as Gateway\n" +
			"  C->>G: GET /x\n  loop retry\n    G->>G: attempt\n  end\n  Note over C,G: done\n",
		"state": "stateDiagram-v2\n  [*] --> Closed\n  Closed --> Open: failures\n  Open --> HalfOpen: timeout\n  HalfOpen --> Closed: success\n",
	}
	for name, body := range valid {
		if problems := LintMermaid(body); len(problems) != 0 {
			t.Errorf("valid %s: %q", name, problems)
		}
	}
	invalid := map[string]string{
		"empty":           "",
		"unknown type":    "mindmap\n  root",
		"unbalanced":      "flowchart TD\n  A[Start --> B",
		"odd quotes":      "flowchart TD\n  A[\"Start] --> B[End]",
		"subgraph no end": "flowchart TD\n  subgraph S\n  A --> B",
		"loop no end":     "sequenceDiagram\n  participant A\n  participant B\n  loop x\n  A->>B: hi",
		"undeclared":      "sequenceDiagram\n  participant A\n  A->>Z: hi",
	}
	for name, body := range invalid {
		if problems := LintMermaid(body); len(problems) == 0 {
			t.Errorf("invalid %s: no problem reported", name)
		}
	}
}

func TestHygiene(t *testing.T) {
	if got := Hygiene("clean.md", []byte("plain documentation text about the gateway\n")); len(got) != 0 {
		t.Errorf("clean text findings = %+v", got)
	}
	families := []string{
		"see " + "M" + "3",
		"see " + "AC" + "12",
		"see " + "PAC" + "4",
		"see " + "AD" + "R-" + "7",
		"the " + "mile" + "stone",
		"the " + "road" + "map",
		"the " + "blue" + "print",
		"the " + "char" + "ter",
		"a " + "bri" + "ef",
		"an " + "assist" + "ant",
		"ask " + "clau" + "de",
		"use " + "open" + "ai",
		"under " + ".ens" + "emble",
		"read " + "spec" + ".md",
		"the " + "lych" + "gate",
		"as " + "re" + "quested",
		"the user " + "asked",
	}
	for i, text := range families {
		src := "clean line\n" + text + "\n"
		got := Hygiene("x.md", []byte(src))
		found := false
		for _, f := range got {
			if f.Line == 2 {
				found = true
			}
		}
		if !found {
			t.Errorf("family %d (%q): no finding on line 2, got %+v", i, text, got)
		}
	}
	bad := Hygiene("notes/"+"spec"+".md", []byte("clean\n"))
	if len(bad) != 1 || bad[0].Line != 0 {
		t.Errorf("bad path findings = %+v, want one at line 0", bad)
	}
}

func TestHygieneSourcesClean(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".go" {
			continue
		}
		src, err := os.ReadFile(e.Name())
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range Hygiene(e.Name(), src) {
			t.Errorf("%s:%d: matches %s", f.Path, f.Line, f.Pattern)
		}
	}
}

func TestContainsExcerpt(t *testing.T) {
	file := "func f() {\n\tx := 1\n\ty := 2\n\tz := 3\n}\n"
	if !ContainsExcerpt(file, "x := 1\ny := 2") {
		t.Error("indentation-changed contiguous excerpt not found")
	}
	if ContainsExcerpt(file, "x := 1\nz := 3") {
		t.Error("non-contiguous excerpt matched")
	}
	if ContainsExcerpt(file, "") || ContainsExcerpt(file, "  \n\n") {
		t.Error("empty snippet matched")
	}
}

type customValue struct{ Raw string }

func (c *customValue) UnmarshalYAML(unmarshal func(any) error) error { return nil }

type schemaInline struct {
	Deep int `yaml:"deep"`
}

type schemaRoot struct {
	Name   string `yaml:"name"`
	Skip   string `yaml:"-"`
	Nested struct {
		Port int `yaml:"port"`
	} `yaml:"nested"`
	Items []struct {
		ID   string   `yaml:"id"`
		Tags []string `yaml:"tags"`
	} `yaml:"items"`
	Labels map[string]string `yaml:"labels"`
	Ptr    *struct {
		On bool `yaml:"on"`
	} `yaml:"ptr"`
	Custom   customValue  `yaml:"custom"`
	Inline   schemaInline `yaml:",inline"`
	Untagged int
}

func TestSchemaPaths(t *testing.T) {
	got := SchemaPaths(reflect.TypeOf(schemaRoot{}))
	want := []string{
		"custom",
		"deep",
		"items[].id",
		"items[].tags",
		"labels",
		"name",
		"nested.port",
		"ptr.on",
		"untagged",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("SchemaPaths = %q, want %q", got, want)
	}
}

type valueLimit struct {
	On bool `yaml:"on"`
}

type valueItem struct {
	Ptr *valueLimit `yaml:"ptr"`
}

type valueRoot struct {
	Items []valueItem `yaml:"items"`
}

func TestValues(t *testing.T) {
	root := valueRoot{Items: []valueItem{{Ptr: &valueLimit{On: true}}, {}}}
	leaves := Values(reflect.ValueOf(root), "items[].ptr")
	if len(leaves) != 2 {
		t.Fatalf("items[].ptr values = %d, want 2", len(leaves))
	}
	if leaves[1].Kind() != reflect.Pointer || !leaves[1].IsNil() {
		t.Errorf("second leaf should be a nil pointer, got %v", leaves[1])
	}
	on := Values(reflect.ValueOf(root), "items[].ptr.on")
	if len(on) != 1 || !on[0].Bool() {
		t.Errorf("items[].ptr.on values = %v, want one true value", on)
	}
}

func TestCompareKeys(t *testing.T) {
	schema := []string{"a", "b", "c"}
	documented := []string{"a", "b", "b", "x"}
	got := CompareKeys(schema, documented)
	if len(got) != 3 {
		t.Fatalf("problems = %q, want three", got)
	}
	joined := strings.Join(got, "\n")
	for _, want := range []string{"undocumented key c", "documented key x is not in the schema", "key b documented more than once"} {
		if !strings.Contains(joined, want) {
			t.Errorf("problems %q do not contain %q", got, want)
		}
	}
}

func TestExampleLookup(t *testing.T) {
	t.Setenv("CATALOG_URL", "x")
	lookup := ExampleLookup(nil)
	if _, ok := lookup("CATALOG_URL"); ok {
		t.Error("CATALOG_URL resolved from the process environment")
	}
	if v, ok := lookup("REDIS_ADDR"); !ok || v != "127.0.0.1:6379" {
		t.Errorf("REDIS_ADDR = %q, %v", v, ok)
	}
	override := ExampleLookup(map[string]string{"REDIS_ADDR": "10.0.0.1:6379"})
	if v, ok := override("REDIS_ADDR"); !ok || v != "10.0.0.1:6379" {
		t.Errorf("override REDIS_ADDR = %q, %v", v, ok)
	}
}
