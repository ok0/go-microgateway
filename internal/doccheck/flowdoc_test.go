package doccheck

import (
	"reflect"
	"strings"
	"testing"
)

func mustParse(path, src string) *Doc {
	return Parse(path, []byte(src))
}

func TestParseFlowchart(t *testing.T) {
	body := "flowchart TD\n  subgraph Box\n    a[\"A node\"] --> b{\"Choice?\"} --> c\n    end\n  b -- \"no\" --> d[\"Stop\"]\n  c -->|yes| a"
	fc, err := ParseFlowchart(body)
	if err != nil {
		t.Fatalf("ParseFlowchart: %v", err)
	}
	wantLabels := map[string]string{"a": "A node", "b": "Choice?", "c": "", "d": "Stop"}
	if !reflect.DeepEqual(fc.Labels, wantLabels) {
		t.Errorf("Labels = %v, want %v", fc.Labels, wantLabels)
	}
	wantEdges := []Edge{{From: "a", To: "b", Line: 3}, {From: "b", To: "c", Line: 3}, {From: "b", To: "d", Label: "no", Line: 5}, {From: "c", To: "a", Label: "yes", Line: 6}}
	if !reflect.DeepEqual(fc.Edges, wantEdges) {
		t.Errorf("Edges = %v, want %v", fc.Edges, wantEdges)
	}
	for _, bad := range []string{"sequenceDiagram\n  x->>y: hi", "flowchart TD\n  a ==> b", "flowchart TD\n  a[\"one\"]\n  a[\"two\"]"} {
		if _, err := ParseFlowchart(bad); err == nil {
			t.Errorf("ParseFlowchart(%q) returned no error", bad)
		}
	}
}

func TestMainPath(t *testing.T) {
	ok := []struct {
		name, body string
		want       []string
	}{
		{"loop back edge is labelled", "flowchart TD\n  s --> x\n  x --> y\n  y -- \"back\" --> x", []string{"s", "x", "y"}},
	}
	for _, c := range ok {
		fc, err := ParseFlowchart(c.body)
		if err != nil {
			t.Fatalf("%s: ParseFlowchart: %v", c.name, err)
		}
		got, err := fc.MainPath()
		if err != nil {
			t.Errorf("%s: MainPath: %v", c.name, err)
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: MainPath = %v, want %v", c.name, got, c.want)
		}
	}
	bad := []struct{ name, body string }{
		{"two starts", "flowchart TD\n  s --> x\n  t --> y"},
		{"fork", "flowchart TD\n  s --> x\n  s --> y"},
		{"cycle with no start", "flowchart TD\n  x --> y\n  y --> x"},
	}
	for _, c := range bad {
		fc, err := ParseFlowchart(c.body)
		if err != nil {
			t.Fatalf("%s: ParseFlowchart: %v", c.name, err)
		}
		if got, err := fc.MainPath(); err == nil {
			t.Errorf("%s: MainPath = %v, want an error", c.name, got)
		}
	}
}

func TestSectionItems(t *testing.T) {
	src := "## One\n\n1. first `a`\n   more\n2. second\n\n```text\n3. fenced\n```\n\n## Two\n\n1. other\n"
	d := mustParse("doc.md", src)
	if got, want := SectionItems(d, "One"), []string{"first `a` more", "second"}; !reflect.DeepEqual(got, want) {
		t.Errorf("SectionItems(One) = %q, want %q", got, want)
	}
	if got := SectionItems(d, "Missing"); got != nil {
		t.Errorf("SectionItems(Missing) = %q, want nil", got)
	}
}

const flowFixture = "# Fixture\n\n" +
	"## Request flow\n\n" +
	"1. `p` first\n2. `q` second\n3. `r` third\n4. `s` fourth\n5. `t` fifth\n\n" +
	"```mermaid\nflowchart TD\n" +
	"  client --> p\n" +
	"  p -- \"no\" --> bad[\"400 bad\"]\n" +
	"  p --> q\n" +
	"  q --> r[\"r, if configured\"]\n" +
	"  r --> s[\"s, if configured\"]\n" +
	"  s --> t\n" +
	"  t -- \"retry\" --> s\n" +
	"```\n\n" +
	"## Default pipeline order\n\n" +
	"1. `r`\n2. `s`\n3. `t`\n"

func flowFixtureSpec() FlowSpec {
	return FlowSpec{
		Flow:      []string{"p", "q", "r", "s", "t"},
		Stages:    []string{"r", "s"},
		Terminal:  "t",
		RetryFrom: "t",
		RetryTo:   "s",
		Errors:    map[string]string{"p": "400 bad"},
	}
}

func TestFlowDocFindingsFixture(t *testing.T) {
	spec := flowFixtureSpec()
	if got := FlowDocFindings(mustParse("docs/fixture.md", flowFixture), spec); len(got) != 0 {
		t.Fatalf("clean fixture findings = %v, want none", got)
	}
	mutations := []struct{ name, old, new string }{
		{"path order swapped", "  q --> r[\"r, if configured\"]\n  r --> s[\"s, if configured\"]", "  q --> s[\"s, if configured\"]\n  s --> r[\"r, if configured\"]\n  r --> t"},
		{"if configured removed", "r[\"r, if configured\"]", "r[\"r\"]"},
		{"retry edge removed", "\n  t -- \"retry\" --> s", ""},
		{"error edge removed", "\n  p -- \"no\" --> bad[\"400 bad\"]", ""},
		{"list order swapped", "2. `q` second\n3. `r` third", "2. `r` third\n3. `q` second"},
		{"pipeline list order swapped", "1. `r`\n2. `s`", "1. `s`\n2. `r`"},
	}
	for _, m := range mutations {
		if !strings.Contains(flowFixture, m.old) {
			t.Fatalf("%s: fixture lacks %q", m.name, m.old)
		}
		src := strings.Replace(flowFixture, m.old, m.new, 1)
		if got := FlowDocFindings(mustParse("docs/fixture.md", src), spec); len(got) == 0 {
			t.Errorf("%s: no findings", m.name)
		}
	}
}

const flowCopyArch = "# Architecture\n\n" +
	"## Layers\n\n```mermaid\nflowchart TD\n  x --> y\n```\n\n" +
	"## Request flow\n\n```mermaid\nflowchart TD\n  p --> q\n  q --> r\n```\n"

func TestFlowCopyFindingsFixture(t *testing.T) {
	const arch = "docs/architecture.md"
	ids := []string{"p", "q", "r", "s", "t"}
	stages := []string{"r", "s", "t"}
	clean := []*Doc{
		mustParse(arch, flowCopyArch),
		mustParse("docs/other.md", "# Other\n\nPlain text with nothing to report.\n"),
	}
	if got := FlowCopyFindings(clean, arch, ids, stages); len(got) != 0 {
		t.Fatalf("clean findings = %v, want none", got)
	}
	cases := []struct {
		name, prefix, want string
		docs               []*Doc
	}{
		{"fence names step ids as labels only", "docs/other.md:", "consecutive request-flow steps", []*Doc{
			mustParse(arch, flowCopyArch),
			mustParse("docs/other.md", "# Other\n\n```mermaid\nflowchart TD\n  a[\"p\"] --> b[\"q\"] --> c[\"r\"]\n```\n"),
		}},
		{"line names every stage in order", "docs/other.md:", "line names every default stage in order", []*Doc{
			mustParse(arch, flowCopyArch),
			mustParse("docs/other.md", "# Other\n\nOrder: r then s then t.\n"),
		}},
		{"third mermaid fence in architecture", arch + ":", "", []*Doc{
			mustParse(arch, flowCopyArch+"\n```mermaid\nflowchart TD\n  x --> y\n```\n"),
		}},
		{"architecture without layers fence", arch + ":", "", []*Doc{
			mustParse(arch, "# Architecture\n\n## Request flow\n\n```mermaid\nflowchart TD\n  p --> q\n  q --> r\n```\n"),
		}},
		{"request-flow fence names only two ids", arch + ":", "", []*Doc{
			mustParse(arch, "# Architecture\n\n## Layers\n\n```mermaid\nflowchart TD\n  x --> y\n```\n\n## Request flow\n\n```mermaid\nflowchart TD\n  p --> q\n```\n"),
		}},
		{"architecture document missing", arch + ":", "", []*Doc{
			mustParse("docs/other.md", "# Other\n"),
		}},
		{"fence draws step ids as nodes", "docs/other2.md:", "as nodes", []*Doc{
			mustParse(arch, flowCopyArch),
			mustParse("docs/other2.md", "# Other\n\n## Copy\n\n```mermaid\nflowchart TD\n  p --> r\n  r --> t\n```\n"),
		}},
		{"unparsable fence names three step ids", "docs/other2.md:", "as nodes", []*Doc{
			mustParse(arch, flowCopyArch),
			mustParse("docs/other2.md", "# Other\n\n## Copy\n\n```mermaid\nsequenceDiagram\n  x->>y: p r t\n```\n"),
		}},
		{"consecutive steps spread over labels", "docs/other2.md:", "\"q\", \"r\", \"s\"", []*Doc{
			mustParse(arch, flowCopyArch),
			mustParse("docs/other2.md", "# Other\n\n## Copy\n\n```mermaid\nflowchart TD\n  a[\"q then x\"] --> b[\"y\"]\n  b --> c[\"r, s\"]\n```\n"),
		}},
	}
	for _, c := range cases {
		got := FlowCopyFindings(c.docs, arch, ids, stages)
		found := false
		for _, s := range got {
			if strings.HasPrefix(s, c.prefix) && strings.Contains(s, c.want) {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: findings = %v, want one starting with %q and containing %q", c.name, got, c.prefix, c.want)
		}
	}
	controls := []string{
		"flowchart TD\n  a[\"p\"] --> b[\"r\"]\n  b --> c[\"t\"]",
		"flowchart TD\n  a[\"r\"] --> b[\"q\"]\n  b --> c[\"p\"]",
	}
	for _, body := range controls {
		docs := []*Doc{
			mustParse(arch, flowCopyArch),
			mustParse("docs/other2.md", "# Other\n\n## Copy\n\n```mermaid\n"+body+"\n```\n"),
		}
		if got := FlowCopyFindings(docs, arch, ids, stages); len(got) != 0 {
			t.Errorf("precision control %q: findings = %v, want none", body, got)
		}
	}
}

const readmeFixture = "# x\n## Features\n- f\nSee [arch](docs/architecture.md).\n## Requirements\n## Install\n## Quick start\n## Module layout\n## Dependencies\n## Documentation\n- [Architecture](docs/architecture.md): x\n## License\n"

func TestReadmeArchitectureFindingsFixture(t *testing.T) {
	const target = "docs/architecture.md"
	if got := ReadmeArchitectureFindings(mustParse("README.md", readmeFixture), target); len(got) != 0 {
		t.Fatalf("clean README findings = %v, want none", got)
	}
	mutations := []struct{ name, old, new string }{
		{"request flow heading added", "## License", "## Request flow\n\n## License"},
		{"architecture overview sub-heading added", "## License", "### Architecture overview\n\n## License"},
		{"mermaid fence added", "## License", "```mermaid\nflowchart TD\n  x --> y\n```\n\n## License"},
		{"pointer as three lines", "See [arch](docs/architecture.md).", "Read this.\nAbout the design.\nSee [arch](docs/architecture.md)."},
		{"pointer removed", "See [arch](docs/architecture.md).\n", ""},
		{"second pointer outside Documentation", "## Documentation", "See [again](docs/architecture.md#layers).\n\n## Documentation"},
		{"module layout before install", "## Install\n## Quick start\n## Module layout\n", "## Module layout\n## Install\n## Quick start\n"},
		{"quick start before install", "## Install\n## Quick start\n", "## Quick start\n## Install\n"},
	}
	for _, m := range mutations {
		if !strings.Contains(readmeFixture, m.old) {
			t.Fatalf("%s: fixture lacks %q", m.name, m.old)
		}
		src := strings.Replace(readmeFixture, m.old, m.new, 1)
		if got := ReadmeArchitectureFindings(mustParse("README.md", src), target); len(got) == 0 {
			t.Errorf("%s: no findings", m.name)
		}
	}
}
