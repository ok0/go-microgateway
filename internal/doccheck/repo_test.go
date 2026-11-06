package doccheck

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var docsFiles = []string{"architecture.md", "configuration.md", "middleware.md", "operations.md", "multi-instance.md", "caching.md", "embedding.md"}

var readmeTopics = []string{"api-gateway", "microgateway", "golang", "reverse-proxy", "rate-limiting", "circuit-breaker", "http-cache", "redis", "load-balancer"}

var mermaidTopics = []struct{ file, heading string }{
	{"README.md", "Architecture overview"},
	{"README.md", "Request flow"},
	{"docs/middleware.md", "Circuit breaker state machine"},
	{"docs/operations.md", "Hot reload sequence"},
	{"docs/middleware.md", "Retry and budget flow"},
	{"docs/middleware.md", "Authentication flow"},
	{"docs/multi-instance.md", "Multi-instance topology"},
	{"docs/caching.md", "Cache request flow"},
	{"docs/caching.md", "Cluster purge sequence"},
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := Root()
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func repoDocs(t *testing.T, root string) []*Doc {
	t.Helper()
	paths := []string{filepath.Join(root, "README.md"), filepath.Join(root, "CONTRIBUTING.md"), filepath.Join(root, "SECURITY.md"), filepath.Join(root, "CHANGELOG.md")}
	more, err := filepath.Glob(filepath.Join(root, "docs", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	paths = append(paths, more...)
	var docs []*Doc
	for _, p := range paths {
		d, err := Load(p)
		if err != nil {
			t.Fatal(err)
		}
		docs = append(docs, d)
	}
	return docs
}

func readTree(t *testing.T, root string, dirs ...string) map[string]string {
	t.Helper()
	files := map[string]string{}
	for _, dir := range dirs {
		err := filepath.WalkDir(filepath.Join(root, dir), func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			src, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return err
			}
			files[filepath.ToSlash(rel)] = string(src)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return files
}

func relPath(root, p string) string {
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return p
	}
	return filepath.ToSlash(rel)
}

func TestDocsSetPresent(t *testing.T) {
	root := repoRoot(t)
	for _, name := range append([]string{"README.md"}, docsFiles...) {
		p := name
		if name != "README.md" {
			p = filepath.Join("docs", name)
		}
		if _, err := os.Stat(filepath.Join(root, p)); err != nil {
			t.Errorf("%s: %v", p, err)
		}
	}
	readme, err := Load(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	linked := map[string]bool{}
	for _, l := range readme.Links {
		ref, _, _ := strings.Cut(l.Target, "#")
		linked[filepath.ToSlash(filepath.Clean(ref))] = true
	}
	for _, name := range docsFiles {
		if p := "docs/" + name; !linked[p] {
			t.Errorf("README does not link %s", p)
		}
	}
}

func TestDocsLinks(t *testing.T) {
	root := repoRoot(t)
	for _, p := range CheckLinks(repoDocs(t, root)) {
		t.Error(p)
	}
}

func TestReadmeContent(t *testing.T) {
	root := repoRoot(t)
	readme, err := Load(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	title := -1
	for i, line := range readme.Lines {
		if strings.HasPrefix(line, "# ") {
			title = i
			break
		}
	}
	if title < 0 {
		t.Fatal("README has no title")
	}
	i := title + 1
	for i < len(readme.Lines) && strings.TrimSpace(readme.Lines[i]) == "" {
		i++
	}
	var para []string
	for ; i < len(readme.Lines) && strings.TrimSpace(readme.Lines[i]) != ""; i++ {
		para = append(para, readme.Lines[i])
	}
	first := strings.Join(para, " ")
	for _, want := range []string{"API gateway", "microgateway", "Go"} {
		if !strings.Contains(first, want) {
			t.Errorf("first paragraph lacks %q", want)
		}
	}

	var topicLine string
	for _, line := range readme.Lines {
		if strings.HasPrefix(line, "Suggested topics:") {
			topicLine = line
			break
		}
	}
	if topicLine == "" {
		t.Error("README has no Suggested topics line")
	} else {
		var got []string
		for _, m := range codeSpanRE.FindAllString(topicLine, -1) {
			got = append(got, strings.Trim(m, "`"))
		}
		if !reflect.DeepEqual(got, readmeTopics) {
			t.Errorf("Suggested topics = %v, want %v", got, readmeTopics)
		}
	}

	body := strings.Join(readme.Lines, "\n")
	for _, want := range []string{"Apache-2.0", "Redis 6.2", "go get github.com/ok0/go-microgateway", "redis/cmd/microgateway"} {
		if !strings.Contains(body, want) {
			t.Errorf("README lacks %q", want)
		}
	}

	headings := map[string]bool{}
	for _, h := range readme.Headings {
		headings[h.Text] = true
	}
	for _, want := range []string{"Install", "Quick start", "Module layout", "Documentation", "License"} {
		if !headings[want] {
			t.Errorf("README has no heading %q", want)
		}
	}
}

func TestDocsMermaid(t *testing.T) {
	root := repoRoot(t)
	docs := repoDocs(t, root)
	total := 0
	bodies := map[string]string{}
	byPath := map[string]*Doc{}
	for _, d := range docs {
		byPath[d.Path] = d
		for _, f := range d.Fences {
			if f.Lang != "mermaid" {
				continue
			}
			total++
			where := fmt.Sprintf("%s:%d", relPath(root, d.Path), f.Line)
			for _, p := range LintMermaid(f.Body) {
				t.Errorf("%s: %s", where, p)
			}
			key := strings.Join(Normalize(f.Body), "\n")
			if prev, ok := bodies[key]; ok {
				t.Errorf("%s: diagram body repeats the one at %s", where, prev)
			} else {
				bodies[key] = where
			}
		}
	}
	if total < 9 {
		t.Errorf("found %d mermaid diagrams, want at least 9", total)
	}
	for _, want := range mermaidTopics {
		d := byPath[filepath.Join(root, want.file)]
		if d == nil {
			t.Errorf("%s is missing", want.file)
			continue
		}
		level2 := false
		for _, h := range d.Headings {
			if h.Level == 2 && h.Text == want.heading {
				level2 = true
			}
		}
		if !level2 {
			t.Errorf("%s has no level-2 heading %q", want.file, want.heading)
			continue
		}
		found := false
		for _, f := range d.Fences {
			if f.Lang == "mermaid" && f.Heading == want.heading {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: no mermaid diagram under %q", want.file, want.heading)
		}
	}
}

func TestDocsHygiene(t *testing.T) {
	root := repoRoot(t)
	files := readTree(t, root, "docs", "examples", "redis/examples")
	for _, name := range []string{"README.md", "CONTRIBUTING.md", "SECURITY.md", "CHANGELOG.md"} {
		src, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		files[name] = string(src)
	}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		for _, f := range Hygiene(name, []byte(files[name])) {
			t.Errorf("%s:%d: %s", name, f.Line, f.Text)
		}
	}
}

func TestDocsGoSnippets(t *testing.T) {
	root := repoRoot(t)
	const mainFile = "redis/examples/redisgateway/main.go"
	files := readTree(t, root, "examples", "redis/examples")
	var goFiles []string
	for name := range files {
		if strings.HasSuffix(name, ".go") {
			goFiles = append(goFiles, name)
		}
	}
	sort.Strings(goFiles)
	matchedMain, sawFetcher := false, false
	for _, d := range repoDocs(t, root) {
		for _, f := range d.Fences {
			if f.Lang != "go" {
				continue
			}
			where := fmt.Sprintf("%s:%d", relPath(root, d.Path), f.Line)
			matched := false
			for _, name := range goFiles {
				if ContainsExcerpt(files[name], f.Body) {
					matched = true
					break
				}
			}
			if !matched {
				t.Errorf("%s: go fence is not an excerpt of any example file", where)
			}
			if ContainsExcerpt(files[mainFile], f.Body) {
				matchedMain = true
			}
			if strings.Contains(f.Body, "WithKeySetFetcher") {
				sawFetcher = true
			}
		}
	}
	if !matchedMain {
		t.Errorf("no go fence is an excerpt of %s", mainFile)
	}
	if !sawFetcher {
		t.Error("no go fence contains WithKeySetFetcher")
	}
}

func TestEmbeddingGuideNamesExports(t *testing.T) {
	root := repoRoot(t)
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	names := map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(root, name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		exportedNames(f, names)
	}
	for _, want := range []string{"Gateway", "New", "WithStoreType", "Gateway.Purge", "KeySetFetcher.FetchKeySet"} {
		if !names[want] {
			t.Errorf("exported names lack %s; the check would be vacuous", want)
		}
	}
	guide, err := os.ReadFile(filepath.Join(root, "docs", "embedding.md"))
	if err != nil {
		t.Fatal(err)
	}
	var missing []string
	for name := range names {
		re := regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\b`)
		if !re.Match(guide) {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	for _, name := range missing {
		t.Errorf("docs/embedding.md does not name exported %s", name)
	}
}

func exportedNames(f *ast.File, out map[string]bool) {
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if !d.Name.IsExported() {
				continue
			}
			if d.Recv == nil || len(d.Recv.List) == 0 {
				out[d.Name.Name] = true
				continue
			}
			if base := recvBase(d.Recv.List[0].Type); ast.IsExported(base) {
				out[base+"."+d.Name.Name] = true
			}
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					if !s.Name.IsExported() {
						continue
					}
					out[s.Name.Name] = true
					if it, ok := s.Type.(*ast.InterfaceType); ok {
						for _, m := range it.Methods.List {
							for _, n := range m.Names {
								if n.IsExported() {
									out[s.Name.Name+"."+n.Name] = true
								}
							}
						}
					}
				case *ast.ValueSpec:
					for _, n := range s.Names {
						if n.IsExported() {
							out[n.Name] = true
						}
					}
				}
			}
		}
	}
}

func recvBase(e ast.Expr) string {
	for {
		switch t := e.(type) {
		case *ast.StarExpr:
			e = t.X
		case *ast.IndexExpr:
			e = t.X
		case *ast.IndexListExpr:
			e = t.X
		case *ast.ParenExpr:
			e = t.X
		case *ast.Ident:
			return t.Name
		default:
			return ""
		}
	}
}
