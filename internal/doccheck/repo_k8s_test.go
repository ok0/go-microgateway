package doccheck

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

var (
	k8sCodeSpanRE = regexp.MustCompile("`[^`]*`")
	k8sValuesRE   = regexp.MustCompile(`^values\.[A-Za-z][A-Za-z0-9]*(\.[A-Za-z][A-Za-z0-9]*)*$`)
	k8sKeyCellRE  = regexp.MustCompile("^`values\\.[^`]+`$")
)

// kubernetesDocProblems returns the problems of the Kubernetes guide: its README entry, its links, its backticked values paths and values table, the shutdown formula, the subPath warning, the Redis wiring and the per-pod caveat. The result is sorted.
func kubernetesDocProblems(root string) []string {
	guide, err := Load(filepath.Join(root, "docs", "kubernetes.md"))
	if err != nil {
		return []string{"docs/kubernetes.md: missing: " + err.Error()}
	}
	var problems []string

	if readme, err := Load(filepath.Join(root, "README.md")); err != nil {
		problems = append(problems, "README.md: missing: "+err.Error())
	} else {
		_, first, last, _ := section(readme, "Documentation")
		linked := false
		for _, link := range readme.Links {
			if link.Target == "docs/kubernetes.md" && link.Line >= first && link.Line <= last {
				linked = true
			}
		}
		if !linked {
			problems = append(problems, "README Documentation list does not link docs/kubernetes.md")
		}
	}

	problems = append(problems, CheckLinks([]*Doc{guide})...)

	var values map[string]any
	if src, err := os.ReadFile(filepath.Join(root, "deploy", "helm", "microgateway", "values.yaml")); err != nil {
		problems = append(problems, "deploy/helm/microgateway/values.yaml: missing: "+err.Error())
	} else if err := yaml.Unmarshal(src, &values); err != nil {
		problems = append(problems, "deploy/helm/microgateway/values.yaml does not parse: "+err.Error())
	}

	spans := 0
	inFence := false
	for i, line := range guide.Lines {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		for _, m := range k8sCodeSpanRE.FindAllString(line, -1) {
			span := strings.Trim(m, "`")
			if !strings.HasPrefix(span, "values.") || span == "values.yaml" {
				continue
			}
			spans++
			path := strings.Split(strings.TrimPrefix(span, "values."), ".")
			if !k8sValuesRE.MatchString(span) || !valuePathResolves(values, path) {
				problems = append(problems, fmt.Sprintf("docs/kubernetes.md:%d: `%s` does not resolve to a key of deploy/helm/microgateway/values.yaml", i+1, span))
			}
		}
	}
	if spans == 0 {
		problems = append(problems, "docs/kubernetes.md has no backticked values. path")
	}

	for _, table := range guide.Tables {
		if table.Heading != "Values" {
			continue
		}
		for i, row := range table.Rows {
			key := ""
			if len(row) > 0 {
				key = row[0]
			}
			if !k8sKeyCellRE.MatchString(key) {
				problems = append(problems, fmt.Sprintf("docs/kubernetes.md: Values table row %d key %s is not a backticked values. path", i+1, key))
			}
		}
	}

	if body, _, _, _ := section(guide, "Shutdown timing"); !strings.Contains(body, "shutdown_delay + drain_timeout + 5s <= terminationGracePeriodSeconds") {
		problems = append(problems, "docs/kubernetes.md Shutdown timing section does not state the formula shutdown_delay + drain_timeout + 5s <= terminationGracePeriodSeconds")
	}

	subPath := false
	for _, h := range guide.Headings {
		if strings.Contains(h.Text, "subPath") {
			if body, _, _, _ := section(guide, h.Text); strings.Contains(body, "never receives updates") {
				subPath = true
			}
		}
	}
	if !subPath {
		problems = append(problems, "docs/kubernetes.md has no subPath warning")
	}

	redisBody, _, _, _ := section(guide, "Shared state with Redis")
	for _, want := range []string{"redis.enabled", "existingSecret", "kubectl create secret", "${REDIS_PASSWORD}"} {
		if !strings.Contains(redisBody, want) {
			problems = append(problems, "docs/kubernetes.md has no Redis wiring section")
			break
		}
	}

	caveat := false
	for _, h := range guide.Headings {
		if strings.HasPrefix(h.Text, "Per-pod state") {
			if body, _, _, _ := section(guide, h.Text); strings.Contains(body, "one pod only") {
				caveat = true
			}
		}
	}
	if !caveat {
		problems = append(problems, "docs/kubernetes.md has no per-pod in-memory caveat")
	}

	sort.Strings(problems)
	return problems
}

func valuePathResolves(values map[string]any, path []string) bool {
	var cur any = values
	for _, seg := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return false
		}
		next, ok := m[seg]
		if !ok {
			return false
		}
		cur = next
	}
	return true
}

func kubernetesFixture(t *testing.T, root string) map[string]string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(root, "docs", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	paths = append(paths, filepath.Join(root, "README.md"), filepath.Join(root, "deploy", "helm", "microgateway", "values.yaml"))
	files := map[string]string{}
	for _, p := range paths {
		src, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			t.Fatal(err)
		}
		files[filepath.ToSlash(rel)] = string(src)
	}
	return files
}

func replaceIn(file, old, with string) func(*testing.T, map[string]string) {
	return func(t *testing.T, files map[string]string) {
		files[file] = replaceOnce(t, files[file], old, with)
	}
}

func TestKubernetesDoc(t *testing.T) {
	root := repoRoot(t)
	for _, p := range kubernetesDocProblems(root) {
		t.Errorf("%s", p)
	}

	cases := []struct {
		name string
		edit func(*testing.T, map[string]string)
		want string
	}{
		{"unmutated", func(*testing.T, map[string]string) {}, ""},
		{"missing-guide", func(_ *testing.T, files map[string]string) { delete(files, "docs/kubernetes.md") }, "docs/kubernetes.md: missing"},
		{"readme-link", replaceIn("README.md", "(docs/kubernetes.md)", "(docs/operations.md)"), "does not link docs/kubernetes.md"},
		{"broken-link", replaceIn("docs/kubernetes.md", "(multi-instance.md)", "(multi-instance-missing.md)"), "broken link"},
		{"values-path", replaceIn("docs/kubernetes.md", "`values.redis.passwordKey`", "`values.redis.passwordKeys`"), "`values.redis.passwordKeys` does not resolve"},
		{"formula", replaceIn("docs/kubernetes.md", "shutdown_delay + drain_timeout + 5s <= terminationGracePeriodSeconds", "shutdown_delay + drain_timeout <= terminationGracePeriodSeconds"), "does not state the formula"},
		{"subpath-warning", replaceIn("docs/kubernetes.md", "### Why the chart does not use subPath", "### Mounts"), "no subPath warning"},
		{"redis-section", replaceIn("docs/kubernetes.md", "## Shared state with Redis", "## Shared state"), "no Redis wiring section"},
		{"in-memory-caveat", replaceIn("docs/kubernetes.md", "### Per-pod state without Redis", "### Notes"), "no per-pod in-memory caveat"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			files := kubernetesFixture(t, root)
			c.edit(t, files)
			dir := t.TempDir()
			writeTree(t, dir, files)
			problems := kubernetesDocProblems(dir)
			if c.want == "" {
				if len(problems) > 0 {
					t.Errorf("unmutated copy has problems: %q", problems)
				}
				return
			}
			found := false
			for _, p := range problems {
				if strings.Contains(p, c.want) {
					found = true
				}
			}
			if !found {
				t.Errorf("want a problem containing %q, got %q", c.want, problems)
			}
		})
	}
}
