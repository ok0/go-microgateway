package cli

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/doccheck"
)

const probeAddressKey = "admin.probes.address"

var defaultKeys = append(slices.Clone(restoreKeys), probeAddressKey)

var restoreFiles = []string{"CHANGELOG.md", "docs/configuration.md", "docs/operations.md", "docs/kubernetes.md"}

func restoreDefaults(t *testing.T) map[string]string {
	t.Helper()
	cfg, err := config.Parse([]byte(noAdminConfig))
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return map[string]string{
		"global.shutdown_delay": cfg.Global.ShutdownDelay.String(),
		"global.drain_timeout":  cfg.Global.DrainTimeout.String(),
		"admin.probes.enabled":  strconv.FormatBool(*cfg.Admin.Probes.Enabled),
		"admin.auth.management": cfg.Admin.Auth.Management,
		probeAddressKey:         cfg.Admin.Probes.Address,
	}
}

type restoreDoc struct {
	doc      *doccheck.Doc
	restores []string
	defaults []string
}

func keyAlt(keys []string) string {
	q := make([]string, len(keys))
	for i, k := range keys {
		q[i] = regexp.QuoteMeta(k)
	}
	return strings.Join(q, "|")
}

func changelogBreakingProblems(restore, defaults map[string]string, changelog *doccheck.Doc) []string {
	start, end, err := doccheck.ChangelogSubsection(changelog, restoreRelease, restoreSubsection)
	if err != nil {
		return []string{err.Error()}
	}
	var problems []string
	have := map[string]bool{}
	defaulted := map[string]bool{}
	nowRE := regexp.MustCompile("`(" + keyAlt(restoreKeys) + ")` now defaults to `([^`]+)` \\(was `([^`]+)`\\)")
	for i := start; i < end; i++ {
		line := changelog.Lines[i]
		if !strings.HasPrefix(line, "- ") {
			continue
		}
		for _, k := range restoreKeys {
			if strings.Contains(line, "`"+k+": "+restore[k]+"`") {
				have[k] = true
			}
		}
		for _, m := range nowRE.FindAllStringSubmatch(line, -1) {
			k, now, was := m[1], m[2], m[3]
			defaulted[k] = true
			if now != defaults[k] || was != restore[k] {
				problems = append(problems, fmt.Sprintf("%s:%d: %s now defaults to %s (was %s), want default %s (was %s)", changelog.Path, i+1, k, now, was, defaults[k], restore[k]))
			}
		}
		if strings.Contains(line, "`admin.probes.enabled: ") && !strings.Contains(line, "`"+defaults[probeAddressKey]+"`") {
			problems = append(problems, fmt.Sprintf("%s:%d: the admin.probes.enabled entry does not name the default probe address %s", changelog.Path, i+1, defaults[probeAddressKey]))
		}
	}
	for _, k := range restoreKeys {
		if !have[k] {
			problems = append(problems, fmt.Sprintf("%s: %s subsection of %s has no entry stating `%s: %s`", changelog.Path, restoreSubsection, restoreRelease, k, restore[k]))
		}
		if (k == "global.shutdown_delay" || k == "global.drain_timeout") && !defaulted[k] {
			problems = append(problems, fmt.Sprintf("%s: %s subsection of %s has no entry saying %s now defaults to a new value", changelog.Path, restoreSubsection, restoreRelease, k))
		}
	}
	return problems
}

func restoreSpanProblems(restore, defaults map[string]string, doc *doccheck.Doc, stated map[string]bool) []string {
	spanRE := regexp.MustCompile("`(" + keyAlt(restoreKeys) + "): ([^`]+)`")
	var problems []string
	for i, line := range doc.Lines {
		for _, m := range spanRE.FindAllStringSubmatch(line, -1) {
			k, v := m[1], m[2]
			switch v {
			case restore[k]:
				stated[k] = true
			case defaults[k]:
			default:
				problems = append(problems, fmt.Sprintf("%s:%d: states %s: %s, want the restore value %s or the default %s", doc.Path, i+1, k, v, restore[k], defaults[k]))
			}
		}
	}
	return problems
}

func restoreFenceProblems(restore map[string]string, doc *doccheck.Doc, stated map[string]bool) []string {
	var problems []string
	for _, f := range doc.Fences {
		if f.Lang != "yaml" || !strings.Contains(strings.ToLower(f.Marker), "restore") {
			continue
		}
		leaves, err := flattenYAML(f.Body)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s:%d: %v", doc.Path, f.Line, err))
			continue
		}
		for _, k := range slices.Sorted(maps.Keys(leaves)) {
			v := leaves[k]
			switch {
			case !slices.Contains(restoreKeys, k):
				problems = append(problems, fmt.Sprintf("%s:%d: restore block states %s, which is not a restore key", doc.Path, f.Line, k))
			case v != restore[k]:
				problems = append(problems, fmt.Sprintf("%s:%d: restore block states %s: %s, want %s", doc.Path, f.Line, k, v, restore[k]))
			default:
				stated[k] = true
			}
		}
	}
	return problems
}

func restoreDefaultProblems(defaults map[string]string, d restoreDoc) []string {
	defRE := regexp.MustCompile("`(" + keyAlt(defaultKeys) + ")` \\(default `([^`]+)`")
	var problems []string
	seen := map[string]bool{}
	for i, line := range d.doc.Lines {
		for _, m := range defRE.FindAllStringSubmatch(line, -1) {
			k, v := m[1], m[2]
			seen[k] = true
			if v != defaults[k] {
				problems = append(problems, fmt.Sprintf("%s:%d: %s default %s, want %s", d.doc.Path, i+1, k, v, defaults[k]))
			}
		}
	}
	for _, k := range d.defaults {
		if !seen[k] {
			problems = append(problems, fmt.Sprintf("%s: states no default for %s", d.doc.Path, k))
		}
	}
	return problems
}

func restoreTableProblems(defaults map[string]string, d restoreDoc) []string {
	var problems []string
	for _, t := range d.doc.Tables {
		keyCol, defCol := slices.Index(t.Header, "Key"), slices.Index(t.Header, "Default")
		if keyCol < 0 || defCol < 0 {
			continue
		}
		for r, row := range t.Rows {
			if keyCol >= len(row) {
				continue
			}
			k := strings.Trim(row[keyCol], "`")
			if !slices.Contains(defaultKeys, k) {
				continue
			}
			line := t.Line + 2 + r
			if defCol >= len(row) {
				problems = append(problems, fmt.Sprintf("%s:%d: %s row has no Default cell", d.doc.Path, line, k))
				continue
			}
			if v := strings.Trim(row[defCol], "`\""); v != defaults[k] {
				problems = append(problems, fmt.Sprintf("%s:%d: %s default %s, want %s", d.doc.Path, line, k, v, defaults[k]))
			}
		}
	}
	return problems
}

func restoreDocProblems(restore, defaults map[string]string, changelog *doccheck.Doc, docs []restoreDoc) []string {
	var problems []string
	for _, k := range slices.Sorted(maps.Keys(restore)) {
		if restore[k] == defaults[k] {
			problems = append(problems, fmt.Sprintf("restore value of %s equals its current default %s", k, defaults[k]))
		}
	}
	problems = append(problems, changelogBreakingProblems(restore, defaults, changelog)...)

	stated := map[*doccheck.Doc]map[string]bool{changelog: {}}
	scanned := []*doccheck.Doc{changelog}
	for _, d := range docs {
		stated[d.doc] = map[string]bool{}
		scanned = append(scanned, d.doc)
	}
	for _, doc := range scanned {
		problems = append(problems, restoreSpanProblems(restore, defaults, doc, stated[doc])...)
	}
	for _, d := range docs {
		problems = append(problems, restoreFenceProblems(restore, d.doc, stated[d.doc])...)
		problems = append(problems, restoreDefaultProblems(defaults, d)...)
		problems = append(problems, restoreTableProblems(defaults, d)...)
		for _, k := range d.restores {
			if !stated[d.doc][k] {
				problems = append(problems, fmt.Sprintf("%s: states no restore value for %s, want `%s: %s`", d.doc.Path, k, k, restore[k]))
			}
		}
	}
	return problems
}

func readRestoreFiles(t *testing.T) map[string]string {
	t.Helper()
	root, err := doccheck.Root()
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, f := range restoreFiles {
		src, err := os.ReadFile(filepath.Join(root, f))
		if err != nil {
			t.Fatal(err)
		}
		files[f] = string(src)
	}
	return files
}

func restoreDocsOf(files map[string]string) (*doccheck.Doc, []restoreDoc) {
	parse := func(p string) *doccheck.Doc {
		return doccheck.Parse(p, []byte(files[p]))
	}
	return parse("CHANGELOG.md"), []restoreDoc{
		{doc: parse("docs/configuration.md"), restores: []string{"global.shutdown_delay", "global.drain_timeout"}},
		{doc: parse("docs/operations.md"), restores: restoreKeys, defaults: []string{"global.shutdown_delay", "global.drain_timeout", probeAddressKey}},
		{doc: parse("docs/kubernetes.md")},
	}
}

func repoRestoreDocs(t *testing.T) (*doccheck.Doc, []restoreDoc) {
	t.Helper()
	return restoreDocsOf(readRestoreFiles(t))
}

func TestRestoreSettingsMatchDocs(t *testing.T) {
	_, restore := changelogRestore(t)
	changelog, docs := repoRestoreDocs(t)
	for _, p := range restoreDocProblems(restore, restoreDefaults(t), changelog, docs) {
		t.Error(p)
	}
}

func expectDriftProblem(t *testing.T, problems []string, key string) {
	t.Helper()
	if !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, key) }) {
		t.Errorf("no problem names %s; problems:\n%s", key, strings.Join(problems, "\n"))
	}
}

func TestRestoreDocProblemsDetectsDrift(t *testing.T) {
	base := readRestoreFiles(t)
	defaults := restoreDefaults(t)
	cases := []struct {
		name, file, old, with, key string
	}{
		{"CHANGELOG fence drain_timeout", "CHANGELOG.md", "  drain_timeout: 30s\nadmin:", "  drain_timeout: 25s\nadmin:", "global.drain_timeout"},
		{"operations fence drain_timeout", "docs/operations.md", "    drain_timeout: 30s", "    drain_timeout: 31s", "global.drain_timeout"},
		{"configuration shutdown_delay", "docs/configuration.md", "`global.shutdown_delay: 0s`", "`global.shutdown_delay: 1s`", "global.shutdown_delay"},
		{"operations drain_timeout default", "docs/operations.md", "(default `20s`)", "(default `25s`)", "global.drain_timeout"},
		{"operations management sentence", "docs/operations.md", " To restore the earlier behavior, in which these endpoints answered without a token on any address, set `admin.auth.management: none`.", "", "admin.auth.management"},
		{"configuration probe address", "docs/configuration.md", "`\":9902\"`", "`\":9903\"`", probeAddressKey},
		{"kubernetes appended probes value", "docs/kubernetes.md", "", "\n`admin.probes.enabled: maybe`\n", "admin.probes.enabled"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			files := maps.Clone(base)
			if c.old == "" {
				files[c.file] += c.with
			} else {
				files[c.file] = mustReplace(t, files[c.file], c.old, c.with)
			}
			changelog, docs := restoreDocsOf(files)
			_, restore, err := changelogRestoreFrom(changelog)
			if err != nil {
				t.Fatal(err)
			}
			expectDriftProblem(t, restoreDocProblems(restore, defaults, changelog, docs), c.key)
		})
	}

	t.Run("defaults drain_timeout", func(t *testing.T) {
		changelog, docs := restoreDocsOf(base)
		_, restore, err := changelogRestoreFrom(changelog)
		if err != nil {
			t.Fatal(err)
		}
		drifted := maps.Clone(defaults)
		drifted["global.drain_timeout"] = "30s"
		expectDriftProblem(t, restoreDocProblems(restore, drifted, changelog, docs), "global.drain_timeout")
	})
}
