package doccheck

import (
	"bytes"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

const minTrackedGoLines = 10000

// Commit is one commit read from git log output: its hash and its full message.
type Commit struct {
	Hash    string
	Message string
}

// RemoteURL is one remote address read from git output. Name is the remote name, URL the address and Kind one of fetch, push, url or pushurl. A line of an unexpected form is kept with Kind unparsed, Name set to the line and an empty URL.
type RemoteURL struct {
	Name string
	URL  string
	Kind string
}

var allowedRemoteURLs = []string{
	"https://github.com/ok0/go-microgateway",
	"https://github.com/ok0/go-microgateway.git",
	"git@github.com:ok0/go-microgateway",
	"git@github.com:ok0/go-microgateway.git",
}

var commitTrailerPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)^co-` + `authored-by:`),
	regexp.MustCompile(`(?i)^[a-z-]*-` + `session:`),
	regexp.MustCompile(`(?i)clau` + `de\.ai/`),
}

var makeRuleRE = regexp.MustCompile(`^([A-Za-z0-9_.-]+(?: +[A-Za-z0-9_.-]+)*):([^=].*)?$`)

var makeRunRE = regexp.MustCompile(`^\s*make((?:\s+[A-Za-z0-9_.][A-Za-z0-9_.-]*)+)\s*$`)

var (
	checklistEnvRE  = regexp.MustCompile(`MICROGATEWAY_[A-Z0-9_]+`)
	checklistMakeRE = regexp.MustCompile("`make ([A-Za-z0-9_.-]+)`")
	checklistTestRE = regexp.MustCompile(`\b(?:Test|Benchmark|Fuzz)[A-Z0-9_]\w*`)
)

// ParseLog reads git log output written with the format hash, unit separator, full message, record separator. Records are split on the record separator and fields on the unit separator. Both fields are trimmed and empty records are skipped.
func ParseLog(out []byte) []Commit {
	var commits []Commit
	for _, rec := range strings.Split(string(out), "\x1e") {
		rec = strings.TrimSpace(rec)
		if rec == "" {
			continue
		}
		hash, msg, _ := strings.Cut(rec, "\x1f")
		commits = append(commits, Commit{Hash: strings.TrimSpace(hash), Message: strings.TrimSpace(msg)})
	}
	return commits
}

// ScanRefs returns the sorted refs whose history is scanned: local branches, tags and remote-tracking branches. The retained backup branches, local or remote-tracking, and the symbolic HEAD refs are left out.
func ScanRefs(refs []string) []string {
	var out []string
	for _, ref := range refs {
		if isBackupRef(ref) || strings.HasSuffix(ref, "/HEAD") {
			continue
		}
		if strings.HasPrefix(ref, "refs/heads/") || strings.HasPrefix(ref, "refs/tags/") || strings.HasPrefix(ref, "refs/remotes/") {
			out = append(out, ref)
		}
	}
	slices.Sort(out)
	return out
}

// isBackupRef reports whether ref is a retained backup branch, local (refs/heads/backup/...) or remote-tracking (refs/remotes/<remote>/backup/...).
func isBackupRef(ref string) bool {
	if strings.HasPrefix(ref, "refs/heads/backup/") {
		return true
	}
	if rest, ok := strings.CutPrefix(ref, "refs/remotes/"); ok {
		_, branch, found := strings.Cut(rest, "/")
		return found && strings.HasPrefix(branch, "backup/")
	}
	return false
}

// CommitFindings returns a finding for each message line that matches a hygiene pattern or a commit trailer pattern: a co-author trailer, a session trailer or a session URL. Path is the first seven characters of the commit hash and Line is the 1-based line within the message.
func CommitFindings(commits []Commit) []Finding {
	patterns := append(slices.Clone(hygienePatterns), commitTrailerPatterns...)
	var out []Finding
	for _, c := range commits {
		short := c.Hash
		if len(short) > 7 {
			short = short[:7]
		}
		for i, line := range strings.Split(c.Message, "\n") {
			for _, re := range patterns {
				if re.MatchString(line) {
					out = append(out, Finding{Path: short, Line: i + 1, Text: line, Pattern: re.String()})
				}
			}
		}
	}
	return out
}

// ParseRemoteV parses git remote -v output: one line per remote and direction, the name, a tab, the address and the direction in parentheses. Blank lines are ignored and a line of any other form is kept as an unparsed remote.
func ParseRemoteV(out []byte) []RemoteURL {
	var remotes []RemoteURL
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		name, rest, ok := strings.Cut(line, "\t")
		if ok && strings.HasSuffix(rest, ")") {
			if i := strings.LastIndex(rest, " ("); i >= 0 {
				kind := rest[i+2 : len(rest)-1]
				if kind == "fetch" || kind == "push" {
					remotes = append(remotes, RemoteURL{Name: name, URL: rest[:i], Kind: kind})
					continue
				}
			}
		}
		remotes = append(remotes, RemoteURL{Name: line, Kind: "unparsed"})
	}
	return remotes
}

// ParseRemoteConfig parses git config --get-regexp output for remote address keys: one line per key, the key and its value separated by a space. The remote name is the text between the remote prefix and the last dot, so it may contain dots. A key of any other form is kept as an unparsed remote.
func ParseRemoteConfig(out []byte) []RemoteURL {
	const prefix = "remote."
	var remotes []RemoteURL
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		key, value, _ := strings.Cut(line, " ")
		i := strings.LastIndex(key, ".")
		kind := ""
		if i >= 0 {
			kind = key[i+1:]
		}
		if strings.HasPrefix(key, prefix) && i > len(prefix) && (kind == "url" || kind == "pushurl") {
			remotes = append(remotes, RemoteURL{Name: key[len(prefix):i], URL: value, Kind: kind})
			continue
		}
		remotes = append(remotes, RemoteURL{Name: line, Kind: "unparsed"})
	}
	return remotes
}

// RemoteFindings returns a finding for each remote address that is not exactly one of the allowed github.com/ok0/go-microgateway addresses. The comparison is exact: there is no case folding, no trailing slash or .git stripping and no ssh:// form. Path is the remote name and Text the address. No remotes yields no findings.
func RemoteFindings(remotes []RemoteURL) []Finding {
	var out []Finding
	for _, r := range remotes {
		if slices.Contains(allowedRemoteURLs, r.URL) {
			continue
		}
		out = append(out, Finding{Path: r.Name, Text: r.URL, Pattern: "remote outside github.com/ok0/go-microgateway"})
	}
	return out
}

// CountLines returns the number of lines in src: the newlines, plus one for a final line that has no newline. Empty input has no lines.
func CountLines(src []byte) int {
	n := bytes.Count(src, []byte{'\n'})
	if len(src) > 0 && src[len(src)-1] != '\n' {
		n++
	}
	return n
}

// LineGate returns an error when n, the number of tracked Go lines, is below the minimum of 10000.
func LineGate(n int) error {
	if n < minTrackedGoLines {
		return fmt.Errorf("tracked Go lines %d, want at least %d", n, minTrackedGoLines)
	}
	return nil
}

// DependencyFindings returns the problems with the direct requirements of the core and Redis modules. The core module may have at most six direct requirements and none of them may be a Redis package. The Redis module must require exactly github.com/ok0/go-microgateway, github.com/redis/go-redis/v9 and github.com/alicebob/miniredis/v2 directly.
func DependencyFindings(core, redis []string) []string {
	var out []string
	if len(core) > 6 {
		out = append(out, fmt.Sprintf("core module has %d direct requirements, want at most 6", len(core)))
	}
	for _, dep := range core {
		if strings.Contains(dep, "redis") {
			out = append(out, "core module requires a Redis package: "+dep)
		}
	}
	want := []string{"github.com/ok0/go-microgateway", "github.com/redis/go-redis/v9", "github.com/alicebob/miniredis/v2"}
	for _, w := range want {
		if !slices.Contains(redis, w) {
			out = append(out, "redis module lacks the direct requirement "+w)
		}
	}
	for _, dep := range redis {
		if !slices.Contains(want, dep) {
			out = append(out, "redis module has an unexpected direct requirement "+dep)
		}
	}
	return out
}

// MakeTargets returns the rules of a makefile: each target name maps to its prerequisites in order. A rule line may name several targets. Recipe lines, which start with a tab, and variable assignments are not rules. The .PHONY line is kept as a target.
func MakeTargets(makefile []byte) map[string][]string {
	out := map[string][]string{}
	for _, line := range strings.Split(string(makefile), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if strings.HasPrefix(line, "\t") {
			continue
		}
		m := makeRuleRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		prereqs, _, _ := strings.Cut(m[2], "#")
		for _, name := range strings.Fields(m[1]) {
			out[name] = append(out[name], strings.Fields(prereqs)...)
		}
	}
	return out
}

// MakeRecipes returns the recipe of each makefile target: the lines that start with a tab after its rule line, tab stripped, in order. Blank lines inside a recipe are skipped and a rule line naming several targets gives the recipe to each of them. A target without recipe lines maps to an empty slice.
func MakeRecipes(makefile []byte) map[string][]string {
	out := map[string][]string{}
	var current []string
	for _, line := range strings.Split(string(makefile), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if strings.HasPrefix(line, "\t") {
			for _, name := range current {
				out[name] = append(out[name], strings.TrimPrefix(line, "\t"))
			}
			continue
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		current = nil
		m := makeRuleRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		current = strings.Fields(m[1])
		for _, name := range current {
			if _, ok := out[name]; !ok {
				out[name] = []string{}
			}
		}
	}
	return out
}

// MakeVar is a makefile variable assignment. Value is the right-hand side as written, unexpanded, with leading and trailing space trimmed. Conditional is true for ?=.
type MakeVar struct {
	Value       string
	Conditional bool
}

var makeAssignRE = regexp.MustCompile(`^([A-Za-z0-9_.-]+)\s*(::=|:=|\?=|\+=|!=|=)\s*(.*)$`)

var makeNameRE = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

var makeDirectives = []string{"define", "endef", "override", "export", "unexport", "include", "-include", "sinclude", "ifeq", "ifneq", "ifdef", "ifndef", "else", "endif", "vpath"}

var makeBuiltins = []string{"CURDIR", "MAKEFILE_LIST", "MAKE"}

const makeAutoVars = "@<^+?*%|"

// MakeVariables returns the variables that a makefile assigns, keyed by name. Lines that start with a tab, rule lines and comments are not assignments. A make directive, an assignment operator other than =, :=, ::= and ?=, a name assigned twice, and a := value that references a name assigned on a later line are errors, each reported with its line number.
func MakeVariables(makefile []byte) (map[string]MakeVar, error) {
	type assignment struct {
		line            int
		name, op, value string
	}
	var assigns []assignment
	for i, line := range strings.Split(string(makefile), "\n") {
		n := i + 1
		line = strings.TrimSuffix(line, "\r")
		if strings.HasPrefix(line, "\t") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if slices.Contains(makeDirectives, fields[0]) {
			return nil, fmt.Errorf("line %d: unsupported make directive %q", n, fields[0])
		}
		m := makeAssignRE.FindStringSubmatch(strings.TrimLeft(line, " "))
		if m == nil {
			continue
		}
		if m[2] == "+=" || m[2] == "!=" {
			return nil, fmt.Errorf("line %d: unsupported assignment operator %q for %s", n, m[2], m[1])
		}
		assigns = append(assigns, assignment{line: n, name: m[1], op: m[2], value: m[3]})
	}
	vars := map[string]MakeVar{}
	for i, a := range assigns {
		if _, ok := vars[a.name]; ok {
			return nil, fmt.Errorf("line %d: %s is assigned twice", a.line, a.name)
		}
		if a.op == ":=" || a.op == "::=" {
			for _, ref := range makeRefNames(a.value) {
				if _, ok := vars[ref]; ok {
					continue
				}
				if slices.ContainsFunc(assigns[i+1:], func(b assignment) bool { return b.name == ref }) {
					return nil, fmt.Errorf("line %d: %s uses %s before its assignment", a.line, a.name, ref)
				}
			}
		}
		vars[a.name] = MakeVar{Value: strings.TrimSpace(a.value), Conditional: a.op == "?="}
	}
	return vars, nil
}

// makeRefNames returns the variable names that s references, in order. References inside function calls and substitution references are included. Escaped dollars and automatic variables are not names, and an unterminated reference ends the scan.
func makeRefNames(s string) []string {
	var names []string
	for i := 0; i < len(s); i++ {
		if s[i] != '$' || i+1 == len(s) {
			continue
		}
		c := s[i+1]
		switch {
		case c == '$' || strings.IndexByte(makeAutoVars, c) >= 0:
			i++
		case c == '(' || c == '{':
			end, ok := closingDelim(s, i+1)
			if !ok {
				return names
			}
			if content := s[i+2 : end]; makeNameRE.MatchString(content) {
				names = append(names, content)
			} else {
				names = append(names, makeRefNames(content)...)
			}
			i = end
		default:
			names = append(names, string(c))
			i++
		}
	}
	return names
}

// closingDelim returns the index of the delimiter that closes the reference opened at s[open], counting nested delimiters of the same kind, and whether that delimiter exists.
func closingDelim(s string, open int) (int, bool) {
	closer := byte(')')
	if s[open] == '{' {
		closer = '}'
	}
	depth := 0
	for k := open; k < len(s); k++ {
		switch s[k] {
		case s[open]:
			depth++
		case closer:
			depth--
			if depth == 0 {
				return k, true
			}
		}
	}
	return 0, false
}

// makeValue returns the unexpanded value of the variable name in make's resolution order without -e: a non-conditional makefile variable, then an environment entry, then a conditional makefile variable. ok is false when none of them defines name.
func makeValue(name string, vars map[string]MakeVar, env map[string]string) (string, bool) {
	if v, ok := vars[name]; ok && !v.Conditional {
		return v.Value, true
	}
	if val, ok := env[name]; ok {
		return val, true
	}
	if v, ok := vars[name]; ok {
		return v.Value, true
	}
	return "", false
}

// expandMakeVars returns s with its make variable references expanded recursively. The stack holds the names being expanded and detects cycles. A reference to a variable that is not defined is an error, except for the built-ins CURDIR, MAKEFILE_LIST and MAKE, which are kept as written. Function calls and substitution references are kept as written and unevaluated, and so are automatic variables. $$ becomes $.
func expandMakeVars(s string, vars map[string]MakeVar, env map[string]string, stack []string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '$' {
			b.WriteByte(s[i])
			continue
		}
		if i+1 == len(s) {
			return "", fmt.Errorf("trailing $ in %q", s)
		}
		c := s[i+1]
		if c == '$' {
			b.WriteByte('$')
			i++
			continue
		}
		if strings.IndexByte(makeAutoVars, c) >= 0 {
			b.WriteString(s[i : i+2])
			i++
			continue
		}
		ref, name, end := s[i:i+2], string(c), i+1
		if c == '(' || c == '{' {
			closeAt, ok := closingDelim(s, i+1)
			if !ok {
				return "", fmt.Errorf("unterminated variable reference in %q", s)
			}
			ref, name, end = s[i:closeAt+1], s[i+2:closeAt], closeAt
			if !makeNameRE.MatchString(name) {
				b.WriteString(ref)
				i = end
				continue
			}
		}
		i = end
		value, ok := makeValue(name, vars, env)
		if !ok {
			if slices.Contains(makeBuiltins, name) {
				b.WriteString(ref)
				continue
			}
			return "", fmt.Errorf("make variable %q is not defined in the Makefile or the environment", name)
		}
		if slices.Contains(stack, name) {
			return "", fmt.Errorf("make variable cycle: %s", strings.Join(append(slices.Clone(stack), name), " -> "))
		}
		exp, err := expandMakeVars(value, vars, env, append(slices.Clone(stack), name))
		if err != nil {
			return "", err
		}
		b.WriteString(exp)
	}
	return b.String(), nil
}

// ExpandMakeRun returns run with the recipes of the make invocations it contains. A make invocation is a line of the form make <target>..., with plain target names and no flags or variable arguments. After each such line the recipe lines of each named target follow, and before a target the recipes of its prerequisites, depth-first, each target once. Each recipe line has its make variable references expanded from the makefile's assignments (see MakeVariables) and from env, which plays the role of the environment that make imports as variables; a Makefile assignment wins over env except for ?=. Function calls and substitution references, automatic variables and the built-ins CURDIR, MAKEFILE_LIST and MAKE are kept as written. Other lines are kept as they are and not expanded. Errors: an invalid makefile, a line whose first field is make that is not a plain make invocation, a named target or prerequisite that the makefile does not define, a reference to an undefined variable, a variable cycle and an unterminated reference.
func ExpandMakeRun(run string, makefile []byte, env map[string]string) (string, error) {
	vars, err := MakeVariables(makefile)
	if err != nil {
		return "", err
	}
	rules := MakeTargets(makefile)
	recipes := MakeRecipes(makefile)
	var out []string
	for _, line := range strings.Split(run, "\n") {
		out = append(out, line)
		m := makeRunRE.FindStringSubmatch(line)
		if m == nil {
			if fields := strings.Fields(line); len(fields) > 0 && fields[0] == "make" {
				return "", fmt.Errorf("unsupported make invocation %q (only \"make <target>...\" is expanded)", line)
			}
			continue
		}
		done := map[string]bool{}
		var expanded []string
		var visit func(name string) error
		visit = func(name string) error {
			if done[name] {
				return nil
			}
			done[name] = true
			prereqs, ok := rules[name]
			if !ok {
				return fmt.Errorf("make target %q not in Makefile", name)
			}
			for _, p := range prereqs {
				if err := visit(p); err != nil {
					return err
				}
			}
			for _, recipe := range recipes[name] {
				text, err := expandMakeVars(recipe, vars, env, nil)
				if err != nil {
					return fmt.Errorf("make target %q: %w", name, err)
				}
				expanded = append(expanded, text)
			}
			return nil
		}
		for _, name := range strings.Fields(m[1]) {
			if err := visit(name); err != nil {
				return "", err
			}
		}
		out = append(out, expanded...)
	}
	return strings.Join(out, "\n"), nil
}

// ChecklistProblems returns the problems with a release checklist section. Each MICROGATEWAY_ name in the section must appear in an evidence file, each make target must be a Makefile target, each Go test name must be declared by a line func Name( in an evidence _test.go file, and each required text must appear in the section. Evidence files are those whose path does not end in .md and is not under internal/doccheck. The result is sorted and has no duplicates.
func ChecklistProblems(section string, files map[string][]byte, targets map[string][]string, required []string) []string {
	var sources, tests [][]byte
	for name, src := range files {
		if strings.HasSuffix(name, ".md") || strings.HasPrefix(name, "internal/doccheck/") {
			continue
		}
		sources = append(sources, src)
		if strings.HasSuffix(name, "_test.go") {
			tests = append(tests, src)
		}
	}
	var out []string
	for _, token := range uniqueGroup(checklistEnvRE, section, 0) {
		if !slices.ContainsFunc(sources, func(src []byte) bool { return bytes.Contains(src, []byte(token)) }) {
			out = append(out, token+" appears in no non-Markdown file outside internal/doccheck")
		}
	}
	for _, name := range uniqueGroup(checklistMakeRE, section, 1) {
		if _, ok := targets[name]; !ok {
			out = append(out, "make "+name+" is not a Makefile target")
		}
	}
	for _, name := range uniqueGroup(checklistTestRE, section, 0) {
		decl := regexp.MustCompile("(?m)^func " + regexp.QuoteMeta(name) + `\(`)
		if !slices.ContainsFunc(tests, decl.Match) {
			out = append(out, name+" is not declared as func "+name+"( in a _test.go file outside internal/doccheck")
		}
	}
	for _, want := range required {
		if !strings.Contains(section, want) {
			out = append(out, `section lacks "`+want+`"`)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// uniqueGroup returns the sorted, distinct values of the given capture group of re in s.
func uniqueGroup(re *regexp.Regexp, s string, group int) []string {
	var out []string
	for _, m := range re.FindAllStringSubmatch(s, -1) {
		out = append(out, m[group])
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// ChangelogRelease is a released section of a changelog: its version, its date and the 1-based line of its heading.
type ChangelogRelease struct {
	Version string
	Date    string
	Line    int
}

// changelogSubheadings are the level-3 headings a changelog section may have, in their required order: Breaking (changes that need user action) first, then the Keep a Changelog types.
var changelogSubheadings = []string{"Breaking", "Added", "Changed", "Deprecated", "Removed", "Fixed", "Security"}

// changelogHeadingRE matches a released section heading [vX.Y.Z] - YYYY-MM-DD.
var changelogHeadingRE = regexp.MustCompile(`^\[(v[^\]]+)\] - ([0-9]{4}-[0-9]{2}-[0-9]{2})$`)

// ChangelogFindings returns the released sections of a Keep a Changelog document and its structure problems. The first level-2 heading must be [Unreleased] and no other level-2 heading may be [Unreleased]. Every other level-2 heading must be [vX.Y.Z] - YYYY-MM-DD with a release version without prerelease and a valid calendar date; an invalid heading is a finding and is not returned as a release. A section runs from its heading to the line before the next heading of level 2 or less. Its level-3 headings must be known subheadings in the order of changelogSubheadings, each at most once; deeper headings are not checked. A released section must have at least one entry line starting with "- ". No section may repeat an entry line. Consecutive releases in document order must have a lower version and a date that is not after the date of the previous release.
func ChangelogFindings(doc *Doc) ([]ChangelogRelease, []Finding) {
	var h2 []Heading
	for _, h := range doc.Headings {
		if h.Level == 2 {
			h2 = append(h2, h)
		}
	}
	var findings []Finding
	add := func(line int, text, pattern string) {
		findings = append(findings, Finding{Path: doc.Path, Line: line, Text: text, Pattern: pattern})
	}
	if len(h2) == 0 || h2[0].Text != "[Unreleased]" {
		line, text := 0, "none"
		if len(h2) > 0 {
			line, text = h2[0].Line, h2[0].Text
		}
		add(line, "first level-2 heading is "+text+", want [Unreleased]", "changelog-unreleased")
	}
	for i, h := range h2 {
		if i > 0 && h.Text == "[Unreleased]" {
			add(h.Line, "second [Unreleased] heading", "changelog-unreleased")
		}
	}
	var releases []ChangelogRelease
	for _, h := range h2 {
		end := len(doc.Lines) + 1
		for _, n := range doc.Headings {
			if n.Level <= 2 && n.Line > h.Line {
				end = n.Line
				break
			}
		}
		last := -1
		for _, n := range doc.Headings {
			if n.Level != 3 || n.Line <= h.Line || n.Line >= end {
				continue
			}
			i := slices.Index(changelogSubheadings, n.Text)
			switch {
			case i < 0:
				add(n.Line, "unknown subheading "+n.Text, "changelog-subheading")
			case i <= last:
				add(n.Line, "subheading "+n.Text+" repeated or out of order", "changelog-subheading")
			default:
				last = i
			}
		}
		version := ""
		if h.Text != "[Unreleased]" {
			m := changelogHeadingRE.FindStringSubmatch(h.Text)
			valid := m != nil && releaseVersion(m[1]) && !strings.Contains(m[1], "-")
			if valid {
				_, err := time.Parse("2006-01-02", m[2])
				valid = err == nil
			}
			if valid {
				version = m[1]
				releases = append(releases, ChangelogRelease{Version: m[1], Date: m[2], Line: h.Line})
			} else {
				add(h.Line, "heading "+h.Text+", want [vX.Y.Z] - YYYY-MM-DD with a release version and a calendar date", "changelog-heading")
			}
		}
		entries := map[string]bool{}
		count := 0
		for n := h.Line + 1; n < end; n++ {
			line := doc.Lines[n-1]
			if !strings.HasPrefix(line, "- ") {
				continue
			}
			count++
			if entries[line] {
				add(n, "entry repeated: "+line, "changelog-repeat")
			}
			entries[line] = true
		}
		if version != "" && count == 0 {
			add(h.Line, version+" has no entry", "changelog-empty")
		}
	}
	for i := 1; i < len(releases); i++ {
		prev, cur := releases[i-1], releases[i]
		if compareRelease(cur.Version, prev.Version) >= 0 {
			add(cur.Line, cur.Version+" is not below "+prev.Version, "changelog-order")
		}
		if cur.Date > prev.Date {
			add(cur.Line, "date "+cur.Date+" of "+cur.Version+" is after "+prev.Date+" of "+prev.Version, "changelog-order")
		}
	}
	return releases, findings
}

// ChangelogTagFindings returns the problems of the pairing of changelog sections with the core release tags. Core tags are the tags vX.Y.Z without prerelease; other tags are ignored. Every core tag needs a section with the same version (changelog-missing, Path is the tag). Every section needs a core tag or be the pending release (changelog-untagged, Path CHANGELOG.md). When requirePending is set, the pending release needs a section (changelog-missing).
func ChangelogTagFindings(releases []ChangelogRelease, tags []string, pending string, requirePending bool) []Finding {
	tagged := map[string]bool{}
	for _, t := range tags {
		if releaseVersion(t) && !strings.Contains(t, "-") {
			tagged[t] = true
		}
	}
	have := map[string]bool{}
	for _, r := range releases {
		have[r.Version] = true
	}
	var findings []Finding
	var versions []string
	for v := range tagged {
		versions = append(versions, v)
	}
	slices.SortFunc(versions, compareRelease)
	for _, v := range versions {
		if !have[v] {
			findings = append(findings, Finding{Path: v, Text: "core tag " + v + " has no CHANGELOG section", Pattern: "changelog-missing"})
		}
	}
	for _, r := range releases {
		if tagged[r.Version] || r.Version == pending {
			continue
		}
		text := "section " + r.Version + " has no core tag and is not the pending release " + pending
		if pending == "" {
			text = "section " + r.Version + " has no core tag and no release is pending"
		}
		findings = append(findings, Finding{Path: "CHANGELOG.md", Line: r.Line, Text: text, Pattern: "changelog-untagged"})
	}
	if requirePending && !have[pending] {
		findings = append(findings, Finding{Path: "CHANGELOG.md", Text: "no section ## [" + pending + "] - YYYY-MM-DD for the release being checked", Pattern: "changelog-missing"})
	}
	return findings
}

// ChangelogSubsection returns the 1-based line of the level-3 heading subheading inside the section of release version of a changelog, and the last line of that subsection: the line before the next heading of level 3 or less, or the last line of the document. The section is the level-2 heading whose text starts with "[" + version + "] - ". It returns an error naming the version, and the subheading when the section has none.
func ChangelogSubsection(doc *Doc, version, subheading string) (start, end int, err error) {
	prefix := "[" + version + "] - "
	sec := -1
	for _, h := range doc.Headings {
		if h.Level == 2 && strings.HasPrefix(h.Text, prefix) {
			sec = h.Line
			break
		}
	}
	if sec < 0 {
		return 0, 0, fmt.Errorf("%s: no section [%s] - YYYY-MM-DD", doc.Path, version)
	}
	secEnd := len(doc.Lines)
	for _, h := range doc.Headings {
		if h.Level <= 2 && h.Line > sec {
			secEnd = h.Line - 1
			break
		}
	}
	start = -1
	for _, h := range doc.Headings {
		if h.Level == 3 && h.Text == subheading && h.Line > sec && h.Line <= secEnd {
			start = h.Line
			break
		}
	}
	if start < 0 {
		return 0, 0, fmt.Errorf("%s: section %s has no %s subsection", doc.Path, version, subheading)
	}
	end = secEnd
	for _, h := range doc.Headings {
		if h.Level <= 3 && h.Line > start {
			end = min(h.Line-1, secEnd)
			break
		}
	}
	return start, end, nil
}

// ChangelogFence returns the only fenced block with info-string language lang inside the subsection found by ChangelogSubsection, including blocks under deeper headings of that subsection. It returns an error when the subsection is missing, or when it holds no such block or more than one; the error names the version, the subheading, the language and the count.
func ChangelogFence(doc *Doc, version, subheading, lang string) (Fence, error) {
	start, end, err := ChangelogSubsection(doc, version, subheading)
	if err != nil {
		return Fence{}, err
	}
	var found []Fence
	for _, f := range doc.Fences {
		if f.Lang == lang && f.Line > start && f.Line <= end {
			found = append(found, f)
		}
	}
	switch {
	case len(found) == 0:
		return Fence{}, fmt.Errorf("%s:%d: %s subsection of %s has no %s fence", doc.Path, start, subheading, version, lang)
	case len(found) > 1:
		return Fence{}, fmt.Errorf("%s:%d: %s subsection of %s has %d %s fences, want one", doc.Path, start, subheading, version, len(found), lang)
	}
	return found[0], nil
}
