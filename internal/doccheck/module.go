package doccheck

import (
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
)

var (
	semverRE = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`)
	pseudoRE = regexp.MustCompile(`(^|\.)[0-9]{14}-[0-9A-Za-z]+$`)
)

// goModEntry is one directive of a go.mod or go.work file: a single-line directive or one line of a block. Verb is the directive keyword, Args the entry without the keyword and without a trailing // comment, Comment the trimmed text after //, and Line the 1-based line number.
type goModEntry struct {
	Line    int
	Verb    string
	Args    string
	Comment string
}

// goModEntries returns the directives of src in file order. A line `verb (` (also `verb(`) opens a block whose lines are entries of verb until a line `)`. Blank and comment-only lines are skipped. An unterminated block is an error; the entries read before the error are returned with it.
func goModEntries(src []byte) ([]goModEntry, error) {
	var out []goModEntry
	inBlock := false
	blockVerb := ""
	openLine := 0
	for i, raw := range strings.Split(string(src), "\n") {
		body, comment, _ := strings.Cut(strings.TrimSpace(raw), "//")
		body = strings.TrimSpace(body)
		comment = strings.TrimSpace(comment)
		if body == "" {
			continue
		}
		if inBlock {
			if body == ")" {
				inBlock = false
				continue
			}
			out = append(out, goModEntry{Line: i + 1, Verb: blockVerb, Args: body, Comment: comment})
			continue
		}
		if head, ok := strings.CutSuffix(body, "("); ok && len(strings.Fields(head)) == 1 {
			inBlock = true
			blockVerb = strings.Fields(head)[0]
			openLine = i + 1
			continue
		}
		verb := strings.Fields(body)[0]
		out = append(out, goModEntry{Line: i + 1, Verb: verb, Args: strings.TrimSpace(body[len(verb):]), Comment: comment})
	}
	if inBlock {
		return out, fmt.Errorf("doccheck: line %d: unterminated %s block", openLine, blockVerb)
	}
	return out, nil
}

// GoMod is the part of a go.mod file that the module checks read: the module path, the go version, every require (direct and indirect) with its version, and every replace entry as written with fields joined by one space.
type GoMod struct {
	Module   string
	Go       string
	Requires map[string]string
	Replaces []string
}

// ParseGoMod reads a go.mod file line by line. Quoted module paths are unquoted. A missing module directive, a require entry that is not "path version", and an unterminated block are errors.
func ParseGoMod(src []byte) (GoMod, error) {
	entries, err := goModEntries(src)
	if err != nil {
		return GoMod{}, err
	}
	m := GoMod{Requires: map[string]string{}}
	for _, e := range entries {
		switch e.Verb {
		case "module":
			if f := strings.Fields(e.Args); len(f) > 0 {
				m.Module = unquote(f[0])
			}
		case "go":
			m.Go = e.Args
		case "require":
			f := strings.Fields(e.Args)
			if len(f) != 2 {
				return GoMod{}, fmt.Errorf("doccheck: line %d: malformed require %q", e.Line, e.Args)
			}
			m.Requires[unquote(f[0])] = f[1]
		case "replace":
			m.Replaces = append(m.Replaces, strings.Join(strings.Fields(e.Args), " "))
		}
	}
	if m.Module == "" {
		return GoMod{}, fmt.Errorf("doccheck: go.mod has no module directive")
	}
	return m, nil
}

// ModuleFindings returns the problems that keep the core and Redis modules from being installed from a release: any replace in either go.mod (Pattern "replace", Path "go.mod" or "redis/go.mod", Text "replace <entry>"); a Redis module that does not require the core (Pattern "core-require"); and a core requirement that is not a semantic version ("core-semver"), is a v0.0.0- placeholder ("core-placeholder") or is a pseudo-version ("core-pseudo"). Version findings have Path "redis/go.mod" and Text "requires <core> <version>".
func ModuleFindings(core, redis GoMod) []Finding {
	var out []Finding
	for _, r := range core.Replaces {
		out = append(out, Finding{Path: "go.mod", Text: "replace " + r, Pattern: "replace"})
	}
	for _, r := range redis.Replaces {
		out = append(out, Finding{Path: "redis/go.mod", Text: "replace " + r, Pattern: "replace"})
	}
	v, ok := redis.Requires[core.Module]
	if !ok {
		return append(out, Finding{Path: "redis/go.mod", Text: "no require of " + core.Module, Pattern: "core-require"})
	}
	text := "requires " + core.Module + " " + v
	if !semverRE.MatchString(v) {
		out = append(out, Finding{Path: "redis/go.mod", Text: text, Pattern: "core-semver"})
	}
	if strings.HasPrefix(v, "v0.0.0-") {
		out = append(out, Finding{Path: "redis/go.mod", Text: text, Pattern: "core-placeholder"})
	}
	if isPseudoVersion(v) {
		out = append(out, Finding{Path: "redis/go.mod", Text: text, Pattern: "core-pseudo"})
	}
	return out
}

// WorkFindings returns the problems of a go.work file. It must hold exactly: go <core go version>, use of . and ./redis, and one replace of the core at the version that redis/go.mod requires with the checkout root (./ or .). A replace without a version or at another version is a problem. Comments are allowed. Path is "go.work" and Line the entry's line (0 for a missing entry).
func WorkFindings(work []byte, core, redis GoMod) []Finding {
	entries, err := goModEntries(work)
	if err != nil {
		return []Finding{{Path: "go.work", Text: err.Error(), Pattern: "parse"}}
	}
	var out []Finding
	var goEntries, uses, replaces []goModEntry
	for _, e := range entries {
		switch e.Verb {
		case "go":
			goEntries = append(goEntries, e)
		case "use":
			uses = append(uses, e)
		case "replace":
			replaces = append(replaces, e)
		default:
			out = append(out, Finding{Path: "go.work", Line: e.Line, Text: strings.TrimSpace(e.Verb + " " + e.Args), Pattern: "extra"})
		}
	}

	switch {
	case len(goEntries) == 0:
		out = append(out, Finding{Path: "go.work", Text: "no go directive", Pattern: "go"})
	case len(goEntries) > 1:
		out = append(out, Finding{Path: "go.work", Line: goEntries[1].Line, Text: "more than one go directive", Pattern: "go"})
	case goEntries[0].Args != core.Go:
		out = append(out, Finding{Path: "go.work", Line: goEntries[0].Line, Text: fmt.Sprintf("go %s, want go %s", goEntries[0].Args, core.Go), Pattern: "go"})
	}

	seen := map[string]bool{}
	for _, u := range uses {
		if (u.Args != "." && u.Args != "./redis") || seen[u.Args] {
			out = append(out, Finding{Path: "go.work", Line: u.Line, Text: "use " + u.Args, Pattern: "use-extra"})
		}
		seen[u.Args] = true
	}
	for _, dir := range []string{".", "./redis"} {
		if !seen[dir] {
			out = append(out, Finding{Path: "go.work", Text: "no use " + dir, Pattern: "use-missing"})
		}
	}

	want := redis.Requires[core.Module]
	matches := 0
	for _, r := range replaces {
		f := strings.Fields(r.Args)
		var reason string
		if len(f) == 4 && f[0] == core.Module && f[1] == want && f[2] == "=>" && path.Clean(f[3]) == "." {
			matches++
			if matches == 1 {
				continue
			}
			reason = ": duplicate"
		} else {
			reason = replaceReason(f, core.Module, want)
		}
		out = append(out, Finding{Path: "go.work", Line: r.Line, Text: "replace " + strings.Join(f, " ") + reason, Pattern: "replace"})
	}
	if matches == 0 {
		out = append(out, Finding{Path: "go.work", Text: fmt.Sprintf("no replace %s %s => ./", core.Module, want), Pattern: "replace-missing"})
	}
	return out
}

// replaceReason explains why the fields f of a go.work replace entry are not the core replace that the Redis module needs; module is the core path and want the version that redis/go.mod requires.
func replaceReason(f []string, module, want string) string {
	switch {
	case len(f) >= 2 && f[0] == module && f[1] == "=>":
		return ": no version, want " + want
	case len(f) >= 2 && f[0] == module && f[1] != want:
		return ": version " + f[1] + ", want " + want + " as redis/go.mod requires"
	case len(f) == 4 && f[0] == module && f[1] == want && f[2] == "=>":
		return ": target " + f[3] + ", want ./"
	}
	return ": not the core module"
}

// isPseudoVersion reports whether v is a pseudo-version: its pre-release part, the text after the first -, cut at the first +, ends in a 14-digit timestamp and a revision.
func isPseudoVersion(v string) bool {
	_, pre, ok := strings.Cut(v, "-")
	if !ok {
		return false
	}
	pre, _, _ = strings.Cut(pre, "+")
	return pseudoRE.MatchString(pre)
}

// unquote returns the unquoted form of s, or s itself when it is not a quoted string.
func unquote(s string) string {
	if q, err := strconv.Unquote(s); err == nil {
		return q
	}
	return s
}
