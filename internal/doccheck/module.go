package doccheck

import (
	"cmp"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const corePath = "github.com/ok0/go-microgateway"

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

// releaseVersion reports whether v is a release version vX.Y.Z with an optional prerelease: a semantic version without build metadata that is not a pseudo-version.
func releaseVersion(v string) bool {
	return semverRE.MatchString(v) && !strings.Contains(v, "+") && !isPseudoVersion(v)
}

// compareRelease orders two release versions by SemVer 2.0.0 precedence and returns -1, 0 or 1. Both must satisfy releaseVersion. A version without prerelease is greater than the same version with one; prerelease identifiers compare left to right, numeric ones numerically, numeric ones before alphanumeric ones, alphanumeric ones in ASCII order, and a shorter list before a longer one with an equal prefix.
func compareRelease(a, b string) int {
	ca, pa, aPre := strings.Cut(strings.TrimPrefix(a, "v"), "-")
	cb, pb, bPre := strings.Cut(strings.TrimPrefix(b, "v"), "-")
	xs, ys := strings.Split(ca, "."), strings.Split(cb, ".")
	for i := range xs {
		if c := compareNumeric(xs[i], ys[i]); c != 0 {
			return c
		}
	}
	switch {
	case !aPre && !bPre:
		return 0
	case !aPre:
		return 1
	case !bPre:
		return -1
	}
	px, py := strings.Split(pa, "."), strings.Split(pb, ".")
	for i := 0; i < len(px) && i < len(py); i++ {
		if c := comparePreIdentifier(px[i], py[i]); c != 0 {
			return c
		}
	}
	return cmp.Compare(len(px), len(py))
}

// compareNumeric compares two decimal digit strings by value, ignoring leading zeros, without converting them to integers.
func compareNumeric(a, b string) int {
	a, b = strings.TrimLeft(a, "0"), strings.TrimLeft(b, "0")
	if len(a) != len(b) {
		return cmp.Compare(len(a), len(b))
	}
	return strings.Compare(a, b)
}

// comparePreIdentifier compares two prerelease identifiers by SemVer 2.0.0 precedence.
func comparePreIdentifier(a, b string) int {
	an, bn := isDigits(a), isDigits(b)
	switch {
	case an && bn:
		return compareNumeric(a, b)
	case an:
		return -1
	case bn:
		return 1
	}
	return strings.Compare(a, b)
}

// isDigits reports whether s is a non-empty string of decimal digits.
func isDigits(s string) bool {
	return s != "" && strings.Trim(s, "0123456789") == ""
}

// ReleaseFindings returns the problems of a release at version. version must be a release version. redis/go.mod must require the core module at version, and go.work must replace the core module at version: every versioned replace of the core in go.work must name version. Findings of ModuleFindings and WorkFindings are appended. Path is "VERSION" for a version that is not a release version, otherwise "redis/go.mod" or "go.work".
func ReleaseFindings(version string, work []byte, core, redis GoMod) []Finding {
	if !releaseVersion(version) {
		return []Finding{{Path: "VERSION", Text: fmt.Sprintf("%q is not a release version vX.Y.Z", version), Pattern: "release-version"}}
	}
	var out []Finding
	if got := redis.Requires[corePath]; got == "" {
		out = append(out, Finding{Path: "redis/go.mod", Text: "does not require " + corePath + ", want " + version, Pattern: "release-require"})
	} else if got != version {
		out = append(out, Finding{Path: "redis/go.mod", Text: "requires " + corePath + " " + got + ", want " + version, Pattern: "release-require"})
	}

	entries, _ := goModEntries(work)
	versioned := 0
	for _, e := range entries {
		f := strings.Fields(e.Args)
		if e.Verb != "replace" || len(f) < 2 || f[0] != corePath || f[1] == "=>" {
			continue
		}
		versioned++
		if f[1] != version {
			out = append(out, Finding{Path: "go.work", Line: e.Line, Text: "replaces " + corePath + " " + f[1] + ", want " + version, Pattern: "release-replace"})
		}
	}
	if versioned == 0 {
		out = append(out, Finding{Path: "go.work", Text: "has no versioned replace of " + corePath + ", want " + version, Pattern: "release-replace"})
	}

	out = append(out, ModuleFindings(core, redis)...)
	return append(out, WorkFindings(work, core, redis)...)
}

// TagFindings returns the problems of the release tags. Core tags are release versions vX.Y.Z and Redis tags redis/vX.Y.Z; other tags are ignored. The highest core tag may lack its Redis tag while a release is in progress. Every other core tag needs a Redis tag, every Redis tag needs a core tag, and the redis/go.mod at each Redis tag must require the core module at that version; redisRequireAt returns the core version that the redis/go.mod at a tag requires, and its error is reported as a finding. TagFindings never creates tags and reads only through redisRequireAt. Findings are sorted by Path and Pattern; no tags give nil.
func TagFindings(tags []string, redisRequireAt func(tag string) (string, error)) []Finding {
	coreTags := map[string]bool{}
	redisVersions := map[string]bool{}
	for _, t := range tags {
		switch {
		case releaseVersion(t):
			coreTags[t] = true
		case strings.HasPrefix(t, "redis/") && releaseVersion(t[len("redis/"):]):
			redisVersions[t[len("redis/"):]] = true
		}
	}
	highest := ""
	for v := range coreTags {
		if highest == "" || compareRelease(v, highest) > 0 {
			highest = v
		}
	}

	var out []Finding
	for v := range coreTags {
		if !redisVersions[v] && v != highest {
			out = append(out, Finding{Path: v, Text: "core tag " + v + " has no redis/" + v + " tag", Pattern: "tag-redis-missing"})
		}
	}
	for v := range redisVersions {
		tag := "redis/" + v
		if !coreTags[v] {
			out = append(out, Finding{Path: tag, Text: tag + " has no core tag " + v, Pattern: "tag-core-missing"})
		}
		got, err := redisRequireAt(tag)
		switch {
		case err != nil:
			out = append(out, Finding{Path: tag, Text: "redis/go.mod at " + tag + ": " + err.Error(), Pattern: "tag-read"})
		case got != v:
			out = append(out, Finding{Path: tag, Text: "redis/go.mod at " + tag + " requires " + corePath + " " + got + ", want " + v, Pattern: "tag-require"})
		}
	}
	slices.SortFunc(out, func(a, b Finding) int {
		return cmp.Or(strings.Compare(a.Path, b.Path), strings.Compare(a.Pattern, b.Pattern))
	})
	return out
}

// InstallFindings returns the problems of the install command in the README. mainDirs must hold exactly one directory of a main package that calls the CLI entry; the README must have one go install line for that package at a release version or at latest, and no other go install line. Every finding has Path "README.md".
func InstallFindings(readme []byte, mainDirs []string) []Finding {
	if len(mainDirs) != 1 {
		return []Finding{{Path: "README.md", Text: fmt.Sprintf("want exactly one main package that calls the CLI entry, found %d %q", len(mainDirs), mainDirs), Pattern: "install-main"}}
	}
	want := corePath + "/" + mainDirs[0]
	lines := strings.Split(string(readme), "\n")
	var installs []int
	for i, raw := range lines {
		if strings.HasPrefix(strings.TrimSpace(raw), "go install ") {
			installs = append(installs, i+1)
		}
	}
	if len(installs) == 0 {
		return []Finding{{Path: "README.md", Text: "no go install command for " + want, Pattern: "install-missing"}}
	}

	var out []Finding
	for _, n := range installs[1:] {
		out = append(out, Finding{Path: "README.md", Line: n, Text: "more than one go install command", Pattern: "install-multiple"})
	}
	first := installs[0]
	fields := strings.Fields(strings.TrimSpace(lines[first-1]))
	if len(fields) != 3 || !strings.Contains(fields[2], "@") {
		return append(out, Finding{Path: "README.md", Line: first, Text: "install command lacks a single pkg@version argument", Pattern: "install-version"})
	}
	at := strings.LastIndex(fields[2], "@")
	pkg, ver := fields[2][:at], fields[2][at+1:]
	if pkg != want {
		out = append(out, Finding{Path: "README.md", Line: first, Text: "installs " + pkg + ", want " + want, Pattern: "install-path"})
	}
	if ver != "latest" && !releaseVersion(ver) {
		out = append(out, Finding{Path: "README.md", Line: first, Text: "version " + strconv.Quote(ver) + " is not latest or a release version", Pattern: "install-version"})
	}
	return out
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
