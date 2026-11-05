package doccheck

import (
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// CopiedSource reports the marks of copied upstream source in the file or directory p. p is slash-separated and relative to the repository root. Path marks are reported with line 0 and Text p. Line marks are reported once per matching line and pattern; package marks apply only to .go files. A nil src checks path marks only.
func CopiedSource(p string, src []byte) []Finding {
	type mark struct {
		label string
		re    *regexp.Regexp
	}
	var out []Finding
	segs := strings.Split(p, "/")
	pathMark := func(label string) {
		out = append(out, Finding{Path: p, Line: 0, Text: p, Pattern: label})
	}
	if slices.Contains(segs, "vendor") {
		pathMark("vendor directory")
	}
	for i := 0; i+1 < len(segs); i++ {
		if segs[i] == "internal" && segs[i+1] == "h2server" {
			pathMark("copied HTTP/2 server directory")
			break
		}
	}
	if path.Base(p) == "PATENTS" {
		pathMark("patent grant file")
	}
	marks := []mark{
		{"Go copyright notice", regexp.MustCompile(`(?i)copyright\b.*\bthe go author[s]`)},
		{"BSD governance notice", regexp.MustCompile(`governed by a BSD-styl[e]`)},
		{"bundler output", regexp.MustCompile(`golang\.org/x/tools/cmd/bundl[e]`)},
	}
	if strings.HasSuffix(p, ".go") {
		marks = append(marks, mark{"copied net/http package", regexp.MustCompile(`^package (http[2]|hpack|httpgut[s]|httpcommo[n]|httpsf[v])\b`)})
	}
	for i, line := range strings.Split(string(src), "\n") {
		for _, m := range marks {
			if m.re.MatchString(line) {
				out = append(out, Finding{Path: p, Line: i + 1, Text: line, Pattern: m.label})
			}
		}
	}
	return out
}

// DirectRequires returns the module paths of the require directives in gomod that are not marked indirect. Both single-line and block forms are read, and quoted paths are unquoted. The paths keep their file order.
func DirectRequires(gomod []byte) []string {
	var out []string
	inBlock := false
	for _, line := range strings.Split(string(gomod), "\n") {
		t := strings.TrimSpace(line)
		switch {
		case inBlock:
			if t == ")" {
				inBlock = false
				continue
			}
			if mod, ok := directRequire(t); ok {
				out = append(out, mod)
			}
		case t == "require (":
			inBlock = true
		case strings.HasPrefix(t, "require "):
			if mod, ok := directRequire(strings.TrimSpace(strings.TrimPrefix(t, "require "))); ok {
				out = append(out, mod)
			}
		}
	}
	return out
}

// directRequire parses one require entry such as `example.com/m v1.0.0 // indirect` and reports its module path and whether it is a direct requirement.
func directRequire(entry string) (string, bool) {
	body, comment, hasComment := strings.Cut(entry, "//")
	fields := strings.Fields(body)
	if len(fields) == 0 {
		return "", false
	}
	if hasComment {
		c := strings.TrimSpace(comment)
		if c == "indirect" || strings.HasPrefix(c, "indirect;") {
			return "", false
		}
	}
	mod := fields[0]
	if q, err := strconv.Unquote(mod); err == nil {
		mod = q
	}
	return mod, true
}
