package doccheck

import (
	"path"
	"regexp"
	"slices"
	"strings"
)

// CopiedSource reports the marks of copied upstream source in the file or directory p. p is slash-separated and relative to the repository root. Path marks are reported with line 0 and Text p. Line marks cover upstream copyright and governance notices, bundler output, and the license texts of the MIT, Apache, SPDX, GPL and MPL families; they are reported once per matching line and pattern. Package marks apply only to .go files. A nil src checks path marks only.
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
		{"MIT grant", regexp.MustCompile(`(?i)permission is hereby grante[d]`)},
		{"Apache header", regexp.MustCompile(`(?i)licensed under the apache licens[e]`)},
		{"SPDX identifier", regexp.MustCompile(`SPDX-License-Identifie[r]:`)},
		{"GPL text", regexp.MustCompile(`(?i)gnu (lesser )?general public licens[e]`)},
		{"MPL text", regexp.MustCompile(`(?i)mozilla public licens[e]`)},
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
	entries, _ := goModEntries(gomod) // an unterminated block still yields the entries read before it
	for _, e := range entries {
		if e.Verb != "require" {
			continue
		}
		if mod, ok := directRequire(e.Args, e.Comment); ok {
			out = append(out, mod)
		}
	}
	return out
}

// directRequire parses one require entry from its args, the entry without the comment, and its comment, the text after //. For example `example.com/m v1.0.0` with comment `indirect`. It reports the module path and whether it is a direct requirement.
func directRequire(args, comment string) (string, bool) {
	fields := strings.Fields(args)
	if len(fields) == 0 {
		return "", false
	}
	if comment == "indirect" || strings.HasPrefix(comment, "indirect;") {
		return "", false
	}
	return unquote(fields[0]), true
}
