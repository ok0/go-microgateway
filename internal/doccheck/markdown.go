// Package doccheck holds checks that keep the repository documentation and examples consistent with the code. It is test support and depends only on the standard library.
package doccheck

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"unicode"
)

const modulePath = "github.com/ok0/go-microgateway"

type Doc struct {
	Path     string
	Lines    []string
	Headings []Heading
	Fences   []Fence
	Links    []Link
	Tables   []Table
}

type Heading struct {
	Level  int
	Text   string
	Anchor string
	Line   int
}

type Fence struct {
	Lang    string
	Body    string
	Heading string
	Marker  string
	Line    int
}

type Link struct {
	Target string
	Line   int
}

type Table struct {
	Header  []string
	Rows    [][]string
	Heading string
	Line    int
}

var (
	headingRE   = regexp.MustCompile(`^(#{1,6}) (.*)$`)
	linkRE      = regexp.MustCompile(`\[[^\]]*\]\(([^)\s]+)`)
	codeSpanRE  = regexp.MustCompile("`[^`]*`")
	separatorRE = regexp.MustCompile(`^\s*\|?\s*:?-+:?\s*(\|\s*:?-+:?\s*)*\|?\s*$`)
)

func Root() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("doccheck: cannot locate the package source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	gomod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(gomod), "\n") {
		t := strings.TrimSpace(line)
		if !strings.HasPrefix(t, "module") {
			continue
		}
		if t != "module "+modulePath {
			return "", fmt.Errorf("doccheck: go.mod at %s declares %q", root, t)
		}
		return root, nil
	}
	return "", fmt.Errorf("doccheck: go.mod at %s has no module line", root)
}

func Load(path string) (*Doc, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(path, src), nil
}

func Parse(path string, src []byte) *Doc {
	doc := &Doc{Path: path, Lines: strings.Split(string(src), "\n")}
	for i, line := range doc.Lines {
		doc.Lines[i] = strings.TrimSuffix(line, "\r")
	}
	seen := map[string]int{}
	var heading, marker string
	inFence := false
	var fenceChar byte
	fenceLen, fenceIdx, openIdx := 0, -1, -1
	for i, line := range doc.Lines {
		lineNo := i + 1
		if inFence {
			if isFenceClose(line, fenceChar, fenceLen) {
				doc.Fences[fenceIdx].Body = strings.Join(doc.Lines[openIdx+1:i], "\n")
				inFence = false
			}
			continue
		}
		if c, n, info, ok := fenceOpen(line); ok {
			lang := ""
			if fields := strings.Fields(info); len(fields) > 0 {
				lang = fields[0]
			}
			doc.Fences = append(doc.Fences, Fence{Lang: lang, Heading: heading, Marker: marker, Line: lineNo})
			fenceIdx = len(doc.Fences) - 1
			inFence, fenceChar, fenceLen, openIdx = true, c, n, i
			continue
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		marker = strings.TrimSpace(line)
		if m := headingRE.FindStringSubmatch(line); m != nil {
			text := trimClosing(m[2])
			anchor := Anchor(text)
			n := seen[anchor]
			seen[anchor]++
			if n > 0 {
				anchor = fmt.Sprintf("%s-%d", anchor, n)
			}
			doc.Headings = append(doc.Headings, Heading{Level: len(m[1]), Text: text, Anchor: anchor, Line: lineNo})
			heading = text
		}
		for _, m := range linkRE.FindAllStringSubmatch(codeSpanRE.ReplaceAllStringFunc(line, blank), -1) {
			doc.Links = append(doc.Links, Link{Target: m[1], Line: lineNo})
		}
		if i+1 < len(doc.Lines) && strings.Contains(line, "|") && strings.Contains(doc.Lines[i+1], "|") && separatorRE.MatchString(doc.Lines[i+1]) {
			t := Table{Header: splitRow(line), Heading: heading, Line: lineNo}
			for j := i + 2; j < len(doc.Lines) && strings.TrimSpace(doc.Lines[j]) != "" && strings.Contains(doc.Lines[j], "|"); j++ {
				t.Rows = append(t.Rows, splitRow(doc.Lines[j]))
			}
			doc.Tables = append(doc.Tables, t)
		}
	}
	if inFence {
		doc.Fences[fenceIdx].Body = strings.Join(doc.Lines[openIdx+1:], "\n")
	}
	return doc
}

func Anchor(text string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(text) {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r), r == '-', r == '_':
			b.WriteRune(r)
		case r == ' ':
			b.WriteByte('-')
		}
	}
	return b.String()
}

func CheckLinks(docs []*Doc) []string {
	known := map[string]*Doc{}
	for _, d := range docs {
		known[absPath(d.Path)] = d
	}
	load := func(p string) (*Doc, error) {
		key := absPath(p)
		if d, ok := known[key]; ok {
			return d, nil
		}
		d, err := Load(p)
		if err != nil {
			return nil, err
		}
		known[key] = d
		return d, nil
	}
	var problems []string
	for _, doc := range docs {
		for _, link := range doc.Links {
			if isExternal(link.Target) {
				continue
			}
			report := func(reason string) {
				problems = append(problems, fmt.Sprintf("%s:%d: %s", doc.Path, link.Line, reason))
			}
			ref, frag, hasFrag := strings.Cut(link.Target, "#")
			refPath := doc.Path
			if ref != "" {
				refPath = filepath.Join(filepath.Dir(doc.Path), ref)
				if _, err := os.Stat(refPath); err != nil {
					report(fmt.Sprintf("broken link %q: file does not exist", link.Target))
					continue
				}
			}
			if !hasFrag || frag == "" {
				continue
			}
			if filepath.Ext(refPath) != ".md" {
				report(fmt.Sprintf("broken link %q: fragment on a non-markdown file", link.Target))
				continue
			}
			target := doc
			if ref != "" {
				d, err := load(refPath)
				if err != nil {
					report(fmt.Sprintf("broken link %q: %v", link.Target, err))
					continue
				}
				target = d
			}
			if !hasAnchor(target, frag) {
				report(fmt.Sprintf("broken link %q: no heading with anchor %q", link.Target, frag))
			}
		}
	}
	sort.Strings(problems)
	return problems
}

func fenceOpen(line string) (byte, int, string, bool) {
	t := strings.TrimLeft(line, " \t")
	if len(t) < 3 || (t[0] != '`' && t[0] != '~') {
		return 0, 0, "", false
	}
	c := t[0]
	n := 0
	for n < len(t) && t[n] == c {
		n++
	}
	if n < 3 {
		return 0, 0, "", false
	}
	return c, n, strings.TrimSpace(t[n:]), true
}

func isFenceClose(line string, c byte, n int) bool {
	t := strings.TrimSpace(line)
	run := 0
	for run < len(t) && t[run] == c {
		run++
	}
	return run >= n && strings.TrimSpace(t[run:]) == ""
}

func trimClosing(text string) string {
	t := strings.TrimSpace(text)
	u := strings.TrimRight(t, "#")
	if u == t {
		return t
	}
	if u == "" {
		return ""
	}
	if strings.HasSuffix(u, " ") || strings.HasSuffix(u, "\t") {
		return strings.TrimSpace(u)
	}
	return t
}

func splitRow(line string) []string {
	s := strings.TrimPrefix(strings.TrimSpace(line), "|")
	if strings.HasSuffix(s, "|") && !strings.HasSuffix(s, `\|`) {
		s = s[:len(s)-1]
	}
	var cells []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '|' && (i == 0 || s[i-1] != '\\') {
			cells = append(cells, strings.TrimSpace(s[start:i]))
			start = i + 1
		}
	}
	return append(cells, strings.TrimSpace(s[start:]))
}

func blank(s string) string {
	return strings.Repeat(" ", len(s))
}

func isExternal(target string) bool {
	return strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") || strings.HasPrefix(target, "mailto:")
}

func hasAnchor(d *Doc, anchor string) bool {
	for _, h := range d.Headings {
		if h.Anchor == anchor {
			return true
		}
	}
	return false
}

func absPath(p string) string {
	if abs, err := filepath.Abs(p); err == nil {
		return abs
	}
	return filepath.Clean(p)
}
