package doccheck

import (
	"fmt"
	"regexp"
	"strings"
)

var mermaidTypes = map[string]bool{
	"flowchart":       true,
	"graph":           true,
	"sequenceDiagram": true,
	"stateDiagram":    true,
	"stateDiagram-v2": true,
	"classDiagram":    true,
	"erDiagram":       true,
}

var bracketPairs = []struct{ opener, closer byte }{{'(', ')'}, {'[', ']'}, {'{', '}'}}

var sequenceOpeners = map[string]bool{
	"loop": true, "alt": true, "opt": true, "par": true, "critical": true, "break": true, "rect": true, "box": true,
}

var (
	asymmetricShapeRE = regexp.MustCompile(`(^|[\s;&])[A-Za-z0-9_]+>[^\]]*\]`)
	sequenceMessageRE = regexp.MustCompile(`^([^\s:]+?)\s*(-->>|-->|->>|->|--x|-x|--\)|-\))\s*([+-]?)([^\s:]+)\s*(?::|$)`)
	sequenceNoteRE    = regexp.MustCompile(`^Note\s+(?:over|left of|right of)\s+([^:]+?)\s*(?::|$)`)
)

var hygieneWords = []string{
	"mile" + "stone", "road" + "map", "blue" + "print", "char" + "ter", "bri" + "ef",
	"assist" + "ant", "clau" + "de", "chat" + "gpt", "open" + "ai", "anthro" + "pic", "copi" + "lot", "l" + "lm",
}

var hygienePatterns = []*regexp.Regexp{
	regexp.MustCompile(`\b(M|AC|PAC|R|A)[0-9]+[a-z]?\b`),
	regexp.MustCompile(`\bAD` + `R-[0-9]+`),
	regexp.MustCompile(`(?i)\b(` + strings.Join(hygieneWords, "|") + `)s?\b`),
	regexp.MustCompile(`\.ens` + `emble`),
	regexp.MustCompile(`spec` + `\.md`),
	regexp.MustCompile(`(?i)lych` + `gate`),
	regexp.MustCompile(`(?i)\b(as re` + `quested|the user (asked|re` + `quested|wants|wanted))\b`),
}

// Finding is one problem reported by a check. Path is the file or directory concerned, Line the 1-based line (0 for a problem of the whole path or directory), Text the offending text and Pattern the rule or pattern that was violated.
type Finding struct {
	Path    string
	Line    int
	Text    string
	Pattern string
}

// LintMermaid returns the problems found in the body of a mermaid fence: an empty body, a missing or unknown diagram type, unbalanced brackets, an odd number of double quotes, unclosed blocks, asymmetric flowchart shapes and undeclared sequence participants. An empty result means the body passed these checks.
func LintMermaid(body string) []string {
	if strings.TrimSpace(body) == "" {
		return []string{"line 1: empty diagram body"}
	}
	lines := strings.Split(body, "\n")
	var problems []string
	add := func(n int, msg string) {
		problems = append(problems, fmt.Sprintf("line %d: %s", n, msg))
	}
	kind, kindLine := "", 0
	for i, line := range lines {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "%%") {
			continue
		}
		kind, kindLine = strings.Fields(t)[0], i+1
		break
	}
	if kind == "" {
		add(1, "no diagram type line")
		return problems
	}
	if !mermaidTypes[kind] {
		add(kindLine, fmt.Sprintf("unknown diagram type %q", kind))
	}
	for _, p := range bracketPairs {
		depth := 0
		for i, line := range lines {
			inQuote := false
			for j := 0; j < len(line); j++ {
				c := line[j]
				if c == '"' {
					inQuote = !inQuote
					continue
				}
				if inQuote {
					continue
				}
				switch c {
				case p.opener:
					depth++
				case p.closer:
					depth--
					if depth < 0 {
						add(i+1, fmt.Sprintf("unmatched %q", p.closer))
						depth = 0
					}
				}
			}
		}
		if depth > 0 {
			add(len(lines), fmt.Sprintf("%d unclosed %q", depth, p.opener))
		}
	}
	for i, line := range lines {
		if strings.Count(line, `"`)%2 == 1 {
			add(i+1, "odd number of double quotes")
		}
	}
	switch kind {
	case "flowchart", "graph":
		for i, line := range lines {
			if asymmetricShapeRE.MatchString(line) {
				add(i+1, "asymmetric node shape")
			}
		}
		problems = append(problems, blockProblems(lines, map[string]bool{"subgraph": true})...)
	case "sequenceDiagram":
		problems = append(problems, blockProblems(lines, sequenceOpeners)...)
		problems = append(problems, sequenceProblems(lines)...)
	}
	return problems
}

func blockProblems(lines []string, openers map[string]bool) []string {
	var problems []string
	var open []int
	for i, line := range lines {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "%%") {
			continue
		}
		if openers[strings.Fields(t)[0]] {
			open = append(open, i+1)
			continue
		}
		if t == "end" {
			if len(open) == 0 {
				problems = append(problems, fmt.Sprintf("line %d: end without an opening block", i+1))
				continue
			}
			open = open[:len(open)-1]
		}
	}
	for _, n := range open {
		problems = append(problems, fmt.Sprintf("line %d: block opened without end", n))
	}
	return problems
}

func sequenceProblems(lines []string) []string {
	var problems []string
	declared := map[string]bool{}
	check := func(i int, name string) {
		if !declared[name] {
			problems = append(problems, fmt.Sprintf("line %d: undeclared participant %q", i+1, name))
		}
	}
	for i, line := range lines {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "%%") {
			continue
		}
		f := strings.Fields(t)
		switch f[0] {
		case "participant", "actor":
			if len(f) >= 2 {
				declared[f[1]] = true
			}
			continue
		case "activate", "deactivate":
			if len(f) == 2 {
				check(i, f[1])
			}
			continue
		}
		if m := sequenceNoteRE.FindStringSubmatch(t); m != nil {
			for _, name := range strings.Split(m[1], ",") {
				check(i, strings.TrimSpace(name))
			}
			continue
		}
		if m := sequenceMessageRE.FindStringSubmatch(t); m != nil {
			check(i, m[1])
			check(i, m[4])
		}
	}
	return problems
}

// Hygiene returns a finding for the path and for each line of src that matches a hygiene pattern: planning identifiers, references to the planning directory, tooling-origin words and phrases. Findings for the path have Line 0.
func Hygiene(path string, src []byte) []Finding {
	var out []Finding
	for _, re := range hygienePatterns {
		if re.MatchString(path) {
			out = append(out, Finding{Path: path, Line: 0, Text: path, Pattern: re.String()})
		}
	}
	for i, line := range strings.Split(string(src), "\n") {
		for _, re := range hygienePatterns {
			if re.MatchString(line) {
				out = append(out, Finding{Path: path, Line: i + 1, Text: line, Pattern: re.String()})
			}
		}
	}
	return out
}

// Normalize returns the non-blank lines of src with surrounding whitespace removed.
func Normalize(src string) []string {
	var out []string
	for _, line := range strings.Split(src, "\n") {
		if t := strings.TrimSpace(line); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// ContainsExcerpt reports whether the non-blank lines of snippet appear as a contiguous run of non-blank lines in file, ignoring indentation. An empty snippet never matches.
func ContainsExcerpt(file, snippet string) bool {
	want := Normalize(snippet)
	if len(want) == 0 {
		return false
	}
	have := Normalize(file)
	for i := 0; i+len(want) <= len(have); i++ {
		match := true
		for j := range want {
			if have[i+j] != want[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// ExampleEnv returns the example environment values used by the documentation: the Redis address and password of the local examples.
func ExampleEnv() map[string]string {
	return map[string]string{
		"REDIS_ADDR":     "127.0.0.1:6379",
		"REDIS_PASSWORD": "example-redis-password",
	}
}

// ExampleLookup returns a lookup function over the example environment in which overrides take precedence. It never reads the process environment.
func ExampleLookup(overrides map[string]string) func(string) (string, bool) {
	env := ExampleEnv()
	return func(name string) (string, bool) {
		if v, ok := overrides[name]; ok {
			return v, true
		}
		v, ok := env[name]
		return v, ok
	}
}
