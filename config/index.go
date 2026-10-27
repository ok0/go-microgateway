package config

import (
	"fmt"
	"strings"

	"go.yaml.in/yaml/v3"
)

// lineIndex maps normalized YAML paths to the lines where they are declared.
type lineIndex struct {
	byPath map[string]int
	byLine map[int][]string
}

func buildIndex(root *yaml.Node) *lineIndex {
	x := &lineIndex{byPath: map[string]int{}, byLine: map[int][]string{}}
	x.walk(root, "")
	return x
}

func (x *lineIndex) walk(n *yaml.Node, path string) {
	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) > 0 {
			x.walk(n.Content[0], path)
		}
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			key, val := n.Content[i], n.Content[i+1]
			p := joinPath(path, key.Value)
			x.record(p, key.Line)
			x.walk(val, p)
		}
	case yaml.SequenceNode:
		for i, item := range n.Content {
			p := fmt.Sprintf("%s[%d]", path, i)
			x.record(p, item.Line)
			x.walk(item, p)
		}
	}
}

func (x *lineIndex) record(path string, line int) {
	if _, ok := x.byPath[path]; !ok {
		x.byPath[path] = line
	}
	x.byLine[line] = append(x.byLine[line], path)
}

// lineFor returns the line of path, or of its nearest declared ancestor; 0 when none is known.
func (x *lineIndex) lineFor(path string) int {
	if x == nil {
		return 0
	}
	p := path
	for {
		if l, ok := x.byPath[p]; ok {
			return l
		}
		parent, ok := parentPath(p)
		if !ok {
			return 0
		}
		p = parent
	}
}

// pathAt returns the path declared on line. When field is not empty it returns the last path on
// that line equal to field or ending in "."+field; otherwise the last path on that line. It returns
// "" when no path matches.
func (x *lineIndex) pathAt(line int, field string) string {
	if x == nil {
		return ""
	}
	paths := x.byLine[line]
	for i := len(paths) - 1; i >= 0; i-- {
		p := paths[i]
		if field == "" {
			return p
		}
		if p == field || strings.HasSuffix(p, "."+field) {
			return p
		}
	}
	return ""
}

func parentPath(p string) (string, bool) {
	if strings.HasSuffix(p, "]") {
		if i := strings.LastIndex(p, "["); i >= 0 {
			return p[:i], true
		}
		return "", false
	}
	if i := strings.LastIndex(p, "."); i >= 0 {
		return p[:i], true
	}
	return "", false
}

func joinPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "." + name
}
