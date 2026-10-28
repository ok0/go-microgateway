package config

import (
	"fmt"
	"reflect"
	"strings"

	"go.yaml.in/yaml/v3"
)

// lineIndex maps normalized YAML paths to the lines where they are declared.
type lineIndex struct {
	byPath map[string]int
}

func buildIndex(root *yaml.Node) *lineIndex {
	x := &lineIndex{byPath: map[string]int{}}
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

// nodeRef is one mapping entry or sequence item of the parsed document, addressed by its normalized path.
// key is nil for sequence items.
type nodeRef struct {
	path string
	key  *yaml.Node
	val  *yaml.Node
	used bool
}

// locator resolves decode error entries to the document nodes they describe. It exists only while a document
// is loaded and is never stored in a Config.
type locator struct {
	refs   []*nodeRef
	schema reflect.Type
}

// newLocator records every mapping entry and sequence item under root in document (pre-)order. Paths follow
// the same rules as buildIndex. schema is the type the document decodes into.
func newLocator(root *yaml.Node, schema reflect.Type) *locator {
	l := &locator{schema: schema}
	l.walk(root, "")
	return l
}

func (l *locator) walk(n *yaml.Node, path string) {
	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) > 0 {
			l.walk(n.Content[0], path)
		}
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			key, val := n.Content[i], n.Content[i+1]
			p := joinPath(path, key.Value)
			l.refs = append(l.refs, &nodeRef{path: p, key: key, val: val})
			l.walk(val, p)
		}
	case yaml.SequenceNode:
		for i, item := range n.Content {
			p := fmt.Sprintf("%s[%d]", path, i)
			l.refs = append(l.refs, &nodeRef{path: p, val: item})
			l.walk(item, p)
		}
	}
}

// resolved follows aliases until it reaches the node that holds the value.
func resolved(n *yaml.Node) *yaml.Node {
	for n != nil && n.Kind == yaml.AliasNode && n.Alias != nil {
		n = n.Alias
	}
	return n
}

// typeAt returns the Go type of the field at path in the schema, with pointers dereferenced, or nil when the
// path does not name a typed position. The empty path names the schema itself.
func (l *locator) typeAt(path string) reflect.Type {
	t := deref(l.schema)
	if path == "" {
		return t
	}
	for _, seg := range strings.Split(path, ".") {
		name, indexes, _ := strings.Cut(seg, "[")
		if name != "" {
			if t == nil {
				return nil
			}
			switch t.Kind() {
			case reflect.Struct:
				f, ok := yamlField(t, name)
				if !ok {
					return nil
				}
				t = deref(f.Type)
			case reflect.Map:
				t = deref(t.Elem())
			default:
				return nil
			}
		}
		if indexes != "" {
			for range strings.Count(seg, "[") {
				if t == nil || (t.Kind() != reflect.Slice && t.Kind() != reflect.Array) {
					return nil
				}
				t = deref(t.Elem())
			}
		}
	}
	return t
}

// yamlField returns the exported field of struct type t whose YAML name is name.
func yamlField(t reflect.Type, name string) (reflect.StructField, bool) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.IsExported() && yamlName(f) == name {
			return f, true
		}
	}
	return reflect.StructField{}, false
}

func deref(t reflect.Type) reflect.Type {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

// claim marks and returns the first unused reference that match accepts, in document order. When typeOK is not
// nil and some candidate passes it, only the candidates passing it are considered. It returns nil when no
// candidate remains.
func (l *locator) claim(match func(*nodeRef) bool, typeOK func(*nodeRef) bool) *nodeRef {
	var cands []*nodeRef
	for _, r := range l.refs {
		if !r.used && match(r) {
			cands = append(cands, r)
		}
	}
	if typeOK != nil {
		var typed []*nodeRef
		for _, r := range cands {
			if typeOK(r) {
				typed = append(typed, r)
			}
		}
		if len(typed) > 0 {
			cands = typed
		}
	}
	if len(cands) == 0 {
		return nil
	}
	cands[0].used = true
	return cands[0]
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
