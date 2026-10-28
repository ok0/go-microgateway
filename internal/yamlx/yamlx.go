// Package yamlx is the single place that decodes YAML into Go values.
package yamlx

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// ErrMultipleDocuments is returned by DecodeStrict when the input contains more than one YAML document.
var ErrMultipleDocuments = errors.New("multiple YAML documents are not supported")

// PathError is one problem found by DecodeValue; Path is dotted with [i] indexes, relative to the decoded value ("" = the value itself).
type PathError struct{ Path, Msg string }

// ValueError lists every problem DecodeValue found, in document order.
type ValueError struct{ Errors []PathError }

var (
	lineEntryPattern     = regexp.MustCompile(`^line (\d+): (.*)$`)
	unknownFieldPattern  = regexp.MustCompile(`^field (\S+) not found in type`)
	customDecoderPrefix  = regexp.MustCompile(`^column \d+: type \S+: `)
	customDecoderPattern = regexp.MustCompile(`^column (\d+): type \S+: `)
)

type pos struct{ line, col int }

// Error implements error.
func (e *ValueError) Error() string {
	parts := make([]string, 0, len(e.Errors))
	for _, pe := range e.Errors {
		if pe.Path == "" {
			parts = append(parts, pe.Msg)
			continue
		}
		parts = append(parts, pe.Path+": "+pe.Msg)
	}
	return strings.Join(parts, "; ")
}

// DecodeStrict decodes the single YAML document in data into v, rejecting unknown fields.
// Empty input leaves v untouched and returns nil. Type errors are returned as *yaml.TypeError
// after decoding continues through the whole document. If more than one document is present,
// the result includes ErrMultipleDocuments. A floating-point scalar (including 1.0, 1e3, .inf, .nan)
// is never accepted for an integer field; it is reported as a type error at its line.
func DecodeStrict(data []byte, v any) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	err := dec.Decode(v)
	if err == io.EOF {
		return nil
	}
	var typeErr *yaml.TypeError
	if err != nil && !errors.As(err, &typeErr) {
		return err
	}
	var extra yaml.Node
	multi := dec.Decode(&extra) != io.EOF
	err = mergeFloatIntoInt(data, v, err)
	if multi {
		return errors.Join(err, ErrMultipleDocuments)
	}
	return err
}

type visit struct {
	n *yaml.Node
	t reflect.Type
}

var (
	yamlUnmarshalerType   = reflect.TypeOf((*yaml.Unmarshaler)(nil)).Elem()
	legacyUnmarshalerType = reflect.TypeOf((*interface {
		UnmarshalYAML(func(any) error) error
	})(nil)).Elem()
	durationType = reflect.TypeOf(time.Duration(0))
)

func mergeFloatIntoInt(data []byte, v any, err error) error {
	rt := reflect.TypeOf(v)
	if rt == nil {
		return err
	}
	var root yaml.Node
	if yaml.NewDecoder(bytes.NewReader(data)).Decode(&root) != nil {
		return err
	}
	var extra []string
	floatIntoInt(&root, rt, map[visit]bool{}, &extra)
	if len(extra) == 0 {
		return err
	}
	var typeErr *yaml.TypeError
	errors.As(err, &typeErr)
	var entries []string
	have := map[string]int{}
	if typeErr != nil {
		entries = append(entries, typeErr.Errors...)
		for _, e := range typeErr.Errors {
			have[e]++
		}
	}
	for _, e := range extra {
		if have[e] > 0 {
			have[e]--
			continue
		}
		entries = append(entries, e)
	}
	sort.SliceStable(entries, func(i, j int) bool {
		return entryLine(entries[i]) < entryLine(entries[j])
	})
	return &yaml.TypeError{Errors: entries}
}

func entryLine(entry string) int {
	m := lineEntryPattern.FindStringSubmatch(entry)
	if m == nil {
		return 0
	}
	line, _ := strconv.Atoi(m[1])
	return line
}

func hasCustomDecoder(t reflect.Type) bool {
	p := reflect.PointerTo(t)
	return t.Implements(yamlUnmarshalerType) || p.Implements(yamlUnmarshalerType) ||
		t.Implements(legacyUnmarshalerType) || p.Implements(legacyUnmarshalerType)
}

func resolveAlias(n *yaml.Node) *yaml.Node {
	for n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	return n
}

func isMergeKey(k *yaml.Node) bool {
	return k.Kind == yaml.ScalarNode && k.Value == "<<" && (k.Tag == "" || k.Tag == "!" || k.ShortTag() == "!!merge")
}

func mergeSources(v *yaml.Node) []*yaml.Node {
	v = resolveAlias(v)
	switch v.Kind {
	case yaml.MappingNode:
		return []*yaml.Node{v}
	case yaml.SequenceNode:
		var out []*yaml.Node
		for _, item := range v.Content {
			if m := resolveAlias(item); m.Kind == yaml.MappingNode {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

func eachPair(n *yaml.Node, skip map[string]bool, fn func(k, v *yaml.Node)) {
	own := make(map[string]bool)
	for i := 0; i+1 < len(n.Content); i += 2 {
		if !isMergeKey(n.Content[i]) {
			own[n.Content[i].Value] = true
		}
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		if isMergeKey(k) {
			inner := make(map[string]bool, len(skip)+len(own))
			for key := range skip {
				inner[key] = true
			}
			for key := range own {
				inner[key] = true
			}
			for _, src := range mergeSources(v) {
				eachPair(src, inner, fn)
			}
			continue
		}
		if skip[k.Value] {
			continue
		}
		fn(k, v)
	}
}

func structFields(t reflect.Type) (map[string]reflect.Type, reflect.Type) {
	fields := make(map[string]reflect.Type)
	var catchAll reflect.Type
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("yaml")
		if tag == "-" {
			continue
		}
		if !f.IsExported() && !f.Anonymous {
			continue
		}
		name, opts, _ := strings.Cut(tag, ",")
		if hasOption(opts, "inline") {
			ft := f.Type
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			switch ft.Kind() {
			case reflect.Struct:
				sub, sc := structFields(ft)
				for k, st := range sub {
					if _, ok := fields[k]; !ok {
						fields[k] = st
					}
				}
				if sc != nil && catchAll == nil {
					catchAll = sc
				}
			case reflect.Map:
				catchAll = ft
			}
			continue
		}
		if name == "" {
			name = strings.ToLower(f.Name)
		}
		fields[name] = f.Type
	}
	return fields, catchAll
}

func hasOption(opts, name string) bool {
	for _, o := range strings.Split(opts, ",") {
		if o == name {
			return true
		}
	}
	return false
}

func quotedScalar(v string) string {
	if len(v) > 10 {
		v = v[:7] + "..."
	}
	return "`" + v + "`"
}

func floatIntoInt(n *yaml.Node, t reflect.Type, seen map[visit]bool, out *[]string) {
	key := visit{n, t}
	if seen[key] {
		return
	}
	seen[key] = true
	switch {
	case n.Kind == yaml.AliasNode:
		floatIntoInt(n.Alias, t, seen, out)
		return
	case n.Kind == yaml.DocumentNode:
		if len(n.Content) > 0 {
			floatIntoInt(n.Content[0], t, seen, out)
		}
		return
	case n.ShortTag() == "!!null":
		return
	case hasCustomDecoder(t):
		return
	case t.Kind() == reflect.Pointer:
		floatIntoInt(n, t.Elem(), seen, out)
		return
	case t == durationType:
		return
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if n.Kind == yaml.ScalarNode && n.ShortTag() == "!!float" {
			*out = append(*out, fmt.Sprintf("line %d: cannot unmarshal !!float %s into %s", n.Line, quotedScalar(n.Value), t))
		}
	case reflect.Struct:
		if n.Kind == yaml.MappingNode {
			fields, catchAll := structFields(t)
			eachPair(n, nil, func(k, v *yaml.Node) {
				if ft, ok := fields[k.Value]; ok {
					floatIntoInt(v, ft, seen, out)
					return
				}
				if catchAll != nil {
					floatIntoInt(v, catchAll.Elem(), seen, out)
				}
			})
		}
	case reflect.Map:
		if n.Kind == yaml.MappingNode {
			eachPair(n, nil, func(k, v *yaml.Node) {
				floatIntoInt(k, t.Key(), seen, out)
				floatIntoInt(v, t.Elem(), seen, out)
			})
		}
	case reflect.Slice, reflect.Array:
		if n.Kind == yaml.SequenceNode {
			for _, item := range n.Content {
				floatIntoInt(item, t.Elem(), seen, out)
			}
		}
	}
}

// DecodeValue strictly decodes src (e.g. map[string]any) into v with the same rules as DecodeStrict.
func DecodeValue(src, v any) (err error) {
	rv := reflect.ValueOf(src)
	if !rv.IsValid() || (rv.Kind() == reflect.Map && rv.Len() == 0) {
		return nil
	}
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("yamlx: cannot encode value: %v", p)
		}
	}()
	b, mErr := yaml.Marshal(src)
	if mErr != nil {
		return fmt.Errorf("yamlx: encode value: %w", mErr)
	}
	var root yaml.Node
	if uErr := yaml.Unmarshal(b, &root); uErr != nil {
		return fmt.Errorf("yamlx: encode value: %w", uErr)
	}
	lines := make(map[int]string)
	indexLines(&root, "", lines)
	at := make(map[pos]string)
	indexPositions(&root, "", at)
	err = DecodeStrict(b, v)
	if err == nil {
		return nil
	}
	var te *yaml.TypeError
	if !errors.As(err, &te) {
		return err
	}
	ve := &ValueError{}
	for _, entry := range te.Errors {
		m := lineEntryPattern.FindStringSubmatch(entry)
		if m == nil {
			ve.Errors = append(ve.Errors, PathError{Msg: entry})
			continue
		}
		line, _ := strconv.Atoi(m[1])
		path := lines[line]
		if c := customDecoderPattern.FindStringSubmatch(m[2]); c != nil {
			col, _ := strconv.Atoi(c[1])
			if p, ok := at[pos{line, col}]; ok {
				path = p
			}
		}
		msg := customDecoderPrefix.ReplaceAllString(m[2], "")
		if f := unknownFieldPattern.FindStringSubmatch(msg); f != nil {
			msg = fmt.Sprintf("unknown field %q", f[1])
		}
		ve.Errors = append(ve.Errors, PathError{Path: path, Msg: msg})
	}
	return ve
}

func indexPositions(n *yaml.Node, path string, at map[pos]string) {
	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) > 0 {
			indexPositions(n.Content[0], path, at)
		}
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			key, val := n.Content[i], n.Content[i+1]
			p := key.Value
			if path != "" {
				p = path + "." + key.Value
			}
			if _, ok := at[pos{val.Line, val.Column}]; !ok {
				at[pos{val.Line, val.Column}] = p
			}
			indexPositions(val, p, at)
		}
	case yaml.SequenceNode:
		for i, item := range n.Content {
			p := fmt.Sprintf("%s[%d]", path, i)
			if _, ok := at[pos{item.Line, item.Column}]; !ok {
				at[pos{item.Line, item.Column}] = p
			}
			indexPositions(item, p, at)
		}
	}
}

func indexLines(n *yaml.Node, path string, lines map[int]string) {
	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) > 0 {
			indexLines(n.Content[0], path, lines)
		}
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			key, val := n.Content[i], n.Content[i+1]
			p := key.Value
			if path != "" {
				p = path + "." + key.Value
			}
			lines[key.Line] = p
			indexLines(val, p, lines)
		}
	case yaml.SequenceNode:
		for i, item := range n.Content {
			p := fmt.Sprintf("%s[%d]", path, i)
			lines[item.Line] = p
			indexLines(item, p, lines)
		}
	}
}
