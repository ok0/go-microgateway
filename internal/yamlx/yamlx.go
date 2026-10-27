// Package yamlx is the single place that decodes YAML into Go values.
package yamlx

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// ErrMultipleDocuments is returned by DecodeStrict when the input contains more than one YAML document.
var ErrMultipleDocuments = errors.New("multiple YAML documents are not supported")

// PathError is one problem found by DecodeValue; Path is dotted with [i] indexes, relative to the decoded value ("" = the value itself).
type PathError struct{ Path, Msg string }

// ValueError lists every problem DecodeValue found, in document order.
type ValueError struct{ Errors []PathError }

var (
	lineEntryPattern    = regexp.MustCompile(`^line (\d+): (.*)$`)
	unknownFieldPattern = regexp.MustCompile(`^field (\S+) not found in type`)
)

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
// the result includes ErrMultipleDocuments.
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
	if err2 := dec.Decode(&extra); err2 != io.EOF {
		return errors.Join(err, ErrMultipleDocuments)
	}
	return err
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
		msg := m[2]
		if f := unknownFieldPattern.FindStringSubmatch(msg); f != nil {
			msg = fmt.Sprintf("unknown field %q", f[1])
		}
		ve.Errors = append(ve.Errors, PathError{Path: lines[line], Msg: msg})
	}
	return ve
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
