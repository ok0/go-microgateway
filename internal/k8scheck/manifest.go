package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"

	"go.yaml.in/yaml/v3"
)

// Manifest is one object of a rendered manifest: its kind, its metadata.name and the decoded object.
type Manifest struct {
	Kind, Name string
	Obj        map[string]any
}

// Decode splits a multi-document YAML manifest into its objects. Documents that are empty or hold only comments are skipped; a document that is not a mapping or has no kind is an error.
func Decode(data []byte) ([]Manifest, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var out []Manifest
	for n := 1; ; n++ {
		var obj map[string]any
		err := dec.Decode(&obj)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("document %d: %w", n, err)
		}
		if obj == nil {
			continue
		}
		kind, ok := obj["kind"].(string)
		if !ok || kind == "" {
			return nil, fmt.Errorf("document %d has no kind", n)
		}
		out = append(out, Manifest{Kind: kind, Name: stringAt(obj, "metadata", "name"), Obj: obj})
	}
}

func valueAt(m map[string]any, path ...string) (any, bool) {
	var cur any = m
	for _, k := range path {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = mm[k]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

func mapAt(m map[string]any, path ...string) map[string]any {
	v, ok := valueAt(m, path...)
	if !ok {
		return nil
	}
	mm, _ := v.(map[string]any)
	return mm
}

func listAt(m map[string]any, path ...string) []any {
	v, ok := valueAt(m, path...)
	if !ok {
		return nil
	}
	l, _ := v.([]any)
	return l
}

func stringAt(m map[string]any, path ...string) string {
	v, ok := valueAt(m, path...)
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func intValue(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case uint64:
		return int(n), true
	case float64:
		if n == math.Trunc(n) {
			return int(n), true
		}
	}
	return 0, false
}

func containers(pod map[string]any, init bool) []map[string]any {
	key := "containers"
	if init {
		key = "initContainers"
	}
	var out []map[string]any
	for _, c := range listAt(pod, key) {
		if m, ok := c.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}
