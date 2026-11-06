package doccheck

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// SchemaPaths returns the sorted dotted YAML key paths of the struct type t. Fields are followed recursively, slices of structs add a [] segment, inline fields add no segment and fields tagged "-" are skipped. A type with an UnmarshalYAML method, or any other non-struct value, is a leaf path.
func SchemaPaths(t reflect.Type) []string {
	set := map[string]bool{}
	walkSchema(derefType(t), "", set, map[reflect.Type]bool{})
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func walkSchema(t reflect.Type, prefix string, set map[string]bool, stack map[reflect.Type]bool) {
	if t.Kind() != reflect.Struct {
		return
	}
	stack[t] = true
	defer delete(stack, t)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		name, inline, skip := yamlName(f)
		if skip {
			continue
		}
		if name == "" && !inline {
			name = strings.ToLower(f.Name)
		}
		path := prefix
		if !inline {
			path = joinPath(prefix, name)
		}
		ft := derefType(f.Type)
		switch {
		case hasUnmarshal(ft):
			set[path] = true
		case ft.Kind() == reflect.Struct && !stack[ft]:
			walkSchema(ft, path, set, stack)
		case (ft.Kind() == reflect.Slice || ft.Kind() == reflect.Array) && isStructElem(ft, stack):
			walkSchema(derefType(ft.Elem()), path+"[]", set, stack)
		default:
			set[path] = true
		}
	}
}

func isStructElem(t reflect.Type, stack map[reflect.Type]bool) bool {
	el := derefType(t.Elem())
	return el.Kind() == reflect.Struct && !hasUnmarshal(el) && !stack[el]
}

func hasUnmarshal(t reflect.Type) bool {
	_, ok := reflect.PointerTo(t).MethodByName("UnmarshalYAML")
	return ok
}

func derefType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

func yamlName(f reflect.StructField) (name string, inline, skip bool) {
	tag := f.Tag.Get("yaml")
	if tag == "-" {
		return "", false, true
	}
	parts := strings.Split(tag, ",")
	for _, opt := range parts[1:] {
		if opt == "inline" {
			inline = true
		}
	}
	return parts[0], inline, false
}

func joinPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "." + name
}

// Values returns the reflected values found at the dotted YAML path of v, where a [] segment expands each element of a slice or array. A nil pointer that ends the path is returned as it is; a nil pointer before the end of the path yields no value for that branch.
func Values(v reflect.Value, path string) []reflect.Value {
	cur := []reflect.Value{v}
	for _, seg := range strings.Split(path, ".") {
		name := strings.TrimSuffix(seg, "[]")
		many := strings.HasSuffix(seg, "[]")
		var next []reflect.Value
		for _, c := range cur {
			c = derefValue(c)
			if !c.IsValid() || c.Kind() != reflect.Struct {
				continue
			}
			f, ok := fieldByYAML(c, name)
			if !ok {
				continue
			}
			if !many {
				next = append(next, f)
				continue
			}
			if f.Kind() != reflect.Slice && f.Kind() != reflect.Array {
				continue
			}
			for j := 0; j < f.Len(); j++ {
				next = append(next, f.Index(j))
			}
		}
		cur = next
	}
	return cur
}

func derefValue(v reflect.Value) reflect.Value {
	for v.IsValid() && v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	return v
}

func fieldByYAML(v reflect.Value, name string) (reflect.Value, bool) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		fieldName, inline, skip := yamlName(f)
		if skip {
			continue
		}
		if inline {
			sub := derefValue(v.Field(i))
			if sub.IsValid() && sub.Kind() == reflect.Struct {
				if found, ok := fieldByYAML(sub, name); ok {
					return found, true
				}
			}
			continue
		}
		if fieldName == "" {
			fieldName = strings.ToLower(f.Name)
		}
		if fieldName == name {
			return v.Field(i), true
		}
	}
	return reflect.Value{}, false
}

// CompareKeys compares the schema key paths with the documented key paths and returns the sorted problems: schema keys missing from the documentation, documented keys absent from the schema, and keys documented more than once.
func CompareKeys(schema, documented []string) []string {
	inSchema := map[string]bool{}
	for _, p := range schema {
		inSchema[p] = true
	}
	count := map[string]int{}
	for _, p := range documented {
		count[p]++
	}
	var problems []string
	for p := range inSchema {
		if count[p] == 0 {
			problems = append(problems, fmt.Sprintf("undocumented key %s", p))
		}
	}
	for p, n := range count {
		if !inSchema[p] {
			problems = append(problems, fmt.Sprintf("documented key %s is not in the schema", p))
		}
		if n > 1 {
			problems = append(problems, fmt.Sprintf("key %s documented more than once", p))
		}
	}
	sort.Strings(problems)
	return problems
}
