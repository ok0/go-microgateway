package config

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

var varName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// expand substitutes environment references in s and returns the result with any problems found.
//
// Grammar:
//   - "$$" produces a literal "$".
//   - "${NAME}" produces the value of NAME. NAME must be set; an empty value produces "".
//   - "${NAME:-DEFAULT}" produces the value of NAME when it is set and non-empty, otherwise DEFAULT,
//     which may itself contain references. The default is split at the first ":-".
//   - NAME must match ^[A-Za-z_][A-Za-z0-9_]*$.
//   - Braces nest: the reference ends at the matching "}". Without a matching "}" the input is
//     returned unchanged with an "unterminated" problem.
//   - Any other "$" is literal.
//
// Substituted values are inserted verbatim and are never expanded again.
func expand(s string, lookup func(string) (string, bool)) (string, []string) {
	if !strings.Contains(s, "$") {
		return s, nil
	}
	var b strings.Builder
	var problems []string
	for i := 0; i < len(s); {
		if s[i] != '$' {
			b.WriteByte(s[i])
			i++
			continue
		}
		switch {
		case i+1 < len(s) && s[i+1] == '$':
			b.WriteByte('$')
			i += 2
		case i+1 < len(s) && s[i+1] == '{':
			end := closingBrace(s, i+2)
			if end < 0 {
				return s, []string{`unterminated "${"`}
			}
			val, ps := resolveRef(s[i+2:end], lookup)
			b.WriteString(val)
			problems = append(problems, ps...)
			i = end + 1
		default:
			b.WriteByte('$')
			i++
		}
	}
	return b.String(), problems
}

func resolveRef(ref string, lookup func(string) (string, bool)) (string, []string) {
	name, def, hasDef := strings.Cut(ref, ":-")
	if !varName.MatchString(name) {
		return "", []string{fmt.Sprintf("invalid variable name %q", name)}
	}
	v, ok := lookup(name)
	switch {
	case ok && v != "":
		return v, nil
	case hasDef:
		return expand(def, lookup)
	case ok:
		return "", nil
	}
	return "", []string{fmt.Sprintf("environment variable %q is not set", name)}
}

func closingBrace(s string, start int) int {
	depth := 1
	for j := start; j < len(s); j++ {
		switch s[j] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return j
			}
		}
	}
	return -1
}

// substituteEnv applies expand to every string in *c: string struct fields, string slice elements,
// map[string]string values and strings inside map[string]any and []any values. YAML keys, numbers,
// booleans and Duration values are not touched, and map keys are never substituted. Each problem
// becomes a FieldError at the path of the value; a value with a problem is left unchanged.
func substituteEnv(c *Config, lookup func(string) (string, bool)) []FieldError {
	var errs []FieldError
	substituteValue(reflect.ValueOf(c).Elem(), "", lookup, &errs)
	return errs
}

func substituteValue(v reflect.Value, path string, lookup func(string) (string, bool), errs *[]FieldError) {
	switch v.Kind() {
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			if !t.Field(i).IsExported() {
				continue
			}
			substituteValue(v.Field(i), joinPath(path, yamlName(t.Field(i))), lookup, errs)
		}
	case reflect.Pointer:
		if !v.IsNil() {
			substituteValue(v.Elem(), path, lookup, errs)
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			substituteValue(v.Index(i), fmt.Sprintf("%s[%d]", path, i), lookup, errs)
		}
	case reflect.Map:
		if v.IsNil() {
			return
		}
		iter := v.MapRange()
		for iter.Next() {
			elem := reflect.New(v.Type().Elem()).Elem()
			elem.Set(iter.Value())
			substituteValue(elem, path+"."+fmt.Sprint(iter.Key().Interface()), lookup, errs)
			v.SetMapIndex(iter.Key(), elem)
		}
	case reflect.Interface:
		if v.IsNil() {
			return
		}
		elem := reflect.New(v.Elem().Type()).Elem()
		elem.Set(v.Elem())
		substituteValue(elem, path, lookup, errs)
		v.Set(elem)
	case reflect.String:
		s := v.String()
		out, problems := expand(s, lookup)
		if len(problems) > 0 {
			for _, p := range problems {
				*errs = append(*errs, FieldError{Path: path, Msg: p})
			}
			return
		}
		if out != s {
			v.SetString(out)
		}
	}
}

func yamlName(f reflect.StructField) string {
	name, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
	if name == "" {
		return strings.ToLower(f.Name)
	}
	return name
}
