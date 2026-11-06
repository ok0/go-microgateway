// Package leakcheck provides test helpers that prove secret values never reach formatted, logged or encoded output. It imports only the standard library and go.yaml.in/yaml/v3.
package leakcheck

import (
	"bytes"
	"encoding"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// Leak records one secret form found in one rendering. Rendering is the label from Renderings and Form is the leaked text.
type Leak struct{ Rendering, Form string }

const verbs = "bcdeEfFgGoOpqstTUvxXwz"

var flags = []string{"", "+", "#", "-", " ", "0", "+#", "#0", "10", "-10", ".3", "10.3", "#10.3", "+ 0"}

var (
	stringType = reflect.TypeFor[string]()
)

// Forms returns the distinct encodings of secret that a rendering could contain, including hex, base64,
// quoted and URL-escaped forms. Empty forms are omitted.
func Forms(secret string) []string {
	b := []byte(secret)
	h := hex.EncodeToString(b)
	quoted := strconv.Quote(secret)
	candidates := []string{
		secret,
		h,
		strings.ToUpper(h),
		fmt.Sprintf("% x", b),
		fmt.Sprintf("% X", b),
		quoted[1 : len(quoted)-1],
		base64.StdEncoding.EncodeToString(b),
		base64.RawStdEncoding.EncodeToString(b),
		base64.URLEncoding.EncodeToString(b),
		base64.RawURLEncoding.EncodeToString(b),
		url.QueryEscape(secret),
		url.PathEscape(secret),
	}
	var out []string
	for _, c := range candidates {
		if c != "" && !slices.Contains(out, c) {
			out = append(out, c)
		}
	}
	return out
}

// Renderings formats v through every wrapper, verb, flag, and encoder, and returns each output keyed by its rendering label.
func Renderings(v any) map[string]string {
	out := make(map[string]string)
	for label, w := range wrappers(reflect.ValueOf(v)) {
		for _, flag := range flags {
			for _, verb := range verbs {
				out[label+"|%"+flag+string(verb)] = fmt.Sprintf("%"+flag+string(verb), w)
			}
		}
		out[label+"|Sprint"] = fmt.Sprint(w)
		out[label+"|Sprintln"] = fmt.Sprintln(w)
		out[label+"|Errorf"] = fmt.Errorf("%v", w).Error()
		out[label+"|json"] = jsonOutput(w)
		out[label+"|yaml"] = yamlOutput(w)
		if tm, ok := w.(encoding.TextMarshaler); ok {
			out[label+"|text"] = textOutput(tm)
		}
		out[label+"|slog-json"] = slogOutput(jsonHandler, false, w)
		out[label+"|slog-text"] = slogOutput(textHandler, false, w)
		out[label+"|slog-json-group"] = slogOutput(jsonHandler, true, w)
		out[label+"|slog-text-group"] = slogOutput(textHandler, true, w)
	}
	return out
}

// Find returns every Leak in which a form of one of secrets appears in a rendering of v, sorted by rendering label.
// It returns nil when no secret leaks.
func Find(v any, secrets ...string) []Leak {
	outs := Renderings(v)
	var leaks []Leak
	for _, label := range slices.Sorted(maps.Keys(outs)) {
		for _, s := range secrets {
			for _, f := range Forms(s) {
				if !strings.Contains(outs[label], f) {
					continue
				}
				l := Leak{Rendering: label, Form: f}
				if !slices.Contains(leaks, l) {
					leaks = append(leaks, l)
				}
			}
		}
	}
	return leaks
}

// Check reports an error on t for each Leak that Find returns for v, prefixed by name.
func Check(t testing.TB, name string, v any, secrets ...string) {
	t.Helper()
	for _, l := range Find(v, secrets...) {
		t.Errorf("%s: %s reveals a secret (form %q)", name, l.Rendering, l.Form)
	}
}

// Fill returns a value of typ with exported fields, elements, keys and values filled in. leaf supplies the value
// for every type it accepts; a type that recurs on the current path is left zero.
func Fill(typ reflect.Type, leaf func(reflect.Type) (reflect.Value, bool)) reflect.Value {
	return fill(typ, leaf, map[reflect.Type]bool{})
}

// Reaches reports whether a value of typ can contain a value of one of targets, following pointers, slices,
// arrays, maps and struct fields.
func Reaches(typ reflect.Type, targets ...reflect.Type) bool {
	return reaches(typ, targets, map[reflect.Type]bool{})
}

// Types returns the named struct types reachable from root. root comes first when it is one of them; the rest are sorted by name.
func Types(root reflect.Type) []reflect.Type {
	seen := map[reflect.Type]bool{}
	var named []reflect.Type
	var walk func(t reflect.Type)
	walk = func(t reflect.Type) {
		if seen[t] {
			return
		}
		seen[t] = true
		switch t.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			walk(t.Elem())
		case reflect.Map:
			walk(t.Key())
			walk(t.Elem())
		case reflect.Struct:
			if t.Name() != "" {
				named = append(named, t)
			}
			for i := range t.NumField() {
				walk(t.Field(i).Type)
			}
		}
	}
	walk(root)
	slices.SortFunc(named, func(a, b reflect.Type) int {
		return strings.Compare(a.String(), b.String())
	})
	var out []reflect.Type
	if slices.Contains(named, root) {
		out = append(out, root)
	}
	for _, t := range named {
		if t != root {
			out = append(out, t)
		}
	}
	return out
}

// ExportedTypes returns the sorted, de-duplicated names of the exported type declarations in the non-test Go files of dir.
// It returns an error when dir cannot be read or a file in it cannot be parsed.
func ExportedTypes(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	var names []string
	for _, e := range entries {
		name := e.Name()
		if !e.Type().IsRegular() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}
			for _, spec := range gd.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if ok && ast.IsExported(ts.Name.Name) {
					names = append(names, ts.Name.Name)
				}
			}
		}
	}
	slices.Sort(names)
	return slices.Compact(names), nil
}

func wrappers(rv reflect.Value) map[string]any {
	T := rv.Type()

	ptr := reflect.New(T)
	ptr.Elem().Set(rv)

	slice := reflect.MakeSlice(reflect.SliceOf(T), 1, 1)
	slice.Index(0).Set(rv)

	array := reflect.New(reflect.ArrayOf(1, T)).Elem()
	array.Index(0).Set(rv)

	mp := reflect.MakeMap(reflect.MapOf(stringType, T))
	mp.SetMapIndex(reflect.ValueOf("k"), rv)

	ptrslice := reflect.MakeSlice(reflect.SliceOf(ptr.Type()), 1, 1)
	ptrslice.Index(0).Set(ptr)

	field := reflect.New(reflect.StructOf([]reflect.StructField{{Name: "F", Type: T}})).Elem()
	field.Field(0).Set(rv)

	ptrStruct := reflect.StructOf([]reflect.StructField{{Name: "P", Type: ptr.Type()}})

	ptrfield := reflect.New(ptrStruct).Elem()
	ptrfield.Field(0).Set(ptr)

	ptrfieldptr := reflect.New(ptrStruct)
	ptrfieldptr.Elem().Field(0).Set(ptr)

	return map[string]any{
		"val":         rv.Interface(),
		"ptr":         ptr.Interface(),
		"slice":       slice.Interface(),
		"array":       array.Interface(),
		"map":         mp.Interface(),
		"ptrslice":    ptrslice.Interface(),
		"field":       field.Interface(),
		"ptrfield":    ptrfield.Interface(),
		"any":         map[string]any{"k": rv.Interface()},
		"ptrfieldptr": ptrfieldptr.Interface(),
	}
}

func jsonOutput(w any) string {
	b, err := json.Marshal(w)
	if err != nil {
		return err.Error()
	}
	return string(b)
}

func yamlOutput(w any) (out string) {
	defer func() {
		if recover() != nil {
			out = "panic"
		}
	}()
	b, err := yaml.Marshal(w)
	if err != nil {
		return err.Error()
	}
	return string(b)
}

func textOutput(tm encoding.TextMarshaler) string {
	b, err := tm.MarshalText()
	if err != nil {
		return err.Error()
	}
	return string(b)
}

func jsonHandler(w io.Writer) slog.Handler {
	return slog.NewJSONHandler(w, nil)
}

func textHandler(w io.Writer) slog.Handler {
	return slog.NewTextHandler(w, nil)
}

func slogOutput(newHandler func(io.Writer) slog.Handler, group bool, w any) string {
	var buf bytes.Buffer
	logger := slog.New(newHandler(&buf))
	if group {
		logger.Info("m", slog.Group("g", "v", w))
	} else {
		logger.Info("m", "v", w)
	}
	return buf.String()
}

func fill(typ reflect.Type, leaf func(reflect.Type) (reflect.Value, bool), stack map[reflect.Type]bool) reflect.Value {
	if v, ok := leaf(typ); ok {
		return v
	}
	if stack[typ] {
		return reflect.Zero(typ)
	}
	stack[typ] = true
	defer delete(stack, typ)

	v := reflect.New(typ).Elem()
	switch typ.Kind() {
	case reflect.Pointer:
		v = reflect.New(typ.Elem())
		v.Elem().Set(fill(typ.Elem(), leaf, stack))
	case reflect.Struct:
		for i := range typ.NumField() {
			if f := typ.Field(i); f.IsExported() {
				v.Field(i).Set(fill(f.Type, leaf, stack))
			}
		}
	case reflect.Slice:
		v = reflect.MakeSlice(typ, 1, 1)
		v.Index(0).Set(fill(typ.Elem(), leaf, stack))
	case reflect.Array:
		for i := range typ.Len() {
			v.Index(i).Set(fill(typ.Elem(), leaf, stack))
		}
	case reflect.Map:
		v = reflect.MakeMap(typ)
		v.SetMapIndex(fill(typ.Key(), leaf, stack), fill(typ.Elem(), leaf, stack))
	case reflect.String:
		v.SetString("x")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(7)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		v.SetUint(7)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(7.5)
	default:
		return reflect.Zero(typ)
	}
	return v
}

// Holds reports whether a string reachable from v, including the bytes of byte slices and arrays, contains one of secrets.
func Holds(v any, secrets ...string) bool {
	return holds(reflect.ValueOf(v), secrets, map[uintptr]bool{})
}

func holds(rv reflect.Value, secrets []string, seen map[uintptr]bool) bool {
	if !rv.IsValid() {
		return false
	}
	switch rv.Kind() {
	case reflect.String:
		for _, s := range secrets {
			if strings.Contains(rv.String(), s) {
				return true
			}
		}
	case reflect.Pointer:
		if rv.IsNil() || seen[rv.Pointer()] {
			return false
		}
		seen[rv.Pointer()] = true
		return holds(rv.Elem(), secrets, seen)
	case reflect.Interface:
		if rv.IsNil() {
			return false
		}
		return holds(rv.Elem(), secrets, seen)
	case reflect.Struct:
		for i := range rv.NumField() {
			if holds(rv.Field(i), secrets, seen) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			b := make([]byte, rv.Len())
			for i := range b {
				b[i] = byte(rv.Index(i).Uint())
			}
			for _, s := range secrets {
				if strings.Contains(string(b), s) {
					return true
				}
			}
			return false
		}
		for i := range rv.Len() {
			if holds(rv.Index(i), secrets, seen) {
				return true
			}
		}
	case reflect.Map:
		iter := rv.MapRange()
		for iter.Next() {
			if holds(iter.Key(), secrets, seen) || holds(iter.Value(), secrets, seen) {
				return true
			}
		}
	}
	return false
}

func reaches(typ reflect.Type, targets []reflect.Type, seen map[reflect.Type]bool) bool {
	if slices.Contains(targets, typ) {
		return true
	}
	if seen[typ] {
		return false
	}
	seen[typ] = true
	switch typ.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		return reaches(typ.Elem(), targets, seen)
	case reflect.Map:
		return reaches(typ.Key(), targets, seen) || reaches(typ.Elem(), targets, seen)
	case reflect.Struct:
		for i := range typ.NumField() {
			if reaches(typ.Field(i).Type, targets, seen) {
				return true
			}
		}
	}
	return false
}
