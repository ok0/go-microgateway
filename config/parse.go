package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/ok0/go-microgateway/internal/yamlx"
)

// ParseOption configures Parse and LoadFile.
type ParseOption func(*parseOptions)

type parseOptions struct {
	lookup  func(string) (string, bool)
	baseDir string
	checks  []func(*Config) []FieldError
}

// WithEnvLookup replaces the function used to resolve ${VAR} references. The default is os.LookupEnv.
func WithEnvLookup(f func(string) (string, bool)) ParseOption {
	return func(o *parseOptions) {
		o.lookup = f
	}
}

// WithBaseDir resolves relative key_file and public_key_file paths in Parse against dir instead of the working directory.
func WithBaseDir(dir string) ParseOption {
	return func(o *parseOptions) {
		o.baseDir = dir
	}
}

// WithCheck adds f to the checks Parse and LoadFile run. f runs on every document that decodes, after
// environment substitution, key loading and defaults, also when other problems were found. It must not modify
// the configuration. Its errors join the single *ValidationError, except an error whose path equals, or is nested
// under, the path of a problem already reported, so each field is reported once. f does not run when the
// document cannot be decoded at all.
func WithCheck(f func(*Config) []FieldError) ParseOption {
	return func(o *parseOptions) {
		if f != nil {
			o.checks = append(o.checks, f)
		}
	}
}

// Parse decodes, substitutes environment references in, applies defaults to, and validates a
// configuration document. Key files named by the document are always read: relative paths resolve
// against the directory set by WithBaseDir, or else against the working directory. Relative TLS file
// paths are not resolved. On failure it returns nil and a *ValidationError that lists every problem found,
// including those reported by WithCheck.
func Parse(data []byte, opts ...ParseOption) (*Config, error) {
	return load(data, "", opts)
}

// LoadFile reads and parses the configuration file at path, like Parse. Key files are always read.
// Relative paths to key files and TLS files are resolved against the directory containing path. A file
// that cannot be read is reported as a plain error, not as a *ValidationError.
func LoadFile(path string, opts ...ParseOption) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	return load(data, filepath.Dir(path), opts)
}

var (
	customDecodeRe = regexp.MustCompile(`^line (\d+): column (\d+): type (\S+): (.*)$`)
	unknownFieldRe = regexp.MustCompile(`^line (\d+): field (\S+) not found in type (\S+)`)
	mismatchRe     = regexp.MustCompile("^line (\\d+): cannot unmarshal (!!\\w+)(?: `(.*)`)? into (\\S+)$")
	lineEntryRe    = regexp.MustCompile(`^line (\d+): (.*)$`)
	lineNumRe      = regexp.MustCompile(`line (\d+)`)
	yamlLinePrefix = regexp.MustCompile(`^line \d+: `)
)

func load(data []byte, dir string, opts []ParseOption) (*Config, error) {
	o := parseOptions{lookup: os.LookupEnv}
	for _, opt := range opts {
		opt(&o)
	}
	if dir == "" {
		dir = o.baseDir
	}

	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, newValidationError([]FieldError{{Line: lineFromMsg(err.Error()), Msg: trimYAMLMsg(err.Error())}})
	}
	idx := buildIndex(&root)
	loc := newLocator(&root, reflect.TypeFor[Config]())

	cfg := &Config{}
	var errs []FieldError
	decoded := map[string]bool{}
	rootFailure := false
	if err := yamlx.DecodeStrict(data, cfg); err != nil {
		var typeErr *yaml.TypeError
		isType := errors.As(err, &typeErr)
		isMulti := errors.Is(err, yamlx.ErrMultipleDocuments)
		if !isType && !isMulti {
			return nil, newValidationError([]FieldError{{Line: lineFromMsg(err.Error()), Msg: trimYAMLMsg(err.Error())}})
		}
		if isType {
			for _, entry := range typeErr.Errors {
				fe := decodeFieldError(entry, loc)
				if fe.Path == "" {
					rootFailure = true
				} else {
					decoded[fe.Path] = true
				}
				errs = append(errs, fe)
			}
		}
		if isMulti {
			errs = append(errs, FieldError{Msg: "multiple YAML documents are not supported"})
		}
	}

	errs = append(errs, substituteEnv(cfg, o.lookup)...)
	if dir != "" {
		resolveFiles(cfg, dir)
	}
	errs = append(errs, loadKeyMaterial(cfg, dir)...)
	cfg.applyDefaults()
	if !rootFailure {
		for _, fe := range cfg.validate() {
			if !underPath(fe.Path, decoded) {
				errs = append(errs, fe)
			}
		}
	}
	if !rootFailure && len(o.checks) > 0 {
		cfg.lines = idx
		reported := make(map[string]bool, len(errs))
		for _, fe := range errs {
			if fe.Path != "" {
				reported[fe.Path] = true
			}
		}
		for _, check := range o.checks {
			for _, fe := range check(cfg) {
				if fe.Path != "" && underPath(fe.Path, reported) {
					continue
				}
				errs = append(errs, fe)
			}
		}
	}
	for i := range errs {
		if errs[i].Line == 0 && errs[i].Path != "" {
			errs[i].Line = idx.lineFor(errs[i].Path)
		}
	}
	if len(errs) > 0 {
		return nil, newValidationError(errs)
	}
	cfg.lines = idx
	return cfg, nil
}

// decodeFieldError converts one yaml.TypeError entry into a FieldError located through loc. The first rule that
// matches the entry decides the result; a rule whose node cannot be claimed reports the line only. Each claim
// marks one node, so repeated identical errors on one line each locate a distinct node. A flow mapping, flow
// sequence, alias or block collection sharing the error line is located by its own node, not by the last key
// written on that line.
func decodeFieldError(entry string, loc *locator) FieldError {
	if m := customDecodeRe.FindStringSubmatch(entry); m != nil {
		line, _ := strconv.Atoi(m[1])
		col, _ := strconv.Atoi(m[2])
		target, msg := m[3], m[4]
		r := loc.claim(
			func(r *nodeRef) bool { return at(r, line, col) },
			func(r *nodeRef) bool { return sameType(loc, r, target) },
		)
		if r == nil {
			return FieldError{Line: line, Msg: msg}
		}
		return FieldError{Path: r.path, Line: r.val.Line, Msg: msg}
	}
	if m := unknownFieldRe.FindStringSubmatch(entry); m != nil {
		line, _ := strconv.Atoi(m[1])
		name, parent := m[2], m[3]
		r := loc.claim(
			func(r *nodeRef) bool { return r.key != nil && r.key.Line == line && r.key.Value == name },
			func(r *nodeRef) bool {
				pp, _ := parentPath(r.path)
				t := loc.typeAt(pp)
				return t != nil && t.String() == parent
			},
		)
		if r == nil {
			return FieldError{Line: line, Msg: fmt.Sprintf("unknown field %q", name)}
		}
		return FieldError{Path: r.path, Line: line, Msg: fmt.Sprintf("unknown field %q", name)}
	}
	if m := mismatchRe.FindStringSubmatch(entry); m != nil {
		line, _ := strconv.Atoi(m[1])
		tag, quoted, into := m[2], m[3], m[4]
		msg := lineEntryRe.FindStringSubmatch(entry)[2]
		r := loc.claim(
			func(r *nodeRef) bool {
				n := resolved(r.val)
				return n.Line == line && valueFitsTag(n, tag, quoted)
			},
			func(r *nodeRef) bool { return sameType(loc, r, into) },
		)
		if r == nil {
			return FieldError{Line: line, Msg: msg}
		}
		return FieldError{Path: r.path, Line: r.val.Line, Msg: msg}
	}
	m := lineEntryRe.FindStringSubmatch(entry)
	if m == nil {
		return FieldError{Msg: entry}
	}
	line, _ := strconv.Atoi(m[1])
	msg := m[2]
	r := loc.claim(func(r *nodeRef) bool { return resolved(r.val).Line == line }, nil)
	if r == nil {
		r = loc.claim(func(r *nodeRef) bool { return r.key != nil && r.key.Line == line }, nil)
	}
	if r == nil {
		return FieldError{Line: line, Msg: msg}
	}
	return FieldError{Path: r.path, Line: line, Msg: msg}
}

// at reports whether the value of r starts at line and column.
func at(r *nodeRef, line, col int) bool {
	n := resolved(r.val)
	return n.Line == line && n.Column == col
}

// sameType reports whether the field at r has the Go type named want, as reflect prints it.
func sameType(loc *locator, r *nodeRef, want string) bool {
	t := loc.typeAt(r.path)
	return t != nil && t.String() == want
}

// valueFitsTag reports whether n is the node yaml.v3 names in a "cannot unmarshal" message with the given tag
// and quoted value excerpt.
func valueFitsTag(n *yaml.Node, tag, quoted string) bool {
	switch tag {
	case "!!map":
		return n.Kind == yaml.MappingNode
	case "!!seq":
		return n.Kind == yaml.SequenceNode
	}
	return n.Kind == yaml.ScalarNode && n.ShortTag() == tag && excerpt(n.Value) == quoted
}

// excerpt shortens a scalar value the way yaml.v3 does in "cannot unmarshal" messages.
func excerpt(v string) string {
	if len(v) > 10 {
		return v[:7] + "..."
	}
	return v
}

// underPath reports whether path equals, or is nested under, one of the decode error paths.
func underPath(path string, decoded map[string]bool) bool {
	for p := range decoded {
		if path == p || strings.HasPrefix(path, p+".") || strings.HasPrefix(path, p+"[") {
			return true
		}
	}
	return false
}

// resolveFiles makes relative TLS file paths absolute against dir.
func resolveFiles(c *Config, dir string) {
	absPath := func(p string) string {
		if p == "" || filepath.IsAbs(p) {
			return p
		}
		return filepath.Join(dir, p)
	}
	for i := range c.Listeners {
		if t := c.Listeners[i].TLS; t != nil {
			t.CertFile = absPath(t.CertFile)
			t.KeyFile = absPath(t.KeyFile)
		}
	}
}

func lineFromMsg(msg string) int {
	m := lineNumRe.FindStringSubmatch(msg)
	if m == nil {
		return 0
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

func trimYAMLMsg(msg string) string {
	msg = strings.TrimPrefix(msg, "yaml: ")
	return yamlLinePrefix.ReplaceAllString(msg, "")
}
