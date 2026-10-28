package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/ok0/lychgate/internal/yamlx"
)

// ParseOption configures Parse and LoadFile.
type ParseOption func(*parseOptions)

type parseOptions struct {
	lookup  func(string) (string, bool)
	baseDir string
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

// Parse decodes, substitutes environment references in, applies defaults to, and validates a
// configuration document. Key files named by the document are always read: relative paths resolve
// against the directory set by WithBaseDir, or else against the working directory. Relative TLS file
// paths are not resolved. On failure it returns nil and a *ValidationError that lists every problem found.
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
	typeLineRe     = regexp.MustCompile(`^line (\d+): (.*)$`)
	unknownFieldRe = regexp.MustCompile(`^field (\S+) not found in type`)
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
				fe := decodeFieldError(entry, idx)
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

// decodeFieldError converts one yaml.TypeError entry into a FieldError located through idx.
func decodeFieldError(entry string, idx *lineIndex) FieldError {
	m := typeLineRe.FindStringSubmatch(entry)
	if m == nil {
		return FieldError{Msg: entry}
	}
	line, _ := strconv.Atoi(m[1])
	msg := m[2]
	if u := unknownFieldRe.FindStringSubmatch(msg); u != nil {
		name := u[1]
		return FieldError{Path: idx.pathAt(line, name), Line: line, Msg: fmt.Sprintf("unknown field %q", name)}
	}
	return FieldError{Path: idx.pathAt(line, ""), Line: line, Msg: msg}
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
