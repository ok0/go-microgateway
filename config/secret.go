package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strconv"

	"go.yaml.in/yaml/v3"
)

// Redacted is the text shown in place of a secret value by every formatting and encoding path.
const Redacted = "REDACTED"

// Secret holds a credential. Every formatting and encoding path shows "REDACTED" (empty stays empty); only Reveal returns the value.
type Secret struct{ b *secretBox }

type secretBox struct{ v string }

// NewSecret returns a Secret holding v. An empty v yields the zero Secret.
func NewSecret(v string) Secret {
	if v == "" {
		return Secret{}
	}
	return Secret{&secretBox{v}}
}

// Reveal returns the secret value, or "" for the zero Secret.
func (s Secret) Reveal() string {
	if s.b == nil {
		return ""
	}
	return s.b.v
}

// IsZero reports whether s holds no value.
func (s Secret) IsZero() bool {
	return s.b == nil
}

// String returns Redacted for a held value and "" for the zero Secret.
func (s Secret) String() string {
	if s.b == nil {
		return ""
	}
	return Redacted
}

// GoString returns the quoted form of String, so %#v never shows the value.
func (s Secret) GoString() string {
	return strconv.Quote(s.String())
}

// Format writes String using the verb and flags given to it.
func (s Secret) Format(f fmt.State, verb rune) {
	fmt.Fprintf(f, fmt.FormatString(f, verb), s.String())
}

// LogValue returns String as a slog value.
func (s Secret) LogValue() slog.Value {
	return slog.StringValue(s.String())
}

// MarshalJSON encodes String as a JSON string.
func (s Secret) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// MarshalYAML returns String for YAML encoding.
func (s Secret) MarshalYAML() (any, error) {
	return s.String(), nil
}

// MarshalText returns String as text.
func (s Secret) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

// UnmarshalYAML decodes a YAML string scalar into s. Any other node kind is an error.
func (s *Secret) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return decodeError(n, reflect.TypeFor[Secret](), "must be a string")
	}
	var v string
	if n.Decode(&v) != nil {
		return decodeError(n, reflect.TypeFor[Secret](), "must be a string")
	}
	*s = NewSecret(v)
	return nil
}

// decodeError reports a value that does not fit target at the position of n. The message carries the line and
// column of n and the target type, which config/parse.go uses to name the exact node.
func decodeError(n *yaml.Node, target reflect.Type, msg string) error {
	return &yaml.TypeError{Errors: []string{fmt.Sprintf("line %d: column %d: type %s: %s", n.Line, n.Column, target, msg)}}
}

// SecretURL holds a URL that may carry credentials in its userinfo or query. Every formatting and encoding path
// shows the URL with RedactURL applied; only Reveal returns the value.
type SecretURL struct{ b *secretBox }

// NewSecretURL returns a SecretURL holding v. An empty v yields the zero SecretURL.
func NewSecretURL(v string) SecretURL {
	if v == "" {
		return SecretURL{}
	}
	return SecretURL{&secretBox{v}}
}

// Reveal returns the URL, or "" for the zero SecretURL.
func (u SecretURL) Reveal() string {
	if u.b == nil {
		return ""
	}
	return u.b.v
}

// IsZero reports whether u holds no value.
func (u SecretURL) IsZero() bool {
	return u.b == nil
}

// String returns the URL with RedactURL applied, or "" for the zero SecretURL.
func (u SecretURL) String() string {
	if u.b == nil {
		return ""
	}
	return RedactURL(u.b.v)
}

// GoString returns the quoted form of String, so %#v never shows the URL.
func (u SecretURL) GoString() string {
	return strconv.Quote(u.String())
}

// Format writes String using the verb and flags given to it.
func (u SecretURL) Format(f fmt.State, verb rune) {
	fmt.Fprintf(f, fmt.FormatString(f, verb), u.String())
}

// LogValue returns String as a slog value.
func (u SecretURL) LogValue() slog.Value {
	return slog.StringValue(u.String())
}

// MarshalJSON encodes String as a JSON string.
func (u SecretURL) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}

// MarshalYAML returns String for YAML encoding.
func (u SecretURL) MarshalYAML() (any, error) {
	return u.String(), nil
}

// MarshalText returns String as text.
func (u SecretURL) MarshalText() ([]byte, error) {
	return []byte(u.String()), nil
}

// UnmarshalYAML decodes a YAML string scalar into u. Any other node kind is an error.
func (u *SecretURL) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return decodeError(n, reflect.TypeFor[SecretURL](), "must be a string")
	}
	var v string
	if n.Decode(&v) != nil {
		return decodeError(n, reflect.TypeFor[SecretURL](), "must be a string")
	}
	*u = NewSecretURL(v)
	return nil
}

// SecretParams holds store parameters; every leaf is treated as secret when formatted or encoded.
type SecretParams struct{ b *paramsBox }

type paramsBox struct{ m map[string]any }

// NewSecretParams returns SecretParams holding a deep copy of m. A nil m yields the zero SecretParams.
func NewSecretParams(m map[string]any) SecretParams {
	if m == nil {
		return SecretParams{}
	}
	return SecretParams{&paramsBox{m: revealParams(m).(map[string]any)}}
}

// IsZero reports whether p is absent.
func (p SecretParams) IsZero() bool {
	return p.b == nil
}

// Reveal returns a deep copy of the parameters, or nil for the zero SecretParams.
func (p SecretParams) Reveal() map[string]any {
	if p.b == nil {
		return nil
	}
	return revealMap(p.b.m)
}

// Redacted returns a copy of p in which every non-empty leaf is replaced by "REDACTED". Keys are kept.
func (p SecretParams) Redacted() SecretParams {
	if p.b == nil {
		return SecretParams{}
	}
	return SecretParams{&paramsBox{m: p.redactedMap()}}
}

func (p SecretParams) redactedMap() map[string]any {
	if p.b == nil {
		return nil
	}
	return redactLeaves(p.b.m).(map[string]any)
}

func redactLeaves(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case string:
		if t == "" {
			return t
		}
		return Redacted
	case Secret:
		if t.IsZero() {
			return t
		}
		return Redacted
	case SecretParams:
		return t.Redacted()
	case map[string]any:
		if t == nil {
			return t
		}
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[k] = redactLeaves(x)
		}
		return out
	case []any:
		if t == nil {
			return t
		}
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = redactLeaves(x)
		}
		return out
	}
	return Redacted
}

// String returns the fmt form of the parameters with every leaf redacted.
func (p SecretParams) String() string {
	return fmt.Sprint(p.redactedMap())
}

// GoString returns the %#v form of the parameters with every leaf redacted.
func (p SecretParams) GoString() string {
	return fmt.Sprintf("%#v", p.redactedMap())
}

// Format writes the parameters with every leaf redacted, using the verb and flags given to it.
func (p SecretParams) Format(f fmt.State, verb rune) {
	fmt.Fprintf(f, fmt.FormatString(f, verb), p.redactedMap())
}

// LogValue returns the parameters with every leaf redacted as a slog value.
func (p SecretParams) LogValue() slog.Value {
	return slog.AnyValue(p.redactedMap())
}

// MarshalJSON encodes the parameters with every leaf redacted.
func (p SecretParams) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.redactedMap())
}

// MarshalYAML returns the parameters with every leaf redacted for YAML encoding.
func (p SecretParams) MarshalYAML() (any, error) {
	return p.redactedMap(), nil
}

// UnmarshalYAML decodes a YAML mapping into p. Any other node kind is an error.
func (p *SecretParams) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.MappingNode {
		return decodeError(n, reflect.TypeFor[SecretParams](), "must be a mapping")
	}
	var m map[string]any
	if err := n.Decode(&m); err != nil {
		return err
	}
	*p = NewSecretParams(m)
	return nil
}

// StringLeaves returns every non-empty string leaf of p, recursively, deduplicated and sorted.
func (p SecretParams) StringLeaves() []string {
	seen := map[string]bool{}
	if p.b != nil {
		collectStringLeaves(p.b.m, seen)
	}
	return slices.Sorted(maps.Keys(seen))
}

func collectStringLeaves(v any, seen map[string]bool) {
	switch t := v.(type) {
	case string:
		if t != "" {
			seen[t] = true
		}
	case Secret:
		if !t.IsZero() {
			seen[t.Reveal()] = true
		}
	case SecretParams:
		if t.b != nil {
			collectStringLeaves(t.b.m, seen)
		}
	case map[string]any:
		for _, x := range t {
			collectStringLeaves(x, seen)
		}
	case []any:
		for _, x := range t {
			collectStringLeaves(x, seen)
		}
	}
}

// revealParams returns a recursive copy of v in which every Secret and SecretParams is a plain value.
func revealParams(v any) any {
	switch t := v.(type) {
	case Secret:
		return t.Reveal()
	case SecretParams:
		return t.Reveal()
	case map[string]any:
		return revealMap(t)
	case []any:
		if t == nil {
			return t
		}
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = revealParams(x)
		}
		return out
	}
	return v
}

func revealMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, x := range m {
		out[k] = revealParams(x)
	}
	return out
}

// DecodeSecretParams strictly decodes p into v like DecodeParams. Every message in the returned error is
// value-free: unknown keys are named, type mismatches name only the YAML tag and target type, and any
// other problem is reported as "invalid value".
func DecodeSecretParams(p SecretParams, v any) error {
	err := DecodeParams(p.Reveal(), v)
	var pe *ParamsError
	if errors.As(err, &pe) {
		for i := range pe.Errors {
			pe.Errors[i].Msg = valueFree(pe.Errors[i].Msg)
		}
	}
	return err
}

var (
	valueFreeUnknownField = regexp.MustCompile(`^unknown field "([^"]*)"$`)
	valueFreeNotFound     = regexp.MustCompile(`^field (\S+) not found in type`)
	valueFreeMismatch     = regexp.MustCompile(`^cannot unmarshal (!!\w+) .* into (\S+)$`)
)

func valueFree(msg string) string {
	if m := valueFreeUnknownField.FindStringSubmatch(msg); m != nil {
		return fmt.Sprintf("unknown key %q", m[1])
	}
	if m := valueFreeNotFound.FindStringSubmatch(msg); m != nil {
		return fmt.Sprintf("unknown key %q", m[1])
	}
	if m := valueFreeMismatch.FindStringSubmatch(msg); m != nil {
		return fmt.Sprintf("cannot decode a %s value into %s", m[1], m[2])
	}
	return "invalid value"
}
