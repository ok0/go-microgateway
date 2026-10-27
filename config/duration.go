package config

import (
	"encoding/json"
	"fmt"
	"time"

	"go.yaml.in/yaml/v3"
)

// Duration is a time.Duration that reads and writes as a duration string such as "5s" or "250ms".
// Duration fields do not support ${VAR} substitution.
type Duration time.Duration

// Std returns d as a time.Duration.
func (d Duration) Std() time.Duration {
	return time.Duration(d)
}

// String formats d using time.Duration notation.
func (d Duration) String() string {
	return time.Duration(d).String()
}

// UnmarshalYAML decodes a scalar holding a duration string. An invalid value is reported as a
// *yaml.TypeError so that decoding of the rest of the document continues.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		if v, err := time.ParseDuration(n.Value); err == nil {
			*d = Duration(v)
			return nil
		}
	}
	return &yaml.TypeError{Errors: []string{fmt.Sprintf("line %d: invalid duration %q (use a value such as \"250ms\" or \"5s\")", n.Line, n.Value)}}
}

// MarshalYAML encodes d as its duration string.
func (d Duration) MarshalYAML() (any, error) {
	return d.String(), nil
}

// MarshalJSON encodes d as a JSON string holding its duration string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON decodes a JSON string holding a duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}
