// Package yamlx is the single place that decodes YAML into Go values.
package yamlx

import (
	"bytes"
	"errors"
	"io"

	"go.yaml.in/yaml/v3"
)

// ErrMultipleDocuments is returned by DecodeStrict when the input contains more than one YAML document.
var ErrMultipleDocuments = errors.New("multiple YAML documents are not supported")

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
