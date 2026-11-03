package httpcache

import (
	"net/http"
	"strings"
)

// Delimiter separates the tags of a tag header.
type Delimiter uint8

const (
	DelimSpace Delimiter = iota
	DelimComma
)

const (
	MaxTagHeaderBytes = 16 << 10
	MaxTagBytes       = 1 << 10
	MaxTags           = 64
)

// ParseTags returns the distinct tags of the header name in h. ok is false when the
// header is too large, a tag is too long or holds a control byte, or there are too many tags.
func ParseTags(h http.Header, name string, delim Delimiter) ([]string, bool) {
	vals := h.Values(name)
	if len(vals) == 0 {
		return nil, true
	}
	sep := " "
	if delim == DelimComma {
		sep = ","
	}
	joined := strings.Join(vals, sep)
	if len(joined) > MaxTagHeaderBytes {
		return nil, false
	}
	var parts []string
	if delim == DelimComma {
		parts = strings.Split(joined, ",")
	} else {
		parts = strings.FieldsFunc(joined, func(r rune) bool { return r == ' ' || r == '\t' })
	}
	seen := map[string]bool{}
	var tags []string
	for _, p := range parts {
		t := trimOWS(p)
		if t == "" {
			continue
		}
		for i := 0; i < len(t); i++ {
			if t[i] < 0x20 || t[i] == 0x7f {
				return nil, false
			}
		}
		if len(t) > MaxTagBytes {
			return nil, false
		}
		if seen[t] {
			continue
		}
		seen[t] = true
		tags = append(tags, t)
		if len(tags) > MaxTags {
			return nil, false
		}
	}
	return tags, true
}
