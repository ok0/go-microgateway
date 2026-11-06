// Package reqpath derives the one canonical form of a request path. The derivation is pure: it takes no configuration and keeps no state, so every gateway instance derives the same form from the same request. Ambiguous forms are rejected, and equivalent spellings are normalized to one form.
package reqpath

import (
	"errors"
	"net/url"
	"strings"
)

// ErrInvalidPath is matched by every error that Canonicalize returns for a rejected path.
var ErrInvalidPath = errors.New("reqpath: invalid path")

const (
	ruleMissingURL  = "missing URL"
	ruleSlash       = "path must start with a slash"
	ruleEscape      = "invalid percent escape"
	ruleControl     = "control byte"
	ruleBackslash   = "backslash"
	ruleSeparator   = "encoded path separator"
	ruleRawMismatch = "raw path does not match decoded path"
	ruleDotSegment  = "dot segment"
)

const upperHex = "0123456789ABCDEF"

// Error reports why a request path was rejected. Rule names the broken rule and never contains any part of the input.
type Error struct {
	Rule string
}

// Error returns the message, which names the broken rule only.
func (e *Error) Error() string {
	return ErrInvalidPath.Error() + ": " + e.Rule
}

// Unwrap returns ErrInvalidPath.
func (e *Error) Unwrap() error {
	return ErrInvalidPath
}

// Path is the canonical form of a request path. Decoded goes to URL.Path. Raw goes to URL.RawPath and is "" when it equals the default escaping of Decoded.
type Path struct {
	Decoded string
	Raw     string
}

// Canonicalize returns the canonical form of the request URL u. It never modifies u. A path that breaks a rule is rejected with an *Error that matches ErrInvalidPath.
func Canonicalize(u *url.URL) (Path, error) {
	if u == nil {
		return Path{}, &Error{Rule: ruleMissingURL}
	}
	if u.Path == "*" && (u.RawPath == "" || u.RawPath == "*") {
		return Path{Decoded: "*"}, nil
	}
	var in string
	if u.RawPath != "" {
		in = u.RawPath
	} else {
		if u.Path == "" {
			return Path{Decoded: "/"}, nil
		}
		in = u.EscapedPath()
	}
	if !strings.HasPrefix(in, "/") {
		return Path{}, &Error{Rule: ruleSlash}
	}
	norm, err := normalize(in)
	if err != nil {
		return Path{}, err
	}
	if u.RawPath != "" {
		d, err := url.PathUnescape(u.RawPath)
		if err != nil || d != u.Path {
			return Path{}, &Error{Rule: ruleRawMismatch}
		}
	}
	dec, err := url.PathUnescape(norm)
	if err != nil {
		return Path{}, &Error{Rule: ruleEscape}
	}
	if hasDotSegment(norm) || hasDotSegment(dec) {
		return Path{}, &Error{Rule: ruleDotSegment}
	}
	raw := norm
	if (&url.URL{Path: dec}).EscapedPath() == norm {
		raw = ""
	}
	return Path{Decoded: dec, Raw: raw}, nil
}

// IsCanonical reports whether p is already in canonical escaped form, so that Canonicalize of p yields p again.
func IsCanonical(p string) bool {
	if !strings.HasPrefix(p, "/") {
		return false
	}
	dec, err := url.PathUnescape(p)
	if err != nil {
		return false
	}
	c, err := Canonicalize(&url.URL{Path: dec, RawPath: p})
	if err != nil {
		return false
	}
	esc := c.Raw
	if esc == "" {
		esc = (&url.URL{Path: c.Decoded}).EscapedPath()
	}
	return esc == p
}

// normalize rewrites the escaped path in into canonical escaped form. It decodes escapes of unreserved bytes, upper-cases the other escapes, escapes bytes outside the path character set, and merges repeated slashes.
func normalize(in string) (string, error) {
	var b strings.Builder
	var last byte
	put := func(c byte) {
		if c == '/' && last == '/' {
			return
		}
		b.WriteByte(c)
		last = c
	}
	putEscaped := func(c byte) {
		b.WriteByte('%')
		b.WriteByte(upperHex[c>>4])
		b.WriteByte(upperHex[c&0x0f])
		last = '%'
	}
	for i := 0; i < len(in); i++ {
		c := in[i]
		switch {
		case c == '%':
			if i+2 >= len(in) {
				return "", &Error{Rule: ruleEscape}
			}
			hi, okHi := unhex(in[i+1])
			lo, okLo := unhex(in[i+2])
			if !okHi || !okLo {
				return "", &Error{Rule: ruleEscape}
			}
			i += 2
			v := hi<<4 | lo
			switch {
			case v < 0x20 || v == 0x7f:
				return "", &Error{Rule: ruleControl}
			case v == '/' || v == '\\':
				return "", &Error{Rule: ruleSeparator}
			case isUnreserved(v):
				put(v)
			default:
				putEscaped(v)
			}
		case c < 0x20 || c == 0x7f:
			return "", &Error{Rule: ruleControl}
		case c == '\\':
			return "", &Error{Rule: ruleBackslash}
		case c == '/' || isPchar(c):
			put(c)
		default:
			putEscaped(c)
		}
	}
	return b.String(), nil
}

// hasDotSegment reports whether the path p has a segment that is "." or "..", ignoring any part of the segment from its first ";" on. p must start with a slash.
func hasDotSegment(p string) bool {
	for _, seg := range strings.Split(p[1:], "/") {
		if i := strings.IndexByte(seg, ';'); i >= 0 {
			seg = seg[:i]
		}
		if seg == "." || seg == ".." {
			return true
		}
	}
	return false
}

func unhex(c byte) (byte, bool) {
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

func isUnreserved(c byte) bool {
	switch {
	case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		return true
	}
	return c == '-' || c == '.' || c == '_' || c == '~'
}

func isPchar(c byte) bool {
	return isUnreserved(c) || strings.IndexByte("!$&'()*+,;=:@", c) >= 0
}
