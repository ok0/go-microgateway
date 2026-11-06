// Package router matches requests to routes.
package router

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// PathType selects how a route path is matched.
type PathType string

const (
	// PathExact matches only the exact path.
	PathExact PathType = "exact"
	// PathPrefix matches the path and any path below it.
	PathPrefix PathType = "prefix"
	// PathPattern matches paths containing named parameters and an optional final glob.
	PathPattern PathType = "pattern"
)

// SegmentKind identifies what a path segment matches.
type SegmentKind int

const (
	// SegLiteral matches a segment whose text equals Value.
	SegLiteral SegmentKind = iota
	// SegParam matches exactly one path segment and captures it under the name Value.
	SegParam
	// SegGlob matches the remainder of the path and captures it under the name Value.
	SegGlob
)

// Segment is one element of a compiled path.
type Segment struct {
	Kind  SegmentKind
	Value string // literal text, or param/glob name
}

// Pattern is a compiled route path.
type Pattern struct {
	Raw      string
	Type     PathType
	Segments []Segment
}

var paramName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Compile parses path according to t and returns its compiled form.
//
// Rules:
//
//  1. t must be exact, prefix or pattern, otherwise an error "unknown path type" is returned.
//  2. path must be non-empty and start with "/".
//  3. path must not contain '?', '#', space or ASCII control characters (below 0x20, or 0x7f).
//  4. path[1:] is split on "/"; the path "/" has zero segments. Empty segments are rejected,
//     except a single trailing empty segment for exact and prefix paths (a trailing slash).
//     The segments "." and ".." are rejected.
//  5. For exact and prefix paths every segment is a literal; ':' and '*' carry no meaning.
//  6. For pattern paths a path must not end with "/" unless it is "/". A segment starting
//     with ':' is a parameter and a segment starting with '*' is a glob, which must be the
//     last segment. Names must match ^[A-Za-z_][A-Za-z0-9_]*$ and be unique across parameters
//     and globs. Literal segments must not contain ':' or '*'.
//
// Errors are plain messages describing the first rule violated.
func Compile(path string, t PathType) (*Pattern, error) {
	switch t {
	case PathExact, PathPrefix, PathPattern:
	default:
		return nil, fmt.Errorf("unknown path type %q", t)
	}
	if path == "" || path[0] != '/' {
		return nil, errors.New(`path must start with "/"`)
	}
	for i := 0; i < len(path); i++ {
		c := path[i]
		if c < 0x20 || c == 0x7f || c == '?' || c == '#' || c == ' ' {
			return nil, fmt.Errorf("path contains invalid character %q", string(rune(c)))
		}
	}
	if t == PathPattern && path != "/" && strings.HasSuffix(path, "/") {
		return nil, errors.New(`pattern must not end with "/"`)
	}

	var parts []string
	if raw := path[1:]; raw != "" {
		parts = strings.Split(raw, "/")
		if t != PathPattern && parts[len(parts)-1] == "" {
			parts = parts[:len(parts)-1]
		}
	}

	p := &Pattern{Raw: path, Type: t}
	seen := make(map[string]bool)
	for i, s := range parts {
		switch s {
		case "":
			return nil, errors.New("empty path segment")
		case ".", "..":
			return nil, errors.New("dot segment not allowed")
		}

		if t != PathPattern {
			p.Segments = append(p.Segments, Segment{Kind: SegLiteral, Value: s})
			continue
		}

		switch {
		case strings.HasPrefix(s, ":"):
			name := s[1:]
			if !paramName.MatchString(name) {
				return nil, fmt.Errorf("invalid parameter name %q", name)
			}
			if seen[name] {
				return nil, fmt.Errorf("duplicate parameter name %q", name)
			}
			seen[name] = true
			p.Segments = append(p.Segments, Segment{Kind: SegParam, Value: name})
		case strings.HasPrefix(s, "*"):
			if i != len(parts)-1 {
				return nil, errors.New("glob segment must be last")
			}
			name := s[1:]
			if !paramName.MatchString(name) {
				return nil, fmt.Errorf("invalid parameter name %q", name)
			}
			if seen[name] {
				return nil, fmt.Errorf("duplicate parameter name %q", name)
			}
			seen[name] = true
			p.Segments = append(p.Segments, Segment{Kind: SegGlob, Value: name})
		default:
			if strings.ContainsAny(s, ":*") {
				return nil, fmt.Errorf("literal segment %q must not contain \":\" or \"*\"", s)
			}
			p.Segments = append(p.Segments, Segment{Kind: SegLiteral, Value: s})
		}
	}
	return p, nil
}
