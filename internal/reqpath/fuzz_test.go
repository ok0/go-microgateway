package reqpath

import (
	"errors"
	"net/url"
	"slices"
	"strings"
	"testing"
)

var fuzzRules = []string{
	ruleMissingURL,
	ruleSlash,
	ruleEscape,
	ruleControl,
	ruleBackslash,
	ruleSeparator,
	ruleRawMismatch,
	ruleDotSegment,
}

func FuzzCanonicalize(f *testing.F) {
	for _, tc := range canonicalizeCases(f) {
		if tc.u != nil {
			f.Add(tc.u.Path, tc.u.RawPath)
		}
	}
	f.Fuzz(func(t *testing.T, path, raw string) {
		u := &url.URL{Path: path, RawPath: raw}
		before := *u
		got, err := Canonicalize(u)
		if *u != before {
			t.Fatal("Canonicalize modified its argument")
		}
		if err != nil {
			var pe *Error
			if !errors.Is(err, ErrInvalidPath) || !errors.As(err, &pe) || !slices.Contains(fuzzRules, pe.Rule) {
				t.Fatalf("unexpected error %q", err.Error())
			}
			return
		}
		if got.Decoded == "*" {
			if path != "*" {
				t.Fatalf("Decoded \"*\" for path %q", path)
			}
			return
		}
		esc := got.Raw
		if esc == "" {
			esc = (&url.URL{Path: got.Decoded}).EscapedPath()
		}
		if !strings.HasPrefix(esc, "/") {
			t.Fatalf("escaped path %q does not start with a slash", esc)
		}
		if strings.Contains(esc, "//") {
			t.Fatalf("escaped path %q has an empty segment", esc)
		}
		if strings.Contains(esc, `\`) || strings.Contains(got.Decoded, `\`) {
			t.Fatalf("backslash in %q or %q", esc, got.Decoded)
		}
		if up := strings.ToUpper(esc); strings.Contains(up, "%2F") || strings.Contains(up, "%5C") {
			t.Fatalf("encoded separator in %q", esc)
		}
		for _, s := range []string{esc, got.Decoded} {
			for _, seg := range strings.Split(s[1:], "/") {
				if i := strings.IndexByte(seg, ';'); i >= 0 {
					seg = seg[:i]
				}
				if seg == "." || seg == ".." {
					t.Fatalf("dot segment in %q", s)
				}
			}
		}
		dec, err := url.PathUnescape(esc)
		if err != nil || dec != got.Decoded {
			t.Fatalf("PathUnescape(%q) = %q, %v; want %q", esc, dec, err, got.Decoded)
		}
		if got.Raw != "" && got.Raw == (&url.URL{Path: got.Decoded}).EscapedPath() {
			t.Fatalf("Raw %q equals the default escaping", got.Raw)
		}
		again, err := Canonicalize(&url.URL{Path: got.Decoded, RawPath: got.Raw})
		if err != nil || again != got {
			t.Fatalf("not idempotent: %+v, %v; want %+v", again, err, got)
		}
		if !IsCanonical(esc) {
			t.Fatalf("IsCanonical(%q) = false", esc)
		}
	})
}
