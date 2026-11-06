package reqpath

import (
	"errors"
	"net/url"
	"strings"
	"testing"
)

type canonicalizeCase struct {
	name        string
	u           *url.URL
	wantDecoded string
	wantRaw     string
	wantRule    string
}

func target(t testing.TB, s string) *url.URL {
	t.Helper()
	u, err := url.ParseRequestURI(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func canonicalizeCases(t testing.TB) []canonicalizeCase {
	return []canonicalizeCase{
		{name: "dot segment", u: target(t, "/public/../admin/x"), wantRule: "dot segment"},
		{name: "encoded dot segment", u: target(t, "/public/%2e%2e/admin/x"), wantRule: "dot segment"},
		{name: "upper encoded dot segment", u: target(t, "/public/%2E%2E/admin/x"), wantRule: "dot segment"},
		{name: "dot segment before semicolon", u: target(t, "/public/..;/admin/x"), wantRule: "dot segment"},
		{name: "empty segment before dot segment", u: target(t, "/public//../admin/x"), wantRule: "dot segment"},
		{name: "encoded single dot", u: target(t, "/admin/%2e/x"), wantRule: "dot segment"},
		{name: "encoded dot at end", u: target(t, "/.%2E"), wantRule: "dot segment"},
		{name: "dot dot at end", u: target(t, "/a/.."), wantRule: "dot segment"},
		{name: "dot at end", u: target(t, "/a/."), wantRule: "dot segment"},
		{name: "encoded semicolon after dot dot", u: target(t, "/..%3bx/y"), wantRule: "dot segment"},
		{name: "dot before semicolon p", u: target(t, "/x/.;p"), wantRule: "dot segment"},

		{name: "encoded slash after dot dot", u: target(t, "/public/..%2fadmin/x"), wantRule: "encoded path separator"},
		{name: "encoded backslash after dot dot", u: target(t, "/public/..%5cadmin/x"), wantRule: "encoded path separator"},
		{name: "encoded slash", u: target(t, "/admin%2fx"), wantRule: "encoded path separator"},
		{name: "upper encoded slash", u: target(t, "/admin%2Fx"), wantRule: "encoded path separator"},
		{name: "upper encoded backslash", u: target(t, "/a%5Cb"), wantRule: "encoded path separator"},

		{name: "literal backslash target", u: target(t, "/public/..\\admin/x"), wantRule: "backslash"},
		{name: "backslash in path without raw form", u: &url.URL{Path: "/a\\b"}, wantRule: "encoded path separator"},
		{name: "literal backslash short target", u: target(t, "/a\\b"), wantRule: "backslash"},
		{name: "literal backslash in raw path", u: &url.URL{Path: "/a\\b", RawPath: "/a\\b"}, wantRule: "backslash"},

		{name: "invalid escape", u: &url.URL{Path: "/a", RawPath: "/a%zz"}, wantRule: "invalid percent escape"},
		{name: "short escape", u: &url.URL{Path: "/a", RawPath: "/a%2"}, wantRule: "invalid percent escape"},
		{name: "bare percent", u: &url.URL{Path: "/a", RawPath: "/a%"}, wantRule: "invalid percent escape"},

		{name: "nul byte", u: &url.URL{Path: "/a\x00b"}, wantRule: "control byte"},
		{name: "encoded nul byte", u: &url.URL{Path: "/a\x00b", RawPath: "/a%00b"}, wantRule: "control byte"},
		{name: "tab", u: &url.URL{Path: "/a\tb"}, wantRule: "control byte"},
		{name: "encoded unit separator", u: &url.URL{Path: "/a\x1fb", RawPath: "/a%1fb"}, wantRule: "control byte"},
		{name: "encoded delete", u: &url.URL{Path: "/a\x7f", RawPath: "/a%7F"}, wantRule: "control byte"},

		{name: "no leading slash", u: &url.URL{Path: "a/b"}, wantRule: "path must start with a slash"},
		{name: "raw path differs", u: &url.URL{Path: "/a", RawPath: "/b"}, wantRule: "raw path does not match decoded path"},
		{name: "missing URL", u: nil, wantRule: "missing URL"},

		{name: "asterisk", u: &url.URL{Path: "*"}, wantDecoded: "*"},
		{name: "empty path", u: &url.URL{}, wantDecoded: "/"},
		{name: "root", u: target(t, "/"), wantDecoded: "/"},
		{name: "slashes only", u: target(t, "///"), wantDecoded: "/"},
		{name: "repeated slashes", u: target(t, "/admin//x"), wantDecoded: "/admin/x"},
		{name: "trailing slash kept", u: target(t, "/admin/"), wantDecoded: "/admin/"},
		{name: "encoded tilde", u: target(t, "/a%7Ex"), wantDecoded: "/a~x"},
		{name: "lower encoded tilde", u: target(t, "/a%7ex"), wantDecoded: "/a~x"},
		{name: "plain tilde", u: target(t, "/a~x"), wantDecoded: "/a~x"},
		{name: "encoded semicolon", u: target(t, "/a%3Bb"), wantDecoded: "/a;b", wantRaw: "/a%3Bb"},
		{name: "lower encoded semicolon", u: target(t, "/a%3bb"), wantDecoded: "/a;b", wantRaw: "/a%3Bb"},
		{name: "literal semicolon", u: target(t, "/a;b"), wantDecoded: "/a;b"},
		{name: "literal bang", u: target(t, "/a!b"), wantDecoded: "/a!b", wantRaw: "/a!b"},
		{name: "encoded bang", u: target(t, "/a%21b"), wantDecoded: "/a!b"},
		{name: "encoded utf8", u: target(t, "/a%c3%b6"), wantDecoded: "/a\xc3\xb6"},
		{name: "raw utf8", u: &url.URL{Path: "/a\xc3\xb6"}, wantDecoded: "/a\xc3\xb6"},
		{name: "space", u: &url.URL{Path: "/a b"}, wantDecoded: "/a b"},
		{name: "encoded percent", u: target(t, "/a%25b"), wantDecoded: "/a%b"},
		{name: "dot prefix segment", u: target(t, "/a/.b"), wantDecoded: "/a/.b"},
		{name: "double dot prefix segment", u: target(t, "/a/..b"), wantDecoded: "/a/..b"},
		{name: "triple dot segment", u: target(t, "/a/..."), wantDecoded: "/a/..."},
	}
}

func TestCanonicalize(t *testing.T) {
	for _, tc := range canonicalizeCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			var before url.URL
			if tc.u != nil {
				before = *tc.u
			}
			got, err := Canonicalize(tc.u)
			if tc.u != nil && *tc.u != before {
				t.Fatal("Canonicalize modified its argument")
			}
			if tc.wantRule != "" {
				if !errors.Is(err, ErrInvalidPath) {
					t.Fatalf("err = %v, want ErrInvalidPath", err)
				}
				var pe *Error
				if !errors.As(err, &pe) || pe.Rule != tc.wantRule {
					t.Fatalf("err = %v, want rule %q", err, tc.wantRule)
				}
				if tc.u != nil {
					for _, in := range []string{tc.u.Path, tc.u.RawPath} {
						if len(in) > 1 && strings.Contains(err.Error(), in) {
							t.Fatalf("error %q carries the input path", err.Error())
						}
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if got.Decoded != tc.wantDecoded || got.Raw != tc.wantRaw {
				t.Fatalf("got {%q %q}, want {%q %q}", got.Decoded, got.Raw, tc.wantDecoded, tc.wantRaw)
			}
		})
	}
}

func TestCanonicalizeEquivalence(t *testing.T) {
	same := [][]string{
		{"/a%7Ex", "/a%7ex", "/a~x"},
		{"/admin//x", "/admin/x", "/admin///x"},
		{"/a%3Bb", "/a%3bb"},
	}
	for _, group := range same {
		want, err := Canonicalize(target(t, group[0]))
		if err != nil {
			t.Fatalf("Canonicalize(%q): %v", group[0], err)
		}
		for _, s := range group[1:] {
			got, err := Canonicalize(target(t, s))
			if err != nil {
				t.Fatalf("Canonicalize(%q): %v", s, err)
			}
			if got != want {
				t.Fatalf("Canonicalize(%q) = %+v, want %+v (as %q)", s, got, want, group[0])
			}
		}
	}
	differ := [][2]string{
		{"/a%3Bb", "/a;b"},
		{"/a%21b", "/a!b"},
	}
	for _, pair := range differ {
		a, err := Canonicalize(target(t, pair[0]))
		if err != nil {
			t.Fatalf("Canonicalize(%q): %v", pair[0], err)
		}
		b, err := Canonicalize(target(t, pair[1]))
		if err != nil {
			t.Fatalf("Canonicalize(%q): %v", pair[1], err)
		}
		if a == b {
			t.Fatalf("Canonicalize(%q) and Canonicalize(%q) are both %+v", pair[0], pair[1], a)
		}
	}
}

func TestCanonicalizeIdempotent(t *testing.T) {
	for _, tc := range canonicalizeCases(t) {
		if tc.wantRule != "" {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			got, err := Canonicalize(tc.u)
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			again, err := Canonicalize(&url.URL{Path: got.Decoded, RawPath: got.Raw})
			if err != nil {
				t.Fatalf("second pass err = %v", err)
			}
			if again != got {
				t.Fatalf("second pass = %+v, want %+v", again, got)
			}
		})
	}
}

func TestIsCanonical(t *testing.T) {
	yes := []string{"/", "/a", "/a/", "/api/v2", "/a%3Bb", "/a;b", "/a!b", "/caf%C3%B6"}
	no := []string{"", "a", "*", "/a//b", "/a/../b", "/a/./b", "/a%7Eb", "/a%3bb", "/a%2Fb", "/a\\b", "/a%zz", "/a b"}
	for _, p := range yes {
		if !IsCanonical(p) {
			t.Errorf("IsCanonical(%q) = false, want true", p)
		}
	}
	for _, p := range no {
		if IsCanonical(p) {
			t.Errorf("IsCanonical(%q) = true, want false", p)
		}
	}
}
