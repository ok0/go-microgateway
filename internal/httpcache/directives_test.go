package httpcache

import (
	"net/http"
	"testing"
	"time"
)

func sec(n int) time.Duration { return time.Duration(n) * time.Second }

func TestParseCacheControl(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		lines []string
		want  Directives
	}{
		{"uppercase names", []string{"NO-STORE, Max-Age=5"}, Directives{NoStore: true, MaxAge: Delta{true, sec(5)}}},
		{"quoted max-age", []string{`max-age="60"`}, Directives{MaxAge: Delta{true, sec(60)}}},
		{"repeated max-age takes minimum", []string{"max-age=60, max-age=30"}, Directives{MaxAge: Delta{true, sec(30)}}},
		{"repeated s-maxage takes minimum", []string{"s-maxage=9", "s-maxage=4"}, Directives{SMaxAge: Delta{true, sec(4)}}},
		{"min-fresh repeated takes maximum", []string{"min-fresh=5, min-fresh=9"}, Directives{MinFresh: Delta{true, sec(9)}}},
		{"max-age=-1 invalid", []string{"max-age=-1"}, Directives{MaxAge: Delta{true, 0}}},
		{"max-age empty invalid", []string{"max-age="}, Directives{MaxAge: Delta{true, 0}}},
		{"max-age without value invalid", []string{"max-age"}, Directives{MaxAge: Delta{true, 0}}},
		{"max-age=1.5 invalid", []string{"max-age=1.5"}, Directives{MaxAge: Delta{true, 0}}},
		{"max-age=abc invalid", []string{"max-age=abc"}, Directives{MaxAge: Delta{true, 0}}},
		{"40-digit max-age ceiling", []string{"max-age=1111111111111111111111111111111111111111"}, Directives{MaxAge: Delta{true, MaxDelta}}},
		{"2147483648 ceiling", []string{"max-age=2147483648"}, Directives{MaxAge: Delta{true, MaxDelta}}},
		{"2147483649 ceiling", []string{"max-age=2147483649"}, Directives{MaxAge: Delta{true, MaxDelta}}},
		{"min-fresh invalid ignored", []string{"min-fresh=x"}, Directives{}},
		{"max-stale without value", []string{"max-stale"}, Directives{MaxStale: Delta{true, MaxDelta}}},
		{"max-stale=10", []string{"max-stale=10"}, Directives{MaxStale: Delta{true, sec(10)}}},
		{"max-stale invalid ignored", []string{"max-stale=x"}, Directives{}},
		{"max-stale bare then valid", []string{"max-stale, max-stale=3"}, Directives{MaxStale: Delta{true, sec(3)}}},
		{"multi-line", []string{"no-store", "max-age=5"}, Directives{NoStore: true, MaxAge: Delta{true, sec(5)}}},
		{"private field list is a flag", []string{`private="Set-Cookie"`}, Directives{Private: true}},
		{"no-cache field list is a flag", []string{`no-cache="X"`}, Directives{NoCache: true}},
		{"public with value is a flag", []string{"public=1"}, Directives{Public: true}},
		{"unknown ignored", []string{"foo=bar"}, Directives{}},
		{"quoted comma kept in value", []string{`foo="a,b", max-age=3`}, Directives{MaxAge: Delta{true, sec(3)}}},
		{"unterminated quote", []string{`max-age="60`}, Directives{MaxAge: Delta{true, 0}}},
		{"flags", []string{"must-revalidate, proxy-revalidate, only-if-cached, no-cache"}, Directives{MustRevalidate: true, ProxyRevalidate: true, OnlyIfCached: true, NoCache: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := http.Header{}
			for _, l := range tc.lines {
				h.Add("Cache-Control", l)
			}
			if got := ParseCacheControl(h); got != tc.want {
				t.Fatalf("ParseCacheControl(%q) = %+v, want %+v", tc.lines, got, tc.want)
			}
		})
	}
}

func TestRequestDirectivesPragma(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		h    http.Header
		want bool
	}{
		{"pragma alone", http.Header{"Pragma": {"no-cache"}}, true},
		{"cache-control suppresses pragma", http.Header{"Pragma": {"no-cache"}, "Cache-Control": {"max-age=5"}}, false},
		{"pragma list case-insensitive", http.Header{"Pragma": {"x, NO-CACHE"}}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := RequestDirectives(tc.h).NoCache; got != tc.want {
				t.Fatalf("NoCache = %v, want %v", got, tc.want)
			}
		})
	}
}
