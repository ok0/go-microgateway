package httpcache

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
)

func numberedTagList(n int) []string {
	parts := make([]string, 0, n)
	for i := range n {
		parts = append(parts, fmt.Sprintf("t%d", i))
	}
	return parts
}

func numberedTags(n int) string {
	return strings.Join(numberedTagList(n), " ")
}

func TestParseTags(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		lines []string
		delim Delimiter
		want  []string
		ok    bool
	}{
		{"space delimiter", []string{"a b  c"}, DelimSpace, []string{"a", "b", "c"}, true},
		{"comma delimiter", []string{"a, b ,c"}, DelimComma, []string{"a", "b", "c"}, true},
		{"multi-line joined", []string{"a", "b c"}, DelimSpace, []string{"a", "b", "c"}, true},
		{"dedupe preserves first order", []string{"b a b c a"}, DelimSpace, []string{"b", "a", "c"}, true},
		{"case sensitive", []string{"A a"}, DelimSpace, []string{"A", "a"}, true},
		{"empties dropped comma", []string{" , ,"}, DelimComma, nil, true},
		{"empties dropped space", []string{"  "}, DelimSpace, nil, true},
		{"HTAB separates space delimiter", []string{"a\tb"}, DelimSpace, []string{"a", "b"}, true},
		{"16384-byte header ok", []string{strings.Repeat("a ", 8192)}, DelimSpace, []string{"a"}, true},
		{"16385-byte header refused", []string{strings.Repeat("a ", 8192) + "b"}, DelimSpace, nil, false},
		{"1024-byte tag ok", []string{strings.Repeat("t", 1024)}, DelimSpace, []string{strings.Repeat("t", 1024)}, true},
		{"1025-byte tag refused", []string{strings.Repeat("t", 1025)}, DelimSpace, nil, false},
		{"64 distinct ok", []string{numberedTags(64)}, DelimSpace, numberedTagList(64), true},
		{"65 distinct refused", []string{numberedTags(65)}, DelimSpace, nil, false},
		{"65 entries with one duplicate ok", []string{numberedTags(64) + " t0"}, DelimSpace, numberedTagList(64), true},
		{"control byte 0x01 refused", []string{"a\x01b"}, DelimSpace, nil, false},
		{"DEL 0x7f refused", []string{"a\x7fb"}, DelimComma, nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := http.Header{}
			for _, l := range tc.lines {
				h.Add("Surrogate-Key", l)
			}
			got, ok := ParseTags(h, "Surrogate-Key", tc.delim)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if tc.ok && !slices.Equal(got, tc.want) {
				t.Fatalf("tags = %q, want %q", got, tc.want)
			}
		})
	}
	t.Run("absent header", func(t *testing.T) {
		t.Parallel()
		got, ok := ParseTags(http.Header{}, "Surrogate-Key", DelimSpace)
		if !ok || got != nil {
			t.Fatalf("ParseTags absent = (%q, %v), want (nil, true)", got, ok)
		}
	})
}
