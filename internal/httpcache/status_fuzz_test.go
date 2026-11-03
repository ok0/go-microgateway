package httpcache

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func FuzzParseStatus(f *testing.F) {
	for _, s := range []string{
		``,
		`  c`,
		`c  ,  d`,
		`c; foo=:aGk=:; hit`,
		`c; detail=token`,
		`c; hit=?0`,
		`c; hit=?1`,
		`c; fwd-status=599`,
		`c; ttl=999999999999999`,
		`c; ttl=1; ttl=2`,
		`"my cache"`,
		`c; key="k\"\\"`,
		`microgateway; hit; fwd=stale; fwd-status=503; ttl=-5`,
		`c; (a b)`,
		`(a)`,
		`c, d=1`,
		`c,`,
		`c, `,
		`1c`,
		`c; hit="x"`,
		`"unterminated`,
		`c; Ttl=1`,
		`c; ttl=1234567890123456`,
		`c; ttl="1"`,
		`c; hit=1`,
		`c; fwd="stale"`,
		`c; fwd-status=99`,
		`c; fwd-status=600`,
		`c; key=abc`,
		`c; ttl=1.5`,
		`c;`,
		`c; =1`,
		"\"\x01\"",
		`"\x01"`,
		`"\q"`,
		strings.Repeat("c, ", 64) + "c",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		list, err := ParseStatus(s)
		if err != nil {
			if !errors.Is(err, ErrMalformedStatus) {
				t.Fatalf("ParseStatus(%q) error %v does not wrap ErrMalformedStatus", s, err)
			}
			return
		}
		for _, m := range list {
			if m.TTL < -999999999999999 || m.TTL > 999999999999999 {
				t.Fatalf("ParseStatus(%q) produced ttl %d outside the bound", s, m.TTL)
			}
		}
		again, err := ParseStatus(FormatStatus(list...))
		if err != nil {
			t.Fatalf("ParseStatus(FormatStatus(%q)) = %v", s, err)
		}
		if !slices.Equal(list, again) {
			t.Fatalf("round trip of %q = %+v, want %+v", s, again, list)
		}
	})
}
