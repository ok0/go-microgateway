package httpcache

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestStatusString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   Status
		want string
	}{
		{
			name: "hit with ttl",
			in:   Status{Name: "microgateway", Hit: true, TTL: 30, HasTTL: true},
			want: `microgateway; hit; ttl=30`,
		},
		{
			name: "miss stored",
			in:   Status{Name: "microgateway", Fwd: FwdURIMiss, Stored: true},
			want: `microgateway; fwd=uri-miss; stored`,
		},
		{
			name: "stale hit with negative ttl and forward status",
			in:   Status{Name: "microgateway", Hit: true, Fwd: FwdStale, TTL: -5, HasTTL: true, FwdStatus: 503, HasFwdStatus: true},
			want: `microgateway; hit; fwd=stale; fwd-status=503; ttl=-5`,
		},
		{
			name: "bypass detail escapes quote and backslash and replaces non-ASCII bytes",
			in:   Status{Name: "microgateway", Fwd: FwdBypass, Detail: "a\"b\\cé"},
			want: `microgateway; fwd=bypass; detail="a\"b\\c??"`,
		},
		{
			name: "non-token name is quoted",
			in:   Status{Name: "my cache"},
			want: `"my cache"`,
		},
		{
			name: "ttl clamped to the upper bound",
			in:   Status{Name: "m", TTL: 10000000000000000, HasTTL: true},
			want: `m; ttl=999999999999999`,
		},
		{
			name: "ttl clamped to the lower bound",
			in:   Status{Name: "m", TTL: -10000000000000000, HasTTL: true},
			want: `m; ttl=-999999999999999`,
		},
		{
			name: "forward status outside 100 to 599 is omitted",
			in:   Status{Name: "m", Fwd: FwdMiss, FwdStatus: 600, HasFwdStatus: true},
			want: `m; fwd=miss`,
		},
		{
			name: "invalid forward value is omitted",
			in:   Status{Name: "m", Fwd: "bad value"},
			want: `m`,
		},
		{
			name: "key is escaped",
			in:   Status{Name: "m", Key: `k"\`},
			want: `m; key="k\"\\"`,
		},
		{
			name: "collapsed follows stored",
			in:   Status{Name: "m", Stored: true, Collapsed: true},
			want: `m; stored; collapsed`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.in.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFormatStatusJoins(t *testing.T) {
	t.Parallel()
	a := Status{Name: "a", Hit: true}
	b := Status{Name: "b", Stored: true}
	if got, want := FormatStatus(a, b), `a; hit, b; stored`; got != want {
		t.Fatalf("FormatStatus() = %q, want %q", got, want)
	}
	if got := FormatStatus(); got != "" {
		t.Fatalf("FormatStatus() with no members = %q, want empty", got)
	}
}

func TestStatusRoundTrip(t *testing.T) {
	t.Parallel()
	fwds := []string{"", FwdBypass, FwdMethod, FwdURIMiss, FwdVaryMiss, FwdMiss, FwdRequest, FwdStale, FwdPartial}
	ttls := []struct {
		has bool
		ttl int64
	}{{false, 0}, {true, 1}, {true, 0}, {true, -1}}
	keys := []string{"", `k"\`}
	details := []string{"", "a b"}
	names := []string{"microgateway", "my cache", ""}
	for _, hit := range []bool{false, true} {
		for _, stored := range []bool{false, true} {
			for _, collapsed := range []bool{false, true} {
				for _, fwd := range fwds {
					for _, hasFwdStatus := range []bool{false, true} {
						for _, tt := range ttls {
							for _, key := range keys {
								for _, detail := range details {
									for _, name := range names {
										s := Status{
											Name:         name,
											Hit:          hit,
											Fwd:          fwd,
											HasFwdStatus: hasFwdStatus,
											TTL:          tt.ttl,
											HasTTL:       tt.has,
											Stored:       stored,
											Collapsed:    collapsed,
											Key:          key,
											Detail:       detail,
										}
										if hasFwdStatus {
											s.FwdStatus = 200
										}
										got, err := ParseStatus(s.String())
										if err != nil {
											t.Fatalf("ParseStatus(%q): %v", s.String(), err)
										}
										if want := []Status{s}; !slices.Equal(got, want) {
											t.Fatalf("round trip of %q = %+v, want %+v", s.String(), got, want)
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
}

func TestStatusRoundTripList(t *testing.T) {
	t.Parallel()
	in := []Status{
		{Name: "microgateway", Hit: true, Fwd: FwdStale, FwdStatus: 503, HasFwdStatus: true, TTL: -5, HasTTL: true},
		{Name: "edge", Fwd: FwdURIMiss, Stored: true, Key: `k"\`},
		{Name: "my cache", Fwd: FwdBypass, Detail: "a b", Collapsed: true},
	}
	got, err := ParseStatus(FormatStatus(in...))
	if err != nil {
		t.Fatalf("ParseStatus: %v", err)
	}
	if !slices.Equal(got, in) {
		t.Fatalf("round trip = %+v, want %+v", got, in)
	}
}

func TestParseStatusRejects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{"inner list in member", `c; (a b)`},
		{"bare inner list", `(a)`},
		{"dictionary form", `c, d=1`},
		{"trailing comma", `c,`},
		{"trailing comma with space", `c, `},
		{"numeric member", `1c`},
		{"hit as string", `c; hit="x"`},
		{"unterminated string", `"unterminated`},
		{"uppercase parameter key", `c; Ttl=1`},
		{"ttl beyond 15 digits", `c; ttl=1234567890123456`},
		{"ttl as string", `c; ttl="1"`},
		{"hit as integer", `c; hit=1`},
		{"fwd as string", `c; fwd="stale"`},
		{"fwd-status below range", `c; fwd-status=99`},
		{"fwd-status above range", `c; fwd-status=600`},
		{"key as token", `c; key=abc`},
		{"ttl as decimal", `c; ttl=1.5`},
		{"parameter without key", `c;`},
		{"parameter key starts with equals", `c; =1`},
		{"control byte in string", "\"\x01\""},
		{"escaped control in string", `"\x01"`},
		{"invalid escape", `"\q"`},
		{"empty after comma with spaces then member", `c,  ,d`},
		{"tab before semicolon", "c\t; hit"},
		{"space before semicolon", `c ; hit`},
		{"byte sequence unterminated", `c; foo=:aGk=`},
		{"boolean out of set", `c; hit=?2`},
		{"decimal with four fraction digits", `c; ttl=1.2345`},
		{"sixty-five members", strings.Repeat("c, ", 64) + "c"},
		{"input over the byte limit", strings.Repeat("c", MaxStatusBytes+1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseStatus(tt.input)
			if err == nil {
				t.Fatalf("ParseStatus(%q) = %+v, want error", tt.input, got)
			}
			if !errors.Is(err, ErrMalformedStatus) {
				t.Fatalf("ParseStatus(%q) error %v does not wrap ErrMalformedStatus", tt.input, err)
			}
		})
	}
}

func TestParseStatusAccepts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  []Status
	}{
		{"leading space", `  c`, []Status{{Name: "c"}}},
		{"empty", ``, nil},
		{"only spaces", `   `, nil},
		{"optional whitespace around comma", `c  ,  d`, []Status{{Name: "c"}, {Name: "d"}}},
		{"unknown parameter with byte sequence", `c; foo=:aGk=:; hit`, []Status{{Name: "c", Hit: true}}},
		{"detail as token", `c; detail=token`, []Status{{Name: "c", Detail: "token"}}},
		{"hit false", `c; hit=?0`, []Status{{Name: "c"}}},
		{"hit true", `c; hit=?1`, []Status{{Name: "c", Hit: true}}},
		{"fwd-status at range edge", `c; fwd-status=599`, []Status{{Name: "c", FwdStatus: 599, HasFwdStatus: true}}},
		{"ttl upper bound", `c; ttl=999999999999999`, []Status{{Name: "c", TTL: 999999999999999, HasTTL: true}}},
		{"duplicate parameter overwrites", `c; ttl=1; ttl=2`, []Status{{Name: "c", TTL: 2, HasTTL: true}}},
		{"quoted name", `"my cache"`, []Status{{Name: "my cache"}}},
		{"escaped key", `c; key="k\"\\"`, []Status{{Name: "c", Key: `k"\`}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseStatus(tt.input)
			if err != nil {
				t.Fatalf("ParseStatus(%q): %v", tt.input, err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("ParseStatus(%q) = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseStatusAcceptsMaxMembers(t *testing.T) {
	t.Parallel()
	got, err := ParseStatus(strings.Repeat("c, ", MaxStatusMembers-1) + "c")
	if err != nil {
		t.Fatalf("ParseStatus: %v", err)
	}
	if len(got) != MaxStatusMembers {
		t.Fatalf("got %d members, want %d", len(got), MaxStatusMembers)
	}
}
