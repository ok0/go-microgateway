package httpcache

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

type storableCase struct {
	name     string
	method   string
	req      http.Header
	status   int
	resp     http.Header
	complete bool
	o        Options
	ok       bool
	reason   string
}

func baseStorableCase(name string, mod func(c *storableCase)) storableCase {
	c := storableCase{
		name:     name,
		method:   http.MethodGet,
		req:      http.Header{},
		status:   http.StatusOK,
		resp:     http.Header{"Cache-Control": {"max-age=60"}},
		complete: true,
		o: Options{
			Statuses:  []int{200, 203, 204, 300, 301, 308, 404, 405, 410, 414, 501},
			MaxTTL:    24 * time.Hour,
			TagHeader: "Surrogate-Key",
		},
		ok: true,
	}
	if mod != nil {
		mod(&c)
	}
	return c
}

func TestStorable(t *testing.T) {
	t.Parallel()
	cases := []storableCase{
		baseStorableCase("passing base", nil),
		baseStorableCase("POST", func(c *storableCase) { c.method = http.MethodPost; c.ok = false; c.reason = ReasonMethod }),
		baseStorableCase("request no-store", func(c *storableCase) {
			c.req = http.Header{"Cache-Control": {"no-store"}}
			c.ok, c.reason = false, ReasonRequestNoStore
		}),
		baseStorableCase("Range", func(c *storableCase) {
			c.req = http.Header{"Range": {"bytes=0-1"}}
			c.ok, c.reason = false, ReasonRange
		}),
		baseStorableCase("206", func(c *storableCase) {
			c.status = 206
			c.ok, c.reason = false, ReasonPartialContent
		}),
		baseStorableCase("304", func(c *storableCase) {
			c.status = 304
			c.ok, c.reason = false, ReasonNotModified
		}),
		baseStorableCase("500 not listed", func(c *storableCase) {
			c.status = 500
			c.ok, c.reason = false, ReasonStatus
		}),
		baseStorableCase("incomplete", func(c *storableCase) {
			c.complete = false
			c.ok, c.reason = false, ReasonIncomplete
		}),
		baseStorableCase("response no-store", func(c *storableCase) {
			c.resp = http.Header{"Cache-Control": {"no-store, max-age=60"}}
			c.ok, c.reason = false, ReasonNoStore
		}),
		baseStorableCase("private", func(c *storableCase) {
			c.resp = http.Header{"Cache-Control": {"private, max-age=60"}}
			c.ok, c.reason = false, ReasonPrivate
		}),
		baseStorableCase("private field list", func(c *storableCase) {
			c.resp = http.Header{"Cache-Control": {`private="x", max-age=60`}}
			c.ok, c.reason = false, ReasonPrivate
		}),
		baseStorableCase("no-cache without validator", func(c *storableCase) {
			c.resp = http.Header{"Cache-Control": {"no-cache, max-age=60"}}
			c.ok, c.reason = false, ReasonNoCacheWithoutValidator
		}),
		baseStorableCase("no-cache with ETag", func(c *storableCase) {
			c.resp = http.Header{"Cache-Control": {"no-cache, max-age=60"}, "Etag": {`"v1"`}}
		}),
		baseStorableCase("Vary star", func(c *storableCase) {
			c.resp = http.Header{"Cache-Control": {"max-age=60"}, "Vary": {"*"}}
			c.ok, c.reason = false, ReasonVaryStar
		}),
		baseStorableCase("Vary star among lines", func(c *storableCase) {
			c.resp = http.Header{"Cache-Control": {"max-age=60"}, "Vary": {"Accept", "*"}}
			c.ok, c.reason = false, ReasonVaryStar
		}),
		baseStorableCase("Set-Cookie", func(c *storableCase) {
			c.resp = http.Header{"Cache-Control": {"max-age=60"}, "Set-Cookie": {"a=b"}}
			c.ok, c.reason = false, ReasonSetCookie
		}),
		baseStorableCase("17 Vary names refused", func(c *storableCase) {
			c.resp = http.Header{"Cache-Control": {"max-age=60"}, "Vary": {varyList(17)}}
			c.ok, c.reason = false, ReasonVaryLimit
		}),
		baseStorableCase("16 Vary names stored", func(c *storableCase) {
			c.resp = http.Header{"Cache-Control": {"max-age=60"}, "Vary": {varyList(16)}}
		}),
		baseStorableCase("tag header over 16 KiB", func(c *storableCase) {
			c.resp = http.Header{"Cache-Control": {"max-age=60"}, "Surrogate-Key": {strings.Repeat("t ", 9000)}}
			c.ok, c.reason = false, ReasonTags
		}),
		baseStorableCase("no freshness and zero DefaultTTL", func(c *storableCase) {
			c.resp = http.Header{}
			c.ok, c.reason = false, ReasonZeroTTL
		}),
		baseStorableCase("no freshness with keep and validator", func(c *storableCase) {
			c.resp = http.Header{"Etag": {`"v1"`}}
			c.o.Keep = time.Hour
			c.o.DefaultTTL = 0
		}),
		baseStorableCase("Authorization with max-age", func(c *storableCase) {
			c.req = http.Header{"Authorization": {"Bearer x"}}
			c.ok, c.reason = false, ReasonCredentialed
		}),
		baseStorableCase("Authorization with public", func(c *storableCase) {
			c.req = http.Header{"Authorization": {"Bearer x"}}
			c.resp = http.Header{"Cache-Control": {"public, max-age=60"}}
		}),
		baseStorableCase("Authorization with s-maxage", func(c *storableCase) {
			c.req = http.Header{"Authorization": {"Bearer x"}}
			c.resp = http.Header{"Cache-Control": {"s-maxage=60"}}
		}),
		baseStorableCase("Authorization with must-revalidate", func(c *storableCase) {
			c.req = http.Header{"Authorization": {"Bearer x"}}
			c.resp = http.Header{"Cache-Control": {"must-revalidate, max-age=60"}}
		}),
		baseStorableCase("Authorization with only DefaultTTL", func(c *storableCase) {
			c.req = http.Header{"Authorization": {"Bearer x"}}
			c.resp = http.Header{}
			c.o.DefaultTTL = time.Hour
			c.ok, c.reason = false, ReasonCredentialed
		}),
		baseStorableCase("Credential func without public", func(c *storableCase) {
			c.o.Credential = func(*http.Request) bool { return true }
			c.ok, c.reason = false, ReasonCredentialed
		}),
		baseStorableCase("uncredentialed with only DefaultTTL", func(c *storableCase) {
			c.resp = http.Header{}
			c.o.DefaultTTL = time.Hour
		}),
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := &http.Request{Method: tc.method, Header: tc.req}
			ok, reason := Storable(req, ResponseMeta{
				Status:       tc.status,
				Header:       tc.resp,
				Complete:     tc.complete,
				ResponseTime: t0,
			}, tc.o)
			if ok != tc.ok || reason != tc.reason {
				t.Fatalf("Storable = (%v, %q), want (%v, %q)", ok, reason, tc.ok, tc.reason)
			}
		})
	}
}

func TestCheckSide(t *testing.T) {
	t.Parallel()
	credential := func(*http.Request) bool { return true }
	cases := []struct {
		name string
		req  http.Header
		resp http.Header
		cred func(*http.Request) bool
		want Verdict
	}{
		{"storable", http.Header{}, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, nil, Verdict{true, "", SideNone}},
		{"request no-store", http.Header{"Cache-Control": {"no-store"}}, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, nil, Verdict{false, ReasonRequestNoStore, SideRequest}},
		{"request no-store, response private", http.Header{"Cache-Control": {"no-store"}}, http.Header{"Cache-Control": {"private, max-age=60"}, "Etag": {`"v1"`}}, nil, Verdict{false, ReasonRequestNoStore, SideResponse}},
		{"range", http.Header{"Range": {"bytes=0-1"}}, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, nil, Verdict{false, ReasonRange, SideRequest}},
		{"authorization", http.Header{"Authorization": {"Bearer t"}}, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, nil, Verdict{false, ReasonCredentialed, SideRequest}},
		{"authorization, public", http.Header{"Authorization": {"Bearer t"}}, http.Header{"Cache-Control": {"public, max-age=60"}, "Etag": {`"v1"`}}, nil, Verdict{true, "", SideNone}},
		{"credential func", http.Header{}, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, credential, Verdict{false, ReasonCredentialed, SideRequest}},
		{"authorization, private", http.Header{"Authorization": {"Bearer t"}}, http.Header{"Cache-Control": {"private, max-age=60"}, "Etag": {`"v1"`}}, nil, Verdict{false, ReasonPrivate, SideResponse}},
		{"authorization, no freshness", http.Header{"Authorization": {"Bearer t"}}, http.Header{}, nil, Verdict{false, ReasonCredentialed, SideResponse}},
		{"set-cookie", http.Header{}, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}, "Set-Cookie": {"s=1"}}, nil, Verdict{false, ReasonSetCookie, SideResponse}},
		{"vary star", http.Header{}, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}, "Vary": {"*"}}, nil, Verdict{false, ReasonVaryStar, SideResponse}},
		{"no-store", http.Header{}, http.Header{"Cache-Control": {"no-store, max-age=60"}, "Etag": {`"v1"`}}, nil, Verdict{false, ReasonNoStore, SideResponse}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := &http.Request{Method: http.MethodGet, Header: tc.req}
			meta := ResponseMeta{Status: http.StatusOK, Header: tc.resp, Complete: true, ResponseTime: t0}
			o := Options{Statuses: []int{http.StatusOK}, MaxTTL: 24 * time.Hour, TagHeader: "Surrogate-Key", Credential: tc.cred}
			if got := Check(req, meta, o); got != tc.want {
				t.Fatalf("Check = %+v, want %+v", got, tc.want)
			}
			ok, reason := Storable(req, meta, o)
			if ok != tc.want.Storable || reason != tc.want.Reason {
				t.Fatalf("Storable = (%v, %q), want (%v, %q)", ok, reason, tc.want.Storable, tc.want.Reason)
			}
		})
	}
}

func varyList(n int) string {
	names := make([]string, 0, n)
	for i := range n {
		names = append(names, fmt.Sprintf("X-H%d", i))
	}
	return strings.Join(names, ", ")
}

func TestReasonsDistinct(t *testing.T) {
	t.Parallel()
	reasons := []string{
		ReasonMethod, ReasonRequestNoStore, ReasonRange, ReasonPartialContent, ReasonNotModified,
		ReasonStatus, ReasonIncomplete, ReasonNoStore, ReasonPrivate, ReasonVaryStar, ReasonSetCookie,
		ReasonNoCacheWithoutValidator, ReasonVaryLimit, ReasonTags, ReasonCredentialed, ReasonZeroTTL,
	}
	seen := map[string]bool{}
	for _, r := range reasons {
		if r == "" {
			t.Fatal("empty reason constant")
		}
		if seen[r] {
			t.Fatalf("duplicate reason %q", r)
		}
		seen[r] = true
	}
	if len(seen) != 16 {
		t.Fatalf("got %d reasons, want 16", len(seen))
	}
}

func TestVaryNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		lines     []string
		wantNames []string
		wantStar  bool
	}{
		{"dedupe case and order", []string{"Accept, accept-encoding", "ACCEPT, ,"}, []string{"accept", "accept-encoding"}, false},
		{"star across lines", []string{"X-A", "*", "x-b"}, []string{"x-a", "x-b"}, true},
		{"empties dropped", []string{" , ,", ""}, nil, false},
		{"sorted", []string{"Z-h, A-h, m-h"}, []string{"a-h", "m-h", "z-h"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := http.Header{}
			for _, l := range tc.lines {
				h.Add("Vary", l)
			}
			names, star := VaryNames(h)
			if star != tc.wantStar || !slices.Equal(names, tc.wantNames) {
				t.Fatalf("VaryNames = (%q, %v), want (%q, %v)", names, star, tc.wantNames, tc.wantStar)
			}
		})
	}
}
