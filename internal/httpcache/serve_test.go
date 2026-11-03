package httpcache

import (
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestStoredHeader(t *testing.T) {
	t.Parallel()
	in := http.Header{
		"Connection":        {"x-foo"},
		"X-Foo":             {"secret"},
		"Keep-Alive":        {"timeout=5"},
		"Transfer-Encoding": {"chunked"},
		"Trailer":           {"X-Sum"},
		"Upgrade":           {"websocket"},
		"Age":               {"5"},
		"Cache-Status":      {"up; hit"},
		"Etag":              {`"v1"`},
	}
	snapshot := in.Clone()
	out := StoredHeader(in)
	want := http.Header{"Cache-Status": {"up; hit"}, "Etag": {`"v1"`}}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("StoredHeader = %v, want %v", out, want)
	}
	if !reflect.DeepEqual(in, snapshot) {
		t.Fatal("input header changed")
	}
	out.Set("Etag", `"changed"`)
	if in.Get("Etag") != `"v1"` {
		t.Fatal("output shares storage with input")
	}
	empty := StoredHeader(nil)
	if empty == nil || len(empty) != 0 {
		t.Fatalf("StoredHeader(nil) = %v, want empty non-nil", empty)
	}
}

func TestStoredLifetimeMatchesDecide(t *testing.T) {
	t.Parallel()
	const minute = time.Minute
	tests := []struct {
		name string
		h    http.Header
		o    Options
		want time.Duration
	}{
		{"max-age", http.Header{"Cache-Control": {"max-age=60"}}, Options{MaxTTL: time.Hour}, minute},
		{"s-maxage over max-age", http.Header{"Cache-Control": {"s-maxage=30, max-age=60"}}, Options{MaxTTL: time.Hour}, 30 * time.Second},
		{"Expires with Date", http.Header{"Date": {fmtDate(t0)}, "Expires": {fmtDate(t0.Add(2 * minute))}}, Options{MaxTTL: time.Hour}, 2 * minute},
		{"Expires without Date", http.Header{"Expires": {fmtDate(t0.Add(2 * minute))}}, Options{MaxTTL: time.Hour}, 0},
		{"default_ttl", http.Header{}, Options{DefaultTTL: 90 * time.Second, MaxTTL: time.Hour}, 90 * time.Second},
		{"max_ttl clamp", http.Header{"Cache-Control": {"max-age=3600"}}, Options{MaxTTL: 5 * minute}, 5 * minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := StoredLifetime(tc.h, tc.o)
			if got != tc.want {
				t.Fatalf("StoredLifetime = %v, want %v", got, tc.want)
			}
			e := &Entry{Status: http.StatusOK, Header: tc.h}
			r := newReq(http.Header{})
			if tc.want == 0 {
				if Decide(r, e, 0, tc.o) == Fresh {
					t.Fatal("entry with zero lifetime is fresh at age 0")
				}
				return
			}
			if Decide(r, e, tc.want-time.Nanosecond, tc.o) != Fresh {
				t.Fatal("entry not fresh at lifetime-1ns")
			}
			if Decide(r, e, tc.want, tc.o) == Fresh {
				t.Fatal("entry still fresh at lifetime")
			}
		})
	}
}

func TestNotModified(t *testing.T) {
	t.Parallel()
	later := fmtDate(t0.Add(time.Hour))
	tests := []struct {
		name   string
		method string
		req    http.Header
		h      http.Header
		want   bool
	}{
		{"INM weak matches strong", http.MethodGet, http.Header{"If-None-Match": {`"a"`}}, http.Header{"Etag": {`W/"a"`}}, true},
		{"INM list with one match", http.MethodGet, http.Header{"If-None-Match": {`"x", "a"`}}, http.Header{"Etag": {`"a"`}}, true},
		{"INM mismatch", http.MethodGet, http.Header{"If-None-Match": {`"b"`}}, http.Header{"Etag": {`"a"`}}, false},
		{"INM star with ETag", http.MethodGet, http.Header{"If-None-Match": {"*"}}, http.Header{"Etag": {`"a"`}}, true},
		{"INM star without ETag", http.MethodGet, http.Header{"If-None-Match": {"*"}}, http.Header{"Last-Modified": {fmtDate(t0)}}, false},
		{"INM malformed member ignored", http.MethodGet, http.Header{"If-None-Match": {`a, "a"`}}, http.Header{"Etag": {`"a"`}}, true},
		{"INM present no match ignores IMS", http.MethodGet, http.Header{"If-None-Match": {`"b"`}, "If-Modified-Since": {later}}, http.Header{"Etag": {`"a"`}, "Last-Modified": {fmtDate(t0)}}, false},
		{"IMS equal Last-Modified", http.MethodGet, http.Header{"If-Modified-Since": {fmtDate(t0)}}, http.Header{"Last-Modified": {fmtDate(t0)}}, true},
		{"IMS after Last-Modified", http.MethodGet, http.Header{"If-Modified-Since": {later}}, http.Header{"Last-Modified": {fmtDate(t0)}}, true},
		{"IMS before Last-Modified", http.MethodGet, http.Header{"If-Modified-Since": {fmtDate(t0.Add(-time.Hour))}}, http.Header{"Last-Modified": {fmtDate(t0)}}, false},
		{"IMS against Date without Last-Modified", http.MethodGet, http.Header{"If-Modified-Since": {fmtDate(t0)}}, http.Header{"Date": {fmtDate(t0)}}, true},
		{"IMS without any reference", http.MethodGet, http.Header{"If-Modified-Since": {fmtDate(t0)}}, http.Header{}, false},
		{"HEAD with matching INM", http.MethodHead, http.Header{"If-None-Match": {`"a"`}}, http.Header{"Etag": {`"a"`}}, true},
		{"POST with matching INM", http.MethodPost, http.Header{"If-None-Match": {`"a"`}}, http.Header{"Etag": {`"a"`}}, false},
		{"unparsable IMS", http.MethodGet, http.Header{"If-Modified-Since": {"yesterday"}}, http.Header{"Last-Modified": {fmtDate(t0)}}, false},
		{"no preconditions", http.MethodGet, http.Header{}, http.Header{"Etag": {`"a"`}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newReq(tc.req)
			r.Method = tc.method
			if got := NotModified(r, tc.h); got != tc.want {
				t.Fatalf("NotModified = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNotModifiedHeader(t *testing.T) {
	t.Parallel()
	in := http.Header{
		"Cache-Control":    {"max-age=60"},
		"Content-Location": {"/a"},
		"Date":             {fmtDate(t0)},
		"Etag":             {`"a"`},
		"Expires":          {fmtDate(t0.Add(time.Hour))},
		"Last-Modified":    {fmtDate(t0)},
		"Vary":             {"Accept"},
		"Content-Type":     {"text/plain"},
		"Content-Length":   {"4"},
		"Age":              {"5"},
		"Cache-Status":     {"up; hit"},
		"Set-Cookie":       {"a=b"},
	}
	out := NotModifiedHeader(in)
	if len(out) != 7 {
		t.Fatalf("NotModifiedHeader has %d fields, want 7: %v", len(out), out)
	}
	for _, name := range []string{"Cache-Control", "Content-Location", "Date", "Etag", "Expires", "Last-Modified", "Vary"} {
		if !reflect.DeepEqual(out[http.CanonicalHeaderKey(name)], in[http.CanonicalHeaderKey(name)]) {
			t.Fatalf("%s not copied", name)
		}
	}
}
