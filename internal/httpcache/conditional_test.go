package httpcache

import (
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestConditional(t *testing.T) {
	t.Parallel()
	clientConditionals := func() http.Header {
		return http.Header{
			"If-None-Match":       {`"old"`},
			"If-Modified-Since":   {"Mon, 01 Jan 2024 00:00:00 GMT"},
			"If-Match":            {`"m"`},
			"If-Unmodified-Since": {"Mon, 01 Jan 2024 00:00:00 GMT"},
			"If-Range":            {`"r"`},
			"Accept":              {"text/html"},
		}
	}
	names := []string{"If-None-Match", "If-Modified-Since", "If-Match", "If-Unmodified-Since", "If-Range"}
	tests := []struct {
		name    string
		entry   http.Header
		wantNil bool
		want    map[string]string
	}{
		{"ETag only", http.Header{"Etag": {`"v1"`}}, false, map[string]string{"If-None-Match": `"v1"`}},
		{"Last-Modified only", http.Header{"Last-Modified": {fmtDate(t0)}}, false, map[string]string{"If-Modified-Since": fmtDate(t0)}},
		{"both", http.Header{"Etag": {`"v1"`}, "Last-Modified": {fmtDate(t0)}}, false, map[string]string{"If-None-Match": `"v1"`, "If-Modified-Since": fmtDate(t0)}},
		{"neither", http.Header{"Date": {fmtDate(t0)}}, true, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newReq(clientConditionals())
			snapshot := r.Header.Clone()
			c := Conditional(r, &Entry{Header: tc.entry})
			if !reflect.DeepEqual(r.Header, snapshot) {
				t.Fatal("original request headers changed")
			}
			if tc.wantNil {
				if c != nil {
					t.Fatal("Conditional returned a request without a validator")
				}
				return
			}
			for _, n := range names {
				if got := c.Header.Get(n); got != tc.want[n] {
					t.Fatalf("%s = %q, want %q", n, got, tc.want[n])
				}
			}
			if c.Header.Get("Accept") != "text/html" {
				t.Fatal("unrelated header not kept")
			}
		})
	}
}

func storedEntry(mut func(h http.Header)) *Entry {
	h := http.Header{
		"Etag":             {`"v1"`},
		"Date":             {fmtDate(t0)},
		"Cache-Control":    {"max-age=60"},
		"Expires":          {fmtDate(t0.Add(time.Hour))},
		"Content-Length":   {"2"},
		"Content-Encoding": {"gzip"},
		"Content-Range":    {"bytes 0-1/3"},
		"Set-Cookie":       {"s=1"},
		"X-Old":            {"o"},
	}
	if mut != nil {
		mut(h)
	}
	return &Entry{Status: 200, Header: h, Body: []byte("ok"), InitialAge: time.Second, Stale: true}
}

func TestMerge304(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		entry  *Entry
		h      http.Header
		wantOK bool
		check  func(t *testing.T, m *Entry)
	}{
		{
			name:   "allowed update",
			entry:  storedEntry(nil),
			h:      http.Header{"Etag": {`"v1"`}, "Cache-Control": {"max-age=120"}, "Expires": {fmtDate(t0.Add(2 * time.Hour))}, "Date": {fmtDate(t0.Add(time.Second))}, "X-Old": {"new"}},
			wantOK: true,
			check: func(t *testing.T, m *Entry) {
				if m.Header.Get("Cache-Control") != "max-age=120" || m.Header.Get("Expires") != fmtDate(t0.Add(2*time.Hour)) ||
					m.Header.Get("Date") != fmtDate(t0.Add(time.Second)) || m.Header.Get("X-Old") != "new" {
					t.Fatalf("allowed fields not replaced: %v", m.Header)
				}
			},
		},
		{
			name:   "content fields and Set-Cookie not applied from 304",
			entry:  storedEntry(nil),
			h:      http.Header{"Etag": {`"v1"`}, "Content-Length": {"99"}, "Content-Encoding": {"br"}, "Content-Range": {"bytes 0-1/9"}, "Set-Cookie": {"x=2"}},
			wantOK: true,
			check: func(t *testing.T, m *Entry) {
				if m.Header.Get("Content-Length") != "2" || m.Header.Get("Content-Encoding") != "gzip" ||
					m.Header.Get("Content-Range") != "bytes 0-1/3" || !reflect.DeepEqual(m.Header.Values("Set-Cookie"), []string{"s=1"}) {
					t.Fatalf("304 content fields applied: %v", m.Header)
				}
			},
		},
		{
			name:   "hop-by-hop removed",
			entry:  storedEntry(nil),
			h:      http.Header{"Etag": {`"v1"`}, "Connection": {"X-Hop"}, "X-Hop": {"1"}, "Keep-Alive": {"5"}, "Transfer-Encoding": {"chunked"}},
			wantOK: true,
			check: func(t *testing.T, m *Entry) {
				for _, k := range []string{"Connection", "X-Hop", "Keep-Alive", "Transfer-Encoding"} {
					if _, ok := m.Header[k]; ok {
						t.Fatalf("%s kept in merged header", k)
					}
				}
			},
		},
		{
			name:   "weak 304 ETag matches strong stored",
			entry:  storedEntry(nil),
			h:      http.Header{"Etag": {`W/"v1"`}},
			wantOK: true,
		},
		{
			name:   "strong 304 ETag matches weak stored",
			entry:  storedEntry(func(h http.Header) { h["Etag"] = []string{`W/"v1"`} }),
			h:      http.Header{"Etag": {`"v1"`}},
			wantOK: true,
		},
		{
			name:   "ETag mismatch refused",
			entry:  storedEntry(nil),
			h:      http.Header{"Etag": {`"v2"`}},
			wantOK: false,
		},
		{
			name:   "304 ETag with stored none refused",
			entry:  storedEntry(func(h http.Header) { delete(h, "Etag") }),
			h:      http.Header{"Etag": {`"v1"`}},
			wantOK: false,
		},
		{
			name:   "no ETags and equal Last-Modified accepted",
			entry:  storedEntry(func(h http.Header) { delete(h, "Etag"); h["Last-Modified"] = []string{fmtDate(t0)} }),
			h:      http.Header{"Last-Modified": {fmtDate(t0)}},
			wantOK: true,
		},
		{
			name:   "no ETags and different Last-Modified refused",
			entry:  storedEntry(func(h http.Header) { delete(h, "Etag"); h["Last-Modified"] = []string{fmtDate(t0)} }),
			h:      http.Header{"Last-Modified": {fmtDate(t0.Add(time.Second))}},
			wantOK: false,
		},
		{
			name:   "no ETags and 304 without Last-Modified accepted",
			entry:  storedEntry(func(h http.Header) { delete(h, "Etag") }),
			h:      http.Header{"Cache-Control": {"max-age=30"}},
			wantOK: true,
		},
		{
			name:   "304 without Date removes Date",
			entry:  storedEntry(nil),
			h:      http.Header{"Etag": {`"v1"`}},
			wantOK: true,
			check: func(t *testing.T, m *Entry) {
				if _, ok := m.Header["Date"]; ok {
					t.Fatal("Date kept from stored entry")
				}
			},
		},
		{
			name:   "status body and age kept and Stale cleared",
			entry:  storedEntry(nil),
			h:      http.Header{"Etag": {`"v1"`}},
			wantOK: true,
			check: func(t *testing.T, m *Entry) {
				if m.Status != 200 || string(m.Body) != "ok" || m.InitialAge != time.Second || m.Stale {
					t.Fatalf("merged = %+v", m)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before := tc.entry.Header.Clone()
			m, ok := Merge304(tc.entry, tc.h)
			if !reflect.DeepEqual(tc.entry.Header, before) {
				t.Fatal("input entry header changed")
			}
			if ok != tc.wantOK {
				t.Fatalf("Merge304 ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				if m != nil {
					t.Fatal("refused merge returned an entry")
				}
				return
			}
			if tc.check != nil {
				tc.check(t, m)
			}
		})
	}
}
