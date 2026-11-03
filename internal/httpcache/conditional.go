package httpcache

import (
	"net/http"
	"net/textproto"
	"slices"
	"strings"
)

// Conditional returns a clone of r that revalidates e, or nil when e has no validator.
func Conditional(r *http.Request, e *Entry) *http.Request {
	if e == nil || !HasValidator(e.Header) {
		return nil
	}
	c := r.Clone(r.Context())
	for _, k := range []string{"If-None-Match", "If-Modified-Since", "If-Match", "If-Unmodified-Since", "If-Range"} {
		c.Header.Del(k)
	}
	if v := e.Header.Get("ETag"); v != "" {
		c.Header.Set("If-None-Match", v)
	}
	if v := e.Header.Get("Last-Modified"); v != "" {
		c.Header.Set("If-Modified-Since", v)
	}
	return c
}

// Merge304 applies the header fields of a 304 response h to the stored entry e (RFC 9111
// §4.3.4). It refuses when the validators do not match. e is never modified.
func Merge304(e *Entry, h http.Header) (*Entry, bool) {
	nE := h.Get("ETag")
	sE := e.Header.Get("ETag")
	switch {
	case nE != "":
		if sE == "" || weak(nE) != weak(sE) {
			return nil, false
		}
	case sE != "":
		return nil, false
	default:
		if lm := h.Get("Last-Modified"); lm != "" {
			sl := e.Header.Get("Last-Modified")
			if sl == "" || !sameTime(lm, sl) {
				return nil, false
			}
		}
	}
	named := map[string]bool{}
	for _, src := range []http.Header{h, e.Header} {
		for _, line := range src.Values("Connection") {
			for _, part := range strings.Split(line, ",") {
				if n := trimOWS(part); n != "" {
					named[textproto.CanonicalMIMEHeaderKey(n)] = true
				}
			}
		}
	}
	excluded := func(k string) bool {
		ck := textproto.CanonicalMIMEHeaderKey(k)
		return isHopByHop(ck) || named[ck]
	}
	merged := make(http.Header, len(e.Header))
	for k, v := range e.Header {
		merged[k] = slices.Clone(v)
	}
	for k := range merged {
		if excluded(k) {
			delete(merged, k)
		}
	}
	for k, v := range h {
		if excluded(k) || isNotCopiedFrom304(textproto.CanonicalMIMEHeaderKey(k)) {
			continue
		}
		merged[textproto.CanonicalMIMEHeaderKey(k)] = slices.Clone(v)
	}
	if _, ok := singleDate(h); !ok {
		merged.Del("Date")
	}
	return &Entry{Status: e.Status, Header: merged, Body: e.Body, InitialAge: e.InitialAge, Stale: false}, true
}

func isHopByHop(k string) bool {
	switch k {
	case "Connection", "Keep-Alive", "Proxy-Connection", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
		"Proxy-Authenticate", "Proxy-Authentication-Info", "Proxy-Authorization":
		return true
	}
	return false
}

func isNotCopiedFrom304(k string) bool {
	switch k {
	case "Content-Length", "Content-Encoding", "Content-Range", "Set-Cookie":
		return true
	}
	return false
}

func weak(s string) string {
	return strings.TrimPrefix(s, "W/")
}

func sameTime(a, b string) bool {
	ta, errA := http.ParseTime(a)
	tb, errB := http.ParseTime(b)
	if errA != nil || errB != nil {
		return a == b
	}
	return ta.Equal(tb)
}
