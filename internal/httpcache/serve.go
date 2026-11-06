package httpcache

import (
	"net/http"
	"net/textproto"
	"slices"
	"strings"
	"time"
)

// StoredHeader returns a copy of h for storage without the hop-by-hop fields, the fields named by a Connection header, and Age.
func StoredHeader(h http.Header) http.Header {
	out := make(http.Header, len(h))
	named := map[string]bool{}
	for k, vs := range h {
		if textproto.CanonicalMIMEHeaderKey(k) != "Connection" {
			continue
		}
		for _, line := range vs {
			for _, part := range strings.Split(line, ",") {
				if n := trimOWS(part); n != "" {
					named[textproto.CanonicalMIMEHeaderKey(n)] = true
				}
			}
		}
	}
	for k, v := range h {
		ck := textproto.CanonicalMIMEHeaderKey(k)
		if isHopByHop(ck) || named[ck] || ck == "Age" {
			continue
		}
		out[k] = slices.Clone(v)
	}
	return out
}

// StoredLifetime returns the freshness lifetime of a stored response with header h under the options o.
func StoredLifetime(h http.Header, o Options) time.Duration {
	return lifetime(ParseCacheControl(h), h, time.Time{}, false, o)
}

// NotModified reports whether a GET or HEAD request req can be answered with 304 Not Modified from a stored response with header h. If-None-Match takes precedence over If-Modified-Since.
func NotModified(req *http.Request, h http.Header) bool {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		return false
	}
	if inm := req.Header.Values("If-None-Match"); len(inm) > 0 {
		return etagListMatches(inm, h.Get("ETag"))
	}
	ims, err := http.ParseTime(req.Header.Get("If-Modified-Since"))
	if err != nil {
		return false
	}
	ref, err := http.ParseTime(h.Get("Last-Modified"))
	if err != nil {
		ref, err = http.ParseTime(h.Get("Date"))
		if err != nil {
			return false
		}
	}
	return !ref.After(ims)
}

func etagListMatches(lines []string, stored string) bool {
	for _, line := range lines {
		for _, part := range strings.Split(line, ",") {
			m := trimOWS(part)
			if m == "*" {
				if stored != "" {
					return true
				}
				continue
			}
			if !validEntityTag(m) || stored == "" {
				continue
			}
			if weak(m) == weak(stored) {
				return true
			}
		}
	}
	return false
}

func validEntityTag(s string) bool {
	s = weak(s)
	return len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"'
}

// NotModifiedHeader returns a copy of the validator and freshness fields of h (Cache-Control, Content-Location, Date, Etag, Expires, Last-Modified and Vary) for a 304 response.
func NotModifiedHeader(h http.Header) http.Header {
	fields := []string{"Cache-Control", "Content-Location", "Date", "Etag", "Expires", "Last-Modified", "Vary"}
	out := make(http.Header, len(fields))
	for _, name := range fields {
		if vs := h.Values(name); len(vs) > 0 {
			out[name] = slices.Clone(vs)
		}
	}
	return out
}
