package cache

import (
	"bufio"
	"net"
	"net/http"
	"net/textproto"
	"slices"
	"strconv"
	"time"

	"github.com/ok0/go-microgateway/internal/httpcache"
)

// serve writes the stored response e to w. age is the current age of the entry and st the Cache-Status
// member to report; its TTL is derived from the stored lifetime and age.
func (s *stage) serve(w http.ResponseWriter, r *http.Request, e *httpcache.Entry, age time.Duration, st httpcache.Status) {
	lifetime := httpcache.StoredLifetime(e.Header, s.opts)
	st.TTL, st.HasTTL = httpcache.TTLSeconds(httpcache.Remaining(lifetime, age)), true
	h := w.Header()
	for k, vs := range e.Header {
		h[textproto.CanonicalMIMEHeaderKey(k)] = slices.Clone(vs)
	}
	if httpcache.NotModified(r, e.Header) {
		keep := httpcache.NotModifiedHeader(e.Header)
		for k := range e.Header {
			if ck := textproto.CanonicalMIMEHeaderKey(k); keep[ck] == nil {
				h.Del(ck)
			}
		}
		h.Del("Content-Length")
		h.Set("Age", strconv.FormatInt(httpcache.AgeSeconds(age), 10))
		appendCacheStatus(h, s.member(st))
		w.WriteHeader(http.StatusNotModified)
		return
	}
	h.Set("Age", strconv.FormatInt(httpcache.AgeSeconds(age), 10))
	appendCacheStatus(h, s.member(st))
	if s.stripTag {
		h.Del(s.tagHeader)
	}
	bodyBearing := e.Status >= 200 && e.Status != http.StatusNoContent && e.Status != http.StatusNotModified
	if r.Method == http.MethodGet && bodyBearing {
		h.Set("Content-Length", strconv.Itoa(len(e.Body)))
	}
	w.WriteHeader(e.Status)
	if r.Method != http.MethodHead && bodyBearing {
		_, _ = w.Write(e.Body)
	}
}

// serveShared serves a response shared by a leader. It reports false, writing nothing, when the variant of the
// waiter differs from the leader's.
func (s *stage) serveShared(w http.ResponseWriter, r *http.Request, in fillInput, sh *sharedResponse) bool {
	h0 := sh.header
	if sh.merged != nil {
		h0 = sh.merged.Header
	}
	names, star := httpcache.VaryNames(h0)
	sec, ok := httpcache.SecondaryKey(names, r)
	if star || !ok || sec != sh.secondary {
		return false
	}
	st := sh.st
	st.Stored = false
	st.Collapsed = true
	st.Key = in.key
	if sh.merged != nil {
		s.serve(w, r, sh.merged, sh.age, st)
		return true
	}
	h := w.Header()
	for k, vs := range sh.header {
		h[textproto.CanonicalMIMEHeaderKey(k)] = slices.Clone(vs)
	}
	if httpcache.NotModified(r, sh.header) {
		keep := httpcache.NotModifiedHeader(sh.header)
		for k := range sh.header {
			if ck := textproto.CanonicalMIMEHeaderKey(k); keep[ck] == nil && ck != "Age" {
				h.Del(ck)
			}
		}
		h.Del("Content-Length")
		appendCacheStatus(h, s.member(st))
		w.WriteHeader(http.StatusNotModified)
		return true
	}
	appendCacheStatus(h, s.member(st))
	w.WriteHeader(sh.status)
	bodyBearing := sh.status >= 200 && sh.status != http.StatusNoContent && sh.status != http.StatusNotModified
	if r.Method != http.MethodHead && bodyBearing {
		_, _ = w.Write(sh.body)
	}
	return true
}

// statusWriter appends one Cache-Status member to the response it wraps, before the first final header is written.
type statusWriter struct {
	http.ResponseWriter
	member string
	wrote  bool
	code   int
}

func (sw *statusWriter) WriteHeader(code int) {
	if sw.wrote || (code >= 100 && code < 200 && code != http.StatusSwitchingProtocols) {
		sw.ResponseWriter.WriteHeader(code)
		return
	}
	sw.wrote = true
	sw.code = code
	appendCacheStatus(sw.Header(), sw.member)
	sw.ResponseWriter.WriteHeader(code)
}

func (sw *statusWriter) Write(p []byte) (int, error) {
	if !sw.wrote {
		sw.WriteHeader(http.StatusOK)
	}
	return sw.ResponseWriter.Write(p)
}

func (sw *statusWriter) Flush() {
	if !sw.wrote {
		sw.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(sw.ResponseWriter).Flush()
}

func (sw *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(sw.ResponseWriter).Hijack()
}

func (sw *statusWriter) Unwrap() http.ResponseWriter {
	return sw.ResponseWriter
}
