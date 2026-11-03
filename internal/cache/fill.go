package cache

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/store"
)

// fillInput describes a request that is forwarded to the origin.
type fillInput struct {
	primary string
	lk      store.Lookup
	e       *httpcache.Entry
	fwd     string
	key     string
	forced  bool
}

// fillWriter is the response writer the origin writes to during a forward. It forwards every byte to the client
// as it arrives, buffers a copy of a storable body within max_body_bytes, and decides at the first final header
// whether the response may be stored.
type fillWriter struct {
	s           *stage
	w           http.ResponseWriter
	r           *http.Request
	hdr         http.Header
	in          fillInput
	conditional bool
	requestTime time.Time

	responseTime time.Time
	final        bool
	swallowed    bool
	failed       bool
	writeErr     error
	code         int
	h304         http.Header

	notStorable bool
	tooLarge    bool
	buffering   bool
	ttl         time.Duration
	tags        []string
	stored      http.Header
	initialAge  time.Duration
	declared    int64
	written     int64
	maxBody     int
	buf         []byte
}

func (s *stage) newFill(w http.ResponseWriter, r *http.Request, in fillInput, conditional bool) *fillWriter {
	return &fillWriter{
		s:           s,
		w:           w,
		r:           r,
		hdr:         w.Header().Clone(),
		in:          in,
		conditional: conditional,
		requestTime: s.clk.Now(),
		maxBody:     s.maxBody,
	}
}

// forward sends the request to the origin through next and settles the response. A revalidate forward is
// conditional when the stored entry carries a validator. A conditional forward that receives 304 refreshes the
// stored entry; when that is not possible the request is forwarded again, unconditionally.
func (s *stage) forward(next http.Handler, w http.ResponseWriter, r *http.Request, in fillInput, revalidate bool) {
	fr, conditional := r, false
	if revalidate {
		if c := httpcache.Conditional(r, in.e); c != nil {
			fr, conditional = c, true
		}
	}
	ex := &reqctx.Exchange{}
	fr = fr.WithContext(reqctx.WithExchange(fr.Context(), ex))
	fw := s.newFill(w, r, in, conditional)
	fw.run(next, fr)
	if fw.swallowed {
		if s.refresh(w, r, fw) {
			return
		}
		s.forward(next, w, r, in, false)
		return
	}
	s.settle(r, fw, ex)
	s.count(resultMiss)
}

// run calls the origin. A panic marks the fill aborted and is propagated.
func (fw *fillWriter) run(next http.Handler, fr *http.Request) {
	done := false
	defer func() {
		if !done {
			fw.s.countStore(storeAborted)
			fw.s.count(resultMiss)
		}
	}()
	next.ServeHTTP(fw, fr)
	done = true
	if !fw.final {
		fw.WriteHeader(http.StatusOK)
	}
}

// Header returns the origin's view of the response header. It is copied to the client at the first final header.
func (fw *fillWriter) Header() http.Header {
	return fw.hdr
}

// WriteHeader handles 1xx responses by passing them through and the first final status by settling the fill.
func (fw *fillWriter) WriteHeader(code int) {
	if fw.final {
		return
	}
	if code >= 100 && code < 200 && code != http.StatusSwitchingProtocols {
		replaceHeader(fw.w.Header(), fw.hdr)
		fw.w.WriteHeader(code)
		return
	}
	fw.final = true
	fw.responseTime = fw.s.clk.Now()
	fw.code = code
	if fw.conditional && code == http.StatusNotModified {
		fw.swallowed = true
		fw.h304 = fw.hdr.Clone()
		return
	}
	fw.commit()
	replaceHeader(fw.w.Header(), fw.hdr)
	fw.w.WriteHeader(code)
}

// commit decides the fill for the final response and sets the client header.
func (fw *fillWriter) commit() {
	s := fw.s
	code := fw.code
	if v, ok := httpcache.DateToAdd(fw.hdr, fw.responseTime); ok {
		fw.hdr.Set("Date", v)
	}
	st := fw.hdr.Clone()
	fw.notStorable = fw.in.forced || code == http.StatusSwitchingProtocols || len(fw.hdr.Values("Trailer")) > 0 ||
		(fw.in.e != nil && code >= 500)
	if !fw.notStorable {
		ok, _ := httpcache.Storable(fw.r, httpcache.ResponseMeta{Status: code, Header: st, Complete: true, ResponseTime: fw.responseTime}, s.opts)
		fw.notStorable = !ok
	}
	fw.initialAge = httpcache.InitialAge(st, fw.requestTime, fw.responseTime)
	lifetime := httpcache.FreshnessLifetime(st, fw.responseTime, s.opts)
	swr, sie := httpcache.StaleWindows(st, s.opts)
	fw.ttl = httpcache.StorageTTL(lifetime, swr, sie, s.opts.Keep, httpcache.HasValidator(st))
	fw.tags, _ = httpcache.ParseTags(st, s.tagHeader, s.opts.TagDelimiter)
	fw.stored = httpcache.StoredHeader(st)
	if s.stripTag {
		fw.stored.Del(s.tagHeader)
		fw.hdr.Del(s.tagHeader)
	}
	fw.declared = declaredLength(fw.r, fw.hdr, code)
	storable := !fw.notStorable && fw.ttl > 0
	fw.tooLarge = fw.declared > int64(fw.maxBody)
	fw.buffering = storable && fw.r.Method == http.MethodGet && !fw.tooLarge
	if fw.buffering && fw.declared >= 0 {
		fw.buf = make([]byte, 0, fw.declared)
	}
	member := httpcache.Status{Fwd: fw.in.fwd, Key: fw.in.key}
	if storable && fw.declared >= 0 && fw.declared <= int64(fw.maxBody) {
		member.Stored = true
		member.TTL = httpcache.TTLSeconds(httpcache.Remaining(lifetime, fw.initialAge))
		member.HasTTL = true
	}
	appendCacheStatus(fw.hdr, s.member(member))
}

// declaredLength returns the body length the response declares, 0 when it carries no body, or -1 when the length
// is unknown.
func declaredLength(r *http.Request, h http.Header, code int) int64 {
	if r.Method == http.MethodHead || code < 200 || code == http.StatusNoContent || code == http.StatusNotModified {
		return 0
	}
	n, err := strconv.ParseInt(h.Get("Content-Length"), 10, 64)
	if err != nil || n < 0 {
		return -1
	}
	return n
}

// Write sends p to the client and, while buffering, keeps a copy of it.
func (fw *fillWriter) Write(p []byte) (int, error) {
	if !fw.final {
		fw.WriteHeader(http.StatusOK)
	}
	if fw.swallowed {
		return len(p), nil
	}
	if fw.failed {
		return 0, fw.writeErr
	}
	n, err := fw.w.Write(p)
	fw.written += int64(n)
	if err != nil {
		fw.failed = true
		fw.writeErr = err
		fw.buf = nil
		fw.buffering = false
		return n, err
	}
	if fw.buffering {
		fw.appendBody(p)
	}
	return n, nil
}

// appendBody copies p into the buffer. The buffer never grows beyond max_body_bytes; exceeding it abandons the copy.
func (fw *fillWriter) appendBody(p []byte) {
	need := len(fw.buf) + len(p)
	if need > fw.maxBody {
		fw.buf = nil
		fw.buffering = false
		fw.tooLarge = true
		return
	}
	if need > cap(fw.buf) {
		nb := make([]byte, len(fw.buf), min(fw.maxBody, max(2*cap(fw.buf), need)))
		copy(nb, fw.buf)
		fw.buf = nb
	}
	fw.buf = append(fw.buf, p...)
}

// Flush flushes the client response. A swallowed 304 has nothing to flush.
func (fw *fillWriter) Flush() {
	if !fw.final {
		fw.WriteHeader(http.StatusOK)
	}
	if fw.swallowed {
		return
	}
	_ = http.NewResponseController(fw.w).Flush()
}

// Unwrap returns the client response writer.
func (fw *fillWriter) Unwrap() http.ResponseWriter {
	return fw.w
}

// complete reports whether the origin body was read to its end while the client stayed connected, and whether the
// declared length, when there is one, was written in full.
func (fw *fillWriter) complete(r *http.Request, ex *reqctx.Exchange) bool {
	return fw.final && !fw.failed && r.Context().Err() == nil && ex.Complete() &&
		(fw.declared < 0 || fw.written == fw.declared)
}

// settle stores the response of a forward when it qualifies and accounts for the outcome.
func (s *stage) settle(r *http.Request, fw *fillWriter, ex *reqctx.Exchange) {
	var outcome string
	switch {
	case fw.notStorable:
		outcome = storeNotStorable
	case fw.tooLarge:
		outcome = storeTooLarge
	case !fw.complete(r, ex):
		outcome = storeAborted
	default:
		outcome = s.storeEntry(r, fw.in.lk.Token, fw.in.primary, fw.stored, fw.buf, fw.code, fw.tags, fw.initialAge, fw.ttl)
	}
	fw.buf = nil
	s.countStore(outcome)
}

// storeEntry puts one entry under the fill token tok and returns the storage outcome.
func (s *stage) storeEntry(r *http.Request, tok store.FillToken, primary string, header http.Header, body []byte, status int, tags []string, initialAge, ttl time.Duration) string {
	if ttl <= 0 {
		return storeNotStorable
	}
	varyNames, _ := httpcache.VaryNames(header)
	secondary, ok := httpcache.SecondaryKey(varyNames, r)
	if !ok {
		return storeNotStorable
	}
	e := &store.Entry{Status: status, Header: header, Body: body, Tags: tags, InitialAge: initialAge}
	if _, err := store.ValidateFill(tok, primary, secondary, varyNames, e, ttl); err != nil {
		return storeNotStorable
	}
	ctx := context.WithoutCancel(r.Context())
	stored, err := s.put(ctx, tok, primary, secondary, varyNames, e, ttl)
	if err != nil {
		s.fail(ctx, opPut, err)
		return storeError
	}
	if !stored {
		return storeRefused
	}
	return storeStored
}

// refresh applies a 304 received for a conditional forward to the stored entry, stores the merged entry and
// serves it. It returns false, without writing anything, when the 304 cannot be merged.
func (s *stage) refresh(w http.ResponseWriter, r *http.Request, fw *fillWriter) bool {
	in := fw.in
	merged, ok := httpcache.Merge304(in.e, fw.h304)
	if !ok {
		return false
	}
	tags := in.lk.Entry.Tags
	if len(fw.h304.Values(s.tagHeader)) > 0 {
		t, ok := httpcache.ParseTags(fw.h304, s.tagHeader, s.opts.TagDelimiter)
		if !ok {
			return false
		}
		tags = t
	}
	if v, ok := httpcache.DateToAdd(fw.h304, fw.responseTime); ok {
		merged.Header.Set("Date", v)
	}
	initialAge := httpcache.InitialAge(merged.Header, fw.requestTime, fw.responseTime)
	lifetime := httpcache.FreshnessLifetime(merged.Header, fw.responseTime, s.opts)
	swr, sie := httpcache.StaleWindows(merged.Header, s.opts)
	ttl := httpcache.StorageTTL(lifetime, swr, sie, s.opts.Keep, httpcache.HasValidator(merged.Header))
	stored := httpcache.StoredHeader(merged.Header)
	if s.stripTag {
		stored.Del(s.tagHeader)
	}
	s.countStore(s.storeEntry(r, in.lk.Token, in.primary, stored, merged.Body, merged.Status, tags, initialAge, ttl))
	s.serve(w, r, merged, initialAge, httpcache.Status{
		Hit:          true,
		Fwd:          httpcache.FwdStale,
		FwdStatus:    http.StatusNotModified,
		HasFwdStatus: true,
		Key:          in.key,
	})
	s.count(resultRevalidated)
	return true
}

func replaceHeader(dst, src http.Header) {
	clear(dst)
	for k, v := range src {
		dst[k] = v
	}
}
