package cache

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/ok0/go-microgateway/internal/coalesce"
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
	detail  string
	age     time.Duration
	sie     bool
}

// fetch describes how one forward runs. ctx is the upstream request context: the request context unless the forward
// is detached. lead ignores client write errors and keeps buffering after the client left. call is completed early
// with errNotShared when the response cannot be shared. sent records that a final header reached the client.
type fetch struct {
	ctx  context.Context
	lead bool
	call *coalesce.Call
	sent bool
}

// outcome reports one forward. result is the request result to count, shared the response to share with waiters
// (nil when it is not shareable or not complete), and stored the store outcome, already counted. responded reports
// whether an upstream response was received, and failure is the final status when it is 500, 502, 503 or 504, else 0.
type outcome struct {
	result    string
	shared    *sharedResponse
	stored    string
	responded bool
	failure   int
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
	f           *fetch
	outer       http.Header
	clientGone  bool

	responseTime time.Time
	final        bool
	swallowed    bool
	held         bool
	failed       bool
	writeErr     error
	code         int
	h304         http.Header

	notStorable bool
	tooLarge    bool
	buffering   bool
	shareable   bool
	shareHdr    http.Header
	member      httpcache.Status
	ttl         time.Duration
	tags        []string
	stored      http.Header
	initialAge  time.Duration
	declared    int64
	written     int64
	maxBody     int
	buf         []byte
}

func (s *stage) newFill(w http.ResponseWriter, r *http.Request, in fillInput, conditional bool, f *fetch) *fillWriter {
	return &fillWriter{
		s:           s,
		w:           w,
		r:           r,
		hdr:         w.Header().Clone(),
		in:          in,
		conditional: conditional,
		requestTime: s.clk.Now(),
		f:           f,
		outer:       w.Header().Clone(),
		maxBody:     s.maxBody,
	}
}

// forward sends the request to the origin through next and settles the response. A revalidate forward is
// conditional when the stored entry carries a validator. A conditional forward that receives 304 refreshes the
// stored entry; when that is not possible the request is forwarded again, unconditionally. No request is counted
// here; the caller counts outcome.result.
func (s *stage) forward(next http.Handler, w http.ResponseWriter, r *http.Request, in fillInput, revalidate bool, f *fetch) outcome {
	fr, conditional := r, false
	if revalidate {
		if c := httpcache.Conditional(r, in.e); c != nil {
			fr, conditional = c, true
		}
	}
	ex := &reqctx.Exchange{}
	fr = fr.WithContext(reqctx.WithExchange(f.ctx, ex))
	fw := s.newFill(w, r, in, conditional, f)
	fw.run(next, fr)
	if fw.swallowed {
		if o, ok := s.refresh(w, r, fw); ok {
			return o
		}
		return s.forward(next, w, r, in, false, f)
	}
	if fw.held {
		fw.buf = nil
		s.countStore(storeNotStorable)
		f.sent = true
		s.serve(w, r, in.e, in.age, staleIfError(in, fw.code))
		return outcome{result: resultStale, failure: fw.code, responded: ex.Responded()}
	}
	o := outcome{result: resultMiss, responded: ex.Responded(), failure: failureStatus(fw.code)}
	if fw.shareable && !fw.tooLarge && fw.complete(ex) {
		o.shared = s.share(r, &sharedResponse{status: fw.code, header: fw.shareHdr, body: fw.buf, result: resultMiss, st: fw.member})
	}
	o.stored = s.settle(fw, ex)
	return o
}

// run calls the origin. A panic marks the fill aborted and is propagated.
func (fw *fillWriter) run(next http.Handler, fr *http.Request) {
	done := false
	defer func() {
		if !done {
			fw.s.countStore(storeAborted)
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
		dropOuter(fw.h304, fw.outer)
		return
	}
	if failureStatus(code) != 0 {
		if fw.f.call != nil {
			fw.f.call.Complete(nil, &fetchFailure{status: code})
		}
		if fw.in.sie {
			fw.held = true
			return
		}
	}
	fw.commit()
	if fw.f.call != nil && !fw.shareable {
		fw.f.call.Complete(nil, errNotShared)
	}
	replaceHeader(fw.w.Header(), fw.hdr)
	fw.f.sent = true
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
	dropOuter(st, fw.outer)
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
	fw.shareable = storable && fw.declared <= int64(fw.maxBody)
	fw.tooLarge = fw.declared > int64(fw.maxBody)
	fw.buffering = storable && fw.r.Method == http.MethodGet && !fw.tooLarge
	if fw.buffering && fw.declared >= 0 {
		fw.buf = make([]byte, 0, fw.declared)
	}
	member := httpcache.Status{Fwd: fw.in.fwd, Key: fw.in.key, Detail: fw.in.detail}
	if storable && fw.declared >= 0 && fw.declared <= int64(fw.maxBody) {
		member.Stored = true
		member.TTL = httpcache.TTLSeconds(httpcache.Remaining(lifetime, fw.initialAge))
		member.HasTTL = true
	}
	fw.member = member
	fw.shareHdr = sharedHeader(fw.hdr, fw.outer)
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
	if fw.swallowed || fw.held {
		return len(p), nil
	}
	if fw.failed {
		return 0, fw.writeErr
	}
	if !fw.clientGone {
		n, err := fw.w.Write(p)
		if err != nil && !fw.f.lead {
			fw.written += int64(n)
			fw.failed = true
			fw.writeErr = err
			fw.buf = nil
			fw.buffering = false
			return n, err
		}
		if err != nil {
			fw.clientGone = true
		}
	}
	fw.written += int64(len(p))
	if fw.buffering {
		fw.appendBody(p)
	}
	return len(p), nil
}

// appendBody copies p into the buffer. The buffer never grows beyond max_body_bytes; exceeding it abandons the copy.
func (fw *fillWriter) appendBody(p []byte) {
	need := len(fw.buf) + len(p)
	if need > fw.maxBody {
		fw.buf = nil
		fw.buffering = false
		fw.tooLarge = true
		if fw.f.call != nil {
			fw.f.call.Complete(nil, errNotShared)
		}
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
	if fw.swallowed || fw.held || fw.clientGone {
		return
	}
	_ = http.NewResponseController(fw.w).Flush()
}

// Unwrap returns the client response writer.
func (fw *fillWriter) Unwrap() http.ResponseWriter {
	return fw.w
}

// complete reports whether the origin body was read to its end while the upstream context stayed alive, and whether
// the declared length, when there is one, was written in full.
func (fw *fillWriter) complete(ex *reqctx.Exchange) bool {
	return fw.final && !fw.failed && fw.f.ctx.Err() == nil && ex.Complete() &&
		(fw.declared < 0 || fw.written == fw.declared)
}

// settle stores the response of a forward when it qualifies, counts the store outcome and returns it.
func (s *stage) settle(fw *fillWriter, ex *reqctx.Exchange) string {
	var storage string
	switch {
	case fw.notStorable:
		storage = storeNotStorable
	case fw.tooLarge:
		storage = storeTooLarge
	case !fw.complete(ex):
		storage = storeAborted
	default:
		storage = s.storeEntry(fw.r, fw.in.lk.Token, fw.in.primary, fw.stored, fw.buf, fw.code, fw.tags, fw.initialAge, fw.ttl)
	}
	fw.buf = nil
	s.countStore(storage)
	return storage
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
func (s *stage) refresh(w http.ResponseWriter, r *http.Request, fw *fillWriter) (outcome, bool) {
	in := fw.in
	merged, ok := httpcache.Merge304(in.e, fw.h304)
	if !ok {
		return outcome{}, false
	}
	tags := in.lk.Entry.Tags
	if len(fw.h304.Values(s.tagHeader)) > 0 {
		t, ok := httpcache.ParseTags(fw.h304, s.tagHeader, s.opts.TagDelimiter)
		if !ok {
			return outcome{}, false
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
	storage := storeNotStorable
	if !in.forced {
		storage = s.storeEntry(r, in.lk.Token, in.primary, stored, merged.Body, merged.Status, tags, initialAge, ttl)
	}
	s.countStore(storage)
	st := httpcache.Status{
		Hit:          true,
		Fwd:          httpcache.FwdStale,
		FwdStatus:    http.StatusNotModified,
		HasFwdStatus: true,
		Detail:       in.detail,
		Key:          in.key,
	}
	fw.f.sent = true
	s.serve(w, r, merged, initialAge, st)
	return outcome{
		result:    resultRevalidated,
		stored:    storage,
		shared:    s.share(r, &sharedResponse{merged: merged, age: initialAge, result: resultRevalidated, st: st}),
		responded: true,
	}, true
}

// failureStatus returns code when it is 500, 502, 503 or 504, and 0 otherwise.
func failureStatus(code int) int {
	switch code {
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return code
	}
	return 0
}

// staleIfError returns the status of a stored entry served in place of a failed forward with status code.
func staleIfError(in fillInput, code int) httpcache.Status {
	return httpcache.Status{Hit: true, Fwd: httpcache.FwdStale, FwdStatus: code, HasFwdStatus: true, Detail: detailSIE, Key: in.key}
}

func replaceHeader(dst, src http.Header) {
	clear(dst)
	for k, v := range src {
		dst[k] = v
	}
}
