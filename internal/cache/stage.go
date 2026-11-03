package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/middleware"
	"github.com/ok0/go-microgateway/internal/storeguard"
	"github.com/ok0/go-microgateway/store"
)

const (
	resultHit         = "hit"
	resultStale       = "stale"
	resultRevalidated = "revalidated"
	resultMiss        = "miss"
	resultBypass      = "bypass"

	storeStored        = "stored"
	storeNotStorable   = "not_storable"
	storeTooLarge      = "too_large"
	storeAborted       = "aborted"
	storeRefused       = "refused"
	storeError         = "error"
	opGet              = "get"
	opPut              = "put"
	opPurge            = "purge"
	memoryStoreLabel   = "memory"
	bypassStoreError   = "store-error"
	bypassUpgrade      = "upgrade"
	bypassRange        = "range"
	bypassPrecondition = "precondition"
	detailOnlyIfCache  = "only-if-cached"
)

// Metrics holds the families the stage reports to. Each vector is required.
type Metrics struct {
	// Requests counts requests by route and result: hit, stale, revalidated, miss or bypass.
	Requests *metrics.CounterVec
	// Stores counts storage outcomes by route and result: stored, not_storable, too_large, aborted, refused or error.
	Stores *metrics.CounterVec
	// StoreErrors counts failed store calls by operation: get, put or purge.
	StoreErrors *metrics.CounterVec
}

// Deps holds the collaborators of one cache stage.
type Deps struct {
	// Route is the name of the route the stage serves, used as the route label.
	Route string
	// Store is the store the stage reads and writes.
	Store store.Cache
	// Guard bounds the calls to Store. Nil calls Store directly.
	Guard *storeguard.Guard
	// StoreName names a configured store in logs. It is empty for the built-in memory cache.
	StoreName string
	// Credential reports whether a request carries credentials. It may be nil.
	Credential func(*http.Request) bool
	// Retain is the Purge.Retain used for unsafe-method invalidation.
	Retain time.Duration
	// OnStoreError, if set, is called once for every counted store failure with the operation name.
	OnStoreError func(op string)
	// Errors rate-limits the store failure log. Nil creates one from Logger and Clock.
	Errors *ErrorLog
	// Clock is the gateway clock. It times the requests, the store timeouts and the error log.
	Clock clock.Clock
	// Metrics receives the stage counters.
	Metrics *Metrics
	// Logger receives store failure logs. Nil discards them.
	Logger *slog.Logger
}

type stage struct {
	name         string
	opts         httpcache.Options
	tagHeader    string
	stripTag     bool
	statusKey    bool
	maxBody      int
	timeout      time.Duration
	retain       time.Duration
	store        store.Cache
	guard        *storeguard.Guard
	storeName    string
	onStoreError func(op string)
	errs         *ErrorLog
	clk          clock.Clock
	requests     map[string]*metrics.Counter
	stores       map[string]*metrics.Counter
	storeErrs    map[string]*metrics.Counter
}

// NewStage returns the cache middleware for a route whose cache settings are rc, with the store-wide settings gc
// and the collaborators d. The settings must have their defaults applied. It returns an error when a required
// collaborator or setting is missing.
func NewStage(rc config.RouteCache, gc config.Cache, d Deps) (middleware.Middleware, error) {
	switch {
	case d.Store == nil:
		return nil, errors.New("cache: store is nil")
	case d.Clock == nil:
		return nil, errors.New("cache: clock is nil")
	case d.Metrics == nil || d.Metrics.Requests == nil || d.Metrics.Stores == nil || d.Metrics.StoreErrors == nil:
		return nil, errors.New("cache: metrics are incomplete")
	case d.Retain <= 0:
		return nil, errors.New("cache: retain must be positive")
	case gc.Name == nil:
		return nil, errors.New("cache: name is not set")
	case gc.MaxBodyBytes == nil:
		return nil, errors.New("cache: max_body_bytes is not set")
	case gc.StoreTimeout == nil:
		return nil, errors.New("cache: store_timeout is not set")
	case gc.TagHeader == nil:
		return nil, errors.New("cache: tag_header is not set")
	case gc.StripTagHeader == nil:
		return nil, errors.New("cache: strip_tag_header is not set")
	case rc.MaxTTL == nil:
		return nil, errors.New("cache: max_ttl is not set")
	}
	logger := d.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	errs := d.Errors
	if errs == nil {
		errs = NewErrorLog(logger, d.Clock)
	}
	delim := httpcache.DelimSpace
	if gc.TagDelimiter == "comma" {
		delim = httpcache.DelimComma
	}
	s := &stage{
		name: *gc.Name,
		opts: httpcache.Options{
			Statuses:             rc.Statuses,
			DefaultTTL:           rc.DefaultTTL.Std(),
			MaxTTL:               rc.MaxTTL.Std(),
			Keep:                 rc.Keep.Std(),
			StaleWhileRevalidate: rc.StaleWhileRevalidate.Std(),
			StaleIfError:         rc.StaleIfError.Std(),
			TagHeader:            *gc.TagHeader,
			TagDelimiter:         delim,
			Credential:           d.Credential,
		},
		tagHeader:    *gc.TagHeader,
		stripTag:     *gc.StripTagHeader,
		statusKey:    gc.StatusKey,
		maxBody:      int(*gc.MaxBodyBytes),
		timeout:      gc.StoreTimeout.Std(),
		retain:       d.Retain,
		store:        d.Store,
		guard:        d.Guard,
		storeName:    d.StoreName,
		onStoreError: d.OnStoreError,
		errs:         errs,
		clk:          d.Clock,
		requests:     make(map[string]*metrics.Counter, 5),
		stores:       make(map[string]*metrics.Counter, 6),
		storeErrs:    make(map[string]*metrics.Counter, 3),
	}
	for _, res := range []string{resultHit, resultStale, resultRevalidated, resultMiss, resultBypass} {
		s.requests[res] = d.Metrics.Requests.With(d.Route, res)
	}
	for _, res := range []string{storeStored, storeNotStorable, storeTooLarge, storeAborted, storeRefused, storeError} {
		s.stores[res] = d.Metrics.Stores.With(d.Route, res)
	}
	for _, op := range []string{opGet, opPut, opPurge} {
		s.storeErrs[op] = d.Metrics.StoreErrors.With(op)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.handle(next, w, r)
		})
	}, nil
}

func (s *stage) handle(next http.Handler, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		s.bypassMethod(next, w, r)
		return
	}
	if detail := bypassDetail(r); detail != "" {
		sw := &statusWriter{ResponseWriter: w, member: s.member(httpcache.Status{Fwd: httpcache.FwdBypass, Detail: detail})}
		next.ServeHTTP(sw, r)
		s.count(resultBypass)
		return
	}
	primary := httpcache.PrimaryKey(r)
	if len(primary) > store.MaxKeyBytes {
		s.forward(next, w, r, fillInput{primary: primary, fwd: httpcache.FwdURIMiss, key: s.keyOf(primary, ""), forced: true}, true)
		return
	}
	ctx := r.Context()
	var selected string
	sel := func(names []string) (string, bool) {
		sec, ok := httpcache.SecondaryKey(names, r)
		selected = sec
		return sec, ok
	}
	lk, err := s.get(ctx, primary, sel)
	if err != nil {
		s.fail(ctx, opGet, err)
		sw := &statusWriter{ResponseWriter: w, member: s.member(httpcache.Status{Fwd: httpcache.FwdBypass, Detail: bypassStoreError, Key: s.keyOf(primary, "")})}
		next.ServeHTTP(sw, r)
		s.count(resultBypass)
		return
	}
	key := s.keyOf(primary, selected)
	var e *httpcache.Entry
	var age time.Duration
	if lk.Entry != nil {
		e = &httpcache.Entry{
			Status:     lk.Entry.Status,
			Header:     lk.Entry.Header,
			Body:       lk.Entry.Body,
			InitialAge: lk.Entry.InitialAge,
			Stale:      lk.Entry.Stale,
		}
		age = httpcache.CurrentAge(lk.Entry.InitialAge, lk.Entry.StoredAt, lk.Now)
	}
	switch httpcache.Decide(r, e, age, s.opts) {
	case httpcache.Fresh:
		s.serve(w, r, e, age, httpcache.Status{Hit: true, Key: key})
		s.count(resultHit)
	case httpcache.ServeStale:
		s.serve(w, r, e, age, httpcache.Status{Hit: true, Key: key})
		s.count(resultStale)
	case httpcache.Unsatisfiable:
		appendCacheStatus(w.Header(), s.member(httpcache.Status{Detail: detailOnlyIfCache, Key: key}))
		httperr.Write(w, r, http.StatusGatewayTimeout, httperr.CodeNotCached, httperr.Message(httperr.CodeNotCached))
		s.count(resultMiss)
	default:
		fwd := httpcache.FwdURIMiss
		switch {
		case e != nil:
			fwd = httpcache.FwdStale
		case len(lk.VaryNames) > 0:
			fwd = httpcache.FwdVaryMiss
		}
		s.forward(next, w, r, fillInput{primary: primary, lk: lk, e: e, fwd: fwd, key: key}, true)
	}
}

func (s *stage) bypassMethod(next http.Handler, w http.ResponseWriter, r *http.Request) {
	sw := &statusWriter{ResponseWriter: w, member: s.member(httpcache.Status{Fwd: httpcache.FwdMethod})}
	next.ServeHTTP(sw, r)
	if unsafeMethod(r.Method) && sw.wrote && sw.code >= 200 && sw.code < 400 {
		s.purgeUnsafe(r)
	}
	s.count(resultBypass)
}

func (s *stage) purgeUnsafe(r *http.Request) {
	ctx := context.WithoutCancel(r.Context())
	get := *r
	get.Method = http.MethodGet
	head := *r
	head.Method = http.MethodHead
	p := store.Purge{
		Kind:   store.PurgeKeys,
		Keys:   []string{httpcache.PrimaryKey(&get), httpcache.PrimaryKey(&head)},
		Retain: s.retain,
	}
	if _, err := s.purge(ctx, p); err != nil {
		s.fail(ctx, opPurge, err)
	}
}

func unsafeMethod(m string) bool {
	switch m {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}
	return false
}

// bypassDetail returns why a request skips the cache, or "" when it may use it.
func bypassDetail(r *http.Request) string {
	if hasToken(r.Header.Values("Connection"), "upgrade") && r.Header.Get("Upgrade") != "" {
		return bypassUpgrade
	}
	if len(r.Header.Values("Range")) > 0 || len(r.Header.Values("If-Range")) > 0 {
		return bypassRange
	}
	if len(r.Header.Values("If-Match")) > 0 || len(r.Header.Values("If-Unmodified-Since")) > 0 {
		return bypassPrecondition
	}
	return ""
}

func hasToken(lines []string, token string) bool {
	for _, line := range lines {
		for _, part := range strings.Split(line, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

func (s *stage) member(st httpcache.Status) string {
	st.Name = s.name
	return st.String()
}

func (s *stage) keyOf(primary, secondary string) string {
	if !s.statusKey {
		return ""
	}
	sum := sha256.Sum256([]byte(strconv.Itoa(len(primary)) + ":" + primary + secondary))
	return hex.EncodeToString(sum[:])
}

func (s *stage) count(result string) {
	s.requests[result].Inc()
}

func (s *stage) countStore(result string) {
	s.stores[result].Inc()
}

func appendCacheStatus(h http.Header, member string) {
	vals := h.Values("Cache-Status")
	h.Set("Cache-Status", strings.Join(append(vals, member), ", "))
}
