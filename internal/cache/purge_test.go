package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/memstore"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/storeguard"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

// syncBuffer is a bytes.Buffer that tolerates concurrent writers.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// nthStore fails the failAt-th purge call with err. The count is 1-based, so a failAt of zero never fails.
type nthStore struct {
	store.Cache
	n, failAt int
	err       error
}

func (s *nthStore) Purge(ctx context.Context, p store.Purge) (store.PurgeResult, error) {
	s.n++
	if s.n == s.failAt {
		return store.PurgeResult{}, s.err
	}
	return s.Cache.Purge(ctx, p)
}

// unknownStore reports every purge with AffectedKnown false and Affected 0, as a store may for PurgeAll.
type unknownStore struct{ store.Cache }

func (s unknownStore) Purge(ctx context.Context, p store.Purge) (store.PurgeResult, error) {
	r, err := s.Cache.Purge(ctx, p)
	return store.PurgeResult{Accepted: r.Accepted}, err
}

func noVary([]string) (string, bool) { return "", true }

// putEntry stores a GET entry for url with tags through the memory cache of sp, with a one-hour storage TTL.
func putEntry(tb testing.TB, sp *spy, url string, tags ...string) {
	tb.Helper()
	keys, err := httpcache.URLKeys(url)
	if err != nil {
		tb.Fatal(err)
	}
	ctx := context.Background()
	lk, err := sp.inner.Get(ctx, keys[0], noVary)
	if err != nil {
		tb.Fatal(err)
	}
	e := &store.Entry{Status: http.StatusOK, Header: http.Header{}, Body: []byte("body"), Tags: tags}
	stored, err := sp.inner.Put(ctx, lk.Token, keys[0], "", nil, e, time.Hour)
	if err != nil || !stored {
		tb.Fatalf("put %s: stored %v, err %v", url, stored, err)
	}
}

// entryOf returns the entry that the GET key of url holds in the memory cache of sp, or nil on a miss.
func entryOf(tb testing.TB, sp *spy, url string) *store.Entry {
	tb.Helper()
	keys, err := httpcache.URLKeys(url)
	if err != nil {
		tb.Fatal(err)
	}
	lk, err := sp.inner.Get(context.Background(), keys[0], noVary)
	if err != nil {
		tb.Fatal(err)
	}
	return lk.Entry
}

// live reports whether url has an entry in the memory cache of sp that is not marked stale.
func live(tb testing.TB, sp *spy, url string) bool {
	e := entryOf(tb, sp, url)
	return e != nil && !e.Stale
}

// numberedURLs returns n distinct absolute URLs on host h.
func numberedURLs(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("http://h/%d", i)
	}
	return out
}

// numberedTags returns n distinct tags.
func numberedTags(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("tg%d", i)
	}
	return out
}

// purgeOpts selects the collaborators of a purge rig. st, when set, replaces sp as the store of the service.
type purgeOpts struct {
	clk          *clock.Fake
	sp           *spy
	st           store.Cache
	guard        bool
	retain       time.Duration
	retention    *Retention
	onStoreError func(op string)
}

// purgeRig is a purge service over a spy store, with its metrics and its log.
type purgeRig struct {
	p      *Purger
	sp     *spy
	clk    *clock.Fake
	purges *metrics.CounterVec
	errs   *metrics.CounterVec
	logs   *syncBuffer
}

// newPurgeRig returns a purge service over the spy of o. The service retains markers for 30 days unless o.retain is
// set, and writes JSON records to the log of the rig. With o.guard the store is guarded and named "shared".
func newPurgeRig(tb testing.TB, o purgeOpts) *purgeRig {
	tb.Helper()
	clk := o.clk
	if clk == nil {
		clk = clock.NewFake(start)
	}
	sp := o.sp
	if sp == nil {
		sp = newSpy(tb, clk)
	}
	st := o.st
	if st == nil {
		st = sp
	}
	reg := metrics.NewRegistry()
	purges, err := reg.Counter("microgateway_cache_purges_total", "Successful cache purge calls.", "type", "mode")
	if err != nil {
		tb.Fatal(err)
	}
	errs, err := reg.Counter("microgateway_cache_store_errors_total", "Cache store call failures.", "op")
	if err != nil {
		tb.Fatal(err)
	}
	retain := o.retain
	if retain == 0 {
		retain = 30 * 24 * time.Hour
	}
	logs := &syncBuffer{}
	d := PurgerDeps{
		Store:        st,
		Retain:       retain,
		Retention:    o.retention,
		OnStoreError: o.onStoreError,
		Metrics:      &PurgeMetrics{Purges: purges, StoreErrors: errs},
		Logger:       slog.New(slog.NewJSONHandler(logs, nil)),
	}
	if o.guard {
		g, err := storeguard.New("shared", storeguard.Settings{ConsecutiveFailures: 2, OpenTimeout: time.Second, MaxInflight: 16}, clk, nil)
		if err != nil {
			tb.Fatal(err)
		}
		d.Guard = g
		d.StoreName = "shared"
		d.Timeout = 100 * time.Millisecond
	}
	p, err := NewPurger(d)
	if err != nil {
		tb.Fatal(err)
	}
	return &purgeRig{p: p, sp: sp, clk: clk, purges: purges, errs: errs, logs: logs}
}

// purged returns the number of successful purge calls of typ and mode.
func (r *purgeRig) purged(typ, mode string) float64 {
	return r.purges.With(typ, mode).Value()
}

// purgedTotal returns the number of successful purge calls of every type and mode.
func (r *purgeRig) purgedTotal() float64 {
	var n float64
	for _, typ := range []string{PurgeTypeURL, PurgeTypeTag, PurgeTypeAll} {
		for _, mode := range []string{modeHard, modeSoft} {
			n += r.purged(typ, mode)
		}
	}
	return n
}

// storeErrs returns the number of counted purge store failures.
func (r *purgeRig) storeErrs() float64 {
	return r.errs.With(opPurge).Value()
}

func TestPurgeRequestInvalid(t *testing.T) {
	long := "http://h/" + strings.Repeat("a", 8<<10)
	cases := []struct {
		name string
		req  PurgeRequest
		vals []string
	}{
		{"no selector", PurgeRequest{}, nil},
		{"url and tag", PurgeRequest{URLs: []string{"http://h/a"}, Tags: []string{"zz-tag"}}, []string{"http://h/a", "zz-tag"}},
		{"url and all", PurgeRequest{URLs: []string{"http://h/a"}, All: true}, []string{"http://h/a"}},
		{"1001 urls", PurgeRequest{URLs: numberedURLs(1001)}, []string{"http://h/0"}},
		{"1001 tags", PurgeRequest{Tags: numberedTags(1001)}, []string{"tg0"}},
		{"empty tag", PurgeRequest{Tags: []string{""}}, nil},
		{"tag of 1025 bytes", PurgeRequest{Tags: []string{strings.Repeat("x", 1025)}}, []string{strings.Repeat("x", 1025)}},
		{"tag with NUL", PurgeRequest{Tags: []string{"a\x00b"}}, []string{"a\x00b"}},
		{"tag with unit separator", PurgeRequest{Tags: []string{"c\x1fd"}}, []string{"c\x1fd"}},
		{"tag with DEL", PurgeRequest{Tags: []string{"e\x7ff"}}, []string{"e\x7ff"}},
		{"relative URL", PurgeRequest{URLs: []string{"/rel"}}, []string{"/rel"}},
		{"ftp scheme", PurgeRequest{URLs: []string{"ftp://h/"}}, []string{"ftp://h/"}},
		{"empty host", PurgeRequest{URLs: []string{"http:///p"}}, []string{"http:///p"}},
		{"userinfo", PurgeRequest{URLs: []string{"http://u:p@h/"}}, []string{"http://u:p@h/"}},
		{"port out of range", PurgeRequest{URLs: []string{"http://h:99999/"}}, []string{"http://h:99999/"}},
		{"key over MaxKeyBytes", PurgeRequest{URLs: []string{long}}, []string{long}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newPurgeRig(t, purgeOpts{})
			_, err := r.p.Purge(context.Background(), tc.req, "")
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("error %v, want ErrInvalidRequest", err)
			}
			var re *RequestError
			if !errors.As(err, &re) {
				t.Fatalf("error %T, want *RequestError", err)
			}
			if n := len(r.sp.callsOf(opPurge)); n != 0 {
				t.Fatalf("%d store purge calls for a rejected request", n)
			}
			if n := r.purgedTotal(); n != 0 {
				t.Fatalf("purges counted %v for a rejected request", n)
			}
			if n := r.storeErrs(); n != 0 {
				t.Fatalf("store errors counted %v for a rejected request", n)
			}
			if out := r.logs.String(); out != "" {
				t.Fatalf("log %q for a rejected request", out)
			}
			for _, v := range tc.vals {
				if strings.Contains(err.Error(), v) {
					t.Fatalf("error %q echoes %q", err.Error(), v)
				}
			}
		})
	}
}

func TestPurgeRequestBoundaries(t *testing.T) {
	cases := []struct {
		name     string
		req      PurgeRequest
		accepted int
	}{
		{"1000 urls", PurgeRequest{URLs: numberedURLs(1000)}, 1000},
		{"tag of 1024 bytes", PurgeRequest{Tags: []string{strings.Repeat("t", 1024)}}, 1},
		{"tags with bytes 0x20 and 0x7e", PurgeRequest{Tags: []string{string([]byte{0x20}), string([]byte{0x7e})}}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newPurgeRig(t, purgeOpts{})
			res, err := r.p.Purge(context.Background(), tc.req, "")
			if err != nil {
				t.Fatalf("error %v, want success", err)
			}
			if res.Accepted != tc.accepted {
				t.Fatalf("accepted %d, want %d", res.Accepted, tc.accepted)
			}
		})
	}
}

func TestPurgeURLCanonical(t *testing.T) {
	cases := []struct {
		name     string
		urls     []string
		accepted int
	}{
		{"case and default port", []string{"HTTP://API.Example.com:80/p?x=1", "http://api.example.com/p?x=1"}, 1},
		{"default https port", []string{"https://h:443/", "https://h/"}, 1},
		{"query order is significant", []string{"http://h/p?a=1&b=2", "http://h/p?b=2&a=1"}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newPurgeRig(t, purgeOpts{})
			res, err := r.p.Purge(context.Background(), PurgeRequest{URLs: tc.urls}, "")
			if err != nil {
				t.Fatal(err)
			}
			if res.Accepted != tc.accepted || res.Type != PurgeTypeURL || res.Scope != ScopeInstance {
				t.Fatalf("result %+v, want %d accepted, type url, scope instance", res, tc.accepted)
			}
			if n := len(r.sp.callsOf(opPurge)); n != 1 {
				t.Fatalf("%d store calls, want 1", n)
			}
		})
	}
	t.Run("keys of the canonical URL", func(t *testing.T) {
		r := newPurgeRig(t, purgeOpts{})
		if _, err := r.p.Purge(context.Background(), PurgeRequest{URLs: []string{"HTTP://API.Example.com:80/p?x=1"}}, ""); err != nil {
			t.Fatal(err)
		}
		want, err := httpcache.URLKeys("http://api.example.com/p?x=1")
		if err != nil {
			t.Fatal(err)
		}
		if got := r.sp.callsOf(opPurge)[0].Purge.Keys; !slices.Equal(got, want) {
			t.Fatalf("keys %q, want %q", got, want)
		}
	})
}

func TestPurgeBatches(t *testing.T) {
	r := newPurgeRig(t, purgeOpts{})
	urls := numberedURLs(1000)
	for _, u := range urls {
		putEntry(t, r.sp, u)
	}
	res, err := r.p.Purge(context.Background(), PurgeRequest{URLs: urls}, "")
	if err != nil {
		t.Fatal(err)
	}
	calls := r.sp.callsOf(opPurge)
	if len(calls) != 2 {
		t.Fatalf("%d store calls, want 2", len(calls))
	}
	head := make(map[string]string, len(urls))
	for _, u := range urls {
		ks, err := httpcache.URLKeys(u)
		if err != nil {
			t.Fatal(err)
		}
		head[ks[0]] = ks[1]
	}
	for i, c := range calls {
		if len(c.Purge.Keys) != store.MaxPurgeSelectors {
			t.Fatalf("call %d has %d keys, want %d", i, len(c.Purge.Keys), store.MaxPurgeSelectors)
		}
		for j := 0; j < len(c.Purge.Keys); j += 2 {
			if head[c.Purge.Keys[j]] != c.Purge.Keys[j+1] {
				t.Fatalf("call %d splits the GET and HEAD keys of one URL at key %d", i, j)
			}
		}
	}
	if res.Accepted != 1000 || res.Affected != 1000 || !res.AffectedKnown {
		t.Fatalf("result %+v, want 1000 accepted and 1000 affected", res)
	}
	if n := r.purged(PurgeTypeURL, modeHard); n != 1 {
		t.Fatalf("url hard purges %v, want 1", n)
	}
}

func TestPurgeBatchFailure(t *testing.T) {
	clk := clock.NewFake(start)
	sp := newSpy(t, clk)
	st := &nthStore{Cache: sp, failAt: 2, err: errStoreDown}
	r := newPurgeRig(t, purgeOpts{clk: clk, sp: sp, st: st})
	urls := numberedURLs(1000)
	for _, u := range urls {
		putEntry(t, sp, u)
	}
	_, err := r.p.Purge(context.Background(), PurgeRequest{URLs: urls}, "")
	if !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("error %v, want ErrUnavailable", err)
	}
	if live(t, sp, urls[0]) {
		t.Fatal("entry of the applied batch is still live")
	}
	if !live(t, sp, urls[999]) {
		t.Fatal("entry of the batch after the failure was purged")
	}
	if n := r.storeErrs(); n != 1 {
		t.Fatalf("store errors %v, want 1", n)
	}
	if n := r.purgedTotal(); n != 0 {
		t.Fatalf("purges counted %v after a failure", n)
	}
	st.failAt = 0
	res, err := r.p.Purge(context.Background(), PurgeRequest{URLs: urls}, "")
	if err != nil || res.Accepted != 1000 {
		t.Fatalf("repeated purge: result %+v, error %v", res, err)
	}
	if live(t, sp, urls[999]) {
		t.Fatal("repeated purge left an entry live")
	}
}

func TestPurgeTags(t *testing.T) {
	fixture := func(t *testing.T) *purgeRig {
		r := newPurgeRig(t, purgeOpts{})
		putEntry(t, r.sp, "http://h/1", "a", "b", "c")
		putEntry(t, r.sp, "http://h/2", "c", "d")
		return r
	}
	cases := []struct {
		name     string
		tags     []string
		accepted int
		affected int
	}{
		{"a", []string{"a"}, 1, 1},
		{"c", []string{"c"}, 1, 2},
		{"d", []string{"d"}, 1, 1},
		{"duplicates", []string{"a", "a"}, 1, 1},
		{"absent", []string{"zz"}, 1, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := fixture(t)
			res, err := r.p.Purge(context.Background(), PurgeRequest{Tags: tc.tags}, "")
			if err != nil {
				t.Fatal(err)
			}
			if res.Type != PurgeTypeTag || res.Accepted != tc.accepted || res.Affected != tc.affected || !res.AffectedKnown {
				t.Fatalf("result %+v, want type tag, %d accepted, %d affected", res, tc.accepted, tc.affected)
			}
		})
	}
}

func TestPurgeAll(t *testing.T) {
	for _, soft := range []bool{false, true} {
		mode := modeHard
		if soft {
			mode = modeSoft
		}
		t.Run(mode, func(t *testing.T) {
			r := newPurgeRig(t, purgeOpts{})
			putEntry(t, r.sp, "http://h/1")
			putEntry(t, r.sp, "http://h/2")
			res, err := r.p.Purge(context.Background(), PurgeRequest{All: true, Soft: soft}, "")
			if err != nil {
				t.Fatal(err)
			}
			if res.Type != PurgeTypeAll || res.Accepted != 1 || res.Affected != 2 || !res.AffectedKnown || res.Soft != soft {
				t.Fatalf("result %+v, want type all, 1 accepted, 2 affected", res)
			}
			if n := r.purged(PurgeTypeAll, mode); n != 1 {
				t.Fatalf("all %s purges %v, want 1", mode, n)
			}
		})
	}
}

func TestPurgeSoftCounts(t *testing.T) {
	r := newPurgeRig(t, purgeOpts{})
	putEntry(t, r.sp, "http://h/s")
	res, err := r.p.Purge(context.Background(), PurgeRequest{URLs: []string{"http://h/s"}, Soft: true}, "")
	if err != nil {
		t.Fatal(err)
	}
	if res.Affected != 1 || !res.Soft {
		t.Fatalf("result %+v, want 1 affected and soft", res)
	}
	if e := entryOf(t, r.sp, "http://h/s"); e == nil || !e.Stale {
		t.Fatalf("entry after a soft purge %+v, want a stale entry", e)
	}
}

func TestPurgeAffectedUnknown(t *testing.T) {
	clk := clock.NewFake(start)
	sp := newSpy(t, clk)
	r := newPurgeRig(t, purgeOpts{clk: clk, sp: sp, st: unknownStore{Cache: sp}})
	putEntry(t, sp, "http://h/u")
	res, err := r.p.Purge(context.Background(), PurgeRequest{URLs: []string{"http://h/u"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	if res.AffectedKnown || res.Affected != 0 || res.Accepted != 1 {
		t.Fatalf("result %+v, want unknown affected and 1 accepted", res)
	}
	if !strings.Contains(r.logs.String(), `"affected":"unknown"`) {
		t.Fatalf("log %q lacks the unknown affected count", r.logs.String())
	}
}

func TestPurgeStoreFailure(t *testing.T) {
	var hooked []string
	r := newPurgeRig(t, purgeOpts{onStoreError: func(op string) { hooked = append(hooked, op) }})
	putEntry(t, r.sp, "http://h/f")
	r.sp.setErr(opPurge, errStoreDown)
	_, err := r.p.Purge(context.Background(), PurgeRequest{URLs: []string{"http://h/f"}}, "")
	if !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("error %v, want ErrUnavailable", err)
	}
	if errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("store failure reported as an invalid request: %v", err)
	}
	if n := r.storeErrs(); n != 1 {
		t.Fatalf("store errors %v, want 1", n)
	}
	if strings.Join(hooked, ",") != opPurge {
		t.Fatalf("OnStoreError calls %v, want [purge]", hooked)
	}
	if n := r.purgedTotal(); n != 0 {
		t.Fatalf("purges counted %v after a failure", n)
	}
	out := r.logs.String()
	if n := strings.Count(out, "\n"); n != 1 || !strings.Contains(out, `"store":"memory"`) {
		t.Fatalf("log %q, want one record naming the store", out)
	}
	if strings.Contains(out, "connection refused") {
		t.Fatalf("log %q carries the backend error text", out)
	}
}

func TestPurgeGuardOpen(t *testing.T) {
	r := newPurgeRig(t, purgeOpts{guard: true})
	putEntry(t, r.sp, "http://h/g")
	r.sp.setErr(opPurge, errStoreDown)
	req := PurgeRequest{URLs: []string{"http://h/g"}}
	for i := range 2 {
		if _, err := r.p.Purge(context.Background(), req, ""); !errors.Is(err, store.ErrUnavailable) {
			t.Fatalf("call %d: error %v, want ErrUnavailable", i+1, err)
		}
	}
	if n := len(r.sp.callsOf(opPurge)); n != 2 {
		t.Fatalf("%d store calls before the guard opened, want 2", n)
	}
	if _, err := r.p.Purge(context.Background(), req, ""); !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("open guard: error %v, want ErrUnavailable", err)
	}
	if n := len(r.sp.callsOf(opPurge)); n != 2 {
		t.Fatalf("open guard reached the store: %d calls, want 2", n)
	}
	if n := r.storeErrs(); n != 2 {
		t.Fatalf("store errors %v, want 2", n)
	}
	if !strings.Contains(r.logs.String(), `"reason":"open"`) {
		t.Fatalf("log %q lacks the open reason", r.logs.String())
	}
}

func TestPurgeTimeout(t *testing.T) {
	r := newPurgeRig(t, purgeOpts{guard: true})
	putEntry(t, r.sp, "http://h/t")
	release := r.sp.hang(opPurge)
	t.Cleanup(release)
	done := make(chan error, 1)
	go func() {
		_, err := r.p.Purge(context.Background(), PurgeRequest{URLs: []string{"http://h/t"}}, "")
		done <- err
	}()
	testwait.Until(t, "guard timer armed", func() bool { return r.clk.Waiters() >= 1 }, nil)
	r.clk.Advance(100*time.Millisecond - time.Nanosecond)
	select {
	case err := <-done:
		t.Fatalf("purge returned before the store timeout: %v", err)
	default:
	}
	r.clk.Advance(time.Nanosecond)
	err, ok := testwait.Recv(t, "purge returned", done, nil)
	if !ok || !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("error %v, want ErrUnavailable", err)
	}
	if !strings.Contains(r.logs.String(), `"reason":"timeout"`) {
		t.Fatalf("log %q lacks the timeout reason", r.logs.String())
	}
	if n := r.storeErrs(); n != 1 {
		t.Fatalf("store errors %v, want 1", n)
	}
}

func TestPurgeCanceled(t *testing.T) {
	r := newPurgeRig(t, purgeOpts{})
	putEntry(t, r.sp, "http://h/c")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := r.p.Purge(ctx, PurgeRequest{URLs: []string{"http://h/c"}}, "")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v, want context.Canceled", err)
	}
	if !live(t, r.sp, "http://h/c") {
		t.Fatal("entry purged by a canceled request")
	}
	if n := r.storeErrs(); n != 0 {
		t.Fatalf("store errors %v for a canceled request, want 0", n)
	}
	if n := r.purgedTotal(); n != 0 {
		t.Fatalf("purges counted %v for a canceled request, want 0", n)
	}
	if out := r.logs.String(); out != "" {
		t.Fatalf("log %q for a canceled request", out)
	}
}

func TestPurgeSuccessLog(t *testing.T) {
	r := newPurgeRig(t, purgeOpts{})
	putEntry(t, r.sp, "http://h/p?secret=Q7z")
	if _, err := r.p.Purge(context.Background(), PurgeRequest{URLs: []string{"http://h/p?secret=Q7z"}}, "ops"); err != nil {
		t.Fatal(err)
	}
	line := r.logs.String()
	for _, want := range []string{`"msg":"cache purged"`, `"key_id":"ops"`, `"type":"url"`, `"mode":"hard"`,
		`"accepted":1`, `"affected":1`, `"scope":"instance"`, `"store":"memory"`} {
		if !strings.Contains(line, want) {
			t.Fatalf("log %q lacks %s", line, want)
		}
	}
	for _, leak := range []string{"Q7z", "secret", "http://"} {
		if strings.Contains(line, leak) {
			t.Fatalf("log %q carries %q from the request", line, leak)
		}
	}
	tagged := newPurgeRig(t, purgeOpts{})
	putEntry(t, tagged.sp, "http://h/q", "tag-T9")
	if _, err := tagged.p.Purge(context.Background(), PurgeRequest{Tags: []string{"tag-T9"}}, "ops"); err != nil {
		t.Fatal(err)
	}
	if out := tagged.logs.String(); !strings.Contains(out, `"type":"tag"`) || strings.Contains(out, "tag-T9") {
		t.Fatalf("tag log %q", out)
	}
}

func TestPurgeRetention(t *testing.T) {
	var r Retention
	if got := r.At(2 * time.Hour); got != 2*time.Hour {
		t.Fatalf("zero value At(2h) = %v, want 2h", got)
	}
	r.Raise(5 * time.Hour)
	if got := r.At(2 * time.Hour); got != 5*time.Hour {
		t.Fatalf("At(2h) after Raise(5h) = %v, want 5h", got)
	}
	if got := r.At(6 * time.Hour); got != 6*time.Hour {
		t.Fatalf("At(6h) after Raise(5h) = %v, want 6h", got)
	}
	r.Raise(time.Hour)
	r.Raise(0)
	r.Raise(-1)
	if got := r.At(2 * time.Hour); got != 5*time.Hour {
		t.Fatalf("At(2h) after lower and non-positive raises = %v, want 5h", got)
	}
	var big Retention
	big.Raise(store.MaxTTL + time.Hour)
	if got := big.At(0); got != store.MaxTTL {
		t.Fatalf("At(0) after an oversized Raise = %v, want MaxTTL", got)
	}
	var nilRetention *Retention
	if got := nilRetention.At(store.MaxTTL * 2); got != store.MaxTTL {
		t.Fatalf("nil At = %v, want MaxTTL", got)
	}
	shared := new(Retention)
	shared.Raise(5 * time.Hour)
	t.Run("url", func(t *testing.T) {
		pr := newPurgeRig(t, purgeOpts{retain: 2 * time.Hour, retention: shared})
		putEntry(t, pr.sp, "http://h/r")
		if _, err := pr.p.Purge(context.Background(), PurgeRequest{URLs: []string{"http://h/r"}}, ""); err != nil {
			t.Fatal(err)
		}
		if got := pr.sp.callsOf(opPurge)[0].Purge.Retain; got != shared.At(2*time.Hour) {
			t.Fatalf("url purge retain %v, want %v", got, shared.At(2*time.Hour))
		}
	})
	t.Run("tag", func(t *testing.T) {
		pr := newPurgeRig(t, purgeOpts{retain: 2 * time.Hour, retention: shared})
		if _, err := pr.p.Purge(context.Background(), PurgeRequest{Tags: []string{"t"}}, ""); err != nil {
			t.Fatal(err)
		}
		if got := pr.sp.callsOf(opPurge)[0].Purge.Retain; got != shared.At(2*time.Hour) {
			t.Fatalf("tag purge retain %v, want %v", got, shared.At(2*time.Hour))
		}
	})
	t.Run("all", func(t *testing.T) {
		pr := newPurgeRig(t, purgeOpts{retain: 2 * time.Hour, retention: shared})
		if _, err := pr.p.Purge(context.Background(), PurgeRequest{All: true}, ""); err != nil {
			t.Fatal(err)
		}
		if got := pr.sp.callsOf(opPurge)[0].Purge.Retain; got != shared.At(2*time.Hour) {
			t.Fatalf("all purge retain %v, want %v", got, shared.At(2*time.Hour))
		}
	})
}

func TestStageUnsafeRetention(t *testing.T) {
	t.Run("raised high-water", func(t *testing.T) {
		var ret Retention
		ret.Raise(400 * 24 * time.Hour)
		rg := newRig(t, rigOpts{retention: &ret})
		rg.orig.script(http.StatusOK, nil, "")
		rg.serve(http.MethodPost, "/x", nil)
		calls := rg.spy.callsOf(opPurge)
		if len(calls) != 1 || calls[0].Purge.Retain != 400*24*time.Hour {
			t.Fatalf("purge calls %+v, want one with retain 400 days", calls)
		}
	})
	t.Run("without retention", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, nil, "")
		rg.serve(http.MethodPost, "/x", nil)
		calls := rg.spy.callsOf(opPurge)
		if len(calls) != 1 || calls[0].Purge.Retain != 30*24*time.Hour {
			t.Fatalf("purge calls %+v, want one with retain 30 days", calls)
		}
	})
}

func TestPurgeMarkerCap(t *testing.T) {
	clk := clock.NewFake(start)
	inner := memstore.NewCache(memstore.CacheOptions{MaxBytes: 1 << 22, Clock: clk, MaxMarkers: 64})
	t.Cleanup(func() { _ = inner.Close() })
	sp := &spy{inner: inner, errs: map[string]error{}, holds: map[string]chan struct{}{}, panics: map[string]any{}}
	r := newPurgeRig(t, purgeOpts{clk: clk, sp: sp})
	putEntry(t, sp, "http://h/keep")
	if _, err := r.p.Purge(context.Background(), PurgeRequest{URLs: numberedURLs(640)}, ""); err != nil {
		t.Fatal(err)
	}
	if n := inner.Stats().Markers; n > 64 {
		t.Fatalf("markers %d, want at most 64", n)
	}
	if !live(t, sp, "http://h/keep") {
		t.Fatal("unrelated entry lost to the marker cap")
	}
}

func TestPurgeScope(t *testing.T) {
	if got := newPurgeRig(t, purgeOpts{}).p.Scope(); got != ScopeInstance {
		t.Fatalf("scope %q without a named store, want %q", got, ScopeInstance)
	}
	if got := newPurgeRig(t, purgeOpts{guard: true}).p.Scope(); got != ScopeStore {
		t.Fatalf("scope %q with a named store, want %q", got, ScopeStore)
	}
}

func TestNewPurgerErrors(t *testing.T) {
	clk := clock.NewFake(start)
	sp := newSpy(t, clk)
	reg := metrics.NewRegistry()
	purges, err := reg.Counter("microgateway_cache_purges_total", "Successful cache purge calls.", "type", "mode")
	if err != nil {
		t.Fatal(err)
	}
	errs, err := reg.Counter("microgateway_cache_store_errors_total", "Cache store call failures.", "op")
	if err != nil {
		t.Fatal(err)
	}
	g, err := storeguard.New("shared", storeguard.Settings{ConsecutiveFailures: 2, OpenTimeout: time.Second, MaxInflight: 16}, clk, nil)
	if err != nil {
		t.Fatal(err)
	}
	m := &PurgeMetrics{Purges: purges, StoreErrors: errs}
	if _, err := NewPurger(PurgerDeps{Store: sp, Retain: time.Hour, Metrics: m}); err != nil {
		t.Fatalf("valid dependencies: %v", err)
	}
	cases := []struct {
		name string
		d    PurgerDeps
	}{
		{"nil store", PurgerDeps{Retain: time.Hour, Metrics: m}},
		{"nil metrics", PurgerDeps{Store: sp, Retain: time.Hour}},
		{"nil purges", PurgerDeps{Store: sp, Retain: time.Hour, Metrics: &PurgeMetrics{StoreErrors: errs}}},
		{"nil store errors", PurgerDeps{Store: sp, Retain: time.Hour, Metrics: &PurgeMetrics{Purges: purges}}},
		{"zero retain", PurgerDeps{Store: sp, Metrics: m}},
		{"guard without timeout", PurgerDeps{Store: sp, Retain: time.Hour, Metrics: m, Guard: g}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewPurger(tc.d); err == nil {
				t.Fatal("NewPurger returned no error")
			}
		})
	}
}

func TestPurgeNoGoroutine(t *testing.T) {
	r := newPurgeRig(t, purgeOpts{})
	putEntry(t, r.sp, "http://h/n1")
	putEntry(t, r.sp, "http://h/n2")
	baseline := runtime.NumGoroutine()
	ctx := context.Background()
	if _, err := r.p.Purge(ctx, PurgeRequest{URLs: []string{"http://h/n1"}}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := r.p.Purge(ctx, PurgeRequest{URLs: []string{"http://h/n2"}, Soft: true}, ""); err != nil {
		t.Fatal(err)
	}
	r.sp.setErr(opPurge, errStoreDown)
	if _, err := r.p.Purge(ctx, PurgeRequest{Tags: []string{"x"}}, ""); !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("error %v, want ErrUnavailable", err)
	}
	r.sp.setErr(opPurge, nil)
	testwait.Until(t, "goroutines back to baseline", func() bool {
		return runtime.NumGoroutine() <= baseline
	}, func() string { return fmt.Sprintf("goroutines %d, baseline %d", runtime.NumGoroutine(), baseline) })
}
