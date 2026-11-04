package redis

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	microgateway "github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/hooks"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/testwait"
)

// redisServer is the Redis server shared by the gateways under test. exact
// reports that the server clock moves only through advance, so decisions are
// fully determined; otherwise advance moves real time. now reads the server clock.
type redisServer struct {
	addr    string
	db      int
	exact   bool
	advance func(time.Duration)
	now     func(t *testing.T) time.Time
}

const (
	multiBurst       = 10
	multiSenders     = 8
	multiClientAddr  = "192.0.2.10:4000"
	multiBucketBody  = "rate: 1\nper: 1h\nburst: 10\n"
	multiWindowBody  = "algorithm: sliding_window\nlimit: 10\nwindow: 1h\n"
	multiElapsedBody = "rate: 10\nper: 1s\nburst: 5\n"
	multiStoreTail   = "store: shared\non_store_error: deny\nstore_timeout: 2s\n"
)

type multiTally struct {
	admitted int
	denied   int
}

// multiGateways builds n gateways with fake clocks hours apart. When useStore is
// set every gateway shares the Redis server under a prefix unique to the caller.
func multiGateways(t *testing.T, srv redisServer, n int, route string, useStore bool) []*microgateway.Gateway {
	t.Helper()
	prefix := uniquePrefix(t)
	c := newTestClient(t, srv.addr, srv.db)
	t.Cleanup(func() { deletePrefix(t, c, prefix) })
	var stores string
	if useStore {
		stores = redisStoreYAML("shared", []string{srv.addr}, fmt.Sprintf("db: %d\nkey_prefix: %s\n", srv.db, prefix), "")
	}
	doc := gwDoc(stores, routeYAML("rl", route), upstream(t))
	gws := make([]*microgateway.Gateway, n)
	for i := range gws {
		clk := clock.NewFake(miniStart.Add(time.Duration(i) * 3 * time.Hour))
		gws[i] = mustGateway(t, doc, clk, nil)
	}
	return gws
}

// fire sends reqs requests from one client address. Request k goes to gateway
// k mod len(gws); each gateway is driven by multiSenders concurrent senders.
// It returns the recorders per gateway in send order.
func fire(gws []*microgateway.Gateway, reqs int) [][]*httptest.ResponseRecorder {
	n := len(gws)
	out := make([][]*httptest.ResponseRecorder, n)
	var wg sync.WaitGroup
	for i, g := range gws {
		count := 0
		for k := i; k < reqs; k += n {
			count++
		}
		out[i] = make([]*httptest.ResponseRecorder, count)
		for w := 0; w < multiSenders; w++ {
			wg.Add(1)
			go func(g *microgateway.Gateway, recs []*httptest.ResponseRecorder, first int) {
				defer wg.Done()
				for j := first; j < len(recs); j += multiSenders {
					recs[j] = doGet(g, "/rl", multiClientAddr)
				}
			}(g, out[i], w)
		}
	}
	wg.Wait()
	return out
}

// tallyRecorders counts admitted (200) and denied (429) responses per gateway.
// Any other status fails the test; every 429 must carry a Retry-After of at
// least one second and the rate_limited error code.
func tallyRecorders(t *testing.T, recs [][]*httptest.ResponseRecorder) []multiTally {
	t.Helper()
	out := make([]multiTally, len(recs))
	for i, rs := range recs {
		for _, rec := range rs {
			switch rec.Code {
			case http.StatusOK:
				out[i].admitted++
			case http.StatusTooManyRequests:
				out[i].denied++
				ra := rec.Header().Get("Retry-After")
				if v, err := strconv.Atoi(ra); err != nil || v < 1 {
					t.Errorf("gateway %d: 429 Retry-After %q, want integer >= 1", i, ra)
				}
				if code := responseErrorCode(t, rec); code != "rate_limited" {
					t.Errorf("gateway %d: 429 error code %q, want rate_limited", i, code)
				}
			default:
				t.Fatalf("gateway %d: status %d, want 200 or 429", i, rec.Code)
			}
		}
	}
	return out
}

func multiSum(tallies []multiTally) (admitted, sent int) {
	for _, ta := range tallies {
		admitted += ta.admitted
		sent += ta.admitted + ta.denied
	}
	return admitted, sent
}

// runBurst sends n·2·multiBurst requests and checks the admitted total.
func runBurst(t *testing.T, srv redisServer, n int, route string, useStore bool) {
	t.Helper()
	gws := multiGateways(t, srv, n, route, useStore)
	reqs := n * multiBurst * 2
	tallies := tallyRecorders(t, fire(gws, reqs))
	admitted, sent := multiSum(tallies)
	if sent != reqs {
		t.Fatalf("accounted %d requests, sent %d", sent, reqs)
	}
	switch {
	case !useStore:
		if admitted != n*multiBurst {
			t.Errorf("admitted %d across %d in-memory gateways, want %d", admitted, n, n*multiBurst)
		}
	case srv.exact:
		if admitted != multiBurst {
			t.Errorf("admitted %d across %d gateways, want exactly %d", admitted, n, multiBurst)
		}
	default:
		if admitted > multiBurst {
			t.Errorf("admitted %d across %d gateways, want at most %d", admitted, n, multiBurst)
		}
	}
}

// runElapsed sends four rounds of n·10 requests with 250 ms of server time
// between rounds. A 100 ms refill interval and a burst of 5 admit 5 + 7 requests
// over the 750 ms exactly.
func runElapsed(t *testing.T, srv redisServer, n int) {
	t.Helper()
	gws := multiGateways(t, srv, n, multiElapsedBody+multiStoreTail, true)
	start := srv.now(t)
	var admitted, sent int
	for round := 0; round < 4; round++ {
		if round > 0 {
			srv.advance(250 * time.Millisecond)
		}
		a, s := multiSum(tallyRecorders(t, fire(gws, n*10)))
		admitted += a
		sent += s
	}
	// Server time is read at millisecond resolution; one extra millisecond keeps elapsed at or above the server time that passed.
	elapsed := srv.now(t).Sub(start) + time.Millisecond
	if srv.exact {
		elapsed = 750 * time.Millisecond
	}
	if sent != n*40 {
		t.Fatalf("accounted %d requests, sent %d", sent, n*40)
	}
	if srv.exact && admitted != 5+7 {
		t.Errorf("admitted %d over %s, want exactly %d", admitted, elapsed, 5+7)
	}
	if bound := 5 + int(elapsed/(100*time.Millisecond)); admitted > bound {
		t.Errorf("admitted %d over %s, want at most %d", admitted, elapsed, bound)
	}
}

func runMultiInstance(t *testing.T, srv redisServer) {
	t.Helper()
	t.Run("token_bucket/2", func(t *testing.T) {
		runBurst(t, srv, 2, multiBucketBody+multiStoreTail, true)
	})
	t.Run("token_bucket/3", func(t *testing.T) {
		runBurst(t, srv, 3, multiBucketBody+multiStoreTail, true)
	})
	t.Run("sliding_window/2", func(t *testing.T) {
		runBurst(t, srv, 2, multiWindowBody+multiStoreTail, true)
	})
	t.Run("sliding_window/3", func(t *testing.T) {
		runBurst(t, srv, 3, multiWindowBody+multiStoreTail, true)
	})
	t.Run("elapsed/2", func(t *testing.T) {
		runElapsed(t, srv, 2)
	})
	t.Run("memory/2", func(t *testing.T) {
		runBurst(t, srv, 2, multiBucketBody, false)
	})
	t.Run("memory/3", func(t *testing.T) {
		runBurst(t, srv, 3, multiBucketBody, false)
	})
}

func TestMultiInstance(t *testing.T) {
	m := newMini(t)
	mc := &miniClock{m: m, now: miniStart}
	runMultiInstance(t, redisServer{addr: m.Addr(), exact: true, advance: mc.Advance, now: func(*testing.T) time.Time { return mc.now }})
}

type gcValidator struct {
	name   string
	header http.Header
	cond   string
	value  string
}

var gcValidators = []gcValidator{
	{name: "etag", header: http.Header{"Etag": {`"v1"`}}, cond: "If-None-Match", value: `"v1"`},
	{name: "last-modified", header: http.Header{"Last-Modified": {gcLastModified}}, cond: "If-Modified-Since", value: gcLastModified},
	{name: "none"},
}

func gcResponseHeader(v gcValidator) http.Header {
	h := http.Header{"Cache-Control": {"max-age=60"}, "Vary": {"Accept-Language"}}
	for k, vals := range v.header {
		h[k] = vals
	}
	return h
}

var gcEntries = []gcEntry{
	{method: http.MethodGet, path: "/p", lang: "en"},
	{method: http.MethodGet, path: "/p", lang: "fr"},
	{method: http.MethodHead, path: "/p", lang: "en"},
	{method: http.MethodGet, path: "/o"},
}

func gcCovered(sel string, e gcEntry) bool {
	return sel == "all" || e.path == "/p"
}

func gcPurgeBody(sel string, soft bool) string {
	switch sel {
	case "url":
		return fmt.Sprintf(`{"urls":["http://example.com/p"],"soft":%t}`, soft)
	case "tag":
		return fmt.Sprintf(`{"tags":["tg1"],"soft":%t}`, soft)
	}
	return fmt.Sprintf(`{"all":true,"soft":%t}`, soft)
}

func gcPurgeRequest(sel string, soft bool) microgateway.PurgeRequest {
	switch sel {
	case "url":
		return microgateway.PurgeRequest{URLs: []string{"http://example.com/p"}, Soft: soft}
	case "tag":
		return microgateway.PurgeRequest{Tags: []string{"tg1"}, Soft: soft}
	}
	return microgateway.PurgeRequest{All: true, Soft: soft}
}

func gcReplyOf(sel string, soft bool, affected int) gcReply {
	return gcReply{Type: sel, Soft: soft, Accepted: 1, Affected: gcIntp(affected), Scope: "store"}
}

func gcWant(sel string, soft bool) gcReply {
	switch {
	case sel == "all":
		return gcReply{Type: sel, Soft: soft, Accepted: 1, Scope: "store"}
	case sel == "url" && soft:
		return gcReplyOf(sel, soft, 2)
	}
	return gcReplyOf(sel, soft, 3)
}

func gcIntp(n int) *int {
	return &n
}

func gcLibraryReply(res microgateway.PurgeResult) gcReply {
	out := gcReply{Type: res.Type, Soft: res.Soft, Accepted: res.Accepted, Scope: res.Scope}
	if res.AffectedKnown {
		out.Affected = gcIntp(res.Affected)
	}
	return out
}

var gcOffsets = []time.Duration{time.Hour, -time.Hour, 0}

type gcFixture struct {
	t        *testing.T
	mc       *miniClock
	client   goredis.UniversalClient
	prefix   string
	origin   *gcOrigin
	stores   string
	cacheTop string
	routes   string
	gws      []*microgateway.Gateway
}

func newGCFixture(t *testing.T, n int) *gcFixture {
	t.Helper()
	m := newMini(t)
	f := &gcFixture{
		t:        t,
		mc:       &miniClock{m: m, now: miniStart},
		client:   newTestClient(t, m.Addr(), 0),
		prefix:   uniquePrefix(t),
		origin:   newGCOrigin(t),
		cacheTop: "  store: shared\n",
		routes:   gcRoute("r", "stale_while_revalidate: 1h", "stale_if_error: 1h"),
	}
	t.Cleanup(func() { deletePrefix(t, f.client, f.prefix) })
	f.stores = redisStoreYAML("shared", []string{m.Addr()}, fmt.Sprintf("db: 0\nkey_prefix: %s\n", f.prefix), "")
	for i := 0; i < n; i++ {
		f.gws = append(f.gws, f.gateway(gcOffsets[i%len(gcOffsets)]))
	}
	return f
}

func (f *gcFixture) doc(admin string) string {
	return gcDoc(f.stores, f.cacheTop, admin, f.routes, f.origin.url)
}

func (f *gcFixture) gateway(offset time.Duration) *microgateway.Gateway {
	return mustGateway(f.t, f.doc(gcAdminYAML(gcToken)), clock.NewFake(miniStart.Add(offset)), nil)
}

func gcMatrix(t *testing.T, n int, sel string, library, soft bool, v gcValidator) {
	t.Helper()
	f := newGCFixture(t, n)
	f.origin.set(http.StatusOK, gcResponseHeader(v), "v1")
	f.origin.tag("/p", "tg1 tg2")
	f.origin.tag("/o", "tg2")
	a := f.gws[0]
	for _, e := range gcEntries {
		rec, st := gcFetch(t, a, e)
		if rec.Code != http.StatusOK || st.Hit || !st.Stored {
			t.Fatalf("fill %s %s: status %d %+v, want 200 stored miss", e.method, e.path, rec.Code, st)
		}
	}
	if calls, _ := f.origin.counts(); calls != len(gcEntries) {
		t.Fatalf("origin requests after the fill = %d, want %d", calls, len(gcEntries))
	}
	for _, e := range gcEntries {
		ra, sa := gcFetch(t, a, e)
		if !sa.Hit {
			t.Fatalf("%s %s on the filling gateway: status %+v, want hit", e.method, e.path, sa)
		}
		for _, g := range f.gws[1:] {
			rb, sb := gcFetch(t, g, e)
			if !sb.Hit || sb.TTL != sa.TTL || sb.HasTTL != sa.HasTTL || rb.Header().Get("Age") != ra.Header().Get("Age") {
				t.Fatalf("%s %s on another gateway: status %+v Age %q, want %+v Age %q", e.method, e.path, sb, rb.Header().Get("Age"), sa, ra.Header().Get("Age"))
			}
			if e.method == http.MethodGet && rb.Body.String() != "v1|"+e.lang {
				t.Fatalf("%s %s on another gateway: body %q, want %q", e.method, e.path, rb.Body.String(), "v1|"+e.lang)
			}
		}
	}
	if calls, _ := f.origin.counts(); calls != len(gcEntries) {
		t.Fatalf("origin requests after the hits = %d, want %d", calls, len(gcEntries))
	}

	if !library {
		before := f.gwSnapshot()
		plain := mustGateway(t, f.doc(""), clock.NewFake(miniStart), nil)
		body := gcPurgeBody(sel, soft)
		rejected := []struct {
			rec    *httptest.ResponseRecorder
			status int
		}{
			{gcPurge(t, a, "", body), http.StatusUnauthorized},
			{gcPurge(t, a, gcToken+"x", body), http.StatusForbidden},
			{gcPurge(t, a, gcToken, `{"urls":`), http.StatusBadRequest},
			{gcPurge(t, a, gcToken, `{"urls":["ftp://example.com/p"]}`), http.StatusBadRequest},
			{gcPurge(t, plain, gcToken, body), http.StatusNotFound},
		}
		for i, r := range rejected {
			if r.rec.Code != r.status {
				t.Fatalf("rejected purge %d: status %d, want %d: %s", i, r.rec.Code, r.status, r.rec.Body.String())
			}
		}
		if after := f.gwSnapshot(); !reflect.DeepEqual(before, after) {
			t.Fatal("a rejected purge changed the store")
		}
	}

	var got gcReply
	if library {
		res, err := a.Purge(context.Background(), gcPurgeRequest(sel, soft))
		if err != nil {
			t.Fatalf("Purge(%s): %v", sel, err)
		}
		got = gcLibraryReply(res)
	} else {
		got = gcPurgeOK(t, gcPurge(t, a, gcToken, gcPurgeBody(sel, soft)))
	}
	if want := gcWant(sel, soft); !reflect.DeepEqual(got, want) {
		t.Fatalf("purge reply %+v, want %+v", got, want)
	}
	f.mc.Advance(2 * time.Second)
	late := f.gateway(2 * time.Hour)

	f.origin.set(http.StatusOK, gcResponseHeader(v), "v2")
	b := f.gws[1]
	calls0, _ := f.origin.counts()
	covered := 0
	for _, e := range gcEntries {
		if !gcCovered(sel, e) {
			continue
		}
		covered++
		calls1, cond1 := f.origin.counts()
		hdr := e.header()
		if hdr == nil {
			hdr = http.Header{}
		}
		if soft {
			hdr.Set("Cache-Control", "max-stale")
		}
		rec := gcRequest(b, e.method, e.url(), hdr)
		st := gcStatus(t, rec)
		calls2, cond2 := f.origin.counts()
		if calls2-calls1 != 1 {
			t.Fatalf("%s %s on the second gateway: origin requests +%d, want +1", e.method, e.path, calls2-calls1)
		}
		wantCond := 0
		if soft && v.cond != "" {
			wantCond = 1
		}
		if cond2-cond1 != wantCond {
			t.Fatalf("%s %s on the second gateway: conditional origin requests +%d, want +%d", e.method, e.path, cond2-cond1, wantCond)
		}
		last := f.origin.lastHeader()
		for _, h := range []string{"If-None-Match", "If-Modified-Since"} {
			want := ""
			if soft && h == v.cond {
				want = v.value
			}
			if got := last.Get(h); got != want {
				t.Fatalf("%s %s on the second gateway: origin %s = %q, want %q", e.method, e.path, h, got, want)
			}
		}
		wantFwd := httpcache.FwdURIMiss
		switch {
		case soft:
			wantFwd = httpcache.FwdStale
		case e.method == http.MethodGet && e.path == "/p" && e.lang != "en":
			wantFwd = httpcache.FwdVaryMiss
		}
		if rec.Code != http.StatusOK || st.Hit || !st.Stored || st.Fwd != wantFwd {
			t.Fatalf("%s %s on the second gateway: status %d %+v, want 200 miss stored fwd=%s", e.method, e.path, rec.Code, st, wantFwd)
		}
		if st.Detail == "stale-while-revalidate" || st.Detail == "stale-if-error" {
			t.Fatalf("%s %s on the second gateway served a stale entry: %+v", e.method, e.path, st)
		}
		if e.method == http.MethodGet && rec.Body.String() != "v2|"+e.lang {
			t.Fatalf("%s %s on the second gateway: body %q, want %q", e.method, e.path, rec.Body.String(), "v2|"+e.lang)
		}
		if _, st2 := gcFetch(t, b, e); !st2.Hit {
			t.Fatalf("%s %s second request on the second gateway: status %+v, want hit", e.method, e.path, st2)
		}
	}
	if calls, _ := f.origin.counts(); calls-calls0 != covered {
		t.Fatalf("origin requests for the covered entries = %d, want %d", calls-calls0, covered)
	}

	for _, g := range append(append([]*microgateway.Gateway(nil), f.gws...), late) {
		for _, e := range gcEntries {
			rec, st := gcFetch(t, g, e)
			want := "v1|" + e.lang
			if gcCovered(sel, e) {
				want = "v2|" + e.lang
			}
			if !st.Hit {
				t.Fatalf("%s %s after the purge: status %+v, want hit", e.method, e.path, st)
			}
			if e.method == http.MethodGet && rec.Body.String() != want {
				t.Fatalf("%s %s after the purge: body %q, want %q", e.method, e.path, rec.Body.String(), want)
			}
		}
	}
	if calls, _ := f.origin.counts(); calls-calls0 != covered {
		t.Fatalf("origin requests after the hits = %d, want %d", calls-calls0, covered)
	}
}

func (f *gcFixture) gwSnapshot() map[string]string {
	return gcSnapshot(f.t, f.client, f.prefix)
}

func gcSoftRevalidation(t *testing.T, n int) {
	t.Helper()
	f := newGCFixture(t, n)
	v := gcValidators[0]
	f.origin.set(http.StatusOK, gcResponseHeader(v), "v1")
	a, b := f.gws[0], f.gws[1]
	e := gcEntries[0]
	if rec, st := gcFetch(t, a, e); rec.Code != http.StatusOK || !st.Stored {
		t.Fatalf("fill: status %d %+v, want 200 stored", rec.Code, st)
	}
	if rep := gcPurgeOK(t, gcPurge(t, a, gcToken, gcPurgeBody("url", true))); !reflect.DeepEqual(rep, gcReplyOf("url", true, 1)) {
		t.Fatalf("soft purge reply %+v, want %+v", rep, gcReplyOf("url", true, 1))
	}
	f.origin.set(http.StatusServiceUnavailable, gcResponseHeader(v), "down")
	for i := 1; i <= 2; i++ {
		calls0, cond0 := f.origin.counts()
		rec := gcRequest(b, e.method, e.url(), e.header())
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("request %d during origin failure: status %d, want 503", i, rec.Code)
		}
		calls1, cond1 := f.origin.counts()
		if calls1-calls0 != 1 || cond1-cond0 != 1 {
			t.Fatalf("request %d: origin requests +%d (conditional +%d), want +1 (+1): the entry is not soft-purged", i, calls1-calls0, cond1-cond0)
		}
	}
	f.origin.set(http.StatusNotModified, gcResponseHeader(v), "")
	rec, st := gcFetch(t, b, e)
	if rec.Code != http.StatusOK || rec.Body.String() != "v1|en" || !st.Hit || st.Fwd != httpcache.FwdStale || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified || st.Detail != "" {
		t.Fatalf("304 response %d %q status %+v, want 200 old hit fwd=stale fwd-status=304", rec.Code, rec.Body.String(), st)
	}
	calls0, _ := f.origin.counts()
	recA, stA := gcFetch(t, a, e)
	if recA.Code != http.StatusOK || recA.Body.String() != "v1|en" || !stA.Hit || stA.Fwd != "" {
		t.Fatalf("refreshed entry on the first gateway: %d %q status %+v, want fresh hit with old body", recA.Code, recA.Body.String(), stA)
	}
	if calls, _ := f.origin.counts(); calls != calls0 {
		t.Fatalf("refreshed entry reached the origin: %d requests, want %d", calls, calls0)
	}
}

func gcSoftCoalesced(t *testing.T, n int) {
	t.Helper()
	f := newGCFixture(t, n)
	v := gcValidators[0]
	f.origin.set(http.StatusOK, gcResponseHeader(v), "v1")
	a, b := f.gws[0], f.gws[1]
	e := gcEntries[0]
	if rec, st := gcFetch(t, a, e); rec.Code != http.StatusOK || !st.Stored {
		t.Fatalf("fill: status %d %+v, want 200 stored", rec.Code, st)
	}
	if rep := gcPurgeOK(t, gcPurge(t, a, gcToken, gcPurgeBody("url", true))); !reflect.DeepEqual(rep, gcReplyOf("url", true, 1)) {
		t.Fatalf("soft purge reply %+v, want %+v", rep, gcReplyOf("url", true, 1))
	}
	f.origin.set(http.StatusOK, gcResponseHeader(v), "v2")
	calls0, cond0 := f.origin.counts()
	f.origin.hold()
	const waiters = 100
	out := make(chan *httptest.ResponseRecorder, waiters)
	for range waiters {
		go func() {
			out <- gcRequest(b, e.method, e.url(), e.header())
		}()
	}
	testwait.Recv(t, "leader at origin", f.origin.arrived, nil)
	testwait.Until(t, "every other concurrent request parked on the leader's call", func() bool {
		st := hooks.Inspect(b).Coalesce
		return st != nil && st.Waiters == waiters-1
	}, nil)
	f.origin.release()
	for range waiters {
		rec, _ := testwait.Recv(t, "concurrent response", out, nil)
		if rec.Code != http.StatusOK || rec.Body.String() != "v2|en" {
			t.Errorf("concurrent response %d %q, want 200 v2|en", rec.Code, rec.Body.String())
		}
	}
	calls1, cond1 := f.origin.counts()
	if calls1-calls0 != 1 || cond1-cond0 != 1 {
		t.Fatalf("origin requests for %d concurrent requests = +%d (conditional +%d), want +1 (+1)", waiters, calls1-calls0, cond1-cond0)
	}
}

func gcHeldFill(t *testing.T, n int, soft bool) {
	t.Helper()
	f := newGCFixture(t, n)
	f.origin.set(http.StatusOK, gcResponseHeader(gcValidators[2]), "held")
	f.origin.hold()
	a, b := f.gws[0], f.gws[1]
	e := gcEntry{method: http.MethodGet, path: "/h"}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- gcRequest(a, e.method, e.url(), e.header())
	}()
	testwait.Recv(t, "held fill at origin", f.origin.arrived, nil)
	rep := gcPurgeOK(t, gcPurge(t, b, gcToken, fmt.Sprintf(`{"urls":["http://example.com/h"],"soft":%t}`, soft)))
	if want := (gcReply{Type: "url", Soft: soft, Accepted: 1, Affected: gcIntp(0), Scope: "store"}); !reflect.DeepEqual(rep, want) {
		t.Fatalf("purge reply %+v, want %+v", rep, want)
	}
	f.origin.release()
	rec, _ := testwait.Recv(t, "held fill response", done, nil)
	if st := gcStatus(t, rec); rec.Code != http.StatusOK || rec.Body.String() != "held|" || st.Hit {
		t.Fatalf("held fill: status %d %q %+v, want 200 held delivered", rec.Code, rec.Body.String(), st)
	}
	refused, ok := seriesValue(metricsText(t, a), "microgateway_cache_stores_total", map[string]string{"route": "r", "result": "refused"})
	if !ok || refused != 1 {
		t.Fatalf("held fill: stores refused = %v (present %t), want 1", refused, ok)
	}
	for i, g := range []*microgateway.Gateway{b, a} {
		calls0, _ := f.origin.counts()
		rec, st := gcFetch(t, g, e)
		calls1, _ := f.origin.counts()
		if rec.Body.String() != "held|" {
			t.Fatalf("request %d: body %q, want held|", i+1, rec.Body.String())
		}
		if i == 0 && (st.Hit || st.Fwd != httpcache.FwdURIMiss || calls1-calls0 != 1) {
			t.Fatalf("first request after the purge: status %+v origin +%d, want uri-miss with one origin request", st, calls1-calls0)
		}
		if i == 1 && (!st.Hit || calls1 != calls0) {
			t.Fatalf("second request after the purge: status %+v origin +%d, want a hit", st, calls1-calls0)
		}
	}
}

func gcClockOffsets(t *testing.T) {
	t.Helper()
	f := newGCFixture(t, 0)
	f.routes = gcRoute("r")
	f.origin.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "v1")
	plus := f.gateway(time.Hour)
	minus := f.gateway(-time.Hour)
	e := gcEntry{method: http.MethodGet, path: "/c"}
	if _, st := gcFetch(t, plus, e); !st.Stored {
		t.Fatalf("fill on the gateway ahead: status %+v, want stored", st)
	}
	f.mc.Advance(59 * time.Second)
	rp, sp := gcFetch(t, plus, e)
	rm, sm := gcFetch(t, minus, e)
	if !sp.Hit || !sm.Hit || !reflect.DeepEqual(sp, sm) || rp.Header().Get("Age") != "59" || rm.Header().Get("Age") != "59" {
		t.Fatalf("at 59s: ahead %+v Age %q, behind %+v Age %q, want equal hits with Age 59", sp, rp.Header().Get("Age"), sm, rm.Header().Get("Age"))
	}
	f.mc.Advance(2 * time.Second)
	if _, sm2 := gcFetch(t, minus, e); sm2.Hit {
		t.Fatalf("at 61s on the gateway behind: status %+v, want no hit", sm2)
	}
	if rp2, sp2 := gcFetch(t, plus, e); !sp2.Hit || rp2.Body.String() != "v1|" {
		t.Fatalf("at 61s on the gateway ahead after the refill: status %+v body %q, want hit", sp2, rp2.Body.String())
	}
}

func gcMemoryScope(t *testing.T) {
	t.Helper()
	o := newGCOrigin(t)
	o.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "v1")
	doc := gcDoc("", "", gcAdminYAML(gcToken), gcRoute("r"), o.url)
	a := mustGateway(t, doc, clock.NewFake(miniStart), nil)
	b := mustGateway(t, doc, clock.NewFake(miniStart.Add(-time.Hour)), nil)
	e := gcEntry{method: http.MethodGet, path: "/m"}
	for _, g := range []*microgateway.Gateway{a, b} {
		if _, st := gcFetch(t, g, e); !st.Stored {
			t.Fatalf("fill: status %+v, want stored", st)
		}
	}
	rep := gcPurgeOK(t, gcPurge(t, a, gcToken, `{"urls":["http://example.com/m"]}`))
	if rep.Scope != "instance" || rep.Accepted != 1 || rep.Type != "url" {
		t.Fatalf("memory purge reply %+v, want url scope instance", rep)
	}
	if _, st := gcFetch(t, a, e); st.Hit || st.Fwd != httpcache.FwdURIMiss {
		t.Fatalf("receiving gateway after the purge: status %+v, want uri-miss", st)
	}
	if rec, st := gcFetch(t, b, e); !st.Hit || rec.Body.String() != "v1|" {
		t.Fatalf("other gateway after the purge: status %+v body %q, want hit", st, rec.Body.String())
	}
}

func TestCacheMultiInstance(t *testing.T) {
	for _, n := range []int{2, 3} {
		t.Run(fmt.Sprintf("gateways=%d", n), func(t *testing.T) {
			for _, sel := range []string{"url", "tag", "all"} {
				for _, library := range []bool{false, true} {
					surface := "admin"
					if library {
						surface = "library"
					}
					t.Run(sel+"/"+surface+"/hard", func(t *testing.T) {
						gcMatrix(t, n, sel, library, false, gcValidators[2])
					})
					for _, v := range gcValidators {
						t.Run(sel+"/"+surface+"/soft/"+v.name, func(t *testing.T) {
							gcMatrix(t, n, sel, library, true, v)
						})
					}
				}
			}
			t.Run("soft/origin_failure_then_304", func(t *testing.T) {
				gcSoftRevalidation(t, n)
			})
			t.Run("soft/coalesced", func(t *testing.T) {
				gcSoftCoalesced(t, n)
			})
			t.Run("held_fill/hard", func(t *testing.T) {
				gcHeldFill(t, n, false)
			})
			t.Run("held_fill/soft", func(t *testing.T) {
				gcHeldFill(t, n, true)
			})
		})
	}
	t.Run("clock_offsets", gcClockOffsets)
	t.Run("memory_scope", gcMemoryScope)
}
