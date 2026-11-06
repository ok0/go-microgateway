package e2e

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/teststore"
	"github.com/ok0/go-microgateway/store"
)

type refreshReq struct {
	name string
	hdr  http.Header
	cc   string
}

type refreshRig struct {
	a, b     *microgateway.Gateway
	scr      *cacheScript
	spy      *teststore.Spy
	storeClk *clock.Fake
}

func newRefreshRig(t *testing.T, route string) refreshRig {
	t.Helper()
	n := newMemNetwork()
	scr := &cacheScript{}
	be := n.serve(t, scr)
	storeClk := clock.NewFake(cacheStart)
	sc := teststore.NewSharedCache(storeClk, 1<<20)
	t.Cleanup(func() { _ = sc.Close() })
	spy := teststore.NewSpy()
	doc := cacheGatewayDoc(be.URL, "stores:\n  - name: sc\n    type: test\ncache:\n  store: sc\n", cacheRouteYAML("r", route))
	gws, _ := miInstances(t, n, 2, doc, cacheStart, 5*time.Hour, miSpyOption(spy, store.CapCache, nil, sc, nil))
	return refreshRig{a: gws[0], b: gws[1], scr: scr, spy: spy, storeClk: storeClk}
}

func (r refreshRig) fill(t *testing.T) {
	t.Helper()
	r.scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "v1")
	if st := cacheLast(t, cacheDo(r.a, http.MethodGet, "http://example.com/k", nil)); !st.Stored {
		t.Fatalf("fill %+v, want stored", st)
	}
}

func (r refreshRig) get(g *microgateway.Gateway, hdr http.Header) *httptest.ResponseRecorder {
	return cacheDo(g, http.MethodGet, "http://example.com/k", hdr)
}

func TestRefreshNotStorableInstances(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		cc   string
	}{
		{"private", "private"},
		{"no-store", "no-store"},
		{"ordinary", "max-age=60"},
	}
	for _, trig := range []string{"expired", "no-cache"} {
		for _, c := range cases {
			t.Run(trig+"/"+c.name, func(t *testing.T) {
				t.Parallel()
				r := newRefreshRig(t, "stale_if_error: 1h")
				r.fill(t)
				r.scr.set(http.StatusNotModified, http.Header{"Cache-Control": {c.cc}, "Etag": {`"v1"`}}, "")

				var rec *httptest.ResponseRecorder
				if trig == "expired" {
					r.storeClk.Advance(61 * time.Second)
					rec = r.get(r.a, nil)
				} else {
					rec = r.get(r.a, http.Header{"Cache-Control": {"no-cache"}})
				}
				st := cacheLast(t, rec)
				if rec.Code != http.StatusOK || rec.Body.String() != "v1" || st.FwdStatus != http.StatusNotModified || !st.HasFwdStatus {
					t.Fatalf("A response %d %q %+v, want 200 v1 with fwd-status 304", rec.Code, rec.Body.String(), st)
				}

				if c.name == "ordinary" {
					if !st.Hit {
						t.Fatalf("A %+v, want hit", st)
					}
					if d := r.spy.Count("delete"); d != 0 {
						t.Fatalf("deletes = %d, want 0", d)
					}
					if puts := r.spy.Count("put"); puts != 2 {
						t.Fatalf("puts = %d, want 2", puts)
					}
					before := r.scr.count()
					if stB := cacheLast(t, r.get(r.b, nil)); !stB.Hit || r.scr.count() != before {
						t.Fatalf("B %+v origin requests %d, want hit with no origin request", stB, r.scr.count()-before)
					}
					return
				}

				if st.Hit {
					t.Fatalf("A %+v, want no hit", st)
				}
				if d := r.spy.Count("delete"); d != 1 {
					t.Fatalf("deletes = %d, want 1", d)
				}
				if p := r.spy.Count("put"); p != 1 {
					t.Fatalf("puts = %d, want 1", p)
				}
				if v := rsMetric(t, r.a, "microgateway_cache_stores_total", "route", "r", "result", "not_storable"); v != 1 {
					t.Fatalf("not_storable = %v, want 1", v)
				}

				r.scr.set(http.StatusServiceUnavailable, nil, "down")
				before := r.scr.count()
				recB := r.get(r.b, nil)
				if recB.Code != http.StatusServiceUnavailable || recB.Body.String() != "down" {
					t.Fatalf("B response %d %q, want 503 down", recB.Code, recB.Body.String())
				}
				if d := r.scr.count() - before; d != 1 {
					t.Fatalf("B origin requests = %d, want 1", d)
				}
				if h := r.scr.lastHeader().Get("If-None-Match"); h != "" {
					t.Fatalf("B If-None-Match = %q, want none", h)
				}
				if recA := r.get(r.a, nil); recA.Code != http.StatusServiceUnavailable {
					t.Fatalf("A response %d, want 503", recA.Code)
				}
			})
		}
	}
}

func TestRefreshRequestSideInstances(t *testing.T) {
	t.Parallel()
	requests := []refreshReq{
		{"no-store", http.Header{"Cache-Control": {"no-store"}}, "no-cache, no-store"},
		{"authorization", http.Header{"Authorization": {"Bearer t"}}, "no-cache"},
	}
	triggerHeader := func(req refreshReq, trig string) http.Header {
		h := http.Header{}
		for k, v := range req.hdr {
			h[k] = v
		}
		if trig == "no-cache" {
			h.Set("Cache-Control", req.cc)
		}
		return h
	}

	for _, trig := range []string{"no-cache", "expired"} {
		for _, req := range requests {
			t.Run(trig+"/"+req.name, func(t *testing.T) {
				t.Parallel()
				r := newRefreshRig(t, "stale_if_error: 1h")
				r.fill(t)
				r.scr.set(http.StatusNotModified, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "")

				if trig == "expired" {
					r.storeClk.Advance(61 * time.Second)
				}
				rec := r.get(r.a, triggerHeader(req, trig))
				st := cacheLast(t, rec)
				if rec.Code != http.StatusOK || rec.Body.String() != "v1" || st.Hit || st.FwdStatus != http.StatusNotModified || !st.HasFwdStatus {
					t.Fatalf("A response %d %q %+v, want 200 v1 without hit and with fwd-status 304", rec.Code, rec.Body.String(), st)
				}
				if d := r.spy.Count("delete"); d != 0 {
					t.Fatalf("deletes = %d, want 0", d)
				}
				if p := r.spy.Count("put"); p != 1 {
					t.Fatalf("puts = %d, want 1", p)
				}
				if v := rsMetric(t, r.a, "microgateway_cache_stores_total", "route", "r", "result", "not_storable"); v != 1 {
					t.Fatalf("not_storable = %v, want 1", v)
				}

				before := r.scr.count()
				stB := cacheLast(t, r.get(r.b, nil))
				if trig == "no-cache" {
					if !stB.Hit || r.scr.count() != before {
						t.Fatalf("B %+v origin requests %d, want hit with no origin request", stB, r.scr.count()-before)
					}
					return
				}
				if r.scr.count() != before+1 || r.scr.lastHeader().Get("If-None-Match") != `"v1"` {
					t.Fatalf("B origin requests %d If-None-Match %q, want one conditional request", r.scr.count()-before, r.scr.lastHeader().Get("If-None-Match"))
				}
				if !stB.Hit || stB.FwdStatus != http.StatusNotModified {
					t.Fatalf("B %+v, want hit with fwd-status 304", stB)
				}
				if p := r.spy.Count("put"); p != 2 {
					t.Fatalf("puts after B = %d, want 2", p)
				}
			})
		}
	}

	t.Run("no-cache/authorization-private", func(t *testing.T) {
		t.Parallel()
		r := newRefreshRig(t, "stale_if_error: 1h")
		r.fill(t)
		r.scr.set(http.StatusNotModified, http.Header{"Cache-Control": {"private, max-age=60"}, "Etag": {`"v1"`}}, "")
		rec := r.get(r.a, http.Header{"Authorization": {"Bearer t"}, "Cache-Control": {"no-cache"}})
		if rec.Code != http.StatusOK || rec.Body.String() != "v1" {
			t.Fatalf("A response %d %q, want 200 v1", rec.Code, rec.Body.String())
		}
		if d := r.spy.Count("delete"); d != 1 {
			t.Fatalf("deletes = %d, want 1", d)
		}
		before := r.scr.count()
		r.get(r.b, nil)
		if r.scr.count() != before+1 {
			t.Fatalf("B origin requests = %d, want 1", r.scr.count()-before)
		}
		if h := r.scr.lastHeader().Get("If-None-Match"); h != "" {
			t.Fatalf("B If-None-Match = %q, want none", h)
		}
	})
}

func TestRefreshDeleteFailureGateway(t *testing.T) {
	t.Parallel()
	n := newMemNetwork()
	scr := &cacheScript{}
	scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "v1")
	be := n.serve(t, scr)
	fake := clock.NewFake(cacheStart)
	shared := teststore.NewSharedCache(fake, 1<<20)
	t.Cleanup(func() { _ = shared.Close() })
	logs := &logBuf{}
	doc := cacheSharedDoc(be.URL, "  store_timeout: 100ms\n", cacheRouteYAML("r"))
	g := n.gateway(t, doc, withClock(fake), cacheSharedOption("shared", shared), microgateway.WithLogger(newJSONLogger(logs)))

	if v := rsMetric(t, g, "microgateway_cache_store_errors_total", "op", "delete"); v != 0 {
		t.Fatalf("cache delete errors before requests = %v, want 0", v)
	}
	if v := rsMetric(t, g, "microgateway_store_errors_total", "store", "sc", "op", "delete"); v != 0 {
		t.Fatalf("store delete errors before requests = %v, want 0", v)
	}

	if st := cacheLast(t, cacheDo(g, http.MethodGet, "http://example.com/k", nil)); !st.Stored {
		t.Fatalf("fill %+v, want stored", st)
	}
	shared.SetOpMode("delete", teststore.Fail, fmt.Errorf("delete down: %w", store.ErrUnavailable))
	scr.set(http.StatusNotModified, http.Header{"Cache-Control": {"private, max-age=60"}, "Etag": {`"v1"`}}, "")
	rec := cacheDo(g, http.MethodGet, "http://example.com/k", http.Header{"Cache-Control": {"no-cache"}})
	st := cacheLast(t, rec)
	if rec.Code != http.StatusOK || rec.Body.String() != "v1" || st.Hit || st.FwdStatus != http.StatusNotModified || !st.HasFwdStatus {
		t.Fatalf("response %d %q %+v, want 200 v1 without hit and with fwd-status 304", rec.Code, rec.Body.String(), st)
	}
	shared.SetOpMode("delete", teststore.Inherit, nil)

	if v := rsMetric(t, g, "microgateway_cache_store_errors_total", "op", "delete"); v != 1 {
		t.Fatalf("cache delete errors = %v, want 1", v)
	}
	if v := rsMetric(t, g, "microgateway_store_errors_total", "store", "sc", "op", "delete"); v != 1 {
		t.Fatalf("store delete errors = %v, want 1", v)
	}
	warns := logs.records(t, "cache store call failed; request served without the cache")
	if len(warns) != 1 || warns[0]["op"] != "delete" {
		t.Fatalf("store failure records = %v, want one with op delete", warns)
	}
	if v := rsMetric(t, g, "microgateway_cache_stores_total", "route", "r", "result", "not_storable"); v != 1 {
		t.Fatalf("not_storable = %v, want 1", v)
	}
}
