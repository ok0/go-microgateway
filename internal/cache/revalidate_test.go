package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/store"
)

func TestRevalidationETag(t *testing.T) {
	rg := newRig(t, rigOpts{route: "stale_if_error: 1h"})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "old")
	rg.get("/rv", nil)
	rg.clk.Advance(61 * time.Second)
	rg.orig.script(http.StatusNotModified, http.Header{"Cache-Control": {"max-age=120"}, "Etag": {`"v1"`}}, "")
	rec := rg.get("/rv", nil)
	if got := rg.orig.last().Get("If-None-Match"); got != `"v1"` {
		t.Fatalf("origin If-None-Match %q, want \"v1\"", got)
	}
	st := lastStatus(t, rec)
	if !st.Hit || st.Fwd != httpcache.FwdStale || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified {
		t.Fatalf("status %+v, want hit fwd=stale fwd-status=304", st)
	}
	if !st.HasTTL || st.TTL != 120 {
		t.Errorf("ttl %d, want 120", st.TTL)
	}
	if rec.Code != http.StatusOK || rec.Body.String() != "old" {
		t.Errorf("client response %d %q, want 200 old", rec.Code, rec.Body.String())
	}
	if age := rec.Header().Get("Age"); age != "0" {
		t.Errorf("Age %q, want 0", age)
	}
	puts := rg.spy.callsOf(opPut)
	if len(puts) != 2 || puts[1].TTL != 120*time.Second+time.Hour {
		t.Fatalf("puts %+v, want second put with storage ttl 120s plus stale_if_error", puts)
	}
	if rg.requests(resultRevalidated) != 1 {
		t.Errorf("revalidated count %v, want 1", rg.requests(resultRevalidated))
	}
}

func TestRevalidationLastModified(t *testing.T) {
	rg := newRig(t, rigOpts{route: "stale_if_error: 1h"})
	lm := start.Format(http.TimeFormat)
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Last-Modified": {lm}}, "old")
	rg.get("/lm", nil)
	rg.clk.Advance(61 * time.Second)
	rg.orig.script(http.StatusNotModified, http.Header{"Cache-Control": {"max-age=60"}, "Last-Modified": {lm}}, "")
	rec := rg.get("/lm", nil)
	if got := rg.orig.last().Get("If-Modified-Since"); got != lm {
		t.Fatalf("origin If-Modified-Since %q, want %q", got, lm)
	}
	if st := lastStatus(t, rec); !st.Hit || st.Fwd != httpcache.FwdStale {
		t.Fatalf("status %+v, want hit fwd=stale", st)
	}
}

func TestRevalidationNonMergeable304(t *testing.T) {
	rg := newRig(t, rigOpts{route: "stale_if_error: 1h"})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "old")
	rg.get("/nm", nil)
	rg.clk.Advance(61 * time.Second)
	rg.orig.setHandle(func(w http.ResponseWriter, r *http.Request) {
		if rg.orig.count() == 2 {
			w.Header().Set("Etag", `"v2"`)
			w.Header().Set("Cache-Control", "max-age=60")
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Etag", `"v2"`)
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("new"))
	})
	rec := rg.get("/nm", nil)
	if rg.orig.count() != 3 {
		t.Fatalf("origin calls %d, want exactly one extra unconditional call (3)", rg.orig.count())
	}
	if rec.Code != http.StatusOK || rec.Body.String() != "new" {
		t.Fatalf("client response %d %q, want 200 new", rec.Code, rec.Body.String())
	}
	if st := lastStatus(t, rec); st.Hit || st.Fwd != httpcache.FwdStale {
		t.Fatalf("status %+v, want miss with fwd=stale", st)
	}
	if rg.requests(resultMiss) != 2 {
		t.Errorf("miss count %v, want 2", rg.requests(resultMiss))
	}
}

func TestRevalidationOrigin200Replaces(t *testing.T) {
	rg := newRig(t, rigOpts{route: "stale_if_error: 1h"})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "old")
	rg.get("/r200", nil)
	rg.clk.Advance(61 * time.Second)
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v2"`}}, "new")
	if rec := rg.get("/r200", nil); rec.Body.String() != "new" {
		t.Fatalf("client body %q, want new", rec.Body.String())
	}
	calls := rg.orig.count()
	rec := rg.get("/r200", nil)
	if !hit(t, rec) || rec.Body.String() != "new" {
		t.Fatalf("follow-up hit=%v body %q, want hit new", hit(t, rec), rec.Body.String())
	}
	if rg.orig.count() != calls {
		t.Errorf("origin called again for a fresh replaced entry")
	}
}

func TestRevalidationOrigin500LeavesEntry(t *testing.T) {
	rg := newRig(t, rigOpts{route: "stale_if_error: 1h"})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "old")
	rg.get("/r500", nil)
	rg.clk.Advance(61 * time.Second)
	rg.orig.script(http.StatusInternalServerError, nil, "err")
	rec := rg.get("/r500", nil)
	if rec.Code != http.StatusInternalServerError || rec.Body.String() != "err" {
		t.Fatalf("client response %d %q, want the origin 500", rec.Code, rec.Body.String())
	}
	if n := len(rg.spy.callsOf(opPut)); n != 1 {
		t.Fatalf("puts %d, want only the fill", n)
	}
	rg.orig.script(http.StatusNotModified, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "")
	rec = rg.get("/r500", nil)
	if rec.Body.String() != "old" {
		t.Fatalf("entry changed after 500: body %q", rec.Body.String())
	}
}

func TestRevalidationClientPreconditions(t *testing.T) {
	t.Run("weak ETag match gives 304 without body", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "body")
		rg.get("/c", nil)
		rec := rg.get("/c", http.Header{"If-None-Match": {`W/"v1"`}})
		if rec.Code != http.StatusNotModified || rec.Body.Len() != 0 {
			t.Fatalf("status %d body %q, want 304 without body", rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Etag") != `"v1"` || len(rec.Header().Values("Age")) != 1 || rec.Header().Get("Cache-Status") == "" {
			t.Errorf("304 headers %v", rec.Header())
		}
		if rg.orig.count() != 1 {
			t.Errorf("origin calls %d, want 1", rg.orig.count())
		}
	})
	t.Run("non-matching ETag gives 200", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "body")
		rg.get("/c", nil)
		rec := rg.get("/c", http.Header{"If-None-Match": {`"other"`}})
		if rec.Code != http.StatusOK || rec.Body.String() != "body" {
			t.Fatalf("status %d body %q, want 200 body", rec.Code, rec.Body.String())
		}
	})
	t.Run("If-Modified-Since gives 304", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		lm := start.Format(http.TimeFormat)
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Last-Modified": {lm}}, "body")
		rg.get("/c", nil)
		rec := rg.get("/c", http.Header{"If-Modified-Since": {lm}})
		if rec.Code != http.StatusNotModified {
			t.Fatalf("status %d, want 304", rec.Code)
		}
	})
}

func TestRevalidationSoftPurgeNeverServedStale(t *testing.T) {
	rg := newRig(t, rigOpts{})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "old")
	rg.get("/soft", nil)
	primary := httpcache.PrimaryKey(httptest.NewRequest(http.MethodGet, "http://example.com/soft", nil))
	if _, err := rg.spy.Purge(context.Background(), store.Purge{Kind: store.PurgeKeys, Keys: []string{primary}, Soft: true, Retain: time.Hour}); err != nil {
		t.Fatal(err)
	}
	rg.orig.script(http.StatusNotModified, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "")
	rec := rg.get("/soft", http.Header{"Cache-Control": {"max-stale"}})
	if rg.orig.count() != 2 {
		t.Fatalf("origin calls %d, want the soft-purged entry revalidated", rg.orig.count())
	}
	st := lastStatus(t, rec)
	if st.Fwd != httpcache.FwdStale || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified {
		t.Fatalf("status %+v, want revalidated with fwd-status 304", st)
	}
}

func TestRevalidationStaleWhileRevalidateWindow(t *testing.T) {
	rg := newRig(t, rigOpts{route: "stale_while_revalidate: 1h"})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "old")
	rg.get("/swr", nil)
	rg.clk.Advance(61 * time.Second)
	rg.orig.script(http.StatusNotModified, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "")
	rec := rg.get("/swr", nil)
	if got := rg.orig.last().Get("If-None-Match"); got != `"v1"` {
		t.Fatalf("origin If-None-Match %q, want \"v1\"", got)
	}
	if rg.requests(resultRevalidated) != 1 || rec.Body.String() != "old" {
		t.Fatalf("revalidated=%v body %q", rg.requests(resultRevalidated), rec.Body.String())
	}
}
