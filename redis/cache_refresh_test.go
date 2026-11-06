package redis

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ok0/go-microgateway/internal/httpcache"
)

func gcRefreshFill(t *testing.T, f *gcFixture, v gcValidator) {
	t.Helper()
	f.origin.set(http.StatusOK, gcResponseHeader(v), "v1")
	for _, e := range gcEntries[:2] {
		if rec, st := gcFetch(t, f.gws[0], e); rec.Code != http.StatusOK || !st.Stored {
			t.Fatalf("fill %s: status %d %+v, want 200 stored", e.lang, rec.Code, st)
		}
	}
}

func TestCacheRefreshNotStorable(t *testing.T) {
	t.Parallel()
	v := gcValidators[0]
	cases := []struct {
		name string
		cc   string
	}{
		{"private", "private, max-age=60"},
		{"no-store", "no-store"},
		{"ordinary", "max-age=60"},
	}
	for _, trigger := range []string{"no-cache", "soft"} {
		for _, c := range cases {
			t.Run(trigger+"/"+c.name, func(t *testing.T) {
				f := newGCFixture(t, 2)
				a, b := f.gws[0], f.gws[1]
				en, fr := gcEntries[0], gcEntries[1]
				gcRefreshFill(t, f, v)
				h := gcResponseHeader(v)
				h.Set("Cache-Control", c.cc)
				f.origin.set(http.StatusNotModified, h, "")

				var rec *httptest.ResponseRecorder
				var st httpcache.Status
				if trigger == "no-cache" {
					nh := en.header()
					nh.Set("Cache-Control", "no-cache")
					rec = gcRequest(a, en.method, en.url(), nh)
					st = gcStatus(t, rec)
				} else {
					gcPurgeOK(t, gcPurge(t, a, gcToken, gcPurgeBody("url", true)))
					rec, st = gcFetch(t, a, en)
				}
				if rec.Code != http.StatusOK || rec.Body.String() != "v1|en" {
					t.Fatalf("refresh response %d %q, want 200 with the fill body", rec.Code, rec.Body.String())
				}

				if c.name == "ordinary" {
					if !st.Hit || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified {
						t.Fatalf("storable refresh status %+v, want hit with fwd-status=304", st)
					}
					calls0, _ := f.origin.counts()
					if rb, sb := gcFetch(t, b, en); rb.Body.String() != "v1|en" || !sb.Hit || sb.Fwd != "" {
						t.Fatalf("refreshed entry on the other gateway: %q status %+v, want fresh hit with fill body", rb.Body.String(), sb)
					}
					if calls, _ := f.origin.counts(); calls != calls0 {
						t.Fatalf("refreshed entry reached the origin: %d requests, want %d", calls, calls0)
					}
					return
				}

				if st.Hit || st.Fwd != httpcache.FwdStale || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified {
					t.Fatalf("non-storable refresh status %+v, want non-hit fwd=stale fwd-status=304", st)
				}
				if trigger == "no-cache" {
					calls0, _ := f.origin.counts()
					if _, sb := gcFetch(t, b, fr); !sb.Hit {
						t.Fatalf("sibling variant after the refresh: status %+v, want hit", sb)
					}
					if calls, _ := f.origin.counts(); calls != calls0 {
						t.Fatalf("sibling variant reached the origin: %d requests, want %d", calls, calls0)
					}
				}
				f.origin.set(http.StatusServiceUnavailable, gcResponseHeader(v), "down")
				for _, g := range []http.Handler{b, a} {
					calls0, cond0 := f.origin.counts()
					rr, _ := gcFetch(t, g, en)
					calls1, cond1 := f.origin.counts()
					if rr.Code != http.StatusServiceUnavailable || calls1-calls0 != 1 || cond1-cond0 != 0 {
						t.Fatalf("after the origin failure: status %d, origin requests +%d (conditional +%d), want 503, +1, +0", rr.Code, calls1-calls0, cond1-cond0)
					}
				}
			})
		}
	}
}

func gcRequestSideRefresh(t *testing.T, trigger, req, resp string, storable bool) {
	t.Helper()
	f := newGCFixture(t, 2)
	v := gcValidators[0]
	a, b := f.gws[0], f.gws[1]
	en := gcEntries[0]
	gcRefreshFill(t, f, v)
	h := gcResponseHeader(v)
	h.Set("Cache-Control", resp)
	f.origin.set(http.StatusNotModified, h, "")

	rh := en.header()
	switch req {
	case "no-store":
		cc := "no-store"
		if trigger == "no-cache" {
			cc = "no-cache, no-store"
		}
		rh.Set("Cache-Control", cc)
	case "authorization":
		rh.Set("Authorization", "Bearer t")
		if trigger == "no-cache" {
			rh.Set("Cache-Control", "no-cache")
		}
	}
	if trigger == "soft" {
		gcPurgeOK(t, gcPurge(t, a, gcToken, gcPurgeBody("url", true)))
	}
	rec := gcRequest(a, en.method, en.url(), rh)
	st := gcStatus(t, rec)
	if rec.Code != http.StatusOK || rec.Body.String() != "v1|en" || st.Hit || st.Fwd != httpcache.FwdStale || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified {
		t.Fatalf("request-side refresh: %d %q status %+v, want 200 non-hit fwd=stale fwd-status=304", rec.Code, rec.Body.String(), st)
	}

	calls0, cond0 := f.origin.counts()
	rb, sb := gcFetch(t, b, en)
	calls1, cond1 := f.origin.counts()
	switch {
	case !storable:
		if calls1-calls0 != 1 || cond1-cond0 != 0 {
			t.Fatalf("entry after the refresh on the other gateway: origin requests +%d (conditional +%d), want +1 (+0)", calls1-calls0, cond1-cond0)
		}
	case trigger == "no-cache":
		if rb.Body.String() != "v1|en" || !sb.Hit || sb.Fwd != "" || calls1 != calls0 {
			t.Fatalf("entry on the other gateway: %q status %+v origin requests +%d, want fresh hit with fill body and no origin request", rb.Body.String(), sb, calls1-calls0)
		}
	default:
		if cond1-cond0 != 1 || !sb.Hit || !sb.HasFwdStatus || sb.FwdStatus != http.StatusNotModified {
			t.Fatalf("entry on the other gateway: status %+v conditional +%d, want hit with fwd-status=304 after one conditional request", sb, cond1-cond0)
		}
	}
}

func TestCacheRefreshRequestSide(t *testing.T) {
	t.Parallel()
	for _, trigger := range []string{"no-cache", "soft"} {
		for _, req := range []string{"no-store", "authorization"} {
			t.Run(trigger+"/"+req, func(t *testing.T) {
				gcRequestSideRefresh(t, trigger, req, "max-age=60", true)
			})
		}
	}
	t.Run("no-cache/authorization-private", func(t *testing.T) {
		gcRequestSideRefresh(t, "no-cache", "authorization", "private, max-age=60", false)
	})
}
