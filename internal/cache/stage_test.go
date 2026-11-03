package cache

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/store"
)

func TestEligibility(t *testing.T) {
	cases := []struct {
		name   string
		method string
		hdr    http.Header
		fwd    string
		detail string
		bypass bool
	}{
		{name: "post", method: http.MethodPost, fwd: httpcache.FwdMethod},
		{name: "put", method: http.MethodPut, fwd: httpcache.FwdMethod},
		{name: "patch", method: http.MethodPatch, fwd: httpcache.FwdMethod},
		{name: "delete", method: http.MethodDelete, fwd: httpcache.FwdMethod},
		{name: "options", method: http.MethodOptions, fwd: httpcache.FwdMethod},
		{name: "upgrade", method: http.MethodGet, bypass: true, fwd: httpcache.FwdBypass, detail: "upgrade",
			hdr: http.Header{"Connection": {"keep-alive, Upgrade"}, "Upgrade": {"websocket"}}},
		{name: "range", method: http.MethodGet, bypass: true, fwd: httpcache.FwdBypass, detail: "range",
			hdr: http.Header{"Range": {"bytes=0-1"}}},
		{name: "if-range", method: http.MethodGet, bypass: true, fwd: httpcache.FwdBypass, detail: "range",
			hdr: http.Header{"If-Range": {`"v1"`}}},
		{name: "if-match", method: http.MethodGet, bypass: true, fwd: httpcache.FwdBypass, detail: "precondition",
			hdr: http.Header{"If-Match": {`"v1"`}}},
		{name: "if-unmodified-since", method: http.MethodGet, bypass: true, fwd: httpcache.FwdBypass, detail: "precondition",
			hdr: http.Header{"If-Unmodified-Since": {start.Format(http.TimeFormat)}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rg := newRig(t, rigOpts{})
			rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "body")
			rec := rg.serve(c.method, "/e", c.hdr)
			st := lastStatus(t, rec)
			if st.Fwd != c.fwd || st.Detail != c.detail {
				t.Fatalf("fwd=%q detail=%q, want fwd=%q detail=%q", st.Fwd, st.Detail, c.fwd, c.detail)
			}
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d, want 200", rec.Code)
			}
			if c.bypass {
				if n := len(rg.spy.callsOf("")); n != 0 {
					t.Fatalf("store called %d times, want 0", n)
				}
			} else if n := len(rg.spy.callsOf(opGet)); n != 0 {
				t.Fatalf("get called %d times, want 0", n)
			}
			if got := rg.orig.count(); got != 1 {
				t.Fatalf("origin called %d times, want 1", got)
			}
		})
	}
}

type ruleStep struct {
	advance time.Duration
	query   string
	hdr     http.Header
	ident   bool
}

type ruleCase struct {
	name   string
	route  string
	cred   func(*http.Request) bool
	origin http.Header
	steps  []ruleStep
	hit    bool
	fwd    string
	code   int
	detail string
	body   string
	calls  int
}

func manyVary(n int) string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("X-Vary-%02d", i)
	}
	return strings.Join(names, ", ")
}

func TestRules(t *testing.T) {
	apiKey := func(r *http.Request) bool { return r.Header.Get("X-Api-Key") != "" }
	apiKeyQuery := func(r *http.Request) bool { return r.URL.Query().Has("k") }
	identity := func(r *http.Request) bool {
		_, ok := reqctx.IdentityFrom(r.Context())
		return ok
	}
	cases := []ruleCase{
		{name: "fresh hit after fill", origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []ruleStep{{}, {}}, hit: true, code: 200, calls: 1},
		{name: "no-store", origin: http.Header{"Cache-Control": {"no-store, max-age=60"}},
			steps: []ruleStep{{}, {}}, fwd: "uri-miss", code: 200, calls: 2},
		{name: "private", origin: http.Header{"Cache-Control": {"private, max-age=60"}},
			steps: []ruleStep{{}, {}}, fwd: "uri-miss", code: 200, calls: 2},
		{name: "no-cache with validator and keep revalidates", route: "keep: 1h",
			origin: http.Header{"Cache-Control": {"no-cache"}, "Etag": {`"a"`}},
			steps:  []ruleStep{{}, {}}, fwd: "stale", code: 200, calls: 2},
		{name: "no-cache with validator and no keep is not stored", origin: http.Header{"Cache-Control": {"no-cache"}, "Etag": {`"a"`}},
			steps: []ruleStep{{}, {}}, fwd: "uri-miss", code: 200, calls: 2},
		{name: "no-cache without validator is not stored", origin: http.Header{"Cache-Control": {"no-cache, max-age=60"}},
			steps: []ruleStep{{}, {}}, fwd: "uri-miss", code: 200, calls: 2},
		{name: "s-maxage over max-age", origin: http.Header{"Cache-Control": {"max-age=0, s-maxage=60"}},
			steps: []ruleStep{{}, {}}, hit: true, code: 200, calls: 1},
		{name: "expires", origin: http.Header{"Expires": {start.Add(time.Minute).Format(http.TimeFormat)}},
			steps: []ruleStep{{}, {}}, hit: true, code: 200, calls: 1},
		{name: "vary hit for same variant", origin: http.Header{"Vary": {"Accept-Language"}, "Cache-Control": {"max-age=60"}},
			steps: []ruleStep{{hdr: http.Header{"Accept-Language": {"en"}}}, {hdr: http.Header{"Accept-Language": {"en"}}}},
			hit:   true, code: 200, calls: 1},
		{name: "vary miss for other variant", origin: http.Header{"Vary": {"Accept-Language"}, "Cache-Control": {"max-age=60"}},
			steps: []ruleStep{{hdr: http.Header{"Accept-Language": {"en"}}}, {hdr: http.Header{"Accept-Language": {"fr"}}}},
			fwd:   "vary-miss", code: 200, calls: 2},
		{name: "vary star", origin: http.Header{"Vary": {"*"}, "Cache-Control": {"max-age=60"}},
			steps: []ruleStep{{}, {}}, fwd: "uri-miss", code: 200, calls: 2},
		{name: "seventeen vary names", origin: http.Header{"Vary": {manyVary(17)}, "Cache-Control": {"max-age=60"}},
			steps: []ruleStep{{}, {}}, fwd: "uri-miss", code: 200, calls: 2},
		{name: "set-cookie", origin: http.Header{"Set-Cookie": {"a=b"}, "Cache-Control": {"max-age=60"}},
			steps: []ruleStep{{}, {}}, fwd: "uri-miss", code: 200, calls: 2},
		{name: "authorization without public", origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []ruleStep{{hdr: http.Header{"Authorization": {"Bearer t"}}}, {hdr: http.Header{"Authorization": {"Bearer t"}}}},
			fwd:   "uri-miss", code: 200, calls: 2},
		{name: "authorization with public", origin: http.Header{"Cache-Control": {"public, max-age=60"}},
			steps: []ruleStep{{hdr: http.Header{"Authorization": {"Bearer t"}}}, {hdr: http.Header{"Authorization": {"Bearer t"}}}},
			hit:   true, code: 200, calls: 1},
		{name: "authorization with s-maxage", origin: http.Header{"Cache-Control": {"s-maxage=60"}},
			steps: []ruleStep{{hdr: http.Header{"Authorization": {"Bearer t"}}}, {hdr: http.Header{"Authorization": {"Bearer t"}}}},
			hit:   true, code: 200, calls: 1},
		{name: "authorization with must-revalidate", origin: http.Header{"Cache-Control": {"must-revalidate, max-age=60"}},
			steps: []ruleStep{{hdr: http.Header{"Authorization": {"Bearer t"}}}, {hdr: http.Header{"Authorization": {"Bearer t"}}}},
			hit:   true, code: 200, calls: 1},
		{name: "api key header without public", cred: apiKey, origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []ruleStep{{hdr: http.Header{"X-Api-Key": {"k"}}}, {hdr: http.Header{"X-Api-Key": {"k"}}}},
			fwd:   "uri-miss", code: 200, calls: 2},
		{name: "api key header with public", cred: apiKey, origin: http.Header{"Cache-Control": {"public, max-age=60"}},
			steps: []ruleStep{{hdr: http.Header{"X-Api-Key": {"k"}}}, {hdr: http.Header{"X-Api-Key": {"k"}}}},
			hit:   true, code: 200, calls: 1},
		{name: "api key query without public", cred: apiKeyQuery, origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []ruleStep{{query: "?k=1"}, {query: "?k=1"}}, fwd: "uri-miss", code: 200, calls: 2},
		{name: "api key query with s-maxage", cred: apiKeyQuery, origin: http.Header{"Cache-Control": {"s-maxage=60"}},
			steps: []ruleStep{{query: "?k=1"}, {query: "?k=1"}}, hit: true, code: 200, calls: 1},
		{name: "api key header with must-revalidate", cred: apiKey, origin: http.Header{"Cache-Control": {"must-revalidate, max-age=60"}},
			steps: []ruleStep{{hdr: http.Header{"X-Api-Key": {"k"}}}, {hdr: http.Header{"X-Api-Key": {"k"}}}}, hit: true, code: 200, calls: 1},
		{name: "credential known only from identity", cred: identity, origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []ruleStep{{ident: true}, {ident: true}}, fwd: "uri-miss", code: 200, calls: 2},
		{name: "request no-cache revalidates", origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []ruleStep{{}, {hdr: http.Header{"Cache-Control": {"no-cache"}}}}, fwd: "stale", code: 200, calls: 2},
		{name: "request max-age zero revalidates", origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []ruleStep{{}, {advance: time.Second, hdr: http.Header{"Cache-Control": {"max-age=0"}}}}, fwd: "stale", code: 200, calls: 2},
		{name: "request min-fresh above remaining", origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []ruleStep{{}, {hdr: http.Header{"Cache-Control": {"min-fresh=100"}}}}, fwd: "stale", code: 200, calls: 2},
		{name: "request max-stale serves stale", route: "stale_if_error: 1h", origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []ruleStep{{}, {advance: 120 * time.Second, hdr: http.Header{"Cache-Control": {"max-stale=3600"}}}},
			hit:   true, code: 200, calls: 1},
		{name: "only-if-cached miss", origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []ruleStep{{hdr: http.Header{"Cache-Control": {"only-if-cached"}}}},
			code:  http.StatusGatewayTimeout, detail: "only-if-cached", body: "not_cached", calls: 0},
		{name: "only-if-cached hit", origin: http.Header{"Cache-Control": {"max-age=60"}},
			steps: []ruleStep{{}, {hdr: http.Header{"Cache-Control": {"only-if-cached"}}}}, hit: true, code: 200, calls: 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rg := newRig(t, rigOpts{route: c.route, credential: c.cred})
			rg.orig.script(http.StatusOK, c.origin, "body")
			var rec *httptest.ResponseRecorder
			for _, s := range c.steps {
				if s.advance > 0 {
					rg.clk.Advance(s.advance)
				}
				req := httptest.NewRequest(http.MethodGet, "http://example.com/r"+s.query, nil)
				for k, v := range s.hdr {
					req.Header[k] = v
				}
				if s.ident {
					req = req.WithContext(reqctx.WithIdentity(req.Context(), reqctx.Identity{Provider: "p", Subject: "s"}))
				}
				rec = do(rg.h, req)
			}
			st := lastStatus(t, rec)
			if st.Hit != c.hit {
				t.Errorf("hit=%v, want %v", st.Hit, c.hit)
			}
			if st.Fwd != c.fwd {
				t.Errorf("fwd=%q, want %q", st.Fwd, c.fwd)
			}
			if rec.Code != c.code {
				t.Errorf("status %d, want %d", rec.Code, c.code)
			}
			if c.detail != "" && st.Detail != c.detail {
				t.Errorf("detail=%q, want %q", st.Detail, c.detail)
			}
			if c.body != "" && !strings.Contains(rec.Body.String(), c.body) {
				t.Errorf("body %q lacks %q", rec.Body.String(), c.body)
			}
			if got := rg.orig.count(); got != c.calls {
				t.Errorf("origin calls %d, want %d", got, c.calls)
			}
		})
	}
}

func TestHitHeaders(t *testing.T) {
	t.Run("status headers and body", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, http.Header{
			"Content-Type":   {"text/plain"},
			"Cache-Control":  {"max-age=60"},
			"Etag":           {`"v1"`},
			"Keep-Alive":     {"timeout=5"},
			"X-Custom":       {"1"},
			"Content-Length": {"5"},
		}, "hello")
		first := rg.get("/p?q=1", nil)
		if first.Code != http.StatusOK || first.Body.String() != "hello" {
			t.Fatalf("miss response %d %q", first.Code, first.Body.String())
		}
		second := rg.get("/p?q=1", nil)
		if !hit(t, second) {
			t.Fatal("second request not a hit")
		}
		if second.Code != http.StatusOK || second.Body.String() != "hello" {
			t.Fatalf("hit response %d %q", second.Code, second.Body.String())
		}
		h := second.Header()
		if h.Get("Content-Type") != "text/plain" || h.Get("X-Custom") != "1" || h.Get("Etag") != `"v1"` {
			t.Errorf("hit headers differ from origin: %v", h)
		}
		if h.Get("Content-Length") != "5" {
			t.Errorf("Content-Length %q, want 5", h.Get("Content-Length"))
		}
		if len(h.Values("Age")) != 1 {
			t.Errorf("Age values %v, want one", h.Values("Age"))
		}
		if h.Get("Keep-Alive") != "" {
			t.Errorf("hop-by-hop Keep-Alive stored and served")
		}
		if rg.orig.count() != 1 {
			t.Errorf("origin calls %d, want 1", rg.orig.count())
		}
	})
	t.Run("HEAD hit has no body and GET entry is not used", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"5"}}, "hello")
		rg.get("/h", nil)
		head := rg.serve(http.MethodHead, "/h", nil)
		if hit(t, head) {
			t.Fatal("HEAD served from the GET entry")
		}
		if rg.orig.count() != 2 {
			t.Fatalf("origin calls %d after HEAD, want 2", rg.orig.count())
		}
		fill := rg.serve(http.MethodHead, "/h", nil)
		if !hit(t, fill) {
			t.Fatal("second HEAD not a hit")
		}
		if fill.Body.Len() != 0 {
			t.Errorf("HEAD hit body %q, want empty", fill.Body.String())
		}
		if fill.Header().Get("Content-Length") != "5" {
			t.Errorf("HEAD hit Content-Length %q, want 5", fill.Header().Get("Content-Length"))
		}
	})
	t.Run("Date added when absent", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		rec := rg.get("/d", nil)
		if got := rec.Header().Get("Date"); got != start.Format(http.TimeFormat) {
			t.Errorf("Date %q, want %q", got, start.Format(http.TimeFormat))
		}
	})
	t.Run("tag header stripped and recorded", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Surrogate-Key": {"a b"}}, "x")
		rec := rg.get("/t", nil)
		if rec.Header().Get("Surrogate-Key") != "" {
			t.Errorf("client saw tag header %q", rec.Header().Get("Surrogate-Key"))
		}
		puts := rg.spy.callsOf(opPut)
		if len(puts) != 1 || strings.Join(puts[0].Tags, ",") != "a,b" {
			t.Fatalf("puts %+v, want one with tags a,b", puts)
		}
		hitRec := rg.get("/t", nil)
		if !hit(t, hitRec) || hitRec.Header().Get("Surrogate-Key") != "" {
			t.Errorf("hit hit=%v tag=%q", hit(t, hitRec), hitRec.Header().Get("Surrogate-Key"))
		}
	})
	t.Run("tag header kept without strip", func(t *testing.T) {
		rg := newRig(t, rigOpts{global: "strip_tag_header: false"})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Surrogate-Key": {"a"}}, "x")
		rec := rg.get("/t", nil)
		if rec.Header().Get("Surrogate-Key") != "a" {
			t.Errorf("client tag header %q, want a", rec.Header().Get("Surrogate-Key"))
		}
	})
	t.Run("over-limit tags are not stored", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		tags := make([]string, 65)
		for i := range tags {
			tags[i] = fmt.Sprintf("t%d", i)
		}
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Surrogate-Key": {strings.Join(tags, " ")}}, "x")
		rg.get("/t", nil)
		if n := len(rg.spy.callsOf(opPut)); n != 0 {
			t.Errorf("put called %d times, want 0", n)
		}
		if rg.requests(resultHit) != 0 {
			t.Errorf("hit counted")
		}
	})
	t.Run("origin Cache-Status first, stage member appended", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Cache-Status": {"origin"}}, "x")
		rec := rg.get("/c", nil)
		members := cacheStatus(t, rec)
		if len(members) != 2 || members[0].Name != "origin" || members[1].Name != "microgateway" {
			t.Fatalf("members %+v, want origin then microgateway", members)
		}
	})
	t.Run("trailer prevents storage", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Trailer": {"X-Sum"}}, "x")
		rg.get("/tr", nil)
		if n := len(rg.spy.callsOf(opPut)); n != 0 {
			t.Errorf("put called %d times, want 0", n)
		}
	})
	t.Run("status key is a digest without the query", func(t *testing.T) {
		rg := newRig(t, rigOpts{global: "status_key: true"})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		rec := rg.get("/k?token=secret", nil)
		st := lastStatus(t, rec)
		if len(st.Key) != 64 || strings.Contains(st.Key, "secret") || strings.Contains(st.Key, "token") {
			t.Errorf("key %q", st.Key)
		}
		for _, c := range st.Key {
			if !strings.ContainsRune("0123456789abcdef", c) {
				t.Fatalf("key %q is not lower-case hex", st.Key)
			}
		}
	})
}

func TestUnsafeInvalidation(t *testing.T) {
	target := "http://example.com/x?q=1"
	for _, secure := range []bool{false, true} {
		scheme := "http"
		if secure {
			scheme = "https"
		}
		newReq := func(method string, ctx context.Context) *http.Request {
			req := httptest.NewRequest(method, target, nil)
			if secure {
				req.TLS = &tls.ConnectionState{}
			}
			if ctx != nil {
				req = req.WithContext(ctx)
			}
			return req
		}
		name := map[bool]string{false: "plain", true: "tls"}[secure]
		t.Run(name+" POST 201 removes GET and HEAD entries", func(t *testing.T) {
			rg := newRig(t, rigOpts{})
			rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
			do(rg.h, newReq(http.MethodGet, nil))
			do(rg.h, newReq(http.MethodHead, nil))
			if rg.orig.count() != 2 {
				t.Fatalf("fill calls %d, want 2", rg.orig.count())
			}
			rg.orig.script(http.StatusCreated, nil, "")
			do(rg.h, newReq(http.MethodPost, nil))
			purges := rg.spy.callsOf(opPurge)
			want, err := httpcache.URLKeys(scheme + "://example.com/x?q=1")
			if err != nil {
				t.Fatal(err)
			}
			if len(purges) != 1 || strings.Join(purges[0].Purge.Keys, "|") != strings.Join(want, "|") {
				t.Fatalf("purge %+v, want keys %v", purges, want)
			}
			if purges[0].Purge.Kind != store.PurgeKeys || purges[0].Purge.Soft || purges[0].Purge.Retain <= 0 {
				t.Errorf("purge kind/soft/retain %+v", purges[0].Purge)
			}
			rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
			if hit(t, do(rg.h, newReq(http.MethodGet, nil))) {
				t.Error("GET hit after unsafe invalidation")
			}
			if hit(t, do(rg.h, newReq(http.MethodHead, nil))) {
				t.Error("HEAD hit after unsafe invalidation")
			}
		})
	}
	t.Run("POST 404 and 500 purge nothing", func(t *testing.T) {
		for _, code := range []int{http.StatusNotFound, http.StatusInternalServerError} {
			rg := newRig(t, rigOpts{})
			rg.orig.script(code, nil, "")
			rec := rg.serve(http.MethodPost, "/x", nil)
			if rec.Code != code {
				t.Fatalf("status %d, want %d", rec.Code, code)
			}
			if n := len(rg.spy.callsOf(opPurge)); n != 0 {
				t.Errorf("status %d purged %d times", code, n)
			}
		}
	})
	t.Run("purge failure keeps the 201 response", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.spy.setErr(opPurge, errors.New("purge refused"))
		rg.orig.script(http.StatusCreated, nil, "")
		rec := rg.serve(http.MethodPost, "/x", nil)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status %d, want 201", rec.Code)
		}
		if rg.storeErrs(opPurge) != 1 {
			t.Errorf("purge store errors %v, want 1", rg.storeErrs(opPurge))
		}
	})
	t.Run("client cancellation still purges", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		rg.get("/x?q=1", nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		rg.orig.script(http.StatusCreated, nil, "")
		do(rg.h, newReq2("POST", "/x?q=1", ctx))
		if n := len(rg.spy.callsOf(opPurge)); n != 1 {
			t.Fatalf("purge calls %d, want 1", n)
		}
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		if hit(t, rg.get("/x?q=1", nil)) {
			t.Error("entry survived a purge issued after client cancellation")
		}
	})
}

func newReq2(method, target string, ctx context.Context) *http.Request {
	return httptest.NewRequest(method, "http://example.com"+target, nil).WithContext(ctx)
}

func TestKeyIgnoresForwardedHeaders(t *testing.T) {
	rg := newRig(t, rigOpts{})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
	rg.get("/k", nil)
	forwarded := http.Header{
		"X-Forwarded-Proto": {"https"},
		"Forwarded":         {"proto=https;host=evil.example"},
		"X-Forwarded-Host":  {"evil.example"},
	}
	if !hit(t, rg.get("/k", forwarded)) {
		t.Fatal("forwarded headers changed the key")
	}
	req := httptest.NewRequest(http.MethodGet, "http://example.com/k", nil)
	req.TLS = &tls.ConnectionState{}
	if hit(t, do(rg.h, req)) {
		t.Fatal("TLS request shares the plain key")
	}
}

func TestOneRequestCount(t *testing.T) {
	rg := newRig(t, rigOpts{})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
	n := 0
	run := func(method, path string, hdr http.Header) {
		rg.serve(method, path, hdr)
		n++
	}
	run(http.MethodGet, "/a", nil)
	run(http.MethodGet, "/a", nil)
	run(http.MethodPost, "/a", nil)
	run(http.MethodOptions, "/a", nil)
	run(http.MethodGet, "/a", http.Header{"Range": {"bytes=0-1"}})
	run(http.MethodGet, "/b", http.Header{"Cache-Control": {"only-if-cached"}})
	run(http.MethodGet, "/"+strings.Repeat("p", 9000), nil)
	run(http.MethodGet, "/a", http.Header{"Connection": {"Upgrade"}, "Upgrade": {"websocket"}})
	var total float64
	for _, res := range []string{resultHit, resultStale, resultRevalidated, resultMiss, resultBypass} {
		total += rg.requests(res)
	}
	if total != float64(n) {
		t.Fatalf("counted %v requests, served %d", total, n)
	}
}
