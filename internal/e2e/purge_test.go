package e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/leakcheck"
	"github.com/ok0/go-microgateway/internal/teststore"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

const purgeNoCacheRoute = "  - name: r\n    match:\n      path: /\n    upstream: up\n"

type failIfRead struct{ t *testing.T }

func (r failIfRead) Read(p []byte) (int, error) {
	r.t.Errorf("request body was read before authentication")
	return 0, io.EOF
}

func purgeGetAdmin(g *microgateway.Gateway, method string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(rec, httptest.NewRequest(method, "/cache/purge", nil))
	return rec
}

func purgeZero() []float64 { return make([]float64, len(purgeSeriesLabels)) }

func purgeTaggedOrigin(w http.ResponseWriter, r *http.Request) {
	seg := strings.Split(r.URL.Path, "/")[1]
	writeOrigin(w, http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Surrogate-Key": {"tg" + strings.TrimPrefix(seg, "k")}}, "body")
}

func TestPurgeEndpointAuth(t *testing.T) {
	t.Parallel()
	g, _, origin, shared := purgeSharedFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	origin.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "body")
	if st := purgeLast(t, purgeGet(t, g, "http://h/p", nil)); st.Hit || !st.Stored {
		t.Fatalf("prime %+v, want stored", st)
	}

	for _, auth := range []string{"", "Basic " + purgeTokA, "Bearer ", "Bearer"} {
		req := httptest.NewRequest(http.MethodPost, "/cache/purge", failIfRead{t})
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		g.AdminHandler().ServeHTTP(rec, req)
		purgeWantErr(t, rec, http.StatusUnauthorized, httperr.CodeUnauthorized)
		if got := rec.Header().Get("WWW-Authenticate"); got != "Bearer" {
			t.Fatalf("auth %q: WWW-Authenticate = %q, want Bearer", auth, got)
		}
	}

	wrong := httptest.NewRequest(http.MethodPost, "/cache/purge", failIfRead{t})
	wrong.Header.Set("Authorization", "Bearer pt-Z-wrong")
	wrongRec := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(wrongRec, wrong)
	purgeWantErr(t, wrongRec, http.StatusForbidden, httperr.CodeForbidden)

	if n := len(purgeCallsOf(shared)); n != 0 {
		t.Fatalf("store purge calls = %d, want 0", n)
	}
	if got := purgeCounts(t, g); !slices.Equal(got, purgeZero()) {
		t.Fatalf("purge counters = %v, want all 0", got)
	}
	if st := purgeLast(t, purgeGet(t, g, "http://h/p", nil)); !st.Hit {
		t.Fatalf("entry after rejections %+v, want hit", st)
	}

	noKeys, _, _ := purgeFixture(t, "", cacheRouteYAML("r"))
	purgeWantErr(t, purgeDo(t, noKeys, purgeTokA, `{"all":true}`), http.StatusNotFound, httperr.CodeNotFound)
	purgeWantErr(t, purgeGetAdmin(noKeys, http.MethodPost), http.StatusNotFound, httperr.CodeNotFound)
	purgeWantErr(t, purgeGetAdmin(noKeys, http.MethodGet), http.StatusNotFound, httperr.CodeNotFound)

	nc, _, _ := purgeFixture(t, purgeAdminYAML(purgeTokA), purgeNoCacheRoute)
	purgeWantErr(t, purgeDo(t, nc, purgeTokA, `{"all":true}`), http.StatusNotFound, httperr.CodeCacheNotConfigured)
	purgeWantErr(t, purgeDo(t, nc, "pt-Z-wrong", `{"all":true}`), http.StatusForbidden, httperr.CodeForbidden)

	getRec := purgeGetAdmin(g, http.MethodGet)
	purgeWantErr(t, getRec, http.StatusMethodNotAllowed, httperr.CodeMethodNotAllowed)
	if got := getRec.Header().Get("Allow"); got != "POST" {
		t.Fatalf("Allow = %q, want POST", got)
	}

	logs := &logBuf{}
	two, _, _ := purgeFixture(t, purgeAdminYAML(purgeTokA, purgeTokB), cacheRouteYAML("r"), microgateway.WithLogger(newJSONLogger(logs)))
	purgeOK(t, purgeDo(t, two, purgeTokA, `{"all":true}`))
	purgeOK(t, purgeDo(t, two, purgeTokB, `{"all":true}`))
	recs := logs.records(t, "cache purged")
	if len(recs) != 2 {
		t.Fatalf("cache purged records = %d, want 2", len(recs))
	}
	if recs[0]["key_id"] != "key-0" || recs[1]["key_id"] != "key-1" {
		t.Fatalf("key_id values = %v, %v, want key-0 and key-1", recs[0]["key_id"], recs[1]["key_id"])
	}
}

func TestPurgeKeyRotation(t *testing.T) {
	t.Parallel()
	g, _, _, be := purgeFixtureOn(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	purgeOK(t, purgeDo(t, g, purgeTokA, `{"all":true}`))

	if err := g.ReloadYAML([]byte(cacheGatewayDoc(be, purgeAdminYAML(purgeTokB), cacheRouteYAML("r")))); err != nil {
		t.Fatalf("reload to key B: %v", err)
	}
	purgeWantErr(t, purgeDo(t, g, purgeTokA, `{"all":true}`), http.StatusForbidden, httperr.CodeForbidden)
	purgeOK(t, purgeDo(t, g, purgeTokB, `{"all":true}`))

	dup := "admin:\n  auth:\n    keys:\n" +
		"      - id: dup\n        hash: \"" + auHash(purgeTokA) + "\"\n" +
		"      - id: dup\n        hash: \"" + auHash(purgeTokB) + "\"\n"
	err := g.ReloadYAML([]byte(cacheGatewayDoc(be, dup, cacheRouteYAML("r"))))
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("duplicate key reload error = %T (%v), want *config.ValidationError", err, err)
	}
	purgeWantErr(t, purgeDo(t, g, purgeTokA, `{"all":true}`), http.StatusForbidden, httperr.CodeForbidden)
	purgeOK(t, purgeDo(t, g, purgeTokB, `{"all":true}`))

	if err := g.ReloadYAML([]byte(cacheGatewayDoc(be, "", cacheRouteYAML("r")))); err != nil {
		t.Fatalf("reload without keys: %v", err)
	}
	purgeWantErr(t, purgeDo(t, g, purgeTokA, `{"all":true}`), http.StatusNotFound, httperr.CodeNotFound)
	purgeWantErr(t, purgeDo(t, g, purgeTokB, `{"all":true}`), http.StatusNotFound, httperr.CodeNotFound)
}

func TestPurgeNotOnDataPath(t *testing.T) {
	t.Parallel()
	const noMatch = "  - name: r\n    match:\n      path: /x\n    upstream: up\n    cache:\n      enabled: true\n"
	post := func(g *microgateway.Gateway) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:9901/cache/purge", strings.NewReader(`{"all":true}`))
		req.Header.Set("Authorization", "Bearer "+purgeTokA)
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, req)
		return rec
	}

	g1, _, origin1, shared1 := purgeSharedFixture(t, purgeAdminYAML(purgeTokA), noMatch)
	origin1.set(http.StatusConflict, nil, "")
	purgeWantErr(t, post(g1), http.StatusNotFound, httperr.CodeRouteNotFound)
	if calls, _, _ := origin1.counts(); calls != 0 {
		t.Fatalf("origin requests without a route = %d, want 0", calls)
	}
	if n := len(purgeCallsOf(shared1)); n != 0 {
		t.Fatalf("store purge calls without a route = %d, want 0", n)
	}

	g2, _, origin2, shared2 := purgeSharedFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	origin2.set(http.StatusConflict, nil, "")
	if rec := post(g2); rec.Code != http.StatusConflict {
		t.Fatalf("route / status = %d, want the origin's 409", rec.Code)
	}
	if calls, _, _ := origin2.counts(); calls != 1 {
		t.Fatalf("origin requests on route / = %d, want 1", calls)
	}
	if n := len(purgeCallsOf(shared2)); n != 0 {
		t.Fatalf("store purge calls on route / = %d, want 0", n)
	}
	if got := purgeCounts(t, g2); !slices.Equal(got, purgeZero()) {
		t.Fatalf("purge counters on route / = %v, want all 0", got)
	}

	g3, _, origin3, shared3 := purgeSharedFixture(t, purgeAdminYAML(purgeTokA), strings.Replace(cacheRouteYAML("r"), "path: /\n", "path: /cache/\n", 1))
	origin3.set(http.StatusConflict, nil, "")
	if rec := post(g3); rec.Code != http.StatusConflict {
		t.Fatalf("route /cache/ status = %d, want the origin's 409", rec.Code)
	}
	if calls, _, _ := origin3.counts(); calls != 1 {
		t.Fatalf("origin requests on route /cache/ = %d, want 1", calls)
	}
	if n := len(purgeCallsOf(shared3)); n != 0 {
		t.Fatalf("store purge calls on route /cache/ = %d, want 0", n)
	}
}

func TestPurgeBodyValidation(t *testing.T) {
	t.Parallel()
	g, _, origin, shared := purgeSharedFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	origin.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "body")
	if st := purgeLast(t, purgeGet(t, g, "http://h/p", nil)); st.Hit || !st.Stored {
		t.Fatalf("prime %+v, want stored", st)
	}

	bad := []string{
		`{"urls":`,
		`{"all":true} x`,
		``,
		`{"all":true,"x":1}`,
		`{"urls":"http://h/p"}`,
		`{"tags":[1]}`,
		`{"all":"yes"}`,
		`{"all":true,"soft":"yes"}`,
		`{"soft":true}`,
		`{"all":false}`,
		`{"urls":["http://h/p"],"all":true}`,
		`{"urls":[]}`,
		`{"urls":` + jsonURLs(numbered("http://h/u", 1001)...) + `}`,
		strings.Repeat(" ", 1<<20+1),
		`{"tags":[""]}`,
		`{"tags":["` + strings.Repeat("t", 1025) + `"]}`,
		`{"tags":["a\u0001b"]}`,
		`{"urls":["/rel"]}`,
		`{"urls":["ftp://h/"]}`,
		`{"urls":["http:///p"]}`,
		`{"urls":["http://h/` + strings.Repeat("p", 9000) + `"]}`,
	}
	for _, body := range bad {
		purgeWantErr(t, purgeDo(t, g, purgeTokA, body), http.StatusBadRequest, httperr.CodeBadRequest)
		if st := purgeLast(t, purgeGet(t, g, "http://h/p", nil)); !st.Hit {
			t.Fatalf("entry after a rejected body %.30q: %+v, want hit", body, st)
		}
	}
	if got := purgeCounts(t, g); !slices.Equal(got, purgeZero()) {
		t.Fatalf("purge counters after rejected bodies = %v, want all 0", got)
	}
	if n := len(purgeCallsOf(shared)); n != 0 {
		t.Fatalf("store purge calls after rejected bodies = %d, want 0", n)
	}

	want := []purgeReply{
		{Type: "url", Soft: false, Accepted: 1000, Affected: intp(0), Scope: "store"},
		{Type: "tag", Soft: false, Accepted: 1, Affected: intp(0), Scope: "store"},
		{Type: "all", Soft: false, Accepted: 1, Affected: intp(1), Scope: "store"},
	}
	bodies := []string{
		`{"urls":` + jsonURLs(numbered("http://h/u", 1000)...) + `}`,
		`{"tags":["` + strings.Repeat("t", 1024) + `"]}`,
		`{"all":true}` + strings.Repeat(" ", 1<<20-len(`{"all":true}`)),
	}
	for i, body := range bodies {
		if rep := purgeOK(t, purgeDo(t, g, purgeTokA, body)); !reflect.DeepEqual(rep, want[i]) {
			t.Fatalf("valid boundary %d reply = %+v, want %+v", i, rep, want[i])
		}
	}
}

func TestPurgeHard(t *testing.T) {
	t.Parallel()
	g, _, origin := purgeFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	origin.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Vary": {"Accept-Language"}}, "body")
	langs := []string{"en", "fr", "de"}
	for _, lang := range langs {
		if st := purgeLast(t, purgeGet(t, g, "http://h/p", http.Header{"Accept-Language": {lang}})); st.Hit || !st.Stored {
			t.Fatalf("fill %s: %+v, want stored", lang, st)
		}
	}
	if st := purgeLast(t, purgeReq(t, g, http.MethodHead, "http://h/p", nil)); st.Hit || !st.Stored {
		t.Fatalf("HEAD fill %+v, want stored", st)
	}
	if calls, _, _ := origin.counts(); calls != 4 {
		t.Fatalf("origin requests after fills = %d, want 4", calls)
	}
	for _, lang := range langs {
		if st := purgeLast(t, purgeGet(t, g, "http://h/p", http.Header{"Accept-Language": {lang}})); !st.Hit {
			t.Fatalf("variant %s %+v, want hit", lang, st)
		}
	}
	if entries := purgeEntries(t, g); entries != 4 {
		t.Fatalf("entries before purge = %v, want 4", entries)
	}

	rep := purgeOK(t, purgeDo(t, g, purgeTokA, `{"urls":["http://h/p"]}`))
	if want := (purgeReply{Type: "url", Soft: false, Accepted: 1, Affected: intp(4), Scope: "instance"}); !reflect.DeepEqual(rep, want) {
		t.Fatalf("URL purge reply = %+v, want %+v", rep, want)
	}
	if entries := purgeEntries(t, g); entries != 0 {
		t.Fatalf("entries after purge = %v, want 0", entries)
	}
	for i, lang := range langs {
		want := httpcache.FwdURIMiss
		if i > 0 {
			want = httpcache.FwdVaryMiss
		}
		st := purgeLast(t, purgeGet(t, g, "http://h/p", http.Header{"Accept-Language": {lang}}))
		if st.Hit || st.Fwd != want {
			t.Fatalf("variant %s after purge %+v, want miss with fwd %s", lang, st, want)
		}
	}
	if st := purgeLast(t, purgeReq(t, g, http.MethodHead, "http://h/p", nil)); st.Hit || st.Fwd != httpcache.FwdURIMiss {
		t.Fatalf("HEAD after purge %+v, want uri-miss", st)
	}
	if calls, cond, _ := origin.counts(); calls != 8 || cond != 0 {
		t.Fatalf("origin requests = %d (conditional %d), want 8 and 0", calls, cond)
	}

	g2, _, origin2 := purgeFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	origin2.setBegin(func(w http.ResponseWriter, r *http.Request) {
		h := http.Header{"Cache-Control": {"max-age=60"}}
		switch r.URL.Path {
		case "/s1":
			h.Set("Surrogate-Key", "a b c")
		case "/s2":
			h.Set("Surrogate-Key", "c d")
		}
		writeOrigin(w, http.StatusOK, h, "body")
	})
	for _, p := range []string{"/s1", "/s2"} {
		if st := purgeLast(t, purgeGet(t, g2, "http://h"+p, nil)); st.Hit || !st.Stored {
			t.Fatalf("fill %s: %+v, want stored", p, st)
		}
	}
	tagRows := []struct {
		tag      string
		affected int
		gone     []string
		kept     []string
	}{
		{"a", 1, []string{"/s1"}, []string{"/s2"}},
		{"c", 2, []string{"/s1", "/s2"}, nil},
		{"d", 1, []string{"/s2"}, []string{"/s1"}},
	}
	for _, row := range tagRows {
		for _, p := range append(slices.Clone(row.gone), row.kept...) {
			if st := purgeLast(t, purgeGet(t, g2, "http://h"+p, nil)); !st.Hit && !st.Stored {
				t.Fatalf("ensure %s: %+v, want hit or stored", p, st)
			}
		}
		rep := purgeOK(t, purgeDo(t, g2, purgeTokA, `{"tags":["`+row.tag+`"]}`))
		if want := (purgeReply{Type: "tag", Soft: false, Accepted: 1, Affected: intp(row.affected), Scope: "instance"}); !reflect.DeepEqual(rep, want) {
			t.Fatalf("tag %s reply = %+v, want %+v", row.tag, rep, want)
		}
		for _, p := range row.gone {
			if st := purgeLast(t, purgeGet(t, g2, "http://h"+p, nil)); st.Hit {
				t.Fatalf("tag %s: %s %+v, want miss", row.tag, p, st)
			}
		}
		for _, p := range row.kept {
			if st := purgeLast(t, purgeGet(t, g2, "http://h"+p, nil)); !st.Hit {
				t.Fatalf("tag %s: %s %+v, want hit", row.tag, p, st)
			}
		}
	}
	if calls, _, _ := origin2.counts(); calls != 6 {
		t.Fatalf("surrogate origin requests = %d, want 6", calls)
	}

	g3, _, origin3 := purgeFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	origin3.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "body")
	for _, p := range []string{"/a", "/b", "/c"} {
		if st := purgeLast(t, purgeGet(t, g3, "http://h"+p, nil)); !st.Stored {
			t.Fatalf("fill %s: %+v, want stored", p, st)
		}
	}
	if entries := purgeEntries(t, g3); entries != 3 {
		t.Fatalf("entries before tag and URL purges = %v, want 3", entries)
	}
	if rep := purgeOK(t, purgeDo(t, g3, purgeTokA, `{"tags":["none"]}`)); !reflect.DeepEqual(rep, purgeReply{Type: "tag", Accepted: 1, Affected: intp(0), Scope: "instance"}) {
		t.Fatalf("absent tag reply = %+v", rep)
	}
	if rep := purgeOK(t, purgeDo(t, g3, purgeTokA, `{"urls":["http://h/absent"]}`)); !reflect.DeepEqual(rep, purgeReply{Type: "url", Accepted: 1, Affected: intp(0), Scope: "instance"}) {
		t.Fatalf("absent URL reply = %+v", rep)
	}
	if rep := purgeOK(t, purgeDo(t, g3, purgeTokA, `{"urls":["http://h/a"]}`)); !reflect.DeepEqual(rep, purgeReply{Type: "url", Accepted: 1, Affected: intp(1), Scope: "instance"}) {
		t.Fatalf("URL /a reply = %+v", rep)
	}
	for _, p := range []string{"/b", "/c"} {
		if st := purgeLast(t, purgeGet(t, g3, "http://h"+p, nil)); !st.Hit {
			t.Fatalf("unrelated %s %+v, want hit", p, st)
		}
	}
	if rep := purgeOK(t, purgeDo(t, g3, purgeTokA, `{"all":true}`)); !reflect.DeepEqual(rep, purgeReply{Type: "all", Accepted: 1, Affected: intp(2), Scope: "instance"}) {
		t.Fatalf("all reply = %+v", rep)
	}
	if entries := purgeEntries(t, g3); entries != 0 {
		t.Fatalf("entries after all = %v, want 0", entries)
	}
	for _, p := range []string{"/b", "/c"} {
		if st := purgeLast(t, purgeGet(t, g3, "http://h"+p, nil)); st.Hit || st.Fwd != httpcache.FwdURIMiss {
			t.Fatalf("%s after all %+v, want uri-miss", p, st)
		}
	}
}

func TestPurgeScheme(t *testing.T) {
	t.Parallel()
	g, _, origin := purgeFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	origin.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "body")
	purgeOne := func(body string) purgeReply {
		return purgeOK(t, purgeDo(t, g, purgeTokA, body))
	}
	wantReply := func(got purgeReply, affected int) {
		t.Helper()
		if want := (purgeReply{Type: "url", Soft: false, Accepted: 1, Affected: intp(affected), Scope: "instance"}); !reflect.DeepEqual(got, want) {
			t.Fatalf("reply = %+v, want %+v", got, want)
		}
	}
	hit := func(u string, hdr http.Header, want bool) {
		t.Helper()
		if st := purgeLast(t, purgeGet(t, g, u, hdr)); st.Hit != want {
			t.Fatalf("%s hit = %v, want %v (%+v)", u, st.Hit, want, st)
		}
	}

	if st := purgeLast(t, purgeGet(t, g, "http://h/p", http.Header{"X-Forwarded-Proto": {"https"}})); !st.Stored {
		t.Fatalf("plain fill with X-Forwarded-Proto %+v, want stored", st)
	}
	wantReply(purgeOne(`{"urls":["https://h/p"]}`), 0)
	hit("http://h/p", nil, true)
	wantReply(purgeOne(`{"urls":["http://h/p"]}`), 1)
	hit("http://h/p", nil, false)

	if st := purgeLast(t, purgeGet(t, g, "https://h/t", nil)); !st.Stored {
		t.Fatalf("TLS fill %+v, want stored", st)
	}
	wantReply(purgeOne(`{"urls":["http://h/t"]}`), 0)
	hit("https://h/t", nil, true)
	wantReply(purgeOne(`{"urls":["https://h/t"]}`), 1)
	hit("https://h/t", nil, false)

	for _, u := range []string{"http://api.example.com/p?x=1", "http://api.example.com/p?x=2", "http://api.example.com/p?b=2&a=1"} {
		if st := purgeLast(t, purgeGet(t, g, u, nil)); !st.Stored {
			t.Fatalf("fill %s %+v, want stored", u, st)
		}
	}
	wantReply(purgeOne(`{"urls":["HTTP://API.Example.com:80/p?x=1"]}`), 1)
	hit("http://api.example.com/p?x=1", nil, false)
	hit("http://api.example.com/p?x=2", nil, true)
	hit("http://api.example.com/p?b=2&a=1", nil, true)
	wantReply(purgeOne(`{"urls":["http://api.example.com/p?a=1&b=2"]}`), 0)
	hit("http://api.example.com/p?b=2&a=1", nil, true)

	if st := purgeLast(t, purgeGet(t, g, "https://h/", nil)); !st.Stored {
		t.Fatalf("TLS root fill %+v, want stored", st)
	}
	wantReply(purgeOne(`{"urls":["https://h:443/"]}`), 1)
	hit("https://h/", nil, false)

	if rep := purgeOne(`{"urls":["HTTP://API.Example.com:80/p?x=1","http://api.example.com/p?x=1"]}`); !reflect.DeepEqual(rep, purgeReply{Type: "url", Accepted: 1, Affected: intp(1), Scope: "instance"}) {
		t.Fatalf("equal URLs reply = %+v, want accepted 1 and affected 1 (the miss above refilled ?x=1)", rep)
	}
}

func TestPurgeResponses(t *testing.T) {
	t.Parallel()
	const bodyFmt = `{"type":"%s","soft":%t,"accepted":%d,"affected":%d,"scope":"%s"}`
	exact := func(typ string, soft bool, accepted, affected int, scope string) string {
		return fmt.Sprintf(bodyFmt, typ, soft, accepted, affected, scope)
	}
	urls3 := `["http://h/r1","http://h/r2","http://h/r3"]`
	cases := []struct {
		name string
		body string
		want string
	}{
		{"url hard", `{"urls":` + urls3 + `}`, exact("url", false, 3, 3, "instance")},
		{"url soft", `{"urls":` + urls3 + `,"soft":true}`, exact("url", true, 3, 3, "instance")},
		{"tag hard", `{"tags":["t1","t2","t3"]}`, exact("tag", false, 3, 3, "instance")},
		{"tag soft", `{"tags":["t1","t2","t3"],"soft":true}`, exact("tag", true, 3, 3, "instance")},
		{"all hard", `{"all":true}`, exact("all", false, 1, 3, "instance")},
		{"all soft", `{"all":true,"soft":true}`, exact("all", true, 1, 3, "instance")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, _, origin := purgeFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
			origin.setBegin(func(w http.ResponseWriter, r *http.Request) {
				writeOrigin(w, http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Surrogate-Key": {"t" + strings.TrimPrefix(r.URL.Path, "/r")}}, "body")
			})
			for _, p := range []string{"/r1", "/r2", "/r3"} {
				if st := purgeLast(t, purgeGet(t, g, "http://h"+p, nil)); !st.Stored {
					t.Fatalf("fill %s %+v, want stored", p, st)
				}
			}
			rec := purgeDo(t, g, purgeTokA, tc.body)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
			}
			if got := strings.TrimSpace(rec.Body.String()); got != tc.want {
				t.Fatalf("body = %s, want %s", got, tc.want)
			}
		})
	}

	g, _, origin, shared := purgeSharedFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	origin.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "body")
	fill := func(p string) {
		t.Helper()
		if st := purgeLast(t, purgeGet(t, g, "http://h"+p, nil)); !st.Stored {
			t.Fatalf("fill %s %+v, want stored", p, st)
		}
	}
	fill("/n1")
	rec := purgeDo(t, g, purgeTokA, `{"urls":["http://h/n1"]}`)
	if got := strings.TrimSpace(rec.Body.String()); got != exact("url", false, 1, 1, "store") {
		t.Fatalf("named store body = %s, want %s", got, exact("url", false, 1, 1, "store"))
	}

	shared.SetAffectedUnknown(true)
	fill("/n2")
	rec = purgeDo(t, g, purgeTokA, `{"urls":["http://h/n2"]}`)
	if got := strings.TrimSpace(rec.Body.String()); got != `{"type":"url","soft":false,"accepted":1,"scope":"store"}` {
		t.Fatalf("unknown affected body = %s", got)
	}
	shared.SetAffectedUnknown(false)

	fill("/m0")
	fill("/m1")
	fill("/m2")
	fill("/m3")
	fill("/m4")
	many := `{"urls":` + jsonURLs(numbered("http://h/m", 1000)...) + `}`
	before := len(purgeCallsOf(shared))
	if rep := purgeOK(t, purgeDo(t, g, purgeTokA, many)); !reflect.DeepEqual(rep, purgeReply{Type: "url", Accepted: 1000, Affected: intp(5), Scope: "store"}) {
		t.Fatalf("1000 URL reply = %+v", rep)
	}
	batches := purgeCallsOf(shared)[before:]
	if len(batches) != 2 {
		t.Fatalf("1000 URLs gave %d store calls, want 2", len(batches))
	}
	for _, c := range batches {
		if len(c.Purge.Keys) > store.MaxPurgeSelectors {
			t.Fatalf("store call with %d keys, want at most %d", len(c.Purge.Keys), store.MaxPurgeSelectors)
		}
	}

	fill("/m0")
	fill("/m999")
	shared.SetPurgeFailAfter(1, fmt.Errorf("batch down: %w", store.ErrUnavailable))
	rec = purgeDo(t, g, purgeTokA, many)
	purgeWantErr(t, rec, http.StatusServiceUnavailable, httperr.CodeStoreUnavailable)
	if st := purgeLast(t, purgeGet(t, g, "http://h/m999", nil)); !st.Hit {
		t.Fatalf("second-batch URL %+v, want hit", st)
	}
	if st := purgeLast(t, purgeGet(t, g, "http://h/m0", nil)); st.Hit {
		t.Fatalf("first-batch URL %+v, want miss", st)
	}

	shared.SetPurgeFailAfter(-1, nil)
	if rep := purgeOK(t, purgeDo(t, g, purgeTokA, many)); !reflect.DeepEqual(rep, purgeReply{Type: "url", Accepted: 1000, Affected: intp(2), Scope: "store"}) {
		t.Fatalf("repeated 1000 URL reply = %+v", rep)
	}
}

func TestPurgeStoreFailure(t *testing.T) {
	t.Parallel()
	g, fake, origin, shared := purgeSharedFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	origin.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "body")
	if st := purgeLast(t, purgeGet(t, g, "http://h/p", nil)); !st.Stored {
		t.Fatalf("prime %+v, want stored", st)
	}
	counts := func() (float64, float64) {
		return rsMetricOr0(t, g, "microgateway_cache_store_errors_total", "op", "purge"),
			rsMetricOr0(t, g, "microgateway_store_errors_total", "store", "sc", "op", "purge")
	}
	wantCounts := func(c, s float64) {
		t.Helper()
		if gc, gs := counts(); gc != c || gs != s {
			t.Fatalf("store error counters = (%v, %v), want (%v, %v)", gc, gs, c, s)
		}
	}
	failErr := fmt.Errorf("purge down: %w", store.ErrUnavailable)

	shared.SetOpMode("purge", teststore.Fail, failErr)
	purgeWantErr(t, purgeDo(t, g, purgeTokA, `{"all":true}`), http.StatusServiceUnavailable, httperr.CodeStoreUnavailable)
	wantCounts(1, 1)
	if got := purgeCounts(t, g); !slices.Equal(got, purgeZero()) {
		t.Fatalf("purge counters after failure = %v, want all 0", got)
	}

	shared.SetOpMode("purge", teststore.Hang, nil)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- purgeDo(t, g, purgeTokA, `{"all":true}`) }()
	awaitDue(t, fake, 1, 100*time.Millisecond, "store timeout timer", nil)
	fake.Advance(100*time.Millisecond - time.Nanosecond)
	select {
	case rec := <-done:
		t.Fatalf("purge answered %d before the store timeout", rec.Code)
	default:
	}
	fake.Advance(time.Nanosecond)
	rec, _ := testwait.Recv(t, "purge after store timeout", done, nil)
	purgeWantErr(t, rec, http.StatusServiceUnavailable, httperr.CodeStoreUnavailable)
	wantCounts(2, 2)

	shared.SetOpMode("purge", teststore.Fail, failErr)
	purgeWantErr(t, purgeDo(t, g, purgeTokA, `{"all":true}`), http.StatusServiceUnavailable, httperr.CodeStoreUnavailable)
	wantCounts(3, 3)
	calls := len(purgeCallsOf(shared))

	shared.SetOpMode("purge", teststore.Inherit, nil)
	purgeWantErr(t, purgeDo(t, g, purgeTokA, `{"all":true}`), http.StatusServiceUnavailable, httperr.CodeStoreUnavailable)
	if n := len(purgeCallsOf(shared)); n != calls {
		t.Fatalf("store purge calls with the guard open = %d, want %d", n, calls)
	}
	wantCounts(3, 3)
	if got := purgeCounts(t, g); !slices.Equal(got, purgeZero()) {
		t.Fatalf("purge counters with the guard open = %v, want all 0", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "/cache/purge", strings.NewReader(`{"all":true}`)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+purgeTokA)
	canceled := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(canceled, req)
	if canceled.Body.Len() != 0 || canceled.Header().Get("Content-Type") != "" {
		t.Fatalf("canceled request got header %v and body %q, want no response", canceled.Header(), canceled.Body.String())
	}
	if n := len(purgeCallsOf(shared)); n != calls {
		t.Fatalf("store purge calls after a canceled request = %d, want %d", n, calls)
	}
	wantCounts(3, 3)
}

func TestPurgeLibrary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	libLogs := &logBuf{}
	lib, _, libOrigin := purgeFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"), microgateway.WithLogger(newJSONLogger(libLogs)))
	ep, _, epOrigin := purgeFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	libOrigin.setBegin(purgeTaggedOrigin)
	epOrigin.setBegin(purgeTaggedOrigin)

	cases := []struct {
		name string
		req  microgateway.PurgeRequest
		body string
		urls []string
	}{
		{"url hard", microgateway.PurgeRequest{URLs: []string{"http://h/k0/1", "http://h/k0/2"}}, `{"urls":["http://h/k0/1","http://h/k0/2"]}`, []string{"http://h/k0/1", "http://h/k0/2"}},
		{"url soft", microgateway.PurgeRequest{URLs: []string{"http://h/k1/1", "http://h/k1/2"}, Soft: true}, `{"urls":["http://h/k1/1","http://h/k1/2"],"soft":true}`, []string{"http://h/k1/1", "http://h/k1/2"}},
		{"tag hard", microgateway.PurgeRequest{Tags: []string{"tg2"}}, `{"tags":["tg2"]}`, []string{"http://h/k2/1", "http://h/k2/2"}},
		{"tag soft", microgateway.PurgeRequest{Tags: []string{"tg3"}, Soft: true}, `{"tags":["tg3"],"soft":true}`, []string{"http://h/k3/1", "http://h/k3/2"}},
		{"all hard", microgateway.PurgeRequest{All: true}, `{"all":true}`, []string{"http://h/k4/1"}},
		{"all soft", microgateway.PurgeRequest{All: true, Soft: true}, `{"all":true,"soft":true}`, []string{"http://h/k5/1"}},
	}
	for _, tc := range cases {
		for _, g := range []*microgateway.Gateway{lib, ep} {
			for _, u := range tc.urls {
				if st := purgeLast(t, purgeGet(t, g, u, nil)); !st.Stored {
					t.Fatalf("%s: fill %s %+v, want stored", tc.name, u, st)
				}
			}
		}
		res, err := lib.Purge(ctx, tc.req)
		if err != nil {
			t.Fatalf("%s: library purge: %v", tc.name, err)
		}
		rep := purgeOK(t, purgeDo(t, ep, purgeTokA, tc.body))
		if res.Type != rep.Type || res.Soft != rep.Soft || res.Accepted != rep.Accepted || res.Scope != rep.Scope {
			t.Fatalf("%s: library result %+v, endpoint reply %+v", tc.name, res, rep)
		}
		if res.AffectedKnown != (rep.Affected != nil) || (rep.Affected != nil && res.Affected != *rep.Affected) {
			t.Fatalf("%s: library affected %+v, endpoint affected %v", tc.name, res, rep.Affected)
		}
		if res.Scope != "instance" {
			t.Fatalf("%s: scope = %q, want instance", tc.name, res.Scope)
		}
	}

	if _, err := lib.Purge(ctx, microgateway.PurgeRequest{}); !errors.Is(err, microgateway.ErrInvalidPurgeRequest) {
		t.Fatalf("empty request error = %v, want ErrInvalidPurgeRequest", err)
	}
	if st := purgeLast(t, purgeGet(t, lib, "http://h/k9/1", nil)); !st.Stored {
		t.Fatalf("fill k9 %+v, want stored", st)
	}
	if _, err := lib.Purge(ctx, microgateway.PurgeRequest{URLs: []string{"http://h/k9/1"}, All: true}); !errors.Is(err, microgateway.ErrInvalidPurgeRequest) {
		t.Fatalf("two selectors error = %v, want ErrInvalidPurgeRequest", err)
	}
	if st := purgeLast(t, purgeGet(t, lib, "http://h/k9/1", nil)); !st.Hit {
		t.Fatalf("entry after a rejected request %+v, want hit", st)
	}
	if _, err := lib.Purge(ctx, microgateway.PurgeRequest{URLs: []string{"/rel"}}); !errors.Is(err, microgateway.ErrInvalidPurgeRequest) {
		t.Fatalf("relative URL error = %v, want ErrInvalidPurgeRequest", err)
	}

	nc, _, _ := purgeFixture(t, purgeAdminYAML(purgeTokA), purgeNoCacheRoute)
	if _, err := nc.Purge(ctx, microgateway.PurgeRequest{All: true}); !errors.Is(err, microgateway.ErrCacheDisabled) {
		t.Fatalf("no cache route error = %v, want ErrCacheDisabled", err)
	}

	fs, _, _, shared := purgeSharedFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	shared.SetOpMode("purge", teststore.Fail, fmt.Errorf("purge down: %w", store.ErrUnavailable))
	if _, err := fs.Purge(ctx, microgateway.PurgeRequest{All: true}); !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("failing store error = %v, want store.ErrUnavailable", err)
	}

	before := purgeCounts(t, lib)
	if _, err := lib.Purge(ctx, microgateway.PurgeRequest{URLs: []string{"http://h/k0/1"}}); err != nil {
		t.Fatalf("library purge: %v", err)
	}
	after := purgeCounts(t, lib)
	if after[0] != before[0]+1 {
		t.Fatalf("url hard counter = %v, want %v", after[0], before[0]+1)
	}
	recs := libLogs.records(t, "cache purged")
	last := recs[len(recs)-1]
	if last["key_id"] != "" || last["type"] != "url" || last["mode"] != "hard" {
		t.Fatalf("library purge log %v, want empty key_id, url, hard", last)
	}

	if err := lib.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := lib.Purge(ctx, microgateway.PurgeRequest{All: true}); !errors.Is(err, microgateway.ErrClosed) {
		t.Fatalf("purge after close error = %v, want ErrClosed", err)
	}
}

func TestPurgeMetrics(t *testing.T) {
	t.Parallel()
	g, _, origin, shared := purgeSharedFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	origin.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Surrogate-Key": {"tgx"}}, "body")
	if got := purgeCounts(t, g); !slices.Equal(got, purgeZero()) {
		t.Fatalf("counters before any purge = %v, want all 0", got)
	}
	fill := func(u string) {
		t.Helper()
		if st := purgeLast(t, purgeGet(t, g, u, nil)); !st.Stored {
			t.Fatalf("fill %s %+v, want stored", u, st)
		}
	}
	fill("http://h/a1")
	purgeOK(t, purgeDo(t, g, purgeTokA, `{"urls":["http://h/a1"]}`))
	fill("http://h/a2")
	purgeOK(t, purgeDo(t, g, purgeTokA, `{"urls":["http://h/a2"]}`))
	fill("http://h/b1")
	purgeOK(t, purgeDo(t, g, purgeTokA, `{"tags":["tgx"],"soft":true}`))
	fill("http://h/c1")
	purgeOK(t, purgeDo(t, g, purgeTokA, `{"all":true}`))
	fill("http://h/d1")
	purgeOK(t, purgeDo(t, g, purgeTokA, `{"all":true,"soft":true}`))
	purgeWantErr(t, purgeDo(t, g, purgeTokA, `{"urls":[]}`), http.StatusBadRequest, httperr.CodeBadRequest)
	purgeWantErr(t, purgeDo(t, g, "", `{"all":true}`), http.StatusUnauthorized, httperr.CodeUnauthorized)
	shared.SetOpMode("purge", teststore.Fail, fmt.Errorf("purge down: %w", store.ErrUnavailable))
	purgeWantErr(t, purgeDo(t, g, purgeTokA, `{"all":true}`), http.StatusServiceUnavailable, httperr.CodeStoreUnavailable)
	shared.SetOpMode("purge", teststore.Inherit, nil)
	purgeReq(t, g, http.MethodPost, "http://h/e1", nil)

	if got, want := purgeCounts(t, g), []float64{2, 0, 0, 1, 1, 1}; !slices.Equal(got, want) {
		t.Fatalf("purge counters = %v, want %v (url hard, url soft, tag hard, tag soft, all hard, all soft)", got, want)
	}
	rsCheckExposition(t, metricsText(t, g))
}

func TestPurgeLogs(t *testing.T) {
	t.Parallel()
	logs := &logBuf{}
	g, _, origin, shared := purgeSharedFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"), microgateway.WithLogger(newJSONLogger(logs)))
	origin.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Surrogate-Key": {"tagsecret-K3"}}, "body")
	if st := purgeLast(t, purgeGet(t, g, "http://h/p?qsecret=V0x9", nil)); !st.Stored {
		t.Fatalf("prime %+v, want stored", st)
	}

	var bodies []string
	record := func(rec *httptest.ResponseRecorder) {
		bodies = append(bodies, rec.Body.String())
	}
	ok := purgeDo(t, g, purgeTokA, `{"urls":["http://h/p?qsecret=V0x9"]}`)
	purgeOK(t, ok)
	record(ok)

	bad := purgeDo(t, g, purgeTokA, `{"tags":[""]}`)
	purgeWantErr(t, bad, http.StatusBadRequest, httperr.CodeBadRequest)
	record(bad)

	unauth := purgeDo(t, g, "", `{"all":true}`)
	purgeWantErr(t, unauth, http.StatusUnauthorized, httperr.CodeUnauthorized)
	record(unauth)

	forbid := purgeDo(t, g, "pt-Z-unknown", `{"all":true}`)
	purgeWantErr(t, forbid, http.StatusForbidden, httperr.CodeForbidden)
	record(forbid)

	shared.SetOpMode("purge", teststore.Fail, fmt.Errorf("purge down: %w", store.ErrUnavailable))
	down := purgeDo(t, g, purgeTokA, `{"tags":["tagsecret-K3"]}`)
	purgeWantErr(t, down, http.StatusServiceUnavailable, httperr.CodeStoreUnavailable)
	record(down)
	shared.SetOpMode("purge", teststore.Inherit, nil)

	text := logs.String() + strings.Join(bodies, "\n") + metricsText(t, g)
	secrets := []string{purgeTokA, strings.TrimPrefix(auHash(purgeTokA), "sha256:"), "qsecret=V0x9", "V0x9", "tagsecret-K3"}
	for _, s := range secrets {
		for _, f := range leakcheck.Forms(s) {
			if strings.Contains(text, f) {
				t.Errorf("logs, responses or metrics reveal a form of a secret (%d bytes)", len(s))
			}
		}
	}

	recs := logs.records(t, "cache purged")
	if len(recs) != 1 {
		t.Fatalf("cache purged records = %d, want 1", len(recs))
	}
	m := recs[0]
	if m["key_id"] != "key-0" || m["type"] != "url" || m["mode"] != "hard" || m["accepted"] != float64(1) || m["affected"] != float64(1) || m["scope"] != "store" {
		t.Fatalf("success record = %v", m)
	}
}

func TestPurgeConfigWithoutDefaults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	n := newMemNetwork()
	be := n.serve(t, newFlightOrigin())
	fake := clock.NewFake(cacheStart)
	shared := teststore.NewSharedCache(fake, 1<<20)
	t.Cleanup(func() { _ = shared.Close() })
	shareOpt := cacheSharedOption("shared", shared)

	unset := func(cfg *config.Config) {
		cfg.Cache.StoreTimeout, cfg.Cache.MaxBackground = nil, nil
		cfg.Cache.Coalescing.Enabled, cfg.Cache.Coalescing.MaxKeys, cfg.Cache.Coalescing.MaxWaiters = nil, nil, nil
	}
	parse := func(doc string) *config.Config {
		t.Helper()
		cfg, err := config.Parse([]byte(doc))
		if err != nil {
			t.Fatal(err)
		}
		unset(cfg)
		return cfg
	}

	cfg := parse(cacheSharedDoc(be.URL, purgeAdminYAML(purgeTokA), cacheRouteYAML("c")))
	if err := microgateway.Validate(cfg, shareOpt); err != nil {
		t.Fatalf("Validate of an undefaulted config: %v", err)
	}
	if cfg.Cache.StoreTimeout != nil {
		t.Fatal("Validate changed the caller's config")
	}
	g, err := microgateway.New(cfg, shareOpt, withClock(fake))
	if err != nil {
		t.Fatalf("New of an undefaulted config: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if got := g.Config().Cache.StoreTimeout.Std(); got != 100*time.Millisecond {
		t.Fatalf("store_timeout = %v, want 100ms", got)
	}
	res, err := g.Purge(ctx, microgateway.PurgeRequest{All: true})
	if err != nil || res.Scope != "store" {
		t.Fatalf("Purge all = %+v, %v; want scope store and nil error", res, err)
	}
	purgeOK(t, purgeDo(t, g, purgeTokA, `{"all":true}`))

	shared.SetOpMode("purge", teststore.Hang, nil)
	done := make(chan error, 1)
	go func() {
		_, err := g.Purge(ctx, microgateway.PurgeRequest{All: true})
		done <- err
	}()
	awaitDue(t, fake, 1, 100*time.Millisecond, "store timeout timer", nil)
	fake.Advance(100*time.Millisecond - time.Nanosecond)
	select {
	case err := <-done:
		t.Fatalf("purge returned %v before the default store timeout", err)
	default:
	}
	fake.Advance(time.Nanosecond)
	perr, _ := testwait.Recv(t, "purge after the default store timeout", done, nil)
	if !errors.Is(perr, store.ErrUnavailable) {
		t.Fatalf("purge error = %v, want store.ErrUnavailable", perr)
	}
	shared.SetOpMode("purge", teststore.Inherit, nil)

	cfg2 := parse(cacheSharedDoc(be.URL, purgeAdminYAML(purgeTokA), "  - name: c\n    match:\n      path: /\n    upstream: up\n    timeout: 2s\n    cache:\n      enabled: true\n"))
	if err := g.Reload(cfg2); err != nil {
		t.Fatalf("Reload of an undefaulted config: %v", err)
	}
	if _, err := g.Purge(ctx, microgateway.PurgeRequest{All: true}); err != nil {
		t.Fatalf("Purge all after reload: %v", err)
	}

	mcfg := parse(cacheGatewayDoc(be.URL, purgeAdminYAML(purgeTokA), cacheRouteYAML("c")))
	mcfg.Cache.Memory.MaxBytes, mcfg.Cache.Memory.Shards = nil, nil
	mg, err := microgateway.New(mcfg, withClock(fake))
	if err != nil {
		t.Fatalf("New of an undefaulted memory config: %v", err)
	}
	t.Cleanup(func() { _ = mg.Close() })
	if shards := mg.Config().Cache.Memory.Shards; shards == nil || *shards != 16 {
		t.Fatalf("memory shards = %v, want 16", shards)
	}
	mres, err := mg.Purge(ctx, microgateway.PurgeRequest{All: true})
	if err != nil || mres.Scope != "instance" {
		t.Fatalf("memory Purge all = %+v, %v; want scope instance", mres, err)
	}
}

func TestPurgeErrorShape(t *testing.T) {
	t.Parallel()
	g, _, _, shared := purgeSharedFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	shared.SetOpMode("purge", teststore.Fail, fmt.Errorf("purge down: %w", store.ErrUnavailable))
	noKeys, _, _ := purgeFixture(t, "", cacheRouteYAML("r"))
	noCache, _, _ := purgeFixture(t, purgeAdminYAML(purgeTokA), purgeNoCacheRoute)
	closed, _, _ := purgeFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	if err := closed.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	cases := []struct {
		name   string
		rec    *httptest.ResponseRecorder
		status int
		code   string
	}{
		{"400", purgeDo(t, g, purgeTokA, `{}`), http.StatusBadRequest, httperr.CodeBadRequest},
		{"401", purgeDo(t, g, "", `{"all":true}`), http.StatusUnauthorized, httperr.CodeUnauthorized},
		{"403", purgeDo(t, g, "pt-Z-wrong", `{"all":true}`), http.StatusForbidden, httperr.CodeForbidden},
		{"404 no keys", purgeDo(t, noKeys, purgeTokA, `{"all":true}`), http.StatusNotFound, httperr.CodeNotFound},
		{"404 no cache", purgeDo(t, noCache, purgeTokA, `{"all":true}`), http.StatusNotFound, httperr.CodeCacheNotConfigured},
		{"405", purgeGetAdmin(g, http.MethodGet), http.StatusMethodNotAllowed, httperr.CodeMethodNotAllowed},
		{"503 store", purgeDo(t, g, purgeTokA, `{"all":true}`), http.StatusServiceUnavailable, httperr.CodeStoreUnavailable},
		{"503 closed", purgeDo(t, closed, purgeTokA, `{"all":true}`), http.StatusServiceUnavailable, httperr.CodeUnavailable},
	}
	for _, c := range cases {
		purgeWantErr(t, c.rec, c.status, c.code)
	}
	if httperr.CodeNotFound == httperr.CodeCacheNotConfigured {
		t.Fatal("the two 404 codes are equal")
	}
}

func TestPurgeNoGoroutineLeak(t *testing.T) {
	t.Parallel()
	pre := snapGoroutines()
	g, fake, origin, shared := purgeSharedFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	t.Cleanup(func() { _ = g.Close() })
	origin.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Surrogate-Key": {"tgx"}}, "body")
	purgeGet(t, g, "http://h/p", nil)
	base := snapGoroutines()

	for range 5 {
		purgeGet(t, g, "http://h/p", nil)
		purgeDo(t, g, purgeTokA, `{"urls":["http://h/p"]}`)
		purgeGet(t, g, "http://h/p", nil)
		purgeDo(t, g, purgeTokA, `{"tags":["tgx"],"soft":true}`)
		purgeDo(t, g, purgeTokA, `{"all":true}`)
		purgeDo(t, g, purgeTokA, `{"all":true,"soft":true}`)
	}
	shared.SetOpMode("purge", teststore.Fail, fmt.Errorf("purge down: %w", store.ErrUnavailable))
	purgeDo(t, g, purgeTokA, `{"all":true}`)
	shared.SetOpMode("purge", teststore.Hang, nil)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- purgeDo(t, g, purgeTokA, `{"all":true}`) }()
	awaitDue(t, fake, 1, 100*time.Millisecond, "store timeout timer", nil)
	fake.Advance(100 * time.Millisecond)
	testwait.Recv(t, "purge after store timeout", done, nil)
	shared.SetOpMode("purge", teststore.Inherit, nil)

	awaitGoroutinesSettled(t, "goroutines started by purges to exit", pre, base)
}
