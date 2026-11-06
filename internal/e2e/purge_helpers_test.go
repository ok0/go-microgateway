package e2e

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/teststore"
)

const (
	purgeTokA = "pt-A-Zq81xV"
	purgeTokB = "pt-B-Lm27wK"
)

func purgeAdminYAML(tokens ...string) string {
	if len(tokens) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("admin:\n  auth:\n    keys:\n")
	for i, tok := range tokens {
		b.WriteString("      - id: key-" + strconv.Itoa(i) + "\n        hash: \"" + auHash(tok) + "\"\n")
	}
	return b.String()
}

func purgeDo(t *testing.T, g *microgateway.Gateway, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/cache/purge", strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(rec, req)
	return rec
}

type purgeReply struct {
	Type     string `json:"type"`
	Soft     bool   `json:"soft"`
	Accepted int    `json:"accepted"`
	Affected *int   `json:"affected,omitempty"`
	Scope    string `json:"scope"`
}

func purgeOK(t *testing.T, rec *httptest.ResponseRecorder) purgeReply {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("purge status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("purge Content-Type = %q, want application/json", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("purge Cache-Control = %q, want no-store", cc)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("purge body %q: %v", rec.Body.String(), err)
	}
	for _, k := range []string{"type", "soft", "accepted", "scope"} {
		if _, ok := raw[k]; !ok {
			t.Fatalf("purge body %q has no %q", rec.Body.String(), k)
		}
	}
	dec := json.NewDecoder(bytes.NewReader(rec.Body.Bytes()))
	dec.DisallowUnknownFields()
	var out purgeReply
	if err := dec.Decode(&out); err != nil {
		t.Fatalf("purge body %q: %v", rec.Body.String(), err)
	}
	return out
}

func purgeWantErr(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d: %s", rec.Code, status, rec.Body.String())
	}
	if e := decodeErr(t, rec.Result()); e.Status != status || e.Code != code {
		t.Fatalf("error = %+v, want status %d code %q", e, status, code)
	}
}

func purgeFixtureOn(t *testing.T, top, routes string, opts ...microgateway.Option) (*microgateway.Gateway, *clock.Fake, *flightOrigin, string) {
	t.Helper()
	n := newMemNetwork()
	origin := newFlightOrigin()
	be := n.serve(t, origin)
	fake := clock.NewFake(cacheStart)
	g := n.gateway(t, cacheGatewayDoc(be.URL, top, routes), append([]microgateway.Option{withClock(fake)}, opts...)...)
	return g, fake, origin, be.URL
}

func purgeFixture(t *testing.T, top, routes string, opts ...microgateway.Option) (*microgateway.Gateway, *clock.Fake, *flightOrigin) {
	t.Helper()
	g, fake, origin, _ := purgeFixtureOn(t, top, routes, opts...)
	return g, fake, origin
}

func purgeSharedFixture(t *testing.T, admin, routes string, opts ...microgateway.Option) (*microgateway.Gateway, *clock.Fake, *flightOrigin, *teststore.SharedCache) {
	t.Helper()
	n := newMemNetwork()
	origin := newFlightOrigin()
	be := n.serve(t, origin)
	fake := clock.NewFake(cacheStart)
	shared := teststore.NewSharedCache(fake, 1<<20)
	t.Cleanup(func() { _ = shared.Close() })
	all := append([]microgateway.Option{withClock(fake), cacheSharedOption("shared", shared)}, opts...)
	g := n.gateway(t, cacheSharedDoc(be.URL, admin, routes), all...)
	return g, fake, origin, shared
}

func purgeReq(t *testing.T, g *microgateway.Gateway, method, rawURL string, hdr http.Header) *http.Response {
	t.Helper()
	req := httptest.NewRequest(method, rawURL, nil)
	req.Host = req.URL.Host
	for k, v := range hdr {
		req.Header[k] = v
	}
	if req.URL.Scheme == "https" {
		req.TLS = &tls.ConnectionState{}
	}
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	return rec.Result()
}

func purgeGet(t *testing.T, g *microgateway.Gateway, rawURL string, hdr http.Header) *http.Response {
	t.Helper()
	return purgeReq(t, g, http.MethodGet, rawURL, hdr)
}

func purgeLast(t *testing.T, resp *http.Response) httpcache.Status {
	t.Helper()
	sts, err := httpcache.ParseStatus(strings.Join(resp.Header.Values("Cache-Status"), ", "))
	if err != nil {
		t.Fatalf("Cache-Status %q: %v", resp.Header.Values("Cache-Status"), err)
	}
	if len(sts) == 0 {
		t.Fatal("response has no Cache-Status")
	}
	return sts[len(sts)-1]
}

func jsonURLs(urls ...string) string {
	b, _ := json.Marshal(urls)
	return string(b)
}

var purgeSeriesLabels = [][2]string{
	{"url", "hard"}, {"url", "soft"}, {"tag", "hard"}, {"tag", "soft"}, {"all", "hard"}, {"all", "soft"},
}

func purgeCounts(t *testing.T, g *microgateway.Gateway) []float64 {
	t.Helper()
	body := metricsText(t, g)
	out := make([]float64, 0, len(purgeSeriesLabels))
	for _, l := range purgeSeriesLabels {
		prefix := series("microgateway_cache_purges_total", "type", l[0], "mode", l[1]) + " "
		found := false
		for _, line := range strings.Split(body, "\n") {
			if rest, ok := strings.CutPrefix(line, prefix); ok {
				v, err := strconv.ParseFloat(rest, 64)
				if err != nil {
					t.Fatal(err)
				}
				out = append(out, v)
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("series %s missing from /metrics", strings.TrimSpace(prefix))
		}
	}
	return out
}

func numbered(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = prefix + strconv.Itoa(i)
	}
	return out
}

func intp(n int) *int { return &n }

func purgeEntries(t *testing.T, g *microgateway.Gateway) float64 {
	t.Helper()
	return metricValue(t, metricsText(t, g), "microgateway_cache_entries")
}

func purgeCallsOf(shared *teststore.SharedCache) []teststore.CacheCall {
	var out []teststore.CacheCall
	for _, c := range shared.Calls() {
		if c.Op == "purge" {
			out = append(out, c)
		}
	}
	return out
}
