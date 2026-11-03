package httpcache

import (
	"net/http"
	"testing"
	"time"
)

func BenchmarkParseCacheControl(b *testing.B) {
	b.ReportAllocs()
	h := http.Header{"Cache-Control": {"public, max-age=60, s-maxage=120, stale-while-revalidate=30, no-transform"}}
	for b.Loop() {
		_ = ParseCacheControl(h)
	}
}

func BenchmarkDecide(b *testing.B) {
	b.ReportAllocs()
	e := &Entry{Header: http.Header{"Date": {fmtDate(t0)}, "Cache-Control": {"max-age=60, stale-while-revalidate=30"}, "Etag": {`"v1"`}}}
	req := newReq(http.Header{"Cache-Control": {"max-stale=20"}})
	o := Options{StaleIfError: time.Minute}
	for b.Loop() {
		_ = Decide(req, e, 70*time.Second, o)
	}
}

func BenchmarkPrimaryKey(b *testing.B) {
	b.ReportAllocs()
	r := mkReq(keyReq{method: "GET", host: "Example.com:80", path: "/a/b", query: "x=1&y=2"})
	for b.Loop() {
		_ = PrimaryKey(r)
	}
}

func BenchmarkSecondaryKey(b *testing.B) {
	b.ReportAllocs()
	r := newReq(http.Header{"Accept": {"text/html"}, "Accept-Encoding": {"gzip, br"}})
	names := []string{"Accept", "Accept-Encoding"}
	for b.Loop() {
		_, _ = SecondaryKey(names, r)
	}
}

func BenchmarkStorable(b *testing.B) {
	b.ReportAllocs()
	req := newReq(http.Header{})
	resp := ResponseMeta{Status: 200, Header: http.Header{"Cache-Control": {"max-age=60"}, "Vary": {"Accept"}}, Complete: true, ResponseTime: t0}
	o := Options{Statuses: []int{200}, MaxTTL: time.Hour, TagHeader: "Surrogate-Key"}
	for b.Loop() {
		_, _ = Storable(req, resp, o)
	}
}
