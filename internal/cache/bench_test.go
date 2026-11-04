package cache

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func benchRig(b *testing.B) *rig {
	b.Helper()
	rg := newRig(b, rigOpts{})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}, "benchmark body")
	return rg
}

func BenchmarkCacheHit(b *testing.B) {
	rg := benchRig(b)
	rg.get("/hit", nil)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/hit", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		rg.h.ServeHTTP(rec, req)
	}
}

func BenchmarkCacheMiss(b *testing.B) {
	rg := benchRig(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://example.com/miss?i=%d", i), nil)
		rec := httptest.NewRecorder()
		rg.h.ServeHTTP(rec, req)
	}
}

func BenchmarkCoalescedMiss(b *testing.B) {
	rg := benchRig(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://example.com/coalesced?i=%d", i), nil)
		rec := httptest.NewRecorder()
		rg.h.ServeHTTP(rec, req)
	}
}

func BenchmarkCachePassthrough(b *testing.B) {
	rg := benchRig(b)
	req := httptest.NewRequest(http.MethodPost, "http://example.com/pass", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		rg.h.ServeHTTP(rec, req)
	}
}

// benchPurgeRig returns a purge rig over a memory cache holding n entries, each tagged "load".
func benchPurgeRig(b *testing.B, n int) (*purgeRig, []string) {
	b.Helper()
	r := newPurgeRig(b, purgeOpts{})
	urls := numberedURLs(n)
	for _, u := range urls {
		putEntry(b, r.sp, u, "load")
	}
	return r, urls
}

func BenchmarkPurgeURL(b *testing.B) {
	r, urls := benchPurgeRig(b, 1000)
	req := PurgeRequest{URLs: urls}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.p.Purge(ctx, req, ""); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPurgeTag(b *testing.B) {
	r, _ := benchPurgeRig(b, 1000)
	req := PurgeRequest{Tags: []string{"load"}}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.p.Purge(ctx, req, ""); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPurgeAll(b *testing.B) {
	r, _ := benchPurgeRig(b, 1000)
	req := PurgeRequest{All: true}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.p.Purge(ctx, req, ""); err != nil {
			b.Fatal(err)
		}
	}
}
