package cache

import (
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
