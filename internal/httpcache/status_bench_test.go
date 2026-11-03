package httpcache

import "testing"

func BenchmarkStatusString(b *testing.B) {
	b.ReportAllocs()
	s := Status{Name: "microgateway", Hit: true, Fwd: FwdStale, FwdStatus: 503, HasFwdStatus: true, TTL: -5, HasTTL: true, Key: "k", Detail: "a b"}
	for b.Loop() {
		_ = s.String()
	}
}

func BenchmarkParseStatus(b *testing.B) {
	b.ReportAllocs()
	const v = `microgateway; hit; fwd=stale; fwd-status=503; ttl=-5, edge; fwd=miss; stored, origin; detail="a b"`
	for b.Loop() {
		if _, err := ParseStatus(v); err != nil {
			b.Fatal(err)
		}
	}
}
