package httpcache

import (
	"net/http"
	"testing"
	"time"
)

func TestDecide(t *testing.T) {
	t.Parallel()
	const (
		age10 = 10 * time.Second
		age70 = 70 * time.Second
	)
	expiresOnly := http.Header{"Expires": {fmtDate(t0.Add(time.Hour))}}
	tests := []struct {
		name  string
		nilE  bool
		req   http.Header
		resp  http.Header
		stale bool
		age   time.Duration
		o     Options
		want  Action
	}{
		{name: "nil entry", nilE: true, age: age10, want: Miss},
		{name: "nil entry only-if-cached", nilE: true, req: http.Header{"Cache-Control": {"only-if-cached"}}, age: age10, want: Unsatisfiable},
		{name: "fresh", resp: http.Header{"Cache-Control": {"max-age=60"}}, age: age10, want: Fresh},
		{name: "fresh with request no-cache", req: http.Header{"Cache-Control": {"no-cache"}}, resp: http.Header{"Cache-Control": {"max-age=60"}}, age: age10, want: Revalidate},
		{name: "fresh with Pragma no-cache", req: http.Header{"Pragma": {"no-cache"}}, resp: http.Header{"Cache-Control": {"max-age=60"}}, age: age10, want: Revalidate},
		{name: "fresh with response no-cache and ETag", resp: http.Header{"Cache-Control": {"no-cache, max-age=60"}, "Etag": {`"v1"`}}, age: age10, want: Revalidate},
		{name: "fresh with stale flag", resp: http.Header{"Cache-Control": {"max-age=60"}}, stale: true, age: age10, want: Revalidate},
		{name: "stale flag in SWR window", resp: http.Header{"Cache-Control": {"max-age=60"}}, stale: true, age: age70, o: Options{StaleWhileRevalidate: 30 * time.Second}, want: Revalidate},
		{name: "request max-age passes", req: http.Header{"Cache-Control": {"max-age=30"}}, resp: http.Header{"Cache-Control": {"max-age=60"}}, age: age10, want: Fresh},
		{name: "request max-age fails", req: http.Header{"Cache-Control": {"max-age=5"}}, resp: http.Header{"Cache-Control": {"max-age=60"}}, age: age10, want: Revalidate},
		{name: "request min-fresh passes", req: http.Header{"Cache-Control": {"min-fresh=20"}}, resp: http.Header{"Cache-Control": {"max-age=60"}}, age: age10, want: Fresh},
		{name: "request min-fresh fails", req: http.Header{"Cache-Control": {"min-fresh=55"}}, resp: http.Header{"Cache-Control": {"max-age=60"}}, age: age10, want: Revalidate},
		{name: "request max-age and min-fresh combined", req: http.Header{"Cache-Control": {"max-age=30, min-fresh=20"}}, resp: http.Header{"Cache-Control": {"max-age=60"}}, age: age10, want: Fresh},
		{name: "stale with max-stale", req: http.Header{"Cache-Control": {"max-stale=20"}}, resp: http.Header{"Cache-Control": {"max-age=60"}}, age: age70, want: ServeStale},
		{name: "stale with max-stale and must-revalidate", req: http.Header{"Cache-Control": {"max-stale=20"}}, resp: http.Header{"Cache-Control": {"max-age=60, must-revalidate"}}, age: age70, want: Revalidate},
		{name: "stale with max-stale and proxy-revalidate", req: http.Header{"Cache-Control": {"max-stale=20"}}, resp: http.Header{"Cache-Control": {"max-age=60, proxy-revalidate"}}, age: age70, want: Revalidate},
		{name: "stale with max-stale and response no-cache", req: http.Header{"Cache-Control": {"max-stale=20"}}, resp: http.Header{"Cache-Control": {"max-age=60, no-cache"}, "Etag": {`"v1"`}}, age: age70, want: Revalidate},
		{name: "stale with max-stale and stale flag", req: http.Header{"Cache-Control": {"max-stale=20"}}, resp: http.Header{"Cache-Control": {"max-age=60"}}, stale: true, age: age70, want: Revalidate},
		{name: "stale in directive SWR", resp: http.Header{"Cache-Control": {"max-age=60, stale-while-revalidate=30"}}, age: age70, want: StaleRevalidateBackground},
		{name: "stale in fallback SWR", resp: http.Header{"Cache-Control": {"max-age=60"}}, age: age70, o: Options{StaleWhileRevalidate: 30 * time.Second}, want: StaleRevalidateBackground},
		{name: "SWR blocked by must-revalidate", resp: http.Header{"Cache-Control": {"max-age=60, must-revalidate, stale-while-revalidate=30"}}, age: age70, want: Revalidate},
		{name: "SWR blocked by proxy-revalidate", resp: http.Header{"Cache-Control": {"max-age=60, proxy-revalidate, stale-while-revalidate=30"}}, age: age70, want: Revalidate},
		{name: "SWR blocked by response no-cache", resp: http.Header{"Cache-Control": {"max-age=60, no-cache, stale-while-revalidate=30"}, "Etag": {`"v1"`}}, age: age70, want: Revalidate},
		{name: "SWR blocked by stale flag", resp: http.Header{"Cache-Control": {"max-age=60, stale-while-revalidate=30"}}, stale: true, age: age70, want: Revalidate},
		{name: "s-maxage with SWR directive", resp: http.Header{"Cache-Control": {"s-maxage=60, stale-while-revalidate=30"}}, age: age70, want: StaleRevalidateBackground},
		{name: "s-maxage with fallback SWR", resp: http.Header{"Cache-Control": {"s-maxage=60"}}, age: age70, o: Options{StaleWhileRevalidate: 30 * time.Second}, want: StaleRevalidateBackground},
		{name: "s-maxage with request max-stale", req: http.Header{"Cache-Control": {"max-stale=20"}}, resp: http.Header{"Cache-Control": {"s-maxage=60"}}, age: age70, want: Revalidate},
		{name: "fresh with must-revalidate", resp: http.Header{"Cache-Control": {"max-age=60, must-revalidate"}}, age: age10, want: Fresh},
		{name: "only-if-cached with fresh", req: http.Header{"Cache-Control": {"only-if-cached"}}, resp: http.Header{"Cache-Control": {"max-age=60"}}, age: age10, want: Fresh},
		{name: "only-if-cached with SWR", req: http.Header{"Cache-Control": {"only-if-cached"}}, resp: http.Header{"Cache-Control": {"max-age=60, stale-while-revalidate=30"}}, age: age70, want: ServeStale},
		{name: "only-if-cached with serve-stale", req: http.Header{"Cache-Control": {"only-if-cached, max-stale=20"}}, resp: http.Header{"Cache-Control": {"max-age=60"}}, age: age70, want: ServeStale},
		{name: "only-if-cached with revalidate", req: http.Header{"Cache-Control": {"only-if-cached"}}, resp: http.Header{"Cache-Control": {"max-age=60"}}, age: age70, want: Unsatisfiable},
		{name: "entry without Date and Expires only", resp: expiresOnly, age: 0, want: Revalidate},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var e *Entry
			if !tc.nilE {
				e = &Entry{Header: tc.resp, Stale: tc.stale}
			}
			req := newReq(tc.req)
			o := tc.o
			o.MaxTTL = 24 * time.Hour
			if got := Decide(req, e, tc.age, o); got != tc.want {
				t.Fatalf("Decide = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestStaleOnError(t *testing.T) {
	t.Parallel()
	const age70 = 70 * time.Second
	tests := []struct {
		name  string
		resp  http.Header
		stale bool
		age   time.Duration
		o     Options
		want  bool
	}{
		{"inside sie with fallback", http.Header{"Cache-Control": {"max-age=60"}}, false, age70, Options{StaleIfError: 30 * time.Second}, true},
		{"blocked by no-cache", http.Header{"Cache-Control": {"max-age=60, no-cache"}, "Etag": {`"v1"`}}, false, age70, Options{StaleIfError: 30 * time.Second}, false},
		{"blocked by must-revalidate", http.Header{"Cache-Control": {"max-age=60, must-revalidate"}}, false, age70, Options{StaleIfError: 30 * time.Second}, false},
		{"blocked by proxy-revalidate", http.Header{"Cache-Control": {"max-age=60, proxy-revalidate"}}, false, age70, Options{StaleIfError: 30 * time.Second}, false},
		{"blocked by stale flag", http.Header{"Cache-Control": {"max-age=60"}}, true, age70, Options{StaleIfError: 30 * time.Second}, false},
		{"s-maxage with stale-if-error", http.Header{"Cache-Control": {"s-maxage=60, stale-if-error=30"}}, false, age70, Options{}, true},
		{"fresh entry", http.Header{"Cache-Control": {"max-age=60"}}, false, 10 * time.Second, Options{StaleIfError: 30 * time.Second}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := &Entry{Header: tc.resp, Stale: tc.stale}
			o := tc.o
			o.MaxTTL = 24 * time.Hour
			if got := StaleOnError(e, tc.age, o); got != tc.want {
				t.Fatalf("StaleOnError = %v, want %v", got, tc.want)
			}
		})
	}
	t.Run("nil entry", func(t *testing.T) {
		t.Parallel()
		if StaleOnError(nil, age70, Options{StaleIfError: time.Hour, MaxTTL: 24 * time.Hour}) {
			t.Fatal("StaleOnError(nil) = true")
		}
	})
}

func TestActionString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		a    Action
		want string
	}{
		{Fresh, "fresh"},
		{ServeStale, "serve-stale"},
		{StaleRevalidateBackground, "stale-revalidate-background"},
		{Revalidate, "revalidate"},
		{Miss, "miss"},
		{Unsatisfiable, "unsatisfiable"},
		{Action(0), "unknown"},
		{Action(99), "unknown"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			if got := tc.a.String(); got != tc.want {
				t.Fatalf("String() = %q, want %q", got, tc.want)
			}
		})
	}
}
