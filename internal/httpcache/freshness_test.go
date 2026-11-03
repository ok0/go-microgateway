package httpcache

import (
	"math"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func fmtDate(t time.Time) string {
	return t.UTC().Truncate(time.Second).Format(http.TimeFormat)
}

func newReq(h http.Header) *http.Request {
	return &http.Request{Method: http.MethodGet, URL: &url.URL{Path: "/"}, Header: h}
}

func TestFreshnessLifetime(t *testing.T) {
	t.Parallel()
	hour := time.Hour
	tests := []struct {
		name         string
		h            http.Header
		responseTime time.Time
		o            Options
		want         time.Duration
	}{
		{"s-maxage wins", http.Header{"Cache-Control": {"s-maxage=10, max-age=20"}, "Date": {fmtDate(t0)}, "Expires": {fmtDate(t0.Add(hour))}}, t0, Options{DefaultTTL: 3 * hour, MaxTTL: 24 * hour}, 10 * time.Second},
		{"max-age over Expires", http.Header{"Cache-Control": {"max-age=20"}, "Date": {fmtDate(t0)}, "Expires": {fmtDate(t0.Add(hour))}}, t0, Options{DefaultTTL: 3 * hour, MaxTTL: 24 * hour}, 20 * time.Second},
		{"Expires over default", http.Header{"Date": {fmtDate(t0)}, "Expires": {fmtDate(t0.Add(hour))}}, t0, Options{DefaultTTL: 3 * hour, MaxTTL: 24 * hour}, hour},
		{"default when nothing set", http.Header{}, t0, Options{DefaultTTL: 3 * hour, MaxTTL: 24 * hour}, 3 * hour},
		{"max-age=0 beats default", http.Header{"Cache-Control": {"max-age=0"}}, t0, Options{DefaultTTL: hour, MaxTTL: 24 * hour}, 0},
		{"Expires 0 invalid", http.Header{"Date": {fmtDate(t0)}, "Expires": {"0"}}, t0, Options{DefaultTTL: hour, MaxTTL: 24 * hour}, 0},
		{"Expires garbage", http.Header{"Date": {fmtDate(t0)}, "Expires": {"garbage"}}, t0, Options{DefaultTTL: hour, MaxTTL: 24 * hour}, 0},
		{"Expires RFC 850", http.Header{"Date": {fmtDate(t0)}, "Expires": {t0.Add(hour).Format(time.RFC850)}}, t0, Options{MaxTTL: 24 * hour}, hour},
		{"Expires asctime", http.Header{"Date": {fmtDate(t0)}, "Expires": {t0.Add(hour).Format(time.ANSIC)}}, t0, Options{MaxTTL: 24 * hour}, hour},
		{"Expires wrong format", http.Header{"Date": {fmtDate(t0)}, "Expires": {"2026-01-03"}}, t0, Options{DefaultTTL: hour, MaxTTL: 24 * hour}, 0},
		{"two Expires lines", http.Header{"Date": {fmtDate(t0)}, "Expires": {fmtDate(t0.Add(hour)), fmtDate(t0.Add(2 * hour))}}, t0, Options{DefaultTTL: hour, MaxTTL: 24 * hour}, 0},
		{"max-age=-1", http.Header{"Cache-Control": {"max-age=-1"}}, t0, Options{DefaultTTL: hour, MaxTTL: 24 * hour}, 0},
		{"max-age beyond int64 digits clamps to MaxTTL", http.Header{"Cache-Control": {"max-age=1111111111111111111111111111111111111111"}}, t0, Options{MaxTTL: 24 * hour}, 24 * hour},
		{"MaxTTL clamp", http.Header{"Cache-Control": {"max-age=100000"}}, t0, Options{MaxTTL: hour}, hour},
		{"missing Date uses responseTime truncated", http.Header{"Expires": {fmtDate(t0.Add(hour))}}, t0.Add(500 * time.Millisecond), Options{MaxTTL: 24 * hour}, hour},
		{"invalid Date uses responseTime truncated", http.Header{"Date": {"garbage"}, "Expires": {fmtDate(t0.Add(hour))}}, t0.Add(500 * time.Millisecond), Options{MaxTTL: 24 * hour}, hour},
		{"Expires before Date", http.Header{"Date": {fmtDate(t0)}, "Expires": {fmtDate(t0.Add(-hour))}}, t0, Options{DefaultTTL: hour, MaxTTL: 24 * hour}, 0},
		{"negative DefaultTTL", http.Header{}, t0, Options{DefaultTTL: -hour, MaxTTL: 24 * hour}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := FreshnessLifetime(tc.h, tc.responseTime, tc.o); got != tc.want {
				t.Fatalf("FreshnessLifetime = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLifetimeIndependentOfGatewayClock(t *testing.T) {
	t.Parallel()
	for _, shift := range []time.Duration{-48 * time.Hour, 0, time.Nanosecond, 72 * time.Hour} {
		t.Run(shift.String(), func(t *testing.T) {
			t.Parallel()
			h := http.Header{"Date": {fmtDate(t0)}, "Expires": {fmtDate(t0.Add(time.Hour))}}
			got := FreshnessLifetime(h, t0.Add(shift), Options{MaxTTL: 24 * time.Hour})
			if got != time.Hour {
				t.Fatalf("lifetime = %v, want 1h", got)
			}
		})
	}
}

func TestDateToAdd(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		h       http.Header
		rt      time.Time
		wantVal string
		wantSet bool
	}{
		{"absent", http.Header{}, t0, fmtDate(t0), true},
		{"invalid", http.Header{"Date": {"garbage"}}, t0, fmtDate(t0), true},
		{"repeated", http.Header{"Date": {fmtDate(t0), fmtDate(t0)}}, t0, fmtDate(t0), true},
		{"valid", http.Header{"Date": {fmtDate(t0)}}, t0, "", false},
		{"sub-second truncated", http.Header{}, t0.Add(900 * time.Millisecond), fmtDate(t0), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			val, ok := DateToAdd(tc.h, tc.rt)
			if ok != tc.wantSet || val != tc.wantVal {
				t.Fatalf("DateToAdd = (%q, %v), want (%q, %v)", val, ok, tc.wantVal, tc.wantSet)
			}
		})
	}
}

func TestInitialAge(t *testing.T) {
	t.Parallel()
	resp := t0
	tests := []struct {
		name string
		h    http.Header
		req  time.Time
		resp time.Time
		want time.Duration
	}{
		{"apparent larger", http.Header{"Date": {fmtDate(resp.Add(-30 * time.Second))}, "Age": {"5"}}, resp.Add(-time.Second), resp, 30 * time.Second},
		{"corrected larger", http.Header{"Date": {fmtDate(resp.Add(-10 * time.Second))}, "Age": {"100"}}, resp.Add(-2 * time.Second), resp, 102 * time.Second},
		{"first Age line member used", http.Header{"Age": {"7, 9"}}, resp, resp, 7 * time.Second},
		{"invalid first Age line gives 0", http.Header{"Age": {"x", "9"}}, resp, resp, 0},
		{"40-digit Age ceiling", http.Header{"Age": {"1111111111111111111111111111111111111111"}}, resp, resp, MaxDelta},
		{"response before request", http.Header{"Age": {"3"}}, resp.Add(5 * time.Second), resp, 3 * time.Second},
		{"Date in the future", http.Header{"Date": {fmtDate(resp.Add(time.Hour))}, "Age": {"0"}}, resp, resp, 0},
		{"saturates at MaxDelta", http.Header{"Age": {"2147483647"}}, resp.Add(-2 * time.Second), resp, MaxDelta},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := InitialAge(tc.h, tc.req, tc.resp); got != tc.want {
				t.Fatalf("InitialAge = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestInitialAgeShiftInvariant(t *testing.T) {
	t.Parallel()
	for _, shift := range []time.Duration{-1000 * time.Hour, -time.Hour, time.Hour, 1000 * time.Hour} {
		t.Run(shift.String(), func(t *testing.T) {
			t.Parallel()
			base := InitialAge(http.Header{"Date": {fmtDate(t0.Add(-10 * time.Second))}, "Age": {"100"}}, t0.Add(-2*time.Second), t0)
			shifted := InitialAge(http.Header{"Date": {fmtDate(t0.Add(-10*time.Second + shift))}, "Age": {"100"}}, t0.Add(-2*time.Second+shift), t0.Add(shift))
			if base != shifted || base != 102*time.Second {
				t.Fatalf("base %v shifted %v, want 102s both", base, shifted)
			}
		})
	}
}

func TestCurrentAge(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		initial  time.Duration
		storedAt time.Time
		now      time.Time
		want     time.Duration
	}{
		{"now before storedAt", 5 * time.Second, t0, t0.Add(-time.Hour), 5 * time.Second},
		{"normal", 5 * time.Second, t0, t0.Add(10 * time.Second), 15 * time.Second},
		{"saturates with MaxDelta initial", MaxDelta, t0, t0.Add(time.Second), MaxDelta},
		{"saturates with near MaxInt64 elapsed", time.Second, t0, t0.Add(time.Duration(math.MaxInt64)), MaxDelta},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := CurrentAge(tc.initial, tc.storedAt, tc.now); got != tc.want {
				t.Fatalf("CurrentAge = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBoundaries(t *testing.T) {
	t.Parallel()
	ns := time.Nanosecond

	t.Run("IsFresh at age == lifetime", func(t *testing.T) {
		t.Parallel()
		l := 10 * time.Second
		got := []bool{IsFresh(l, l-ns), IsFresh(l, l), IsFresh(l, l+ns)}
		want := []bool{true, false, false}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("point %d: got %v, want %v", i, got[i], want[i])
			}
		}
	})

	t.Run("StaleRevalidateBackground end at staleAge == swr", func(t *testing.T) {
		t.Parallel()
		h := http.Header{"Date": {fmtDate(t0)}, "Cache-Control": {"max-age=10"}}
		e := &Entry{Header: h}
		o := Options{StaleWhileRevalidate: 5 * time.Second, MaxTTL: 24 * time.Hour}
		l := 10 * time.Second
		ages := []time.Duration{l + 5*time.Second - ns, l + 5*time.Second, l + 5*time.Second + ns}
		want := []Action{StaleRevalidateBackground, Revalidate, Revalidate}
		for i, a := range ages {
			if got := Decide(newReq(http.Header{}), e, a, o); got != want[i] {
				t.Fatalf("point %d: got %v, want %v", i, got, want[i])
			}
		}
	})

	t.Run("StaleOnError at staleAge == sie", func(t *testing.T) {
		t.Parallel()
		h := http.Header{"Date": {fmtDate(t0)}, "Cache-Control": {"max-age=10"}}
		e := &Entry{Header: h}
		o := Options{StaleIfError: 5 * time.Second, MaxTTL: 24 * time.Hour}
		l := 10 * time.Second
		got := []bool{
			StaleOnError(e, l+5*time.Second-ns, o),
			StaleOnError(e, l+5*time.Second, o),
			StaleOnError(e, l+5*time.Second+ns, o),
		}
		want := []bool{true, false, false}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("point %d: got %v, want %v", i, got[i], want[i])
			}
		}
	})

	t.Run("request max-stale at staleAge == N", func(t *testing.T) {
		t.Parallel()
		h := http.Header{"Date": {fmtDate(t0)}, "Cache-Control": {"max-age=10"}}
		e := &Entry{Header: h}
		n := 5 * time.Second
		l := 10 * time.Second
		req := newReq(http.Header{"Cache-Control": {"max-stale=5"}})
		ages := []time.Duration{l + n - ns, l + n, l + n + ns}
		want := []Action{ServeStale, ServeStale, Revalidate}
		for i, a := range ages {
			if got := Decide(req, e, a, Options{MaxTTL: 24 * time.Hour}); got != want[i] {
				t.Fatalf("point %d: got %v, want %v", i, got, want[i])
			}
		}
	})

	t.Run("request min-fresh at rem == N", func(t *testing.T) {
		t.Parallel()
		h := http.Header{"Date": {fmtDate(t0)}, "Cache-Control": {"max-age=10"}}
		e := &Entry{Header: h}
		n := 3 * time.Second
		l := 10 * time.Second
		req := newReq(http.Header{"Cache-Control": {"min-fresh=3"}})
		ages := []time.Duration{l - (n + ns), l - n, l - (n - ns)}
		want := []Action{Fresh, Fresh, Revalidate}
		for i, a := range ages {
			if got := Decide(req, e, a, Options{MaxTTL: 24 * time.Hour}); got != want[i] {
				t.Fatalf("point %d: got %v, want %v", i, got, want[i])
			}
		}
	})

	t.Run("request max-age at age == N", func(t *testing.T) {
		t.Parallel()
		h := http.Header{"Date": {fmtDate(t0)}, "Cache-Control": {"max-age=60"}}
		e := &Entry{Header: h}
		n := 10 * time.Second
		req := newReq(http.Header{"Cache-Control": {"max-age=10"}})
		ages := []time.Duration{n - ns, n, n + ns}
		want := []Action{Fresh, Fresh, Revalidate}
		for i, a := range ages {
			if got := Decide(req, e, a, Options{MaxTTL: 24 * time.Hour}); got != want[i] {
				t.Fatalf("point %d: got %v, want %v", i, got, want[i])
			}
		}
	})

	t.Run("Usable at now == storedAt + ttl", func(t *testing.T) {
		t.Parallel()
		ttl := 10 * time.Second
		got := []bool{
			Usable(t0, t0.Add(ttl-ns), ttl),
			Usable(t0, t0.Add(ttl), ttl),
			Usable(t0, t0.Add(ttl+ns), ttl),
		}
		want := []bool{true, false, false}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("point %d: got %v, want %v", i, got[i], want[i])
			}
		}
	})

	t.Run("TTLSeconds of remaining at lifetime boundary", func(t *testing.T) {
		t.Parallel()
		l := 10 * time.Second
		got := []int64{
			TTLSeconds(Remaining(l, l-ns)),
			TTLSeconds(Remaining(l, l)),
			TTLSeconds(Remaining(l, l+ns)),
		}
		want := []int64{1, 0, -1}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("point %d: got %d, want %d", i, got[i], want[i])
			}
		}
	})

	t.Run("TTLSeconds rounding", func(t *testing.T) {
		t.Parallel()
		cases := map[time.Duration]int64{
			1500 * time.Millisecond:  2,
			-1500 * time.Millisecond: -2,
			2 * time.Second:          2,
			-2 * time.Second:         -2,
		}
		for in, want := range cases {
			if got := TTLSeconds(in); got != want {
				t.Fatalf("TTLSeconds(%v) = %d, want %d", in, got, want)
			}
		}
	})

	t.Run("AgeSeconds at whole-second boundary", func(t *testing.T) {
		t.Parallel()
		got := []int64{AgeSeconds(2*time.Second - ns), AgeSeconds(2 * time.Second), AgeSeconds(2*time.Second + ns)}
		want := []int64{1, 2, 2}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("point %d: got %d, want %d", i, got[i], want[i])
			}
		}
	})

	t.Run("AgeSeconds beyond MaxDelta", func(t *testing.T) {
		t.Parallel()
		if got := AgeSeconds(MaxDelta + 1); got != 1<<31 {
			t.Fatalf("AgeSeconds(MaxDelta+1) = %d, want 2^31", got)
		}
	})

	t.Run("saturation", func(t *testing.T) {
		t.Parallel()
		if got := StorageTTL(math.MaxInt64, MaxWindow, MaxWindow, MaxWindow, true); got != math.MaxInt64 {
			t.Fatalf("StorageTTL = %v, want MaxInt64", got)
		}
		if got := Remaining(math.MaxInt64, 0); got != math.MaxInt64 {
			t.Fatalf("Remaining = %v, want MaxInt64", got)
		}
		if got := CurrentAge(math.MaxInt64, t0, t0.Add(math.MaxInt64)); got != MaxDelta {
			t.Fatalf("CurrentAge = %v, want MaxDelta", got)
		}
	})
}

func TestStaleWindowsAndStorageTTL(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("1", 40)
	tests := []struct {
		name    string
		h       http.Header
		o       Options
		wantSWR time.Duration
		wantSIE time.Duration
	}{
		{"directive over fallback", http.Header{"Cache-Control": {"stale-while-revalidate=10, stale-if-error=20"}}, Options{StaleWhileRevalidate: time.Second, StaleIfError: 2 * time.Second}, 10 * time.Second, 20 * time.Second},
		{"explicit zero over fallback", http.Header{"Cache-Control": {"stale-while-revalidate=0"}}, Options{StaleWhileRevalidate: 30 * time.Second}, 0, 0},
		{"fallback only", http.Header{}, Options{StaleWhileRevalidate: 30 * time.Second, StaleIfError: 5 * time.Minute}, 30 * time.Second, 5 * time.Minute},
		{"directive ceiling", http.Header{"Cache-Control": {"stale-while-revalidate=" + long}}, Options{}, MaxWindow, 0},
		{"fallback ceiling", http.Header{}, Options{StaleWhileRevalidate: 400 * 24 * time.Hour}, MaxWindow, 0},
		{"negative fallback", http.Header{}, Options{StaleWhileRevalidate: -5 * time.Second}, 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			swr, sie := StaleWindows(tc.h, tc.o)
			if swr != tc.wantSWR || sie != tc.wantSIE {
				t.Fatalf("StaleWindows = (%v, %v), want (%v, %v)", swr, sie, tc.wantSWR, tc.wantSIE)
			}
		})
	}

	storage := []struct {
		name      string
		l, swr    time.Duration
		sie, keep time.Duration
		validator bool
		want      time.Duration
	}{
		{"keep added with validator", 10 * time.Second, 5 * time.Second, 2 * time.Second, 3 * time.Second, true, 18 * time.Second},
		{"keep not added without validator", 10 * time.Second, 5 * time.Second, 2 * time.Second, 3 * time.Second, false, 15 * time.Second},
		{"all zero", 0, 0, 0, 0, true, 0},
		{"negative inputs are zero", -time.Second, -time.Second, -time.Second, -time.Second, true, 0},
	}
	for _, tc := range storage {
		t.Run("StorageTTL "+tc.name, func(t *testing.T) {
			t.Parallel()
			if got := StorageTTL(tc.l, tc.swr, tc.sie, tc.keep, tc.validator); got != tc.want {
				t.Fatalf("StorageTTL = %v, want %v", got, tc.want)
			}
		})
	}
}
