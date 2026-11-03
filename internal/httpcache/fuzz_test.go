package httpcache

import (
	"net/http"
	"testing"
	"time"
)

func FuzzParseCacheControl(f *testing.F) {
	seeds := []string{
		"no-store, max-age=5",
		`max-age="60"`,
		"max-age=60, max-age=30",
		"s-maxage=9, s-maxage=4",
		"min-fresh=5, min-fresh=9",
		"max-age=-1",
		"max-age=",
		"max-age",
		"max-age=1.5",
		"max-age=1111111111111111111111111111111111111111",
		"max-age=2147483649",
		"max-stale",
		"max-stale=10",
		"max-stale=x",
		"max-stale, max-stale=3",
		`private="Set-Cookie"`,
		"public=1",
		`foo="a,b", max-age=3`,
		`max-age="60`,
		"stale-while-revalidate=30, stale-if-error=60",
		"s-maxage=60, stale-while-revalidate=30",
		"no-cache, must-revalidate, proxy-revalidate, only-if-cached",
	}
	for _, s := range seeds {
		f.Add(s, s)
	}
	f.Fuzz(func(t *testing.T, a, b string) {
		h := http.Header{
			"Cache-Control": {a, b},
			"Pragma":        {b},
			"Expires":       {a},
			"Date":          {b},
			"Age":           {a},
		}
		o := Options{MaxTTL: 24 * time.Hour}
		for _, d := range []Directives{ParseCacheControl(h), RequestDirectives(h)} {
			for _, v := range []Delta{d.MaxAge, d.SMaxAge, d.StaleWhileRevalidate, d.StaleIfError, d.MinFresh, d.MaxStale} {
				if v.Value < 0 || v.Value > MaxDelta {
					t.Fatalf("delta %v outside [0, MaxDelta]", v.Value)
				}
			}
		}
		if l := FreshnessLifetime(h, t0, o); l < 0 || l > 24*time.Hour {
			t.Fatalf("FreshnessLifetime = %v outside [0, 24h]", l)
		}
		if swr, sie := StaleWindows(h, Options{}); swr < 0 || swr > MaxWindow || sie < 0 || sie > MaxWindow {
			t.Fatalf("StaleWindows = (%v, %v) outside [0, MaxWindow]", swr, sie)
		}
		if ia := InitialAge(h, t0, t0); ia < 0 || ia > MaxDelta {
			t.Fatalf("InitialAge = %v outside [0, MaxDelta]", ia)
		}
	})
}
