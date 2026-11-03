package httpcache

import (
	"math"
	"net/http"
	"strings"
	"time"
)

// MaxWindow is the ceiling of the stale-while-revalidate, stale-if-error and keep windows.
const MaxWindow = 365 * 24 * time.Hour

// Options holds the route rules that apply to every response of a route. TagHeader
// "" disables tag checks. Credential nil means only Authorization marks a credentialed
// request. Options must be safe for concurrent use.
type Options struct {
	Statuses             []int
	DefaultTTL           time.Duration
	MaxTTL               time.Duration
	Keep                 time.Duration
	StaleWhileRevalidate time.Duration
	StaleIfError         time.Duration
	TagHeader            string
	TagDelimiter         Delimiter
	Credential           func(*http.Request) bool
}

func clamp(d, lo, hi time.Duration) time.Duration {
	return min(max(d, lo), hi)
}

func satAdd(a, b time.Duration) time.Duration {
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

func nonNeg(d time.Duration) time.Duration {
	return max(d, 0)
}

// HasValidator reports whether h carries a non-empty ETag or Last-Modified.
func HasValidator(h http.Header) bool {
	return h.Get("ETag") != "" || h.Get("Last-Modified") != ""
}

// FreshnessLifetime returns the freshness lifetime of a response received at responseTime.
func FreshnessLifetime(h http.Header, responseTime time.Time, o Options) time.Duration {
	return lifetime(ParseCacheControl(h), h, responseTime, true, o)
}

func lifetime(d Directives, h http.Header, fallback time.Time, haveFallback bool, o Options) time.Duration {
	var x time.Duration
	switch {
	case d.SMaxAge.Present:
		x = d.SMaxAge.Value
	case d.MaxAge.Present:
		x = d.MaxAge.Value
	case len(h.Values("Expires")) > 0:
		x = expiresLifetime(h, fallback, haveFallback)
	default:
		x = nonNeg(o.DefaultTTL)
	}
	return clamp(x, 0, nonNeg(o.MaxTTL))
}

func expiresLifetime(h http.Header, fallback time.Time, haveFallback bool) time.Duration {
	lines := h.Values("Expires")
	if len(lines) != 1 {
		return 0
	}
	expires, err := http.ParseTime(lines[0])
	if err != nil {
		return 0
	}
	date, ok := singleDate(h)
	if !ok {
		if !haveFallback {
			return 0
		}
		date = fallback.UTC().Truncate(time.Second)
	}
	return nonNeg(expires.Sub(date))
}

func singleDate(h http.Header) (time.Time, bool) {
	lines := h.Values("Date")
	if len(lines) != 1 {
		return time.Time{}, false
	}
	t, err := http.ParseTime(lines[0])
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// DateToAdd returns the Date value a caller sets before calling Storable,
// FreshnessLifetime and InitialAge when h has no single valid Date. The value encodes
// exactly the fallback instant lifetime uses, so the three functions agree.
func DateToAdd(h http.Header, responseTime time.Time) (string, bool) {
	if _, ok := singleDate(h); ok {
		return "", false
	}
	return responseTime.UTC().Truncate(time.Second).Format(http.TimeFormat), true
}

// InitialAge computes the initial age of a response (RFC 9111 §4.2.3).
func InitialAge(h http.Header, requestTime, responseTime time.Time) time.Duration {
	var ageValue time.Duration
	if lines := h.Values("Age"); len(lines) > 0 {
		first, _, _ := strings.Cut(lines[0], ",")
		ageValue, _ = parseDelta(trimOWS(first), true)
	}
	var apparent time.Duration
	if date, ok := singleDate(h); ok {
		apparent = max(0, responseTime.Sub(date))
	}
	delay := max(0, responseTime.Sub(requestTime))
	return min(MaxDelta, max(apparent, satAdd(ageValue, delay)))
}

// CurrentAge returns the age of a stored response at now.
func CurrentAge(initial time.Duration, storedAt, now time.Time) time.Duration {
	return min(MaxDelta, satAdd(clamp(initial, 0, MaxDelta), max(0, now.Sub(storedAt))))
}

// Remaining returns the freshness left at age; it is negative once the entry is stale.
func Remaining(lifetime, age time.Duration) time.Duration {
	return nonNeg(lifetime) - clamp(age, 0, MaxDelta)
}

// IsFresh reports whether an entry of the given lifetime is fresh at age.
func IsFresh(lifetime, age time.Duration) bool {
	return Remaining(lifetime, age) > 0
}

// StaleWindows returns the stale-while-revalidate and stale-if-error windows of h,
// falling back to o when h has no such directive.
func StaleWindows(h http.Header, o Options) (swr, sie time.Duration) {
	return staleWindows(ParseCacheControl(h), o)
}

func staleWindows(d Directives, o Options) (swr, sie time.Duration) {
	swr = o.StaleWhileRevalidate
	if d.StaleWhileRevalidate.Present {
		swr = d.StaleWhileRevalidate.Value
	}
	sie = o.StaleIfError
	if d.StaleIfError.Present {
		sie = d.StaleIfError.Value
	}
	return clamp(swr, 0, MaxWindow), clamp(sie, 0, MaxWindow)
}

// StorageTTL returns how long an entry is retained: lifetime plus the larger stale
// window plus, for validated entries, keep.
func StorageTTL(lifetime, swr, sie, keep time.Duration, validator bool) time.Duration {
	var k time.Duration
	if validator {
		k = clamp(keep, 0, MaxWindow)
	}
	return satAdd(satAdd(nonNeg(lifetime), max(clamp(swr, 0, MaxWindow), clamp(sie, 0, MaxWindow))), k)
}

// Usable reports whether an entry stored at storedAt is still retained at now.
func Usable(storedAt, now time.Time, storageTTL time.Duration) bool {
	return storageTTL > 0 && now.Sub(storedAt) < storageTTL
}

// TTLSeconds rounds remaining away from zero to whole seconds.
func TTLSeconds(remaining time.Duration) int64 {
	q := int64(remaining / time.Second)
	r := remaining % time.Second
	switch {
	case r > 0:
		return q + 1
	case r < 0:
		return q - 1
	}
	return q
}

// AgeSeconds returns age in whole seconds, clamped to [0, MaxDelta].
func AgeSeconds(age time.Duration) int64 {
	return int64(clamp(age, 0, MaxDelta) / time.Second)
}
