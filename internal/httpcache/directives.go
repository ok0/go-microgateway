// Package httpcache implements the HTTP caching rules of RFC 9111 and the Cache-Status field of RFC 9211 as pure functions: every time is a parameter and nothing is read from a clock, so the same inputs give the same results on every instance.
package httpcache

import (
	"net/http"
	"strings"
	"time"
)

// MaxDelta is the ceiling of every delta-seconds value and of ages (RFC 9111 §1.2.2).
const MaxDelta = time.Duration(1<<31) * time.Second

// Delta is a delta-seconds directive. Value is always within [0, MaxDelta].
type Delta struct {
	Present bool
	Value   time.Duration
}

// Directives holds the Cache-Control directives the rules read. MaxStale without a
// value is MaxDelta (accept any staleness; ages never exceed MaxDelta).
type Directives struct {
	NoStore, NoCache, Private, Public, MustRevalidate, ProxyRevalidate, OnlyIfCached bool
	MaxAge, SMaxAge, StaleWhileRevalidate, StaleIfError, MinFresh, MaxStale          Delta
}

// ParseCacheControl parses every Cache-Control line of h.
func ParseCacheControl(h http.Header) Directives {
	var d Directives
	for _, line := range h.Values("Cache-Control") {
		for _, member := range splitMembers(line) {
			name, value, hasValue := splitMember(member)
			switch name {
			case "max-age":
				v, _ := parseDelta(value, hasValue)
				d.MaxAge = minDelta(d.MaxAge, v)
			case "s-maxage":
				v, _ := parseDelta(value, hasValue)
				d.SMaxAge = minDelta(d.SMaxAge, v)
			case "stale-while-revalidate":
				v, _ := parseDelta(value, hasValue)
				d.StaleWhileRevalidate = minDelta(d.StaleWhileRevalidate, v)
			case "stale-if-error":
				v, _ := parseDelta(value, hasValue)
				d.StaleIfError = minDelta(d.StaleIfError, v)
			case "min-fresh":
				if v, ok := parseDelta(value, hasValue); ok {
					d.MinFresh = maxDelta(d.MinFresh, v)
				}
			case "max-stale":
				if !hasValue {
					d.MaxStale = minDelta(d.MaxStale, MaxDelta)
				} else if v, ok := parseDelta(value, true); ok {
					d.MaxStale = minDelta(d.MaxStale, v)
				}
			case "no-store":
				d.NoStore = true
			case "no-cache":
				d.NoCache = true
			case "private":
				d.Private = true
			case "public":
				d.Public = true
			case "must-revalidate":
				d.MustRevalidate = true
			case "proxy-revalidate":
				d.ProxyRevalidate = true
			case "only-if-cached":
				d.OnlyIfCached = true
			}
		}
	}
	return d
}

// RequestDirectives parses the request Cache-Control; without Cache-Control, a
// Pragma: no-cache member sets NoCache.
func RequestDirectives(h http.Header) Directives {
	d := ParseCacheControl(h)
	if len(h.Values("Cache-Control")) == 0 && pragmaNoCache(h) {
		d.NoCache = true
	}
	return d
}

func pragmaNoCache(h http.Header) bool {
	for _, line := range h.Values("Pragma") {
		for _, m := range strings.Split(line, ",") {
			if asciiLower(trimOWS(m)) == "no-cache" {
				return true
			}
		}
	}
	return false
}

func splitMembers(line string) []string {
	var out []string
	start := 0
	inQuote := false
	for i := 0; i < len(line); i++ {
		switch c := line[i]; {
		case inQuote && c == '\\':
			i++
		case c == '"':
			inQuote = !inQuote
		case c == ',' && !inQuote:
			out = appendMember(out, line[start:i])
			start = i + 1
		}
	}
	return appendMember(out, line[start:])
}

func appendMember(out []string, m string) []string {
	m = trimOWS(m)
	if m == "" {
		return out
	}
	return append(out, m)
}

func splitMember(m string) (name, value string, hasValue bool) {
	rawName, rawValue, found := strings.Cut(m, "=")
	name = asciiLower(trimOWS(rawName))
	if !found {
		return name, "", false
	}
	rawValue = trimOWS(rawValue)
	if strings.HasPrefix(rawValue, `"`) {
		return name, unquote(rawValue[1:]), true
	}
	return name, rawValue, true
}

// unquote returns the quoted-string content that follows the opening quote, or ""
// when the closing quote is missing.
func unquote(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			if i+1 >= len(s) {
				return ""
			}
			i++
			b.WriteByte(s[i])
		case '"':
			return b.String()
		default:
			b.WriteByte(s[i])
		}
	}
	return ""
}

func parseDelta(v string, hasValue bool) (time.Duration, bool) {
	if !hasValue || v == "" {
		return 0, false
	}
	var n int64
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = min(n*10+int64(c-'0'), 1<<31)
	}
	return time.Duration(n) * time.Second, true
}

func minDelta(cur Delta, v time.Duration) Delta {
	if !cur.Present || v < cur.Value {
		cur.Value = v
	}
	cur.Present = true
	return cur
}

func maxDelta(cur Delta, v time.Duration) Delta {
	if !cur.Present || v > cur.Value {
		cur.Value = v
	}
	cur.Present = true
	return cur
}

func trimOWS(s string) string {
	return strings.Trim(s, " \t")
}

func asciiLower(s string) string {
	for i := 0; i < len(s); i++ {
		if 'A' <= s[i] && s[i] <= 'Z' {
			b := []byte(s)
			for j := i; j < len(b); j++ {
				if 'A' <= b[j] && b[j] <= 'Z' {
					b[j] += 'a' - 'A'
				}
			}
			return string(b)
		}
	}
	return s
}
