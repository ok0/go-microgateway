package httpcache

import (
	"net/http"
	"slices"
	"strings"
	"time"
)

// Reasons a response is not stored, as returned by Storable.
const (
	ReasonMethod                  = "method"
	ReasonRequestNoStore          = "request-no-store"
	ReasonRange                   = "range"
	ReasonPartialContent          = "partial-content"
	ReasonNotModified             = "not-modified"
	ReasonStatus                  = "status"
	ReasonIncomplete              = "incomplete"
	ReasonNoStore                 = "no-store"
	ReasonPrivate                 = "private"
	ReasonVaryStar                = "vary-star"
	ReasonSetCookie               = "set-cookie"
	ReasonNoCacheWithoutValidator = "no-cache-without-validator"
	ReasonVaryLimit               = "vary-limit"
	ReasonTags                    = "tags"
	ReasonCredentialed            = "credentialed"
	ReasonZeroTTL                 = "zero-ttl"
)

// Side tells where the reasons a response may not be stored lie.
type Side uint8

const (
	// SideNone means the response may be stored.
	SideNone Side = iota
	// SideRequest means only the request prevents storing; the response itself may be stored for other requests.
	SideRequest
	// SideResponse means the response itself may not be stored, whatever the request.
	SideResponse
)

// Verdict is the storability decision for one response. Reason names the first failing rule, in the order
// Storable reports it.
type Verdict struct {
	Storable bool
	Reason   string
	Side     Side
}

// MaxVaryNames is the largest number of distinct Vary field names an entry may carry.
const MaxVaryNames = 16

// ResponseMeta is an origin response offered for storage. Complete is false when the
// body was not fully received.
type ResponseMeta struct {
	Status       int
	Header       http.Header
	Complete     bool
	ResponseTime time.Time
}

// VaryNames returns the distinct lowercased Vary field names of h, sorted. star is
// true when any member is "*".
func VaryNames(h http.Header) (names []string, star bool) {
	for _, line := range h.Values("Vary") {
		for _, part := range strings.Split(line, ",") {
			name := trimOWS(part)
			if name == "" {
				continue
			}
			if name == "*" {
				star = true
				continue
			}
			name = asciiLower(name)
			if !slices.Contains(names, name) {
				names = append(names, name)
			}
		}
	}
	slices.Sort(names)
	return names, star
}

// RequestReason returns the rule of the request alone that forbids storing any response to it (method, request no-store,
// Range), or the empty string.
func RequestReason(req *http.Request) string {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		return ReasonMethod
	}
	if RequestDirectives(req.Header).NoStore {
		return ReasonRequestNoStore
	}
	if len(req.Header.Values("Range")) > 0 {
		return ReasonRange
	}
	return ""
}

// Storable reports whether resp may be stored for req; when it may not, reason names the
// first failing rule.
func Storable(req *http.Request, resp ResponseMeta, o Options) (bool, string) {
	v := Check(req, resp, o)
	return v.Storable, v.Reason
}

// Check reports whether resp may be stored for req. Side is SideResponse when a rule about the response alone fails, even if a request rule fails first.
func Check(req *http.Request, resp ResponseMeta, o Options) Verdict {
	q := RequestReason(req)
	d := ParseCacheControl(resp.Header)
	rs := responseReason(resp, d, o)
	cred := ""
	if (len(req.Header.Values("Authorization")) > 0 || (o.Credential != nil && o.Credential(req))) && !d.Public && !d.SMaxAge.Present && !d.MustRevalidate {
		cred = ReasonCredentialed
	}
	reason := q
	if reason == "" {
		reason = rs
		if cred != "" && (rs == "" || rs == ReasonZeroTTL) {
			reason = cred
		}
	}
	side := SideNone
	switch {
	case rs != "":
		side = SideResponse
	case reason != "":
		side = SideRequest
	}
	return Verdict{Storable: reason == "", Reason: reason, Side: side}
}

// responseReason returns the rule of the response alone that forbids storing it, or the empty string.
func responseReason(resp ResponseMeta, d Directives, o Options) string {
	switch {
	case resp.Status == http.StatusPartialContent:
		return ReasonPartialContent
	case resp.Status == http.StatusNotModified:
		return ReasonNotModified
	case !slices.Contains(o.Statuses, resp.Status):
		return ReasonStatus
	}
	if !resp.Complete {
		return ReasonIncomplete
	}
	if d.NoStore {
		return ReasonNoStore
	}
	if d.Private {
		return ReasonPrivate
	}
	names, star := VaryNames(resp.Header)
	if star {
		return ReasonVaryStar
	}
	if len(resp.Header.Values("Set-Cookie")) > 0 {
		return ReasonSetCookie
	}
	if d.NoCache && !HasValidator(resp.Header) {
		return ReasonNoCacheWithoutValidator
	}
	if len(names) > MaxVaryNames {
		return ReasonVaryLimit
	}
	if o.TagHeader != "" {
		if _, ok := ParseTags(resp.Header, o.TagHeader, o.TagDelimiter); !ok {
			return ReasonTags
		}
	}
	l := lifetime(d, resp.Header, resp.ResponseTime, true, o)
	swr, sie := staleWindows(d, o)
	if StorageTTL(l, swr, sie, o.Keep, HasValidator(resp.Header)) == 0 {
		return ReasonZeroTTL
	}
	return ""
}
