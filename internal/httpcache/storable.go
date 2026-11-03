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

// Storable reports whether resp may be stored for req; when it may not, reason names the
// first failing rule.
func Storable(req *http.Request, resp ResponseMeta, o Options) (bool, string) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		return false, ReasonMethod
	}
	if RequestDirectives(req.Header).NoStore {
		return false, ReasonRequestNoStore
	}
	if len(req.Header.Values("Range")) > 0 {
		return false, ReasonRange
	}
	switch {
	case resp.Status == http.StatusPartialContent:
		return false, ReasonPartialContent
	case resp.Status == http.StatusNotModified:
		return false, ReasonNotModified
	case !slices.Contains(o.Statuses, resp.Status):
		return false, ReasonStatus
	}
	if !resp.Complete {
		return false, ReasonIncomplete
	}
	d := ParseCacheControl(resp.Header)
	if d.NoStore {
		return false, ReasonNoStore
	}
	if d.Private {
		return false, ReasonPrivate
	}
	names, star := VaryNames(resp.Header)
	if star {
		return false, ReasonVaryStar
	}
	if len(resp.Header.Values("Set-Cookie")) > 0 {
		return false, ReasonSetCookie
	}
	if d.NoCache && !HasValidator(resp.Header) {
		return false, ReasonNoCacheWithoutValidator
	}
	if len(names) > MaxVaryNames {
		return false, ReasonVaryLimit
	}
	if o.TagHeader != "" {
		if _, ok := ParseTags(resp.Header, o.TagHeader, o.TagDelimiter); !ok {
			return false, ReasonTags
		}
	}
	credentialed := len(req.Header.Values("Authorization")) > 0 || (o.Credential != nil && o.Credential(req))
	if credentialed && !(d.Public || d.SMaxAge.Present || d.MustRevalidate) {
		return false, ReasonCredentialed
	}
	l := lifetime(d, resp.Header, resp.ResponseTime, true, o)
	swr, sie := staleWindows(d, o)
	if StorageTTL(l, swr, sie, o.Keep, HasValidator(resp.Header)) == 0 {
		return false, ReasonZeroTTL
	}
	return true, ""
}
