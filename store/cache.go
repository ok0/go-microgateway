package store

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"
)

// ErrInvalidEntry reports that an argument of Cache.Get or Cache.Put, or the
// fill token passed to Cache.Put, is invalid. Every such error wraps it. It
// never wraps ErrUnavailable.
var ErrInvalidEntry = errors.New("store: invalid cache entry")

// ErrInvalidPurge reports that a Purge fails validation. Every error returned
// by Purge.Validate wraps it.
var ErrInvalidPurge = errors.New("store: invalid purge")

const (
	// MaxTTL is the largest storage TTL that Cache.Put accepts and the largest
	// Purge.Retain. A ttl equal to MaxTTL is valid; a ttl greater than MaxTTL
	// is an invalid entry.
	MaxTTL = 3 * 365 * 24 * time.Hour
	// MaxKeyBytes is the largest length in bytes of a primary key and of a
	// purge key. A longer primary key is a miss on Get and invalid on Put.
	MaxKeyBytes = 8 << 10
	// MaxSecondaryBytes is the largest length in bytes of a secondary key.
	MaxSecondaryBytes = 128
	// MaxBodyBytes is the largest length in bytes of an entry body.
	MaxBodyBytes = 64 << 20
	// MaxInitialAge is the largest InitialAge an entry may carry.
	MaxInitialAge = time.Duration(1<<31) * time.Second
	// MaxVaryNames is the largest number of distinct Vary field names after
	// canonicalization by CanonicalVaryNames.
	MaxVaryNames = 16
	// MaxVariants is the largest number of variants held for one primary key.
	MaxVariants = 32
	// MaxTags is the largest number of tags an entry may carry.
	MaxTags = 64
	// MaxTagBytes is the largest length in bytes of one tag.
	MaxTagBytes = 1 << 10
	// MaxPurgeSelectors is the largest number of keys, or of tags, in one
	// Purge. The count is taken before de-duplication.
	MaxPurgeSelectors = 1000
	// PurgeTolerance is the window after a covering purge within which a
	// backend may refuse a fill whose token was issued inside that window.
	PurgeTolerance = time.Second
)

// Entry is a stored HTTP response.
//
// On Put, StoredAt and Stale are ignored: the store sets StoredAt from its own
// clock and never stores Stale=true from a fill. On a hit, the backend returns
// a read-only snapshot. Callers must not modify Header, Body or Tags of an
// entry they receive.
//
// InitialAge is the age the response already had when it was stored. The age
// of a hit is InitialAge + Lookup.Now.Sub(StoredAt).
type Entry struct {
	// Status is the HTTP status code, from 100 through 599.
	Status int
	// Header is the stored response header. Put copies it.
	Header http.Header
	// Body is the stored response body, at most MaxBodyBytes bytes. Put
	// copies it.
	Body []byte
	// Tags are the purge tags of the entry. Each tag is 1 to MaxTagBytes
	// bytes without control bytes, and there are at most MaxTags of them. Put
	// copies them.
	Tags []string
	// InitialAge is the age the response had when it was stored, from 0 through
	// MaxInitialAge.
	InitialAge time.Duration
	// StoredAt is the store clock reading at which the entry was stored. It is
	// set by the store on a hit and ignored on Put.
	StoredAt time.Time
	// Stale reports that a soft purge covers the entry. It is set by the store
	// on a hit and ignored on Put.
	Stale bool
}

// FillToken authorizes one Put for a primary key. A token is issued by Cache.Get
// and is a comparable value. A Put whose token was issued before a covering
// purge began is refused. The zero FillToken is never valid for Put.
//
// Backends construct tokens with NewFillToken. The opaque value is meaningful
// only to the backend that issued it. A token is usable with any handle over
// the same shared state.
type FillToken struct {
	opaque string
}

// NewFillToken returns a token carrying opaque. An empty opaque value gives the
// zero token. Backends call it to issue tokens.
func NewFillToken(opaque string) FillToken {
	return FillToken{opaque: opaque}
}

// Opaque returns the value the token was created with.
func (t FillToken) Opaque() string {
	return t.opaque
}

// IsZero reports whether t is the zero token.
func (t FillToken) IsZero() bool {
	return t.opaque == ""
}

// Lookup is the result of Cache.Get.
type Lookup struct {
	// Entry is the live entry for the requested variant, or nil on a miss. A
	// stale entry, covered only by a soft purge, is returned with Stale set.
	Entry *Entry
	// VaryNames are the canonical Vary names of the primary key's most recent
	// Put. The caller owns the slice. It is reported on hits and misses alike,
	// and is nil when the primary key has no live record.
	VaryNames []string
	// Now is the store clock reading the lookup was evaluated at. The caller
	// computes age as Entry.InitialAge + Now.Sub(Entry.StoredAt).
	Now time.Time
	// Token authorizes a later Put for the primary key. It is valid on hits
	// and misses.
	Token FillToken
}

// Cache is a backend that stores HTTP responses under a primary key and a
// secondary key, and that supports purging by key, by tag, or wholesale, and
// removing one variant.
//
// Every method runs in this order:
//
//  1. If ctx.Err() is non-nil, return it unwrapped. Nothing changes.
//  2. Validate the arguments. A failure wraps ErrInvalidEntry or ErrInvalidPurge.
//  3. If the handle is closed, return an error wrapping ErrUnavailable.
//  4. Perform one atomic operation.
//
// The clock of the store is the only time source. Entry.StoredAt and
// Lookup.Now are readings of that clock. An entry is present while
// now < StoredAt + ttl, measured on that clock.
type Cache interface {
	Backend
	// Get looks up the variant of primary selected by sel.
	//
	// An empty primary key, or one longer than MaxKeyBytes, is a miss: Entry is
	// nil, Token is valid, and sel is not called. Otherwise sel is called at most
	// once, and only when an unexpired record exists for primary. sel receives
	// the canonical Vary names of the most recent Put for primary. sel must not
	// modify or retain the slice, and must not call the Cache. sel returns the
	// secondary key and whether it selects a variant; ok=false is a miss.
	//
	// The same canonical names are returned in Lookup.VaryNames, as a slice the
	// caller owns. A nil sel is an invalid entry.
	Get(ctx context.Context, primary string, sel func(varyNames []string) (secondary string, ok bool)) (Lookup, error)
	// Put stores e as the variant secondary of primary, authorized by tok.
	//
	// Put copies the header, body, tags and Vary names. It validates its
	// arguments with ValidateFill. A ttl of zero or less stores nothing and
	// returns false with a nil error, and changes nothing. A ttl greater than
	// MaxTTL is invalid.
	//
	// An entry is present while now < StoredAt + ttl. The deadline is absolute:
	// reads and purges never extend it, and a later Put of the same variant
	// replaces it with a new deadline.
	//
	// When varyNames differ from the canonical names of the primary's record,
	// the primary's variants are discarded. When the primary already holds
	// MaxVariants variants, the variant with the oldest StoredAt is removed.
	//
	// Put returns stored=false with a nil error, and changes no entry and no Vary
	// record, when a hard or soft purge that covers the entry began after tok was
	// issued. A purge covers an entry when it lists the primary in PurgeKeys,
	// lists one of the entry's tags in PurgeTags, or is PurgeAll. A backend may
	// also refuse a fill whose token was issued within PurgeTolerance after a
	// covering purge. A tok issued before the Retain window of a covering purge
	// has an unspecified outcome. A backend may refuse, with stored=false and a
	// nil error, an entry it cannot hold.
	Put(ctx context.Context, tok FillToken, primary, secondary string, varyNames []string, e *Entry, ttl time.Duration) (stored bool, err error)
	// Purge removes or marks the entries selected by p, and reports the result.
	//
	// A hard purge makes each covered variant a miss for every Get that starts
	// after Purge returns. A soft purge keeps each covered variant readable with
	// Stale set. A later hard purge overrides an earlier soft purge. A Put with a
	// token issued after the purge stores a fresh, non-stale entry.
	//
	// PurgeTags matches tags exactly and case-sensitively, per variant. A purge
	// never changes the deadline of any entry, and it is idempotent. Purges
	// record a marker that refuses older fill tokens for p.Retain. A purge at
	// store time P protects against fills with tokens issued before P until
	// P+Retain. Callers must pass a Retain no smaller than their largest storage
	// TTL.
	Purge(ctx context.Context, p Purge) (PurgeResult, error)
	// Delete removes the variant secondary of primary and reports whether a
	// stored variant was removed. It validates its arguments with ValidateDelete.
	// A variant that is absent, expired, evicted or removed by a hard purge
	// reports false with a nil error. A variant marked stale by a soft purge is
	// stored and is removed.
	//
	// Delete removes only that variant. Other variants of primary, the Vary names
	// of primary while it keeps a variant, their tags and every other primary are
	// unchanged. When the removed variant was the last one, primary has no
	// record, as after its last variant expires.
	//
	// Delete is not a purge. It records no marker and changes no purge state, so
	// it refuses no fill token, and a later Put may store the variant again.
	Delete(ctx context.Context, primary, secondary string) (deleted bool, err error)
}

// PurgeKind selects which entries a Purge covers.
type PurgeKind uint8

// Purge kinds.
const (
	// PurgeKeys covers the variants of the primary keys listed in Purge.Keys.
	PurgeKeys PurgeKind = iota + 1
	// PurgeTags covers the variants carrying at least one tag listed in
	// Purge.Tags.
	PurgeTags
	// PurgeAll covers every variant.
	PurgeAll
)

// Purge describes a purge request.
//
// Soft is false for a hard purge and true for a soft purge. Retain is the
// marker lifetime, from greater than 0 through MaxTTL. Keys are used only by
// PurgeKeys and Tags only by PurgeTags. PurgeAll takes neither. A purge with
// Keys or Tags has at most MaxPurgeSelectors of them, and each key is 1 to
// MaxKeyBytes bytes.
type Purge struct {
	// Kind selects the entries covered by the purge.
	Kind PurgeKind
	// Keys are the primary keys covered by a PurgeKeys purge.
	Keys []string
	// Tags are the tags matched by a PurgeTags purge.
	Tags []string
	// Soft selects a soft purge when true and a hard purge when false.
	Soft bool
	// Retain is the marker lifetime. It must be at least the largest storage
	// TTL of any entry that the purge may need to refuse.
	Retain time.Duration
}

// Validate reports whether p is a valid purge. It returns nil for a valid purge
// and an error wrapping ErrInvalidPurge otherwise.
//
// Validate checks the kind, then Retain, then the kind and selector
// consistency, then the selector counts, then each key and tag.
func (p Purge) Validate() error {
	switch p.Kind {
	case PurgeKeys, PurgeTags, PurgeAll:
	default:
		return fmt.Errorf("%w: unknown purge kind %d", ErrInvalidPurge, uint8(p.Kind))
	}
	if p.Retain <= 0 || p.Retain > MaxTTL {
		return fmt.Errorf("%w: retain must be greater than 0 and at most MaxTTL", ErrInvalidPurge)
	}
	switch p.Kind {
	case PurgeKeys:
		if len(p.Keys) < 1 || len(p.Tags) != 0 {
			return fmt.Errorf("%w: keys purge needs keys and no tags", ErrInvalidPurge)
		}
	case PurgeTags:
		if len(p.Tags) < 1 || len(p.Keys) != 0 {
			return fmt.Errorf("%w: tags purge needs tags and no keys", ErrInvalidPurge)
		}
	case PurgeAll:
		if len(p.Keys) != 0 || len(p.Tags) != 0 {
			return fmt.Errorf("%w: all purge takes no keys or tags", ErrInvalidPurge)
		}
	}
	if len(p.Keys) > MaxPurgeSelectors || len(p.Tags) > MaxPurgeSelectors {
		return fmt.Errorf("%w: more than %d keys or tags", ErrInvalidPurge, MaxPurgeSelectors)
	}
	for _, k := range p.Keys {
		if k == "" || len(k) > MaxKeyBytes {
			return fmt.Errorf("%w: key must be 1 to %d bytes", ErrInvalidPurge, MaxKeyBytes)
		}
	}
	for _, t := range p.Tags {
		if reason := tagProblem(t); reason != "" {
			return fmt.Errorf("%w: %s", ErrInvalidPurge, reason)
		}
	}
	return nil
}

// PurgeResult reports the outcome of a Purge.
//
// Accepted is the number of distinct selectors: distinct keys, distinct tags,
// or 1 for PurgeAll. Selectors that match nothing are still accepted.
//
// When AffectedKnown is true, Affected counts the entries changed by the purge:
// for a hard PurgeKeys, the live variants removed; for a soft PurgeKeys, the
// primary keys that have at least one live variant; for PurgeTags, the live
// variants carrying at least one listed tag, each counted once; for PurgeAll,
// every live variant. PurgeKeys and PurgeTags always report AffectedKnown.
// PurgeAll may report AffectedKnown false, in which case Affected is 0.
type PurgeResult struct {
	// Accepted is the number of distinct selectors in the purge.
	Accepted int
	// Affected is the number of entries the purge changed, when AffectedKnown
	// is true.
	Affected int
	// AffectedKnown reports whether Affected is exact.
	AffectedKnown bool
}

// CanonicalVaryNames returns the canonical form of a list of Vary field names:
// each name lower-cased, sorted and de-duplicated. The returned slice is always
// new and never aliases names. An empty input gives an empty, non-nil slice.
//
// Each name must be non-empty, must consist only of RFC 9110 token bytes, and
// must not be "*". At most MaxVaryNames distinct names remain after
// canonicalization. Every error wraps ErrInvalidEntry.
func CanonicalVaryNames(names []string) ([]string, error) {
	out := make([]string, 0, len(names))
	for _, n := range names {
		if !validVaryName(n) {
			return nil, fmt.Errorf("%w: invalid vary name %q", ErrInvalidEntry, n)
		}
		out = append(out, strings.ToLower(n))
	}
	slices.Sort(out)
	out = slices.Compact(out)
	if len(out) > MaxVaryNames {
		return nil, fmt.Errorf("%w: more than %d vary names", ErrInvalidEntry, MaxVaryNames)
	}
	return out, nil
}

// validVaryName reports whether n is a non-empty RFC 9110 token other than "*".
func validVaryName(n string) bool {
	if n == "" || n == "*" {
		return false
	}
	for i := 0; i < len(n); i++ {
		if !isTchar(n[i]) {
			return false
		}
	}
	return true
}

// isTchar reports whether c is an RFC 9110 tchar.
func isTchar(c byte) bool {
	switch {
	case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		return true
	}
	return strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0
}

// tagProblem returns the reason tag s is invalid, or "" when s is a valid tag.
func tagProblem(s string) string {
	if s == "" {
		return "empty tag"
	}
	if len(s) > MaxTagBytes {
		return fmt.Sprintf("tag longer than %d bytes", MaxTagBytes)
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return fmt.Sprintf("tag contains control byte 0x%02x", s[i])
		}
	}
	return ""
}

// validTag returns an error wrapping ErrInvalidEntry when s is not a valid tag.
func validTag(s string) error {
	if reason := tagProblem(s); reason != "" {
		return fmt.Errorf("%w: %s", ErrInvalidEntry, reason)
	}
	return nil
}

func validKeys(primary, secondary string) error {
	if primary == "" {
		return fmt.Errorf("%w: empty primary key", ErrInvalidEntry)
	}
	if len(primary) > MaxKeyBytes {
		return fmt.Errorf("%w: primary key longer than %d bytes", ErrInvalidEntry, MaxKeyBytes)
	}
	if len(secondary) > MaxSecondaryBytes {
		return fmt.Errorf("%w: secondary key longer than %d bytes", ErrInvalidEntry, MaxSecondaryBytes)
	}
	return nil
}

// ValidateFill checks the arguments of Cache.Put in a fixed order and returns
// the canonical Vary names. Each failure wraps ErrInvalidEntry.
//
// The checks run in this order: zero token; empty primary; primary longer than
// MaxKeyBytes; secondary longer than MaxSecondaryBytes; nil entry; status
// outside 100 through 599; body longer than MaxBodyBytes; InitialAge outside
// 0 through MaxInitialAge; more than MaxTags tags; an invalid tag; invalid Vary
// names; ttl greater than MaxTTL. A ttl of zero or less is valid here. Backends
// treat it as a silent no-store.
func ValidateFill(tok FillToken, primary, secondary string, varyNames []string, e *Entry, ttl time.Duration) (canonical []string, err error) {
	if tok.IsZero() {
		return nil, fmt.Errorf("%w: zero fill token", ErrInvalidEntry)
	}
	if err := validKeys(primary, secondary); err != nil {
		return nil, err
	}
	if e == nil {
		return nil, fmt.Errorf("%w: nil entry", ErrInvalidEntry)
	}
	if e.Status < 100 || e.Status > 599 {
		return nil, fmt.Errorf("%w: status %d outside 100 to 599", ErrInvalidEntry, e.Status)
	}
	if len(e.Body) > MaxBodyBytes {
		return nil, fmt.Errorf("%w: body longer than %d bytes", ErrInvalidEntry, MaxBodyBytes)
	}
	if e.InitialAge < 0 || e.InitialAge > MaxInitialAge {
		return nil, fmt.Errorf("%w: initial age outside 0 to MaxInitialAge", ErrInvalidEntry)
	}
	if len(e.Tags) > MaxTags {
		return nil, fmt.Errorf("%w: more than %d tags", ErrInvalidEntry, MaxTags)
	}
	for _, t := range e.Tags {
		if err := validTag(t); err != nil {
			return nil, err
		}
	}
	canonical, err = CanonicalVaryNames(varyNames)
	if err != nil {
		return nil, err
	}
	if ttl > MaxTTL {
		return nil, fmt.Errorf("%w: ttl greater than MaxTTL", ErrInvalidEntry)
	}
	return canonical, nil
}

// ValidateDelete checks the arguments of Cache.Delete. Each failure wraps
// ErrInvalidEntry. The checks run in this order: empty primary; primary longer
// than MaxKeyBytes; secondary longer than MaxSecondaryBytes.
func ValidateDelete(primary, secondary string) error { return validKeys(primary, secondary) }
