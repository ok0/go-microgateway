package redis

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ok0/go-microgateway/store"
)

const (
	// cacheBuckets is the number of hash-tag buckets that hold the primary keys.
	cacheBuckets = 256
	// purgeBatch is the number of primaries per pipeline and the HSCAN COUNT of a purge.
	purgeBatch = 100
	// toleranceUs is store.PurgeTolerance in microseconds.
	toleranceUs = int64(store.PurgeTolerance / time.Microsecond)
	// maxClockUs is the largest clock, in microseconds, that a cache script accepts. It equals
	// 1<<53 - 2 - MaxTTL/µs - clusterSkewUs, the literal in luaCacheLib, so every lifetime sum stays an exact integer.
	maxClockUs = 8912591253740990
)

// primaryKeys are the keys that hold one primary key. Every key shares the hash tag of the primary's bucket.
type primaryKeys struct {
	h, index, marker, eh, es string
	slots                    [store.MaxVariants]string
}

// primaryKeys returns the keys of primary.
func (s *Store) primaryKeys(primary string) primaryKeys {
	return s.keysOf(hash32(primary))
}

// keysOf returns the keys of the primary key whose hash is h, a 32 character lower-case hex string.
func (s *Store) keysOf(h string) primaryKeys {
	bb := h[:2]
	base := s.prefix + ":c:{" + bb + "}:"
	eh, es := s.bucketEpochKeys(bb)
	k := primaryKeys{h: h, index: base + "i:" + h, marker: base + "m:" + h, eh: eh, es: es}
	for i := range k.slots {
		k.slots[i] = base + "e:" + h + ":" + strconv.Itoa(i)
	}
	return k
}

// all returns the keys of a primary key in the order the get and purge-key scripts take them: index, marker,
// hard epoch, soft epoch and the slots 0 to 31.
func (k primaryKeys) all() []string {
	out := make([]string, 0, 4+len(k.slots))
	out = append(out, k.index, k.marker, k.eh, k.es)
	return append(out, k.slots[:]...)
}

// pendingKey returns the key of the pending fill fid of the primary key k.
func (s *Store) pendingKey(k primaryKeys, fid string) string {
	return s.prefix + ":c:{" + k.h[:2] + "}:p:" + k.h + ":" + fid
}

// bucketEpochKeys returns the hard and soft epoch keys of bucket bb.
func (s *Store) bucketEpochKeys(bb string) (eh, es string) {
	base := s.prefix + ":c:{" + bb + "}:"
	return base + "eh", base + "es"
}

// bucketNames returns the names of the cache buckets, "00" through "ff".
func bucketNames() [cacheBuckets]string {
	var out [cacheBuckets]string
	for i := range out {
		out[i] = fmt.Sprintf("%02x", i)
	}
	return out
}

// tagKeys returns the tag set, its replica, the tag marker and the marker replica of tag.
func (s *Store) tagKeys(tag string) (set, setReplica, marker, replica string) {
	base := s.prefix + ":c:t:{" + hash32(tag) + "}:"
	return base + "s", base + "sr", base + "m", base + "r"
}

// counterKeys returns the tag purge counter and its replica.
func (s *Store) counterKeys() (tq, tr string) {
	base := s.prefix + ":c:{q}:"
	return base + "tq", base + "tr"
}

// hash32 returns the first 32 characters of the lower-case hex sha256 of v.
func hash32(v string) string {
	sum := sha256.Sum256([]byte(v))
	return hex.EncodeToString(sum[:])[:32]
}

// newNonce returns 16 random bytes from crypto/rand as 32 lower-case hex characters.
func newNonce() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("redis: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// fillToken is the decoded form of a fill token.
type fillToken struct {
	h         string
	t         int64
	g, eh, es string
	q         string
}

// noneToken is a token that no store issues for a valid primary key. It decodes as foreign.
var noneToken = store.NewFillToken("r1|-")

// encodeToken returns the opaque form of ft.
func encodeToken(ft fillToken) store.FillToken {
	return store.NewFillToken("r1|" + ft.h + "|" + strconv.FormatInt(ft.t, 10) + "|" + ft.g + "|" + ft.eh + "|" + ft.es + "|" + ft.q)
}

// decodeToken parses the opaque form of tok strictly. It reports false for any token this store did not issue.
func decodeToken(tok store.FillToken) (fillToken, bool) {
	f := strings.Split(tok.Opaque(), "|")
	if len(f) != 7 || f[0] != "r1" || !isHex32(f[1]) || !isDigits(f[2]) {
		return fillToken{}, false
	}
	t, err := strconv.ParseInt(f[2], 10, 64)
	if err != nil {
		return fillToken{}, false
	}
	if f[3] != "" && (len(f[3]) != 33 || f[3][0] != 'p' || !isHex32(f[3][1:])) {
		return fillToken{}, false
	}
	for _, e := range []string{f[4], f[5]} {
		if e != "" && (len(e) != 33 || (e[0] != 'b' && e[0] != 'p') || !isHex32(e[1:])) {
			return fillToken{}, false
		}
	}
	if f[6] != "" && (len(f[6]) < 34 || f[6][32] != ' ' || !isHex32(f[6][:32]) || !isDigits(f[6][33:])) {
		return fillToken{}, false
	}
	return fillToken{h: f[1], t: t, g: f[3], eh: f[4], es: f[5], q: f[6]}, true
}

// isHex32 reports whether s is 32 lower-case hex characters.
func isHex32(s string) bool {
	if len(s) != 32 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// isDigits reports whether s is one or more decimal digits.
func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// encodeMeta returns the binary metadata of e: status, initial age, header, and tags, with nil and empty values
// told apart.
func encodeMeta(e *store.Entry) []byte {
	b := []byte{1}
	b = binary.AppendUvarint(b, uint64(e.Status))
	b = binary.AppendUvarint(b, uint64(e.InitialAge))
	if e.Header == nil {
		b = binary.AppendUvarint(b, 0)
	} else {
		names := make([]string, 0, len(e.Header))
		for n := range e.Header {
			names = append(names, n)
		}
		slices.Sort(names)
		b = binary.AppendUvarint(b, uint64(len(names))+1)
		for _, n := range names {
			b = appendStr(b, n)
			b = appendStrs(b, e.Header[n])
		}
	}
	return appendStrs(b, e.Tags)
}

// appendStr appends s as a uvarint length and its bytes.
func appendStr(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

// appendStrs appends v as 0 for a nil slice, or len(v)+1 followed by its strings.
func appendStrs(b []byte, v []string) []byte {
	if v == nil {
		return binary.AppendUvarint(b, 0)
	}
	b = binary.AppendUvarint(b, uint64(len(v))+1)
	for _, s := range v {
		b = appendStr(b, s)
	}
	return b
}

// metaReader reads the fields written by encodeMeta and fails on any overrun.
type metaReader struct {
	b []byte
}

// uvarint reads one uvarint.
func (r *metaReader) uvarint() (uint64, bool) {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		return 0, false
	}
	r.b = r.b[n:]
	return v, true
}

// str reads one length-prefixed string.
func (r *metaReader) str() (string, bool) {
	n, ok := r.uvarint()
	if !ok || n > uint64(len(r.b)) {
		return "", false
	}
	s := string(r.b[:n])
	r.b = r.b[n:]
	return s, true
}

// strs reads a nil-or-counted string slice.
func (r *metaReader) strs() ([]string, bool) {
	k, ok := r.uvarint()
	if !ok {
		return nil, false
	}
	if k == 0 {
		return nil, true
	}
	k--
	if k > uint64(len(r.b)) {
		return nil, false
	}
	out := make([]string, 0, int(k))
	for i := uint64(0); i < k; i++ {
		s, ok := r.str()
		if !ok {
			return nil, false
		}
		out = append(out, s)
	}
	return out, true
}

// decodeMeta parses the output of encodeMeta. It reports false for any malformed input, including a status outside
// 100 through 599, an initial age above store.MaxInitialAge, and trailing bytes.
func decodeMeta(b []byte) (status int, hdr http.Header, tags []string, initialAge time.Duration, ok bool) {
	if len(b) == 0 || b[0] != 1 {
		return 0, nil, nil, 0, false
	}
	r := &metaReader{b: b[1:]}
	st, ok1 := r.uvarint()
	age, ok2 := r.uvarint()
	if !ok1 || !ok2 || st < 100 || st > 599 || age > uint64(store.MaxInitialAge) {
		return 0, nil, nil, 0, false
	}
	hn, ok3 := r.uvarint()
	if !ok3 {
		return 0, nil, nil, 0, false
	}
	if hn > 0 {
		n := hn - 1
		if n > uint64(len(r.b)) {
			return 0, nil, nil, 0, false
		}
		hdr = make(http.Header, int(n))
		for i := uint64(0); i < n; i++ {
			name, ok := r.str()
			if !ok {
				return 0, nil, nil, 0, false
			}
			vals, ok := r.strs()
			if !ok {
				return 0, nil, nil, 0, false
			}
			hdr[name] = vals
		}
	}
	tags, ok = r.strs()
	if !ok || len(r.b) != 0 {
		return 0, nil, nil, 0, false
	}
	return int(st), hdr, tags, time.Duration(age), true
}

// joinTags returns the sorted, de-duplicated tags joined by newlines. It returns "" when there are no tags.
func joinTags(tags []string) string {
	s := slices.Clone(tags)
	slices.Sort(s)
	return strings.Join(slices.Compact(s), "\n")
}

// splitNames returns the comma-separated names in s. It returns an empty, non-nil slice for "".
func splitNames(s string) []string {
	if s == "" {
		return []string{}
	}
	return strings.Split(s, ",")
}

// splitDur returns d as whole microseconds and the remaining nanoseconds, both in decimal. d must be positive.
func splitDur(d time.Duration) (us, rem string) {
	return strconv.FormatInt(int64(d/time.Microsecond), 10), strconv.FormatInt(int64(d%time.Microsecond), 10)
}
