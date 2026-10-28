package config

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"slices"
	"strings"
)

// Clone returns a deep copy of c that shares no mutable data with c. Source line information is kept.
func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}
	n := deepCopy(reflect.ValueOf(c)).Interface().(*Config)
	n.lines = c.lines
	return n
}

// Line returns the 1-based source line of the field at path, such as "listeners[0].tls.cert_file", or of its
// nearest enclosing field, in the document c was parsed from. It returns 0 when c was not parsed from a document.
func (c *Config) Line(path string) int {
	if c == nil || c.lines == nil {
		return 0
	}
	return c.lines.lineFor(path)
}

// Redacted returns a deep copy of c in which every non-empty JWT key secret, every inline API key hash, every
// non-empty store password and sentinel password, every store parameter leaf, every non-empty route header value
// (match, request and response add/set) and every middleware parameter leaf is replaced by "REDACTED".
// The PEM content loaded from public key files and the keys loaded from API key files are dropped. The userinfo
// and query values of every non-empty upstream host URL and JWKS URL are replaced by "REDACTED" as described in
// RedactURL. c itself is not modified.
func (c *Config) Redacted() *Config {
	r := deepCopy(reflect.ValueOf(c)).Interface().(*Config)
	for i := range r.Auth.JWT {
		for k := range r.Auth.JWT[i].Keys {
			if !r.Auth.JWT[i].Keys[k].Secret.IsZero() {
				r.Auth.JWT[i].Keys[k].Secret = NewSecret(Redacted)
			}
			r.Auth.JWT[i].Keys[k].LoadedPublicKey = ""
		}
		if jw := r.Auth.JWT[i].JWKS; jw != nil && !jw.URL.IsZero() {
			jw.URL = NewSecretURL(RedactURL(jw.URL.Reveal()))
		}
	}
	for i := range r.Auth.APIKeys {
		r.Auth.APIKeys[i].FileKeys = nil
		for k := range r.Auth.APIKeys[i].Keys {
			if !r.Auth.APIKeys[i].Keys[k].Hash.IsZero() {
				r.Auth.APIKeys[i].Keys[k].Hash = NewSecret(Redacted)
			}
		}
	}
	for i := range r.Upstreams {
		for j := range r.Upstreams[i].Hosts {
			if u := &r.Upstreams[i].Hosts[j].URL; !u.IsZero() {
				*u = NewSecretURL(RedactURL(u.Reveal()))
			}
		}
	}
	for i := range r.Stores {
		s := &r.Stores[i]
		if rd := s.Redis; rd != nil {
			if !rd.Password.IsZero() {
				rd.Password = NewSecret(Redacted)
			}
			if !rd.SentinelPassword.IsZero() {
				rd.SentinelPassword = NewSecret(Redacted)
			}
		}
		s.Params = s.Params.Redacted()
	}
	for i := range r.Routes {
		rt := &r.Routes[i]
		redactHeaderValues(rt.Match.Headers)
		redactHeaderValues(rt.RequestHeaders.Add)
		redactHeaderValues(rt.RequestHeaders.Set)
		redactHeaderValues(rt.ResponseHeaders.Add)
		redactHeaderValues(rt.ResponseHeaders.Set)
	}
	for i := range r.Middlewares {
		r.Middlewares[i].Params = r.Middlewares[i].Params.Redacted()
	}
	return r
}

func redactHeaderValues(m map[string]Secret) {
	for k, v := range m {
		if !v.IsZero() {
			m[k] = NewSecret(Redacted)
		}
	}
}

// RedactURL returns raw with any userinfo replaced by REDACTED and every query parameter value replaced by
// REDACTED; parameter names and their order are kept. Unparseable input is redacted heuristically.
func RedactURL(raw string) string {
	if strings.Contains(raw, "://") {
		if u, err := url.Parse(raw); err == nil {
			if u.User == nil && u.RawQuery == "" {
				return raw
			}
			if u.User != nil {
				u.User = url.User("REDACTED")
			}
			u.RawQuery = redactQuery(u.RawQuery)
			return u.String()
		}
	}
	start := 0
	if i := strings.Index(raw, "://"); i >= 0 {
		start = i + len("://")
	}
	end := len(raw)
	if i := strings.IndexAny(raw[start:], "/?#"); i >= 0 {
		end = start + i
	}
	out := raw
	if at := strings.LastIndex(raw[start:end], "@"); at >= 0 {
		out = raw[:start] + "REDACTED" + raw[start+at:]
	}
	return redactHeuristicQuery(out)
}

// redactQuery replaces each parameter value in a raw query string with REDACTED. A parameter without "="
// is kept unchanged.
func redactQuery(q string) string {
	if q == "" {
		return q
	}
	parts := strings.Split(q, "&")
	for i, p := range parts {
		if k, _, found := strings.Cut(p, "="); found {
			parts[i] = k + "=REDACTED"
		}
	}
	return strings.Join(parts, "&")
}

// redactHeuristicQuery applies redactQuery to the text between the first "?" before any "#" and the "#" or the end.
func redactHeuristicQuery(s string) string {
	limit := len(s)
	if h := strings.IndexByte(s, '#'); h >= 0 {
		limit = h
	}
	q := strings.IndexByte(s[:limit], '?')
	if q < 0 {
		return s
	}
	return s[:q+1] + redactQuery(s[q+1:limit]) + s[limit:]
}

// Fingerprint returns the lowercase hex SHA-256 of the canonical JSON encoding of c, followed by a length-prefixed
// digest input of every revealed secret, so rotating any secret changes the fingerprint. Secrets are encoded as
// "REDACTED" in the JSON. See FingerprintOf.
func (c *Config) Fingerprint() string {
	return FingerprintOf(c)
}

// FingerprintOf returns the lowercase hex SHA-256 of the canonical JSON encoding of v, followed by a
// length-prefixed digest input of every secret reachable from v, in a fixed order. Secrets never appear in the
// JSON, so the digest input is what makes rotating a secret change the result. For values whose JSON encoding
// fails, the result is the SHA-256 of "unencodable: " followed by the encoding error.
func FingerprintOf(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		sum := sha256.Sum256([]byte("unencodable: " + err.Error()))
		return hex.EncodeToString(sum[:])
	}
	h := sha256.New()
	h.Write(b)
	walkSecrets(reflect.ValueOf(v), func(kind byte, raw []byte) {
		h.Write([]byte{0, kind})
		binary.Write(h, binary.BigEndian, uint64(len(raw)))
		h.Write(raw)
	})
	return hex.EncodeToString(h.Sum(nil))
}

// walkSecrets visits every secret reachable from v in a fixed order and passes each to emit with a kind byte:
// 's' for Secret, 'u' for SecretURL and 'p' for SecretParams. Exported fields are visited in declaration order,
// slice elements in order and map entries sorted by key. A secret is never recursed into.
func walkSecrets(v reflect.Value, emit func(kind byte, raw []byte)) {
	if !v.IsValid() {
		return
	}
	switch v.Type() {
	case secretType:
		emit('s', []byte(v.Interface().(Secret).Reveal()))
		return
	case secretURLType:
		emit('u', []byte(v.Interface().(SecretURL).Reveal()))
		return
	case secretParamsType:
		p := v.Interface().(SecretParams)
		raw, err := json.Marshal(p.Reveal())
		if err != nil {
			raw = []byte("unencodable " + fmt.Sprintf("%T", p))
		}
		emit('p', raw)
		return
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			walkSecrets(v.Elem(), emit)
		}
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			if t.Field(i).IsExported() {
				walkSecrets(v.Field(i), emit)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			walkSecrets(v.Index(i), emit)
		}
	case reflect.Map:
		keys := v.MapKeys()
		slices.SortFunc(keys, func(a, b reflect.Value) int {
			return strings.Compare(fmt.Sprint(a.Interface()), fmt.Sprint(b.Interface()))
		})
		for _, k := range keys {
			walkSecrets(v.MapIndex(k), emit)
		}
	}
}

var (
	secretType       = reflect.TypeFor[Secret]()
	secretParamsType = reflect.TypeFor[SecretParams]()
	secretURLType    = reflect.TypeFor[SecretURL]()
)

func deepCopy(v reflect.Value) reflect.Value {
	if t := v.Type(); t == secretType || t == secretParamsType || t == secretURLType {
		return v
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		n := reflect.New(v.Type().Elem())
		n.Elem().Set(deepCopy(v.Elem()))
		return n
	case reflect.Struct:
		n := reflect.New(v.Type()).Elem()
		for i := 0; i < v.NumField(); i++ {
			if !v.Type().Field(i).IsExported() {
				continue
			}
			n.Field(i).Set(deepCopy(v.Field(i)))
		}
		return n
	case reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		n := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			n.Index(i).Set(deepCopy(v.Index(i)))
		}
		return n
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		n := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			n.SetMapIndex(iter.Key(), deepCopy(iter.Value()))
		}
		return n
	case reflect.Interface:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		n := reflect.New(v.Type()).Elem()
		n.Set(deepCopy(v.Elem()))
		return n
	default:
		return v
	}
}
