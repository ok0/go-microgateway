package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"reflect"
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

// Redacted returns a deep copy of c in which every non-empty JWT key secret and every API key hash
// is replaced by "REDACTED", and the userinfo and query values of every upstream host URL and JWKS URL
// are replaced by "REDACTED" as described in RedactURL. c itself is not modified.
func (c *Config) Redacted() *Config {
	r := deepCopy(reflect.ValueOf(c)).Interface().(*Config)
	for i := range r.Auth.JWT {
		for k := range r.Auth.JWT[i].Keys {
			if r.Auth.JWT[i].Keys[k].Secret != "" {
				r.Auth.JWT[i].Keys[k].Secret = "REDACTED"
			}
		}
		if r.Auth.JWT[i].JWKS != nil {
			r.Auth.JWT[i].JWKS.URL = RedactURL(r.Auth.JWT[i].JWKS.URL)
		}
	}
	for i := range r.Auth.APIKeys {
		for k := range r.Auth.APIKeys[i].Keys {
			if r.Auth.APIKeys[i].Keys[k].Hash != "" {
				r.Auth.APIKeys[i].Keys[k].Hash = "REDACTED"
			}
		}
	}
	for i := range r.Upstreams {
		for j := range r.Upstreams[i].Hosts {
			r.Upstreams[i].Hosts[j].URL = RedactURL(r.Upstreams[i].Hosts[j].URL)
		}
	}
	return r
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

// Fingerprint returns the lowercase hex SHA-256 of the JSON encoding of c. encoding/json sorts map
// keys, so the encoding is canonical. For configurations built in Go whose parameters cannot be
// encoded as JSON, the fingerprint is the SHA-256 of "unencodable: " followed by the encoding error.
func (c *Config) Fingerprint() string {
	b, err := json.Marshal(c)
	if err != nil {
		sum := sha256.Sum256([]byte("unencodable: " + err.Error()))
		return hex.EncodeToString(sum[:])
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func deepCopy(v reflect.Value) reflect.Value {
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
