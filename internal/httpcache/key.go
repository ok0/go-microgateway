package httpcache

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

// ErrInvalidURL is wrapped by every URLError.
var ErrInvalidURL = errors.New("httpcache: invalid URL")

// URLError reports why a URL cannot be used as a cache key. Its text never echoes the URL.
type URLError struct {
	Reason string
}

func (e *URLError) Error() string {
	return "httpcache: invalid URL: " + e.Reason
}

func (e *URLError) Unwrap() error {
	return ErrInvalidURL
}

func field(b *strings.Builder, s string) {
	b.WriteString(strconv.Itoa(len(s)))
	b.WriteByte(':')
	b.WriteString(s)
}

func buildKey(method, scheme, host, path, query string) string {
	var b strings.Builder
	field(&b, method)
	field(&b, scheme)
	field(&b, host)
	field(&b, path)
	field(&b, query)
	return b.String()
}

// PrimaryKey returns the primary cache key of r. The fragment is never part of the key.
func PrimaryKey(r *http.Request) string {
	method := r.Method
	if method == "" {
		method = http.MethodGet
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	host := r.Host
	if host == "" {
		host = r.URL.Host
	}
	path := r.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	return buildKey(method, scheme, normHost(host, scheme), path, r.URL.RawQuery)
}

func normHost(h, scheme string) string {
	h = asciiLower(h)
	name, port, hasPort := splitPort(h)
	if !hasPort || port == "" {
		return name
	}
	if allDigits(port) {
		if n, err := strconv.Atoi(port); err == nil && n <= 65535 {
			if (scheme == "http" && n == 80) || (scheme == "https" && n == 443) {
				return name
			}
			return name + ":" + strconv.Itoa(n)
		}
	}
	return name + ":" + port
}

func splitPort(h string) (name, port string, ok bool) {
	if strings.HasPrefix(h, "[") {
		end := strings.IndexByte(h, ']')
		if end < 0 {
			return h, "", false
		}
		rest := h[end+1:]
		if !strings.HasPrefix(rest, ":") {
			return h, "", false
		}
		return h[:end+1], rest[1:], true
	}
	if strings.Count(h, ":") != 1 {
		return h, "", false
	}
	i := strings.IndexByte(h, ':')
	return h[:i], h[i+1:], true
}

func allDigits(s string) bool {
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

// URLKeys returns the GET and HEAD primary keys of an absolute http or https URL.
func URLKeys(rawURL string) ([]string, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Opaque != "" {
		return nil, &URLError{"not absolute"}
	}
	scheme := asciiLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil, &URLError{"scheme must be http or https"}
	}
	if u.User != nil {
		return nil, &URLError{"userinfo not allowed"}
	}
	if u.Hostname() == "" {
		return nil, &URLError{"empty host"}
	}
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if !allDigits(p) || err != nil || n < 1 || n > 65535 {
			return nil, &URLError{"invalid port"}
		}
	}
	host := normHost(u.Host, scheme)
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	return []string{
		buildKey(http.MethodGet, scheme, host, path, u.RawQuery),
		buildKey(http.MethodHead, scheme, host, path, u.RawQuery),
	}, nil
}

// SecondaryKey returns the digest of the request header values named by varyNames.
// ok is false when a name is empty, "*", not a token, or more than MaxVaryNames remain.
func SecondaryKey(varyNames []string, r *http.Request) (string, bool) {
	names := make([]string, 0, len(varyNames))
	for _, n := range varyNames {
		names = append(names, asciiLower(n))
	}
	slices.Sort(names)
	names = slices.Compact(names)
	if len(names) > MaxVaryNames {
		return "", false
	}
	for _, n := range names {
		if n == "*" || !isToken(n) {
			return "", false
		}
	}
	var b strings.Builder
	for _, n := range names {
		field(&b, n)
		vals := r.Header.Values(n)
		if len(vals) == 0 {
			b.WriteByte('0')
			continue
		}
		b.WriteByte('1')
		parts := make([]string, 0, len(vals))
		for _, v := range vals {
			parts = append(parts, trimOWS(v))
		}
		field(&b, strings.Join(parts, ", "))
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:]), true
}

func isToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return true
}
