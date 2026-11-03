package httpcache

import (
	"bufio"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

type keyReq struct {
	method, host, path, query string
	tls                       bool
}

func mkReq(k keyReq) *http.Request {
	r := httptest.NewRequest("GET", "/", nil)
	r.Method = k.method
	r.Host = k.host
	r.URL.Path = k.path
	r.URL.RawPath = ""
	r.URL.RawQuery = k.query
	if k.tls {
		r.TLS = &tls.ConnectionState{}
	}
	return r
}

func readReq(t *testing.T, raw string) *http.Request {
	t.Helper()
	r, err := http.ReadRequest(bufio.NewReader(strings.NewReader(raw)))
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	return r
}

func TestPrimaryKey(t *testing.T) {
	t.Parallel()
	withFrag := mkReq(keyReq{method: "GET", host: "example.com", path: "/x"})
	withFrag.URL.Fragment = "f"
	tests := []struct {
		name  string
		a, b  *http.Request
		equal bool
	}{
		{"host case folded", mkReq(keyReq{"GET", "Example.COM", "/", "", false}), mkReq(keyReq{"GET", "example.com", "/", "", false}), true},
		{"port 80 over http dropped", mkReq(keyReq{"GET", "example.com:80", "/", "", false}), mkReq(keyReq{"GET", "example.com", "/", "", false}), true},
		{"port 443 over TLS dropped", mkReq(keyReq{"GET", "example.com:443", "/", "", true}), mkReq(keyReq{"GET", "example.com", "/", "", true}), true},
		{"port 443 without TLS differs", mkReq(keyReq{"GET", "example.com:443", "/", "", false}), mkReq(keyReq{"GET", "example.com", "/", "", false}), false},
		{"ipv6 port 80 dropped", mkReq(keyReq{"GET", "[::1]:80", "/", "", false}), mkReq(keyReq{"GET", "[::1]", "/", "", false}), true},
		{"ipv6 port 8080 kept", mkReq(keyReq{"GET", "[::1]:8080", "/", "", false}), mkReq(keyReq{"GET", "[::1]", "/", "", false}), false},
		{"empty port dropped", mkReq(keyReq{"GET", "example.com:", "/", "", false}), mkReq(keyReq{"GET", "example.com", "/", "", false}), true},
		{"zero-padded port normalized", mkReq(keyReq{"GET", "example.com:0080", "/", "", false}), mkReq(keyReq{"GET", "example.com", "/", "", false}), true},
		{"empty path is slash", mkReq(keyReq{"GET", "example.com", "", "", false}), mkReq(keyReq{"GET", "example.com", "/", "", false}), true},
		{"query order-sensitive", mkReq(keyReq{"GET", "example.com", "/x", "a=1&b=2", false}), mkReq(keyReq{"GET", "example.com", "/x", "b=2&a=1", false}), false},
		{"fragment ignored", withFrag, mkReq(keyReq{"GET", "example.com", "/x", "", false}), true},
		{"GET differs from HEAD", mkReq(keyReq{"GET", "example.com", "/", "", false}), mkReq(keyReq{"HEAD", "example.com", "/", "", false}), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := PrimaryKey(tc.a) == PrimaryKey(tc.b); got != tc.equal {
				t.Fatalf("keys equal = %v, want %v", got, tc.equal)
			}
		})
	}
}

func TestPrimaryKeyInjective(t *testing.T) {
	t.Parallel()
	tuples := []keyReq{
		{"GET", "a:1", "/b", "", false},
		{"GET", "a", ":1/b", "", false},
		{"GET", "3:x", "/y", "", false},
		{"GET", "3", ":x/y", "", false},
		{"GET", "1:x", "/", "", false},
		{"GET", "1", "x/", "", false},
		{"GET", "a", "/a?b", "", false},
		{"GET", "a", "/a", "b", false},
		{"GET", "a", "/a", "", false},
		{"GET", "a", "/a", "?", false},
		{"HEAD", "a", "/a", "", false},
		{"GET", "a", "/a", "x=1&y=2", false},
		{"GET", "a", "/a", "y=2&x=1", false},
	}
	keys := make([]string, len(tuples))
	for i, k := range tuples {
		keys[i] = PrimaryKey(mkReq(k))
	}
	for i := range keys {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] == keys[j] {
				t.Fatalf("tuples %d and %d share key", i, j)
			}
		}
	}
}

func TestURLKeys(t *testing.T) {
	t.Parallel()
	t.Run("http matches request keys", func(t *testing.T) {
		t.Parallel()
		keys, err := URLKeys("http://Example.com:80/a?b=1#f")
		if err != nil {
			t.Fatalf("URLKeys: %v", err)
		}
		get := readReq(t, "GET /a?b=1 HTTP/1.1\r\nHost: example.com:80\r\n\r\n")
		head := readReq(t, "HEAD /a?b=1 HTTP/1.1\r\nHost: example.com:80\r\n\r\n")
		if len(keys) != 2 || keys[0] != PrimaryKey(get) || keys[1] != PrimaryKey(head) {
			t.Fatalf("keys mismatch with request keys")
		}
	})
	t.Run("https matches request keys", func(t *testing.T) {
		t.Parallel()
		keys, err := URLKeys("https://example.com/a?b=1")
		if err != nil {
			t.Fatalf("URLKeys: %v", err)
		}
		get := readReq(t, "GET /a?b=1 HTTP/1.1\r\nHost: example.com\r\n\r\n")
		get.TLS = &tls.ConnectionState{}
		head := readReq(t, "HEAD /a?b=1 HTTP/1.1\r\nHost: example.com\r\n\r\n")
		head.TLS = &tls.ConnectionState{}
		if len(keys) != 2 || keys[0] != PrimaryKey(get) || keys[1] != PrimaryKey(head) {
			t.Fatalf("keys mismatch with request keys")
		}
	})
	t.Run("ipv6 matches request key", func(t *testing.T) {
		t.Parallel()
		keys, err := URLKeys("http://[::1]:8080/")
		if err != nil {
			t.Fatalf("URLKeys: %v", err)
		}
		get := readReq(t, "GET / HTTP/1.1\r\nHost: [::1]:8080\r\n\r\n")
		if keys[0] != PrimaryKey(get) {
			t.Fatal("ipv6 key mismatch")
		}
	})
	rejects := []struct {
		raw    string
		reason string
	}{
		{"/a", "not absolute"},
		{"example.com/a", "not absolute"},
		{"http:opaque", "not absolute"},
		{"http://h:x/", "not absolute"},
		{"ftp://h/", "scheme must be http or https"},
		{"http://u:p@h/", "userinfo not allowed"},
		{"http:///a", "empty host"},
		{"http://h:99999/", "invalid port"},
	}
	for _, tc := range rejects {
		t.Run("rejects "+tc.reason+" "+tc.raw, func(t *testing.T) {
			t.Parallel()
			_, err := URLKeys(tc.raw)
			if !errors.Is(err, ErrInvalidURL) {
				t.Fatalf("error %v does not wrap ErrInvalidURL", err)
			}
			var ue *URLError
			if !errors.As(err, &ue) || ue.Reason != tc.reason {
				t.Fatalf("error %v, want reason %q", err, tc.reason)
			}
			if strings.Contains(err.Error(), tc.raw) {
				t.Fatalf("error text echoes input: %q", err.Error())
			}
		})
	}
}

func TestSecondaryKey(t *testing.T) {
	t.Parallel()
	mk := func(pairs ...string) *http.Request {
		h := http.Header{}
		for i := 0; i < len(pairs); i += 2 {
			h.Add(pairs[i], pairs[i+1])
		}
		return newReq(h)
	}
	key := func(names []string, r *http.Request) string {
		k, ok := SecondaryKey(names, r)
		if !ok {
			t.Fatalf("SecondaryKey(%q) ok=false", names)
		}
		return k
	}
	t.Run("order and case independence", func(t *testing.T) {
		t.Parallel()
		a := key([]string{"Accept", "accept-encoding"}, mk("Accept", "text/html", "Accept-Encoding", "gzip"))
		b := key([]string{"Accept-Encoding", "ACCEPT"}, mk("Accept", "text/html", "Accept-Encoding", "gzip"))
		if a != b {
			t.Fatal("keys differ across order or case")
		}
	})
	t.Run("absent and empty differ", func(t *testing.T) {
		t.Parallel()
		if key([]string{"X"}, mk()) == key([]string{"X"}, mk("X", "")) {
			t.Fatal("absent and empty header share a key")
		}
	})
	t.Run("two lines equal one comma line", func(t *testing.T) {
		t.Parallel()
		if key([]string{"X"}, mk("X", "a", "X", "b")) != key([]string{"X"}, mk("X", "a, b")) {
			t.Fatal("two lines differ from one line")
		}
	})
	t.Run("surrounding whitespace ignored", func(t *testing.T) {
		t.Parallel()
		if key([]string{"X"}, mk("X", " \ta\t ")) != key([]string{"X"}, mk("X", "a")) {
			t.Fatal("whitespace changed the key")
		}
	})
	t.Run("value not contained in key", func(t *testing.T) {
		t.Parallel()
		k := key([]string{"X"}, mk("X", "secret-token-123"))
		if len(k) != 64 || strings.Contains(k, "secret-token-123") {
			t.Fatalf("key %q exposes value or wrong length", k)
		}
	})
	t.Run("empty names constant", func(t *testing.T) {
		t.Parallel()
		if key(nil, mk("X", "a")) != key(nil, mk("Y", "b")) {
			t.Fatal("empty-name keys differ")
		}
	})
	t.Run("rejects invalid names", func(t *testing.T) {
		t.Parallel()
		many := make([]string, 0, 17)
		for i := range 17 {
			many = append(many, "h"+strconv.Itoa(i))
		}
		for _, names := range [][]string{{"*"}, {""}, {"bad name"}, many} {
			if _, ok := SecondaryKey(names, mk()); ok {
				t.Fatalf("SecondaryKey(%q) ok=true, want false", names)
			}
		}
	})
}
