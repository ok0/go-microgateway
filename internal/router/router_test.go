package router

import (
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"testing"
)

func precedenceEntries() []Entry {
	specs := []struct {
		name     string
		hosts    []string
		path     string
		typ      PathType
		methods  []string
		headers  map[string]string
		priority int
	}{
		{name: "root", path: "/", typ: PathPrefix},
		{name: "api", path: "/api", typ: PathPrefix},
		{name: "api-v1", path: "/api/v1", typ: PathPrefix},
		{name: "exact-users", path: "/api/v1/users", typ: PathExact},
		{name: "user-by-id", path: "/api/v1/users/:id", typ: PathPattern},
		{name: "user-any-ver", path: "/api/:ver/users/:id", typ: PathPattern},
		{name: "x-two", path: "/x/:a/:b", typ: PathPattern},
		{name: "x-glob", path: "/x/*rest", typ: PathPattern},
		{name: "m-post", path: "/m", typ: PathPrefix, methods: []string{"POST"}},
		{name: "m-any", path: "/m", typ: PathPrefix},
		{name: "h-blue", path: "/h", typ: PathPrefix, headers: map[string]string{"X-Tenant": "blue"}},
		{name: "h-any", path: "/h", typ: PathPrefix},
		{name: "p-low", path: "/p", typ: PathPrefix, priority: 0},
		{name: "p-high", path: "/p", typ: PathPrefix, priority: 5},
		{name: "o-first", path: "/o", typ: PathPrefix},
		{name: "o-second", path: "/o/", typ: PathPrefix},
		{name: "host-exact", hosts: []string{"api.example.com"}, path: "/", typ: PathPrefix},
		{name: "wild-short", hosts: []string{"*.example.com"}, path: "/", typ: PathPrefix},
		{name: "wild-long", hosts: []string{"*.eu.example.com"}, path: "/", typ: PathPrefix},
		{name: "host-delete", hosts: []string{"api.example.com"}, path: "/only", typ: PathExact, methods: []string{"DELETE"}},
		{name: "svc-put", hosts: []string{"*.svc.test"}, path: "/s", typ: PathExact, methods: []string{"PUT"}},
	}
	entries := make([]Entry, 0, len(specs))
	for i, s := range specs {
		entries = append(entries, Entry{
			Matcher: Matcher{
				Hosts:    s.hosts,
				Path:     s.path,
				PathType: s.typ,
				Methods:  s.methods,
				Headers:  s.headers,
				Priority: s.priority,
				Order:    i,
			},
			Value: s.name,
		})
	}
	return entries
}

func newRequest(host, method, path string, headers map[string]string) *http.Request {
	r := &http.Request{Method: method, Host: host, URL: &url.URL{Path: path}, Header: http.Header{}}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return r
}

func TestPrecedence(t *testing.T) {
	rt, err := New(precedenceEntries())
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	cases := []struct {
		name    string
		host    string
		method  string
		path    string
		headers map[string]string
		want    string
		params  Params
	}{
		{name: "exact path beats pattern", path: "/api/v1/users", want: "exact-users"},
		{name: "pattern with param", path: "/api/v1/users/7", want: "user-by-id", params: Params{{Name: "id", Value: "7"}}},
		{name: "pattern with more literals wins", path: "/api/v2/users/7", want: "user-any-ver", params: Params{{Name: "ver", Value: "v2"}, {Name: "id", Value: "7"}}},
		{name: "pattern beats prefix", path: "/api/v1/other", want: "api-v1"},
		{name: "prefix on segment boundary", path: "/api/v1x", want: "api"},
		{name: "glob fewer params wins", path: "/x/1/2", want: "x-glob", params: Params{{Name: "rest", Value: "1/2"}}},
		{name: "glob single segment", path: "/x/1", want: "x-glob", params: Params{{Name: "rest", Value: "1"}}},
		{name: "method constraint first", method: "POST", path: "/m/a", want: "m-post"},
		{name: "method mismatch falls through", path: "/m/a", want: "m-any"},
		{name: "header constraint first", path: "/h", headers: map[string]string{"X-Tenant": "blue"}, want: "h-blue"},
		{name: "header mismatch falls through", path: "/h", want: "h-any"},
		{name: "higher priority wins", path: "/p", want: "p-high"},
		{name: "config order tie-break", path: "/o/z", want: "o-first"},
		{name: "host ignores port and case", host: "API.Example.com:8443", path: "/anything", want: "host-exact"},
		{name: "longest wildcard host", host: "a.eu.example.com", path: "/", want: "wild-long"},
		{name: "shorter wildcard host", host: "a.example.com", path: "/", want: "wild-short"},
		{name: "wildcard does not match bare domain", host: "example.com", path: "/", want: "root"},
		{name: "exact host method match", host: "api.example.com", method: "DELETE", path: "/only", want: "host-delete"},
		{name: "exact host method fall through", host: "api.example.com", path: "/only", want: "host-exact"},
		{name: "wildcard method fall through to any host", host: "a.svc.test", path: "/s", want: "root"},
		{name: "root", path: "/", want: "root"},
		{name: "prefix bare", path: "/api", want: "api"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			host := tc.host
			if host == "" {
				host = "foo.test"
			}
			method := tc.method
			if method == "" {
				method = "GET"
			}
			v, got, ok := rt.Lookup(newRequest(host, method, tc.path, tc.headers))
			if !ok {
				t.Fatalf("no match, want %q", tc.want)
			}
			if s, _ := v.(string); s != tc.want {
				t.Errorf("matched %q, want %q", s, tc.want)
			}
			if !(len(got) == 0 && len(tc.params) == 0) && !reflect.DeepEqual(got, tc.params) {
				t.Errorf("params = %+v, want %+v", got, tc.params)
			}
		})
	}
}

func TestLookupNoMatch(t *testing.T) {
	only := []Entry{{Matcher: Matcher{Path: "/a", PathType: PathExact}, Value: "a"}}
	cases := []struct {
		name    string
		entries []Entry
		path    string
	}{
		{name: "unknown exact path", entries: only, path: "/b"},
		{name: "relative path", entries: only, path: "relative"},
		{name: "empty router", entries: nil, path: "/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt, err := New(tc.entries)
			if err != nil {
				t.Fatalf("New error: %v", err)
			}
			if _, _, ok := rt.Lookup(newRequest("foo.test", "GET", tc.path, nil)); ok {
				t.Errorf("Lookup(%q) matched, want no match", tc.path)
			}
		})
	}
}

func TestNewErrors(t *testing.T) {
	cases := []struct {
		name string
		m    Matcher
	}{
		{name: "bad pattern", m: Matcher{Path: "/a/:", PathType: PathPattern}},
		{name: "wildcard host", m: Matcher{Hosts: []string{"*"}, Path: "/", PathType: PathPrefix}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New([]Entry{{Matcher: tc.m, Value: "x"}}); err == nil {
				t.Error("New succeeded, want error")
			}
		})
	}
}

func TestParamsGet(t *testing.T) {
	p := Params{{Name: "id", Value: "7"}}
	if got := p.Get("id"); got != "7" {
		t.Errorf("Get(id) = %q, want %q", got, "7")
	}
	if got := p.Get("missing"); got != "" {
		t.Errorf("Get(missing) = %q, want empty", got)
	}
}

func BenchmarkLookup1000(b *testing.B) {
	var entries []Entry
	add := func(m Matcher, v string) {
		m.Order = len(entries)
		entries = append(entries, Entry{Matcher: m, Value: v})
	}
	for i := range 250 {
		add(Matcher{Path: fmt.Sprintf("/svc%d/health", i), PathType: PathExact}, fmt.Sprintf("exact%d", i))
		add(Matcher{Path: fmt.Sprintf("/svc%d", i), PathType: PathPrefix}, fmt.Sprintf("prefix%d", i))
		add(Matcher{Path: fmt.Sprintf("/svc%d/items/:id", i), PathType: PathPattern}, fmt.Sprintf("pattern%d", i))
		add(Matcher{Hosts: []string{fmt.Sprintf("h%d.example.com", i)}, Path: "/", PathType: PathPrefix}, fmt.Sprintf("host%d", i))
	}
	rt, err := New(entries)
	if err != nil {
		b.Fatalf("New error: %v", err)
	}
	req := newRequest("other.test", "GET", "/svc137/items/42", nil)
	if _, _, ok := rt.Lookup(req); !ok {
		b.Fatal("first lookup did not match")
	}
	b.ReportAllocs()
	for b.Loop() {
		rt.Lookup(req)
	}
}
