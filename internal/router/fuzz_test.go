package router

import (
	"reflect"
	"testing"
)

func FuzzLookup(f *testing.F) {
	f.Add("api.example.com", "/api/v1/users/7", "GET")
	f.Add("", "/", "POST")
	f.Add("A.EU.EXAMPLE.COM:80", "/x//y", "PUT")
	f.Add("[::1]:8080", "/o/", "DELETE")
	f.Add("", "relative", "GET")

	entries := precedenceEntries()
	rt, err := New(entries)
	if err != nil {
		f.Fatalf("New error: %v", err)
	}
	compiled := make(map[string]*Pattern, len(entries))
	for _, e := range entries {
		p, err := Compile(e.Path, e.PathType)
		if err != nil {
			f.Fatalf("Compile(%q) error: %v", e.Path, err)
		}
		compiled[e.Value.(string)] = p
	}

	f.Fuzz(func(t *testing.T, host, path, method string) {
		req := newRequest(host, method, path, nil)
		v, p, ok := rt.Lookup(req)
		if ok {
			if v == nil {
				t.Fatal("ok with nil value")
			}
			pat := compiled[v.(string)]
			for _, prm := range p {
				if !hasParam(pat, prm.Name) {
					t.Errorf("param %q not in pattern %q", prm.Name, pat.Raw)
				}
			}
		}
		v2, p2, ok2 := rt.Lookup(req)
		if ok != ok2 || !reflect.DeepEqual(v, v2) || !reflect.DeepEqual(p, p2) {
			t.Errorf("second lookup differs: (%v, %+v, %v) vs (%v, %+v, %v)", v, p, ok, v2, p2, ok2)
		}
	})
}

func hasParam(pat *Pattern, name string) bool {
	for _, s := range pat.Segments {
		if s.Kind != SegLiteral && s.Value == name {
			return true
		}
	}
	return false
}
