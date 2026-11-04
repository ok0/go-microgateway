package e2e

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

const caKey = "cache-auth-key-1"

// caTop returns a top-level auth block with one API-key provider, keys1, that reads the default X-API-Key header.
func caTop() string {
	return fmt.Sprintf("auth:\n  api_keys:\n    - name: keys1\n      keys:\n        - id: k1\n          hash: %q\n          consumer: alice\n", auHash(caKey))
}

// caHdr returns a request header carrying key in the canonical X-Api-Key form, which cacheDo copies unchanged.
func caHdr(key string) http.Header {
	h := http.Header{}
	h.Set("X-API-Key", key)
	return h
}

// caDoc returns a configuration with the top-level lines global, caTop and one cached, API-key protected route r
// whose pipeline is the flow sequence pipeline, such as "[auth, cache]".
func caDoc(backendURL, global, pipeline string) string {
	return cacheGatewayDoc(backendURL, global+caTop(), cacheAPIKeyRouteYAML("r")+"    pipeline: "+pipeline+"\n")
}

func TestCacheAuthAheadOfCache(t *testing.T) {
	t.Parallel()
	n := newMemNetwork()
	var origin atomic.Int64
	be := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "private-body")
	}))
	g := n.gateway(t, caDoc(be.URL, "", "[auth, cache]"))

	r1 := cacheDo(g, http.MethodGet, "/x", caHdr(caKey))
	if r1.Code != http.StatusOK {
		t.Fatalf("first authenticated request: status = %d, want 200", r1.Code)
	}
	if got := r1.Body.String(); got != "private-body" {
		t.Fatalf("first authenticated request: body = %q, want %q", got, "private-body")
	}
	if st := cacheLast(t, r1); st.Hit || !st.Stored {
		t.Fatalf("first authenticated request: cache status = %+v, want miss and stored", st)
	}
	if got := origin.Load(); got != 1 {
		t.Fatalf("origin calls after first request = %d, want 1", got)
	}

	r2 := cacheDo(g, http.MethodGet, "/x", caHdr(caKey))
	if r2.Code != http.StatusOK {
		t.Fatalf("second authenticated request: status = %d, want 200", r2.Code)
	}
	if got := r2.Body.String(); got != "private-body" {
		t.Fatalf("second authenticated request: body = %q, want %q", got, "private-body")
	}
	if !cacheLast(t, r2).Hit {
		t.Fatal("second authenticated request: want a cache hit")
	}
	if got := origin.Load(); got != 1 {
		t.Fatalf("origin calls after second request = %d, want 1", got)
	}

	for _, tc := range []struct {
		name string
		hdr  http.Header
	}{
		{"no credentials", nil},
		{"invalid credentials", caHdr("wrong-key")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := cacheDo(g, http.MethodGet, "/x", tc.hdr)
			auAssert401(t, rec, "ApiKey")
			if strings.Contains(rec.Body.String(), "private-body") {
				t.Error("401 response leaks the cached body")
			}
			if len(rec.Header().Values("Cache-Status")) != 0 {
				t.Errorf("401 response has Cache-Status %q, want none", rec.Header().Values("Cache-Status"))
			}
			if got := origin.Load(); got != 1 {
				t.Errorf("origin calls = %d, want 1", got)
			}
		})
	}

	r5 := cacheDo(g, http.MethodGet, "/x", caHdr(caKey))
	if r5.Code != http.StatusOK {
		t.Fatalf("authenticated request after 401s: status = %d, want 200", r5.Code)
	}
	if !cacheLast(t, r5).Hit {
		t.Fatal("authenticated request after 401s: want a cache hit")
	}
	if got := origin.Load(); got != 1 {
		t.Fatalf("origin calls after 401s = %d, want 1", got)
	}
}
