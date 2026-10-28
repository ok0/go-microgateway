package lychgate

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ok0/lychgate/internal/upstream"
)

func adminGet(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

func TestAdminHandler(t *testing.T) {
	hold := holdBackendForAdmin(t)
	secret := "super-secret-signing-key-0123456789"
	doc := routeDoc(hold.URL, "") + fmt.Sprintf(`auth:
  jwt:
    - name: jwt1
      issuer: iss
      audiences: [aud]
      keys:
        - kid: k1
          algorithm: HS256
          secret: %s
  api_keys:
    - name: keys1
      keys:
        - id: k1
          hash: sha256:%s
          consumer: alice
`, secret, strings.Repeat("a", 64))
	g := gwWith(t, doc)
	h := g.AdminHandler()

	if rec := adminGet(t, h, http.MethodGet, "/livez"); rec.Code != http.StatusOK {
		t.Fatalf("/livez status = %d, want 200", rec.Code)
	}

	if rec := adminGet(t, h, http.MethodGet, "/readyz"); rec.Code != http.StatusOK {
		t.Fatalf("/readyz status = %d, want 200", rec.Code)
	}
	g.SetReady(false)
	rec := adminGet(t, h, http.MethodGet, "/readyz")
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "not_ready") {
		t.Fatalf("/readyz after SetReady(false) = %d %q, want 503 not_ready", rec.Code, rec.Body.String())
	}
	if g.Ready() {
		t.Fatal("Ready() = true after SetReady(false)")
	}
	g.SetReady(true)

	rec = adminGet(t, h, http.MethodGet, "/config")
	if rec.Code != http.StatusOK {
		t.Fatalf("/config status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "REDACTED") {
		t.Fatal("/config body lacks REDACTED")
	}
	if strings.Contains(rec.Body.String(), secret) {
		t.Fatal("/config body leaks the JWT secret")
	}
	if got := g.Config().Auth.JWT[0].Keys[0].Secret.Reveal(); got != secret {
		t.Fatalf("g.Config() secret = %q, want the original", got)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/hold", nil))
	}()
	var list []upstream.Status
	eventually(t, func() bool {
		rec := adminGet(t, h, http.MethodGet, "/upstreams")
		list = nil
		if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
			t.Fatal(err)
		}
		return len(list) == 1 && len(list[0].Hosts) == 1 && list[0].Hosts[0].InFlight == 1
	})
	host := list[0].Hosts[0]
	if list[0].Name != "up" || host.URL != hold.URL || !host.Healthy || host.Ejected || host.BreakerState != "closed" || host.Weight != 1 {
		t.Fatalf("upstream status = %+v", list[0])
	}
	close(hold.release)
	<-done
	eventually(t, func() bool {
		rec := adminGet(t, h, http.MethodGet, "/upstreams")
		var l []upstream.Status
		if err := json.Unmarshal(rec.Body.Bytes(), &l); err != nil {
			t.Fatal(err)
		}
		return len(l) == 1 && len(l[0].Hosts) == 1 && l[0].Hosts[0].InFlight == 0
	})

	rec = adminGet(t, h, http.MethodGet, "/metrics")
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("/metrics Content-Type = %q, want text/plain", ct)
	}

	if rec := adminGet(t, h, http.MethodPost, "/livez"); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", rec.Code)
	}
	if rec := adminGet(t, h, http.MethodGet, "/unknown"); rec.Code != http.StatusNotFound {
		t.Fatalf("/unknown status = %d, want 404", rec.Code)
	}
}

func TestAdminPathsNotOnDataHandler(t *testing.T) {
	be := backend(t, "b")
	apiOnly := "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: api
    match:
      path: /api
    upstream: up
`, be.URL)
	g := gwWith(t, apiOnly)
	for _, path := range []string{"/metrics", "/config", "/readyz"} {
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want 404", path, rec.Code)
		}
		if e := decodeErr(t, rec.Result()); e.Code != "route_not_found" {
			t.Fatalf("%s code = %q, want route_not_found", path, e.Code)
		}
	}

	catchAll := gw(t, oneRouteDoc("", be.URL))
	_, e := serve(t, catchAll, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if e.Path != "/metrics" {
		t.Fatalf("backend path = %q, want /metrics", e.Path)
	}
}

type holdServer struct {
	URL     string
	release chan struct{}
}

func holdBackendForAdmin(t *testing.T) *holdServer {
	t.Helper()
	release := make(chan struct{})
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}\n"))
	}))
	t.Cleanup(be.Close)
	return &holdServer{URL: be.URL, release: release}
}
