package microgateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/upstream"
)

type fakeResolver struct {
	mu      sync.Mutex
	answers []string
	err     error
}

func (r *fakeResolver) set(answers []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.answers = slices.Clone(answers)
	r.err = nil
}

func (r *fakeResolver) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func (r *fakeResolver) LookupHost(_ context.Context, _ string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return nil, r.err
	}
	return slices.Clone(r.answers), nil
}

func upstreamStatus(t *testing.T, g *Gateway, name string) upstream.Status {
	t.Helper()
	rec := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/upstreams", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/upstreams status = %d, want 200", rec.Code)
	}
	var list []upstream.Status
	if err := json.NewDecoder(rec.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	for _, st := range list {
		if st.Name == name {
			return st
		}
	}
	t.Fatalf("upstream %q not in /upstreams", name)
	return upstream.Status{}
}

func hostByURL(st upstream.Status, rawURL string) upstream.HostStatus {
	for _, h := range st.Hosts {
		if h.URL == rawURL {
			return h
		}
	}
	return upstream.HostStatus{}
}

type backendHandle struct {
	URL    string
	Status atomic.Int32
	Health atomic.Int32
	Hits   atomic.Int64
	Probes atomic.Int64
}

func failingBackend(t *testing.T, n testNetwork, name string) *backendHandle {
	t.Helper()
	b := &backendHandle{}
	b.Status.Store(http.StatusOK)
	b.Health.Store(http.StatusOK)
	srv := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/health" {
			b.Probes.Add(1)
			w.WriteHeader(int(b.Health.Load()))
		} else {
			b.Hits.Add(1)
			w.WriteHeader(int(b.Status.Load()))
		}
		_ = json.NewEncoder(w).Encode(echo{Backend: name, Path: r.URL.Path})
	}))
	b.URL = srv.URL
	return b
}

func waitProbes(t *testing.T, fake *clock.Fake, completed func() float64, want float64, armed int) {
	t.Helper()
	eventually(t, func() bool { return completed() == want })
	fake.BlockUntil(armed)
}
