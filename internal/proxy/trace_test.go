package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"slices"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
)

func TestAttemptTransportTrace(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()
	up := newUpstream(t, srv.URL, clock.NewFake(time.Unix(0, 0)))
	var steps []string
	tr := &AttemptTransport{Upstream: up, Trace: func(s string) { steps = append(steps, s) }}
	req, err := http.NewRequest(http.MethodGet, "http://upstream.invalid/", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if want := []string{StepLoadBalancer, StepUpstream}; !slices.Equal(steps, want) {
		t.Fatalf("steps = %v, want %v", steps, want)
	}
}

func TestNewFlowTrace(t *testing.T) {
	up := newUpstream(t, "http://127.0.0.1:9001", nil)
	at := func(h http.Handler) *AttemptTransport {
		return h.(*httputil.ReverseProxy).Transport.(*AttemptTransport)
	}
	if at(New(RouteSpec{Name: "r"}, up, Deps{})).Trace != nil {
		t.Fatal("Trace is set without Deps.FlowTrace")
	}
	var got []string
	f := func(s string) { got = append(got, s) }
	tr := at(New(RouteSpec{Name: "r"}, up, Deps{FlowTrace: f})).Trace
	if tr == nil {
		t.Fatal("Trace is nil with Deps.FlowTrace set")
	}
	tr("x")
	if !slices.Equal(got, []string{"x"}) {
		t.Fatalf("got = %v, want [x]", got)
	}
}
