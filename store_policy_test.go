package lychgate

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/httperr"
	"github.com/ok0/lychgate/internal/teststore"
	"github.com/ok0/lychgate/store"
)

var spDown = fmt.Errorf("store unreachable: %w", store.ErrUnavailable)

const spStoreTimeout = 200 * time.Millisecond

func spSeries(t *testing.T, g *Gateway, name string, labels ...string) (float64, bool) {
	t.Helper()
	prefix := series(name, labels...) + " "
	for _, line := range strings.Split(metricsText(t, g), "\n") {
		if rest, ok := strings.CutPrefix(line, prefix); ok {
			v, err := strconv.ParseFloat(rest, 64)
			if err != nil {
				t.Fatal(err)
			}
			return v, true
		}
	}
	return 0, false
}

func spMustSeries(t *testing.T, g *Gateway, name string, labels ...string) float64 {
	t.Helper()
	v, ok := spSeries(t, g, name, labels...)
	if !ok {
		t.Fatalf("series %s is missing", series(name, labels...))
	}
	return v
}

func spExpectValue(t *testing.T, g *Gateway, want float64, name string, labels ...string) {
	t.Helper()
	if got := spMustSeries(t, g, name, labels...); got != want {
		t.Fatalf("%s = %v, want %v", series(name, labels...), got, want)
	}
}

func spRateLimit(policy string, perHour int, timeout time.Duration) string {
	return fmt.Sprintf("    rate_limit:\n      rate: %d\n      per: 1h\n      store: shared\n      on_store_error: %s\n      store_timeout: %s\n",
		perHour, policy, timeout)
}

func spCanceledGet(t *testing.T, g *Gateway, path, remote string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
	req.RemoteAddr = remote
	g.ServeHTTP(httptest.NewRecorder(), req)
}

func spHang(t *testing.T, fake *clock.Fake, h *teststore.Handle, g *Gateway, path, remote string, timeout time.Duration) *httptest.ResponseRecorder {
	t.Helper()
	ch := stAsync(t, g, path, remote)
	h.WaitBlocked(1)
	fake.BlockUntilDue(1, timeout)
	before := fake.Now()
	fake.Advance(timeout)
	if elapsed := fake.Now().Sub(before); elapsed > timeout+100*time.Millisecond {
		t.Fatalf("request elapsed %v of fake time, want <= %v", elapsed, timeout+100*time.Millisecond)
	}
	return <-ch
}

func spExpectDegraded(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("RateLimit-Limit"); got != "" {
		t.Fatalf("RateLimit-Limit = %q, want no rate limit headers", got)
	}
}

func spExpectUnavailable(t *testing.T, rec *httptest.ResponseRecorder, retryAfter string) {
	t.Helper()
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != retryAfter {
		t.Fatalf("Retry-After = %q, want %q", got, retryAfter)
	}
	if e := decodeErr(t, rec.Result()); e.Code != httperr.CodeRateLimitUnavailable {
		t.Fatalf("code = %q, want %q", e.Code, httperr.CodeRateLimitUnavailable)
	}
}

func TestStorePolicyTable(t *testing.T) {
	modes := []struct {
		name string
		mode teststore.Mode
		hang bool
	}{
		{"fail", teststore.Fail, false},
		{"hang", teststore.Hang, true},
		{"hang_ignore_context", teststore.HangIgnoreContext, true},
	}
	for _, m := range modes {
		for _, policy := range []string{"allow", "deny", "local"} {
			t.Run(m.name+"/"+policy, func(t *testing.T) {
				const client = "192.0.2.30:4000"
				fake := rlFake()
				shared := stShared(t, fake)
				shared.SetMode(m.mode, spDown)
				n := newMemNetwork()
				be, hits := rlBackend(t, n)
				var factoryCalls atomic.Int64
				doc := stDoc(be.URL,
					stStore("shared", "    guard:\n      consecutive_failures: 1\n"),
					stRoute("rl", spRateLimit(policy, 3, spStoreTimeout)))
				g := n.gateway(t, doc, withClock(fake), stOption(shared, &factoryCalls))
				handles := shared.Handles()
				if len(handles) != 1 {
					t.Fatalf("handles = %d, want 1", len(handles))
				}
				if factoryCalls.Load() != 1 {
					t.Fatalf("factory calls = %d, want 1", factoryCalls.Load())
				}
				t.Cleanup(handles[0].Release)

				var first *httptest.ResponseRecorder
				if m.hang {
					first = spHang(t, fake, handles[0], g, "/rl", client, spStoreTimeout)
				} else {
					first = stDo(t, g, "/rl", client)
				}
				if got := len(shared.Calls()); got != 1 {
					t.Fatalf("store calls after first request = %d, want 1", got)
				}
				second := stDo(t, g, "/rl", client)
				third := stDo(t, g, "/rl", client)
				if got := len(shared.Calls()); got != 1 {
					t.Fatalf("store calls with guard open = %d, want 1", got)
				}

				switch policy {
				case "allow":
					spExpectDegraded(t, first)
					spExpectDegraded(t, second)
					spExpectDegraded(t, third)
					if got := hits.Load(); got != 3 {
						t.Fatalf("backend requests = %d, want 3", got)
					}
				case "deny":
					spExpectUnavailable(t, first, "2")
					spExpectUnavailable(t, second, "2")
					spExpectUnavailable(t, third, "2")
					if got := hits.Load(); got != 0 {
						t.Fatalf("backend requests = %d, want 0", got)
					}
				case "local":
					rlExpectRemaining(t, first, "2")
					rlExpectRemaining(t, second, "1")
					rlExpectRemaining(t, third, "0")
					rlExpectLimited(t, stDo(t, g, "/rl", client), "1200")
					if got := hits.Load(); got != 3 {
						t.Fatalf("backend requests = %d, want 3", got)
					}
				}

				wantFallbacks := 3
				if policy == "local" {
					wantFallbacks = 4
				}
				spExpectValue(t, g, 0, "lychgate_store_available", "store", "shared")
				spExpectValue(t, g, 1, "lychgate_store_errors_total", "store", "shared", "op", "take")
				spExpectValue(t, g, float64(wantFallbacks), "lychgate_ratelimit_store_fallback_total", "route", "rl", "policy", policy)
				rsCheckExposition(t, metricsText(t, g))
			})
		}
	}
}

func TestStoreRecovery(t *testing.T) {
	const client = "192.0.2.40:4000"
	fake := rlFake()
	shared := stShared(t, fake)
	n := newMemNetwork()
	be, _ := rlBackend(t, n)
	var factoryCalls atomic.Int64
	doc := stDoc(be.URL,
		stStore("shared", "    guard:\n      consecutive_failures: 3\n      open_timeout: 2s\n"),
		stRoute("rl", "    rate_limit:\n      rate: 10\n      per: 1h\n      store: shared\n      on_store_error: local\n      store_timeout: 200ms\n"))
	g := n.gateway(t, doc, withClock(fake), stOption(shared, &factoryCalls))
	handles := shared.Handles()
	if len(handles) != 1 {
		t.Fatalf("handles = %d, want 1", len(handles))
	}

	spExpectValue(t, g, 1, "lychgate_store_available", "store", "shared")
	spExpectValue(t, g, 0, "lychgate_store_errors_total", "store", "shared", "op", "take")
	spExpectValue(t, g, 0, "lychgate_ratelimit_store_fallback_total", "route", "rl", "policy", "local")
	rsCheckExposition(t, metricsText(t, g))

	shared.SetMode(teststore.Fail, spDown)
	for i := range 3 {
		rlExpectRemaining(t, stDo(t, g, "/rl", client), strconv.Itoa(9-i))
	}
	spExpectValue(t, g, 0, "lychgate_store_available", "store", "shared")
	spExpectValue(t, g, 3, "lychgate_store_errors_total", "store", "shared", "op", "take")
	spExpectValue(t, g, 3, "lychgate_ratelimit_store_fallback_total", "route", "rl", "policy", "local")
	if got := len(shared.Calls()); got != 3 {
		t.Fatalf("store calls = %d, want 3", got)
	}

	spCanceledGet(t, g, "/rl", client)
	spExpectValue(t, g, 0, "lychgate_store_available", "store", "shared")
	spExpectValue(t, g, 3, "lychgate_store_errors_total", "store", "shared", "op", "take")
	spExpectValue(t, g, 3, "lychgate_ratelimit_store_fallback_total", "route", "rl", "policy", "local")
	if got := len(shared.Calls()); got != 3 {
		t.Fatalf("store calls after canceled request = %d, want 3", got)
	}

	for i := range 2 {
		rlExpectRemaining(t, stDo(t, g, "/rl", client), strconv.Itoa(6-i))
	}
	spExpectValue(t, g, 5, "lychgate_ratelimit_store_fallback_total", "route", "rl", "policy", "local")
	spExpectValue(t, g, 3, "lychgate_store_errors_total", "store", "shared", "op", "take")
	if got := len(shared.Calls()); got != 3 {
		t.Fatalf("store calls while open = %d, want 3", got)
	}

	fake.Advance(2 * time.Second)
	shared.SetMode(teststore.Healthy, nil)
	rlExpectRemaining(t, stDo(t, g, "/rl", client), "9")
	if got := len(shared.Calls()); got != 4 {
		t.Fatalf("store calls after one probe = %d, want 4", got)
	}
	spExpectValue(t, g, 1, "lychgate_store_available", "store", "shared")
	spExpectValue(t, g, 3, "lychgate_store_errors_total", "store", "shared", "op", "take")
	spExpectValue(t, g, 5, "lychgate_ratelimit_store_fallback_total", "route", "rl", "policy", "local")

	rlExpectRemaining(t, stDo(t, g, "/rl", client), "8")
	if got := len(shared.Calls()); got != 5 {
		t.Fatalf("store calls after recovery = %d, want 5", got)
	}
	rsCheckExposition(t, metricsText(t, g))
}

func TestStoreSharedGuard(t *testing.T) {
	const client = "192.0.2.50:4000"
	fake := rlFake()
	shared := stShared(t, fake)
	n := newMemNetwork()
	be, _ := rlBackend(t, n)
	var factoryCalls atomic.Int64
	routes := stRoute("a", spRateLimit("deny", 10, 100*time.Millisecond)) +
		stRoute("b", spRateLimit("allow", 10, 300*time.Millisecond)) +
		stRoute("c", "    rate_limit:\n      rate: 10\n      per: 1h\n")
	doc := stDoc(be.URL, stStore("shared", ""), routes)
	g := n.gateway(t, doc, withClock(fake), stOption(shared, &factoryCalls))
	handles := shared.Handles()
	if len(handles) != 1 {
		t.Fatalf("handles = %d, want 1", len(handles))
	}
	h := handles[0]

	shared.SetMode(teststore.Fail, spDown)
	for range 3 {
		spExpectUnavailable(t, stDo(t, g, "/a", client), "2")
	}
	spExpectValue(t, g, 0, "lychgate_store_available", "store", "shared")
	if got := len(shared.Calls()); got != 3 {
		t.Fatalf("store calls = %d, want 3", got)
	}

	spExpectDegraded(t, stDo(t, g, "/b", client))
	if got := len(shared.Calls()); got != 3 {
		t.Fatalf("store calls with guard open = %d, want 3", got)
	}
	rlExpectRemaining(t, stDo(t, g, "/c", client), "9")
	if strings.Contains(metricsText(t, g), `lychgate_ratelimit_store_fallback_total{route="c"`) {
		t.Fatal("memory-store route c has a store fallback series")
	}

	fake.Advance(2 * time.Second)
	shared.SetMode(teststore.Hang, nil)
	probe := stAsync(t, g, "/a", client)
	h.WaitBlocked(1)
	if got := len(shared.Calls()); got != 4 {
		t.Fatalf("store calls with probe in flight = %d, want 4", got)
	}
	spExpectDegraded(t, stDo(t, g, "/b", client))
	rlExpectRemaining(t, stDo(t, g, "/c", client), "8")
	if got := len(shared.Calls()); got != 4 {
		t.Fatalf("store calls while probe in flight = %d, want 4", got)
	}

	h.Release()
	rlExpectRemaining(t, <-probe, "9")
	spExpectValue(t, g, 1, "lychgate_store_available", "store", "shared")

	shared.SetMode(teststore.Healthy, nil)
	rlExpectRemaining(t, stDo(t, g, "/b", client), "9")
	if got := len(shared.Calls()); got != 5 {
		t.Fatalf("store calls after recovery = %d, want 5", got)
	}
	rsCheckExposition(t, metricsText(t, g))
}
