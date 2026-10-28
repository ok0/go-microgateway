package ratelimit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/memstore"
	"github.com/ok0/lychgate/store"
)

type takeCall struct {
	key   string
	limit store.Limit
}

type recStore struct {
	calls []takeCall
	dec   store.Decision
	err   error
}

func (s *recStore) Close() error { return nil }

func (s *recStore) Take(_ context.Context, key string, l store.Limit) (store.Decision, error) {
	s.calls = append(s.calls, takeCall{key: key, limit: l})
	return s.dec, s.err
}

var tbLimit = store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}

func newClock() *clock.Fake {
	return clock.NewFake(time.Unix(1_700_000_000, 0))
}

func records(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestPolicyID(t *testing.T) {
	sum := sha256.Sum256([]byte("1|5|1000000000|3"))
	want := "api:" + hex.EncodeToString(sum[:])[:8]
	got := PolicyID("api", tbLimit)
	if got != want {
		t.Fatalf("PolicyID = %q, want %q", got, want)
	}
	if len(got) != len("api")+9 {
		t.Fatalf("len(PolicyID) = %d, want %d", len(got), len("api")+9)
	}
	other := tbLimit
	other.Burst = 4
	if PolicyID("api", other) == got {
		t.Fatalf("different limit produced the same PolicyID %q", got)
	}
}

func TestStubReceivesNamespacedKey(t *testing.T) {
	stub := &recStore{dec: store.Decision{Allowed: true, Limit: 3, Remaining: 2, ResetAfter: 200 * time.Millisecond}}
	p := Policy{ID: "api", Limit: tbLimit, Store: stub, Clock: newClock()}
	l := New(p)

	first := l.Take(context.Background(), "ip:203.0.113.7")
	if len(stub.calls) != 1 {
		t.Fatalf("stub calls = %d, want 1", len(stub.calls))
	}
	if got, want := stub.calls[0].key, "api:ip:203.0.113.7"; got != want {
		t.Fatalf("stub key = %q, want %q", got, want)
	}
	if stub.calls[0].limit != tbLimit {
		t.Fatalf("stub limit = %+v, want %+v", stub.calls[0].limit, tbLimit)
	}
	if first.Decision != stub.dec || first.Degraded {
		t.Fatalf("Take = %+v, want decision %+v not degraded", first, stub.dec)
	}

	second := l.Take(context.Background(), "ip:203.0.113.7")
	if len(stub.calls) != 2 || stub.calls[1] != stub.calls[0] {
		t.Fatalf("second call = %+v, want identical to %+v", stub.calls, stub.calls[0])
	}
	if second != first {
		t.Fatalf("second Take = %+v, want %+v", second, first)
	}
}

func TestLongKeyHashed(t *testing.T) {
	stub := &recStore{dec: store.Decision{Allowed: true}}
	l := New(Policy{ID: "api", Limit: tbLimit, Store: stub, Clock: newClock()})

	long := strings.Repeat("a", MaxKeyBytes+1)
	l.Take(context.Background(), long)
	sum := sha256.Sum256([]byte(long))
	if got, want := stub.calls[0].key, "api:"+hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("hashed key = %q, want %q", got, want)
	}

	exact := strings.Repeat("b", MaxKeyBytes)
	l.Take(context.Background(), exact)
	if got, want := stub.calls[1].key, "api:"+exact; got != want {
		t.Fatalf("128-byte key = %q, want unchanged %q", got, want)
	}
}

func TestStoreErrorFailsOpen(t *testing.T) {
	clk := newClock()
	stub := &recStore{err: fmt.Errorf("boom: %w", store.ErrUnavailable)}
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	l := New(Policy{ID: "api", Limit: tbLimit, Store: stub, Logger: logger, Clock: clk})

	for i := 0; i < 5; i++ {
		res := l.Take(context.Background(), "ip:203.0.113.7")
		if !res.Degraded {
			t.Fatalf("Take %d: Degraded = false, want true", i)
		}
		if res.Decision != (store.Decision{}) {
			t.Fatalf("Take %d: Decision = %+v, want zero", i, res.Decision)
		}
	}
	recs := records(t, &buf)
	if len(recs) != 1 {
		t.Fatalf("records after 5 failures at one instant = %d, want 1", len(recs))
	}
	if recs[0]["level"] != "ERROR" {
		t.Fatalf("level = %v, want ERROR", recs[0]["level"])
	}
	if recs[0]["policy"] != "api" {
		t.Fatalf("policy attr = %v, want api", recs[0]["policy"])
	}
	if recs[0]["suppressed"] != float64(0) {
		t.Fatalf("first suppressed = %v, want 0", recs[0]["suppressed"])
	}

	clk.Advance(time.Second)
	l.Take(context.Background(), "ip:203.0.113.7")
	recs = records(t, &buf)
	if len(recs) != 2 {
		t.Fatalf("records after second window = %d, want 2", len(recs))
	}
	if recs[1]["suppressed"] != float64(4) {
		t.Fatalf("second suppressed = %v, want 4", recs[1]["suppressed"])
	}
	if recs[1]["policy"] != "api" || recs[1]["level"] != "ERROR" {
		t.Fatalf("second record = %v", recs[1])
	}
}

func TestStoreErrorWithoutLoggerDoesNotPanic(t *testing.T) {
	stub := &recStore{err: store.ErrUnavailable}
	l := New(Policy{ID: "api", Limit: tbLimit, Store: stub})
	if res := l.Take(context.Background(), "k"); !res.Degraded {
		t.Fatalf("Degraded = false, want true")
	}
}

func TestContextErrorNoLog(t *testing.T) {
	for _, cerr := range []error{context.Canceled, context.DeadlineExceeded} {
		clk := newClock()
		stub := &recStore{err: fmt.Errorf("take: %w", cerr)}
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, nil))
		l := New(Policy{ID: "api", Limit: tbLimit, Store: stub, Logger: logger, Clock: clk})

		res := l.Take(context.Background(), "k")
		if !res.Degraded {
			t.Fatalf("%v: Degraded = false, want true", cerr)
		}
		if n := len(records(t, &buf)); n != 0 {
			t.Fatalf("%v: log records = %d, want 0", cerr, n)
		}
	}
}

func TestTwoLimitersSharedStore(t *testing.T) {
	clk := newClock()
	rl := memstore.NewRateLimiter(memstore.RateLimiterOptions{MaxKeys: 10, Clock: clk})
	t.Cleanup(func() { _ = rl.Close() })
	p := Policy{ID: PolicyID("api", tbLimit), Limit: tbLimit, Store: rl, Clock: clk}
	a := New(p)
	b := New(p)

	allowed := 0
	for i := 0; i < 6; i++ {
		l := a
		if i%2 == 1 {
			l = b
		}
		if l.Take(context.Background(), "ip:203.0.113.7").Decision.Allowed {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("allowed across two limiters on one store = %d, want 3", allowed)
	}
}

func TestTwoLimitersSeparateStores(t *testing.T) {
	clk := newClock()
	for i := 0; i < 2; i++ {
		rl := memstore.NewRateLimiter(memstore.RateLimiterOptions{MaxKeys: 10, Clock: clk})
		t.Cleanup(func() { _ = rl.Close() })
		l := New(Policy{ID: PolicyID("api", tbLimit), Limit: tbLimit, Store: rl, Clock: clk})
		allowed := 0
		for j := 0; j < 6; j++ {
			if l.Take(context.Background(), "ip:203.0.113.7").Decision.Allowed {
				allowed++
			}
		}
		if allowed != 3 {
			t.Fatalf("instance %d admitted %d, want 3", i, allowed)
		}
	}
}

func TestNewFillsNilLoggerAndClock(t *testing.T) {
	stub := &recStore{}
	l := New(Policy{ID: "api", Limit: tbLimit, Store: stub})
	if l.p.Logger == nil {
		t.Fatalf("nil Logger was not replaced")
	}
	if l.p.Clock == nil {
		t.Fatalf("nil Clock was not replaced")
	}
}
