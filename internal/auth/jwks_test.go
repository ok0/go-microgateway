package auth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/authkey"
	"github.com/ok0/lychgate/internal/clock"
)

const (
	testJWKSURL      = "https://u:pw@idp.example/jwks?token=s3cret"
	testDisplayURL   = "https://idp.example/jwks"
	testTTL          = 10 * time.Minute
	testMinRefresh   = time.Minute
	testFetchTimeout = 5 * time.Second
)

func testJWKSConfig() JWKSConfig {
	return JWKSConfig{
		Provider:           "idp",
		URL:                testJWKSURL,
		DisplayURL:         testDisplayURL,
		Algorithms:         authkey.Algorithms,
		TTL:                testTTL,
		MinRefreshInterval: testMinRefresh,
		Timeout:            testFetchTimeout,
	}
}

type refreshCount struct {
	mu   sync.Mutex
	ok   int
	fail int
}

func (r *refreshCount) record(ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ok {
		r.ok++
	} else {
		r.fail++
	}
}

func (r *refreshCount) get() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ok, r.fail
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func newTestJWKS(t *testing.T, f Fetcher, clk clock.Clock, rc *refreshCount, logs *syncBuffer) *JWKS {
	t.Helper()
	deps := JWKSDeps{Fetcher: f, Clock: clk}
	if rc != nil {
		deps.OnRefresh = rc.record
	}
	if logs != nil {
		deps.Logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	j, err := NewJWKS(testJWKSConfig(), deps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	return j
}

func mustLookup(t *testing.T, j *JWKS, kid string) Key {
	t.Helper()
	k, err := j.Lookup(t.Context(), kid)
	if err != nil {
		t.Fatalf("Lookup(%q): %v", kid, err)
	}
	return k
}

func TestJWKSConstructorErrors(t *testing.T) {
	mf := newMemFetcher(nil)
	cases := []struct {
		name string
		mut  func(*JWKSConfig, *JWKSDeps)
	}{
		{name: "nil fetcher", mut: func(_ *JWKSConfig, d *JWKSDeps) { d.Fetcher = nil }},
		{name: "empty provider", mut: func(c *JWKSConfig, _ *JWKSDeps) { c.Provider = "" }},
		{name: "empty url", mut: func(c *JWKSConfig, _ *JWKSDeps) { c.URL = "" }},
		{name: "no algorithms", mut: func(c *JWKSConfig, _ *JWKSDeps) { c.Algorithms = nil }},
		{name: "zero ttl", mut: func(c *JWKSConfig, _ *JWKSDeps) { c.TTL = 0 }},
		{name: "zero min refresh", mut: func(c *JWKSConfig, _ *JWKSDeps) { c.MinRefreshInterval = 0 }},
		{name: "zero timeout", mut: func(c *JWKSConfig, _ *JWKSDeps) { c.Timeout = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := testJWKSConfig()
			d := JWKSDeps{Fetcher: mf, Clock: clock.NewFake(testNow)}
			tc.mut(&c, &d)
			if _, err := NewJWKS(c, d); err == nil {
				t.Fatal("NewJWKS succeeded, want error")
			}
		})
	}
}

func TestJWKSDefaultsAndAccessors(t *testing.T) {
	ks := loadKeyset()
	mf := newMemFetcher(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)))
	j, err := NewJWKS(testJWKSConfig(), JWKSDeps{Fetcher: mf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	if k := mustLookup(t, j, "rs256"); k.Algorithm != "RS256" {
		t.Fatalf("algorithm = %q", k.Algorithm)
	}

	keys, err := j.Keys(t.Context())
	if err != nil || len(keys) != 1 {
		t.Fatalf("Keys = %v, %v", keys, err)
	}
	keys[0].ID = "mutated"
	if k := mustLookup(t, j, "rs256"); k.ID != "rs256" {
		t.Fatal("Keys returned an alias of internal state")
	}
}

func TestJWKSLazyFirstFetch(t *testing.T) {
	ks := loadKeyset()
	clk := clock.NewFake(testNow)
	mf := newMemFetcher(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)))
	j := newTestJWKS(t, mf, clk, nil, nil)
	if n := mf.count(); n != 0 {
		t.Fatalf("fetches at construction = %d, want 0", n)
	}
	mustLookup(t, j, "rs256")
	if n := mf.count(); n != 1 {
		t.Fatalf("fetches after first lookup = %d, want 1", n)
	}
}

func TestJWKSTTL(t *testing.T) {
	ks := loadKeyset()
	clk := clock.NewFake(testNow)
	mf := newMemFetcher(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)))
	j := newTestJWKS(t, mf, clk, nil, nil)

	mustLookup(t, j, "rs256")
	clk.Advance(testTTL - time.Second)
	mustLookup(t, j, "rs256")
	if n := mf.count(); n != 1 {
		t.Fatalf("fetches before TTL = %d, want 1", n)
	}
	clk.Advance(time.Second)
	mustLookup(t, j, "rs256")
	if n := mf.count(); n != 2 {
		t.Fatalf("fetches after TTL = %d, want 2", n)
	}
}

func TestJWKSRotationForcedRefresh(t *testing.T) {
	ks := loadKeyset()
	rs := jwk("rs256", "RS256", &ks.rsa1.PublicKey)
	rs2 := jwk("rs2", "RS256", &ks.rsa2.PublicKey)
	mf := newMemFetcher(jwksBytes(rs))
	clk := clock.NewFake(testNow)
	j := newTestJWKS(t, mf, clk, nil, nil)
	ctx := t.Context()

	mustLookup(t, j, "rs256")
	mf.set(jwksBytes(rs, rs2), nil)

	if _, err := j.Lookup(ctx, "rs2"); !errors.Is(err, ErrUnknownKID) {
		t.Fatalf("unknown kid within cooldown: err = %v, want ErrUnknownKID", err)
	}
	if n := mf.count(); n != 1 {
		t.Fatalf("fetches within cooldown = %d, want 1", n)
	}

	clk.Advance(testMinRefresh)
	mustLookup(t, j, "rs2")
	if n := mf.count(); n != 2 {
		t.Fatalf("fetches after cooldown = %d, want 2", n)
	}

	mf.set(jwksBytes(rs2), nil)
	clk.Advance(testMinRefresh)
	if _, err := j.Lookup(ctx, "missing"); !errors.Is(err, ErrUnknownKID) {
		t.Fatalf("missing kid: err = %v, want ErrUnknownKID", err)
	}
	if n := mf.count(); n != 3 {
		t.Fatalf("fetches for missing kid = %d, want 3", n)
	}
	if _, err := j.Lookup(ctx, "rs256"); !errors.Is(err, ErrUnknownKID) {
		t.Fatalf("removed kid: err = %v, want ErrUnknownKID", err)
	}
	if n := mf.count(); n != 3 {
		t.Fatalf("fetches for removed kid within cooldown = %d, want 3", n)
	}
}

func TestJWKSSingleflight(t *testing.T) {
	ks := loadKeyset()
	mf := newMemFetcher(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)))
	block := make(chan struct{})
	mf.setBlock(block)
	j := newTestJWKS(t, mf, clock.NewFake(testNow), nil, nil)
	ctx := t.Context()

	const n = 100
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			_, err := j.Lookup(ctx, "rs256")
			errs <- err
		}()
	}
	waitWaiters(j, n)
	close(block)
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("Lookup: %v", err)
		}
	}
	if c := mf.count(); c != 1 {
		t.Fatalf("fetches = %d, want 1", c)
	}
}

func TestJWKSWaiterCancel(t *testing.T) {
	ks := loadKeyset()
	mf := newMemFetcher(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)))
	block := make(chan struct{})
	mf.setBlock(block)
	j := newTestJWKS(t, mf, clock.NewFake(testNow), nil, nil)

	ctxA, cancelA := context.WithCancel(t.Context())
	defer cancelA()
	aErr := make(chan error, 1)
	go func() {
		_, err := j.Lookup(ctxA, "rs256")
		aErr <- err
	}()
	bErr := make(chan error, 1)
	go func() {
		_, err := j.Lookup(t.Context(), "rs256")
		bErr <- err
	}()
	waitWaiters(j, 2)

	cancelA()
	if err := <-aErr; !errors.Is(err, ErrKeysUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter err = %v", err)
	}
	close(block)
	if err := <-bErr; err != nil {
		t.Fatalf("remaining waiter: %v", err)
	}
	if c := mf.count(); c != 1 {
		t.Fatalf("fetches = %d, want 1", c)
	}
}

func TestJWKSFailureKeepsLastGood(t *testing.T) {
	ks := loadKeyset()
	good := jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey))
	cases := []struct {
		name string
		fail func(*memFetcher)
	}{
		{name: "fetch error", fail: func(mf *memFetcher) { mf.set(nil, errTestFetch) }},
		{name: "invalid json", fail: func(mf *memFetcher) { mf.set([]byte("{"), nil) }},
		{name: "only oct keys", fail: func(mf *memFetcher) {
			mf.set([]byte(`{"keys":[{"kty":"oct","kid":"o","k":"AAAA"}]}`), nil)
		}},
		{name: "fetcher panics", fail: func(mf *memFetcher) { mf.setPanic() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mf := newMemFetcher(good)
			rc := &refreshCount{}
			clk := clock.NewFake(testNow)
			j := newTestJWKS(t, mf, clk, rc, nil)
			mustLookup(t, j, "rs256")

			tc.fail(mf)
			clk.Advance(testTTL)
			mustLookup(t, j, "rs256")
			if n := mf.count(); n != 2 {
				t.Fatalf("fetches after failed refresh = %d, want 2", n)
			}
			if ok, fail := rc.get(); ok != 1 || fail != 1 {
				t.Fatalf("refresh counts ok=%d fail=%d, want 1/1", ok, fail)
			}

			mustLookup(t, j, "rs256")
			if n := mf.count(); n != 2 {
				t.Fatalf("fetches before cooldown = %d, want 2", n)
			}

			clk.Advance(testMinRefresh)
			mustLookup(t, j, "rs256")
			if n := mf.count(); n != 3 {
				t.Fatalf("fetches after cooldown = %d, want 3", n)
			}
		})
	}
}

func TestJWKSTimeout(t *testing.T) {
	ks := loadKeyset()
	mf := newMemFetcher(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)))
	mf.setBlock(make(chan struct{}))
	clk := clock.NewFake(testNow)
	j := newTestJWKS(t, mf, clk, nil, nil)

	errc := make(chan error, 1)
	go func() {
		_, err := j.Lookup(t.Context(), "rs256")
		errc <- err
	}()
	clk.BlockUntil(1)
	clk.Advance(testFetchTimeout)
	if err := <-errc; !errors.Is(err, ErrKeysUnavailable) {
		t.Fatalf("Lookup after timeout: err = %v, want ErrKeysUnavailable", err)
	}
	<-mf.canceled
}

func TestJWKSDownAtStart(t *testing.T) {
	ks := loadKeyset()
	mf := newMemFetcher(nil)
	mf.set(nil, errTestFetch)
	clk := clock.NewFake(testNow)
	j := newTestJWKS(t, mf, clk, nil, nil)
	ctx := t.Context()

	if _, err := j.Lookup(ctx, "rs256"); !errors.Is(err, ErrKeysUnavailable) {
		t.Fatalf("first lookup: err = %v, want ErrKeysUnavailable", err)
	}
	if _, err := j.Lookup(ctx, "rs256"); !errors.Is(err, ErrKeysUnavailable) {
		t.Fatalf("second lookup: err = %v, want ErrKeysUnavailable", err)
	}
	if n := mf.count(); n != 1 {
		t.Fatalf("fetches during cooldown = %d, want 1", n)
	}

	mf.set(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)), nil)
	clk.Advance(testMinRefresh)
	mustLookup(t, j, "rs256")
	if n := mf.count(); n != 2 {
		t.Fatalf("fetches after recovery = %d, want 2", n)
	}
}

func TestJWKSClose(t *testing.T) {
	ks := loadKeyset()
	mf := newMemFetcher(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)))
	mf.setBlock(make(chan struct{}))
	j := newTestJWKS(t, mf, clock.NewFake(testNow), nil, nil)
	ctx := t.Context()

	errc := make(chan error, 1)
	go func() {
		_, err := j.Lookup(ctx, "rs256")
		errc <- err
	}()
	waitWaiters(j, 1)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	<-mf.canceled
	if err := <-errc; !errors.Is(err, ErrKeysUnavailable) {
		t.Fatalf("in-flight waiter: err = %v", err)
	}
	if _, err := j.Lookup(ctx, "rs256"); !errors.Is(err, ErrKeysUnavailable) {
		t.Fatalf("Lookup after Close: err = %v", err)
	}
	if _, err := j.Keys(ctx); !errors.Is(err, ErrKeysUnavailable) {
		t.Fatalf("Keys after Close: err = %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestJWKSKidless(t *testing.T) {
	ks := loadKeyset()
	clk := clock.NewFake(testNow)

	mf := newMemFetcher(jwksBytes(jwk("", "RS256", &ks.rsa1.PublicKey)))
	single := newTestJWKS(t, mf, clk, nil, nil)
	if k := mustLookup(t, single, ""); k.Algorithm != "RS256" {
		t.Fatalf("kid-less lookup algorithm = %q", k.Algorithm)
	}
	if _, err := single.Lookup(t.Context(), "x"); !errors.Is(err, ErrUnknownKID) {
		t.Fatalf("unknown kid: err = %v, want ErrUnknownKID", err)
	}

	mf2 := newMemFetcher(jwksBytes(jwk("a", "RS256", &ks.rsa1.PublicKey), jwk("b", "RS256", &ks.rsa2.PublicKey)))
	two := newTestJWKS(t, mf2, clk, nil, nil)
	if _, err := two.Lookup(t.Context(), ""); !errors.Is(err, ErrUnknownKID) {
		t.Fatalf("kid-less lookup with two keys: err = %v, want ErrUnknownKID", err)
	}
}

func TestJWKSDefaultsWithRealClock(t *testing.T) {
	ks := loadKeyset()
	mf := newMemFetcher(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)))
	j, err := NewJWKS(testJWKSConfig(), JWKSDeps{Fetcher: mf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	if k := mustLookup(t, j, "rs256"); k.ID != "rs256" {
		t.Fatalf("kid = %q", k.ID)
	}
}

func TestJWKSLogScrubbed(t *testing.T) {
	ks := loadKeyset()
	mf := newMemFetcher(nil)
	mf.set(nil, fmt.Errorf("dial %s: connection refused", testJWKSURL))
	logs := &syncBuffer{}
	clk := clock.NewFake(testNow)
	j := newTestJWKS(t, mf, clk, nil, logs)

	if _, err := j.Lookup(t.Context(), "rs256"); !errors.Is(err, ErrKeysUnavailable) {
		t.Fatalf("Lookup: err = %v, want ErrKeysUnavailable", err)
	}
	out := logs.String()
	for _, secret := range []string{"s3cret", "pw", "u:"} {
		if strings.Contains(out, secret) {
			t.Fatalf("log output leaks %q: %s", secret, out)
		}
	}
	for _, want := range []string{"jwks refresh failed", "REDACTED", "keeping_last_good=false", testDisplayURL} {
		if !strings.Contains(out, want) {
			t.Fatalf("log output missing %q: %s", want, out)
		}
	}

	mf.set(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)), nil)
	clk.Advance(testMinRefresh)
	mustLookup(t, j, "rs256")
	if out := logs.String(); !strings.Contains(out, "jwks refreshed") || !strings.Contains(out, "keys=1") {
		t.Fatalf("success log missing: %s", out)
	}
}
