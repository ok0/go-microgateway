package auth

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/authkey"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testwait"
)

// promptly runs calls concurrently while a fetch of j is blocked and returns their errors. It fails the test as
// soon as the fetch in flight has more than base waiters, that is when a call waits on the fetch instead of returning.
func promptly(t *testing.T, j *JWKS, base int, calls ...func() error) []error {
	t.Helper()
	res := make(chan error, len(calls))
	for _, c := range calls {
		go func() { res <- c() }()
	}
	var errs []error
	joined := false
	testwait.Until(t, fmt.Sprintf("%d calls to return while the fetch is blocked", len(calls)), func() bool {
		for drained := false; !drained; {
			select {
			case err := <-res:
				errs = append(errs, err)
			default:
				drained = true
			}
		}
		if len(errs) == len(calls) {
			return true
		}
		j.mu.Lock()
		defer j.mu.Unlock()
		if j.flight != nil && j.flight.waiters > base {
			joined = true
			return true
		}
		return false
	}, func() string {
		j.mu.Lock()
		defer j.mu.Unlock()
		waiters := 0
		if j.flight != nil {
			waiters = j.flight.waiters
		}
		return fmt.Sprintf("%d of %d calls returned, %d waiters on the in-flight fetch", len(errs), len(calls), waiters)
	})
	if joined {
		t.Fatalf("a call waits on the in-flight fetch: %d of %d returned", len(errs), len(calls))
	}
	return errs
}

func requireNoErrors(t *testing.T, errs []error) {
	t.Helper()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("call returned %v, want nil", err)
		}
	}
}

func repeatCall(n int, call func() error) []func() error {
	calls := make([]func() error, n)
	for i := range calls {
		calls[i] = call
	}
	return calls
}

func lookupCall(t *testing.T, j *JWKS, kid, want string) func() error {
	return func() error {
		k, err := j.Lookup(t.Context(), kid)
		if err != nil {
			return err
		}
		if k.ID != want {
			return fmt.Errorf("lookup(%q) id = %q, want %q", kid, k.ID, want)
		}
		return nil
	}
}

func keysCall(t *testing.T, j *JWKS, n int) func() error {
	return func() error {
		keys, err := j.Keys(t.Context())
		if err != nil {
			return err
		}
		if len(keys) != n {
			return fmt.Errorf("keys returned %d keys, want %d", len(keys), n)
		}
		return nil
	}
}

func verifyCall(t *testing.T, p *JWT, tok string) func() error {
	return func() error {
		_, err := p.Authenticate(bearerReq(t, tok))
		return err
	}
}

func inFlight(t *testing.T, j *JWKS) bool {
	t.Helper()
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.flight != nil
}

func startLookupCtx(ctx context.Context, j *JWKS, kid string) <-chan lookupResult {
	ch := make(chan lookupResult, 1)
	go func() {
		k, err := j.Lookup(ctx, kid)
		ch <- lookupResult{key: k, err: err}
	}()
	return ch
}

// assertMissingCooldown checks an id outside the set: within MinRefreshInterval of the last attempt it is refused
// without a fetch, and once that has passed the lookup starts one.
func assertMissingCooldown(t *testing.T, j *JWKS, mf *memFetcher, clk *clock.Fake, fetches int) {
	t.Helper()
	if _, err := j.Lookup(t.Context(), "missing"); !errors.Is(err, ErrUnknownKID) {
		t.Fatalf("missing kid in cooldown: err = %v, want ErrUnknownKID", err)
	}
	if n := mf.count(); n != fetches {
		t.Fatalf("fetches in cooldown = %d, want %d", n, fetches)
	}
	clk.Advance(testMinRefresh)
	if _, err := j.Lookup(t.Context(), "missing"); !errors.Is(err, ErrUnknownKID) {
		t.Fatalf("missing kid after cooldown: err = %v, want ErrUnknownKID", err)
	}
	if n := mf.count(); n != fetches+1 {
		t.Fatalf("fetches after cooldown = %d, want %d", n, fetches+1)
	}
}

func TestJWKSKnownKIDDuringForcedRefresh(t *testing.T) {
	ks := loadKeyset()
	rs := jwk("rs256", "RS256", &ks.rsa1.PublicKey)
	rs2 := jwk("rs2", "RS256", &ks.rsa2.PublicKey)
	mf := newMemFetcher(jwksBytes(rs))
	clk := clock.NewFake(testNow)
	rc := &refreshCount{}
	j := newTestJWKS(t, mf, clk, rc, nil)
	mustLookup(t, j, "rs256")

	mf.set(jwksBytes(rs, rs2), nil)
	block := make(chan struct{})
	mf.setBlock(block)
	clk.Advance(testMinRefresh)
	unknown := startLookup(t, j, "rs2")
	waitWaiters(t, j, 1)

	p, err := NewJWT(testJWTConfig(authkey.Algorithms), &Combined{JWKS: j}, clk)
	if err != nil {
		t.Fatal(err)
	}
	tok := signToken(t, "RS256", "rs256", ks.rsa1, baseClaims(clk.Now()))
	calls := append(repeatCall(8, verifyCall(t, p, tok)), repeatCall(8, lookupCall(t, j, "rs256", "rs256"))...)
	calls = append(calls, repeatCall(4, keysCall(t, j, 1))...)
	requireNoErrors(t, promptly(t, j, 1, calls...))

	testwait.Until(t, "forced refresh fetching", func() bool { return mf.count() >= 2 }, nil)
	if !inFlight(t, j) {
		t.Fatal("no refresh in flight while the unknown-kid lookup waits")
	}
	if n := mf.count(); n != 2 {
		t.Fatalf("fetches during the forced refresh = %d, want 2", n)
	}

	close(block)
	r, _ := testwait.Recv(t, "unknown-kid lookup after the refresh", unknown, nil)
	if r.err != nil || r.key.ID != "rs2" {
		t.Fatalf("unknown-kid lookup = %q, %v, want rs2", r.key.ID, r.err)
	}
	waitRefreshes(t, rc, 2, 0)
	if n := mf.count(); n != 2 {
		t.Fatalf("fetches after the forced refresh = %d, want 2", n)
	}
}

func TestJWKSKnownKIDDuringDetachedRefresh(t *testing.T) {
	ks := loadKeyset()
	rs := jwk("rs256", "RS256", &ks.rsa1.PublicKey)

	t.Run("ttl", func(t *testing.T) {
		mf := newMemFetcher(jwksBytes(rs))
		clk := clock.NewFake(testNow)
		rc := &refreshCount{}
		j := newTestJWKS(t, mf, clk, rc, nil)
		mustLookup(t, j, "rs256")
		waitRefreshes(t, rc, 1, 0)

		block := make(chan struct{})
		mf.setBlock(block)
		clk.Advance(testTTL)
		calls := append(repeatCall(8, lookupCall(t, j, "rs256", "rs256")), repeatCall(4, keysCall(t, j, 1))...)
		requireNoErrors(t, promptly(t, j, 0, calls...))

		testwait.Until(t, "detached refresh fetching", func() bool { return mf.count() >= 2 }, nil)
		if !inFlight(t, j) {
			t.Fatal("no refresh in flight after the cached set expired")
		}
		if n := mf.count(); n != 2 {
			t.Fatalf("fetches for the expired set = %d, want 2", n)
		}

		close(block)
		waitRefreshes(t, rc, 2, 0)
		mustLookup(t, j, "rs256")
		if n := mf.count(); n != 2 {
			t.Fatalf("fetches after the detached refresh = %d, want 2", n)
		}
	})

	t.Run("retry", func(t *testing.T) {
		mf := newMemFetcher(jwksBytes(rs))
		clk := clock.NewFake(testNow)
		rc := &refreshCount{}
		j := newTestJWKS(t, mf, clk, rc, nil)
		mustLookup(t, j, "rs256")

		mf.set(nil, errTestFetch)
		clk.Advance(testTTL)
		mustLookup(t, j, "rs256")
		waitRefreshes(t, rc, 1, 1)
		if n := mf.count(); n != 2 {
			t.Fatalf("fetches after the failed detached refresh = %d, want 2", n)
		}

		mf.set(jwksBytes(rs), nil)
		block := make(chan struct{})
		mf.setBlock(block)
		clk.Advance(testMinRefresh)
		calls := append(repeatCall(8, lookupCall(t, j, "rs256", "rs256")), repeatCall(4, keysCall(t, j, 1))...)
		requireNoErrors(t, promptly(t, j, 0, calls...))

		testwait.Until(t, "retry refresh fetching", func() bool { return mf.count() >= 3 }, nil)
		if !inFlight(t, j) {
			t.Fatal("no retry in flight after MinRefreshInterval")
		}
		if n := mf.count(); n != 3 {
			t.Fatalf("fetches for the retry = %d, want 3", n)
		}

		close(block)
		waitRefreshes(t, rc, 2, 1)
		mustLookup(t, j, "rs256")
		if n := mf.count(); n != 3 {
			t.Fatalf("fetches after the retry = %d, want 3", n)
		}
	})
}

func TestJWKSFirstLoadWaitsForFetch(t *testing.T) {
	ks := loadKeyset()
	mf := newMemFetcher(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)))
	mf.setBlock(make(chan struct{}))
	clk := clock.NewFake(testNow)
	rc := &refreshCount{}
	j := newTestJWKS(t, mf, clk, rc, nil)

	lookups := []<-chan lookupResult{
		startLookup(t, j, "rs256"),
		startLookup(t, j, "rs256"),
		startLookup(t, j, "rs256"),
	}
	keysErr := make(chan error, 1)
	go func() {
		_, err := j.Keys(t.Context())
		keysErr <- err
	}()
	waitWaiters(t, j, 4)
	testwait.Until(t, "fetch timeout timer", func() bool { return clk.Waiters() == 1 }, nil)

	clk.Advance(testFetchTimeout - time.Nanosecond)
	for _, ch := range lookups {
		assertNoLookupResult(t, ch)
	}
	select {
	case err := <-keysErr:
		t.Fatalf("Keys returned before the timeout: err=%v", err)
	default:
	}

	clk.Advance(time.Nanosecond)
	for _, ch := range lookups {
		r, _ := testwait.Recv(t, "Lookup to return after the timeout", ch, nil)
		if !errors.Is(r.err, ErrKeysUnavailable) {
			t.Fatalf("Lookup after timeout: err = %v, want ErrKeysUnavailable", r.err)
		}
	}
	if err, _ := testwait.Recv(t, "Keys to return after the timeout", keysErr, nil); !errors.Is(err, ErrKeysUnavailable) {
		t.Fatalf("Keys after timeout: err = %v, want ErrKeysUnavailable", err)
	}
	waitRefreshes(t, rc, 0, 1)
	if j.hasKeys() {
		t.Fatal("key set loaded after a timed-out first fetch")
	}
	if n := mf.count(); n != 1 {
		t.Fatalf("fetches after the timeout = %d, want 1", n)
	}

	mf.setBlock(nil)
	clk.Advance(testMinRefresh - testFetchTimeout - time.Nanosecond)
	if _, err := j.Lookup(t.Context(), "rs256"); !errors.Is(err, ErrKeysUnavailable) {
		t.Fatalf("Lookup in cooldown: err = %v, want ErrKeysUnavailable", err)
	}
	if n := mf.count(); n != 1 {
		t.Fatalf("fetches in cooldown = %d, want 1", n)
	}
	clk.Advance(time.Nanosecond)
	mustLookup(t, j, "rs256")
	if n := mf.count(); n != 2 {
		t.Fatalf("fetches after cooldown = %d, want 2", n)
	}
}

// unknownWaitSetup loads the key set rs, moves past MinRefreshInterval so that an unknown id may start a refresh, and
// blocks the refresh that will fetch body.
func unknownWaitSetup(t *testing.T, body []byte) (*JWKS, *memFetcher, *refreshCount, *clock.Fake, chan struct{}) {
	t.Helper()
	ks := loadKeyset()
	mf := newMemFetcher(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)))
	clk := clock.NewFake(testNow)
	rc := &refreshCount{}
	j := newTestJWKS(t, mf, clk, rc, nil)
	mustLookup(t, j, "rs256")
	clk.Advance(testMinRefresh)
	block := make(chan struct{})
	mf.set(body, nil)
	mf.setBlock(block)
	return j, mf, rc, clk, block
}

func TestJWKSUnknownKIDWaitsForRefresh(t *testing.T) {
	ks := loadKeyset()
	rs := jwk("rs256", "RS256", &ks.rsa1.PublicKey)
	rs2 := jwk("rs2", "RS256", &ks.rsa2.PublicKey)
	both := jwksBytes(rs, rs2)
	only := jwksBytes(rs)

	t.Run("released with kid", func(t *testing.T) {
		j, mf, rc, _, block := unknownWaitSetup(t, both)
		unknown := startLookupCtx(t.Context(), j, "rs2")
		waitWaiters(t, j, 1)

		close(block)
		r, _ := testwait.Recv(t, "unknown-kid lookup after the refresh", unknown, nil)
		if r.err != nil || r.key.ID != "rs2" {
			t.Fatalf("unknown-kid lookup = %q, %v, want rs2", r.key.ID, r.err)
		}
		waitRefreshes(t, rc, 2, 0)
		if n := mf.count(); n != 2 {
			t.Fatalf("fetches = %d, want 2", n)
		}
	})

	t.Run("released without kid", func(t *testing.T) {
		j, mf, rc, _, block := unknownWaitSetup(t, only)
		unknown := startLookupCtx(t.Context(), j, "rs2")
		waitWaiters(t, j, 1)

		close(block)
		r, _ := testwait.Recv(t, "unknown-kid lookup after the refresh", unknown, nil)
		if !errors.Is(r.err, ErrUnknownKID) {
			t.Fatalf("unknown-kid lookup err = %v, want ErrUnknownKID", r.err)
		}
		waitRefreshes(t, rc, 2, 0)
		if n := mf.count(); n != 2 {
			t.Fatalf("fetches = %d, want 2", n)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		j, mf, rc, clk, _ := unknownWaitSetup(t, both)
		unknown := startLookupCtx(t.Context(), j, "rs2")
		waitWaiters(t, j, 1)

		testwait.Until(t, "fetch timeout timer", func() bool { return clk.Waiters() == 1 }, nil)
		clk.Advance(testFetchTimeout - time.Nanosecond)
		assertNoLookupResult(t, unknown)
		clk.Advance(time.Nanosecond)
		r, _ := testwait.Recv(t, "unknown-kid lookup after the timeout", unknown, nil)
		if !errors.Is(r.err, ErrUnknownKID) {
			t.Fatalf("unknown-kid lookup err = %v, want ErrUnknownKID", r.err)
		}
		waitRefreshes(t, rc, 1, 1)
		mustLookup(t, j, "rs256")
		if n := mf.count(); n != 2 {
			t.Fatalf("fetches after the timed-out refresh = %d, want 2", n)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		j, mf, rc, _, block := unknownWaitSetup(t, both)
		ctx, cancel := context.WithCancel(t.Context())
		unknown := startLookupCtx(ctx, j, "rs2")
		waitWaiters(t, j, 1)

		cancel()
		r, _ := testwait.Recv(t, "cancelled unknown-kid lookup to return", unknown, nil)
		if !errors.Is(r.err, ErrKeysUnavailable) || !errors.Is(r.err, context.Canceled) {
			t.Fatalf("cancelled unknown-kid lookup err = %v", r.err)
		}
		requireNoErrors(t, promptly(t, j, 0, lookupCall(t, j, "rs256", "rs256")))

		close(block)
		waitRefreshes(t, rc, 2, 0)
		mustLookup(t, j, "rs2")
		if n := mf.count(); n != 2 {
			t.Fatalf("fetches = %d, want 2", n)
		}
	})
}

func TestJWKSUnknownKIDInCooldown(t *testing.T) {
	ks := loadKeyset()
	mf := newMemFetcher(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)))
	clk := clock.NewFake(testNow)
	j := newTestJWKS(t, mf, clk, nil, nil)
	ctx := t.Context()
	mustLookup(t, j, "rs256")

	for i := range 50 {
		if _, err := j.Lookup(ctx, "missing"); !errors.Is(err, ErrUnknownKID) {
			t.Fatalf("lookup %d in cooldown: err = %v, want ErrUnknownKID", i, err)
		}
	}
	if n := mf.count(); n != 1 {
		t.Fatalf("fetches in cooldown = %d, want 1", n)
	}
	if inFlight(t, j) {
		t.Fatal("refresh started within MinRefreshInterval")
	}

	clk.Advance(testMinRefresh)
	if _, err := j.Lookup(ctx, "missing"); !errors.Is(err, ErrUnknownKID) {
		t.Fatalf("missing kid after cooldown: err = %v, want ErrUnknownKID", err)
	}
	if n := mf.count(); n != 2 {
		t.Fatalf("fetches after cooldown = %d, want 2", n)
	}
	for i := range 50 {
		if _, err := j.Lookup(ctx, "missing"); !errors.Is(err, ErrUnknownKID) {
			t.Fatalf("lookup %d after refresh: err = %v, want ErrUnknownKID", i, err)
		}
	}
	if n := mf.count(); n != 2 {
		t.Fatalf("fetches within the new cooldown = %d, want 2", n)
	}
	if inFlight(t, j) {
		t.Fatal("refresh started within MinRefreshInterval of the last attempt")
	}
}

func TestJWKSRemovedKeyVerifiesUntilRefresh(t *testing.T) {
	ks := loadKeyset()
	rs := jwk("rs256", "RS256", &ks.rsa1.PublicKey)
	rs2 := jwk("rs2", "RS256", &ks.rsa2.PublicKey)
	mf := newMemFetcher(jwksBytes(rs, rs2))
	clk := clock.NewFake(testNow)
	rc := &refreshCount{}
	j := newTestJWKS(t, mf, clk, rc, nil)
	mustLookup(t, j, "rs256")

	mf.set(nil, errTestFetch)
	clk.Advance(testTTL)
	mustLookup(t, j, "rs256")
	waitRefreshes(t, rc, 1, 1)
	mustLookup(t, j, "rs256")
	if n := mf.count(); n != 2 {
		t.Fatalf("fetches after failed refresh = %d, want 2", n)
	}

	mf.set(jwksBytes(rs2), nil)
	mf.setBlock(make(chan struct{}))
	clk.Advance(testMinRefresh)
	requireNoErrors(t, promptly(t, j, 0, lookupCall(t, j, "rs256", "rs256")))
	testwait.Until(t, "fetch timeout timer", func() bool { return clk.Waiters() == 1 }, nil)
	clk.Advance(testFetchTimeout)
	waitRefreshes(t, rc, 1, 2)
	mustLookup(t, j, "rs256")
	if n := mf.count(); n != 3 {
		t.Fatalf("fetches after timed-out refresh = %d, want 3", n)
	}

	block2 := make(chan struct{})
	mf.setBlock(block2)
	clk.Advance(testMinRefresh)
	requireNoErrors(t, promptly(t, j, 0, lookupCall(t, j, "rs256", "rs256")))
	close(block2)
	waitRefreshes(t, rc, 2, 2)
	if n := mf.count(); n != 4 {
		t.Fatalf("fetches after successful refresh = %d, want 4", n)
	}
	if _, err := j.Lookup(t.Context(), "rs256"); !errors.Is(err, ErrUnknownKID) {
		t.Fatalf("removed kid after refresh: err = %v, want ErrUnknownKID", err)
	}
	mustLookup(t, j, "rs2")
	if n := mf.count(); n != 4 {
		t.Fatalf("fetches after removal = %d, want 4", n)
	}
}

func TestJWKSSingleFlightMixedCallers(t *testing.T) {
	ks := loadKeyset()
	rs := jwk("rs256", "RS256", &ks.rsa1.PublicKey)
	rs2 := jwk("rs2", "RS256", &ks.rsa2.PublicKey)

	t.Run("unknown first", func(t *testing.T) {
		mf := newMemFetcher(jwksBytes(rs))
		clk := clock.NewFake(testNow)
		rc := &refreshCount{}
		j := newTestJWKS(t, mf, clk, rc, nil)
		mustLookup(t, j, "rs256")

		mf.set(jwksBytes(rs, rs2), nil)
		block := make(chan struct{})
		mf.setBlock(block)
		clk.Advance(testTTL)
		unknown := make([]<-chan lookupResult, 20)
		for i := range unknown {
			unknown[i] = startLookup(t, j, "rs2")
		}
		waitWaiters(t, j, 20)
		calls := append(repeatCall(20, lookupCall(t, j, "rs256", "rs256")), repeatCall(10, keysCall(t, j, 1))...)
		requireNoErrors(t, promptly(t, j, 20, calls...))
		testwait.Until(t, "refresh fetching", func() bool { return mf.count() >= 2 }, nil)
		if n := mf.count(); n != 2 {
			t.Fatalf("fetches during the refresh = %d, want 2", n)
		}

		close(block)
		for _, ch := range unknown {
			r, _ := testwait.Recv(t, "unknown-kid lookup after the refresh", ch, nil)
			if r.err != nil || r.key.ID != "rs2" {
				t.Fatalf("unknown-kid lookup = %q, %v, want rs2", r.key.ID, r.err)
			}
		}
		waitRefreshes(t, rc, 2, 0)
		if n := mf.count(); n != 2 {
			t.Fatalf("fetches after the refresh = %d, want 2", n)
		}
		assertMissingCooldown(t, j, mf, clk, 2)
	})

	t.Run("known first", func(t *testing.T) {
		mf := newMemFetcher(jwksBytes(rs))
		clk := clock.NewFake(testNow)
		rc := &refreshCount{}
		j := newTestJWKS(t, mf, clk, rc, nil)
		mustLookup(t, j, "rs256")

		mf.set(jwksBytes(rs, rs2), nil)
		block := make(chan struct{})
		mf.setBlock(block)
		clk.Advance(testTTL)
		calls := append(repeatCall(20, lookupCall(t, j, "rs256", "rs256")), repeatCall(10, keysCall(t, j, 1))...)
		requireNoErrors(t, promptly(t, j, 0, calls...))
		unknown := make([]<-chan lookupResult, 20)
		for i := range unknown {
			unknown[i] = startLookup(t, j, "rs2")
		}
		waitWaiters(t, j, 20)
		testwait.Until(t, "refresh fetching", func() bool { return mf.count() >= 2 }, nil)
		if n := mf.count(); n != 2 {
			t.Fatalf("fetches during the refresh = %d, want 2", n)
		}

		close(block)
		for _, ch := range unknown {
			r, _ := testwait.Recv(t, "unknown-kid lookup after the refresh", ch, nil)
			if r.err != nil || r.key.ID != "rs2" {
				t.Fatalf("unknown-kid lookup = %q, %v, want rs2", r.key.ID, r.err)
			}
		}
		waitRefreshes(t, rc, 2, 0)
		if n := mf.count(); n != 2 {
			t.Fatalf("fetches after the refresh = %d, want 2", n)
		}
		assertMissingCooldown(t, j, mf, clk, 2)
	})
}

func TestJWKSKeySetsRefreshIndependently(t *testing.T) {
	ks := loadKeyset()
	rs := jwk("rs256", "RS256", &ks.rsa1.PublicKey)
	clk := clock.NewFake(testNow)
	mfA := newMemFetcher(jwksBytes(rs))
	mfB := newMemFetcher(jwksBytes(rs))
	rcA := &refreshCount{}
	rcB := &refreshCount{}
	ja := newTestJWKS(t, mfA, clk, rcA, nil)
	jb := newTestJWKS(t, mfB, clk, rcB, nil)
	mustLookup(t, ja, "rs256")
	mustLookup(t, jb, "rs256")

	block := make(chan struct{})
	mfA.setBlock(block)
	clk.Advance(testTTL)
	unknownA := startLookup(t, ja, "rs2")
	waitWaiters(t, ja, 1)
	requireNoErrors(t, promptly(t, ja, 1, repeatCall(4, lookupCall(t, ja, "rs256", "rs256"))...))

	mustLookup(t, jb, "rs256")
	waitRefreshes(t, rcB, 2, 0)
	if n := mfB.count(); n != 2 {
		t.Fatalf("B fetches = %d, want 2", n)
	}
	mustLookup(t, jb, "rs256")
	testwait.Until(t, "A refresh fetching", func() bool { return mfA.count() >= 2 }, nil)
	if n := mfA.count(); n != 2 {
		t.Fatalf("A fetches while its refresh is blocked = %d, want 2", n)
	}
	if !inFlight(t, ja) {
		t.Fatal("A has no refresh in flight while it is blocked")
	}
	if inFlight(t, jb) {
		t.Fatal("B has a refresh in flight after its refresh completed")
	}

	close(block)
	r, _ := testwait.Recv(t, "A unknown-kid lookup after its refresh", unknownA, nil)
	if !errors.Is(r.err, ErrUnknownKID) {
		t.Fatalf("A unknown-kid lookup err = %v, want ErrUnknownKID", r.err)
	}
	waitRefreshes(t, rcA, 2, 0)
	if n := mfA.count(); n != 2 {
		t.Fatalf("A fetches = %d, want 2", n)
	}
	if n := mfB.count(); n != 2 {
		t.Fatalf("B fetches = %d, want 2", n)
	}
}
