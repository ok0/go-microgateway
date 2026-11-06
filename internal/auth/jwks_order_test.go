package auth

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testwait"
)

// forcedOrder replaces the fetch observation of a JWKS: with resultFirst it takes the fetch result even when the
// fetch context has ended, otherwise it takes the end of the context. observed receives whether the context had
// ended when the observation was made.
type forcedOrder struct {
	resultFirst bool
	observed    chan bool
}

func (o forcedOrder) await(fctx context.Context, res <-chan fetchResult) (fetchResult, bool) {
	if o.resultFirst {
		r := <-res
		o.observed <- fctx.Err() != nil
		return r, false
	}
	<-fctx.Done()
	o.observed <- true
	return fetchResult{}, true
}

func TestJWKSTimeoutClassifiedInEitherOrder(t *testing.T) {
	for _, tc := range []struct {
		name        string
		resultFirst bool
	}{{"result-first", true}, {"context-first", false}} {
		t.Run(tc.name, func(t *testing.T) {
			ks := loadKeyset()
			mf := newMemFetcher(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)))
			mf.setBlock(make(chan struct{}))
			clk := clock.NewFake(testNow)
			logs := &syncBuffer{}
			j := newTestJWKS(t, mf, clk, nil, logs)
			o := forcedOrder{resultFirst: tc.resultFirst, observed: make(chan bool, 1)}
			j.awaitFetch = o.await

			errc := make(chan error, 1)
			go func() {
				_, err := j.Lookup(t.Context(), "rs256")
				errc <- err
			}()
			testwait.Until(t, "fetch timeout timer", func() bool { return clk.Waiters() == 1 }, nil)
			clk.Advance(testFetchTimeout)

			if err, _ := testwait.Recv(t, "Lookup to return", errc, nil); !errors.Is(err, ErrKeysUnavailable) {
				t.Errorf("Lookup error = %v, want ErrKeysUnavailable", err)
			}
			if observed, _ := testwait.Recv(t, "fetch observation", o.observed, nil); !observed {
				t.Error("the fetch was observed before the timeout ended its context")
			}
			got := logs.String()
			if !strings.Contains(got, "key set fetch timed out after 5s") {
				t.Errorf("log lacks the timeout classification:\n%s", got)
			}
			if strings.Contains(got, "context canceled") || strings.Contains(got, "cancelled") {
				t.Errorf("log carries a cancellation classification:\n%s", got)
			}
		})
	}
}

func TestJWKSCloseClassifiedInEitherOrder(t *testing.T) {
	for _, tc := range []struct {
		name        string
		resultFirst bool
	}{{"result-first", true}, {"context-first", false}} {
		t.Run(tc.name, func(t *testing.T) {
			ks := loadKeyset()
			mf := newMemFetcher(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)))
			mf.setBlock(make(chan struct{}))
			clk := clock.NewFake(testNow)
			logs := &syncBuffer{}
			j := newTestJWKS(t, mf, clk, nil, logs)
			o := forcedOrder{resultFirst: tc.resultFirst, observed: make(chan bool, 1)}
			j.awaitFetch = o.await

			errc := make(chan error, 1)
			go func() {
				_, err := j.Lookup(t.Context(), "rs256")
				errc <- err
			}()
			waitWaiters(t, j, 1)
			j.Close()

			if err, _ := testwait.Recv(t, "Lookup to return", errc, nil); !errors.Is(err, ErrKeysUnavailable) {
				t.Errorf("Lookup error = %v, want ErrKeysUnavailable", err)
			}
			if observed, _ := testwait.Recv(t, "fetch observation", o.observed, nil); !observed {
				t.Error("the fetch was observed before the timeout ended its context")
			}
			got := logs.String()
			if !strings.Contains(got, `error="key set fetch cancelled"`) {
				t.Errorf("log lacks the cancellation classification:\n%s", got)
			}
			if strings.Contains(got, "timed out") || strings.Contains(got, "context canceled") {
				t.Errorf("log carries a timeout or raw context classification:\n%s", got)
			}
		})
	}
}
