package lychgate

import (
	"fmt"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/teststore"
	"github.com/ok0/lychgate/store"
)

const (
	stSecret         = "p4ss-UNIQUE-1"
	stSentinelSecret = "s3nt-UNIQUE-2"
	stParamSecret    = "par4m-UNIQUE-3"
)

func stShared(t *testing.T, fake *clock.Fake) *teststore.Shared {
	t.Helper()
	s := teststore.NewShared(fake)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func stOption(shared *teststore.Shared, calls *atomic.Int64) Option {
	return WithStoreType("test", store.CapRateLimit, func(p StoreParams) (store.Backend, error) {
		calls.Add(1)
		return shared.Handle(), nil
	})
}

func stDoc(backendURL, stores, routes string) string {
	if stores != "" {
		stores = "stores:\n" + stores
	}
	return rlDoc(backendURL, stores, routes)
}

func stStore(name, extra string) string {
	return fmt.Sprintf("  - name: %s\n    type: test\n", name) + extra
}

func stRoute(name, rl string) string {
	return rlRoute(name, rl)
}

func stDo(t *testing.T, g *Gateway, path, remote string) *httptest.ResponseRecorder {
	t.Helper()
	return rlGet(t, g, path, remote, nil)
}

func stAsync(t *testing.T, g *Gateway, path, remote string) <-chan *httptest.ResponseRecorder {
	t.Helper()
	ch := make(chan *httptest.ResponseRecorder, 1)
	go func() { ch <- rlGet(t, g, path, remote, nil) }()
	return ch
}

func stGauge(t *testing.T, g *Gateway, name string, labels ...string) float64 {
	t.Helper()
	return rsMetricOr0(t, g, name, labels...)
}

func stCounter(t *testing.T, g *Gateway, name string, labels ...string) float64 {
	t.Helper()
	return rsMetricOr0(t, g, name, labels...)
}
