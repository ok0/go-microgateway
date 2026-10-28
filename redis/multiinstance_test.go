package redis

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	microgateway "github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
)

// redisServer is the Redis server shared by the gateways under test. exact
// reports that the server clock moves only through advance, so decisions are
// fully determined; otherwise advance moves real time.
type redisServer struct {
	addr    string
	db      int
	exact   bool
	advance func(time.Duration)
}

const (
	multiBurst       = 10
	multiSenders     = 8
	multiClientAddr  = "192.0.2.10:4000"
	multiBucketBody  = "rate: 1\nper: 1h\nburst: 10\n"
	multiWindowBody  = "algorithm: sliding_window\nlimit: 10\nwindow: 1h\n"
	multiElapsedBody = "rate: 10\nper: 1s\nburst: 5\n"
	multiStoreTail   = "store: shared\non_store_error: deny\nstore_timeout: 2s\n"
)

type multiTally struct {
	admitted int
	denied   int
}

// multiGateways builds n gateways with fake clocks hours apart. When useStore is
// set every gateway shares the Redis server under a prefix unique to the caller.
func multiGateways(t *testing.T, srv redisServer, n int, route string, useStore bool) []*microgateway.Gateway {
	t.Helper()
	prefix := uniquePrefix(t)
	c := newTestClient(t, srv.addr, srv.db)
	t.Cleanup(func() { deletePrefix(t, c, prefix) })
	var stores string
	if useStore {
		stores = redisStoreYAML("shared", []string{srv.addr}, fmt.Sprintf("db: %d\nkey_prefix: %s\n", srv.db, prefix), "")
	}
	doc := gwDoc(stores, routeYAML("rl", route), upstream(t))
	gws := make([]*microgateway.Gateway, n)
	for i := range gws {
		clk := clock.NewFake(miniStart.Add(time.Duration(i) * 3 * time.Hour))
		gws[i] = mustGateway(t, doc, clk, nil)
	}
	return gws
}

// fire sends reqs requests from one client address. Request k goes to gateway
// k mod len(gws); each gateway is driven by multiSenders concurrent senders.
// It returns the recorders per gateway in send order.
func fire(gws []*microgateway.Gateway, reqs int) [][]*httptest.ResponseRecorder {
	n := len(gws)
	out := make([][]*httptest.ResponseRecorder, n)
	var wg sync.WaitGroup
	for i, g := range gws {
		count := 0
		for k := i; k < reqs; k += n {
			count++
		}
		out[i] = make([]*httptest.ResponseRecorder, count)
		for w := 0; w < multiSenders; w++ {
			wg.Add(1)
			go func(g *microgateway.Gateway, recs []*httptest.ResponseRecorder, first int) {
				defer wg.Done()
				for j := first; j < len(recs); j += multiSenders {
					recs[j] = doGet(g, "/rl", multiClientAddr)
				}
			}(g, out[i], w)
		}
	}
	wg.Wait()
	return out
}

// tallyRecorders counts admitted (200) and denied (429) responses per gateway.
// Any other status fails the test; every 429 must carry a Retry-After of at
// least one second and the rate_limited error code.
func tallyRecorders(t *testing.T, recs [][]*httptest.ResponseRecorder) []multiTally {
	t.Helper()
	out := make([]multiTally, len(recs))
	for i, rs := range recs {
		for _, rec := range rs {
			switch rec.Code {
			case http.StatusOK:
				out[i].admitted++
			case http.StatusTooManyRequests:
				out[i].denied++
				ra := rec.Header().Get("Retry-After")
				if v, err := strconv.Atoi(ra); err != nil || v < 1 {
					t.Errorf("gateway %d: 429 Retry-After %q, want integer >= 1", i, ra)
				}
				if code := responseErrorCode(t, rec); code != "rate_limited" {
					t.Errorf("gateway %d: 429 error code %q, want rate_limited", i, code)
				}
			default:
				t.Fatalf("gateway %d: status %d, want 200 or 429", i, rec.Code)
			}
		}
	}
	return out
}

func multiSum(tallies []multiTally) (admitted, sent int) {
	for _, ta := range tallies {
		admitted += ta.admitted
		sent += ta.admitted + ta.denied
	}
	return admitted, sent
}

// runBurst sends n·2·multiBurst requests and checks the admitted total.
func runBurst(t *testing.T, srv redisServer, n int, route string, useStore bool) {
	t.Helper()
	gws := multiGateways(t, srv, n, route, useStore)
	reqs := n * multiBurst * 2
	tallies := tallyRecorders(t, fire(gws, reqs))
	admitted, sent := multiSum(tallies)
	if sent != reqs {
		t.Fatalf("accounted %d requests, sent %d", sent, reqs)
	}
	switch {
	case !useStore:
		if admitted != n*multiBurst {
			t.Errorf("admitted %d across %d in-memory gateways, want %d", admitted, n, n*multiBurst)
		}
	case srv.exact:
		if admitted != multiBurst {
			t.Errorf("admitted %d across %d gateways, want exactly %d", admitted, n, multiBurst)
		}
	default:
		if admitted > multiBurst {
			t.Errorf("admitted %d across %d gateways, want at most %d", admitted, n, multiBurst)
		}
	}
}

// runElapsed sends four rounds of n·10 requests with 250 ms of server time
// between rounds. A 100 ms refill interval and a burst of 5 admit 5 + 7 requests
// over the 750 ms exactly.
func runElapsed(t *testing.T, srv redisServer, n int) {
	t.Helper()
	gws := multiGateways(t, srv, n, multiElapsedBody+multiStoreTail, true)
	start := time.Now()
	var admitted, sent int
	for round := 0; round < 4; round++ {
		if round > 0 {
			srv.advance(250 * time.Millisecond)
		}
		a, s := multiSum(tallyRecorders(t, fire(gws, n*10)))
		admitted += a
		sent += s
	}
	elapsed := time.Since(start)
	if srv.exact {
		elapsed = 750 * time.Millisecond
	}
	if sent != n*40 {
		t.Fatalf("accounted %d requests, sent %d", sent, n*40)
	}
	if srv.exact && admitted != 5+7 {
		t.Errorf("admitted %d over %s, want exactly %d", admitted, elapsed, 5+7)
	}
	if bound := 5 + int(elapsed/(100*time.Millisecond)); admitted > bound {
		t.Errorf("admitted %d over %s, want at most %d", admitted, elapsed, bound)
	}
}

func runMultiInstance(t *testing.T, srv redisServer) {
	t.Helper()
	t.Run("token_bucket/2", func(t *testing.T) {
		runBurst(t, srv, 2, multiBucketBody+multiStoreTail, true)
	})
	t.Run("token_bucket/3", func(t *testing.T) {
		runBurst(t, srv, 3, multiBucketBody+multiStoreTail, true)
	})
	t.Run("sliding_window/2", func(t *testing.T) {
		runBurst(t, srv, 2, multiWindowBody+multiStoreTail, true)
	})
	t.Run("sliding_window/3", func(t *testing.T) {
		runBurst(t, srv, 3, multiWindowBody+multiStoreTail, true)
	})
	t.Run("elapsed/2", func(t *testing.T) {
		runElapsed(t, srv, 2)
	})
	t.Run("memory/2", func(t *testing.T) {
		runBurst(t, srv, 2, multiBucketBody, false)
	})
	t.Run("memory/3", func(t *testing.T) {
		runBurst(t, srv, 3, multiBucketBody, false)
	})
}

func TestMultiInstance(t *testing.T) {
	m := newMini(t)
	mc := &miniClock{m: m, now: miniStart}
	runMultiInstance(t, redisServer{addr: m.Addr(), exact: true, advance: mc.Advance})
}
