package microgateway

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
)

type balHost struct {
	url    string
	weight int
}

func balDoc(lb string, hosts []balHost) string {
	var b strings.Builder
	b.WriteString("version: 1\n" + listenersYAML + "upstreams:\n  - name: up\n")
	b.WriteString("    timeouts:\n      connect: 1h\n      response_header: 1h\n")
	b.WriteString(lb)
	b.WriteString("    hosts:\n")
	for _, h := range hosts {
		fmt.Fprintf(&b, "      - url: %q\n        weight: %d\n", h.url, h.weight)
	}
	b.WriteString("routes:\n  - name: all\n    upstream: up\n")
	return b.String()
}

func balRequest(path, remote string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.RemoteAddr = remote
	return req
}

func balDo(g *Gateway, req *http.Request) (int, string) {
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	var e echo
	_ = json.NewDecoder(rec.Body).Decode(&e)
	return rec.Code, e.Backend
}

func balSpread(t *testing.T, n *memNetwork, lb string, requests int) [4]int64 {
	t.Helper()
	weights := []int{5, 1, 1, 0}
	names := []string{"a", "b", "c", "d"}
	hosts := make([]balHost, 0, len(names))
	backs := make([]*backendHandle, 0, len(names))
	for i, name := range names {
		be := failingBackend(t, n, name)
		backs = append(backs, be)
		hosts = append(hosts, balHost{url: be.URL, weight: weights[i]})
	}
	g := n.gateway(t, balDoc(lb, hosts), withClock(clock.NewFake(time.Unix(0, 0))), withRand(clock.NewRand(1)))
	for i := range requests {
		req := balRequest("/", fmt.Sprintf("10.2.0.%d:4000", i%250+1))
		req.Header.Set("X-User", fmt.Sprintf("u%d", i))
		if code, _ := balDo(g, req); code != http.StatusOK {
			t.Fatalf("request %d status = %d, want 200", i, code)
		}
	}
	var counts [4]int64
	for i, be := range backs {
		counts[i] = be.Hits.Load()
	}
	return counts
}

func TestBalancingEndToEnd(t *testing.T) {
	const requests = 700
	cases := []struct {
		name  string
		lb    string
		check func(t *testing.T, counts [4]int64)
	}{
		{
			name: "weighted_round_robin",
			lb:   "    load_balancer:\n      algorithm: weighted_round_robin\n",
			check: func(t *testing.T, counts [4]int64) {
				if counts != [4]int64{500, 100, 100, 0} {
					t.Fatalf("counts = %v, want [500 100 100 0]", counts)
				}
			},
		},
		{
			name: "round_robin",
			lb:   "    load_balancer:\n      algorithm: round_robin\n",
			check: func(t *testing.T, counts [4]int64) {
				if counts != [4]int64{234, 233, 233, 0} {
					t.Fatalf("counts = %v, want [234 233 233 0]", counts)
				}
			},
		},
		{
			name: "random",
			lb:   "    load_balancer:\n      algorithm: random\n",
			check: func(t *testing.T, counts [4]int64) {
				shares := []float64{5.0 / 7, 1.0 / 7, 1.0 / 7}
				for i, share := range shares {
					got := float64(counts[i]) / requests
					if math.Abs(got-share) > 0.03 {
						t.Fatalf("backend %d share = %.4f, want %.4f +-0.03", i, got, share)
					}
				}
				if counts[3] != 0 {
					t.Fatalf("weight-0 backend served %d requests, want 0", counts[3])
				}
			},
		},
		{
			name: "p2c",
			lb:   "    load_balancer:\n      algorithm: p2c\n",
			check: func(t *testing.T, counts [4]int64) {
				if counts[3] != 0 {
					t.Fatalf("weight-0 backend served %d requests, want 0", counts[3])
				}
			},
		},
		{
			name: "consistent_hash",
			lb:   "    load_balancer:\n      algorithm: consistent_hash\n      hash:\n        source: header\n        name: X-User\n",
			check: func(t *testing.T, counts [4]int64) {
				if counts[3] != 0 {
					t.Fatalf("weight-0 backend served %d requests, want 0", counts[3])
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := newMemNetwork()
			tc.check(t, balSpread(t, n, tc.lb, requests))
		})
	}
}

func TestP2CSlowHost(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Unix(0, 0))

	var slowHits, slowPending atomic.Int64
	slow := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slowHits.Add(1)
		slowPending.Add(1)
		defer slowPending.Add(-1)
		delayed(fake, 20*time.Millisecond, echoHandler("slow")).ServeHTTP(w, r)
	}))

	newFast := func(name string, hits *atomic.Int64) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(echo{Backend: name, Path: r.URL.Path})
			fake.Advance(time.Millisecond)
		})
	}

	t.Run("fast hosts absorb load", func(t *testing.T) {
		const workers, perWorker = 8, 200
		fastHits := make([]*atomic.Int64, 3)
		hosts := []balHost{{url: slow.URL, weight: 1}}
		for i := range fastHits {
			fastHits[i] = &atomic.Int64{}
			be := n.serve(t, newFast(fmt.Sprintf("fast%d", i), fastHits[i]))
			hosts = append(hosts, balHost{url: be.URL, weight: 1})
		}
		lb := "    load_balancer:\n      algorithm: p2c\n"
		g := n.gateway(t, balDoc(lb, hosts), withClock(fake))

		var running atomic.Int64
		running.Store(workers)
		var failed atomic.Int64
		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer running.Add(-1)
				for range perWorker {
					rec := httptest.NewRecorder()
					g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
					if rec.Code != http.StatusOK {
						failed.Add(1)
					}
				}
			}()
		}
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		for finished := false; !finished; {
			select {
			case <-done:
				finished = true
			default:
				if running.Load() == slowPending.Load() {
					fake.Advance(time.Millisecond)
				}
				runtime.Gosched()
			}
		}
		fake.Advance(time.Second)

		if got := failed.Load(); got != 0 {
			t.Fatalf("%d requests did not return 200", got)
		}
		var fastTotal int64
		for _, h := range fastHits {
			fastTotal += h.Load()
		}
		total := int64(workers * perWorker)
		slowN := slowHits.Load()
		if slowN+fastTotal != total {
			t.Fatalf("served %d requests, want %d", slowN+fastTotal, total)
		}
		meanFast := float64(fastTotal) / float64(len(fastHits))
		if float64(slowN) >= 0.5*meanFast {
			t.Fatalf("slow host served %d requests, want < %.1f (0.5 x mean fast)", slowN, 0.5*meanFast)
		}
		if fastTotal < total-slowN {
			t.Fatalf("fast hosts served %d requests, want >= %d", fastTotal, total-slowN)
		}
		if slowPending.Load() != 0 {
			t.Fatalf("%d slow requests still pending", slowPending.Load())
		}
	})

	t.Run("single host", func(t *testing.T) {
		hits := &atomic.Int64{}
		be := n.serve(t, newFast("only", hits))
		doc := balDoc("    load_balancer:\n      algorithm: p2c\n", []balHost{{url: be.URL, weight: 1}})
		g := n.gateway(t, doc, withClock(fake))
		const requests = 200
		for i := range requests {
			if code, _ := balDo(g, httptest.NewRequest(http.MethodGet, "/", nil)); code != http.StatusOK {
				t.Fatalf("request %d status = %d, want 200", i, code)
			}
		}
		if got := hits.Load(); got != requests {
			t.Fatalf("single host served %d requests, want %d", got, requests)
		}
	})
}

func balHashLB(source string) string {
	hash := map[string]string{
		"header": "        source: header\n        name: X-User\n",
		"cookie": "        source: cookie\n        name: sid\n",
		"ip":     "        source: ip\n",
		"path":   "        source: path\n",
	}[source]
	return "    load_balancer:\n      algorithm: consistent_hash\n      hash:\n" + hash
}

func balKeyRequest(source string, i int) *http.Request {
	key := fmt.Sprintf("key-%d", i)
	remote := fmt.Sprintf("10.1.0.%d:4000", i+1)
	path := "/"
	if source == "path" {
		path = "/" + key
	}
	req := balRequest(path, remote)
	switch source {
	case "header":
		req.Header.Set("X-User", key)
	case "cookie":
		req.AddCookie(&http.Cookie{Name: "sid", Value: key})
	}
	return req
}

func balMapping(t *testing.T, g *Gateway, source string, keys int) []string {
	t.Helper()
	out := make([]string, keys)
	for i := range keys {
		code, backend := balDo(g, balKeyRequest(source, i))
		if code != http.StatusOK {
			t.Fatalf("key %d status = %d, want 200", i, code)
		}
		out[i] = backend
	}
	return out
}

func balInFlightTotal(t *testing.T, g *Gateway) int64 {
	t.Helper()
	var sum int64
	for _, h := range upstreamStatus(t, g, "up").Hosts {
		sum += h.InFlight
	}
	return sum
}

type balGate struct {
	URL     string
	release chan struct{}
	once    sync.Once
}

func (gt *balGate) open() {
	gt.once.Do(func() { close(gt.release) })
}

func balGated(t *testing.T, n testNetwork, name string) *balGate {
	t.Helper()
	gt := &balGate{release: make(chan struct{})}
	s := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-gt.release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(echo{Backend: name, Path: r.URL.Path})
	}))
	gt.URL = s.URL
	return gt
}

func TestConsistentHashEndToEnd(t *testing.T) {
	const keys = 200
	n := newMemNetwork()
	backs := make([]*backendHandle, 5)
	hosts := make([]balHost, 5)
	for i := range backs {
		backs[i] = failingBackend(t, n, fmt.Sprintf("h%d", i))
		hosts[i] = balHost{url: backs[i].URL, weight: 1}
	}

	for _, src := range []string{"header", "cookie", "ip", "path"} {
		t.Run(src, func(t *testing.T) {
			doc := balDoc(balHashLB(src), hosts)
			g := n.gateway(t, doc, withClock(clock.NewFake(time.Unix(0, 0))))
			first := balMapping(t, g, src, keys)
			for rep := 2; rep <= 3; rep++ {
				if again := balMapping(t, g, src, keys); !slices.Equal(first, again) {
					t.Fatalf("repeat %d: mapping changed for the same keys", rep)
				}
			}
			second := n.gateway(t, doc, withClock(clock.NewFake(time.Unix(0, 0))))
			if got := balMapping(t, second, src, keys); !slices.Equal(first, got) {
				t.Fatal("second gateway from the same YAML maps keys differently")
			}

			if err := g.ReloadYAML([]byte(balDoc(balHashLB(src), hosts[:4]))); err != nil {
				t.Fatal(err)
			}
			after := balMapping(t, g, src, keys)
			for i := range keys {
				if first[i] == "h4" {
					if after[i] == "h4" {
						t.Fatalf("key %d still maps to removed h4", i)
					}
					continue
				}
				if after[i] != first[i] {
					t.Fatalf("key %d moved from %s to %s although its backend stayed", i, first[i], after[i])
				}
			}

			if err := g.ReloadYAML([]byte(balDoc(balHashLB(src), hosts))); err != nil {
				t.Fatal(err)
			}
			if restored := balMapping(t, g, src, keys); !slices.Equal(first, restored) {
				t.Fatal("mapping after re-adding h4 differs from the first mapping")
			}
		})
	}

	t.Run("eight hosts", func(t *testing.T) {
		const minKept = 160
		n8 := newMemNetwork()
		names := make([]string, 8)
		hosts8 := make([]balHost, 8)
		for i := range hosts8 {
			names[i] = fmt.Sprintf("e%d", i)
			hosts8[i] = balHost{url: failingBackend(t, n8, names[i]).URL, weight: 1}
		}
		for _, src := range []string{"header", "cookie", "ip", "path"} {
			g := n8.gateway(t, balDoc(balHashLB(src), hosts8), withClock(clock.NewFake(time.Unix(0, 0))))
			first := balMapping(t, g, src, keys)
			for r := range hosts8 {
				reduced := slices.Delete(slices.Clone(hosts8), r, r+1)
				if err := g.ReloadYAML([]byte(balDoc(balHashLB(src), reduced))); err != nil {
					t.Fatal(err)
				}
				after := balMapping(t, g, src, keys)
				unchanged := 0
				for i := range keys {
					if first[i] == names[r] {
						if after[i] == names[r] {
							t.Fatalf("%s: key %d still maps to removed %s", src, i, names[r])
						}
						continue
					}
					if after[i] != first[i] {
						t.Fatalf("%s: removing %s moved key %d from %s to %s", src, names[r], i, first[i], after[i])
					}
					unchanged++
				}
				if unchanged < minKept {
					t.Fatalf("%s: removing %s kept %d of %d keys, want at least %d", src, names[r], unchanged, keys, minKept)
				}

				if err := g.ReloadYAML([]byte(balDoc(balHashLB(src), hosts8))); err != nil {
					t.Fatal(err)
				}
				if restored := balMapping(t, g, src, keys); !slices.Equal(first, restored) {
					t.Fatalf("%s: mapping after re-adding %s differs from the first mapping", src, names[r])
				}
			}
		}
	})

	t.Run("missing header maps like ip", func(t *testing.T) {
		gHdr := n.gateway(t, balDoc(balHashLB("header"), hosts), withClock(clock.NewFake(time.Unix(0, 0))))
		gIP := n.gateway(t, balDoc(balHashLB("ip"), hosts), withClock(clock.NewFake(time.Unix(0, 0))))
		for i := range keys {
			_, viaHdr := balDo(gHdr, balRequest("/", fmt.Sprintf("10.1.0.%d:4000", i+1)))
			_, viaIP := balDo(gIP, balRequest("/", fmt.Sprintf("10.1.0.%d:4000", i+1)))
			if viaHdr != viaIP {
				t.Fatalf("remote %d: header source (missing) -> %s, ip source -> %s", i, viaHdr, viaIP)
			}
		}
	})

	t.Run("bounded load", func(t *testing.T) {
		const held = 40
		gates := make([]*balGate, 5)
		ghosts := make([]balHost, 5)
		for i := range gates {
			gates[i] = balGated(t, n, fmt.Sprintf("g%d", i))
			ghosts[i] = balHost{url: gates[i].URL, weight: 1}
		}
		g := n.gateway(t, balDoc(balHashLB("header"), ghosts), withClock(clock.NewFake(time.Unix(0, 0))))
		t.Cleanup(func() {
			for _, gt := range gates {
				gt.open()
			}
		})

		codes := make([]int, held)
		var wg sync.WaitGroup
		for i := range held {
			eventually(t, func() bool { return balInFlightTotal(t, g) == int64(i) })
			wg.Add(1)
			go func() {
				defer wg.Done()
				req := balRequest("/", "10.3.0.1:4000")
				req.Header.Set("X-User", "hot")
				rec := httptest.NewRecorder()
				g.ServeHTTP(rec, req)
				codes[i] = rec.Code
			}()
		}
		eventually(t, func() bool { return balInFlightTotal(t, g) == held })

		bound := int64(math.Ceil(1.25 * held / float64(len(gates))))
		for _, h := range upstreamStatus(t, g, "up").Hosts {
			if h.InFlight > bound {
				t.Fatalf("host %s in_flight = %d, want <= %d", h.URL, h.InFlight, bound)
			}
		}

		for _, gt := range gates {
			gt.open()
		}
		wg.Wait()
		for i, code := range codes {
			if code != http.StatusOK {
				t.Fatalf("held request %d status = %d, want 200", i, code)
			}
		}
	})
}
