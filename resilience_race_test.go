package lychgate

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/upstream"
)

func h3Upstreams(g *Gateway) ([]upstream.Status, error) {
	rec := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/upstreams", nil))
	if rec.Code != http.StatusOK {
		return nil, fmt.Errorf("/upstreams status = %d", rec.Code)
	}
	var list []upstream.Status
	if err := json.NewDecoder(rec.Body).Decode(&list); err != nil {
		return nil, err
	}
	return list, nil
}

func h3Violations(list []upstream.Status) []string {
	var out []string
	check := func(where string, b *upstream.BreakerStatus) {
		if b == nil {
			return
		}
		if b.ProbesInFlight > 2 {
			out = append(out, fmt.Sprintf("%s probes_in_flight = %d, want <= 2", where, b.ProbesInFlight))
		}
		if b.InFlight < 0 {
			out = append(out, fmt.Sprintf("%s in_flight = %d, want >= 0", where, b.InFlight))
		}
	}
	for _, st := range list {
		check(st.Name+" aggregate", st.Breaker)
		for _, h := range st.Hosts {
			check(st.Name+" host "+h.URL, h.Breaker)
		}
	}
	return out
}

func h3GetSeq(g *Gateway, path, seq string) (int, string) {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("X-Rs-Seq", seq)
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func h3IsCircuitOpen(body string) bool {
	var e struct {
		Error apiError `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		return false
	}
	return e.Error.Code == "circuit_open"
}

type h3Req struct {
	seq  string
	path string
	code int
	body string
}

func TestResilienceConcurrency(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Now())
	var attemptsMu sync.Mutex
	attempts := make(map[string]int)
	var urls []string
	var backends []*rsBackend
	for range 3 {
		be := newRSBackend(t, n, func(seq int64, w http.ResponseWriter, r *http.Request) {
			attemptsMu.Lock()
			attempts[r.Header.Get("X-Rs-Seq")]++
			attemptsMu.Unlock()
			if seq%3 == 0 {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			_, _ = io.WriteString(w, "ok")
		})
		urls = append(urls, be.URL)
		backends = append(backends, be)
	}
	doc := func(weights []int) string {
		d := rsDoc{hosts: urls, breaker: rsBreaker, routes: []rsRoute{
			{name: "r", path: "/r", extra: "retry:\n      base_delay: 1ns\n      max_delay: 1ns\n      budget:\n        ratio: 0.1\n        min_per_second: 1\n        window: 10s\n"},
			{name: "p", path: "/p"},
		}}.yaml()
		return h3Weights(d, urls, weights)
	}
	g := n.gateway(t, doc([]int{1, 1, 1}), withClock(fake))

	const workers, perWorker = 64, 50
	reqs := make([][]h3Req, workers)
	var problemsMu sync.Mutex
	var problems []string
	addProblem := func(s string) {
		problemsMu.Lock()
		defer problemsMu.Unlock()
		problems = append(problems, s)
	}

	var workersWG sync.WaitGroup
	for w := range workers {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			for j := range perWorker {
				path := "/p"
				if j%2 == 0 {
					path = "/r"
				}
				seq := fmt.Sprintf("%d-%d", w, j)
				code, body := h3GetSeq(g, path, seq)
				reqs[w] = append(reqs[w], h3Req{seq: seq, path: path, code: code, body: body})
			}
		}()
	}

	var reloaderWG sync.WaitGroup
	reloaderWG.Add(1)
	go func() {
		defer reloaderWG.Done()
		for k := range 20 {
			w := []int{1 + k%2, 2 - k%2, 1}
			if err := g.ReloadYAML([]byte(doc(w))); err != nil {
				addProblem(fmt.Sprintf("reload %d: %v", k, err))
			}
		}
	}()

	var clockWG sync.WaitGroup
	clockWG.Add(1)
	go func() {
		defer clockWG.Done()
		for range 40 {
			fake.Advance(5 * time.Second)
		}
	}()

	stop := make(chan struct{})
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			list, err := h3Upstreams(g)
			if err != nil {
				addProblem(err.Error())
				return
			}
			for _, v := range h3Violations(list) {
				addProblem(v)
			}
		}
	}()

	workersWG.Wait()
	reloaderWG.Wait()
	clockWG.Wait()
	close(stop)
	samplerWG.Wait()

	problemsMu.Lock()
	if len(problems) > 0 {
		problemsMu.Unlock()
		t.Fatalf("concurrent invariant violations: %v", problems)
	}
	problemsMu.Unlock()

	var all []h3Req
	for _, rs := range reqs {
		all = append(all, rs...)
	}
	if len(all) != workers*perWorker {
		t.Fatalf("responses = %d, want %d", len(all), workers*perWorker)
	}

	st := rsStatus(t, g)
	if got := h3Snap(t, st.Breaker).InFlight; got != 0 {
		t.Fatalf("aggregate breaker in_flight = %d, want 0", got)
	}
	for _, h := range st.Hosts {
		if got := h3Snap(t, h.Breaker).InFlight; got != 0 {
			t.Fatalf("host %s breaker in_flight = %d, want 0", h.URL, got)
		}
		if h.InFlight != 0 {
			t.Fatalf("host %s in_flight = %d, want 0", h.URL, h.InFlight)
		}
	}

	attemptsMu.Lock()
	defer attemptsMu.Unlock()
	sum := 0
	for _, c := range attempts {
		sum += c
	}
	var hits int64
	for _, be := range backends {
		hits += be.hitCount()
	}
	if int64(sum) != hits {
		t.Fatalf("sum of per-request attempts = %d, backend hits = %d", sum, hits)
	}

	wantRetries := 0
	for _, q := range all {
		if q.code != http.StatusOK && q.code != http.StatusServiceUnavailable {
			t.Fatalf("GET %s seq %s status = %d, want 200 or 503", q.path, q.seq, q.code)
		}
		limit := 3
		if q.path == "/p" {
			limit = 1
		}
		count := attempts[q.seq]
		if count > limit {
			t.Fatalf("GET %s seq %s attempts = %d, want <= %d", q.path, q.seq, count, limit)
		}
		if (count == 0) != h3IsCircuitOpen(q.body) {
			t.Fatalf("GET %s seq %s attempts = %d with body %q", q.path, q.seq, count, q.body)
		}
		if q.path == "/r" && count >= 1 {
			wantRetries += count - 1
		}
	}

	retries := rsMetric(t, g, "lychgate_retries_total", "route", "r")
	if retries != float64(wantRetries) {
		t.Fatalf("retries_total{r} = %v, want %d from per-request attempts", retries, wantRetries)
	}
	if got := rsMetricOr0(t, g, "lychgate_retries_total", "route", "p"); got != 0 {
		t.Fatalf("retries_total{p} = %v, want 0", got)
	}
	if retries <= 0 {
		t.Fatalf("retries_total{r} = %v, want > 0", retries)
	}
	if retries > 530 {
		t.Fatalf("retries_total{r} = %v, want <= 530", retries)
	}
}
