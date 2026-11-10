package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var loadPaths = []string{"/svc", "/new"}

type target struct {
	name, base string
	mu         sync.Mutex
	draining   bool
	notReady   time.Time
}

func (t *target) begin() (time.Time, bool) {
	t.mu.Lock()
	if t.draining {
		t.mu.Unlock()
		return time.Time{}, false
	}
	s := time.Now()
	t.mu.Unlock()
	return s, true
}

func (t *target) markNotReady() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.draining {
		t.notReady = time.Now()
		t.draining = true
	}
}

func (t *target) notReadyAt() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.notReady
}

type generator struct {
	client      *http.Client
	targets     []*target
	rate        int
	concurrency int
	seq         atomic.Uint64
	mu          sync.Mutex
	recs        []Record
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

func newClient(concurrency int) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy:               nil,
			DialContext:         (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
			MaxIdleConnsPerHost: concurrency + 1,
			IdleConnTimeout:     30 * time.Second,
			DisableCompression:  true,
		},
		Timeout: 5 * time.Second,
	}
}

func newGenerator(targets []*target, rate, concurrency int) *generator {
	return &generator{
		client:      newClient(concurrency),
		targets:     targets,
		rate:        rate,
		concurrency: concurrency,
	}
}

func (g *generator) start() {
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	tokens := make(chan struct{}, g.concurrency)
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		tick := time.NewTicker(time.Second / time.Duration(g.rate))
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				select {
				case tokens <- struct{}{}:
				default:
				}
			}
		}
	}()
	n := uint64(len(g.targets))
	m := uint64(len(loadPaths))
	for range g.concurrency {
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tokens:
					seq := g.seq.Add(1) - 1
					t := g.targets[seq%n]
					path := loadPaths[(seq/n)%m]
					_, _ = g.do(ctx, t, path)
				}
			}
		}()
	}
}

func errText(err error) string {
	if errors.Is(err, syscall.ECONNREFUSED) {
		return "refused"
	}
	return err.Error()
}

func (g *generator) do(ctx context.Context, t *target, path string) (Record, bool) {
	start, ok := t.begin()
	if !ok {
		return Record{}, false
	}
	rec := Record{Instance: t.name, Path: path, Start: start}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.base+path, nil)
	if err != nil {
		return Record{}, false
	}
	resp, err := g.client.Do(req)
	rec.End = time.Now()
	if ctx.Err() != nil {
		if err == nil {
			_ = resp.Body.Close()
		}
		return Record{}, false
	}
	if err != nil {
		rec.Err = errText(err)
	} else {
		rec.Status = resp.StatusCode
		rec.Backend = resp.Header.Get("X-Backend")
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
	}
	g.mu.Lock()
	g.recs = append(g.recs, rec)
	g.mu.Unlock()
	return rec, true
}

func (g *generator) stop() {
	g.cancel()
	g.wg.Wait()
}

func (g *generator) records() []Record {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.recs)
}

func between(recs []Record, inst string, from, to time.Time) []Record {
	var out []Record
	for _, r := range recs {
		if r.Instance != inst || r.Start.Before(from) {
			continue
		}
		if !to.IsZero() && !r.Start.Before(to) {
			continue
		}
		out = append(out, r)
	}
	return out
}
