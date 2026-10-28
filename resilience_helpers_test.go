package microgateway

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/upstream"
)

type rsRoute struct{ name, path, extra string }

type rsDoc struct {
	hosts                          []string
	lb, breaker, passive, timeouts string
	routes                         []rsRoute
}

const rsBreaker = "      window: 10s\n      buckets: 10\n      min_requests: 100\n      failure_ratio: 1\n      consecutive_failures: 3\n      open_timeout: 30s\n      half_open_probes: 2\n      success_threshold: 2\n"

func rsWriteBlock(b *strings.Builder, body string) {
	for _, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			b.WriteString("      " + line + "\n")
		}
	}
}

func (d rsDoc) yaml() string {
	var b strings.Builder
	b.WriteString("version: 1\n")
	b.WriteString(listenersYAML)
	b.WriteString("upstreams:\n  - name: up\n    load_balancer:\n")
	lb := d.lb
	if lb == "" {
		lb = "round_robin"
	}
	fmt.Fprintf(&b, "      algorithm: %s\n", lb)
	b.WriteString("    timeouts:\n")
	timeouts := d.timeouts
	if timeouts == "" {
		timeouts = "connect: 1h\nresponse_header: 1h\n"
	}
	rsWriteBlock(&b, timeouts)
	b.WriteString("    hosts:\n")
	for _, h := range d.hosts {
		fmt.Fprintf(&b, "      - url: %q\n", h)
	}
	if d.breaker != "" {
		b.WriteString("    circuit_breaker:\n")
		rsWriteBlock(&b, d.breaker)
	}
	if d.passive != "" {
		b.WriteString("    passive_health:\n")
		rsWriteBlock(&b, d.passive)
	}
	b.WriteString("routes:\n")
	routes := d.routes
	if len(routes) == 0 {
		routes = []rsRoute{{name: "all", path: "/"}}
	}
	for _, r := range routes {
		path := r.path
		if path == "" {
			path = "/"
		}
		fmt.Fprintf(&b, "  - name: %s\n    match:\n      path: %s\n    upstream: up\n", r.name, path)
		if r.extra != "" {
			extra := r.extra
			if !strings.HasSuffix(extra, "\n") {
				extra += "\n"
			}
			b.WriteString("    " + extra)
		}
	}
	return b.String()
}

type rsBackend struct {
	URL    string
	srv    *testServer
	hits   atomic.Int64
	mu     sync.Mutex
	bodies [][]byte
	script func(n int64, w http.ResponseWriter, r *http.Request)
}

func newRSBackend(t *testing.T, n testNetwork, script func(n int64, w http.ResponseWriter, r *http.Request)) *rsBackend {
	t.Helper()
	b := &rsBackend{script: script}
	b.srv = n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		b.mu.Lock()
		b.bodies = append(b.bodies, data)
		seq := b.hits.Add(1)
		b.mu.Unlock()
		if b.script == nil {
			_, _ = io.WriteString(w, "ok")
			return
		}
		b.script(seq, w, r)
	}))
	b.URL = b.srv.URL
	return b
}

func (b *rsBackend) hitCount() int64 {
	return b.hits.Load()
}

func (b *rsBackend) body(i int) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bodies[i]
}

func rsStatusSeq(codes ...int) func(n int64, w http.ResponseWriter, r *http.Request) {
	return func(n int64, w http.ResponseWriter, _ *http.Request) {
		code := codes[min(int(n)-1, len(codes)-1)]
		w.Header().Set("X-Attempt", strconv.FormatInt(n, 10))
		w.WriteHeader(code)
		_, _ = fmt.Fprintf(w, "r%d", n)
	}
}

func rsReset(_ int64, w http.ResponseWriter, _ *http.Request) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		return
	}
	_ = conn.Close()
}

func rsHoldUntil(ch <-chan struct{}) func(n int64, w http.ResponseWriter, r *http.Request) {
	return func(_ int64, w http.ResponseWriter, r *http.Request) {
		select {
		case <-ch:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "ok")
	}
}

func rsGet(t *testing.T, client *http.Client, url, method string, body io.Reader, hdr http.Header) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header[k] = v
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, data
}

func rsMetric(t *testing.T, g *Gateway, name string, labels ...string) float64 {
	t.Helper()
	return metricValue(t, metricsText(t, g), series(name, labels...))
}

func rsMetricOr0(t *testing.T, g *Gateway, name string, labels ...string) float64 {
	t.Helper()
	prefix := series(name, labels...) + " "
	for _, line := range strings.Split(metricsText(t, g), "\n") {
		if rest, ok := strings.CutPrefix(line, prefix); ok {
			v, err := strconv.ParseFloat(rest, 64)
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
	}
	return 0
}

func rsStatus(t *testing.T, g *Gateway) upstream.Status {
	t.Helper()
	return upstreamStatus(t, g, "up")
}

func rsHost(t *testing.T, g *Gateway, url string) upstream.HostStatus {
	t.Helper()
	return hostByURL(rsStatus(t, g), url)
}

type rsLogs = captureHandler

func rsTransitions(h *captureHandler) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, r := range h.records {
		if r.Message != "circuit breaker state changed" {
			continue
		}
		attrs := recordAttrs(r)
		scope := attrs["scope"].String()
		host := attrs["host"].String()
		from := attrs["from"].String()
		to := attrs["to"].String()
		reason := attrs["reason"].String()
		want := slog.LevelInfo
		if to == "open" {
			want = slog.LevelWarn
		}
		if r.Level != want {
			panic(fmt.Sprintf("circuit breaker log for to=%s has level %v, want %v", to, r.Level, want))
		}
		out = append(out, fmt.Sprintf("%s:%s:%s>%s:%s", scope, host, from, to, reason))
	}
	return out
}

var rsExpositionLine = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*(\{([a-zA-Z_][a-zA-Z0-9_]*="([^"\\]|\\.)*",?)*\})? (-?[0-9.eE+-]+|NaN|\+Inf)$`)

func rsCheckExposition(t *testing.T, text string) {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if line == "" || strings.HasPrefix(line, "# HELP ") || strings.HasPrefix(line, "# TYPE ") {
			continue
		}
		if !rsExpositionLine.MatchString(line) {
			t.Errorf("malformed exposition line: %q", line)
		}
	}
}

func rsAdvanceBackoff(t *testing.T, fake *clock.Fake, max time.Duration) {
	t.Helper()
	fake.BlockUntilDue(1, max)
	fake.Advance(max)
}
