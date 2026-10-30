package redis

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	microgateway "github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/hooks"
	"github.com/ok0/go-microgateway/internal/testwait"
)

func upstream(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func gwDoc(stores, routes, backendURL string) string {
	var b strings.Builder
	b.WriteString("version: 1\n")
	if stores != "" {
		b.WriteString("stores:\n" + stores)
	}
	b.WriteString("listeners:\n  - name: public\n    address: \"127.0.0.1:0\"\n")
	fmt.Fprintf(&b, "upstreams:\n  - name: up\n    hosts:\n      - url: %q\n", backendURL)
	b.WriteString("routes:\n" + routes)
	return b.String()
}

func indentLines(s string, n int) string {
	pad := strings.Repeat(" ", n)
	var b strings.Builder
	for _, line := range strings.SplitAfter(s, "\n") {
		if line == "" {
			continue
		}
		b.WriteString(pad)
		b.WriteString(line)
	}
	return b.String()
}

func redisStoreYAML(name string, addrs []string, redisExtra, guardExtra string) string {
	quoted := make([]string, len(addrs))
	for i, a := range addrs {
		quoted[i] = strconv.Quote(a)
	}
	if guardExtra == "" {
		guardExtra = "consecutive_failures: 3\nopen_timeout: 2s\n"
	}
	return "  - name: " + name + "\n    type: redis\n    redis:\n      addresses: [" + strings.Join(quoted, ", ") + "]\n" +
		indentLines(redisExtra, 6) +
		"    guard:\n" + indentLines(guardExtra, 6)
}

func routeYAML(name, rateLimitBody string) string {
	return "  - name: " + name + "\n    match:\n      path: /" + name + "\n    upstream: up\n    rate_limit:\n" +
		indentLines(rateLimitBody, 6)
}

func newGateway(t *testing.T, doc string, register bool, clk clock.Clock, logger *slog.Logger, opts ...microgateway.Option) (*microgateway.Gateway, error) {
	t.Helper()
	all := append([]microgateway.Option(nil), opts...)
	if register {
		all = append(all, Option())
	}
	if clk != nil {
		all = append(all, hooks.WithClock(clk).(microgateway.Option))
	}
	if logger != nil {
		all = append(all, microgateway.WithLogger(logger))
	}
	g, err := microgateway.NewFromYAML([]byte(doc), all...)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = g.Close() })
	return g, nil
}

func mustGateway(t *testing.T, doc string, clk clock.Clock, logger *slog.Logger) *microgateway.Gateway {
	t.Helper()
	g, err := newGateway(t, doc, true, clk, logger)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func doGet(g http.Handler, path, remote string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.RemoteAddr = remote
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	return rec
}

// getGuarded serves GET path from remote on g in its own goroutine and waits for it under a hang guard.
func getGuarded(t *testing.T, g http.Handler, path, remote string) *httptest.ResponseRecorder {
	t.Helper()
	var rec *httptest.ResponseRecorder
	done := make(chan struct{})
	go func() {
		defer close(done)
		rec = doGet(g, path, remote)
	}()
	testwait.Done(t, "GET "+path+" to complete", done, nil)
	return rec
}

// getAdvancing is getGuarded for a gateway on fake: while the request is pending it advances fake by step whenever a
// timer of fake is due within step. It returns the recorder and the fake time that passed while the request ran.
func getAdvancing(t *testing.T, g http.Handler, fake *clock.Fake, step time.Duration, path, remote string) (*httptest.ResponseRecorder, time.Duration) {
	t.Helper()
	start := fake.Now()
	var rec *httptest.ResponseRecorder
	done := make(chan struct{})
	go func() {
		defer close(done)
		rec = doGet(g, path, remote)
	}()
	testwait.Until(t, "GET "+path+" to complete", func() bool {
		select {
		case <-done:
			return true
		default:
		}
		if fake.Due(step) > 0 {
			fake.Advance(step)
		}
		return false
	}, func() string {
		return fmt.Sprintf("fake clock advanced %v; timers due within %v: %d", fake.Since(start), step, fake.Due(step))
	})
	return rec, fake.Since(start)
}

func responseErrorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body is not a JSON error: %v", err)
	}
	return body.Error.Code
}

func metricsText(t *testing.T, g *microgateway.Gateway) string {
	t.Helper()
	rec := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics: status %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

func seriesValue(body, name string, labels map[string]string) (float64, bool) {
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, name+"{") {
			continue
		}
		matched := true
		for k, v := range labels {
			if !strings.Contains(line, k+"=\""+v+"\"") {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		idx := strings.LastIndexByte(line, ' ')
		if idx < 0 {
			return 0, false
		}
		v, err := strconv.ParseFloat(line[idx+1:], 64)
		if err != nil {
			return 0, false
		}
		return v, true
	}
	return 0, false
}

func checkExposition(t *testing.T, body string) {
	t.Helper()
	series := regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*(\{.*\})?$`)
	for _, line := range strings.Split(body, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.LastIndexByte(line, ' ')
		if idx < 0 {
			t.Errorf("exposition line has no value: %q", line)
			continue
		}
		if !series.MatchString(line[:idx]) {
			t.Errorf("exposition series is malformed: %q", line[:idx])
		}
		if _, err := strconv.ParseFloat(line[idx+1:], 64); err != nil {
			t.Errorf("exposition value is not a float: %q", line)
		}
	}
}
