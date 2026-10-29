package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/ok0/go-microgateway/internal/cli"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func backend(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func randomHex(t *testing.T) string {
	t.Helper()
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func uniqueSecret(t *testing.T, prefix string) string {
	t.Helper()
	return prefix + randomHex(t)
}

func uniquePrefix(t *testing.T) string {
	t.Helper()
	return "mgtest-" + randomHex(t)
}

type docOpts struct {
	Backend    string
	RedisAddr  string
	Prefix     string
	DB         int
	Password   bool
	UseStore   bool
	ExtraRoute bool
}

func instanceDoc(o docOpts) string {
	var b strings.Builder
	fmt.Fprintf(&b, "version: 1\n")
	fmt.Fprintf(&b, "listeners:\n  - name: public\n    address: \"127.0.0.1:0\"\n")
	fmt.Fprintf(&b, "admin:\n  address: \"127.0.0.1:0\"\n")
	fmt.Fprintf(&b, "upstreams:\n  - name: up\n    hosts:\n      - url: \"%s\"\n", o.Backend)
	fmt.Fprintf(&b, "stores:\n  - name: shared\n    type: redis\n    redis:\n")
	fmt.Fprintf(&b, "      addresses: [\"%s\"]\n", o.RedisAddr)
	if o.Password {
		fmt.Fprintf(&b, "      password: \"${MG_TEST_REDIS_PASSWORD}\"\n")
	}
	fmt.Fprintf(&b, "      db: %d\n", o.DB)
	fmt.Fprintf(&b, "      key_prefix: \"%s\"\n", o.Prefix)
	fmt.Fprintf(&b, "routes:\n  - name: rl\n    match:\n      path: /rl\n    upstream: up\n")
	fmt.Fprintf(&b, "    rate_limit:\n      rate: 1\n      per: 1h\n      burst: 5\n      key:\n        source: route\n")
	if o.UseStore {
		fmt.Fprintf(&b, "      store: shared\n      on_store_error: deny\n      store_timeout: 2s\n")
	}
	if o.ExtraRoute {
		fmt.Fprintf(&b, "  - name: extra\n    match:\n      path: /extra\n    upstream: up\n")
	}
	return b.String()
}

func lookupWith(vals map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		if v, ok := vals[name]; ok {
			return v, true
		}
		return os.LookupEnv(name)
	}
}

func writeDoc(t *testing.T, src string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "microgateway.yaml")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

type instance struct {
	addrs cli.Addrs
	sig   chan os.Signal
	done  chan int
	errb  *syncBuffer
}

func startInstance(t *testing.T, ctx context.Context, path string, lookup func(string) (string, bool), extraArgs ...string) *instance {
	t.Helper()
	inst := &instance{sig: make(chan os.Signal, 2), done: make(chan int, 1), errb: &syncBuffer{}}
	ready := make(chan cli.Addrs, 1)
	args := append([]string{"run", "-config", path}, extraArgs...)
	go func() {
		inst.done <- gateway(ctx, cli.Env{
			Args:      args,
			Stdout:    io.Discard,
			Stderr:    inst.errb,
			LookupEnv: lookup,
			Signals:   inst.sig,
			Ready:     func(a cli.Addrs) { ready <- a },
		})
	}()
	select {
	case inst.addrs = <-ready:
	case code := <-inst.done:
		t.Fatalf("run exited with code %d before listening: %s", code, inst.errb.String())
	case <-time.After(10 * time.Second):
		t.Fatalf("run not listening after 10s: %s", inst.errb.String())
	}
	return inst
}

func (in *instance) stop(t *testing.T) int {
	t.Helper()
	in.sig <- syscall.SIGTERM
	select {
	case code := <-in.done:
		return code
	case <-time.After(10 * time.Second):
		t.Fatalf("run did not stop within 10s: %s", in.errb.String())
		return -1
	}
}

var httpClient = &http.Client{Timeout: 10 * time.Second}

func get(t *testing.T, url string) (*http.Response, []byte) {
	t.Helper()
	resp, err := httpClient.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

func tally(t *testing.T, base string, n int) (admitted int) {
	t.Helper()
	for range n {
		resp, body := get(t, base+"/rl")
		switch resp.StatusCode {
		case http.StatusOK:
			admitted++
		case http.StatusTooManyRequests:
			retry := resp.Header.Get("Retry-After")
			if v, err := strconv.Atoi(retry); err != nil || v < 1 {
				t.Fatalf("429 Retry-After %q: want an integer >= 1", retry)
			}
			var eb struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(body, &eb); err != nil {
				t.Fatalf("429 body %q: %v", body, err)
			}
			if eb.Error.Code != "rate_limited" {
				t.Fatalf("429 error code %q, want rate_limited", eb.Error.Code)
			}
		default:
			t.Fatalf("GET %s/rl: status %d, want 200 or 429", base, resp.StatusCode)
		}
	}
	return admitted
}

func metric(t *testing.T, adminAddr, series string) float64 {
	t.Helper()
	_, body := get(t, "http://"+adminAddr+"/metrics")
	for _, line := range strings.Split(string(body), "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err != nil {
				t.Fatalf("metric %s: %v", series, err)
			}
			return f
		}
	}
	return 0
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 10s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func deletePrefix(t *testing.T, addr, password string, db int, prefix string) {
	t.Helper()
	ctx := context.Background()
	c := goredis.NewClient(&goredis.Options{Addr: addr, Password: password, DB: db})
	defer c.Close()
	var cursor uint64
	for {
		keys, next, err := c.Scan(ctx, cursor, prefix+"*", 100).Result()
		if err != nil {
			t.Fatalf("scan %s*: %v", prefix, err)
		}
		if len(keys) > 0 {
			if err := c.Del(ctx, keys...).Err(); err != nil {
				t.Fatalf("delete %s*: %v", prefix, err)
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}
