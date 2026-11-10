package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
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
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testwait"
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
		_, _ = fmt.Fprint(w, "ok")
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
	Cache      bool
	AdminToken string
}

func adminKeyHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func instanceDoc(o docOpts) string {
	var b strings.Builder
	fmt.Fprintf(&b, "version: 1\n")
	// Instances stop by SIGTERM without advancing their fake clock, so the fixture keeps the immediate stop explicitly.
	fmt.Fprintf(&b, "global:\n  shutdown_delay: 0s\n")
	fmt.Fprintf(&b, "listeners:\n  - name: public\n    address: \"127.0.0.1:0\"\n")
	fmt.Fprintf(&b, "admin:\n  address: \"127.0.0.1:0\"\n")
	if o.Cache {
		fmt.Fprintf(&b, "  auth:\n    keys:\n      - id: ops\n        hash: \"%s\"\n", adminKeyHash(o.AdminToken))
	}
	fmt.Fprintf(&b, "upstreams:\n  - name: up\n    hosts:\n      - url: \"%s\"\n", o.Backend)
	fmt.Fprintf(&b, "stores:\n  - name: shared\n    type: redis\n    redis:\n")
	fmt.Fprintf(&b, "      addresses: [\"%s\"]\n", o.RedisAddr)
	if o.Password {
		fmt.Fprintf(&b, "      password: \"${MG_TEST_REDIS_PASSWORD}\"\n")
	}
	fmt.Fprintf(&b, "      db: %d\n", o.DB)
	fmt.Fprintf(&b, "      key_prefix: \"%s\"\n", o.Prefix)
	if o.Cache {
		fmt.Fprintf(&b, "cache:\n  store: shared\n")
	}
	fmt.Fprintf(&b, "routes:\n  - name: rl\n    match:\n      path: /rl\n    upstream: up\n")
	fmt.Fprintf(&b, "    rate_limit:\n      rate: 1\n      per: 1h\n      burst: 5\n      key:\n        source: route\n")
	if o.UseStore {
		fmt.Fprintf(&b, "      store: shared\n      on_store_error: deny\n      store_timeout: 2s\n")
	}
	if o.Cache {
		fmt.Fprintf(&b, "  - name: c\n    match:\n      path: /c\n    upstream: up\n    cache:\n      enabled: true\n")
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
	clk   *clock.Fake
}

func startInstance(t *testing.T, ctx context.Context, path string, lookup func(string) (string, bool), extraArgs ...string) *instance {
	t.Helper()
	inst := &instance{sig: make(chan os.Signal, 2), done: make(chan int, 1), errb: &syncBuffer{}, clk: clock.NewFake(time.Now())}
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
			Clock:     inst.clk,
		})
	}()
	testwait.Until(t, "run to report its listeners", func() bool {
		select {
		case inst.addrs = <-ready:
			return true
		case code := <-inst.done:
			t.Fatalf("run exited with code %d before listening: %s", code, inst.errb.String())
		default:
		}
		return false
	}, inst.errb.String)
	return inst
}

func (in *instance) stop(t *testing.T) int {
	t.Helper()
	in.sig <- syscall.SIGTERM
	code, _ := testwait.Recv(t, "run to stop after SIGTERM", in.done, in.errb.String)
	return code
}

func get(t *testing.T, url string) (*http.Response, []byte) {
	t.Helper()
	client := &http.Client{Timeout: testwait.Guard(t)}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
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

func eventually(t *testing.T, what string, cond func() bool, state func() string) {
	t.Helper()
	testwait.Until(t, what, cond, state)
}

func eventuallyAdvancing(t *testing.T, what string, fake *clock.Fake, step time.Duration, cond func() bool, state func() string) {
	t.Helper()
	var advanced time.Duration
	testwait.Until(t, what, func() bool {
		if cond() {
			return true
		}
		if fake.Due(step) > 0 {
			fake.Advance(step)
			advanced += step
		}
		return false
	}, func() string {
		s := fmt.Sprintf("fake clock advanced %v; timers due within %v: %d", advanced, step, fake.Due(step))
		if state != nil {
			s = state() + "; " + s
		}
		return s
	})
}

// newRawClient returns a go-redis client for o that has no limit on the wall clock: it sets no socket deadline, waits
// for a pool connection until the call's context ends, never reaps idle connections, and bounds dials on a fake clock
// that nothing advances. These are the values the redis store uses.
func newRawClient(o *goredis.Options) *goredis.Client {
	fake := clock.NewFake(time.Now())
	o.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		dctx, cancel := context.WithCancel(ctx)
		defer cancel()
		bound := fake.AfterFunc(time.Second, cancel)
		defer bound.Stop()
		var d net.Dialer
		return d.DialContext(dctx, network, addr)
	}
	o.DialTimeout = -1
	o.ReadTimeout = -2
	o.WriteTimeout = -2
	o.PoolTimeout = 100 * 365 * 24 * time.Hour
	o.ConnMaxIdleTime = -1
	return goredis.NewClient(o)
}

func deletePrefix(t *testing.T, addr, password string, db int, prefix string) {
	t.Helper()
	ctx := context.Background()
	c := newRawClient(&goredis.Options{Addr: addr, Password: password, DB: db})
	defer func() { _ = c.Close() }()
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
