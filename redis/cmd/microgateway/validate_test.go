package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/cli"
)

func lineOf(t *testing.T, src, substr string) int {
	t.Helper()
	for i, line := range strings.Split(src, "\n") {
		if strings.Contains(line, substr) {
			return i + 1
		}
	}
	t.Fatalf("%q not found in document", substr)
	return 0
}

func TestValidateRedisStore(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { probe.Close() })
	pw := uniqueSecret(t, "pw-")
	src := instanceDoc(docOpts{
		Backend:   backend(t),
		RedisAddr: probe.Addr().String(),
		Prefix:    uniquePrefix(t),
		Password:  true,
		UseStore:  true,
	})
	path := writeDoc(t, src)

	var out, errb bytes.Buffer
	code := gateway(context.Background(), cli.Env{
		Args:      []string{"validate", "-config", path},
		Stdout:    &out,
		Stderr:    &errb,
		LookupEnv: lookupWith(map[string]string{"MG_TEST_REDIS_PASSWORD": pw}),
	})
	if code != 0 {
		t.Fatalf("validate exit %d, want 0: %s", code, errb.String())
	}
	if !strings.Contains(out.String(), "configuration is valid") {
		t.Fatalf("stdout %q, want configuration is valid", out.String())
	}

	tcp := probe.(*net.TCPListener)
	if err := tcp.SetDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	conn, err := probe.Accept()
	if err == nil {
		conn.Close()
		t.Fatal("validate opened a connection to the Redis address")
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Accept: %v, want a deadline timeout", err)
	}
}

const storeErrBase = `version: 1
listeners:
  - name: public
    address: "127.0.0.1:0"
upstreams:
  - name: up
    hosts:
      - url: "http://127.0.0.1:9001"
`

const plainRoute = `routes:
  - name: rl
    match:
      path: /rl
    upstream: up
`

func TestValidateRedisStoreErrors(t *testing.T) {
	unknownSrc := storeErrBase + "stores:\n  - name: shared\n    type: memcache\n" + plainRoute
	missingSrc := storeErrBase + "stores:\n  - name: shared\n    type: redis\n" + plainRoute
	onePassSrc := storeErrBase + "stores:\n" +
		"  - name: a\n    type: memcache\n" +
		"  - name: b\n    type: redis\n" +
		"routes:\n" +
		"  - name: rl\n    match:\n      path: /rl\n    upstream: up\n" +
		"    rate_limit:\n      rate: 1\n      per: 1h\n      burst: 5\n      key:\n        source: route\n" +
		"      store: missing\n      on_store_error: deny\n      store_timeout: 2s\n"

	cases := []struct {
		name string
		src  string
		want []string
	}{
		{
			name: "unknown type",
			src:  unknownSrc,
			want: []string{
				fmt.Sprintf(`stores[0].type (line %d): store type "memcache" is not registered`, lineOf(t, unknownSrc, "type: memcache")),
			},
		},
		{
			name: "missing block",
			src:  missingSrc,
			want: []string{
				fmt.Sprintf(`stores[0].redis (line %d): is required for type redis`, lineOf(t, missingSrc, "- name: shared")),
			},
		},
		{
			name: "one pass",
			src:  onePassSrc,
			want: []string{
				fmt.Sprintf(`stores[0].type (line %d): store type "memcache" is not registered`, lineOf(t, onePassSrc, "type: memcache")),
				fmt.Sprintf(`stores[1].redis (line %d): is required for type redis`, lineOf(t, onePassSrc, "- name: b")),
				fmt.Sprintf(`routes[0].rate_limit.store (line %d): store "missing" is not defined in stores`, lineOf(t, onePassSrc, "store: missing")),
				": 3 error(s)\n",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeDoc(t, tc.src)
			var out, errb bytes.Buffer
			code := gateway(context.Background(), cli.Env{
				Args:   []string{"validate", "-config", path},
				Stdout: &out,
				Stderr: &errb,
			})
			if code != 1 {
				t.Fatalf("validate exit %d, want 1: %s", code, errb.String())
			}
			for _, w := range tc.want {
				if !strings.Contains(errb.String(), w) {
					t.Fatalf("stderr missing %q:\n%s", w, errb.String())
				}
			}
		})
	}
}
