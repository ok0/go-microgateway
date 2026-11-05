package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/internal/cli"
	"github.com/ok0/go-microgateway/internal/doccheck"
	"github.com/ok0/go-microgateway/internal/testwait"
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

	sentinel, err := net.Dial("tcp", probe.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer sentinel.Close()
	type acceptResult struct {
		conn net.Conn
		err  error
	}
	acc := make(chan acceptResult, 1)
	go func() {
		c, err := probe.Accept()
		acc <- acceptResult{c, err}
	}()
	// Connections are accepted in arrival order, so the first one is the sentinel unless validate connected earlier.
	first, _ := testwait.Recv(t, "the first connection to the Redis address to be accepted", acc, nil)
	if first.err != nil {
		t.Fatalf("Accept: %v", first.err)
	}
	defer first.conn.Close()
	if first.conn.RemoteAddr().String() != sentinel.LocalAddr().String() {
		t.Fatal("validate opened a connection to the Redis address")
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

func TestExamplesValidateWithBinaryPath(t *testing.T) {
	root, err := doccheck.Root()
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(root, "examples", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) < 3 {
		t.Fatalf("found %d examples/*.yaml files, want at least 3", len(paths))
	}
	lookup := doccheck.ExampleLookup(nil)
	for _, path := range paths {
		var out, errb bytes.Buffer
		code := gateway(context.Background(), cli.Env{
			Args:      []string{"validate", "-config", path},
			Stdout:    &out,
			Stderr:    &errb,
			LookupEnv: lookup,
		})
		if code != 0 {
			t.Errorf("validate %s exit %d, want 0: %s", path, code, errb.String())
			continue
		}
		if !strings.Contains(out.String(), "configuration is valid") {
			t.Errorf("validate %s stdout %q, want configuration is valid", path, out.String())
		}
	}

	memoryPath := filepath.Join(root, "examples", "memory.yaml")
	var out, errb bytes.Buffer
	if code := cli.Main(context.Background(), cli.Env{
		Args:      []string{"validate", "-config", memoryPath},
		Stdout:    &out,
		Stderr:    &errb,
		LookupEnv: lookup,
	}); code != 0 {
		t.Fatalf("core validate %s exit %d, want 0: %s", memoryPath, code, errb.String())
	}

	redisPath := filepath.Join(root, "examples", "redis.yaml")
	src, err := os.ReadFile(redisPath)
	if err != nil {
		t.Fatal(err)
	}
	out.Reset()
	errb.Reset()
	if code := cli.Main(context.Background(), cli.Env{
		Args:      []string{"validate", "-config", redisPath},
		Stdout:    &out,
		Stderr:    &errb,
		LookupEnv: lookup,
	}); code != 1 {
		t.Fatalf("core validate %s exit %d, want 1: %s", redisPath, code, errb.String())
	}
	want := fmt.Sprintf("stores[0].type (line %d): store type \"redis\" is not registered\n%s: 1 error(s)\n",
		lineOf(t, string(src), "type: redis"), redisPath)
	if errb.String() != want {
		t.Fatalf("core validate stderr %q, want %q", errb.String(), want)
	}
}

// completeConfig reports whether a yaml fence body is a complete configuration: its first line that is neither
// blank nor a comment starts with version:.
func completeConfig(body string) bool {
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		return strings.HasPrefix(line, "version:")
	}
	return false
}

// pipelineStages returns the stages of the first pipeline key in body, in either block or flow style.
func pipelineStages(body string) []string {
	lines := strings.Split(body, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "pipeline:") {
			continue
		}
		rest := strings.TrimSpace(strings.TrimPrefix(trimmed, "pipeline:"))
		var stages []string
		if strings.HasPrefix(rest, "[") {
			for _, s := range strings.Split(strings.Trim(rest, "[]"), ",") {
				stages = append(stages, strings.Trim(strings.TrimSpace(s), `"'`))
			}
			return stages
		}
		for _, next := range lines[i+1:] {
			item := strings.TrimSpace(next)
			if !strings.HasPrefix(item, "- ") {
				break
			}
			stages = append(stages, strings.Trim(strings.TrimSpace(strings.TrimPrefix(item, "- ")), `"'`))
		}
		return stages
	}
	return nil
}

func TestDocsConfigurationBlocks(t *testing.T) {
	root, err := doccheck.Root()
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{filepath.Join(root, "README.md")}
	docs, err := filepath.Glob(filepath.Join(root, "docs", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	paths = append(paths, docs...)
	lookup := doccheck.ExampleLookup(nil)

	complete := 0
	sawConfiguration, sawMultiInstance := false, false
	sawRejectedPipeline, sawValidPipeline := false, false
	for _, path := range paths {
		doc, err := doccheck.Load(path)
		if err != nil {
			t.Fatal(err)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatal(err)
		}
		rel = filepath.ToSlash(rel)
		for i, f := range doc.Fences {
			if f.Lang != "yaml" || !completeConfig(f.Body) {
				continue
			}
			complete++
			where := fmt.Sprintf("%s:%d", rel, f.Line)
			rejected := false
			switch f.Marker {
			case "Valid configuration:":
			case "Rejected configuration:":
				rejected = true
			default:
				t.Errorf("%s: unmarked complete configuration", where)
				continue
			}

			name := "valid.yaml"
			if rejected {
				name = "rejected.yaml"
			}
			dir := t.TempDir()
			blockPath := filepath.Join(dir, name)
			if err := os.WriteFile(blockPath, []byte(f.Body+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			var out, errb bytes.Buffer
			code := gateway(context.Background(), cli.Env{
				Args:      []string{"validate", "-config", blockPath},
				Stdout:    &out,
				Stderr:    &errb,
				LookupEnv: lookup,
			})
			if rel == "docs/configuration.md" {
				sawConfiguration = true
			}
			if rel == "docs/multi-instance.md" && strings.Contains(f.Body, "type: redis") {
				sawMultiInstance = true
			}
			if !rejected {
				if code != 0 {
					t.Errorf("%s: valid configuration exit %d, want 0: %s", where, code, errb.String())
				}
				stages := pipelineStages(f.Body)
				if f.Heading == "Pipeline ordering" && slices.Index(stages, "cache") >= 0 &&
					slices.Index(stages, "cache") < slices.Index(stages, "rate_limit") {
					sawValidPipeline = true
				}
				continue
			}

			if code != 1 {
				t.Errorf("%s: rejected configuration exit %d, want 1: %s", where, code, errb.String())
			}
			if i+1 >= len(doc.Fences) || doc.Fences[i+1].Lang != "text" {
				t.Errorf("%s: rejected configuration is not followed by a text fence", where)
				continue
			}
			got := strings.TrimSuffix(strings.ReplaceAll(errb.String(), blockPath, "rejected.yaml"), "\n")
			if doc.Fences[i+1].Body != got {
				t.Errorf("%s: text fence %q, want validate stderr %q", where, doc.Fences[i+1].Body, got)
			}
			if f.Heading == "Pipeline ordering" && strings.Contains(got, ".pipeline[") && strings.Contains(got, `stage "cache" must come after "auth"`) {
				sawRejectedPipeline = true
			}
		}
	}

	if complete < 4 {
		t.Fatalf("found %d complete configuration blocks, want at least 4", complete)
	}
	if !sawConfiguration {
		t.Fatal("no complete configuration block in docs/configuration.md")
	}
	if !sawMultiInstance {
		t.Fatal("no Redis configuration block in docs/multi-instance.md")
	}
	if !sawRejectedPipeline {
		t.Fatal("no rejected Pipeline ordering block with the cache-before-auth error")
	}
	if !sawValidPipeline {
		t.Fatal("no valid Pipeline ordering block that lists cache before rate_limit")
	}
}
