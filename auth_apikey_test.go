package lychgate

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ok0/lychgate/config"
)

const apkRoute = "  - name: r\n    match:\n      path: /x\n    upstream: be\n    auth:\n      mode: apikey\n      providers: [keys1]\n"

func apkAuth(hash string, forward bool) string {
	return fmt.Sprintf("auth:\n  api_keys:\n    - name: keys1\n      query: api_key\n      forward_credentials: %t\n      keys:\n        - id: k1\n          hash: %q\n          consumer: alice\n", forward, hash)
}

func apkKeyFileAuth() string {
	return "auth:\n  api_keys:\n    - name: keys1\n      key_file: keys.yaml\n"
}

func apkKeysFile(hash string) string {
	return fmt.Sprintf("keys:\n  - id: file-1\n    hash: %q\n    consumer: bob\n", hash)
}

func apkHdr(v string) http.Header {
	return http.Header{"X-API-Key": {v}}
}

func apkWrite(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}

func apkFromFile(t *testing.T, e *auEnv, path string, opts ...Option) *Gateway {
	t.Helper()
	cfg, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(cfg, append(opts, WithLogger(e.logger()), withDialer(e.n.DialContext))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func TestAuthAPIKeyLocations(t *testing.T) {
	e := newAuEnv(t)
	be, _ := auBackend(e)
	key := e.secret("apk-alpha-7c1d0e9f2b3a4857")
	other := e.secret("apk-omega-0a9b8c7d6e5f4321")
	hash := e.secret(auHash(key))
	g := e.gateway(auDoc(be.URL, apkAuth(hash, false), apkRoute))

	if rec := e.get(g, "/x", apkHdr(key)); rec.Code != http.StatusOK {
		t.Fatalf("key in header: status = %d, want 200", rec.Code)
	}
	if rec := e.get(g, "/x?api_key="+key, nil); rec.Code != http.StatusOK {
		t.Fatalf("key in query: status = %d, want 200", rec.Code)
	}
	if rec := e.get(g, "/x?api_key="+other, apkHdr(key)); rec.Code != http.StatusOK {
		t.Fatalf("valid header with invalid query: status = %d, want 200", rec.Code)
	}
	if rec := e.get(g, "/x?api_key="+key, apkHdr("")); rec.Code != http.StatusOK {
		t.Fatalf("empty header with valid query: status = %d, want 200", rec.Code)
	}
	rec := e.get(g, "/x?api_key="+key, apkHdr(other))
	auAssert401(t, rec, "ApiKey")
	rec = e.get(g, "/x", apkHdr(other))
	auAssert401(t, rec, "ApiKey")
	if got := auFailures(t, g, "r", "invalid_key"); got != 2 {
		t.Fatalf("invalid_key failures = %v, want 2", got)
	}
}

func TestAuthAPIKeyStripping(t *testing.T) {
	e := newAuEnv(t)
	be, rec := auBackend(e)
	key := e.secret("apk-strip-3f2e1d0c9b8a7654")
	hash := e.secret(auHash(key))
	target := "/x?b=2&api_key=" + key + "&a=%2F"

	g := e.gateway(auDoc(be.URL, apkAuth(hash, false), apkRoute))
	if res := e.get(g, target, apkHdr(key)); res.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.Code)
	}
	h, q := rec.Last()
	if h == nil {
		t.Fatal("backend received no request")
	}
	if _, ok := h["X-Api-Key"]; ok {
		t.Error("X-Api-Key reached the backend")
	}
	if q != "b=2&a=%2F" {
		t.Errorf("RawQuery = %q, want %q", q, "b=2&a=%2F")
	}

	kept := e.gateway(auDoc(be.URL, apkAuth(hash, true), apkRoute))
	if res := e.get(kept, target, apkHdr(key)); res.Code != http.StatusOK {
		t.Fatalf("forwarding: status = %d, want 200", res.Code)
	}
	h, q = rec.Last()
	if h == nil {
		t.Fatal("backend received no request")
	}
	if got := h.Get("X-Api-Key"); got != key {
		t.Errorf("forwarded X-Api-Key = %q, want the presented key", got)
	}
	if want := "b=2&api_key=" + key + "&a=%2F"; q != want {
		t.Errorf("forwarding RawQuery = %q, want %q", q, want)
	}
}

func TestAuthAPIKeyFile(t *testing.T) {
	e := newAuEnv(t)
	be, _ := auBackend(e)
	dir := t.TempDir()
	keyA := e.secret("apk-file-a-6d5c4b3a2f1e0d9c")
	keyB := e.secret("apk-file-b-0f1e2d3c4b5a6978")
	hashA := e.secret(auHash(keyA))
	hashB := e.secret(auHash(keyB))
	keysPath := filepath.Join(dir, "keys.yaml")
	apkWrite(t, keysPath, apkKeysFile(hashA))
	cfgPath := writeConfig(t, dir, auDoc(be.URL, apkKeyFileAuth(), apkRoute))

	g := apkFromFile(t, e, cfgPath, withClock(e.clk))
	if res := e.get(g, "/x", apkHdr(keyA)); res.Code != http.StatusOK {
		t.Fatalf("key A before reload: status = %d, want 200", res.Code)
	}

	apkWrite(t, keysPath, apkKeysFile(hashB))
	if err := g.ReloadFile(cfgPath); err != nil {
		t.Fatalf("ReloadFile = %v, want nil", err)
	}
	if res := e.get(g, "/x", apkHdr(keyB)); res.Code != http.StatusOK {
		t.Fatalf("key B after reload: status = %d, want 200", res.Code)
	}
	auAssert401(t, e.get(g, "/x", apkHdr(keyA)), "ApiKey")

	apkWrite(t, keysPath, "keys:\n  - id: file-1\n    hash: \"sha256:not-a-hash\"\n    consumer: bob\n")
	err := g.ReloadFile(cfgPath)
	if err == nil {
		t.Fatal("ReloadFile with a bad key file = nil, want error")
	}
	if !strings.Contains(err.Error(), "keys.yaml (line 3)") {
		t.Fatalf("ReloadFile error = %q, want it to name keys.yaml (line 3)", err.Error())
	}
	if res := e.get(g, "/x", apkHdr(keyB)); res.Code != http.StatusOK {
		t.Fatalf("key B after failed reload: status = %d, want 200", res.Code)
	}
}

func TestWatchFileReloadsKeyFile(t *testing.T) {
	e := newAuEnv(t)
	be, _ := auBackend(e)
	dir := t.TempDir()
	keyA := e.secret("apk-watch-a-2b3c4d5e6f708192")
	keyB := e.secret("apk-watch-b-9a8b7c6d5e4f3021")
	hashA := e.secret(auHash(keyA))
	hashB := e.secret(auHash(keyB))
	keysPath := filepath.Join(dir, "keys.yaml")
	apkWrite(t, keysPath, apkKeysFile(hashA))
	cfgPath := writeConfig(t, dir, auDoc(be.URL, apkKeyFileAuth(), apkRoute))
	g := apkFromFile(t, e, cfgPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- g.WatchFile(ctx, cfgPath) }()
	eventually(t, func() bool {
		_, _, u := counters(g)
		return u == 1
	})

	tmp := filepath.Join(dir, "keys.yaml.tmp")
	apkWrite(t, tmp, apkKeysFile(hashB))
	if err := os.Rename(tmp, keysPath); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return e.get(g, "/x", apkHdr(keyB)).Code == http.StatusOK })
	auAssert401(t, e.get(g, "/x", apkHdr(keyA)), "ApiKey")

	cancel()
	if err := awaitWatch(t, done); err != nil {
		t.Fatalf("WatchFile = %v, want nil", err)
	}
}
