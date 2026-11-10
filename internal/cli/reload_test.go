package cli

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/testcert"
	"github.com/ok0/go-microgateway/internal/testwait"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func logAfter(buf *syncBuffer, marker string) string {
	lines := strings.SplitAfter(buf.String(), "\n")
	for i, line := range lines {
		if strings.Contains(line, marker) {
			return strings.Join(lines[i+1:], "")
		}
	}
	return ""
}

func metric(t *testing.T, adminAddr, series string) float64 {
	t.Helper()
	client := &http.Client{Timeout: testwait.Guard(t)}
	_, body := doGet(t, client, adminAddr, "/metrics", "", nil)
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

func restartFields(t *testing.T, buf *syncBuffer) []string {
	t.Helper()
	var fields []string
	for _, line := range strings.Split(buf.String(), "\n") {
		if !strings.Contains(line, "restart required; change not applied") {
			continue
		}
		var rec struct {
			Field string `json:"field"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode log record %q: %v", line, err)
		}
		fields = append(fields, rec.Field)
	}
	return fields
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func replaceAtomic(t *testing.T, path string, data []byte) {
	t.Helper()
	tmp := path + ".tmp"
	writeFile(t, tmp, data)
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

func statusOf(client *http.Client, url string) int {
	resp, err := client.Get(url)
	if err != nil {
		return -1
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func genPair(t *testing.T, serial int64) (certPEM, keyPEM []byte) {
	t.Helper()
	return testcert.Generate(t, serial, time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))
}

func writePair(t *testing.T, certPath, keyPath string, serial int64) {
	t.Helper()
	c, k := genPair(t, serial)
	writeFile(t, certPath, c)
	writeFile(t, keyPath, k)
}

func mustSymlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
}

func insecureTLS() *tls.Config {
	return &tls.Config{InsecureSkipVerify: true}
}

func serialOf(client *http.Client, addr string) (int64, error) {
	resp, err := client.Get("https://" + addr + "/")
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}
	return resp.TLS.PeerCertificates[0].SerialNumber.Int64(), nil
}

func tlsSerial(t *testing.T, client *http.Client, addr string) int64 {
	t.Helper()
	s, err := serialOf(client, addr)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func twoRouteConfig(aURL, bURL string, routeB bool) string {
	extra := ""
	if routeB {
		extra = `  - name: b
    match:
      path: /b
      path_type: prefix
    upstream: b
`
	}
	return fmt.Sprintf(`version: 1
listeners:
  - name: public
    address: "127.0.0.1:0"
upstreams:
  - name: a
    hosts:
      - url: %q
  - name: b
    hosts:
      - url: %q
routes:
  - name: a
    match:
      path: /a
      path_type: prefix
    upstream: a
%sadmin:
  address: "127.0.0.1:0"
`, aURL, bURL, extra)
}

func TestRunReloadOnFileEdit(t *testing.T) {
	a := echoBackend(t, "a")
	b := echoBackend(t, "b")
	path := writeTemp(t, withZeroShutdownDelay(t, twoRouteConfig(a.URL, b.URL, false)))

	h := startRun(t, path)
	public := h.addrs.Listeners["public"].String()
	client := &http.Client{Timeout: testwait.Guard(t)}
	if resp, body := doGet(t, client, public, "/a/x", "", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("/a status = %d, want 200; body %s", resp.StatusCode, body)
	}

	replaceAtomic(t, path, []byte(withZeroShutdownDelay(t, twoRouteConfig(a.URL, b.URL, true))))
	eventuallyAdvancing(t, "/b served after the rename", h.clk, reloadStep, func() bool { return statusOf(client, "http://"+public+"/b/y") == http.StatusOK }, nil)
	resp, body := doGet(t, client, public, "/b/y", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/b status = %d, want 200; body %s", resp.StatusCode, body)
	}
	if echo := decodeEcho(t, body); echo.Backend != "b" {
		t.Errorf("/b backend = %q, want b", echo.Backend)
	}

	resp, body = doGet(t, client, public, "/a/x", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/a status after reload = %d, want 200; body %s", resp.StatusCode, body)
	}
	if echo := decodeEcho(t, body); echo.Backend != "a" {
		t.Errorf("/a backend after reload = %q, want a", echo.Backend)
	}

	if code := h.stop(); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

func TestRunSIGHUPReloadsEnvironment(t *testing.T) {
	backend := echoBackend(t, "users")
	t.Setenv("MICROGATEWAY_TEST_ENV", "one")
	path := writeTemp(t, fmt.Sprintf(`version: 1
global:
  shutdown_delay: 0s
listeners:
  - name: public
    address: "127.0.0.1:0"
upstreams:
  - name: users
    hosts:
      - url: %q
routes:
  - name: all
    match:
      path: /
      path_type: prefix
    upstream: users
    response_headers:
      set:
        X-Env: "${MICROGATEWAY_TEST_ENV}"
admin:
  address: "127.0.0.1:0"
`, backend.URL))

	var errb syncBuffer
	h := startRunArgs(t, []string{"-config", path, "-log-level", "debug"}, &errb)
	public := h.addrs.Listeners["public"].String()
	admin := h.addrs.Admin.String()
	client := &http.Client{Timeout: testwait.Guard(t)}
	resp, _ := doGet(t, client, public, "/", "", nil)
	if got := resp.Header.Get("X-Env"); got != "one" {
		t.Fatalf("X-Env before SIGHUP = %q, want one", got)
	}

	const unchanged = `microgateway_config_reloads_total{result="unchanged"}`
	testwait.Until(t, "watcher startup attempt counted", func() bool { return metric(t, admin, unchanged) >= 1 }, errb.String)

	t.Setenv("MICROGATEWAY_TEST_ENV", "two")
	h.sig <- syscall.SIGHUP
	eventuallyAdvancing(t, "X-Env two after SIGHUP", h.clk, reloadStep, func() bool {
		resp, _ := doGet(t, client, public, "/", "", nil)
		return resp.Header.Get("X-Env") == "two"
	}, errb.String)

	before := metric(t, admin, unchanged)
	h.sig <- syscall.SIGHUP
	eventuallyAdvancing(t, "unchanged reload counted after the second SIGHUP", h.clk, reloadStep, func() bool { return metric(t, admin, unchanged) == before+1 }, errb.String)

	if code := h.stop(); code != 0 {
		t.Errorf("exit code = %d, want 0; log:\n%s", code, errb.String())
	}
}

func TestRunReloadFailureKeepsServing(t *testing.T) {
	backend := echoBackend(t, "users")
	valid := func(route string) string {
		return fmt.Sprintf(`version: 1
global:
  shutdown_delay: 0s
listeners:
  - name: public
    address: "127.0.0.1:0"
upstreams:
  - name: users
    hosts:
      - url: %q
routes:
  - name: all
    match:
      path: %s
      path_type: prefix
    upstream: users
admin:
  address: "127.0.0.1:0"
`, backend.URL, route)
	}
	path := writeTemp(t, valid("/"))

	var errb syncBuffer
	h := startRunArgs(t, []string{"-config", path, "-log-level", "debug"}, &errb)
	public := h.addrs.Listeners["public"].String()
	admin := h.addrs.Admin.String()
	client := &http.Client{Timeout: testwait.Guard(t)}
	const failure = `microgateway_config_reloads_total{result="failure"}`
	const unchanged = `microgateway_config_reloads_total{result="unchanged"}`
	testwait.Until(t, "watcher startup attempt counted", func() bool { return metric(t, admin, unchanged) >= 1 }, errb.String)
	before := metric(t, admin, failure)

	writeFile(t, path, []byte(`version: 1
listeners:
  - name: public
    address: "127.0.0.1:0"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
routes:
  - name: all
    match:
      path: /
      path_type: prefix
    upstream: missing
admin:
  address: "127.0.0.1:0"
`))
	eventuallyAdvancing(t, "failed reload logged", h.clk, reloadStep, func() bool { return strings.Contains(errb.String(), "configuration reload failed") }, errb.String)
	if !strings.Contains(errb.String(), "routes[0].upstream (line ") {
		t.Errorf("log does not name routes[0].upstream with its line:\n%s", errb.String())
	}
	eventuallyAdvancing(t, "failed reload counted", h.clk, reloadStep, func() bool { return metric(t, admin, failure) == before+1 }, errb.String)
	select {
	case code := <-h.done:
		t.Fatalf("process exited with code %d after a failed reload; log:\n%s", code, errb.String())
	default:
	}
	resp, body := doGet(t, client, public, "/", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status after failed reload = %d, want 200; body %s", resp.StatusCode, body)
	}

	writeFile(t, path, []byte(valid("/edited")))
	eventuallyAdvancing(t, "/edited served after the fix", h.clk, reloadStep, func() bool { return statusOf(client, "http://"+public+"/edited") == http.StatusOK }, errb.String)

	if code := h.stop(); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

func TestRunRestartRequiredWarnings(t *testing.T) {
	backend := echoBackend(t, "users")
	configOf := func(listen, admin, level string, newRoute bool) string {
		extra := ""
		if newRoute {
			extra = `  - name: new
    match:
      path: /new
      path_type: prefix
    upstream: users
`
		}
		return fmt.Sprintf(`version: 1
global:
  shutdown_delay: 0s
  log:
    level: %s
listeners:
  - name: public
    address: %q
upstreams:
  - name: users
    hosts:
      - url: %q
routes:
  - name: old
    match:
      path: /old
      path_type: prefix
    upstream: users
%sadmin:
  address: %q
`, level, listen, backend.URL, extra, admin)
	}
	path := writeTemp(t, configOf("127.0.0.1:0", "127.0.0.1:0", "info", false))

	var errb syncBuffer
	h := startRunArgs(t, []string{"-config", path}, &errb)
	listen := h.addrs.Listeners["public"].String()
	client := &http.Client{Timeout: testwait.Guard(t)}

	writeFile(t, path, []byte(configOf("localhost:0", "localhost:0", "warn", true)))
	eventuallyAdvancing(t, "/new served after the edit", h.clk, reloadStep, func() bool { return statusOf(client, "http://"+listen+"/new") == http.StatusOK }, errb.String)
	eventually(t, "three restart-required warnings logged", func() bool { return len(restartFields(t, &errb)) >= 3 })

	want := []string{"listeners[0].address", "admin.address", "global.log.level"}
	if got := restartFields(t, &errb); !slices.Equal(got, want) {
		t.Errorf("restart required fields = %v, want %v", got, want)
	}

	if code := h.stop(); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

func TestRunNoReloadAfterShutdown(t *testing.T) {
	backend := echoBackend(t, "users")
	global := "global:\n  shutdown_delay: 1s\n"
	path := writeTemp(t, lifecycleConfig(global, backend.URL))

	var errb syncBuffer
	h := startRunArgs(t, []string{"-config", path, "-log-level", "debug"}, &errb)

	h.sig <- syscall.SIGTERM
	eventually(t, "shutdown to begin", func() bool { return strings.Contains(errb.String(), "shutting down") })
	h.sig <- syscall.SIGHUP
	eventually(t, "SIGHUP ignored during shutdown", func() bool { return strings.Contains(errb.String(), "reload ignored; shutting down") })
	writeFile(t, path, []byte(lifecycleConfig(global, backend.URL)+`  - name: extra
    match:
      path: /extra
      path_type: prefix
    upstream: backend
`))
	awaitDue(t, h.clk, 1, time.Second, "shutdown delay timer armed", errb.String)
	h.clk.Advance(time.Second)
	if code := h.wait(t); code != 0 {
		t.Errorf("exit code = %d, want 0; log:\n%s", code, errb.String())
	}
	after := logAfter(&errb, "shutting down")
	for _, msg := range []string{"configuration reloaded", "configuration unchanged", "configuration reload failed"} {
		if strings.Contains(after, msg) {
			t.Errorf("log after shutdown contains %q:\n%s", msg, after)
		}
	}
}

func TestRunCertificateReload(t *testing.T) {
	dir := t.TempDir()
	backend := echoBackend(t, "users")

	inCert := filepath.Join(dir, "inplace", "cert.pem")
	inKey := filepath.Join(dir, "inplace", "key.pem")
	renCert := filepath.Join(dir, "rename", "cert.pem")
	renKey := filepath.Join(dir, "rename", "key.pem")
	k8sDir := filepath.Join(dir, "k8s", "certs")
	k8sCert := filepath.Join(k8sDir, "tls.crt")
	k8sKey := filepath.Join(k8sDir, "tls.key")
	pairDir := filepath.Join(dir, "pair")
	for _, d := range []string{filepath.Dir(inCert), filepath.Dir(renCert), k8sDir, pairDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	writePair(t, inCert, inKey, 1)
	writePair(t, renCert, renKey, 1)
	v1 := filepath.Join(k8sDir, "..v1")
	if err := os.Mkdir(v1, 0o700); err != nil {
		t.Fatal(err)
	}
	writePair(t, filepath.Join(v1, "tls.crt"), filepath.Join(v1, "tls.key"), 1)
	mustSymlink(t, "..v1", filepath.Join(k8sDir, "..data"))
	mustSymlink(t, "..data/tls.crt", k8sCert)
	mustSymlink(t, "..data/tls.key", k8sKey)

	certConfig := func(renC, renK string) string {
		return fmt.Sprintf(`version: 1
global:
  shutdown_delay: 0s
  reload:
    poll_interval: 100ms
listeners:
  - name: inplace
    address: "127.0.0.1:0"
    tls:
      cert_file: %q
      key_file: %q
  - name: rename
    address: "127.0.0.1:0"
    tls:
      cert_file: %q
      key_file: %q
  - name: k8s
    address: "127.0.0.1:0"
    tls:
      cert_file: %q
      key_file: %q
upstreams:
  - name: users
    hosts:
      - url: %q
routes:
  - name: all
    match:
      path: /
      path_type: prefix
    upstream: users
admin:
  address: "127.0.0.1:0"
`, inCert, inKey, renC, renK, k8sCert, k8sKey, backend.URL)
	}
	path := writeTemp(t, certConfig(renCert, renKey))

	var errb syncBuffer
	h := startRunArgs(t, []string{"-config", path}, &errb)
	admin := h.addrs.Admin.String()
	addrOf := func(listener string) string { return h.addrs.Listeners[listener].String() }
	fresh := &http.Client{
		Timeout:   testwait.Guard(t),
		Transport: &http.Transport{TLSClientConfig: insecureTLS(), DisableKeepAlives: true},
	}
	const success = `microgateway_config_reloads_total{result="success"}`
	const failure = `microgateway_config_reloads_total{result="failure"}`

	swapped := func(name, listener string, oldSerial, newSerial int64, apply func()) {
		t.Helper()
		addr := addrOf(listener)
		held := &http.Client{
			Timeout:   testwait.Guard(t),
			Transport: &http.Transport{TLSClientConfig: insecureTLS(), MaxConnsPerHost: 1},
		}
		if got := tlsSerial(t, held, addr); got != oldSerial {
			t.Fatalf("%s: serial before change = %d, want %d", name, got, oldSerial)
		}
		before := metric(t, admin, success)
		apply()
		eventuallyAdvancing(t, name+": new serial served", h.clk, reloadStep, func() bool {
			s, err := serialOf(fresh, addr)
			return err == nil && s == newSerial
		}, errb.String)
		if got := tlsSerial(t, held, addr); got != oldSerial {
			t.Errorf("%s: held connection serial = %d, want %d", name, got, oldSerial)
		}
		eventuallyAdvancing(t, name+": reload counted", h.clk, reloadStep, func() bool { return metric(t, admin, success) >= before+1 }, errb.String)
		if got := metric(t, admin, success); got != before+1 {
			t.Errorf("%s: success count moved by %v, want 1", name, got-before)
		}
	}

	swapped("in-place overwrite", "inplace", 1, 2, func() {
		c, k := genPair(t, 2)
		writeFile(t, inCert, c)
		writeFile(t, inKey, k)
	})

	swapped("atomic rename", "rename", 1, 3, func() {
		c, k := genPair(t, 3)
		replaceAtomic(t, renCert, c)
		replaceAtomic(t, renKey, k)
	})

	swapped("kubernetes ..data swap", "k8s", 1, 4, func() {
		c, k := genPair(t, 4)
		v2 := filepath.Join(k8sDir, "..v2")
		if err := os.Mkdir(v2, 0o700); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(v2, "tls.crt"), c)
		writeFile(t, filepath.Join(v2, "tls.key"), k)
		mustSymlink(t, "..v2", filepath.Join(k8sDir, "..data_tmp"))
		if err := os.Rename(filepath.Join(k8sDir, "..data_tmp"), filepath.Join(k8sDir, "..data")); err != nil {
			t.Fatal(err)
		}
	})

	beforeFail := metric(t, admin, failure)
	mismatched, _ := genPair(t, 5)
	replaceAtomic(t, renCert, mismatched)
	eventuallyAdvancing(t, "failed reload counted", h.clk, reloadStep, func() bool { return metric(t, admin, failure) >= beforeFail+1 }, errb.String)
	if got := metric(t, admin, failure); got != beforeFail+1 {
		t.Errorf("failure count moved by %v, want 1", got-beforeFail)
	}
	if got := tlsSerial(t, fresh, addrOf("rename")); got != 3 {
		t.Errorf("serial after mismatched pair = %d, want previous serial 3", got)
	}

	beforeEdit := metric(t, admin, success)
	c6, k6 := genPair(t, 6)
	pairCert := filepath.Join(pairDir, "cert.pem")
	pairKey := filepath.Join(pairDir, "key.pem")
	writeFile(t, pairCert, c6)
	writeFile(t, pairKey, k6)
	writeFile(t, path, []byte(certConfig(pairCert, pairKey)))
	eventuallyAdvancing(t, "serial 6 served after the YAML edit", h.clk, reloadStep, func() bool {
		s, err := serialOf(fresh, addrOf("rename"))
		return err == nil && s == 6
	}, errb.String)
	eventuallyAdvancing(t, "YAML edit reload counted", h.clk, reloadStep, func() bool { return metric(t, admin, success) >= beforeEdit+1 }, errb.String)
	if got := metric(t, admin, success); got != beforeEdit+1 {
		t.Errorf("success count moved by %v after YAML edit, want 1", got-beforeEdit)
	}

	if code := h.stop(); code != 0 {
		t.Errorf("exit code = %d, want 0; log:\n%s", code, errb.String())
	}
}
