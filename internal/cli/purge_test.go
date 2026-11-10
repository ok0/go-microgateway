package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/internal/testwait"
)

const purgeCLIToken = "cli-tok-4Rt"

const purgeCLIHash = "sha256:7a8b10809cd81e905c6fb3f5b67ba230eb37f718e5ef5959aa952f3e095addcd"

func adminPurgePost(t *testing.T, client *http.Client, addr, host, token, body string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/cache/purge", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if host != "" {
		req.Host = host
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, data
}

func TestRunPurgeAdminOnly(t *testing.T) {
	echo := echoBackend(t, "api")
	cfg := `version: 1
listeners:
  - name: public
    address: "127.0.0.1:0"
upstreams:
  - name: api
    hosts:
      - url: "http://` + hostPort(echo) + `"
routes:
  - name: api
    match:
      path: /api/
      path_type: prefix
    upstream: api
    cache:
      enabled: true
admin:
  address: "127.0.0.1:0"
  probes:
    address: "127.0.0.1:0"
  auth:
    keys:
      - id: cli
        hash: "` + purgeCLIHash + `"
`
	h := startRun(t, writeTemp(t, withZeroShutdownDelay(t, cfg)))
	public := h.addrs.Listeners["public"].String()
	admin := h.addrs.Admin.String()
	client := &http.Client{Timeout: testwait.Guard(t)}

	t.Run("admin purge with token", func(t *testing.T) {
		resp, body := adminPurgePost(t, client, admin, "", purgeCLIToken, `{"all":true}`)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200; body %s", resp.StatusCode, body)
		}
		var got struct {
			Scope string `json:"scope"`
		}
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("decode body %q: %v", body, err)
		}
		if got.Scope != "instance" {
			t.Errorf("scope = %q, want instance; body %s", got.Scope, body)
		}
	})

	t.Run("purge on proxy listener", func(t *testing.T) {
		for _, host := range []string{"", admin} {
			resp, body := adminPurgePost(t, client, public, host, purgeCLIToken, `{"all":true}`)
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("host %q: status = %d, want 404; body %s", host, resp.StatusCode, body)
			}
			var e struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(body, &e); err != nil {
				t.Fatalf("decode error body %q: %v", body, err)
			}
			if e.Error.Code != "route_not_found" {
				t.Errorf("host %q: code = %q, want route_not_found", host, e.Error.Code)
			}
		}
	})

	for _, path := range []string{"/livez", "/readyz", "/config", "/upstreams", "/metrics"} {
		t.Run("admin "+path, func(t *testing.T) {
			resp, body := doGet(t, client, admin, path, "", nil)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200; body %s", resp.StatusCode, body)
			}
			if path != "/config" {
				return
			}
			if !strings.Contains(string(body), "REDACTED") {
				t.Errorf("config body lacks REDACTED: %s", body)
			}
			if strings.Contains(string(body), strings.TrimPrefix(purgeCLIHash, "sha256:")) {
				t.Errorf("config body reveals the key hash: %s", body)
			}
			if !regexp.MustCompile(`"id"\s*:\s*"cli"`).MatchString(string(body)) {
				t.Errorf("config body lacks key id cli: %s", body)
			}
		})
	}

	if code := h.stop(); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

func TestValidateAdminAuth(t *testing.T) {
	const badHash = "sha256:deadbeef-not-a-digest"
	otherHash := "sha256:" + strings.Repeat("ab", 32)
	doc := `version: 1
global:
  drain_timeout: -1s
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
    upstream: users
admin:
  address: "127.0.0.1:0"
  probes:
    address: "127.0.0.1:0"
  auth:
    keys:
      - id: cli
        hash: "` + purgeCLIHash + `"
      - id: cli
        hash: "` + otherHash + `"
      - id: other
        hash: "` + badHash + `"
`
	path := writeTemp(t, doc)
	code, _, errs := runCmd(t, "validate", "-config", path)
	if code == 0 {
		t.Fatalf("exit code = 0, want nonzero; stderr:\n%s", errs)
	}
	seedLine := lineOf(t, doc, "drain_timeout: -1s")
	wants := []string{
		"admin.auth.keys[1].id (line ",
		fmt.Sprintf("admin.auth.keys[2].hash (line %d)", lineOf(t, doc, badHash)),
		fmt.Sprintf("global.drain_timeout (line %d)", seedLine),
	}
	for _, w := range wants {
		if !strings.Contains(errs, w) {
			t.Errorf("stderr lacks %q\nstderr:\n%s", w, errs)
		}
	}
	for _, secret := range []string{purgeCLIHash, strings.TrimPrefix(purgeCLIHash, "sha256:"), badHash, otherHash} {
		if strings.Contains(errs, secret) {
			t.Errorf("stderr reveals hash text %q\nstderr:\n%s", secret, errs)
		}
	}
}
