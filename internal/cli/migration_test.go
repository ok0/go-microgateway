package cli

import (
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/http"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/doccheck"
	"github.com/ok0/go-microgateway/internal/testwait"
	"go.yaml.in/yaml/v3"
)

var restoreKeys = []string{"global.shutdown_delay", "global.drain_timeout", "admin.probes.enabled", "admin.auth.management"}

const restoreRelease, restoreSubsection = "v0.2.0", "Breaking"

const migrationBase = `version: 1
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
  address: "0.0.0.0:0"
`

type managementConfig struct {
	Global struct {
		ShutdownDelay string `json:"shutdown_delay"`
		DrainTimeout  string `json:"drain_timeout"`
	} `json:"global"`
}

func dedent(body string) string {
	lines := strings.Split(body, "\n")
	common := -1
	for _, l := range lines {
		if strings.TrimSpace(l) == "" {
			continue
		}
		n := len(l) - len(strings.TrimLeft(l, " \t"))
		if common < 0 || n < common {
			common = n
		}
	}
	if common <= 0 {
		return body
	}
	for i, l := range lines {
		if strings.TrimSpace(l) == "" {
			lines[i] = ""
			continue
		}
		lines[i] = l[common:]
	}
	return strings.Join(lines, "\n")
}

func flattenYAML(body string) (map[string]string, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(dedent(body)), &doc); err != nil {
		return nil, fmt.Errorf("restore block is not valid YAML: %w", err)
	}
	if doc.Kind == 0 || len(doc.Content) == 0 {
		return nil, fmt.Errorf("restore block is empty")
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("restore block is not a mapping")
	}
	leaves := map[string]string{}
	if err := flattenNode(root, "", leaves); err != nil {
		return nil, err
	}
	return leaves, nil
}

func flattenNode(n *yaml.Node, prefix string, leaves map[string]string) error {
	seen := map[string]bool{}
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		path := prefix + k.Value
		if seen[k.Value] {
			return fmt.Errorf("restore block repeats key %s", path)
		}
		seen[k.Value] = true
		switch v.Kind {
		case yaml.MappingNode:
			if err := flattenNode(v, path+".", leaves); err != nil {
				return err
			}
		case yaml.ScalarNode:
			leaves[path] = v.Value
		default:
			return fmt.Errorf("restore block key %s is not a scalar", path)
		}
	}
	return nil
}

func restoreSettings(body string) (map[string]string, error) {
	leaves, err := flattenYAML(body)
	if err != nil {
		return nil, err
	}
	for _, key := range restoreKeys {
		if _, ok := leaves[key]; !ok {
			return nil, fmt.Errorf("restore block lacks %s", key)
		}
	}
	for _, key := range slices.Sorted(maps.Keys(leaves)) {
		if !slices.Contains(restoreKeys, key) {
			return nil, fmt.Errorf("restore block has unexpected key %s", key)
		}
	}
	return leaves, nil
}

func changelogRestoreFrom(doc *doccheck.Doc) (string, map[string]string, error) {
	f, err := doccheck.ChangelogFence(doc, restoreRelease, restoreSubsection, "yaml")
	if err != nil {
		return "", nil, err
	}
	settings, err := restoreSettings(f.Body)
	if err != nil {
		return "", nil, err
	}
	return f.Body, settings, nil
}

func changelogRestore(t *testing.T) (string, map[string]string) {
	t.Helper()
	root, err := doccheck.Root()
	if err != nil {
		t.Fatal(err)
	}
	doc, err := doccheck.Load(filepath.Join(root, "CHANGELOG.md"))
	if err != nil {
		t.Fatal(err)
	}
	body, settings, err := changelogRestoreFrom(doc)
	if err != nil {
		t.Fatal(err)
	}
	return body, settings
}

func deepMerge(dst, src map[string]any) {
	for k, v := range src {
		sm, sok := v.(map[string]any)
		dm, dok := dst[k].(map[string]any)
		if sok && dok {
			deepMerge(dm, sm)
			continue
		}
		dst[k] = v
	}
}

func mergeYAML(t *testing.T, base, fragment string) string {
	t.Helper()
	var b, f map[string]any
	if err := yaml.Unmarshal([]byte(base), &b); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal([]byte(fragment), &f); err != nil {
		t.Fatal(err)
	}
	deepMerge(b, f)
	out, err := yaml.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func mustReplace(t *testing.T, src, old, with string) string {
	t.Helper()
	if !strings.Contains(src, old) {
		t.Fatalf("text %q not found", old)
	}
	return strings.Replace(src, old, with, 1)
}

func managementGet(t *testing.T, admin net.Addr, path string) (int, []byte) {
	t.Helper()
	tcp, ok := admin.(*net.TCPAddr)
	if !ok {
		t.Fatalf("admin addr = %v, want a TCP address", admin)
	}
	client := &http.Client{Timeout: testwait.Guard(t)}
	resp, body := doGet(t, client, net.JoinHostPort("127.0.0.1", strconv.Itoa(tcp.Port)), path, "", nil)
	return resp.StatusCode, body
}

func TestMigrationRestoresV01Defaults(t *testing.T) {
	fence, restore := changelogRestore(t)
	for _, key := range restoreKeys {
		if _, ok := restore[key]; !ok {
			t.Fatalf("restore block lacks %s", key)
		}
	}
	for _, key := range slices.Sorted(maps.Keys(restore)) {
		if !slices.Contains(restoreKeys, key) {
			t.Fatalf("restore block has unexpected key %s", key)
		}
	}

	t.Run("restored", func(t *testing.T) {
		path := writeTemp(t, mergeYAML(t, migrationBase, fence))

		code, out, errs := runCmd(t, "validate", "-config", path)
		if code != 0 {
			t.Fatalf("validate exit code = %d, want 0; stdout %s stderr %s", code, out, errs)
		}
		for _, bad := range []string{"admin.auth.keys", "admin.probes"} {
			if strings.Contains(out+errs, bad) {
				t.Errorf("validate output names %s:\n%s%s", bad, out, errs)
			}
		}

		var errb syncBuffer
		h := startRunArgs(t, []string{"-config", path}, &errb)
		if h.addrs.Probe != nil {
			t.Errorf("probe addr = %v, want none", h.addrs.Probe)
		}
		if names := slices.Sorted(maps.Keys(h.addrs.Listeners)); !slices.Equal(names, []string{"public"}) {
			t.Errorf("listeners = %v, want [public]", names)
		}
		if admin, ok := h.addrs.Admin.(*net.TCPAddr); !ok || !admin.IP.IsUnspecified() {
			t.Errorf("admin addr = %v, want an unspecified IP", h.addrs.Admin)
		}
		if strings.Contains(errb.String(), `"listener":"probe"`) {
			t.Errorf("stderr names a probe listener:\n%s", errb.String())
		}

		status, body := managementGet(t, h.addrs.Admin, "/config")
		if status != http.StatusOK {
			t.Errorf("GET /config = %d, want 200", status)
		}
		if !strings.Contains(errb.String(), "admin management endpoints are unauthenticated on a non-loopback address") {
			t.Errorf("stderr lacks the unauthenticated management warning:\n%s", errb.String())
		}

		var got managementConfig
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("decode /config body %q: %v", body, err)
		}
		if got.Global.ShutdownDelay != "0s" {
			t.Errorf("/config global.shutdown_delay = %q, want 0s", got.Global.ShutdownDelay)
		}
		if got.Global.DrainTimeout != "30s" {
			t.Errorf("/config global.drain_timeout = %q, want 30s", got.Global.DrainTimeout)
		}

		cfg, err := config.LoadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		if cfg.Global.ShutdownDelay == nil || *cfg.Global.ShutdownDelay != 0 {
			t.Errorf("loaded shutdown_delay = %v, want 0s", cfg.Global.ShutdownDelay)
		}
		if got := cfg.Global.DrainTimeout.Std(); got != 30*time.Second {
			t.Errorf("loaded drain_timeout = %v, want 30s", got)
		}

		if code := h.stop(); code != 0 {
			t.Errorf("exit code = %d, want 0; stderr:\n%s", code, errb.String())
		}
	})

	t.Run("baseline", func(t *testing.T) {
		code, out, errs := runCmd(t, "validate", "-config", writeTemp(t, migrationBase))
		if code != 1 || !strings.Contains(out+errs, "admin.auth.keys") {
			t.Errorf("validate exit code = %d, want 1 naming admin.auth.keys; stdout %s stderr %s", code, out, errs)
		}

		cfg, err := config.Parse([]byte(noAdminConfig))
		if err != nil {
			t.Fatal(err)
		}
		if got := cfg.Global.ShutdownDelay.Std(); got != 5*time.Second {
			t.Errorf("default shutdown_delay = %v, want 5s", got)
		}
		if got := cfg.Global.DrainTimeout.Std(); got != 20*time.Second {
			t.Errorf("default drain_timeout = %v, want 20s", got)
		}
		if !*cfg.Admin.Probes.Enabled {
			t.Errorf("default admin.probes.enabled = false, want true")
		}
		if cfg.Admin.Probes.Address != ":9902" {
			t.Errorf("default admin.probes.address = %q, want :9902", cfg.Admin.Probes.Address)
		}
		if cfg.Admin.Auth.Management != "auto" {
			t.Errorf("default admin.auth.management = %q, want auto", cfg.Admin.Auth.Management)
		}
		defaults := restoreDefaults(t)
		for _, k := range restoreKeys {
			if defaults[k] == restore[k] {
				t.Errorf("default of %s is %s, equal to the restore value", k, defaults[k])
			}
		}

		probeHost, _, err := net.SplitHostPort(defaults[probeAddressKey])
		if err != nil {
			t.Fatal(err)
		}
		fragment := fmt.Sprintf("admin:\n  address: %q\n  probes:\n    address: %q\n", net.JoinHostPort("127.0.0.1", "0"), net.JoinHostPort(probeHost, "0"))
		path := writeTemp(t, mergeYAML(t, migrationBase, fragment))
		var errb syncBuffer
		h := startRunArgs(t, []string{"-config", path}, &errb)
		probe, ok := h.addrs.Probe.(*net.TCPAddr)
		if !ok || !probe.IP.IsUnspecified() || probe.Port == 0 {
			t.Fatalf("probe addr = %v, want a wildcard address with a port", h.addrs.Probe)
		}
		_, body := managementGet(t, h.addrs.Admin, "/config")
		var got managementConfig
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("decode /config body %q: %v", body, err)
		}
		if got.Global.ShutdownDelay != "5s" {
			t.Errorf("/config global.shutdown_delay = %q, want 5s", got.Global.ShutdownDelay)
		}
		if got.Global.DrainTimeout != "20s" {
			t.Errorf("/config global.drain_timeout = %q, want 20s", got.Global.DrainTimeout)
		}

		h.cancel()
		awaitDue(t, h.clk, 1, 5*time.Second, "shutdown delay timer armed", errb.String)
		h.clk.Advance(5 * time.Second)
		if code := h.wait(t); code != 0 {
			t.Errorf("exit code = %d, want 0; stderr:\n%s", code, errb.String())
		}
	})
}

func restoreFixture(fences ...string) string {
	var b strings.Builder
	b.WriteString("## [v0.2.0] - 2026-11-11\n\n### Breaking\n\n- Entry.\n\n")
	for _, f := range fences {
		b.WriteString("```yaml\n" + f + "```\n\n")
	}
	b.WriteString("### Added\n\n- Feature.\n")
	return b.String()
}

func restoreFromFixture(src string) (map[string]string, error) {
	doc := doccheck.Parse("CHANGELOG.md", []byte(src))
	_, settings, err := changelogRestoreFrom(doc)
	return settings, err
}

const validRestoreFence = "global:\n  shutdown_delay: 0s\n  drain_timeout: 30s\nadmin:\n  probes:\n    enabled: false\n  auth:\n    management: none\n"

type rejectCase struct {
	name, src, want string
}

func TestRestoreSettingsRejects(t *testing.T) {
	lineOf := map[string]string{
		"global.shutdown_delay": "  shutdown_delay: 0s\n",
		"global.drain_timeout":  "  drain_timeout: 30s\n",
		"admin.probes.enabled":  "    enabled: false\n",
		"admin.auth.management": "    management: none\n",
	}

	var indented []string
	for _, l := range strings.Split(strings.TrimSuffix(validRestoreFence, "\n"), "\n") {
		indented = append(indented, "    "+l)
	}

	cases := []rejectCase{
		{"no fence", restoreFixture(), "has no yaml fence"},
		{"two fences", restoreFixture(validRestoreFence, validRestoreFence), "has 2 yaml fences"},
		{"invalid YAML", restoreFixture("global: [\n"), "not valid YAML"},
		{"extra key", restoreFixture(mustReplace(t, validRestoreFence, "admin:\n", "admin:\n  address: x\n")), "unexpected key admin.address"},
		{"list leaf", restoreFixture(mustReplace(t, validRestoreFence, "  shutdown_delay: 0s\n", "  shutdown_delay:\n    - 0s\n")), "is not a scalar"},
	}
	for _, key := range restoreKeys {
		cases = append(cases, rejectCase{"missing " + key, restoreFixture(mustReplace(t, validRestoreFence, lineOf[key], "")), "lacks " + key})
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := restoreFromFixture(c.src)
			if err == nil {
				t.Fatalf("restoreSettings accepted the fixture, want an error containing %q", c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error = %q, want it to contain %q", err, c.want)
			}
		})
	}

	t.Run("indented fence parses equal", func(t *testing.T) {
		plain, err := restoreFromFixture(restoreFixture(validRestoreFence))
		if err != nil {
			t.Fatal(err)
		}
		deep, err := restoreFromFixture(restoreFixture(strings.Join(indented, "\n") + "\n"))
		if err != nil {
			t.Fatal(err)
		}
		if !maps.Equal(plain, deep) {
			t.Errorf("indented settings = %v, want %v", deep, plain)
		}
	})
}
