package cli

import (
	"fmt"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/testwait"
)

const noAdminConfig = `version: 1
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
`

// defaultExposureConfig returns noAdminConfig with an admin block that keeps the shipped default hosts (admin on
// loopback, probes on all interfaces) and ports 0. It fails when a shipped default changes.
func defaultExposureConfig(t *testing.T) string {
	t.Helper()
	cfg, err := config.Parse([]byte(noAdminConfig))
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	adminHost, _, err := net.SplitHostPort(cfg.Admin.Address)
	if err != nil {
		t.Fatal(err)
	}
	probeHost, _, err := net.SplitHostPort(cfg.Admin.Probes.Address)
	if err != nil {
		t.Fatal(err)
	}
	if adminHost != "127.0.0.1" {
		t.Fatalf("default admin.address host = %q, want 127.0.0.1", adminHost)
	}
	if probeHost != "" {
		t.Fatalf("default admin.probes.address host = %q, want all interfaces", probeHost)
	}
	return noAdminConfig + fmt.Sprintf("admin:\n  address: %q\n  probes:\n    address: %q\n", net.JoinHostPort(adminHost, "0"), net.JoinHostPort(probeHost, "0"))
}

func probeStatus(t *testing.T, addr, path string) int {
	t.Helper()
	client := &http.Client{Timeout: testwait.Guard(t)}
	resp, _ := doGet(t, client, addr, path, "", nil)
	return resp.StatusCode
}

func TestRunDefaultListeners(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		path := writeTemp(t, withZeroShutdownDelay(t, defaultExposureConfig(t)))
		var errb syncBuffer
		h := startRunArgs(t, []string{"-config", path}, &errb)

		admin, ok := h.addrs.Admin.(*net.TCPAddr)
		if !ok || !admin.IP.IsLoopback() {
			t.Fatalf("admin addr = %v, want a loopback address", h.addrs.Admin)
		}
		probe, ok := h.addrs.Probe.(*net.TCPAddr)
		if !ok || !probe.IP.IsUnspecified() || probe.Port == 0 {
			t.Fatalf("probe addr = %v, want a wildcard address with a port", h.addrs.Probe)
		}
		probeAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(probe.Port))
		if got := probeStatus(t, probeAddr, "/livez"); got != http.StatusOK {
			t.Errorf("GET /livez = %d, want 200", got)
		}
		if got := probeStatus(t, probeAddr, "/readyz"); got != http.StatusOK {
			t.Errorf("GET /readyz = %d, want 200", got)
		}
		if got := probeStatus(t, probeAddr, "/config"); got != http.StatusNotFound {
			t.Errorf("GET /config on probe = %d, want 404", got)
		}
		if got := probeStatus(t, probeAddr, "/metrics"); got != http.StatusNotFound {
			t.Errorf("GET /metrics on probe = %d, want 404", got)
		}
		if !strings.Contains(errb.String(), `"listener":"probe"`) {
			t.Errorf("stderr lacks the probe listening record:\n%s", errb.String())
		}
		if !strings.Contains(errb.String(), probe.String()) {
			t.Errorf("stderr lacks the probe address %s:\n%s", probe.String(), errb.String())
		}
		if !strings.Contains(errb.String(), "probe listener is unauthenticated and serves only /livez and /readyz") {
			t.Errorf("stderr lacks the probe exposure record:\n%s", errb.String())
		}
		if code := h.stop(); code != 0 {
			t.Errorf("exit code = %d, want 0", code)
		}
	})

	t.Run("probes disabled", func(t *testing.T) {
		path := writeTemp(t, withZeroShutdownDelay(t, defaultExposureConfig(t)+"    enabled: false\n"))
		var errb syncBuffer
		h := startRunArgs(t, []string{"-config", path}, &errb)
		if h.addrs.Probe != nil {
			t.Errorf("probe addr = %v, want nil", h.addrs.Probe)
		}
		if strings.Contains(errb.String(), `"listener":"probe"`) {
			t.Errorf("stderr has a probe listening record:\n%s", errb.String())
		}
		if strings.Contains(errb.String(), "probe listener is unauthenticated") {
			t.Errorf("stderr has a probe exposure record:\n%s", errb.String())
		}
		if code := h.stop(); code != 0 {
			t.Errorf("exit code = %d, want 0", code)
		}
	})

	t.Run("admin disabled", func(t *testing.T) {
		path := writeTemp(t, withZeroShutdownDelay(t, noAdminConfig+"admin:\n  enabled: false\n  probes:\n    address: \"127.0.0.1:0\"\n"))
		var errb syncBuffer
		h := startRunArgs(t, []string{"-config", path}, &errb)
		if h.addrs.Admin != nil {
			t.Errorf("admin addr = %v, want nil", h.addrs.Admin)
		}
		probe, ok := h.addrs.Probe.(*net.TCPAddr)
		if !ok {
			t.Fatalf("probe addr = %v, want a TCP address", h.addrs.Probe)
		}
		if got := probeStatus(t, net.JoinHostPort("127.0.0.1", strconv.Itoa(probe.Port)), "/readyz"); got != http.StatusOK {
			t.Errorf("GET /readyz = %d, want 200", got)
		}
		if code := h.stop(); code != 0 {
			t.Errorf("exit code = %d, want 0", code)
		}
	})

	t.Run("metrics opt-in log", func(t *testing.T) {
		path := writeTemp(t, withZeroShutdownDelay(t, defaultExposureConfig(t)+"    metrics: true\n"))
		var errb syncBuffer
		h := startRunArgs(t, []string{"-config", path}, &errb)
		if !strings.Contains(errb.String(), "serves /livez, /readyz and /metrics") {
			t.Errorf("stderr lacks the metrics exposure record:\n%s", errb.String())
		}
		if code := h.stop(); code != 0 {
			t.Errorf("exit code = %d, want 0", code)
		}
	})
}

func TestRunProbeReload(t *testing.T) {
	backend := echoBackend(t, "users")
	route := func(name string) string {
		return fmt.Sprintf("  - name: %s\n    match:\n      path: /%s\n      path_type: prefix\n    upstream: users\n", name, name)
	}
	cfgOf := func(extra, probes string) string {
		return withZeroShutdownDelay(t, fmt.Sprintf(`version: 1
listeners:
  - name: public
    address: "127.0.0.1:0"
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
  address: "127.0.0.1:0"
  probes:
%s`, backend.URL, extra, probes))
	}

	path := writeTemp(t, cfgOf("", "    address: \"127.0.0.1:0\"\n"))
	var errb syncBuffer
	h := startRunArgs(t, []string{"-config", path}, &errb)
	public := h.addrs.Listeners["public"].String()
	probe := h.addrs.Probe.String()
	client := &http.Client{Timeout: testwait.Guard(t)}
	if got := statusOf(client, "http://"+probe+"/metrics"); got != http.StatusNotFound {
		t.Fatalf("GET /metrics on probe before the edit = %d, want 404", got)
	}

	writeFile(t, path, []byte(cfgOf("", "    address: \"127.0.0.1:0\"\n    metrics: true\n")))
	eventuallyAdvancing(t, "probe /metrics served after the edit", h.clk, reloadStep, func() bool { return statusOf(client, "http://"+probe+"/metrics") == http.StatusOK }, errb.String)
	if got := restartFields(t, &errb); len(got) != 0 {
		t.Errorf("restart required fields after metrics edit = %v, want none", got)
	}

	writeFile(t, path, []byte(cfgOf(route("new"), "    address: \"localhost:0\"\n    metrics: true\n")))
	eventuallyAdvancing(t, "/new served after the probe address edit", h.clk, reloadStep, func() bool { return statusOf(client, "http://"+public+"/new") == http.StatusOK }, errb.String)
	eventually(t, "probe address restart warning logged", func() bool { return len(restartFields(t, &errb)) >= 1 })
	if got, want := restartFields(t, &errb), []string{"admin.probes.address"}; !slices.Equal(got, want) {
		t.Errorf("restart required fields = %v, want %v", got, want)
	}
	if got := statusOf(client, "http://"+probe+"/readyz"); got != http.StatusOK {
		t.Errorf("GET /readyz on the original probe address = %d, want 200", got)
	}

	writeFile(t, path, []byte(cfgOf(route("new")+route("new2"), "    address: \"localhost:0\"\n    enabled: false\n    metrics: true\n")))
	eventuallyAdvancing(t, "/new2 served after the enabled edit", h.clk, reloadStep, func() bool { return statusOf(client, "http://"+public+"/new2") == http.StatusOK }, errb.String)
	eventually(t, "probes enabled restart warning logged", func() bool { return len(restartFields(t, &errb)) >= 2 })
	if got, want := restartFields(t, &errb), []string{"admin.probes.address", "admin.probes.enabled"}; !slices.Equal(got, want) {
		t.Errorf("restart required fields = %v, want %v", got, want)
	}
	if got := statusOf(client, "http://"+probe+"/livez"); got != http.StatusOK {
		t.Errorf("GET /livez on the original probe address = %d, want 200", got)
	}

	if code := h.stop(); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

func TestRunProbeBindConflict(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	conflict := held.Addr().String()
	path := writeTemp(t, noAdminConfig+fmt.Sprintf("admin:\n  address: \"127.0.0.1:0\"\n  probes:\n    address: %q\n", conflict))

	before := listeningAddrs(t)
	if !before[conflict] {
		t.Fatalf("held listener %s not seen before run: %v", conflict, before)
	}
	code, _, errs := runCmd(t, "run", "-config", path)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	if !strings.Contains(errs, `listener "probe"`) {
		t.Errorf("stderr = %q, want it to contain %q", errs, `listener "probe"`)
	}
	if !strings.Contains(errs, conflict) {
		t.Errorf("stderr = %q, want it to contain %q", errs, conflict)
	}
	for addr := range listeningAddrs(t) {
		if !before[addr] {
			t.Fatalf("address %s not released: still listening after run returned", addr)
		}
	}
	if !listeningAddrs(t)[conflict] {
		t.Fatalf("held listener %s closed by run", conflict)
	}
}
