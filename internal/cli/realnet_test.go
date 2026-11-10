//go:build realnet

package cli

import (
	"errors"
	"net"
	"net/http"
	"strconv"
	"syscall"
	"testing"

	"github.com/ok0/go-microgateway/internal/testwait"
)

// hostIPv4 returns the first non-loopback, non-link-local IPv4 address of an up interface. It fails when there is none.
func hostIPv4(t *testing.T) net.IP {
	t.Helper()
	ifaces, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range addrs {
			ipn, ok := a.(*net.IPNet)
			if !ok || ipn.IP.To4() == nil || ipn.IP.IsLoopback() || ipn.IP.IsLinkLocalUnicast() {
				continue
			}
			return ipn.IP.To4()
		}
	}
	t.Fatal("realnet: host has no non-loopback IPv4 interface address; TestDefaultExposureNonLoopback needs one")
	return nil
}

func TestDefaultExposureNonLoopback(t *testing.T) {
	ip := hostIPv4(t)
	path := writeTemp(t, withZeroShutdownDelay(t, defaultExposureConfig(t)))
	var errb syncBuffer
	h := startRunArgs(t, []string{"-config", path}, &errb)

	probe, ok := h.addrs.Probe.(*net.TCPAddr)
	if !ok {
		t.Fatalf("probe addr = %v, want a TCP address", h.addrs.Probe)
	}
	admin, ok := h.addrs.Admin.(*net.TCPAddr)
	if !ok {
		t.Fatalf("admin addr = %v, want a TCP address", h.addrs.Admin)
	}
	probeAddr := net.JoinHostPort(ip.String(), strconv.Itoa(probe.Port))
	adminAddr := net.JoinHostPort(ip.String(), strconv.Itoa(admin.Port))
	client := &http.Client{Timeout: testwait.Guard(t)}

	for _, p := range []string{"/livez", "/readyz"} {
		if got := statusOf(client, "http://"+probeAddr+p); got != http.StatusOK {
			t.Errorf("GET %s on probe %s = %d, want 200", p, probeAddr, got)
		}
	}
	for _, p := range []string{"/config", "/upstreams", "/metrics", "/cache/purge"} {
		if got := statusOf(client, "http://"+probeAddr+p); got != http.StatusNotFound {
			t.Errorf("GET %s on probe %s = %d, want 404", p, probeAddr, got)
		}
	}

	conn, err := net.DialTimeout("tcp", adminAddr, testwait.Guard(t))
	if err == nil {
		_ = conn.Close()
		t.Fatalf("management listener reachable on non-loopback %s", adminAddr)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Errorf("dial %s error = %v, want ECONNREFUSED", adminAddr, err)
	}

	loopAdmin := net.JoinHostPort("127.0.0.1", strconv.Itoa(admin.Port))
	for _, p := range []string{"/config", "/upstreams", "/metrics"} {
		if got := statusOf(client, "http://"+loopAdmin+p); got != http.StatusOK {
			t.Errorf("GET %s on management %s = %d, want 200", p, loopAdmin, got)
		}
	}

	if code := h.stop(); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}
