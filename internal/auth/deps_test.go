package auth

import (
	"os/exec"
	"strings"
	"testing"
)

func TestNoStoreDependency(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not available")
	}
	out, err := exec.Command("go", "list", "-deps", "github.com/ok0/lychgate/internal/auth").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == "github.com/ok0/lychgate/store" || strings.HasPrefix(line, "github.com/ok0/lychgate/store/") ||
			strings.Contains(line, "/internal/memstore") ||
			strings.Contains(line, "/internal/storeguard") ||
			strings.Contains(line, "/internal/state") ||
			strings.Contains(line, "/redis") {
			t.Fatalf("internal/auth depends on forbidden package %q", line)
		}
	}
}
