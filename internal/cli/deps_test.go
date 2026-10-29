package cli

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestCoreHasNoRedisDependency(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not available")
	}
	cmd := exec.Command("go", "list", "-deps", "-test", "github.com/ok0/go-microgateway/...")
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "go-redis") || strings.Contains(line, "miniredis") ||
			line == "github.com/ok0/go-microgateway/redis" || strings.HasPrefix(line, "github.com/ok0/go-microgateway/redis/") {
			t.Errorf("core depends on redis package %q", line)
		}
	}
}
