package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// validateConfig writes text to a temporary config.yaml and runs "<binary> validate -config" on it with only REDIS_PASSWORD set to the example password. It returns a finding for the ConfigMap name when the command fails, and nil when it passes. The temporary directory is removed before it returns.
func validateConfig(ctx context.Context, binary, name, text string) *Finding {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	dir, err := os.MkdirTemp("", "k8scheck-")
	if err != nil {
		return &Finding{"ConfigMap", name, "microgateway validate failed: " + err.Error()}
	}
	defer func() { _ = os.RemoveAll(dir) }()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		return &Finding{"ConfigMap", name, "microgateway validate failed: " + err.Error()}
	}
	cmd := exec.CommandContext(ctx, binary, "validate", "-config", path)
	cmd.Env = []string{"REDIS_PASSWORD=" + examplePassword}
	out, err := cmd.CombinedOutput()
	if err == nil {
		return nil
	}
	detail := strings.Join(strings.Fields(string(out)), " ")
	if detail == "" {
		detail = err.Error()
	}
	return &Finding{"ConfigMap", name, "microgateway validate failed: " + detail}
}
