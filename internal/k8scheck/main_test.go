package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func stub(t *testing.T, exit int) (bin, dir string) {
	t.Helper()
	dir = t.TempDir()
	bin = filepath.Join(dir, "stub")
	script := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$3\" > \"$(dirname \"$0\")/config-path\"\nenv > \"$(dirname \"$0\")/env\"\necho \"stub output\"\nexit %d\n", exit)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, dir
}

func readTrimmed(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

func TestValidateConfig(t *testing.T) {
	t.Run("fails", func(t *testing.T) {
		bin, dir := stub(t, 1)
		got := validateConfig(context.Background(), bin, "cm", "version: 1\n")
		want := &Finding{"ConfigMap", "cm", "microgateway validate failed: stub output"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("validateConfig = %+v, want %+v", got, want)
		}
		path := readTrimmed(t, filepath.Join(dir, "config-path"))
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("config file %s still exists: %v", path, err)
		}
		if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
			t.Errorf("temporary directory of %s still exists: %v", path, err)
		}
		env := readTrimmed(t, filepath.Join(dir, "env"))
		if !strings.Contains("\n"+env+"\n", "\nREDIS_PASSWORD=example-redis-password\n") {
			t.Errorf("environment of the command lacks REDIS_PASSWORD: %q", env)
		}
	})

	t.Run("passes", func(t *testing.T) {
		bin, dir := stub(t, 0)
		if got := validateConfig(context.Background(), bin, "cm", "version: 1\n"); got != nil {
			t.Fatalf("validateConfig = %+v, want nil", got)
		}
		path := readTrimmed(t, filepath.Join(dir, "config-path"))
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("config file %s still exists: %v", path, err)
		}
	})

	t.Run("missing binary", func(t *testing.T) {
		_, dir := stub(t, 0)
		got := validateConfig(context.Background(), filepath.Join(dir, "missing"), "cm", "version: 1\n")
		if got == nil || got.Kind != "ConfigMap" || got.Name != "cm" || !strings.HasPrefix(got.Rule, "microgateway validate failed: ") {
			t.Fatalf("validateConfig = %+v, want a ConfigMap finding for cm", got)
		}
	})
}

func runCLI(t *testing.T, args ...string) (int, []string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := run(context.Background(), args, &out, &errOut)
	var lines []string
	if s := strings.TrimSuffix(out.String(), "\n"); s != "" {
		lines = strings.Split(s, "\n")
	}
	return code, lines, errOut.String()
}

func TestRun(t *testing.T) {
	bin, dir := stub(t, 0)
	defaultPath := filepath.Join("testdata", "default.yaml")
	redisPath := filepath.Join("testdata", "redis.yaml")
	original, err := os.ReadFile(defaultPath)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("no args", func(t *testing.T) {
		code, _, errOut := runCLI(t)
		if code != 2 || !strings.Contains(errOut, "-binary is required") {
			t.Errorf("code %d, stderr %q; want 2 with -binary is required", code, errOut)
		}
	})

	t.Run("missing binary", func(t *testing.T) {
		code, _, _ := runCLI(t, "-binary", filepath.Join(dir, "missing"), defaultPath)
		if code != 2 {
			t.Errorf("code %d, want 2", code)
		}
	})

	t.Run("binary is a directory", func(t *testing.T) {
		code, _, errOut := runCLI(t, "-binary", dir, defaultPath)
		if code != 2 || !strings.Contains(errOut, "is a directory") {
			t.Errorf("code %d, stderr %q; want 2 with is a directory", code, errOut)
		}
	})

	t.Run("bad flag", func(t *testing.T) {
		code, _, _ := runCLI(t, "-bogus")
		if code != 2 {
			t.Errorf("code %d, want 2", code)
		}
	})

	t.Run("no manifest files", func(t *testing.T) {
		code, _, errOut := runCLI(t, "-binary", bin)
		if code != 2 || !strings.Contains(errOut, "no manifest files") {
			t.Errorf("code %d, stderr %q; want 2 with no manifest files", code, errOut)
		}
	})

	t.Run("unreadable manifest", func(t *testing.T) {
		code, _, errOut := runCLI(t, "-binary", bin, filepath.Join(dir, "missing.yaml"))
		if code != 2 || !strings.Contains(errOut, "read ") {
			t.Errorf("code %d, stderr %q; want 2 with read", code, errOut)
		}
	})

	t.Run("passing fixtures", func(t *testing.T) {
		code, lines, _ := runCLI(t, "-binary", bin, defaultPath, redisPath)
		if code != 0 {
			t.Fatalf("code %d, want 0", code)
		}
		if want := []string{"k8scheck: no findings in 2 file(s)"}; !reflect.DeepEqual(lines, want) {
			t.Errorf("stdout lines = %q, want %q", lines, want)
		}
	})

	t.Run("grace finding", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "grace.yaml")
		mutated := strings.Replace(string(original), "terminationGracePeriodSeconds: 40", "terminationGracePeriodSeconds: 30", 1)
		if err := os.WriteFile(path, []byte(mutated), 0o600); err != nil {
			t.Fatal(err)
		}
		code, lines, errOut := runCLI(t, "-binary", bin, path)
		want := path + ": Deployment/gw-microgateway: shutdown_delay + drain_timeout + 5s <= terminationGracePeriodSeconds is not met: 10s + 20s + 5s > 30s"
		if code != 1 || !reflect.DeepEqual(lines, []string{want}) || !strings.Contains(errOut, "k8scheck: 1 finding(s)") {
			t.Errorf("code %d, lines %q, stderr %q; want 1 with the single grace finding", code, lines, errOut)
		}
	})

	t.Run("manifest does not parse", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "broken.yaml")
		if err := os.WriteFile(path, []byte("kind: [\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		code, lines, _ := runCLI(t, "-binary", bin, path)
		prefix := path + ": -/-: manifest does not parse: "
		if code != 1 || len(lines) != 1 || !strings.HasPrefix(lines[0], prefix) {
			t.Errorf("code %d, lines %q; want 1 with a line prefixed %q", code, lines, prefix)
		}
	})

	t.Run("validate failure", func(t *testing.T) {
		failing, _ := stub(t, 1)
		code, lines, _ := runCLI(t, "-binary", failing, defaultPath)
		want := defaultPath + ": ConfigMap/gw-microgateway: microgateway validate failed: stub output"
		if code != 1 || !reflect.DeepEqual(lines, []string{want}) {
			t.Errorf("code %d, lines %q; want 1 with %q", code, lines, want)
		}
	})
}
