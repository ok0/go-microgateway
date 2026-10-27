package main

import (
	"bytes"
	"context"
	"regexp"
	"strings"
	"testing"
)

func runCmd(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(context.Background(), args, &out, &errb)
	return code, out.String(), errb.String()
}

func TestValidateExample(t *testing.T) {
	code, out, errs := runCmd(t, "validate", "-config", "../../examples/basic.yaml")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr:\n%s", code, errs)
	}
	if !strings.Contains(out, "configuration is valid") {
		t.Errorf("stdout = %q, want it to contain %q", out, "configuration is valid")
	}
	if errs != "" {
		t.Errorf("stderr = %q, want empty", errs)
	}
}

func TestValidateInvalid(t *testing.T) {
	code, _, errs := runCmd(t, "validate", "-config", "../../config/testdata/invalid_multi.yaml")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	wantPrefixes := []string{
		"listeners[0].bogus_key (line 5): ",
		"upstreams[1].name (line 10): ",
		"routes[0].upstream (line 17): ",
		"routes[1].match.path (line 20): ",
		"routes[1].timeout (line 23): ",
	}
	for _, p := range wantPrefixes {
		if !strings.Contains(errs, p) {
			t.Errorf("stderr missing line with prefix %q\nstderr:\n%s", p, errs)
		}
	}
	lineRE := regexp.MustCompile(`^\S+ \(line \d+\): .+$`)
	lines := strings.Split(strings.TrimRight(errs, "\n"), "\n")
	for _, l := range lines[:len(lines)-1] {
		if !lineRE.MatchString(l) {
			t.Errorf("error line %q does not match %s", l, lineRE)
		}
	}
}

func TestValidateMissingFile(t *testing.T) {
	code, _, errs := runCmd(t, "validate", "-config", "testdata/does-not-exist.yaml")
	if code == 0 {
		t.Fatal("exit code = 0, want nonzero")
	}
	if errs == "" {
		t.Error("stderr is empty, want an error message")
	}
}

func TestValidateMissingFlag(t *testing.T) {
	code, _, errs := runCmd(t, "validate")
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if !strings.Contains(errs, "-config is required") {
		t.Errorf("stderr = %q, want it to contain %q", errs, "-config is required")
	}
}

func TestVersion(t *testing.T) {
	code, out, _ := runCmd(t, "version")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	for _, want := range []string{"dev", "none", "unknown"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout = %q, want it to contain %q", out, want)
		}
	}
}

func TestNoCommand(t *testing.T) {
	var out, errb bytes.Buffer
	code := run(context.Background(), nil, &out, &errb)
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if !strings.Contains(errb.String(), "usage:") {
		t.Errorf("stderr = %q, want usage text", errb.String())
	}
}

func TestUnknownCommand(t *testing.T) {
	for _, cmd := range []string{"bogus", "run"} {
		code, _, errs := runCmd(t, cmd)
		if code != 2 {
			t.Errorf("%s: exit code = %d, want 2", cmd, code)
		}
		if !strings.Contains(errs, "unknown command") {
			t.Errorf("%s: stderr = %q, want it to contain %q", cmd, errs, "unknown command")
		}
	}
}
