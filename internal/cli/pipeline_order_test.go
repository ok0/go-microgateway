package cli

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestValidatePipelineCacheBeforeAuth(t *testing.T) {
	bad := strings.Replace(minimalConfig, "version: 1\n", "version: 1\nglobal:\n  drain_timeout: -1s\n", 1)
	if !strings.Contains(bad, "global:\n  drain_timeout: -1s\n") {
		t.Fatal("drain_timeout seed was not inserted")
	}
	bad = strings.Replace(bad, "    upstream: users\n", "    upstream: users\n    pipeline: [cache, auth]\n", 1)
	if !strings.Contains(bad, "pipeline: [cache, auth]") {
		t.Fatal("pipeline line was not inserted")
	}

	got := validateLines(t, nil, writeTemp(t, bad))
	want := []string{
		fmt.Sprintf("routes[0].pipeline[0] (line %d): %s", lineOf(t, bad, "pipeline: [cache, auth]"), `stage "cache" must come after "auth": cache hits would be served without authentication`),
		fmt.Sprintf("global.drain_timeout (line %d): must be positive", lineOf(t, bad, "drain_timeout: -1s")),
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("validate lines = %q, want %q", got, want)
	}

	good := strings.Replace(bad, "pipeline: [cache, auth]", "pipeline: [auth, cache]", 1)
	good = strings.Replace(good, "global:\n  drain_timeout: -1s\n", "", 1)
	code, out, errs := runCmd(t, "validate", "-config", writeTemp(t, good))
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr:\n%s", code, errs)
	}
	if !strings.Contains(out, "configuration is valid") {
		t.Errorf("stdout = %q, want it to contain %q", out, "configuration is valid")
	}
	if errs != "" {
		t.Errorf("stderr = %q, want empty", errs)
	}

	authFirst := strings.Replace(bad, "pipeline: [cache, auth]", "pipeline: [auth, cache]", 1)
	gotAuthFirst := validateLines(t, nil, writeTemp(t, authFirst))
	wantAuthFirst := []string{
		fmt.Sprintf("global.drain_timeout (line %d): must be positive", lineOf(t, authFirst, "drain_timeout: -1s")),
	}
	if !slices.Equal(gotAuthFirst, wantAuthFirst) {
		t.Fatalf("validate lines = %q, want %q", gotAuthFirst, wantAuthFirst)
	}
}
