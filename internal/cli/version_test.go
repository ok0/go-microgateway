package cli

import (
	"runtime/debug"
	"testing"
)

func withBuildInfo(t *testing.T, v string, ok bool) {
	t.Helper()
	saved := readBuildInfo
	t.Cleanup(func() { readBuildInfo = saved })
	readBuildInfo = func() (*debug.BuildInfo, bool) {
		if !ok {
			return nil, false
		}
		return &debug.BuildInfo{Main: debug.Module{Path: "github.com/ok0/go-microgateway/redis", Version: v}}, true
	}
}

func TestVersionBuildInfo(t *testing.T) {
	cases := []struct {
		name string
		v    string
		ok   bool
		want string
	}{
		{"read fails", "", false, "dev"},
		{"release", "v0.1.0", true, "v0.1.0"},
		{"prerelease", "v0.2.0-rc.1", true, "v0.2.0-rc.1"},
		{"two digit minor", "v1.10.0", true, "v1.10.0"},
		{"empty", "", true, "dev"},
		{"no v", "0.2.0", true, "dev"},
		{"major only", "v1", true, "dev"},
		{"major and minor", "v1.2", true, "dev"},
		{"leading zero", "v01.2.3", true, "dev"},
		{"latest", "latest", true, "dev"},
		{"devel", "(devel)", true, "dev"},
		{"build metadata", "v0.2.0+dirty", true, "dev"},
		{"pseudo-version after prerelease", "v0.2.0-0.20260101000000-abcdefabcdef", true, "dev"},
		{"pseudo-version", "v0.0.0-20260101000000-abcdefabcdef", true, "dev"},
		{"pseudo-version with prerelease", "v1.0.0-pre.0.20260101000000-abcdefabcdef", true, "dev"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			withBuildInfo(t, c.v, c.ok)
			code, stdout, _ := runCmd(t, "version")
			if code != 0 {
				t.Fatalf("exit code = %d, want 0", code)
			}
			if want := "microgateway " + c.want + " (commit none, built unknown)\n"; stdout != want {
				t.Errorf("stdout = %q, want %q", stdout, want)
			}
		})
	}
}

func TestVersionLinkerWins(t *testing.T) {
	saved := version
	t.Cleanup(func() { version = saved })
	version = "1.2.3"
	withBuildInfo(t, "v0.1.0", true)
	code, stdout, _ := runCmd(t, "version")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if want := "microgateway 1.2.3 (commit none, built unknown)\n"; stdout != want {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
}
