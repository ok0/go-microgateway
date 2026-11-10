package cli

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/testwait"
)

const (
	cmDebounce  = 400 * time.Millisecond
	cmPoll      = 1500 * time.Millisecond
	cmBound     = 2 * time.Second
	cmSuccess   = `microgateway_config_reloads_total{result="success"}`
	cmFailure   = `microgateway_config_reloads_total{result="failure"}`
	cmUnchanged = `microgateway_config_reloads_total{result="unchanged"}`
)

func cmConfig(t *testing.T, aURL, bURL string, v int) string {
	t.Helper()
	cfg := twoRouteConfig(aURL, bURL, v >= 2)
	replace := func(cfg, old, repl string) string {
		if n := strings.Count(cfg, old); n != 1 {
			t.Fatalf("config template has %d occurrences of %q, want 1", n, old)
		}
		return strings.Replace(cfg, old, repl, 1)
	}
	cfg = replace(cfg, "version: 1\n", "version: 1\nglobal:\n  shutdown_delay: 0s\n  reload:\n    debounce: "+cmDebounce.String()+"\n    poll_interval: "+cmPoll.String()+"\n")
	if v == 3 {
		cfg = replace(cfg, "admin:\n", "  - name: c\n    match:\n      path: /c\n      path_type: prefix\n    upstream: missing\nadmin:\n")
	}
	return cfg
}

func cmSwap(t *testing.T, dir, from, to, content string) {
	t.Helper()
	if err := os.Mkdir(filepath.Join(dir, to), 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, to, "config.yaml"), []byte(content))
	mustSymlink(t, to, filepath.Join(dir, "..data_tmp"))
	if err := os.Rename(filepath.Join(dir, "..data_tmp"), filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(dir, from)); err != nil {
		t.Fatal(err)
	}
	got, err := os.Readlink(filepath.Join(dir, "..data"))
	if err != nil {
		t.Fatal(err)
	}
	if got != to {
		t.Fatalf("..data points to %q, want %q", got, to)
	}
}

type cmInstance struct {
	name     string
	h        *runHandle
	errb     *syncBuffer
	pollOnly bool
	advanced time.Duration
	s0, f0   float64
}

func (in *cmInstance) admin() string {
	return in.h.addrs.Admin.String()
}

func (in *cmInstance) data() string {
	return in.h.addrs.Listeners["public"].String()
}

func (in *cmInstance) advance(d time.Duration) {
	in.h.clk.Advance(d)
	in.advanced += d
}

func (in *cmInstance) nextTick() time.Duration {
	return cmPoll - in.advanced%cmPoll
}

func (in *cmInstance) state() string {
	return fmt.Sprintf("%s: advanced %v, debounce due %d, waiters %d, stderr:\n%s",
		in.name, in.advanced, in.h.clk.Due(cmDebounce), in.h.clk.Waiters(), in.errb.String())
}

func reloadBoundErr(spent, least, bound time.Duration, observed bool) error {
	if !observed {
		return fmt.Errorf("no reload observed after %v of injected clock advance", spent)
	}
	if spent < least {
		return fmt.Errorf("reload observed after %v of injected clock advance, before the %v the watcher needs", spent, least)
	}
	if spent > bound {
		return fmt.Errorf("reload needed %v of injected clock advance, more than the %v bound", spent, bound)
	}
	return nil
}

func driveReload(t *testing.T, in *cmInstance, what string, observed func() bool) time.Duration {
	t.Helper()
	var detect time.Duration
	if in.pollOnly {
		detect = in.nextTick()
		if detect <= time.Millisecond {
			t.Fatalf("%s: %s: swap is at a poll tick boundary (time to tick %v)", what, in.name, detect)
		}
	}
	need := detect + cmDebounce
	if err := reloadBoundErr(need, need, cmBound, true); err != nil {
		t.Fatalf("%s: %s: %v (detection %v + debounce %v)", what, in.name, err, detect, cmDebounce)
	}
	start := in.advanced
	if observed() {
		t.Fatalf("%s: %s: %v", what, in.name, reloadBoundErr(0, need, cmBound, true))
	}
	if detect > 0 {
		in.advance(detect - time.Millisecond)
		if observed() {
			t.Fatalf("%s: %s: %v", what, in.name, reloadBoundErr(in.advanced-start, need, cmBound, true))
		}
		in.advance(time.Millisecond)
	}
	armed := 1
	if in.nextTick() <= cmDebounce {
		armed++
	}
	awaitDue(t, in.h.clk, armed, cmDebounce, what+": "+in.name+": change detected (debounce armed)", in.state)
	if observed() {
		t.Fatalf("%s: %s: %v", what, in.name, reloadBoundErr(in.advanced-start, need, cmBound, true))
	}
	in.advance(cmDebounce - time.Millisecond)
	if observed() {
		t.Fatalf("%s: %s: %v", what, in.name, reloadBoundErr(in.advanced-start, need, cmBound, true))
	}
	in.advance(time.Millisecond)

	over := false
	testwait.Until(t, what+": "+in.name+": reload observed", func() bool {
		if observed() {
			return true
		}
		if in.h.clk.Due(cmDebounce) > 0 {
			over = true
			if in.advanced-start+cmDebounce > cmBound {
				return true
			}
			in.advance(cmDebounce)
		}
		return false
	}, in.state)

	spent := in.advanced - start
	if over {
		t.Fatalf("%s: %s: %v", what, in.name, reloadBoundErr(spent+cmDebounce, need, cmBound, true))
	}
	if err := reloadBoundErr(spent, need, cmBound, true); err != nil {
		t.Fatalf("%s: %s: %v", what, in.name, err)
	}
	t.Logf("%s: %s reloaded after %v of injected advance", what, in.name, spent)
	return spent
}

func cmGet(t *testing.T, client *http.Client, in *cmInstance, path string) (int, string) {
	t.Helper()
	resp, body := doGet(t, client, in.data(), path, "", nil)
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, ""
	}
	return resp.StatusCode, decodeEcho(t, body).Backend
}

func runConfigMapSwap(t *testing.T, pollOnly bool) {
	dir := t.TempDir()
	a := echoBackend(t, "a")
	b := echoBackend(t, "b")
	v1 := filepath.Join(dir, "..v1")
	if err := os.Mkdir(v1, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(v1, "config.yaml"), []byte(cmConfig(t, a.URL, b.URL, 1)))
	mustSymlink(t, "..v1", filepath.Join(dir, "..data"))
	mustSymlink(t, "..data/config.yaml", filepath.Join(dir, "config.yaml"))
	path := filepath.Join(dir, "config.yaml")
	client := &http.Client{Timeout: testwait.Guard(t), Transport: &http.Transport{DisableKeepAlives: true}}

	var ins []*cmInstance
	for _, name := range []string{"one", "two"} {
		errb := &syncBuffer{}
		h := startRunEnv(t, []string{"-config", path}, errb, Env{PollOnly: pollOnly})
		ins = append(ins, &cmInstance{name: name, h: h, errb: errb, pollOnly: pollOnly})
	}

	for _, in := range ins {
		eventually(t, in.name+": watcher started", func() bool { return metric(t, in.admin(), cmUnchanged) >= 1 })
		logs := in.errb.String()
		if pollOnly {
			if n := strings.Count(logs, "file events disabled; polling only"); n != 1 {
				t.Fatalf("%s: %d polling-only log lines, want 1; stderr:\n%s", in.name, n, logs)
			}
		} else if strings.Contains(logs, "file events disabled") || strings.Contains(logs, "file events unavailable") {
			t.Fatalf("%s: event source not active; stderr:\n%s", in.name, logs)
		}
		in.s0 = metric(t, in.admin(), cmSuccess)
		in.f0 = metric(t, in.admin(), cmFailure)
		if code, body := cmGet(t, client, in, "/a"); code != http.StatusOK || body != "a" {
			t.Fatalf("%s: /a = %d %q, want 200 \"a\"", in.name, code, body)
		}
		if code, _ := cmGet(t, client, in, "/b"); code != http.StatusNotFound {
			t.Fatalf("%s: /b = %d, want 404", in.name, code)
		}
	}

	cmSwap(t, dir, "..v1", "..v2", cmConfig(t, a.URL, b.URL, 2))
	for i, in := range ins {
		for _, later := range ins[i+1:] {
			if got := metric(t, later.admin(), cmSuccess); got != later.s0 {
				t.Fatalf("%s: success = %v before its turn, want %v", later.name, got, later.s0)
			}
			if code, _ := cmGet(t, client, later, "/b"); code != http.StatusNotFound {
				t.Fatalf("%s: /b = %d before its turn, want 404", later.name, code)
			}
		}
		driveReload(t, in, "v2 swap", func() bool { return metric(t, in.admin(), cmSuccess) >= in.s0+1 })
		if got := metric(t, in.admin(), cmSuccess); got != in.s0+1 {
			t.Errorf("%s: success = %v after v2, want %v", in.name, got, in.s0+1)
		}
		if got := metric(t, in.admin(), cmFailure); got != in.f0 {
			t.Errorf("%s: failure = %v after v2, want %v", in.name, got, in.f0)
		}
		if code, body := cmGet(t, client, in, "/b"); code != http.StatusOK || body != "b" {
			t.Errorf("%s: /b after v2 = %d %q, want 200 \"b\"", in.name, code, body)
		}
		if code, body := cmGet(t, client, in, "/a"); code != http.StatusOK || body != "a" {
			t.Errorf("%s: /a after v2 = %d %q, want 200 \"a\"", in.name, code, body)
		}
	}

	cmSwap(t, dir, "..v2", "..v3", cmConfig(t, a.URL, b.URL, 3))
	for i, in := range ins {
		for _, later := range ins[i+1:] {
			if got := metric(t, later.admin(), cmFailure); got != later.f0 {
				t.Fatalf("%s: failure = %v before its turn, want %v", later.name, got, later.f0)
			}
		}
		driveReload(t, in, "v3 swap", func() bool { return metric(t, in.admin(), cmFailure) >= in.f0+1 })
		if got := metric(t, in.admin(), cmFailure); got != in.f0+1 {
			t.Errorf("%s: failure = %v after v3, want %v", in.name, got, in.f0+1)
		}
		if got := metric(t, in.admin(), cmSuccess); got != in.s0+1 {
			t.Errorf("%s: success = %v after v3, want unchanged %v", in.name, got, in.s0+1)
		}
		logs := in.errb.String()
		for _, want := range []string{"configuration reload failed; previous configuration remains active", "routes[2].upstream", "unknown upstream"} {
			if !strings.Contains(logs, want) {
				t.Errorf("%s: stderr lacks %q; stderr:\n%s", in.name, want, logs)
			}
		}
		for range 3 {
			if code, body := cmGet(t, client, in, "/a"); code != http.StatusOK || body != "a" {
				t.Errorf("%s: /a after failed v3 = %d %q, want 200 \"a\"", in.name, code, body)
			}
			if code, body := cmGet(t, client, in, "/b"); code != http.StatusOK || body != "b" {
				t.Errorf("%s: /b after failed v3 = %d %q, want 200 \"b\"", in.name, code, body)
			}
		}
		if code, _ := cmGet(t, client, in, "/c"); code != http.StatusNotFound {
			t.Errorf("%s: /c = %d, want 404", in.name, code)
		}
	}

	for _, in := range ins {
		if got := metric(t, in.admin(), cmSuccess); got != in.s0+1 {
			t.Errorf("%s: final success = %v, want %v", in.name, got, in.s0+1)
		}
		if got := metric(t, in.admin(), cmFailure); got != in.f0+1 {
			t.Errorf("%s: final failure = %v, want %v", in.name, got, in.f0+1)
		}
		if code := in.h.stop(); code != 0 {
			t.Errorf("%s: exit code = %d, want 0; stderr:\n%s", in.name, code, in.errb.String())
		}
	}
}

func TestRunConfigMapSwap(t *testing.T) {
	t.Run("events", func(t *testing.T) { runConfigMapSwap(t, false) })
	t.Run("poll", func(t *testing.T) { runConfigMapSwap(t, true) })
}

func TestReloadBoundErr(t *testing.T) {
	if cmDebounce >= cmBound || cmPoll >= cmBound {
		t.Errorf("cmDebounce %v and cmPoll %v must both be below cmBound %v", cmDebounce, cmPoll, cmBound)
	}
	tests := []struct {
		name     string
		spent    time.Duration
		least    time.Duration
		observed bool
		wantErr  string
	}{
		{"poll within bound", cmPoll + cmDebounce, cmPoll + cmDebounce, true, ""},
		{"at bound", 2 * time.Second, 400 * time.Millisecond, true, ""},
		{"over bound", 2*time.Second + time.Millisecond, 400 * time.Millisecond, true, "more than the 2s bound"},
		{"poll interval raised to 2s", 2*time.Second + cmDebounce, 2*time.Second + cmDebounce, true, "more than the 2s bound"},
		{"default poll interval", 5*time.Second + 250*time.Millisecond, 5*time.Second + 250*time.Millisecond, true, "more than the 2s bound"},
		{"clock never advanced", 0, cmDebounce, false, "no reload observed after 0s"},
		{"reload before debounce", 0, cmDebounce, true, "before the 400ms"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := reloadBoundErr(tc.spent, tc.least, cmBound, tc.observed)
			if tc.wantErr == "" {
				if err != nil {
					t.Errorf("reloadBoundErr = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("reloadBoundErr = %v, want an error containing %q", err, tc.wantErr)
			}
		})
	}
}
