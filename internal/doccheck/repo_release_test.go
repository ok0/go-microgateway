package doccheck

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"go/version"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

const (
	apacheSHA256    = "c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4"
	distrolessImage = "gcr.io/distroless/static-debian13:nonroot"
)

var scrubbedEnvNames = []string{"MICROGATEWAY_HEAVY", "MICROGATEWAY_REDIS_ADDR", "MICROGATEWAY_SOAK", "MAKEFLAGS", "MFLAGS", "GOFLAGS", "VERSION", "MICROGATEWAY_RELEASE_VERSION"}

var requiredMakeTargets = []string{"build", "test", "race", "vet", "cover", "lint", "tidy-check", "realnet", "bench", "fuzz", "soak", "perf", "integration", "docker", "hygiene", "check", "help", "release-prepare", "release-check", "release-verify", "reload-load", "workflow-lint"}

var checklistRequired = []string{
	"make perf", "make soak", "make fuzz", "make integration", "make realnet", "make docker", "make lint", "make reload-load",
	"MICROGATEWAY_HEAVY", "MICROGATEWAY_SOAK", "MICROGATEWAY_LOAD", "MICROGATEWAY_REDIS_ADDR",
	"MICROGATEWAY_REDIS_CLUSTER", "MICROGATEWAY_REDIS_SENTINEL", "MICROGATEWAY_CACHE_FULL",
	"TestCoalesceScale", "TestCacheMixedLoad", "TestPurgeLoad", "TestPurgeMarkerCapDefault",
	"TestInstancesMixedLoad", "TestHostGaugesChurnLoad", "TestMixedMultiInstanceLoad",
	"TestIntegrationSuite", "TestIntegrationTwoInstances", "TestConnectTimeoutRealClockTCP",
	"TestCloseStopsIdleTimers", "-count=20", "Mermaid", "VPN", "vX.Y.Z", "Go 1.27",
}

var (
	workdirRE     = regexp.MustCompile(`WORKDIR \S*redis\b|cd redis\b`)
	dockerXFlagRE = regexp.MustCompile(`-X ([^\s=]+)=`)
)

// git runs git in the repository root and returns its standard output and exit status. The test is skipped when git is not installed or the root is not a work tree.
func git(t *testing.T, args ...string) ([]byte, int) {
	t.Helper()
	root := repoRoot(t)
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("not a git checkout: " + t.Name() + " not run")
	}
	if out, err := exec.Command("git", "-C", root, "rev-parse", "--is-inside-work-tree").Output(); err != nil || strings.TrimSpace(string(out)) != "true" {
		t.Skip("not a git checkout: " + t.Name() + " not run")
	}
	out, err := exec.Command("git", append([]string{"-C", root}, args...)...).Output()
	if err == nil {
		return out, 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return out, exitErr.ExitCode()
	}
	t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	return nil, -1
}

// shallowOutput reads the output of git rev-parse --is-shallow-repository.
func shallowOutput(out []byte) (bool, error) {
	s := strings.TrimSpace(string(out))
	switch s {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, fmt.Errorf("git rev-parse --is-shallow-repository printed %q, not true or false", s)
}

// requireFullHistory fails the test when the repository is a shallow clone. The test reads all history and all tags, which a shallow clone does not have.
func requireFullHistory(t *testing.T) {
	t.Helper()
	out, status := git(t, "rev-parse", "--is-shallow-repository")
	if status != 0 {
		t.Fatalf("git rev-parse --is-shallow-repository: exit status %d", status)
	}
	shallow, err := shallowOutput(out)
	if err != nil {
		t.Fatal(err)
	}
	if shallow {
		t.Fatal(t.Name() + " needs all history and all tags, but this repository is a shallow clone; clone without --depth, run git fetch --unshallow --tags, or set fetch-depth: 0 on actions/checkout")
	}
}

// run executes name with args in dir. The environment is the process environment without the heavy-check, soak and build-flag variables, plus env. It returns the combined output.
func run(t *testing.T, dir string, env []string, name string, args ...string) ([]byte, error) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(scrubbedEnv(), env...)
	return cmd.CombinedOutput()
}

func scrubbedEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if slices.Contains(scrubbedEnvNames, name) {
			continue
		}
		env = append(env, kv)
	}
	return env
}

func requireMake(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("make"); err != nil {
		t.Skip("make not on PATH: " + t.Name() + " not run")
	}
}

func readAt(t *testing.T, root, rel string) []byte {
	t.Helper()
	src, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func readLines(t *testing.T, root, rel string) []string {
	t.Helper()
	var out []string
	for _, line := range strings.Split(string(readAt(t, root, rel)), "\n") {
		out = append(out, strings.TrimSpace(line))
	}
	return out
}

func splitNUL(out []byte) []string {
	var items []string
	for _, item := range strings.Split(string(out), "\x00") {
		if item != "" {
			items = append(items, item)
		}
	}
	return items
}

func outputLines(out []byte) []string {
	var lines []string
	for _, line := range strings.Split(string(out), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// repoFiles returns the sorted repository files that a commit of the working tree would contain: the cached and untracked, not ignored files, without paths that no longer exist on disk.
func repoFiles(t *testing.T) []string {
	t.Helper()
	root := repoRoot(t)
	out, status := git(t, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
	if status != 0 {
		t.Fatalf("git ls-files: exit status %d", status)
	}
	var files []string
	for _, name := range splitNUL(out) {
		_, err := os.Lstat(filepath.Join(root, filepath.FromSlash(name)))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, name)
	}
	slices.Sort(files)
	return slices.Compact(files)
}

// section returns the text of the first heading with the given text: the lines after it up to the next heading of the same or a higher level. first and last are the 1-based numbers of its first and last lines.
func section(d *Doc, text string) (body string, first, last int, ok bool) {
	for i, h := range d.Headings {
		if h.Text != text {
			continue
		}
		last = len(d.Lines)
		for _, next := range d.Headings[i+1:] {
			if next.Level <= h.Level {
				last = next.Line - 1
				break
			}
		}
		first = h.Line + 1
		if first > last {
			return "", first, last, true
		}
		return strings.Join(d.Lines[first-1:last], "\n"), first, last, true
	}
	return "", 0, 0, false
}

// makeVariable returns the value of the makefile variable name, or the empty string when the makefile does not set it.
func makeVariable(makefile []byte, name string) string {
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + `[ \t]*[:?]?=[ \t]*(.*?)[ \t]*$`)
	m := re.FindSubmatch(makefile)
	if m == nil {
		return ""
	}
	return string(m[1])
}

func instructionOf(line string) string {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return ""
	}
	return strings.ToUpper(fields[0])
}

func flagNames(s string) []string {
	var names []string
	for _, m := range dockerXFlagRE.FindAllStringSubmatch(s, -1) {
		names = append(names, m[1])
	}
	slices.Sort(names)
	return slices.Compact(names)
}

func mapOf(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func listOf(v any) []any {
	l, _ := v.([]any)
	return l
}

func isFalse(v any) bool {
	return v == any(false) || v == any("false")
}

func nonEmptyString(v any) bool {
	s, _ := v.(string)
	return s != ""
}

// jobEnv returns the values of an environment variable set at the job level or on any of its steps.
func jobEnv(job map[string]any, name string) []any {
	var vals []any
	if v, ok := mapOf(job["env"])[name]; ok {
		vals = append(vals, v)
	}
	for _, raw := range listOf(job["steps"]) {
		if v, ok := mapOf(mapOf(raw)["env"])[name]; ok {
			vals = append(vals, v)
		}
	}
	return vals
}

// stepEnv returns the environment of a step: the workflow-level env, the job env and the step env merged, later maps winning, with each value formatted as text.
func stepEnv(workflow, job, step map[string]any) map[string]string {
	env := map[string]string{}
	for _, m := range []map[string]any{mapOf(workflow["env"]), mapOf(job["env"]), mapOf(step["env"])} {
		for k, v := range m {
			env[k] = fmt.Sprint(v)
		}
	}
	return env
}

// integrationRunOK reports whether a step runs a race-enabled go test with the integration tag in the redis module. env holds the workflow and job env; each step's own env is merged over it. Make invocations are expanded against makefile, and a step whose run does not expand is not counted.
func integrationRunOK(steps []any, makefile []byte, env map[string]string) bool {
	for _, raw := range steps {
		step := mapOf(raw)
		run, _ := step["run"].(string)
		merged := map[string]string{}
		maps.Copy(merged, env)
		maps.Copy(merged, stepEnv(nil, nil, step))
		text, err := ExpandMakeRun(run, makefile, merged)
		if err != nil {
			continue
		}
		inRedis := strings.Contains(text, "cd redis") || step["working-directory"] == any("redis")
		if strings.Contains(text, "go test") && strings.Contains(text, "-race") && strings.Contains(text, "-tags integration") && inRedis {
			return true
		}
	}
	return false
}

// hygieneExempt reports whether a hygiene finding is the planning directory entry of an ignore file.
func hygieneExempt(name string, f Finding) bool {
	if (name != ".gitignore" && name != ".dockerignore") || f.Line == 0 {
		return false
	}
	line := strings.TrimSpace(f.Text)
	return line == plannedDir || line == plannedDir+"/"
}

func TestTrackedFileHygiene(t *testing.T) {
	root := repoRoot(t)
	files := repoFiles(t)
	if len(files) < 300 {
		t.Fatalf("repository lists %d files, want at least 300", len(files))
	}
	for _, name := range files {
		for _, f := range Hygiene(name, readAt(t, root, name)) {
			if hygieneExempt(name, f) {
				continue
			}
			t.Errorf("%s:%d: %s", name, f.Line, strings.TrimSpace(f.Text))
		}
	}
}

func TestCommitMessageHygiene(t *testing.T) {
	requireFullHistory(t)
	refsOut, status := git(t, "for-each-ref", "--format=%(refname)")
	if status != 0 {
		t.Fatalf("git for-each-ref: exit status %d", status)
	}
	refs := ScanRefs(outputLines(refsOut))
	if len(refs) == 0 {
		t.Fatal("no refs to scan")
	}
	logOut, status := git(t, append([]string{"log", "--format=%H%x1f%B%x1e"}, refs...)...)
	if status != 0 {
		t.Fatalf("git log: exit status %d", status)
	}
	commits := ParseLog(logOut)
	t.Logf("%d commits scanned", len(commits))
	if len(commits) == 0 {
		t.Fatal("no commits scanned")
	}
	for _, f := range CommitFindings(commits) {
		t.Errorf("commit %s:%d: %s", f.Path, f.Line, f.Text)
	}
}

func TestRemotes(t *testing.T) {
	out, status := git(t, "remote", "-v")
	if status != 0 {
		t.Fatalf("git remote -v: exit status %d", status)
	}
	remotes := ParseRemoteV(out)
	cfg, status := git(t, "config", "--get-regexp", `^remote\..*\.(url|pushurl)$`)
	if status != 0 && (status != 1 || len(bytes.TrimSpace(cfg)) != 0) {
		t.Fatalf("git config: exit status %d", status)
	}
	remotes = append(remotes, ParseRemoteConfig(cfg)...)
	for _, f := range RemoteFindings(remotes) {
		t.Errorf("remote %s: %q is outside github.com/ok0/go-microgateway", f.Path, f.Text)
	}
	if len(remotes) == 0 {
		t.Log("no remotes configured")
		return
	}
	for _, r := range remotes {
		t.Logf("remote %s %s %s", r.Name, r.Kind, r.URL)
	}
}

func TestTrackedGoLines(t *testing.T) {
	root := repoRoot(t)
	n := 0
	for _, name := range repoFiles(t) {
		if !strings.HasSuffix(name, ".go") {
			continue
		}
		n += CountLines(readAt(t, root, name))
	}
	t.Logf("tracked Go lines (including tests): %d", n)
	if err := LineGate(n); err != nil {
		t.Error(err)
	}
}

func TestModuleDependencies(t *testing.T) {
	root := repoRoot(t)
	core := DirectRequires(readAt(t, root, "go.mod"))
	redis := DirectRequires(readAt(t, root, "redis/go.mod"))
	for _, p := range DependencyFindings(core, redis) {
		t.Error(p)
	}
	out, err := run(t, root, []string{"GOWORK=off"}, "go", "list", "-m")
	if err != nil {
		t.Fatalf("go list -m in %s: %v\n%s", root, err, out)
	}
	if got := strings.TrimSpace(string(out)); got != "github.com/ok0/go-microgateway" {
		t.Errorf("go list -m in %s = %q, want %q", root, got, "github.com/ok0/go-microgateway")
	}
	redisDir := filepath.Join(root, "redis")
	env := []string{"GOWORK=" + filepath.Join(root, "go.work")}
	out, err = run(t, redisDir, env, "go", "list", "-m")
	if err != nil {
		t.Fatalf("go list -m in %s: %v\n%s", redisDir, err, out)
	}
	mods := outputLines(out)
	slices.Sort(mods)
	wantMods := []string{"github.com/ok0/go-microgateway", "github.com/ok0/go-microgateway/redis"}
	if !slices.Equal(mods, wantMods) {
		t.Errorf("go list -m in the workspace = %q, want %q", mods, wantMods)
	}
	out, err = run(t, redisDir, env, "go", "list", "-m", "-f", "{{.Dir}}", "github.com/ok0/go-microgateway")
	if err != nil {
		t.Fatalf("go list -m -f in %s: %v\n%s", redisDir, err, out)
	}
	coreDir := strings.TrimSpace(string(out))
	wantInfo, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	gotInfo, err := os.Stat(coreDir)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(gotInfo, wantInfo) {
		t.Errorf("core module in the workspace resolves to %s, want the checkout %s", coreDir, root)
	}
	readme := readAt(t, root, "README.md")
	for _, dep := range slices.Concat(core, redis) {
		if dep == "github.com/ok0/go-microgateway" {
			continue
		}
		if !bytes.Contains(readme, []byte(dep)) {
			t.Errorf("README.md does not name the direct dependency %s", dep)
		}
	}
}

func TestModulesInstallable(t *testing.T) {
	root := repoRoot(t)
	core, err := ParseGoMod(readAt(t, root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	redis, err := ParseGoMod(readAt(t, root, "redis/go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	r := redis.Requires[corePath]
	t.Logf("redis/go.mod requires %s %s", corePath, r)
	for _, f := range ReleaseFindings(r, readAt(t, root, "go.work"), core, redis) {
		t.Errorf("%s:%d: %s (%s)", f.Path, f.Line, f.Text, f.Pattern)
	}
}

func TestIgnoreFiles(t *testing.T) {
	root := repoRoot(t)
	gitignore := readLines(t, root, ".gitignore")
	dockerignore := readLines(t, root, ".dockerignore")
	for _, p := range ignoreFileProblems(gitignore, dockerignore) {
		t.Error(p)
	}
	files := repoFiles(t)
	for _, name := range files {
		switch {
		case strings.HasPrefix(name, ".docs/"), strings.HasPrefix(name, plannedDir+"/"), strings.HasPrefix(name, "bin/"):
			t.Errorf("repository file under an ignored directory: %s", name)
		}
	}
	for _, p := range trackedWorkProblems(files) {
		t.Error(p)
	}
}

func TestLicense(t *testing.T) {
	root := repoRoot(t)
	parts := strings.SplitN(string(readAt(t, root, "LICENSE")), "\n", 3)
	if len(parts) < 3 || parts[0] != "Copyright 2026 go-microgateway authors" || parts[1] != "" {
		t.Fatal("LICENSE must start with the copyright line followed by an empty line")
	}
	sum := sha256.Sum256([]byte(parts[2]))
	if got := hex.EncodeToString(sum[:]); got != apacheSHA256 {
		t.Errorf("LICENSE text sha256 = %s, want %s", got, apacheSHA256)
	}
	readme, err := Load(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	body, first, last, ok := section(readme, "License")
	if !ok {
		t.Fatal("README has no License heading")
	}
	if !strings.Contains(body, "Apache-2.0") {
		t.Error("README License section lacks Apache-2.0")
	}
	linked := false
	for _, l := range readme.Links {
		if l.Target == "LICENSE" && l.Line >= first && l.Line <= last {
			linked = true
		}
	}
	if !linked {
		t.Error("README License section lacks a link to LICENSE")
	}
}

func TestMakefileTargets(t *testing.T) {
	requireMake(t)
	root := repoRoot(t)
	targets := MakeTargets(readAt(t, root, "Makefile"))
	for _, name := range requiredMakeTargets {
		if _, ok := targets[name]; !ok {
			t.Errorf("Makefile has no %s target", name)
		}
		if !slices.Contains(targets[".PHONY"], name) {
			t.Errorf("%s is not listed in .PHONY", name)
		}
	}
	if got, want := targets["check"], []string{"vet", "cover", "lint", "tidy-check", "hygiene"}; !slices.Equal(got, want) {
		t.Errorf("check prerequisites = %q, want %q", got, want)
	}
	for _, name := range requiredMakeTargets {
		if out, err := run(t, root, nil, "make", "-n", "-C", root, name); err != nil {
			t.Errorf("make -n %s: %v\n%s", name, err, out)
		}
	}
}

func TestMakefileHeavyGates(t *testing.T) {
	requireMake(t)
	root := repoRoot(t)
	for _, target := range []string{"fuzz", "soak", "perf", "integration", "release-verify", "reload-load"} {
		out, err := run(t, root, nil, "make", "-C", root, target)
		if err == nil {
			t.Errorf("make %s without MICROGATEWAY_HEAVY succeeded", target)
		}
		if !bytes.Contains(out, []byte("MICROGATEWAY_HEAVY=1")) {
			t.Errorf("make %s output does not name MICROGATEWAY_HEAVY=1:\n%s", target, out)
		}
	}
	out, err := run(t, root, []string{"MICROGATEWAY_HEAVY=1"}, "make", "-C", root, "integration")
	if err == nil {
		t.Error("make integration without a Redis address succeeded")
	}
	if !bytes.Contains(out, []byte("MICROGATEWAY_REDIS_ADDR")) {
		t.Errorf("make integration output does not name MICROGATEWAY_REDIS_ADDR:\n%s", out)
	}
}

// workflowFiles returns the sorted workflow files of the repository, relative to root. It fails the test unless ci.yml and release.yml are among them.
func workflowFiles(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, ".github", "workflows"))
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, e := range entries {
		name := e.Name()
		if !e.Type().IsRegular() || (!strings.HasSuffix(name, ".yml") && !strings.HasSuffix(name, ".yaml")) {
			continue
		}
		files = append(files, ".github/workflows/"+name)
	}
	slices.Sort(files)
	for _, want := range []string{".github/workflows/ci.yml", ".github/workflows/release.yml"} {
		if !slices.Contains(files, want) {
			t.Fatalf("%s is not among the workflow files %q", want, files)
		}
	}
	return files
}

// jobCheckoutFindings returns the checkout findings of every job of wf, in sorted job order, each prefixed with its job name.
func jobCheckoutFindings(wf map[string]any) []string {
	jobs := mapOf(wf["jobs"])
	var findings []string
	for _, name := range slices.Sorted(maps.Keys(jobs)) {
		for _, f := range checkoutFindings(listOf(mapOf(jobs[name])["steps"])) {
			findings = append(findings, "job "+name+": "+f)
		}
	}
	return findings
}

func TestWorkflow(t *testing.T) {
	root := repoRoot(t)
	makefile := readAt(t, root, "Makefile")
	for _, rel := range workflowFiles(t, root) {
		var wf map[string]any
		if err := yaml.Unmarshal(readAt(t, root, rel), &wf); err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		if rel == ".github/workflows/ci.yml" {
			for _, problem := range workflowProblems(wf, makefile, goFloor(t, root)) {
				t.Error(problem)
			}
			continue
		}
		for _, f := range jobCheckoutFindings(wf) {
			t.Errorf("%s: %s", rel, f)
		}
	}
}

func TestCheckoutFindings(t *testing.T) {
	cases := []struct {
		name, steps string
		want        int
		subs        []string
	}{
		{"no with", "- uses: actions/checkout@v7\n", 1, []string{"fetch-depth: 0 (got none)"}},
		{"depth 1", "- uses: actions/checkout@v7\n  with: {fetch-depth: 1}\n", 1, []string{"got 1"}},
		{"depth string 1", "- uses: actions/checkout@v7\n  with: {fetch-depth: \"1\"}\n", 1, []string{"fetch-depth: 0"}},
		{"depth empty", "- uses: actions/checkout@v7\n  with: {fetch-depth: }\n", 1, []string{"fetch-depth: 0"}},
		{"depth 0 tags false", "- uses: actions/checkout@v7\n  with: {fetch-depth: 0, fetch-tags: false}\n", 1, []string{"fetch-tags: false"}},
		{"depth 1 tags false", "- uses: actions/checkout@v7\n  with: {fetch-depth: 1, fetch-tags: false}\n", 2, []string{"fetch-depth: 0", "fetch-tags: false"}},
		{"depth 0", "- uses: actions/checkout@v7\n  with: {fetch-depth: 0}\n", 0, nil},
		{"depth string 0", "- uses: actions/checkout@v7\n  with: {fetch-depth: \"0\"}\n", 0, nil},
		{"depth 0 tags true", "- uses: actions/checkout@v7\n  with: {fetch-depth: 0, fetch-tags: true}\n", 0, nil},
		{"other action", "- uses: actions/setup-go@v7\n", 0, nil},
		{"second step bad", "- uses: actions/checkout@v7\n  with: {fetch-depth: 0}\n- uses: actions/checkout@v7\n", 1, []string{"step 1 "}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var steps []any
			if err := yaml.Unmarshal([]byte(c.steps), &steps); err != nil {
				t.Fatal(err)
			}
			found := checkoutFindings(steps)
			if len(found) != c.want {
				t.Fatalf("checkoutFindings = %q, want %d findings", found, c.want)
			}
			for _, sub := range c.subs {
				if !slices.ContainsFunc(found, func(f string) bool { return strings.Contains(f, sub) }) {
					t.Errorf("no finding contains %q: %q", sub, found)
				}
			}
		})
	}
}

func TestWorkflowCheckoutDepthRequired(t *testing.T) {
	root := repoRoot(t)
	makefile := readAt(t, root, "Makefile")
	floor := goFloor(t, root)
	for _, rel := range workflowFiles(t, root) {
		src := string(readAt(t, root, rel))
		lines := strings.Split(src, "\n")
		depthLines, checkoutLines := 0, 0
		for _, line := range lines {
			if strings.TrimSpace(line) == "fetch-depth: 0" {
				depthLines++
			}
			if strings.Contains(line, "uses: actions/checkout@") {
				checkoutLines++
			}
		}
		if depthLines == 0 || depthLines != checkoutLines {
			t.Fatalf("%s has %d fetch-depth: 0 lines and %d actions/checkout lines, want equal and non-zero", rel, depthLines, checkoutLines)
		}
		var wf map[string]any
		if err := yaml.Unmarshal([]byte(src), &wf); err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		problems := jobCheckoutFindings
		if rel == ".github/workflows/ci.yml" {
			problems = func(m map[string]any) []string { return workflowProblems(m, makefile, floor) }
		}
		for _, p := range problems(wf) {
			if strings.Contains(p, "fetch-depth") {
				t.Errorf("unmodified %s reports %q", rel, p)
			}
		}
		for i, line := range lines {
			if strings.TrimSpace(line) != "fetch-depth: 0" {
				continue
			}
			mutated := strings.Join(slices.Delete(slices.Clone(lines), i, i+1), "\n")
			var m map[string]any
			if err := yaml.Unmarshal([]byte(mutated), &m); err != nil {
				t.Fatalf("%s: %v", rel, err)
			}
			found := false
			for _, p := range problems(m) {
				if strings.Contains(p, "fetch-depth: 0") {
					found = true
				}
			}
			if !found {
				t.Errorf("removing line %d of %s reports no fetch-depth: 0 problem", i+1, rel)
			}
		}
	}
}

func TestShallowOutput(t *testing.T) {
	cases := []struct {
		in      string
		want    bool
		wantErr bool
	}{
		{"true\n", true, false},
		{"false\n", false, false},
		{"false", false, false},
		{"", false, true},
		{"--is-shallow-repository\n", false, true},
		{"TRUE\n", false, true},
		{"yes", false, true},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%q", c.in), func(t *testing.T) {
			got, err := shallowOutput([]byte(c.in))
			if (err != nil) != c.wantErr {
				t.Fatalf("shallowOutput(%q) error = %v, want error %t", c.in, err, c.wantErr)
			}
			if got != c.want {
				t.Errorf("shallowOutput(%q) = %t, want %t", c.in, got, c.want)
			}
		})
	}
}

func TestContributingFullClone(t *testing.T) {
	root := repoRoot(t)
	body := string(readAt(t, root, "CONTRIBUTING.md"))
	for _, want := range []string{"full clone", "shallow clone", "fetch-depth: 0", "TestReleaseCheck", "TestCommitMessageHygiene"} {
		if !strings.Contains(body, want) {
			t.Errorf("CONTRIBUTING.md does not contain %q", want)
		}
	}
}

func workflowProblems(wf map[string]any, makefile []byte, floor string) []string {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}
	if _, ok := mapOf(wf["env"])["GOWORK"]; ok {
		add("top-level env sets GOWORK; set the command mode per job")
	}
	jobs := mapOf(wf["jobs"])
	for _, name := range []string{"lint", "test", "coverage", "tidy", "integration", "realnet", "docker", "actionlint"} {
		if _, ok := jobs[name]; !ok {
			add("workflow has no %s job", name)
		}
	}
	var uses []string
	for _, name := range slices.Sorted(maps.Keys(jobs)) {
		job := mapOf(jobs[name])
		if v, ok := job["continue-on-error"]; ok && !isFalse(v) {
			add("job %s sets continue-on-error to %v", name, v)
		}
		for _, raw := range listOf(job["steps"]) {
			step := mapOf(raw)
			if v, ok := step["continue-on-error"]; ok && !isFalse(v) {
				add("a step of job %s sets continue-on-error to %v", name, v)
			}
			if u, ok := step["uses"].(string); ok {
				uses = append(uses, u)
			}
		}
		for _, f := range checkoutFindings(listOf(job["steps"])) {
			add("job %s: %s", name, f)
		}
	}

	for _, name := range slices.Sorted(maps.Keys(jobs)) {
		job := mapOf(jobs[name])
		for i, raw := range listOf(job["steps"]) {
			step := mapOf(raw)
			for _, target := range []string{"release-prepare", "release-verify"} {
				if stepRun, _ := step["run"].(string); strings.Contains(stepRun, target) {
					add("job %s step %d runs %s; release steps are run by the repository owner", name, i, target)
				}
			}
			env := stepEnv(wf, job, step)
			dir := "."
			if wd, ok := step["working-directory"].(string); ok {
				dir = wd
			} else if step["uses"] == any("golangci/golangci-lint-action@v9") {
				if wd, ok := mapOf(step["with"])["working-directory"].(string); ok {
					dir = wd
				}
			}
			var cmds []goCommand
			if step["uses"] == any("golangci/golangci-lint-action@v9") {
				_, set := env["GOWORK"]
				cmds = []goCommand{{Dir: dir, GOWORK: env["GOWORK"], Set: set, Text: "golangci-lint"}}
			} else if runText, ok := step["run"].(string); ok {
				text, err := ExpandMakeRun(runText, makefile, env)
				if err != nil {
					text = runText
				}
				cmds = goCommands(text, dir, env)
			}
			for _, p := range modeProblems(cmds, "$(CURDIR)/go.work", "${{ github.workspace }}/go.work") {
				add("job %s step %d: %s", name, i, p)
			}
		}
	}

	exactPatch := regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
	pin := mapOf(wf["env"])["GO_VERSION"]
	pinText, _ := pin.(string)
	if !exactPatch.MatchString(pinText) || version.Compare("go"+pinText, "go"+floor) < 0 {
		add("workflow env GO_VERSION = %v, want an exact patch release not older than the go.mod floor %s", pin, floor)
	}
	for _, name := range []string{"lint", "coverage", "tidy", "integration", "realnet", "actionlint"} {
		found, pinned := false, true
		for _, raw := range listOf(mapOf(jobs[name])["steps"]) {
			step := mapOf(raw)
			if step["uses"] != any("actions/setup-go@v7") {
				continue
			}
			found = true
			if mapOf(step["with"])["go-version"] != any("${{ env.GO_VERSION }}") {
				pinned = false
			}
		}
		if !found || !pinned {
			add("job %s does not set up Go at the pinned ${{ env.GO_VERSION }}", name)
		}
	}

	testJob := mapOf(jobs["test"])
	if !isFalse(mapOf(testJob["strategy"])["fail-fast"]) {
		add("test matrix does not set fail-fast: false")
	}
	matrix := mapOf(mapOf(testJob["strategy"])["matrix"])
	var goEntries []string
	for _, v := range listOf(matrix["go"]) {
		goEntries = append(goEntries, fmt.Sprint(v))
	}
	if !slices.Contains(goEntries, floor) {
		add("test matrix go lacks the go.mod floor %s", floor)
	}
	if !slices.Contains(goEntries, "stable") {
		add("test matrix go lacks stable")
	}
	for _, e := range goEntries {
		if e != floor && e != "stable" {
			add("test matrix go has %v, want exactly [%s stable]", goEntries, floor)
			break
		}
	}
	setupFromMatrix, rootRace, redisRace := false, false, false
	for _, raw := range listOf(testJob["steps"]) {
		step := mapOf(raw)
		if step["uses"] == any("actions/setup-go@v7") && mapOf(step["with"])["go-version"] == any("${{ matrix.go }}") {
			setupFromMatrix = true
		}
		if run, _ := step["run"].(string); run == "go test -race ./..." {
			if step["working-directory"] == nil {
				rootRace = true
			}
			if step["working-directory"] == any("redis") {
				redisRace = true
			}
		}
	}
	if !setupFromMatrix {
		add("test job does not set up Go from matrix.go")
	}
	if !rootRace || !redisRace {
		add("test job does not run go test -race in both modules")
	}

	for _, want := range []string{"ubuntu-latest", "macos-latest"} {
		if !slices.Contains(listOf(matrix["os"]), any(want)) {
			add("test matrix os lacks %s", want)
		}
	}

	integration := mapOf(jobs["integration"])
	if !isFalse(mapOf(integration["strategy"])["fail-fast"]) {
		add("integration matrix does not set fail-fast: false")
	}
	var redisEntries []string
	for _, v := range listOf(mapOf(mapOf(integration["strategy"])["matrix"])["redis"]) {
		redisEntries = append(redisEntries, fmt.Sprint(v))
	}
	if !slices.Contains(redisEntries, "redis:6.2") {
		add("integration matrix lacks redis:6.2")
	}
	if !slices.Contains(redisEntries, "redis:7.4") {
		add("integration matrix lacks redis:7.4")
	}
	for _, e := range redisEntries {
		if e != "redis:6.2" && e != "redis:7.4" {
			add("integration matrix redis has %v, want exactly [redis:6.2 redis:7.4]", redisEntries)
			break
		}
	}
	if image, _ := mapOf(mapOf(integration["services"])["redis"])["image"].(string); image != "${{ matrix.redis }}" {
		add("integration redis service image = %q, want ${{ matrix.redis }}", image)
	}
	if !slices.Contains(jobEnv(integration, "MICROGATEWAY_HEAVY"), any("1")) {
		add("integration job does not set MICROGATEWAY_HEAVY to \"1\"")
	}
	if !slices.ContainsFunc(jobEnv(integration, "MICROGATEWAY_REDIS_ADDR"), nonEmptyString) {
		add("integration job does not set a non-empty MICROGATEWAY_REDIS_ADDR")
	}
	var expanded []string
	for _, raw := range listOf(integration["steps"]) {
		run, _ := mapOf(raw)["run"].(string)
		if text, err := ExpandMakeRun(run, makefile, stepEnv(wf, integration, mapOf(raw))); err == nil {
			expanded = append(expanded, text)
		}
	}
	if !integrationRunOK(listOf(integration["steps"]), makefile, stepEnv(wf, integration, nil)) {
		add("integration job runs no race-enabled integration-tagged go test in the redis module; expanded runs: %q", expanded)
	}
	for _, name := range slices.Sorted(maps.Keys(jobs)) {
		for i, raw := range listOf(mapOf(jobs[name])["steps"]) {
			run, _ := mapOf(raw)["run"].(string)
			if _, err := ExpandMakeRun(run, makefile, stepEnv(wf, mapOf(jobs[name]), mapOf(raw))); err != nil {
				add("job %s step %d: %v", name, i, err)
			}
		}
	}

	allowed := []string{"actions/checkout@v7", "actions/setup-go@v7", "golangci/golangci-lint-action@v9"}
	seen := map[string]bool{}
	for _, u := range uses {
		if !slices.Contains(allowed, u) {
			add("workflow uses %q, which is not an allowed action", u)
		}
		seen[u] = true
	}
	for _, a := range allowed {
		if !seen[a] {
			add("no step uses %s", a)
		}
	}

	lintVersion := makeVariable(makefile, "GOLANGCI_LINT_VERSION")
	lintPinned := false
	for _, raw := range listOf(mapOf(jobs["lint"])["steps"]) {
		step := mapOf(raw)
		if step["uses"] == any("golangci/golangci-lint-action@v9") && mapOf(step["with"])["version"] == any(lintVersion) {
			lintPinned = true
		}
	}
	if !lintPinned {
		add("lint job golangci action version is not the Makefile GOLANGCI_LINT_VERSION %q", lintVersion)
	}

	alVersion := makeVariable(makefile, "ACTIONLINT_VERSION")
	if !regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+$`).MatchString(alVersion) {
		add("Makefile ACTIONLINT_VERSION = %q, want an exact release vX.Y.Z", alVersion)
	}
	alJob := mapOf(jobs["actionlint"])
	alPinned := false
	for _, raw := range listOf(alJob["steps"]) {
		step := mapOf(raw)
		run, _ := step["run"].(string)
		text, err := ExpandMakeRun(run, makefile, stepEnv(wf, alJob, step))
		if err == nil && strings.Contains(text, "go run github.com/rhysd/actionlint/cmd/actionlint@"+alVersion) {
			alPinned = true
		}
	}
	if !alPinned {
		add("actionlint job does not run actionlint at the Makefile ACTIONLINT_VERSION %q", alVersion)
	}

	dockerSteps := listOf(mapOf(jobs["docker"])["steps"])
	built := -1
	for i, raw := range dockerSteps {
		if run, _ := mapOf(raw)["run"].(string); strings.TrimSpace(run) == "make docker" {
			built = i
			break
		}
	}
	if built < 0 {
		add("docker job does not run make docker")
	}
	passwdChecked := false
	if built >= 0 {
		for _, raw := range dockerSteps[built+1:] {
			run, _ := mapOf(raw)["run"].(string)
			if strings.Contains(run, "docker create") && strings.Contains(run, "/etc/passwd") && strings.Contains(run, "^nonroot:x:65532:65532:") {
				passwdChecked = true
			}
		}
	}
	if !passwdChecked {
		add("docker job does not check /etc/passwd of the built image for nonroot:x:65532:65532:")
	}
	return problems
}

// checkoutFindings reports each actions/checkout step that does not fetch all history and all tags: fetch-depth must be 0 and fetch-tags must not be false.
func checkoutFindings(steps []any) []string {
	var findings []string
	for i, raw := range steps {
		step := mapOf(raw)
		uses, ok := step["uses"].(string)
		if !ok || !strings.HasPrefix(uses, "actions/checkout@") {
			continue
		}
		with := mapOf(step["with"])
		depth, ok := with["fetch-depth"]
		if !ok || (depth != any(0) && depth != any("0")) {
			got := "none"
			if ok {
				got = fmt.Sprint(depth)
			}
			findings = append(findings, fmt.Sprintf("step %d (%s) does not set fetch-depth: 0 (got %s)", i, uses, got))
		}
		if v, ok := with["fetch-tags"]; ok && isFalse(v) {
			findings = append(findings, fmt.Sprintf("step %d (%s) sets fetch-tags: false", i, uses))
		}
	}
	return findings
}

func goFloor(t *testing.T, root string) string {
	t.Helper()
	var floors []string
	for _, rel := range []string{"go.mod", "redis/go.mod"} {
		floor := ""
		for _, line := range strings.Split(string(readAt(t, root, rel)), "\n") {
			if f, ok := strings.CutPrefix(strings.TrimSpace(line), "go "); ok {
				floor = strings.TrimSpace(f)
				break
			}
		}
		if floor == "" {
			t.Fatalf("%s has no go directive", rel)
		}
		floors = append(floors, floor)
	}
	if floors[0] != floors[1] {
		t.Fatalf("go.mod declares go %s but redis/go.mod declares go %s", floors[0], floors[1])
	}
	return floors[0]
}

func setupGoWith(job map[string]any) map[string]any {
	for _, raw := range listOf(job["steps"]) {
		if mapOf(raw)["uses"] == any("actions/setup-go@v7") {
			return mapOf(mapOf(raw)["with"])
		}
	}
	return nil
}

func TestWorkflowVersionPolicy(t *testing.T) {
	root := repoRoot(t)
	src := readAt(t, root, ".github/workflows/ci.yml")
	makefile := readAt(t, root, "Makefile")
	jobMap := func(wf map[string]any, name string) map[string]any {
		return mapOf(mapOf(wf["jobs"])[name])
	}
	matrixOf := func(wf map[string]any, name string) map[string]any {
		return mapOf(mapOf(jobMap(wf, name)["strategy"])["matrix"])
	}
	setStepEnv := func(job string, match func(step map[string]any) bool, env map[string]any) (func(wf map[string]any), *bool) {
		found := new(bool)
		return func(wf map[string]any) {
			for _, raw := range listOf(jobMap(wf, job)["steps"]) {
				if match(mapOf(raw)) {
					mapOf(raw)["env"] = env
					*found = true
					return
				}
			}
		}, found
	}
	named := func(name string) func(step map[string]any) bool {
		return func(step map[string]any) bool { return step["name"] == any(name) }
	}
	redisDir := func(step map[string]any) bool {
		return mapOf(step["with"])["working-directory"] == any("redis")
	}
	redisTestOff, redisTestFound := setStepEnv("test", named("test redis module"), map[string]any{"GOWORK": "off"})
	redisLintOff, redisLintFound := setStepEnv("lint", redisDir, map[string]any{"GOWORK": "off"})
	cases := []struct {
		name   string
		mutate func(wf map[string]any)
		want   string
		found  *bool
	}{
		{"real", nil, "", nil},
		{"no-redis-6.2", func(wf map[string]any) {
			matrixOf(wf, "integration")["redis"] = []any{"redis:7.4"}
		}, "redis:6.2", nil},
		{"no-redis-7.4", func(wf map[string]any) {
			matrixOf(wf, "integration")["redis"] = []any{"redis:6.2"}
		}, "redis:7.4", nil},
		{"fixed-image", func(wf map[string]any) {
			mapOf(mapOf(jobMap(wf, "integration")["services"])["redis"])["image"] = "redis:6.2"
		}, "matrix.redis", nil},
		{"no-floor", func(wf map[string]any) {
			matrixOf(wf, "test")["go"] = []any{"1.27.x", "stable"}
		}, "floor", nil},
		{"no-stable", func(wf map[string]any) {
			matrixOf(wf, "test")["go"] = []any{"1.27.0", "1.27.3"}
		}, "stable", nil},
		{"unpinned-job", func(wf map[string]any) {
			setupGoWith(jobMap(wf, "coverage"))["go-version"] = "1.27.x"
		}, "coverage", nil},
		{"inexact-pin", func(wf map[string]any) {
			mapOf(wf["env"])["GO_VERSION"] = "1.27.x"
		}, "GO_VERSION", nil},
		{"fail-fast", func(wf map[string]any) {
			delete(mapOf(jobMap(wf, "test")["strategy"]), "fail-fast")
		}, "fail-fast", nil},
		{"top-level-gowork", func(wf map[string]any) {
			mapOf(wf["env"])["GOWORK"] = "off"
		}, "top-level", nil},
		{"core-test-auto", func(wf map[string]any) {
			delete(mapOf(jobMap(wf, "test")["env"]), "GOWORK")
		}, "core command", nil},
		{"redis-test-off", redisTestOff, "redis command", redisTestFound},
		{"redis-lint-off", redisLintOff, "redis command", redisLintFound},
		{"release-verify-step", func(wf map[string]any) {
			job := jobMap(wf, "tidy")
			job["steps"] = append(listOf(job["steps"]), map[string]any{"run": "make release-verify VERSION=v0.1.0"})
		}, "release-verify", nil},
		{"no-actionlint-job", func(wf map[string]any) {
			delete(mapOf(wf["jobs"]), "actionlint")
		}, "no actionlint job", nil},
		{"actionlint-unpinned", func(wf map[string]any) {
			for _, raw := range listOf(jobMap(wf, "actionlint")["steps"]) {
				if mapOf(raw)["run"] == any("make workflow-lint") {
					mapOf(raw)["run"] = "GOWORK=off go run github.com/rhysd/actionlint/cmd/actionlint@latest"
				}
			}
		}, "ACTIONLINT_VERSION", nil},
		{"actionlint-go-unpinned", func(wf map[string]any) {
			setupGoWith(jobMap(wf, "actionlint"))["go-version"] = "1.27.x"
		}, "job actionlint", nil},
		{"no-passwd-check", func(wf map[string]any) {
			job := jobMap(wf, "docker")
			s := listOf(job["steps"])
			job["steps"] = s[:len(s)-1]
		}, "/etc/passwd", nil},
		{"wrong-uid", func(wf map[string]any) {
			s := listOf(jobMap(wf, "docker")["steps"])
			last := mapOf(s[len(s)-1])
			run, _ := last["run"].(string)
			last["run"] = strings.ReplaceAll(run, "65532:65532", "0:0")
		}, "/etc/passwd", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var wf map[string]any
			if err := yaml.Unmarshal(src, &wf); err != nil {
				t.Fatal(err)
			}
			if c.mutate != nil {
				c.mutate(wf)
			}
			if c.found != nil && !*c.found {
				t.Fatal("mutation target step not found")
			}
			problems := workflowProblems(wf, makefile, goFloor(t, root))
			if c.want == "" {
				if len(problems) != 0 {
					t.Errorf("unmutated workflow reports problems: %q", problems)
				}
				return
			}
			if !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, c.want) }) {
				t.Errorf("want a problem containing %q, got %q", c.want, problems)
			}
		})
	}
}

func TestVersionFacts(t *testing.T) {
	root := repoRoot(t)
	var wf map[string]any
	if err := yaml.Unmarshal(readAt(t, root, ".github/workflows/ci.yml"), &wf); err != nil {
		t.Fatal(err)
	}
	pin := fmt.Sprint(mapOf(wf["env"])["GO_VERSION"])
	for _, rel := range []string{"README.md", "CONTRIBUTING.md", "docs/operations.md"} {
		text := string(readAt(t, root, rel))
		for _, want := range []string{"Redis 6.2 or later", "Redis 6.2 and 7.4"} {
			if !strings.Contains(text, want) {
				t.Errorf("%s does not contain %q", rel, want)
			}
		}
	}
	for _, rel := range []string{"README.md", "CONTRIBUTING.md"} {
		if !strings.Contains(string(readAt(t, root, rel)), "Go 1.27 or later") {
			t.Errorf("%s does not contain %q", rel, "Go 1.27 or later")
		}
	}
	if want := "GOTOOLCHAIN=go" + pin; !strings.Contains(string(readAt(t, root, "CONTRIBUTING.md")), want) {
		t.Errorf("CONTRIBUTING.md does not contain %q", want)
	}
}

func TestWorkflowIntegrationResolution(t *testing.T) {
	stepsOf := func(run string) []any {
		wf := map[string]any{"jobs": map[string]any{"integration": map[string]any{"steps": []any{map[string]any{"run": run}}}}}
		return listOf(mapOf(mapOf(wf["jobs"])["integration"])["steps"])
	}
	makefile := func(recipe string) []byte {
		return []byte("integration:\n\t" + recipe + "\n")
	}
	const untagged = "cd redis && go test -race ./..."
	const tagged = "cd redis && go test -race -count=1 -tags integration ./..."
	env := map[string]string{"MICROGATEWAY_REDIS_ADDR": "localhost:6379", "MICROGATEWAY_HEAVY": "1"}
	if integrationRunOK(stepsOf("make integration"), makefile(untagged), env) {
		t.Errorf("untagged recipe %q passed the integration predicate", untagged)
	}
	if !integrationRunOK(stepsOf("make integration"), makefile(tagged), env) {
		t.Errorf("tagged recipe %q failed the integration predicate", tagged)
	}
	if integrationRunOK(stepsOf("make nosuch"), makefile(tagged), env) {
		t.Error("run naming a missing make target passed the integration predicate")
	}
	if _, err := ExpandMakeRun("make nosuch", makefile(tagged), env); err == nil {
		t.Error("ExpandMakeRun of a missing make target returned no error")
	}
	const variable = "cd redis && MICROGATEWAY_REDIS_ADDR=$(MICROGATEWAY_REDIS_ADDR) $(GO_REDIS) test -race -count=1 -tags integration ./..."
	withGo := []byte("GO_REDIS := GOWORK=$(CURDIR)/go.work go\nintegration:\n\t" + variable + "\n")
	withoutGo := []byte("integration:\n\t" + variable + "\n")
	if !integrationRunOK(stepsOf("make integration"), withGo, env) {
		t.Errorf("variable recipe %q failed the integration predicate", variable)
	}
	if integrationRunOK(stepsOf("make integration"), withGo, nil) {
		t.Error("recipe with an unresolved MICROGATEWAY_REDIS_ADDR passed the integration predicate")
	}
	if integrationRunOK(stepsOf("make integration"), withoutGo, env) {
		t.Error("recipe with an undefined GO_REDIS passed the integration predicate")
	}
}

func TestDockerfile(t *testing.T) {
	root := repoRoot(t)
	for _, p := range dockerfileProblems(readAt(t, root, "Dockerfile"), readAt(t, root, "Makefile")) {
		t.Error(p)
	}
}

// dockerfileProblems returns the problems of the Dockerfile: the final distroless stage with its numeric user and exposed ports, the cross-compiling build stage on the build platform, the build stage's module copies and go commands in their command modes, and the build of the Redis binary with the Makefile's linker flags and the probe port comment.
func dockerfileProblems(dockerfile, makefile []byte) []string {
	var problems []string
	lines := strings.Split(string(dockerfile), "\n")
	var froms []int
	for i, line := range lines {
		if instructionOf(line) == "FROM" {
			froms = append(froms, i)
		}
	}
	if len(froms) < 2 {
		return []string{fmt.Sprintf("Dockerfile has %d FROM instructions, want at least 2", len(froms))}
	}
	last := froms[len(froms)-1]
	if fields := strings.Fields(lines[last]); len(fields) != 2 || fields[1] != distrolessImage {
		problems = append(problems, fmt.Sprintf("last FROM is %q, want %s", lines[last], distrolessImage))
	}

	build := strings.Join(lines[:last], "\n")
	final := make([]string, 0, len(lines)-last)
	for _, line := range lines[last+1:] {
		final = append(final, strings.TrimSpace(line))
	}
	if !slices.Contains(final, "USER 65532:65532") {
		problems = append(problems, "final stage lacks USER 65532:65532")
	}
	for _, line := range final {
		if instructionOf(line) == "RUN" {
			problems = append(problems, fmt.Sprintf("final stage runs %q", line))
		}
	}
	if !slices.Contains(final, `ENTRYPOINT ["/microgateway"]`) {
		problems = append(problems, `final stage lacks ENTRYPOINT ["/microgateway"]`)
	}
	if !slices.Contains(final, "EXPOSE 8080 9902") {
		problems = append(problems, "final stage lacks EXPOSE 8080 9902")
	}
	for _, line := range lines {
		if instructionOf(line) != "EXPOSE" {
			continue
		}
		for _, field := range strings.Fields(line)[1:] {
			if strings.HasPrefix(field, "9901") {
				problems = append(problems, "Dockerfile exposes the management port 9901")
			}
		}
	}
	cmdOK := false
	for _, line := range final {
		if strings.HasPrefix(line, "CMD ") && strings.Contains(line, `"run", "-config", "/etc/microgateway/config.yaml"`) {
			cmdOK = true
		}
	}
	if !cmdOK {
		problems = append(problems, `final stage CMD lacks "run", "-config", "/etc/microgateway/config.yaml"`)
	}

	if !strings.Contains(lines[froms[0]], "--platform=$BUILDPLATFORM") {
		problems = append(problems, "build stage FROM lacks --platform=$BUILDPLATFORM")
	}
	for _, want := range []string{"CGO_ENABLED=0", "-trimpath", "-s -w", "./cmd/microgateway", "GOOS=${TARGETOS}", "GOARCH=${TARGETARCH}"} {
		if !strings.Contains(build, want) {
			problems = append(problems, fmt.Sprintf("build stage lacks %q", want))
		}
	}
	for _, name := range []string{"ARG TARGETOS", "ARG TARGETARCH"} {
		if !slices.ContainsFunc(lines[:last], func(line string) bool { return strings.TrimSpace(line) == name }) {
			problems = append(problems, "build stage lacks "+name)
		}
	}
	if !slices.ContainsFunc(lines[:last], func(line string) bool { return strings.TrimSpace(line) == "ARG VERSION=dev" }) {
		problems = append(problems, "build stage lacks ARG VERSION=dev")
	}
	probeComment, managementComment := false, false
	for _, line := range lines {
		text := strings.TrimSpace(line)
		if !strings.HasPrefix(text, "#") {
			continue
		}
		if strings.Contains(text, "probe port") && strings.Contains(text, "9902") {
			probeComment = true
		}
		if strings.Contains(text, "management port") && strings.Contains(text, "loopback") && strings.Contains(text, "not exposed") {
			managementComment = true
		}
	}
	if !probeComment {
		problems = append(problems, "Dockerfile has no comment naming the probe port 9902")
	}
	if !managementComment {
		problems = append(problems, "Dockerfile has no comment saying the management port binds to loopback and is not exposed")
	}
	if !workdirRE.MatchString(build) {
		problems = append(problems, "build stage does not enter the redis module")
	}

	ldflags := strings.ReplaceAll(makeVariable(makefile, "LDFLAGS"), "$(CLI)", makeVariable(makefile, "CLI"))
	if got, want := flagNames(build), flagNames(ldflags); !slices.Equal(got, want) {
		problems = append(problems, fmt.Sprintf("Dockerfile -X names %q, Makefile LDFLAGS names %q", got, want))
	}

	var copies [][]string
	var cmds []goCommand
	beforeRun := true
	base, cur := "", ""
	for _, line := range lines[:last] {
		switch instructionOf(line) {
		case "COPY":
			if beforeRun {
				copies = append(copies, copySources(line))
			}
		case "WORKDIR":
			if f := strings.Fields(line); len(f) > 1 {
				cur = f[1]
				if base == "" {
					base = cur
				}
			}
		case "RUN":
			beforeRun = false
			_, text, _ := strings.Cut(strings.TrimSpace(line), " ")
			rel := path.Clean(strings.Trim(strings.TrimPrefix(cur, base), "/"))
			cmds = append(cmds, goCommands(text, rel, nil)...)
		}
	}
	for _, group := range [][]string{{"go.work", "go.mod", "go.sum"}, {"redis/go.mod", "redis/go.sum"}} {
		if !slices.ContainsFunc(copies, func(srcs []string) bool { return containsAll(srcs, group) }) {
			problems = append(problems, fmt.Sprintf("build stage does not copy %s before its first RUN", strings.Join(group, ", ")))
		}
	}
	problems = append(problems, modeProblems(cmds, path.Join(base, "go.work"))...)
	if !slices.ContainsFunc(cmds, func(c goCommand) bool {
		return c.Dir == "redis" && strings.HasPrefix(c.Text, "go build") && strings.Contains(c.Text, "./cmd/microgateway")
	}) {
		problems = append(problems, "build stage does not build ./cmd/microgateway in the redis module")
	}
	return problems
}

// copySources returns the source operands of a COPY instruction line: its operands without flags and without the destination.
func copySources(line string) []string {
	f := strings.Fields(line)[1:]
	for len(f) > 0 && strings.HasPrefix(f[0], "--") {
		f = f[1:]
	}
	if len(f) < 2 {
		return nil
	}
	return f[:len(f)-1]
}

// containsAll reports whether every name of want is in have.
func containsAll(have, want []string) bool {
	for _, w := range want {
		if !slices.Contains(have, w) {
			return false
		}
	}
	return true
}

func TestDockerfileModes(t *testing.T) {
	root := repoRoot(t)
	dockerfile := string(readAt(t, root, "Dockerfile"))
	makefile := readAt(t, root, "Makefile")
	cases := []struct {
		name, old, with, want string
	}{
		{"real", "", "", ""},
		{"copy without go.work", "COPY go.work go.mod go.sum ./", "COPY go.mod go.sum ./", "go.work"},
		{"build with GOWORK off", "GOWORK=/src/go.work go build", "GOWORK=off go build", "redis command"},
		{"build without workspace", "GOWORK=/src/go.work go build", "go build", "redis command"},
		{"download with GOWORK off", "GOWORK=/src/go.work go mod download", "GOWORK=off go mod download", "redis command"},
		{"final stage as root", "USER 65532:65532", "USER root", "USER 65532:65532"},
		{"named user", "USER 65532:65532", "USER nonroot", "USER 65532:65532"},
		{"uid only", "USER 65532:65532", "USER 65532", "USER 65532:65532"},
		{"management port exposed", "EXPOSE 8080 9902", "EXPOSE 8080 9901 9902", "management port 9901"},
		{"no probe port", "EXPOSE 8080 9902", "EXPOSE 8080", "EXPOSE 8080 9902"},
		{"emulated build stage", "FROM --platform=$BUILDPLATFORM golang", "FROM golang", "BUILDPLATFORM"},
		{"no target arch", "GOARCH=${TARGETARCH} ", "", "GOARCH=${TARGETARCH}"},
		{"no target os arg", "ARG TARGETOS\n", "", "ARG TARGETOS"},
		{"no probe comment", "# The probe port is 9902", "# The health port is 9902", "probe port"},
		{"no management comment", "is not exposed", "is published", "management port binds"},
		{"version default", "ARG VERSION=dev", "ARG VERSION=1.0", "ARG VERSION=dev"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := dockerfile
			if c.old != "" {
				src = replaceOnce(t, dockerfile, c.old, c.with)
			}
			problems := dockerfileProblems([]byte(src), makefile)
			if c.want == "" {
				if len(problems) != 0 {
					t.Errorf("unmutated Dockerfile reports problems: %q", problems)
				}
				return
			}
			if !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, c.want) }) {
				t.Errorf("want a problem containing %q, got %q", c.want, problems)
			}
		})
	}
}

func TestContributingCommandModes(t *testing.T) {
	root := repoRoot(t)
	doc, err := Load(filepath.Join(root, "CONTRIBUTING.md"))
	if err != nil {
		t.Fatal(err)
	}
	body, _, _, ok := section(doc, "Command modes")
	if !ok {
		t.Fatal("CONTRIBUTING.md has no Command modes section")
	}
	for _, want := range []string{"GOWORK=off", "go.work", "go.work.sum", "redis/", "./...", "make tidy-check"} {
		if !strings.Contains(body, want) {
			t.Errorf("Command modes section lacks %q", want)
		}
	}
	for _, banned := range []string{"git tag", "git push", "release-prepare", "release-check", "release-verify", "Releasing"} {
		if strings.Contains(body, banned) {
			t.Errorf("Command modes section contains %q", banned)
		}
	}
	for _, rel := range []string{"README.md", "CONTRIBUTING.md"} {
		text := string(readAt(t, root, rel))
		for _, banned := range []string{"replace github.com/ok0/go-microgateway", "No `go.work` file is committed", "Run every Go command with `GOWORK=off`"} {
			if strings.Contains(text, banned) {
				t.Errorf("%s contains %q", rel, banned)
			}
		}
	}
}

func TestReleaseChecklist(t *testing.T) {
	root := repoRoot(t)
	doc, err := Load(filepath.Join(root, "CONTRIBUTING.md"))
	if err != nil {
		t.Fatal(err)
	}
	body, _, _, ok := section(doc, "Release verification")
	if !ok {
		t.Fatal("CONTRIBUTING.md has no Release verification section")
	}
	files := map[string][]byte{}
	hasTestOutside := false
	for _, name := range repoFiles(t) {
		files[name] = readAt(t, root, name)
		if strings.HasSuffix(name, "_test.go") && !strings.HasPrefix(name, "internal/doccheck/") {
			hasTestOutside = true
		}
	}
	if !hasTestOutside {
		t.Fatal("repository has no _test.go file outside internal/doccheck")
	}
	for _, problem := range ChecklistProblems(body, files, MakeTargets(readAt(t, root, "Makefile")), checklistRequired) {
		t.Error(problem)
	}
}
