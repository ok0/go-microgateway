package doccheck

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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

var scrubbedEnvNames = []string{"MICROGATEWAY_HEAVY", "MICROGATEWAY_REDIS_ADDR", "MICROGATEWAY_SOAK", "MAKEFLAGS", "MFLAGS", "GOFLAGS"}

var requiredMakeTargets = []string{"build", "test", "race", "vet", "cover", "lint", "tidy-check", "realnet", "bench", "fuzz", "soak", "perf", "integration", "docker", "hygiene", "check", "help"}

var checklistRequired = []string{
	"make perf", "make soak", "make fuzz", "make integration", "make realnet", "make docker", "make lint",
	"MICROGATEWAY_HEAVY", "MICROGATEWAY_SOAK", "MICROGATEWAY_LOAD", "MICROGATEWAY_REDIS_ADDR",
	"MICROGATEWAY_REDIS_CLUSTER", "MICROGATEWAY_REDIS_SENTINEL", "MICROGATEWAY_CACHE_FULL",
	"TestCoalesceScale", "TestCacheMixedLoad", "TestPurgeLoad", "TestPurgeMarkerCapDefault",
	"TestInstancesMixedLoad", "TestHostGaugesChurnLoad", "TestMixedMultiInstanceLoad",
	"TestIntegrationSuite", "TestIntegrationTwoInstances", "TestConnectTimeoutRealClockTCP",
	"TestCloseStopsIdleTimers", "-count=20", "Mermaid", "VPN", "v0.1.0", "Go 1.27",
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
	checks := []struct{ dir, want string }{
		{root, "github.com/ok0/go-microgateway"},
		{filepath.Join(root, "redis"), "github.com/ok0/go-microgateway/redis"},
	}
	for _, c := range checks {
		out, err := run(t, c.dir, []string{"GOWORK=off"}, "go", "list", "-m")
		if err != nil {
			t.Fatalf("go list -m in %s: %v\n%s", c.dir, err, out)
		}
		if got := strings.TrimSpace(string(out)); got != c.want {
			t.Errorf("go list -m in %s = %q, want %q", c.dir, got, c.want)
		}
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

func TestIgnoreFiles(t *testing.T) {
	root := repoRoot(t)
	gitignore := readLines(t, root, ".gitignore")
	dockerignore := readLines(t, root, ".dockerignore")
	for _, want := range []string{".docs/", plannedDir + "/", "go.work*", "bin/"} {
		if !slices.Contains(gitignore, want) {
			t.Errorf(".gitignore lacks %q", want)
		}
	}
	for _, want := range []string{".git", ".github", ".docs", "go.work*", "bin", "*.test", "*.out", "**/testdata/fuzz"} {
		if !slices.Contains(dockerignore, want) {
			t.Errorf(".dockerignore lacks %q", want)
		}
	}
	if !slices.Contains(dockerignore, plannedDir) && !slices.Contains(dockerignore, plannedDir+"/") {
		t.Errorf(".dockerignore lacks the planning directory")
	}
	for _, name := range repoFiles(t) {
		switch {
		case strings.HasPrefix(name, ".docs/"), strings.HasPrefix(name, plannedDir+"/"), strings.HasPrefix(name, "bin/"):
			t.Errorf("repository file under an ignored directory: %s", name)
		case strings.HasPrefix(path.Base(name), "go.work"):
			t.Errorf("repository go.work file: %s", name)
		}
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
	for _, target := range []string{"fuzz", "soak", "perf", "integration"} {
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

func TestWorkflow(t *testing.T) {
	root := repoRoot(t)
	var wf map[string]any
	if err := yaml.Unmarshal(readAt(t, root, ".github/workflows/ci.yml"), &wf); err != nil {
		t.Fatal(err)
	}
	if got := mapOf(wf["env"])["GOWORK"]; got != "off" {
		t.Errorf("top-level env GOWORK = %v, want off", got)
	}
	jobs := mapOf(wf["jobs"])
	for _, name := range []string{"lint", "test", "coverage", "tidy", "integration", "realnet", "docker"} {
		if _, ok := jobs[name]; !ok {
			t.Errorf("workflow has no %s job", name)
		}
	}
	var uses []string
	for _, name := range slices.Sorted(maps.Keys(jobs)) {
		job := mapOf(jobs[name])
		if v, ok := job["continue-on-error"]; ok && !isFalse(v) {
			t.Errorf("job %s sets continue-on-error to %v", name, v)
		}
		for _, raw := range listOf(job["steps"]) {
			step := mapOf(raw)
			if v, ok := step["continue-on-error"]; ok && !isFalse(v) {
				t.Errorf("a step of job %s sets continue-on-error to %v", name, v)
			}
			if u, ok := step["uses"].(string); ok {
				uses = append(uses, u)
			}
		}
	}

	matrix := mapOf(mapOf(mapOf(jobs["test"])["strategy"])["matrix"])
	if n := len(listOf(matrix["go"])); n != 2 {
		t.Errorf("test matrix has %d go versions, want 2", n)
	}
	for _, want := range []string{"ubuntu-latest", "macos-latest"} {
		if !slices.Contains(listOf(matrix["os"]), any(want)) {
			t.Errorf("test matrix os lacks %s", want)
		}
	}

	integration := mapOf(jobs["integration"])
	image, _ := mapOf(mapOf(integration["services"])["redis"])["image"].(string)
	if !strings.HasPrefix(image, "redis:6.2") {
		t.Errorf("integration redis service image = %q, want redis:6.2", image)
	}
	if !slices.Contains(jobEnv(integration, "MICROGATEWAY_HEAVY"), any("1")) {
		t.Error(`integration job does not set MICROGATEWAY_HEAVY to "1"`)
	}
	if !slices.ContainsFunc(jobEnv(integration, "MICROGATEWAY_REDIS_ADDR"), nonEmptyString) {
		t.Error("integration job does not set a non-empty MICROGATEWAY_REDIS_ADDR")
	}
	makefile := readAt(t, root, "Makefile")
	var expanded []string
	for _, raw := range listOf(integration["steps"]) {
		run, _ := mapOf(raw)["run"].(string)
		if text, err := ExpandMakeRun(run, makefile, stepEnv(wf, integration, mapOf(raw))); err == nil {
			expanded = append(expanded, text)
		}
	}
	if !integrationRunOK(listOf(integration["steps"]), makefile, stepEnv(wf, integration, nil)) {
		t.Errorf("integration job runs no race-enabled integration-tagged go test in the redis module; expanded runs: %q", expanded)
	}
	for _, name := range slices.Sorted(maps.Keys(jobs)) {
		for i, raw := range listOf(mapOf(jobs[name])["steps"]) {
			run, _ := mapOf(raw)["run"].(string)
			if _, err := ExpandMakeRun(run, makefile, stepEnv(wf, mapOf(jobs[name]), mapOf(raw))); err != nil {
				t.Errorf("job %s step %d: %v", name, i, err)
			}
		}
	}

	allowed := []string{"actions/checkout@v7", "actions/setup-go@v7", "golangci/golangci-lint-action@v9"}
	seen := map[string]bool{}
	for _, u := range uses {
		if !slices.Contains(allowed, u) {
			t.Errorf("workflow uses %q, which is not an allowed action", u)
		}
		seen[u] = true
	}
	for _, a := range allowed {
		if !seen[a] {
			t.Errorf("no step uses %s", a)
		}
	}

	lintVersion := makeVariable(readAt(t, root, "Makefile"), "GOLANGCI_LINT_VERSION")
	lintPinned := false
	for _, raw := range listOf(mapOf(jobs["lint"])["steps"]) {
		step := mapOf(raw)
		if step["uses"] == any("golangci/golangci-lint-action@v9") && mapOf(step["with"])["version"] == any(lintVersion) {
			lintPinned = true
		}
	}
	if !lintPinned {
		t.Errorf("lint job golangci action version is not the Makefile GOLANGCI_LINT_VERSION %q", lintVersion)
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
	const variable = "cd redis && MICROGATEWAY_REDIS_ADDR=$(MICROGATEWAY_REDIS_ADDR) $(GO) test -race -count=1 -tags integration ./..."
	withGo := []byte("GO := GOWORK=off go\nintegration:\n\t" + variable + "\n")
	withoutGo := []byte("integration:\n\t" + variable + "\n")
	if !integrationRunOK(stepsOf("make integration"), withGo, env) {
		t.Errorf("variable recipe %q failed the integration predicate", variable)
	}
	if integrationRunOK(stepsOf("make integration"), withGo, nil) {
		t.Error("recipe with an unresolved MICROGATEWAY_REDIS_ADDR passed the integration predicate")
	}
	if integrationRunOK(stepsOf("make integration"), withoutGo, env) {
		t.Error("recipe with an undefined GO passed the integration predicate")
	}
}

func TestDockerfile(t *testing.T) {
	root := repoRoot(t)
	lines := strings.Split(string(readAt(t, root, "Dockerfile")), "\n")
	var froms []int
	for i, line := range lines {
		if instructionOf(line) == "FROM" {
			froms = append(froms, i)
		}
	}
	if len(froms) < 2 {
		t.Fatalf("Dockerfile has %d FROM instructions, want at least 2", len(froms))
	}
	last := froms[len(froms)-1]
	if fields := strings.Fields(lines[last]); len(fields) != 2 || fields[1] != distrolessImage {
		t.Errorf("last FROM is %q, want %s", lines[last], distrolessImage)
	}

	build := strings.Join(lines[:last], "\n")
	final := make([]string, 0, len(lines)-last)
	for _, line := range lines[last+1:] {
		final = append(final, strings.TrimSpace(line))
	}
	if !slices.Contains(final, "USER nonroot") {
		t.Error("final stage lacks USER nonroot")
	}
	for _, line := range final {
		if instructionOf(line) == "RUN" {
			t.Errorf("final stage runs %q", line)
		}
	}
	if !slices.Contains(final, `ENTRYPOINT ["/microgateway"]`) {
		t.Error(`final stage lacks ENTRYPOINT ["/microgateway"]`)
	}
	if !slices.Contains(final, "EXPOSE 8080 9901") {
		t.Error("final stage lacks EXPOSE 8080 9901")
	}
	cmdOK := false
	for _, line := range final {
		if strings.HasPrefix(line, "CMD ") && strings.Contains(line, `"run", "-config", "/etc/microgateway/config.yaml"`) {
			cmdOK = true
		}
	}
	if !cmdOK {
		t.Error(`final stage CMD lacks "run", "-config", "/etc/microgateway/config.yaml"`)
	}

	for _, want := range []string{"CGO_ENABLED=0", "GOWORK=off", "-trimpath", "-s -w", "./cmd/microgateway"} {
		if !strings.Contains(build, want) {
			t.Errorf("build stage lacks %q", want)
		}
	}
	if !workdirRE.MatchString(build) {
		t.Error("build stage does not enter the redis module")
	}

	mk := readAt(t, root, "Makefile")
	ldflags := strings.ReplaceAll(makeVariable(mk, "LDFLAGS"), "$(CLI)", makeVariable(mk, "CLI"))
	if got, want := flagNames(build), flagNames(ldflags); !slices.Equal(got, want) {
		t.Errorf("Dockerfile -X names %q, Makefile LDFLAGS names %q", got, want)
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
