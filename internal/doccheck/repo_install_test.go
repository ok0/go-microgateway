package doccheck

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const cliPath = corePath + "/internal/cli"

// requireTool skips the test when the named tool is not on PATH.
func requireTool(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Skip(name + " not on PATH: " + t.Name() + " not run")
	}
}

// exitStatus returns the exit status of a command that ran and fails the test when the command could not run at all.
func exitStatus(t *testing.T, err error) int {
	t.Helper()
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	t.Fatal(err)
	return -1
}

// cliMainDirs returns the sorted, distinct slash directories of the main packages among files that call the CLI entry: a non-test Go file with package main that imports the internal CLI package and calls its Main function in a function body. Files are read below root; a file that does not parse is an error.
func cliMainDirs(root string, files []string) ([]string, error) {
	var dirs []string
	for _, name := range files {
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
		if err != nil {
			return nil, err
		}
		f, err := parser.ParseFile(token.NewFileSet(), name, src, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		if f.Name.Name == "main" && callsCLIMain(f) {
			dirs = append(dirs, path.Dir(name))
		}
	}
	slices.Sort(dirs)
	return slices.Compact(dirs), nil
}

// callsCLIMain reports whether a function body of f calls Main of the internal CLI package, through its import name or through a dot import.
func callsCLIMain(f *ast.File) bool {
	local := ""
	for _, imp := range f.Imports {
		if p, err := strconv.Unquote(imp.Path.Value); err != nil || p != cliPath {
			continue
		}
		local = "cli"
		if imp.Name != nil {
			local = imp.Name.Name
		}
	}
	if local == "" || local == "_" {
		return false
	}
	found := false
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fun := call.Fun.(type) {
			case *ast.SelectorExpr:
				if x, ok := fun.X.(*ast.Ident); ok && x.Name == local && fun.Sel.Name == "Main" {
					found = true
				}
			case *ast.Ident:
				if local == "." && fun.Name == "Main" {
					found = true
				}
			}
			return !found
		})
	}
	return found
}

// gitTags returns the tag names of the repository at dir. The test is skipped when git is not installed.
func gitTags(t *testing.T, dir string) []string {
	t.Helper()
	requireTool(t, "git")
	out, err := exec.Command("git", "-C", dir, "for-each-ref", "--format=%(refname)", "refs/tags").Output()
	if err != nil {
		t.Fatalf("git for-each-ref in %s: %v", dir, err)
	}
	var tags []string
	for _, ref := range outputLines(out) {
		tags = append(tags, strings.TrimPrefix(ref, "refs/tags/"))
	}
	return tags
}

// redisRequireReader returns a function that reads the core version required by the redis/go.mod at a revision of the repository at dir. A revision without that file, or whose file does not require the core module, is an error.
func redisRequireReader(dir string) func(string) (string, error) {
	return func(tag string) (string, error) {
		out, err := exec.Command("git", "-C", dir, "show", tag+":redis/go.mod").Output()
		if err != nil {
			return "", fmt.Errorf("git show %s:redis/go.mod: %w", tag, err)
		}
		m, err := ParseGoMod(out)
		if err != nil {
			return "", err
		}
		got := m.Requires[corePath]
		if got == "" {
			return "", errors.New("does not require github.com/ok0/go-microgateway")
		}
		return got, nil
	}
}

func TestReleaseCheck(t *testing.T) {
	requireFullHistory(t)
	root := repoRoot(t)
	if _, status := git(t, "rev-parse", "--is-inside-work-tree"); status != 0 {
		t.Fatalf("git rev-parse: exit status %d", status)
	}
	core, err := ParseGoMod(readAt(t, root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	redis, err := ParseGoMod(readAt(t, root, "redis/go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	envVersion := os.Getenv("MICROGATEWAY_RELEASE_VERSION")
	version := envVersion
	if version == "" {
		version = redis.Requires[corePath]
	}
	requirePending := envVersion != ""
	tags := gitTags(t, root)
	t.Logf("%d tags read: %s", len(tags), strings.Join(tags, " "))
	mainDirs, err := cliMainDirs(root, repoFiles(t))
	if err != nil {
		t.Fatal(err)
	}
	changelog, err := Load(filepath.Join(root, "CHANGELOG.md"))
	if err != nil {
		t.Fatal(err)
	}
	releases, changelogFindings := ChangelogFindings(changelog)
	findings := slices.Concat(
		ReleaseFindings(version, readAt(t, root, "go.work"), core, redis),
		TagFindings(tags, redisRequireReader(root)),
		InstallFindings(readAt(t, root, "README.md"), mainDirs),
		RequireTagFindings(redis.Requires[corePath], tags),
		changelogFindings,
		ChangelogTagFindings(releases, tags, version, requirePending),
	)
	for _, f := range findings {
		t.Errorf("%s:%d: %s (%s)", f.Path, f.Line, f.Text, f.Pattern)
	}
	if after := gitTags(t, root); !slices.Equal(tags, after) {
		t.Errorf("tags changed during the release check: before %q, after %q", tags, after)
	}
}

func TestCLIMainDirs(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"cmd/a/main.go":      "package main\n\nimport (\n\t\"context\"\n\t\"os\"\n\n\t\"github.com/ok0/go-microgateway/internal/cli\"\n)\n\nfunc main() { os.Exit(run()) }\n\nfunc run() int { return cli.Main(context.Background(), cli.Env{}) }\n",
		"cmd/b/main.go":      "package main\n\nimport (\n\t\"context\"\n\t\"os\"\n\n\tc \"github.com/ok0/go-microgateway/internal/cli\"\n)\n\nfunc main() { os.Exit(c.Main(context.Background(), c.Env{})) }\n",
		"cmd/c/main.go":      "package main\n\nfunc main() {}\n",
		"lib/x.go":           "package lib\n\nimport \"github.com/ok0/go-microgateway/internal/cli\"\n\nfunc Run() int { return cli.Main(nil, cli.Env{}) }\n",
		"cmd/d/main_test.go": "package main\n\nimport \"github.com/ok0/go-microgateway/internal/cli\"\n\nfunc check() { cli.Main(nil, cli.Env{}) }\n",
	})
	names := []string{"cmd/a/main.go", "cmd/b/main.go", "cmd/c/main.go", "lib/x.go", "cmd/d/main_test.go"}
	got, err := cliMainDirs(root, names)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"cmd/a", "cmd/b"}; !slices.Equal(got, want) {
		t.Errorf("cliMainDirs = %q, want %q", got, want)
	}
	writeTree(t, root, map[string]string{"cmd/e/main.go": "package main\n\nfunc main( {\n"})
	if _, err := cliMainDirs(root, append(names, "cmd/e/main.go")); err == nil {
		t.Error("cliMainDirs of a file with a syntax error returned no error")
	}
}

func TestReadmeInstall(t *testing.T) {
	root := repoRoot(t)
	dirs, err := cliMainDirs(root, repoFiles(t))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"redis/cmd/microgateway"}; !slices.Equal(dirs, want) {
		t.Errorf("main packages that call the CLI entry = %q, want %q", dirs, want)
	}
	readme, err := Load(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	install, _, _, ok := section(readme, "Install")
	if !ok {
		t.Fatal("README has no Install heading")
	}
	if !slices.Contains(Normalize(install), "go install github.com/ok0/go-microgateway/redis/cmd/microgateway@latest") {
		t.Error("Install section lacks the go install line for the Redis binary at latest")
	}
	for _, want := range []string{"includes the Redis backends", "microgateway version", "CONTRIBUTING.md#releasing", "go get github.com/ok0/go-microgateway", "go get github.com/ok0/go-microgateway/redis"} {
		if !strings.Contains(install, want) {
			t.Errorf("Install section lacks %q", want)
		}
	}
	libraries, _, _, ok := section(readme, "Libraries")
	if !ok {
		t.Fatal("README has no Libraries heading")
	}
	for _, banned := range []string{"replace", "checkout"} {
		if strings.Contains(strings.ToLower(libraries), banned) {
			t.Errorf("Libraries section contains %q", banned)
		}
	}
}

func TestContributingReleasing(t *testing.T) {
	root := repoRoot(t)
	doc, err := Load(filepath.Join(root, "CONTRIBUTING.md"))
	if err != nil {
		t.Fatal(err)
	}
	body, _, _, ok := section(doc, "Releasing")
	if !ok {
		t.Fatal("CONTRIBUTING.md has no Releasing section")
	}
	numbered := regexp.MustCompile(`^[0-9]+\. `)
	var items []string
	for _, line := range strings.Split(body, "\n") {
		if numbered.MatchString(line) {
			items = append(items, line)
		}
	}
	if len(items) != 6 {
		t.Fatalf("Releasing has %d numbered steps, want 6", len(items))
	}
	wants := [][]string{
		{"make release-prepare VERSION=vX.Y.Z"},
		{"make check", "make release-check VERSION=vX.Y.Z", "Unreleased", "commit"},
		{"git tag vX.Y.Z", "git push"},
		{"GOWORK=off go mod tidy", "redis/go.sum", "commit"},
		{"git tag redis/vX.Y.Z", "git push"},
		{"MICROGATEWAY_HEAVY=1", "make release-verify VERSION=vX.Y.Z"},
	}
	for i, item := range items {
		step := i + 1
		_, text, _ := strings.Cut(item, ". ")
		for _, want := range wants[i] {
			if !strings.Contains(item, want) {
				t.Errorf("step %d lacks %q", step, want)
			}
		}
		if step <= 2 && strings.Contains(item, "User step") {
			t.Errorf("step %d is marked as a user step", step)
		}
		if step >= 3 && !strings.HasPrefix(text, "User step") {
			t.Errorf("step %d does not start with User step", step)
		}
	}
	for _, want := range []string{"one version number", "`redis/vX.Y.Z`", "`v0.1.0`", "No `replace` directive is ever committed", "Automated agents never tag, push or add remotes", "tagged first"} {
		if !strings.Contains(body, want) {
			t.Errorf("Releasing section lacks %q", want)
		}
	}

	files := map[string][]byte{}
	for _, name := range repoFiles(t) {
		files[name] = readAt(t, root, name)
	}
	for _, problem := range ChecklistProblems(body, files, MakeTargets(readAt(t, root, "Makefile")), []string{"make release-prepare", "make release-check", "make release-verify"}) {
		t.Error(problem)
	}

	verification, _, _, ok := section(doc, "Release verification")
	if !ok {
		t.Fatal("CONTRIBUTING.md has no Release verification section")
	}
	row := ""
	for _, line := range strings.Split(verification, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "| 12. Release |") {
			row = line
		}
	}
	if !strings.Contains(row, "[Releasing](#releasing)") {
		t.Errorf("row 12 of Release verification lacks [Releasing](#releasing): %q", row)
	}
	if strings.Contains(row, "local tags") {
		t.Errorf("row 12 of Release verification contains %q: %q", "local tags", row)
	}
	for _, want := range []string{"`vX.Y.Z`", "`redis/vX.Y.Z`"} {
		if !strings.Contains(row, want) {
			t.Errorf("row 12 of Release verification lacks %s: %q", want, row)
		}
	}
}

func TestChangelog(t *testing.T) {
	root := repoRoot(t)
	doc, err := Load(filepath.Join(root, "CHANGELOG.md"))
	if err != nil {
		t.Fatal(err)
	}
	releases, findings := ChangelogFindings(doc)
	for _, f := range findings {
		t.Errorf("%s:%d: %s (%s)", f.Path, f.Line, f.Text, f.Pattern)
	}
	if len(releases) == 0 {
		t.Fatal("CHANGELOG.md has no released section")
	}
	const history = "[v0.1.0] - 2026-11-06"
	body, _, _, ok := section(doc, history)
	if !ok {
		t.Fatalf("CHANGELOG.md has no section %q", history)
	}
	var subs []string
	for i, h := range doc.Headings {
		if h.Text != history {
			continue
		}
		for _, next := range doc.Headings[i+1:] {
			if next.Level <= h.Level {
				break
			}
			if next.Level == 3 {
				subs = append(subs, next.Text)
			}
		}
		break
	}
	if want := []string{"Added", "Changed", "Security"}; !slices.Equal(subs, want) {
		t.Errorf("%s subheadings = %q, want %q", history, subs, want)
	}
	for _, want := range []string{"`Delete`", "store_errors", "304", "stored=false", "coalesced", "JWKS", "`kid`", "go.work", "replace", "redis/vX.Y.Z", "microgateway version", "release-prepare", "release-check", "release-verify", "Request paths are canonicalized"} {
		if !strings.Contains(body, want) {
			t.Errorf("section %s lacks %q", history, want)
		}
	}
	src := readAt(t, root, "CHANGELOG.md")
	for _, f := range Hygiene("CHANGELOG.md", src) {
		t.Errorf("CHANGELOG.md:%d: %s", f.Line, strings.TrimSpace(f.Text))
	}
}

// releaseTargetProblems returns the problems of the release targets of a makefile: each release target exists and is listed in .PHONY, release-verify is gated by require-heavy, no target depends on a release-prepare or release-verify, check reaches neither require-heavy nor a release target, release-check is not gated by require-heavy, and the release-check, release-prepare and release-verify recipes run the check, the prepare script and the verify script.
func releaseTargetProblems(makefile []byte) []string {
	var problems []string
	targets := MakeTargets(makefile)
	recipes := MakeRecipes(makefile)
	for _, name := range []string{"release-prepare", "release-check", "release-verify"} {
		if _, ok := targets[name]; !ok {
			problems = append(problems, "Makefile has no "+name+" target")
		} else if !slices.Contains(targets[".PHONY"], name) {
			problems = append(problems, name+" is not listed in .PHONY")
		}
	}
	if pre := targets["release-verify"]; len(pre) == 0 || pre[0] != "require-heavy" {
		problems = append(problems, "release-verify is not gated by require-heavy")
	}
	if slices.Contains(targets["release-check"], "require-heavy") {
		problems = append(problems, "release-check depends on require-heavy")
	}
	for _, name := range slices.Sorted(maps.Keys(targets)) {
		if name == ".PHONY" {
			continue
		}
		for _, dep := range targets[name] {
			if dep == "release-prepare" || dep == "release-verify" {
				problems = append(problems, "target "+name+" depends on "+dep)
			}
		}
	}
	reached := map[string]bool{}
	var visit func(name string)
	visit = func(name string) {
		for _, dep := range targets[name] {
			if !reached[dep] {
				reached[dep] = true
				visit(dep)
			}
		}
	}
	visit("check")
	for _, dep := range slices.Sorted(maps.Keys(reached)) {
		if dep == "require-heavy" || strings.HasPrefix(dep, "release-") {
			problems = append(problems, "check reaches "+dep)
		}
	}
	if !strings.Contains(strings.Join(recipes["release-check"], "\n"), "TestReleaseCheck") {
		problems = append(problems, "release-check recipe lacks TestReleaseCheck")
	}
	if !strings.Contains(strings.Join(recipes["release-check"], "\n"), "GOPROXY=off") {
		problems = append(problems, "release-check recipe lacks GOPROXY=off")
	}
	if !strings.Contains(strings.Join(recipes["release-prepare"], "\n"), "scripts/release-prepare.sh") {
		problems = append(problems, "release-prepare recipe lacks scripts/release-prepare.sh")
	}
	if !strings.Contains(strings.Join(recipes["release-verify"], "\n"), "scripts/release-verify.sh") {
		problems = append(problems, "release-verify recipe lacks scripts/release-verify.sh")
	}
	return problems
}

func TestReleaseTargets(t *testing.T) {
	root := repoRoot(t)
	makefile := readAt(t, root, "Makefile")
	if problems := releaseTargetProblems(makefile); len(problems) != 0 {
		t.Errorf("Makefile release targets: %q", problems)
	}
	cases := []struct {
		name, old, with, want string
	}{
		{"verify not gated", "release-verify: require-heavy ##", "release-verify: ##", "require-heavy"},
		{"hygiene depends on prepare", "hygiene: ##", "hygiene: release-prepare ##", "release-prepare"},
		{"hygiene depends on verify", "hygiene: ##", "hygiene: release-verify ##", "release-verify"},
		{"check target missing", "release-check:", "release-checks:", "no release-check target"},
		{"check gated", "release-check: ##", "release-check: require-heavy ##", "release-check"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			problems := releaseTargetProblems([]byte(replaceOnce(t, string(makefile), c.old, c.with)))
			if !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, c.want) }) {
				t.Errorf("want a problem containing %q, got %q", c.want, problems)
			}
		})
	}
}

func TestReleaseVersionRule(t *testing.T) {
	requireTool(t, "sh")
	root := repoRoot(t)
	lib := filepath.Join(root, "scripts", "release-lib.sh")
	for _, c := range releaseVersionCases {
		t.Run(c.v, func(t *testing.T) {
			out, err := run(t, root, nil, "sh", "-c", `. "$1"; check_version "$2"`, "_", lib, c.v)
			if status := exitStatus(t, err); (status == 0) != c.ok {
				t.Errorf("check_version %q exit status %d, want accepted %t\n%s", c.v, status, c.ok, out)
			}
		})
	}
}

func TestInstallCommandScript(t *testing.T) {
	requireTool(t, "sh")
	root := repoRoot(t)
	lib := filepath.Join(root, "scripts", "release-lib.sh")
	const script = `. "$1"; install_command "$2" "$3"`
	out, err := run(t, root, nil, "sh", "-c", script, "_", lib, filepath.Join(root, "README.md"), "v0.1.0")
	if err != nil {
		t.Fatalf("install_command: %v\n%s", err, out)
	}
	if want := "go install github.com/ok0/go-microgateway/redis/cmd/microgateway@v0.1.0\n"; string(out) != want {
		t.Errorf("install_command printed %q, want %q", out, want)
	}
	const line = "go install github.com/ok0/go-microgateway/redis/cmd/microgateway@latest"
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{
		"two.md":  installReadme(line, line),
		"none.md": installReadme("echo nothing"),
	})
	for _, name := range []string{"two.md", "none.md"} {
		out, err := run(t, root, nil, "sh", "-c", script, "_", lib, filepath.Join(dir, name), "v0.1.0")
		if exitStatus(t, err) == 0 {
			t.Errorf("install_command accepted %s: %s", name, out)
		}
	}
}

func TestReleaseVerifyRefusals(t *testing.T) {
	requireTool(t, "sh")
	root := repoRoot(t)
	out, err := run(t, root, nil, "sh", "scripts/release-verify.sh", "v0.1.0")
	if exitStatus(t, err) == 0 {
		t.Error("release-verify ran without MICROGATEWAY_HEAVY=1")
	}
	if !bytes.Contains(out, []byte("MICROGATEWAY_HEAVY=1")) {
		t.Errorf("release-verify output does not name MICROGATEWAY_HEAVY=1:\n%s", out)
	}
	for _, version := range []string{"0.1.0", ""} {
		out, err := run(t, root, []string{"MICROGATEWAY_HEAVY=1"}, "sh", "scripts/release-verify.sh", version)
		if exitStatus(t, err) == 0 {
			t.Errorf("release-verify accepted version %q", version)
		}
		if !bytes.Contains(out, []byte("VERSION")) {
			t.Errorf("release-verify output for version %q does not name VERSION:\n%s", version, out)
		}
	}
}

func TestReleaseVerifyScript(t *testing.T) {
	root := repoRoot(t)
	src := string(readAt(t, root, "scripts/release-verify.sh"))
	for _, want := range []string{`"${MICROGATEWAY_HEAVY:-}" = 1`, "MICROGATEWAY_HEAVY", "-u GOFLAGS", "GOWORK=off", `GOPATH="$tmp/gopath"`, `GOMODCACHE="$tmp/modcache"`, "mktemp -d", "trap cleanup EXIT", `rm -rf "$tmp"`, `cd "$tmp/work"`, `install_command "$root/README.md"`, "version)", "validate -config", "run -config", "examples/redis.yaml"} {
		if !strings.Contains(src, want) {
			t.Errorf("release-verify.sh lacks %q", want)
		}
	}
	for _, banned := range []string{"git tag", "git push", "git remote"} {
		if strings.Contains(src, banned) {
			t.Errorf("release-verify.sh contains %q", banned)
		}
	}
	tmp := strings.Index(src, "mktemp -d")
	for _, gate := range []string{`"${MICROGATEWAY_HEAVY:-}" = 1`, "check_version"} {
		if i := strings.Index(src, gate); i < 0 || i > tmp {
			t.Errorf("%s is not checked before mktemp -d", gate)
		}
	}
}

// nextPatch returns the release after v: the patch number plus one, without any prerelease (vX.Y.Z[-p] gives vX.Y.(Z+1)).
func nextPatch(t *testing.T, v string) string {
	t.Helper()
	numbers, _, _ := strings.Cut(strings.TrimPrefix(v, "v"), "-")
	parts := strings.Split(numbers, ".")
	if len(parts) != 3 {
		t.Fatalf("nextPatch(%q): want three numbers", v)
	}
	z, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil || z == math.MaxUint64 {
		t.Fatalf("nextPatch(%q): patch number is not a number below the maximum", v)
	}
	return "v" + parts[0] + "." + parts[1] + "." + strconv.FormatUint(z+1, 10)
}

func TestNextPatch(t *testing.T) {
	cases := []struct{ v, want string }{
		{"v0.1.0", "v0.1.1"},
		{"v0.1.9", "v0.1.10"},
		{"v1.2.3-rc.1", "v1.2.4"},
		{"v0.0.0", "v0.0.1"},
	}
	for _, c := range cases {
		if got := nextPatch(t, c.v); got != c.want {
			t.Errorf("nextPatch(%q) = %q, want %q", c.v, got, c.want)
		}
	}
}

func TestReleasePrepare(t *testing.T) {
	requireMake(t)
	requireTool(t, "go")
	root := repoRoot(t)
	copied := []string{"Makefile", "go.work", "go.mod", "go.sum", "redis/go.mod", "redis/go.sum", "README.md"}
	entries, err := os.ReadDir(filepath.Join(root, "scripts"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			copied = append(copied, "scripts/"+e.Name())
		}
	}
	orig := map[string]string{}
	for _, name := range copied {
		orig[name] = string(readAt(t, root, name))
	}
	realRedis, err := ParseGoMod([]byte(orig["redis/go.mod"]))
	if err != nil {
		t.Fatal(err)
	}
	r := realRedis.Requires[corePath]
	if !releaseVersion(r) {
		t.Fatalf("redis/go.mod requires %s %q, not a release version", corePath, r)
	}
	n := nextPatch(t, r)
	if compareRelease(n, r) <= 0 {
		t.Fatalf("next version %s is not above %s", n, r)
	}
	t.Logf("preparing %s from %s", n, r)
	dir := t.TempDir()
	writeTree(t, dir, orig)

	wantRedis := replaceOnce(t, orig["redis/go.mod"], corePath+" "+r, corePath+" "+n)
	wantWork := replaceOnce(t, orig["go.work"], corePath+" "+r+" => ./", corePath+" "+n+" => ./")
	expected := maps.Clone(orig)
	expected["redis/go.mod"] = wantRedis
	expected["go.work"] = wantWork

	checkFiles := func(t *testing.T, want map[string]string) {
		t.Helper()
		for name, body := range want {
			if got := string(readAt(t, dir, name)); got != body {
				t.Errorf("%s changed: got %q, want %q", name, got, body)
			}
		}
	}

	out, err := run(t, dir, nil, "make", "-C", dir, "release-prepare", "VERSION="+n)
	if err != nil {
		t.Fatalf("make release-prepare VERSION=%s: %v\n%s", n, err, out)
	}
	checkFiles(t, expected)
	core, err := ParseGoMod(readAt(t, dir, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	redis, err := ParseGoMod(readAt(t, dir, "redis/go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if found := ReleaseFindings(n, readAt(t, dir, "go.work"), core, redis); len(found) != 0 {
		t.Errorf("ReleaseFindings after release-prepare: %+v", found)
	}
	if found := RequireTagFindings(n, []string{r, "redis/" + r}); len(found) != 0 {
		t.Errorf("RequireTagFindings(%s) after release-prepare: %+v", n, found)
	}
	stale := findingPatterns(ReleaseFindings(r, readAt(t, dir, "go.work"), core, redis))
	for _, p := range []string{"release-require", "release-replace"} {
		if !slices.Contains(stale, p) {
			t.Errorf("ReleaseFindings(%s) after release-prepare lacks %s: %q", r, p, stale)
		}
	}

	out, err = run(t, dir, nil, "make", "-C", dir, "release-prepare", "VERSION="+n)
	if err != nil {
		t.Fatalf("second make release-prepare VERSION=%s: %v\n%s", n, err, out)
	}
	checkFiles(t, expected)

	for _, version := range []string{"", "0.2.0", "v1", "v0.2.0-0.20260101000000-abcdefabcdef"} {
		out, err := run(t, dir, nil, "make", "-C", dir, "release-prepare", "VERSION="+version)
		if exitStatus(t, err) == 0 {
			t.Errorf("release-prepare accepted version %q", version)
		}
		if !bytes.Contains(out, []byte("VERSION")) {
			t.Errorf("release-prepare output for version %q does not name VERSION:\n%s", version, out)
		}
		checkFiles(t, expected)
	}

	for _, name := range []string{"go.work", "redis/go.mod"} {
		if got := string(readAt(t, root, name)); got != orig[name] {
			t.Errorf("real %s changed", name)
		}
	}
}

func TestTagFindingsGitRepo(t *testing.T) {
	requireTool(t, "git")
	dir := t.TempDir()
	env := []string{"GIT_CONFIG_GLOBAL=" + os.DevNull, "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@example.com", "GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@example.com"}
	gitIn := func(args ...string) {
		t.Helper()
		if out, err := run(t, dir, env, "git", append([]string{"-C", dir}, args...)...); err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	gitIn("init", "-q")
	writeTree(t, dir, map[string]string{"redis/go.mod": redisModFixture("v0.1.0")})
	gitIn("add", ".")
	gitIn("-c", "commit.gpgsign=false", "commit", "-q", "-m", "first")
	gitIn("tag", "v0.1.0")
	gitIn("tag", "redis/v0.1.0")
	if found := TagFindings(gitTags(t, dir), redisRequireReader(dir)); len(found) != 0 {
		t.Errorf("paired tags have findings: %+v", found)
	}
	gitIn("tag", "v0.2.0")
	gitIn("tag", "redis/v0.2.0")
	found := TagFindings(gitTags(t, dir), redisRequireReader(dir))
	if got := findingPatterns(found); !slices.Equal(got, []string{"tag-require"}) {
		t.Errorf("patterns = %q, want [tag-require] (findings %+v)", got, found)
	}
	for _, f := range found {
		if f.Path != "redis/v0.2.0" {
			t.Errorf("finding %+v has path %q, want redis/v0.2.0", f, f.Path)
		}
	}
}
