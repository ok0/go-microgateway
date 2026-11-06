package doccheck

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// goCommand is one go or golangci-lint invocation found in shell text: the directory it runs in relative to the repository root, the GOWORK value it runs with and whether GOWORK is set at all, and the command from the program name on.
type goCommand struct {
	Dir    string
	GOWORK string
	Set    bool
	Text   string
}

var (
	goSingleQuotedRE = regexp.MustCompile(`'[^']*'`)
	goDoubleQuotedRE = regexp.MustCompile(`"[^"]*"`)
	shellMakeRefRE   = regexp.MustCompile(`\$\(([A-Za-z0-9_.-]+)\)`)
	shellAssignRE    = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*=`)
	quotedSpaces     = strings.NewReplacer("&", " ", "|", " ", ";", " ", "(", " ", ")", " ")
	shellSeparators  = strings.NewReplacer("&&", "\n", "||", "\n", "$(", "\n", ";", "\n", "|", "\n", "(", "\n", ")", "\n")
)

// normalizeRefs rewrites make references $(NAME) to ${NAME}, so that their parentheses are not read as shell syntax.
func normalizeRefs(s string) string {
	return shellMakeRefRE.ReplaceAllString(s, "$${$1}")
}

// goCommands returns the go and golangci-lint invocations of text. Every line starts in dir with env's GOWORK, if any, as the default. Quoted spans are blanked or unquoted so that their contents are not read as syntax, the separators && || $( ; | ( ) split a line into simple commands, and a cd sets the directory for the rest of its line.
func goCommands(text, dir string, env map[string]string) []goCommand {
	gowork, set := env["GOWORK"]
	var out []goCommand
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && strings.ContainsAny(line[:1], "@-+") {
			line = strings.TrimSpace(line[1:])
		}
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = goSingleQuotedRE.ReplaceAllString(line, "''")
		line = goDoubleQuotedRE.ReplaceAllStringFunc(line, func(q string) string {
			return quotedSpaces.Replace(q[1 : len(q)-1])
		})
		line = normalizeRefs(line)
		cur := dir
		for _, seg := range strings.Split(shellSeparators.Replace(line), "\n") {
			fields := strings.Fields(seg)
			if len(fields) == 0 {
				continue
			}
			if fields[0] == "cd" && len(fields) > 1 {
				cur = path.Clean(path.Join(cur, fields[1]))
				continue
			}
			inline := map[string]string{}
			i := 0
			for ; i < len(fields) && shellAssignRE.MatchString(fields[i]); i++ {
				k, v, _ := strings.Cut(fields[i], "=")
				inline[k] = v
			}
			if i == len(fields) {
				continue
			}
			if fields[i] != "go" && path.Base(fields[i]) != "golangci-lint" {
				continue
			}
			c := goCommand{Dir: cur, GOWORK: gowork, Set: set, Text: strings.Join(fields[i:], " ")}
			if v, ok := inline["GOWORK"]; ok {
				c.GOWORK, c.Set = v, true
			}
			out = append(out, c)
		}
	}
	return out
}

// modeProblems returns the command-mode problems of cmds: a command in "." must run with GOWORK=off, a command in "redis" with one of the workspace values, and any other directory is a problem. The workspace values are normalized as goCommands reads them.
func modeProblems(cmds []goCommand, workspaces ...string) []string {
	var ws []string
	for _, w := range workspaces {
		ws = append(ws, normalizeRefs(w))
	}
	var out []string
	for _, c := range cmds {
		shown := "(unset)"
		if c.Set {
			shown = c.GOWORK
		}
		switch c.Dir {
		case ".":
			if c.Set && c.GOWORK == "off" {
				continue
			}
			msg := fmt.Sprintf("core command %q runs with GOWORK=%s, want GOWORK=off", c.Text, shown)
			if strings.Contains(c.Text, "./...") {
				msg += " (no ./... from the root in workspace mode)"
			}
			out = append(out, msg)
		case "redis":
			if c.Set && slices.Contains(ws, c.GOWORK) {
				continue
			}
			out = append(out, fmt.Sprintf("redis command %q runs with GOWORK=%s, want the workspace %s", c.Text, shown, strings.Join(ws, " or ")))
		default:
			out = append(out, fmt.Sprintf("go command %q runs in %s, want the root or redis", c.Text, c.Dir))
		}
	}
	return out
}

// commandDirs returns the set of directories in which cmds run.
func commandDirs(cmds []goCommand) map[string]bool {
	dirs := map[string]bool{}
	for _, c := range cmds {
		dirs[c.Dir] = true
	}
	return dirs
}

// makefileProblems returns the problems of the Makefile recipes and of scripts/fuzz.sh with the command modes. Every go command of every target runs in the mode of its module, and the targets that must run a module do so. The result is sorted and has no duplicates.
func makefileProblems(makefile, fuzz []byte) []string {
	env := map[string]string{"MICROGATEWAY_HEAVY": "1", "MICROGATEWAY_REDIS_ADDR": "localhost:6379"}
	var names []string
	for name := range MakeRecipes(makefile) {
		if name != ".PHONY" {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	var out []string
	for _, name := range names {
		text, err := ExpandMakeRun("make "+name, makefile, env)
		if err != nil {
			out = append(out, fmt.Sprintf("make %s: %v", name, err))
			continue
		}
		cmds := goCommands(text, ".", nil)
		for _, p := range modeProblems(cmds, "$(CURDIR)/go.work") {
			out = append(out, fmt.Sprintf("make %s: %s", name, p))
		}
		dirs := commandDirs(cmds)
		switch name {
		case "test", "race", "vet", "bench", "cover", "lint":
			if !dirs["."] || !dirs["redis"] {
				out = append(out, fmt.Sprintf("make %s does not run both modules", name))
			}
		case "build", "integration":
			if !dirs["redis"] {
				out = append(out, fmt.Sprintf("make %s does not run the redis module", name))
			}
		case "realnet", "soak", "perf":
			if !dirs["."] {
				out = append(out, fmt.Sprintf("make %s does not run the core module", name))
			}
		case "tidy-check":
			if !slices.ContainsFunc(cmds, func(c goCommand) bool {
				return c.Dir == "." && c.Set && c.GOWORK == "off" && c.Text == "go mod tidy -diff"
			}) {
				out = append(out, "make tidy-check does not run GOWORK=off go mod tidy -diff in the core")
			}
			if !slices.Contains(strings.Split(text, "\n"), "sh scripts/tidy-redis.sh") {
				out = append(out, "make tidy-check does not run sh scripts/tidy-redis.sh")
			}
		case "fuzz":
			if !strings.Contains(text, "sh scripts/fuzz.sh") {
				out = append(out, "make fuzz does not run sh scripts/fuzz.sh")
			}
			fuzzCmds := goCommands(string(fuzz), ".", nil)
			for _, p := range modeProblems(fuzzCmds, "$root/go.work") {
				out = append(out, "scripts/fuzz.sh: "+p)
			}
			fuzzDirs := commandDirs(fuzzCmds)
			if !fuzzDirs["."] || !fuzzDirs["redis"] {
				out = append(out, "scripts/fuzz.sh does not run both modules")
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// ignored reports whether ignore-file lines exclude the root-level file name: the last matching pattern decides, a ! pattern re-includes, a leading / anchors, a trailing / matches directories only and is skipped, # lines are comments.
func ignored(lines []string, name string) bool {
	out := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		negate := strings.HasPrefix(line, "!")
		pat := strings.TrimPrefix(strings.TrimPrefix(line, "!"), "/")
		if strings.HasSuffix(pat, "/") {
			continue
		}
		if ok, _ := path.Match(pat, name); ok {
			out = !negate
		}
	}
	return out
}

// ignoreFileProblems returns the problems of the .gitignore and .dockerignore lines: the required entries, and the go.work files that must stay out of the ignore rules' reach.
func ignoreFileProblems(gitignore, dockerignore []string) []string {
	var out []string
	for _, want := range []string{".docs/", plannedDir + "/", "go.work.sum", "bin/"} {
		if !slices.Contains(gitignore, want) {
			out = append(out, fmt.Sprintf(".gitignore lacks %q", want))
		}
	}
	for _, want := range []string{".git", ".github", ".docs", "go.work.sum", "bin", "*.test", "*.out", "**/testdata/fuzz"} {
		if !slices.Contains(dockerignore, want) {
			out = append(out, fmt.Sprintf(".dockerignore lacks %q", want))
		}
	}
	if !slices.Contains(dockerignore, plannedDir) && !slices.Contains(dockerignore, plannedDir+"/") {
		out = append(out, ".dockerignore lacks the planning directory")
	}
	for _, f := range []struct {
		name  string
		lines []string
	}{{".gitignore", gitignore}, {".dockerignore", dockerignore}} {
		if ignored(f.lines, "go.work") {
			out = append(out, f.name+" ignores go.work")
		}
		if !ignored(f.lines, "go.work.sum") {
			out = append(out, f.name+" does not ignore go.work.sum")
		}
	}
	return out
}

// trackedWorkProblems returns the problems of the repository file list: exactly the root go.work may be tracked among files whose base name starts with go.work.
func trackedWorkProblems(files []string) []string {
	var out []string
	found := false
	for _, name := range files {
		if name == "go.work" {
			found = true
			continue
		}
		if strings.HasPrefix(path.Base(name), "go.work") {
			out = append(out, fmt.Sprintf("repository go.work file: %s; only the root go.work is tracked", name))
		}
	}
	if !found {
		out = append(out, "go.work is not tracked")
	}
	return out
}

func TestGoCommands(t *testing.T) {
	cases := []struct {
		name, text string
		env        map[string]string
		want       []goCommand
	}{
		{
			name: "workspace inline",
			text: "cd redis && FOO=1 GOWORK=$(CURDIR)/go.work go test ./...",
			want: []goCommand{{Dir: "redis", GOWORK: "${CURDIR}/go.work", Set: true, Text: "go test ./..."}},
		},
		{
			name: "command substitution",
			text: "GOWORK=off go test $(GOWORK=off go list ./... | grep -v -x -e 'a|b')",
			want: []goCommand{
				{Dir: ".", GOWORK: "off", Set: true, Text: "go test"},
				{Dir: ".", GOWORK: "off", Set: true, Text: "go list ./..."},
			},
		},
		{
			name: "quoted text and default mode",
			text: "echo \"go (x)\"; go vet ./...",
			env:  map[string]string{"GOWORK": "off"},
			want: []goCommand{{Dir: ".", GOWORK: "off", Set: true, Text: "go vet ./..."}},
		},
		{
			name: "at-sign command",
			text: "@command -v golangci-lint >/dev/null",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := goCommands(c.text, ".", c.env)
			if !slices.Equal(got, c.want) {
				t.Errorf("goCommands(%q) = %+v, want %+v", c.text, got, c.want)
			}
		})
	}
}

func TestMakefileModes(t *testing.T) {
	root := repoRoot(t)
	for _, p := range makefileProblems(readAt(t, root, "Makefile"), readAt(t, root, "scripts/fuzz.sh")) {
		t.Error(p)
	}
}

func TestMakefileModeRules(t *testing.T) {
	root := repoRoot(t)
	cases := []struct {
		name, file, old, with, want string
	}{
		{"core command without GOWORK=off", "Makefile", "GO_CORE := $(CORE_ENV) go", "GO_CORE := go", "core command"},
		{"redis command with GOWORK=off", "Makefile", "GO_REDIS := $(REDIS_ENV) go", "GO_REDIS := GOWORK=off go", "redis command"},
		{"workspace on a root pattern", "Makefile", "$(GO_CORE) test ./...", "$(GO_REDIS) test ./...", "no ./... from the root"},
		{"lint core without GOWORK=off", "Makefile", "$(CORE_ENV) $(GOLANGCI_LINT)", "$(GOLANGCI_LINT)", "core command"},
		{"lint redis without workspace", "Makefile", "cd redis && $(REDIS_ENV) $(GOLANGCI_LINT)", "cd redis && $(GOLANGCI_LINT)", "redis command"},
		{"tidy with the core toolchain in redis", "Makefile", "sh scripts/tidy-redis.sh", "cd redis && $(GO_CORE) mod tidy -diff", "redis command"},
		{"fuzz redis without workspace", "fuzz", `GOWORK="$root/go.work" go test`, "GOWORK=off go test", "redis command"},
		{"fuzz core with workspace", "fuzz", "GOWORK=off go test", "go test", "core command"},
		{"real", "", "", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			makefile := readAt(t, root, "Makefile")
			fuzz := readAt(t, root, "scripts/fuzz.sh")
			switch c.file {
			case "Makefile":
				makefile = []byte(replaceOnce(t, string(makefile), c.old, c.with))
			case "fuzz":
				fuzz = []byte(replaceOnce(t, string(fuzz), c.old, c.with))
			}
			problems := makefileProblems(makefile, fuzz)
			if c.want == "" {
				if len(problems) != 0 {
					t.Errorf("real files report problems: %q", problems)
				}
				return
			}
			if !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, c.want) }) {
				t.Errorf("want a problem containing %q, got %q", c.want, problems)
			}
		})
	}
}

func TestIgnoreFileRules(t *testing.T) {
	root := repoRoot(t)
	gitignore := readLines(t, root, ".gitignore")
	dockerignore := readLines(t, root, ".dockerignore")
	files := repoFiles(t)
	without := func(list []string, name string) []string {
		out := slices.DeleteFunc(slices.Clone(list), func(s string) bool { return s == name })
		if len(out) == len(list) {
			t.Fatalf("removing %q changed nothing", name)
		}
		return out
	}
	cases := []struct {
		name                    string
		gitignore, dockerignore []string
		files                   []string
		want                    string
	}{
		{"real", gitignore, dockerignore, files, ""},
		{"gitignore ignores go.work", slices.Concat(gitignore, []string{"go.work*"}), dockerignore, files, ".gitignore ignores go.work"},
		{"gitignore re-includes go.work", slices.Concat(gitignore, []string{"go.work*", "!go.work"}), dockerignore, files, ""},
		{"dockerignore ignores go.work", gitignore, slices.Concat(dockerignore, []string{"go.work"}), files, ".dockerignore ignores go.work"},
		{"gitignore without go.work.sum", without(gitignore, "go.work.sum"), dockerignore, files, "go.work.sum"},
		{"tracked redis go.work", gitignore, dockerignore, append(slices.Clone(files), "redis/go.work"), "redis/go.work"},
		{"tracked go.work.sum", gitignore, dockerignore, append(slices.Clone(files), "go.work.sum"), "go.work.sum"},
		{"no tracked root go.work", gitignore, dockerignore, without(files, "go.work"), "go.work is not tracked"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			problems := append(ignoreFileProblems(c.gitignore, c.dockerignore), trackedWorkProblems(c.files)...)
			if c.want == "" {
				if len(problems) != 0 {
					t.Errorf("problems = %q, want none", problems)
				}
				return
			}
			if !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, c.want) }) {
				t.Errorf("want a problem containing %q, got %q", c.want, problems)
			}
		})
	}
}

func TestRedisTidyCheck(t *testing.T) {
	for _, tool := range []string{"sh", "go"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skip(tool + " not on PATH: " + t.Name() + " not run")
		}
	}
	root := repoRoot(t)
	mod := readAt(t, root, "redis/go.mod")
	sum := readAt(t, root, "redis/go.sum")
	const xxhash = "github.com/cespare/xxhash/v2"
	hash := strings.Repeat("A", 43) + "="
	dropLines := func(src []byte, sub string) []byte {
		var kept []string
		for _, line := range strings.Split(string(src), "\n") {
			if !strings.Contains(line, sub) {
				kept = append(kept, line)
			}
		}
		out := []byte(strings.Join(kept, "\n"))
		if bytes.Equal(out, src) {
			t.Fatalf("removing the lines with %q changed nothing", sub)
		}
		return out
	}
	appendLines := func(src []byte, lines ...string) []byte {
		return []byte(strings.TrimRight(string(src), "\n") + "\n" + strings.Join(lines, "\n") + "\n")
	}
	var edited []string
	for _, line := range strings.Split(string(mod), "\n") {
		if strings.Contains(line, "golang.org/x/sys") {
			line = strings.Replace(line, " // indirect", "", 1)
		}
		edited = append(edited, line)
	}
	sysDirect := []byte(strings.Join(edited, "\n"))
	if bytes.Equal(sysDirect, mod) {
		t.Fatal("no golang.org/x/sys line is marked indirect")
	}
	cases := []struct {
		name     string
		mod, sum []byte
		ok       bool
		want     string
	}{
		{"committed files", mod, sum, true, ""},
		{"core sum lines ignored", mod, appendLines(sum, "github.com/ok0/go-microgateway v0.1.0 h1:"+hash, "github.com/ok0/go-microgateway v0.1.0/go.mod h1:"+hash), true, ""},
		{"missing indirect require", dropLines(mod, xxhash), sum, false, xxhash},
		{"indirect marked direct", sysDirect, sum, false, "golang.org/x/sys"},
		{"unused go.sum line", mod, appendLines(sum, "example.com/unused v1.0.0/go.mod h1:"+hash), false, "example.com/unused"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			modPath := filepath.Join(dir, "go.mod")
			sumPath := filepath.Join(dir, "go.sum")
			if err := os.WriteFile(modPath, c.mod, 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(sumPath, c.sum, 0o644); err != nil {
				t.Fatal(err)
			}
			out, err := run(t, root, nil, "sh", filepath.Join(root, "scripts", "tidy-redis.sh"), modPath, sumPath)
			if c.ok {
				if err != nil {
					t.Errorf("tidy check failed: %v\n%s", err, out)
				}
				return
			}
			if err == nil {
				t.Errorf("tidy check passed; output:\n%s", out)
				return
			}
			if !bytes.Contains(out, []byte(c.want)) {
				t.Errorf("tidy check output lacks %q:\n%s", c.want, out)
			}
		})
	}
}
