package doccheck

import (
	"maps"
	"slices"
	"strings"
	"testing"
)

func TestParseLog(t *testing.T) {
	out := []byte("abc123\x1fsubject line\n\nbody text\n\x1e\ndef456\x1fsecond\x1e\n")
	got := ParseLog(out)
	want := []Commit{{Hash: "abc123", Message: "subject line\n\nbody text"}, {Hash: "def456", Message: "second"}}
	if !slices.Equal(got, want) {
		t.Errorf("ParseLog = %+v, want %+v", got, want)
	}
	if got := ParseLog(nil); len(got) != 0 {
		t.Errorf("ParseLog(nil) = %+v, want none", got)
	}
}

func TestScanRefs(t *testing.T) {
	refs := []string{"refs/heads/main", "refs/heads/backup/pre-rename", "refs/remotes/origin/HEAD", "refs/remotes/origin/main", "refs/tags/v1"}
	want := []string{"refs/heads/main", "refs/remotes/origin/main", "refs/tags/v1"}
	if got := ScanRefs(refs); !slices.Equal(got, want) {
		t.Errorf("ScanRefs = %q, want %q", got, want)
	}
}

func TestCommitFindings(t *testing.T) {
	cases := []struct {
		name, msg string
		want      bool
	}{
		{"clean subject", "feat(cache): add purge", false},
		{"co-author trailer", "fix: thing\n\nCo-" + "authored-by: Someone <a@example.com>", true},
		{"session trailer", "fix: thing\n\n" + "Clau" + "de-Session: session_01", true},
		{"session url", "fix: thing\n\nsee https://" + "clau" + "de.ai/code/x", true},
		{"planning identifier", "fix: thing\n\nsee " + "M" + "3", true},
		{"former name", "fix: " + "lych" + "gate", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := CommitFindings([]Commit{{Hash: "abcdef1234", Message: c.msg}})
			if (len(got) != 0) != c.want {
				t.Fatalf("CommitFindings = %+v, want a finding: %v", got, c.want)
			}
			if c.want && got[0].Path != "abcdef1" {
				t.Errorf("finding path = %q, want the short hash", got[0].Path)
			}
		})
	}
}

func TestRemoteFindings(t *testing.T) {
	for _, u := range allowedRemoteURLs {
		if got := RemoteFindings([]RemoteURL{{Name: "origin", URL: u, Kind: "fetch"}}); len(got) != 0 {
			t.Errorf("allowed %s: findings %+v, want none", u, got)
		}
	}
	if got := RemoteFindings(nil); len(got) != 0 {
		t.Errorf("no remotes: findings %+v, want none", got)
	}
	foreign := []string{
		"https://github.com/someone/fork.git",
		"git@github.com:someone/go-microgateway.git",
		"https://gitlab.com/ok0/go-microgateway",
		"https://github.com/ok0/go-microgateway-fork.git",
		"https://github.com/ok0/go-microgateway/",
		"ssh://git@github.com/ok0/go-microgateway.git",
		"/tmp/repo",
		"",
	}
	for _, u := range foreign {
		got := RemoteFindings([]RemoteURL{{Name: "other", URL: u, Kind: "fetch"}})
		if len(got) != 1 || got[0].Text != u {
			t.Errorf("foreign %q: findings %+v, want exactly one naming it", u, got)
		}
	}
}

func TestParseRemoteV(t *testing.T) {
	out := []byte("origin\thttps://github.com/ok0/go-microgateway.git (fetch)\n" +
		"origin\thttps://github.com/ok0/go-microgateway.git (push)\n" +
		"fork\thttps://github.com/someone/fork.git (fetch)\n" +
		"fork\thttps://github.com/someone/fork.git (push)\n")
	remotes := ParseRemoteV(out)
	if len(remotes) != 4 {
		t.Fatalf("ParseRemoteV = %+v, want 4 entries", remotes)
	}
	findings := RemoteFindings(remotes)
	if len(findings) != 2 {
		t.Fatalf("findings = %+v, want 2", findings)
	}
	for _, f := range findings {
		if f.Path != "fork" {
			t.Errorf("finding %+v, want the fork remote", f)
		}
	}

	pushOut := []byte("origin\thttps://github.com/ok0/go-microgateway.git (fetch)\norigin\thttps://github.com/someone/fork.git (push)\n")
	pushRemotes := ParseRemoteV(pushOut)
	pushFindings := RemoteFindings(pushRemotes)
	if len(pushFindings) != 1 || pushFindings[0].Path != "origin" {
		t.Fatalf("push findings = %+v, want one for origin", pushFindings)
	}
	if pushRemotes[1].Kind != "push" {
		t.Errorf("push entry kind = %q, want push", pushRemotes[1].Kind)
	}

	if got := ParseRemoteV(nil); len(got) != 0 {
		t.Errorf("empty output: entries %+v, want none", got)
	}
	if got := RemoteFindings(ParseRemoteV(nil)); len(got) != 0 {
		t.Errorf("empty output: findings %+v, want none", got)
	}

	garbled := ParseRemoteV([]byte("not a remote line\n"))
	if len(garbled) != 1 || garbled[0].Kind != "unparsed" || len(RemoteFindings(garbled)) != 1 {
		t.Errorf("unparsed line: entries %+v, want one unparsed finding", garbled)
	}
}

func TestParseRemoteConfig(t *testing.T) {
	out := []byte("remote.origin.url git@github.com:ok0/go-microgateway.git\nremote.my.mirror.pushurl https://example.com/x.git\n")
	remotes := ParseRemoteConfig(out)
	if len(remotes) != 2 || remotes[0].Name != "origin" || remotes[1].Name != "my.mirror" {
		t.Fatalf("ParseRemoteConfig = %+v, want names origin and my.mirror", remotes)
	}
	findings := RemoteFindings(remotes)
	if len(findings) != 1 || findings[0].Path != "my.mirror" {
		t.Errorf("findings = %+v, want one for my.mirror", findings)
	}
}

func TestCountLines(t *testing.T) {
	cases := []struct {
		src  string
		want int
	}{
		{"", 0},
		{"a", 1},
		{"a\n", 1},
		{"a\nb", 2},
		{"a\n\n", 2},
	}
	for _, c := range cases {
		if got := CountLines([]byte(c.src)); got != c.want {
			t.Errorf("CountLines(%q) = %d, want %d", c.src, got, c.want)
		}
	}
}

func TestLineGate(t *testing.T) {
	if err := LineGate(9999); err == nil {
		t.Error("LineGate(9999) = nil, want an error")
	}
	if err := LineGate(10000); err != nil {
		t.Errorf("LineGate(10000) = %v, want nil", err)
	}
}

func TestDependencyFindings(t *testing.T) {
	core := []string{"go.yaml.in/yaml/v3", "github.com/golang-jwt/jwt/v5", "github.com/fsnotify/fsnotify"}
	redis := []string{"github.com/ok0/go-microgateway", "github.com/redis/go-redis/v9", "github.com/alicebob/miniredis/v2"}
	cases := []struct {
		name        string
		core, redis []string
		want        bool
	}{
		{"valid sets", core, redis, false},
		{"seven core dependencies", []string{"a/1", "a/2", "a/3", "a/4", "a/5", "a/6", "a/7"}, redis, true},
		{"core with go-redis", []string{"github.com/redis/go-redis/v9"}, redis, true},
		{"redis missing miniredis", core, redis[:2], true},
		{"redis extra dependency", core, append(slices.Clone(redis), "github.com/x/extra"), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := DependencyFindings(c.core, c.redis)
			if (len(got) != 0) != c.want {
				t.Errorf("DependencyFindings = %q, want a problem: %v", got, c.want)
			}
		})
	}
}

func TestMakeTargets(t *testing.T) {
	fixture := ".PHONY: build test\nA := b:c\nbuild:\n\tgo build ./...\n\tcheck:x\ntest: build\n\tgo test ./...\n"
	got := MakeTargets([]byte(fixture))
	if len(got) != 3 {
		t.Fatalf("MakeTargets = %v, want .PHONY, build and test only", got)
	}
	if want := []string{"build", "test"}; !slices.Equal(got[".PHONY"], want) {
		t.Errorf(".PHONY = %q, want %q", got[".PHONY"], want)
	}
	if want := []string{"build"}; !slices.Equal(got["test"], want) {
		t.Errorf("test prerequisites = %q, want %q", got["test"], want)
	}
	if _, ok := got["build"]; !ok {
		t.Error("build target missing")
	}
}

func TestMakeRecipes(t *testing.T) {
	fixture := "a: b\n\tcd x && go test -tags t ./...\n\techo two\n\nb:\n\t@true\nc d:\n\tdo-cd\nV := e:f\n"
	got := MakeRecipes([]byte(fixture))
	if want := []string{"cd x && go test -tags t ./...", "echo two"}; !slices.Equal(got["a"], want) {
		t.Errorf("a recipe = %q, want %q", got["a"], want)
	}
	if want := []string{"@true"}; !slices.Equal(got["b"], want) {
		t.Errorf("b recipe = %q, want %q", got["b"], want)
	}
	for _, name := range []string{"c", "d"} {
		if want := []string{"do-cd"}; !slices.Equal(got[name], want) {
			t.Errorf("%s recipe = %q, want %q", name, got[name], want)
		}
	}
	if _, ok := got["V"]; ok {
		t.Error("variable V is not a target")
	}
}

func TestExpandMakeRun(t *testing.T) {
	fixture := []byte("a: b\n\tcd x && go test -tags t ./...\n\techo two\n\nb:\n\t@true\nc d:\n\tdo-cd\nV := e:f\n")
	got, err := ExpandMakeRun("make a", fixture, nil)
	if err != nil {
		t.Fatal(err)
	}
	before := strings.Index(got, "@true")
	after := strings.Index(got, "cd x && go test -tags t ./...")
	if before < 0 || after < 0 || before > after {
		t.Errorf("make a expansion = %q, want @true before the recipe of a", got)
	}
	got, err = ExpandMakeRun("make c d", fixture, nil)
	if err != nil || !strings.Contains(got, "do-cd") {
		t.Errorf("make c d expansion = %q, %v, want do-cd", got, err)
	}
	got, err = ExpandMakeRun("go test ./...", fixture, nil)
	if err != nil || got != "go test ./..." {
		t.Errorf("plain run = %q, %v, want unchanged", got, err)
	}
	if _, err := ExpandMakeRun("make missing", fixture, nil); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Errorf("make missing error = %v, want an error naming missing", err)
	}
	for _, run := range []string{"make -n a", "make VAR=x a", "make"} {
		if _, err := ExpandMakeRun(run, fixture, nil); err == nil || !strings.Contains(err.Error(), "unsupported make invocation") {
			t.Errorf("%q error = %v, want unsupported make invocation", run, err)
		}
	}
	cycle := []byte("x: y\n\techo x\ny: x\n\techo y\n")
	got, err = ExpandMakeRun("make x", cycle, nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(got, "echo x") != 1 || strings.Count(got, "echo y") != 1 {
		t.Errorf("cyclic expansion = %q, want each recipe once", got)
	}
}

func TestExpandMakeRunVariables(t *testing.T) {
	const goMake = "GO_REDIS := GOWORK=$(CURDIR)/go.work go\nT = -tags $(TAG)\nTAG ?= integration\nx:\n\tcd redis && $(GO_REDIS) test -race ${T} ./...\n"
	cases := []struct {
		name, makefile, run string
		env                 map[string]string
		contains, errWant   string
		exact               string
	}{
		{name: "assignments and conditional default", makefile: goMake, run: "make x", contains: "cd redis && GOWORK=$(CURDIR)/go.work go test -race -tags integration ./..."},
		{name: "environment overrides conditional", makefile: goMake, run: "make x", env: map[string]string{"TAG": "realnet"}, contains: "-tags realnet"},
		{name: "makefile assignment wins over environment", makefile: goMake, run: "make x", env: map[string]string{"GO_REDIS": "other"}, contains: "GOWORK=$(CURDIR)/go.work go test"},
		{name: "dollar escapes", makefile: "x:\n\techo $$HOME $$$$\n", run: "make x", contains: "echo $HOME $$"},
		{name: "undefined variable", makefile: "x:\n\techo $(NOPE)\n", run: "make x", errWant: "NOPE"},
		{name: "variable from environment", makefile: "x:\n\techo $(NOPE)\n", run: "make x", env: map[string]string{"NOPE": "v"}, contains: "echo v"},
		{name: "variable cycle", makefile: "A = $(B)\nB = $(A)\nx:\n\t$(A)\n", run: "make x", errWant: "cycle"},
		{name: "function calls kept", makefile: "V := $(shell git describe)\nx:\n\techo $(V) $(subst a,b,c)\n", run: "make x", contains: "echo $(shell git describe) $(subst a,b,c)"},
		{name: "built-ins and automatic variables kept", makefile: "x:\n\tcd $(CURDIR) && echo $@\n", run: "make x", contains: "cd $(CURDIR) && echo $@"},
		{name: "unterminated reference", makefile: "x:\n\techo $(GO\n", run: "make x", errWant: "unterminated"},
		{name: "single-letter short form", makefile: "x:\n\techo $Q\n", run: "make x", env: map[string]string{"Q": "q"}, contains: "echo q"},
		{name: "unreached recipe not expanded", makefile: "y:\n\techo $(NOPE)\nx:\n\ttrue\n", run: "make x", contains: "true"},
		{name: "non-make line kept", makefile: "x:\n", run: "go test $(X)", exact: "go test $(X)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ExpandMakeRun(c.run, []byte(c.makefile), c.env)
			if c.errWant != "" {
				if err == nil || !strings.Contains(err.Error(), c.errWant) {
					t.Fatalf("error = %v, want one containing %q", err, c.errWant)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.exact != "" && got != c.exact {
				t.Errorf("output = %q, want %q", got, c.exact)
			}
			if !strings.Contains(got, c.contains) {
				t.Errorf("output = %q, want it to contain %q", got, c.contains)
			}
		})
	}
}

func TestMakeVariables(t *testing.T) {
	got, err := MakeVariables([]byte("A := 1\nB ?= 2\nC = $(A)\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]MakeVar{"A": {Value: "1"}, "B": {Value: "2", Conditional: true}, "C": {Value: "$(A)"}}
	if !maps.Equal(got, want) {
		t.Errorf("MakeVariables = %+v, want %+v", got, want)
	}
	errCases := []struct {
		name, makefile, want string
	}{
		{"assigned twice", "A := 1\nA := 2\n", "assigned twice"},
		{"append operator", "A += x\n", "+="},
		{"shell assignment", "A != date\n", "!="},
		{"directive", "ifdef X\nendif\n", "ifdef"},
		{"use before assignment", "A := $(B)\nB := x\n", "before its assignment"},
	}
	for _, c := range errCases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := MakeVariables([]byte(c.makefile)); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("error = %v, want one containing %q", err, c.want)
			}
		})
	}
	got, err = MakeVariables([]byte("a: b\n\tX = 1\n"))
	if err != nil || len(got) != 0 {
		t.Errorf("rule and recipe lines: variables %+v, %v, want none", got, err)
	}
}

func TestChecklistProblems(t *testing.T) {
	cases := []struct {
		name     string
		section  string
		files    map[string][]byte
		targets  map[string][]string
		required []string
		want     []string
	}{
		{
			name:    "prose names are not test names",
			section: "| 1. Benchmarks | 3. Fuzzing | Every `Fuzz` target | Tests and Testing |",
			files:   map[string][]byte{},
			targets: map[string][]string{"perf": nil},
		},
		{
			name:    "declared test",
			section: "`TestFoo`",
			files:   map[string][]byte{"a/a_test.go": []byte("func TestFoo(t *testing.T) {}\n")},
			targets: map[string][]string{"perf": nil},
		},
		{
			name:    "test declared only in doccheck",
			section: "`TestFoo`",
			files:   map[string][]byte{"internal/doccheck/x_test.go": []byte("func TestFoo(t *testing.T) {}\n")},
			targets: map[string][]string{"perf": nil},
			want:    []string{"TestFoo is not declared as func TestFoo( in a _test.go file outside internal/doccheck"},
		},
		{
			name:    "test named inside a string literal",
			section: "`TestFoo`",
			files:   map[string][]byte{"a/a_test.go": []byte("var s = \"func TestFoo(\"\n")},
			targets: map[string][]string{"perf": nil},
			want:    []string{"TestFoo is not declared as func TestFoo( in a _test.go file outside internal/doccheck"},
		},
		{
			name:    "test declared in a non-test file",
			section: "`TestFoo`",
			files:   map[string][]byte{"a/a.go": []byte("func TestFoo(t *testing.T) {}\n")},
			targets: map[string][]string{"perf": nil},
			want:    []string{"TestFoo is not declared as func TestFoo( in a _test.go file outside internal/doccheck"},
		},
		{
			name:    "benchmark and fuzz declarations",
			section: "BenchmarkRouter_Match and FuzzParse",
			files:   map[string][]byte{"r/r_test.go": []byte("func BenchmarkRouter_Match(b *testing.B) {}\nfunc FuzzParse(f *testing.F) {}\n")},
			targets: map[string][]string{"perf": nil},
		},
		{
			name:    "env token in a Makefile",
			section: "MICROGATEWAY_X",
			files:   map[string][]byte{"Makefile": []byte("x:\n\t@test -n \"$(MICROGATEWAY_X)\"\n")},
			targets: map[string][]string{"perf": nil},
		},
		{
			name:    "env token only in doccheck",
			section: "MICROGATEWAY_X",
			files:   map[string][]byte{"internal/doccheck/repo_release_test.go": []byte("\"MICROGATEWAY_X\"")},
			targets: map[string][]string{"perf": nil},
			want:    []string{"MICROGATEWAY_X appears in no non-Markdown file outside internal/doccheck"},
		},
		{
			name:    "env token only in Markdown",
			section: "MICROGATEWAY_X",
			files:   map[string][]byte{"README.md": []byte("MICROGATEWAY_X")},
			targets: map[string][]string{"perf": nil},
			want:    []string{"MICROGATEWAY_X appears in no non-Markdown file outside internal/doccheck"},
		},
		{
			name:    "unknown make target",
			section: "`make perf` and `make nosuch`",
			files:   map[string][]byte{},
			targets: map[string][]string{"perf": nil},
			want:    []string{"make nosuch is not a Makefile target"},
		},
		{
			name:     "required text missing",
			section:  "abc",
			files:    map[string][]byte{},
			targets:  map[string][]string{"perf": nil},
			required: []string{"Go 1.26"},
			want:     []string{`section lacks "Go 1.26"`},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ChecklistProblems(c.section, c.files, c.targets, c.required)
			if !slices.Equal(got, c.want) {
				t.Errorf("ChecklistProblems = %q, want %q", got, c.want)
			}
		})
	}
}
