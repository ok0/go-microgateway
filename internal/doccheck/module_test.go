package doccheck

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

const coreModFixture = "module github.com/ok0/go-microgateway\n\ngo 1.27.0\n"

func redisModFixture(v string) string {
	return "module github.com/ok0/go-microgateway/redis\n\ngo 1.27.0\n\nrequire github.com/ok0/go-microgateway " + v + "\n"
}

func replaceOnce(t *testing.T, src, old, with string) string {
	t.Helper()
	out := strings.Replace(src, old, with, 1)
	if out == src {
		t.Fatalf("replacing %q changed nothing", old)
	}
	return out
}

func findingPatterns(found []Finding) []string {
	var out []string
	for _, f := range found {
		out = append(out, f.Pattern)
	}
	slices.Sort(out)
	return out
}

func TestParseGoMod(t *testing.T) {
	src := "module \"github.com/ok0/go-microgateway/redis\"\n\n" +
		"go 1.27.0\n\n" +
		"require github.com/a/one v1.0.0\n\n" +
		"require (\n" +
		"\tgithub.com/a/two v1.2.0 // indirect\n" +
		"\t\"github.com/b/quoted\" v2.0.0\n" +
		"\t// comment line\n" +
		")\n\n" +
		"replace github.com/ok0/go-microgateway => ../\n\n" +
		"replace (\n" +
		"\tgithub.com/a v1.0.0 => github.com/b v1.1.0\n" +
		")\n"
	got, err := ParseGoMod([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	want := GoMod{
		Module: "github.com/ok0/go-microgateway/redis",
		Go:     "1.27.0",
		Requires: map[string]string{
			"github.com/a/one":    "v1.0.0",
			"github.com/a/two":    "v1.2.0",
			"github.com/b/quoted": "v2.0.0",
		},
		Replaces: []string{"github.com/ok0/go-microgateway => ../", "github.com/a v1.0.0 => github.com/b v1.1.0"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ParseGoMod = %+v, want %+v", got, want)
	}

	errCases := []struct {
		name, src, want string
	}{
		{"no module", "go 1.27.0\n", "module"},
		{"malformed require", "module example.com/m\n\nrequire github.com/x\n", "malformed require"},
		{"unterminated block", "module example.com/m\n\nrequire (\n\tgithub.com/x v1.0.0\n", "unterminated"},
	}
	for _, c := range errCases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseGoMod([]byte(c.src))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("ParseGoMod error = %v, want one containing %q", err, c.want)
			}
		})
	}

	root, err := Root()
	if err != nil {
		t.Fatal(err)
	}
	gomod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	repo, err := ParseGoMod(gomod)
	if err != nil {
		t.Fatal(err)
	}
	if repo.Module != "github.com/ok0/go-microgateway" {
		t.Errorf("repository module = %q, want github.com/ok0/go-microgateway", repo.Module)
	}
	if repo.Go == "" {
		t.Error("repository go.mod has no go version")
	}
}

func TestModuleFindings(t *testing.T) {
	cases := []struct {
		name, coreSrc, redisSrc, wantPath string
		wantPatterns                      []string
	}{
		{"valid", coreModFixture, redisModFixture("v0.1.0"), "", nil},
		{"prerelease version", coreModFixture, redisModFixture("v0.1.0-rc.1"), "", nil},
		{"core replace", coreModFixture + "\nreplace example.com/x => ../x\n", redisModFixture("v0.1.0"), "go.mod", []string{"replace"}},
		{"redis replace", coreModFixture, redisModFixture("v0.1.0") + "\nreplace github.com/ok0/go-microgateway => ../\n", "redis/go.mod", []string{"replace"}},
		{"redis without core require", coreModFixture, "module github.com/ok0/go-microgateway/redis\n\ngo 1.27.0\n", "redis/go.mod", []string{"core-require"}},
		{"pseudo version after release", coreModFixture, redisModFixture("v0.1.1-0.20260101000000-abcdefabcdef"), "redis/go.mod", []string{"core-pseudo"}},
		{"pseudo version on prerelease", coreModFixture, redisModFixture("v1.0.0-pre.0.20260101000000-abcdefabcdef"), "redis/go.mod", []string{"core-pseudo"}},
		{"zero placeholder pseudo version", coreModFixture, redisModFixture("v0.0.0-00010101000000-000000000000"), "redis/go.mod", []string{"core-placeholder", "core-pseudo"}},
		{"zero timestamp pseudo version", coreModFixture, redisModFixture("v0.0.0-20260101000000-abcdefabcdef"), "redis/go.mod", []string{"core-placeholder", "core-pseudo"}},
		{"short version", coreModFixture, redisModFixture("v1.2"), "redis/go.mod", []string{"core-semver"}},
		{"latest", coreModFixture, redisModFixture("latest"), "redis/go.mod", []string{"core-semver"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			core, err := ParseGoMod([]byte(c.coreSrc))
			if err != nil {
				t.Fatal(err)
			}
			redis, err := ParseGoMod([]byte(c.redisSrc))
			if err != nil {
				t.Fatal(err)
			}
			found := ModuleFindings(core, redis)
			if got := findingPatterns(found); !slices.Equal(got, c.wantPatterns) {
				t.Errorf("patterns = %q, want %q (findings %+v)", got, c.wantPatterns, found)
			}
			for _, f := range found {
				if f.Path != c.wantPath {
					t.Errorf("finding %+v has path %q, want %q", f, f.Path, c.wantPath)
				}
			}
		})
	}
}

func TestWorkFindings(t *testing.T) {
	const validWork = "go 1.27.0\n\nuse (\n\t.\n\t./redis\n)\n\nreplace github.com/ok0/go-microgateway v0.1.0 => ./\n"
	const workComments = "// Local development: the Redis module builds and tests against the core module\n" +
		"// in this checkout. The replace maps the core version that redis/go.mod requires\n" +
		"// to the checkout, so workspace builds need neither a published core nor network.\n\n"
	coreGo128 := strings.Replace(coreModFixture, "go 1.27.0", "go 1.28.0", 1)
	cases := []struct {
		name, coreSrc, redisSrc, work string
		wantPatterns, wantText        []string
	}{
		{"valid", "", "", validWork, nil, nil},
		{"valid with comment lines", "", "", workComments + validWork, nil, nil},
		{"single-line use entries", "", "", "go 1.27.0\n\nuse .\nuse ./redis\n\nreplace github.com/ok0/go-microgateway v0.1.0 => ./\n", nil, nil},
		{"replace to dot", "", "", replaceOnce(t, validWork, "=> ./", "=> ."), nil, nil},
		{"extra use", "", "", replaceOnce(t, validWork, "\t./redis\n)", "\t./redis\n\t./other\n)"), []string{"use-extra"}, nil},
		{"duplicate use", "", "", replaceOnce(t, validWork, "\t./redis\n)", "\t./redis\n\t./redis\n)"), []string{"use-extra"}, nil},
		{"missing use redis", "", "", replaceOnce(t, validWork, "\t./redis\n", ""), []string{"use-missing"}, nil},
		{"missing use dot", "", "", replaceOnce(t, validWork, "\t.\n\t./redis\n", "\t./redis\n"), []string{"use-missing"}, nil},
		{"go 1.27.2", "", "", replaceOnce(t, validWork, "go 1.27.0", "go 1.27.2"), []string{"go"}, nil},
		{"no go line", "", "", replaceOnce(t, validWork, "go 1.27.0\n\n", ""), []string{"go"}, nil},
		{"toolchain line", "", "", replaceOnce(t, validWork, "go 1.27.0\n", "go 1.27.0\ntoolchain go1.27.2\n"), []string{"extra"}, nil},
		{"missing replace", "", "", replaceOnce(t, validWork, "replace github.com/ok0/go-microgateway v0.1.0 => ./\n", ""), []string{"replace-missing"}, nil},
		{"replace without version", "", "", replaceOnce(t, validWork, "v0.1.0 => ./", "=> ./"), []string{"replace", "replace-missing"}, []string{"no version"}},
		{"replace version differs from redis require", "", "", replaceOnce(t, validWork, "v0.1.0 => ./", "v0.2.0 => ./"), []string{"replace", "replace-missing"}, []string{"v0.2.0", "v0.1.0"}},
		{"redis requires newer version than replace", "", redisModFixture("v0.2.0"), validWork, []string{"replace", "replace-missing"}, nil},
		{"redis and replace at newer version", "", redisModFixture("v0.2.0"), replaceOnce(t, validWork, "v0.1.0 => ./", "v0.2.0 => ./"), nil, nil},
		{"core and work at go 1.28.0", coreGo128, "", replaceOnce(t, validWork, "go 1.27.0", "go 1.28.0"), nil, nil},
		{"core at go 1.28.0 with work at go 1.27.0", coreGo128, "", validWork, []string{"go"}, nil},
		{"replace to parent directory", "", "", replaceOnce(t, validWork, "=> ./", "=> ../"), []string{"replace", "replace-missing"}, nil},
		{"two identical valid replaces", "", "", replaceOnce(t, validWork, "replace github.com/ok0/go-microgateway v0.1.0 => ./\n", "replace github.com/ok0/go-microgateway v0.1.0 => ./\nreplace github.com/ok0/go-microgateway v0.1.0 => ./\n"), []string{"replace"}, nil},
		{"extra replace", "", "", replaceOnce(t, validWork, "replace github.com/ok0/go-microgateway v0.1.0 => ./\n", "replace github.com/ok0/go-microgateway v0.1.0 => ./\nreplace example.com/x v1.0.0 => ../x\n"), []string{"replace"}, nil},
		{"unterminated use block", "", "", replaceOnce(t, validWork, "use (\n\t.\n\t./redis\n)", "use (\n\t.\n"), []string{"parse"}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			coreSrc, redisSrc := c.coreSrc, c.redisSrc
			if coreSrc == "" {
				coreSrc = coreModFixture
			}
			if redisSrc == "" {
				redisSrc = redisModFixture("v0.1.0")
			}
			core, err := ParseGoMod([]byte(coreSrc))
			if err != nil {
				t.Fatal(err)
			}
			redis, err := ParseGoMod([]byte(redisSrc))
			if err != nil {
				t.Fatal(err)
			}
			found := WorkFindings([]byte(c.work), core, redis)
			if got := findingPatterns(found); !slices.Equal(got, c.wantPatterns) {
				t.Errorf("patterns = %q, want %q (findings %+v)", got, c.wantPatterns, found)
			}
			for _, f := range found {
				if f.Path != "go.work" {
					t.Errorf("finding %+v has path %q, want go.work", f, f.Path)
				}
			}
			for _, w := range c.wantText {
				if !slices.ContainsFunc(found, func(f Finding) bool {
					return f.Pattern == "replace" && strings.Contains(f.Text, w)
				}) {
					t.Errorf("no replace finding mentions %q: %+v", w, found)
				}
			}
		})
	}
}
