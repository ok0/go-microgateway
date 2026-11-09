package doccheck

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

const coreModFixture = "module github.com/ok0/go-microgateway\n\ngo 1.27.0\n"

const validWorkFixture = "go 1.27.0\n\nuse (\n\t.\n\t./redis\n)\n\nreplace github.com/ok0/go-microgateway v0.1.0 => ./\n"

var releaseVersionCases = []struct {
	v  string
	ok bool
}{
	{"v0.1.0", true},
	{"v0.2.0-rc.1", true},
	{"v1.10.0", true},
	{"", false},
	{"0.2.0", false},
	{"v1", false},
	{"v1.2", false},
	{"v01.2.3", false},
	{"latest", false},
	{"(devel)", false},
	{"v0.2.0+dirty", false},
	{"v0.2.0-0.20260101000000-abcdefabcdef", false},
	{"v0.0.0-20260101000000-abcdefabcdef", false},
	{"v1.0.0-pre.0.20260101000000-abcdefabcdef", false},
}

func parseMod(t *testing.T, src string) GoMod {
	t.Helper()
	m, err := ParseGoMod([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func tagReader(reqs map[string]string) func(string) (string, error) {
	return func(tag string) (string, error) {
		if v, ok := reqs[tag]; ok {
			return v, nil
		}
		return "", errors.New("no redis/go.mod")
	}
}

func installReadme(lines ...string) string {
	return "# x\n\n```sh\n" + strings.Join(lines, "\n") + "\n```\n"
}

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

func TestReleaseVersion(t *testing.T) {
	for _, c := range releaseVersionCases {
		if got := releaseVersion(c.v); got != c.ok {
			t.Errorf("releaseVersion(%q) = %t, want %t", c.v, got, c.ok)
		}
	}
}

func TestCompareRelease(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"v0.9.0", "v0.10.0", -1},
		{"v0.2.0-rc.1", "v0.2.0", -1},
		{"v0.2.0-rc.1", "v0.2.0-rc.2", -1},
		{"v0.2.0-rc.2", "v0.2.0-rc.10", -1},
		{"v0.2.0-1", "v0.2.0-alpha", -1},
		{"v0.2.0-alpha", "v0.2.0-alpha.1", -1},
		{"v1.0.0", "v1.0.0", 0},
	}
	for _, c := range cases {
		if got := compareRelease(c.a, c.b); got != c.want {
			t.Errorf("compareRelease(%s, %s) = %d, want %d", c.a, c.b, got, c.want)
		}
		if got := compareRelease(c.b, c.a); got != -c.want {
			t.Errorf("compareRelease(%s, %s) = %d, want %d", c.b, c.a, got, -c.want)
		}
	}
}

func TestReleaseFindings(t *testing.T) {
	cases := []struct {
		name, version, redisVersion, workVersion string
		wantPatterns                             []string
		wantPaths                                map[string]string
		wantText                                 []string
	}{
		{"matching release", "v0.1.0", "v0.1.0", "v0.1.0", nil, nil, nil},
		{"redis and work behind", "v0.2.0", "v0.1.0", "v0.1.0", []string{"release-replace", "release-require"},
			map[string]string{"release-require": "redis/go.mod", "release-replace": "go.work"}, []string{"v0.1.0", "v0.2.0"}},
		{"matching second release", "v0.2.0", "v0.2.0", "v0.2.0", nil, nil, nil},
		{"work at another version", "v0.2.0", "v0.2.0", "v0.3.0", []string{"release-replace", "replace", "replace-missing"},
			map[string]string{"release-replace": "go.work", "replace": "go.work", "replace-missing": "go.work"}, []string{"v0.3.0"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			work := strings.Replace(validWorkFixture, "v0.1.0 =>", c.workVersion+" =>", 1)
			found := ReleaseFindings(c.version, []byte(work), parseMod(t, coreModFixture), parseMod(t, redisModFixture(c.redisVersion)))
			if got := findingPatterns(found); !slices.Equal(got, c.wantPatterns) {
				t.Errorf("patterns = %q, want %q (findings %+v)", got, c.wantPatterns, found)
			}
			for _, f := range found {
				if want, ok := c.wantPaths[f.Pattern]; !ok || f.Path != want {
					t.Errorf("finding %+v has path %q, want %q", f, f.Path, want)
				}
			}
			for _, w := range c.wantText {
				if !slices.ContainsFunc(found, func(f Finding) bool { return strings.Contains(f.Text, w) }) {
					t.Errorf("no finding mentions %q: %+v", w, found)
				}
			}
		})
	}

	for _, c := range releaseVersionCases {
		if c.ok {
			continue
		}
		t.Run("rejected "+c.v, func(t *testing.T) {
			found := ReleaseFindings(c.v, []byte(validWorkFixture), parseMod(t, coreModFixture), parseMod(t, redisModFixture("v0.1.0")))
			if got := findingPatterns(found); !slices.Equal(got, []string{"release-version"}) {
				t.Errorf("patterns = %q, want [release-version] (findings %+v)", got, found)
			}
			if len(found) == 1 && found[0].Path != "VERSION" {
				t.Errorf("finding %+v has path %q, want VERSION", found[0], found[0].Path)
			}
		})
	}
}

func TestTagFindings(t *testing.T) {
	cases := []struct {
		name         string
		tags         []string
		reqs         map[string]string
		wantPatterns []string
		wantPath     string
		wantText     []string
	}{
		{name: "none"},
		{name: "paired release", tags: []string{"v0.1.0", "redis/v0.1.0"}, reqs: map[string]string{"redis/v0.1.0": "v0.1.0"}},
		{name: "release in progress", tags: []string{"v0.1.0"}},
		{name: "unpaired highest", tags: []string{"v0.1.0", "redis/v0.1.0", "v0.2.0"}, reqs: map[string]string{"redis/v0.1.0": "v0.1.0"}},
		{name: "unpaired older", tags: []string{"v0.1.0", "v0.2.0", "redis/v0.2.0"}, reqs: map[string]string{"redis/v0.2.0": "v0.2.0"},
			wantPatterns: []string{"tag-redis-missing"}, wantPath: "v0.1.0"},
		{name: "two unpaired older", tags: []string{"v0.1.0", "v0.2.0"},
			wantPatterns: []string{"tag-redis-missing"}, wantPath: "v0.1.0"},
		{name: "numeric order", tags: []string{"v0.9.0", "v0.10.0", "redis/v0.10.0"}, reqs: map[string]string{"redis/v0.10.0": "v0.10.0"},
			wantPatterns: []string{"tag-redis-missing"}, wantPath: "v0.9.0"},
		{name: "prerelease before release", tags: []string{"v0.2.0-rc.1", "v0.2.0", "redis/v0.2.0"}, reqs: map[string]string{"redis/v0.2.0": "v0.2.0"},
			wantPatterns: []string{"tag-redis-missing"}, wantPath: "v0.2.0-rc.1"},
		{name: "prerelease pair", tags: []string{"v0.2.0-rc.1", "redis/v0.2.0-rc.1", "v0.2.0-rc.2"}, reqs: map[string]string{"redis/v0.2.0-rc.1": "v0.2.0-rc.1"}},
		{name: "redis without core", tags: []string{"redis/v0.1.0"}, reqs: map[string]string{"redis/v0.1.0": "v0.1.0"},
			wantPatterns: []string{"tag-core-missing"}, wantPath: "redis/v0.1.0"},
		{name: "redis requires older", tags: []string{"v0.2.0", "redis/v0.2.0"}, reqs: map[string]string{"redis/v0.2.0": "v0.1.0"},
			wantPatterns: []string{"tag-require"}, wantPath: "redis/v0.2.0", wantText: []string{"v0.1.0", "v0.2.0"}},
		{name: "redis go.mod unreadable", tags: []string{"v0.1.0", "redis/v0.1.0"},
			wantPatterns: []string{"tag-read"}, wantPath: "redis/v0.1.0"},
		{name: "other tags ignored", tags: []string{"backup", "vfoo", "redis/latest", "v1", "redis/v1"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			found := TagFindings(c.tags, tagReader(c.reqs))
			if got := findingPatterns(found); !slices.Equal(got, c.wantPatterns) {
				t.Errorf("patterns = %q, want %q (findings %+v)", got, c.wantPatterns, found)
			}
			for _, f := range found {
				if f.Path != c.wantPath {
					t.Errorf("finding %+v has path %q, want %q", f, f.Path, c.wantPath)
				}
			}
			for _, w := range c.wantText {
				if !slices.ContainsFunc(found, func(f Finding) bool { return strings.Contains(f.Text, w) }) {
					t.Errorf("no finding mentions %q: %+v", w, found)
				}
			}
		})
	}
}

func TestRequireTagFindings(t *testing.T) {
	cases := []struct {
		name         string
		require      string
		tags         []string
		wantPatterns []string
		wantText     []string
	}{
		{name: "no tags", require: "v0.1.0"},
		{name: "only non-release tags", require: "v0.1.0", tags: []string{"redis/v0.2.0", "v1", "release-2"}},
		{name: "equal to highest", require: "v0.1.0", tags: []string{"v0.1.0", "redis/v0.1.0"}},
		{name: "above highest", require: "v0.1.1", tags: []string{"v0.1.0", "redis/v0.1.0"}},
		{name: "above highest with prerelease tag", require: "v0.2.0", tags: []string{"v0.1.0", "v0.3.0-rc.1"}},
		{name: "only a prerelease tag", require: "v0.1.0", tags: []string{"v0.2.0-rc.1"}},
		{name: "next release tagging in progress", require: "v0.1.1", tags: []string{"v0.1.0", "redis/v0.1.0", "v0.1.1"}},
		{name: "numeric order", require: "v0.10.0", tags: []string{"v0.9.0"}},
		{name: "behind", require: "v0.1.0", tags: []string{"v0.1.0", "redis/v0.1.0", "v0.2.0"},
			wantPatterns: []string{"require-behind"}, wantText: []string{"v0.1.0", "v0.2.0"}},
		{name: "prerelease below its release", require: "v0.1.0-rc.1", tags: []string{"v0.1.0"},
			wantPatterns: []string{"require-behind"}, wantText: []string{"v0.1.0-rc.1", "v0.1.0"}},
		{name: "numeric order behind", require: "v0.9.0", tags: []string{"v0.10.0"},
			wantPatterns: []string{"require-behind"}},
		{name: "pseudo-version", require: "v0.0.0-20260101000000-abcdefabcdef", tags: []string{"v0.1.0"},
			wantPatterns: []string{"require-behind"}},
		{name: "empty require", require: "", tags: []string{"v0.1.0"},
			wantPatterns: []string{"require-behind"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			found := RequireTagFindings(c.require, c.tags)
			if got := findingPatterns(found); !slices.Equal(got, c.wantPatterns) {
				t.Errorf("patterns = %q, want %q (findings %+v)", got, c.wantPatterns, found)
			}
			for _, f := range found {
				if f.Path != "redis/go.mod" {
					t.Errorf("finding %+v has path %q, want redis/go.mod", f, f.Path)
				}
			}
			for _, w := range c.wantText {
				if len(found) == 0 || !strings.Contains(found[0].Text, w) {
					t.Errorf("no finding text mentions %q: %+v", w, found)
				}
			}
		})
	}
}

func TestInstallFindings(t *testing.T) {
	const want = "github.com/ok0/go-microgateway/redis/cmd/microgateway"
	mainDirs := []string{"redis/cmd/microgateway"}
	cases := []struct {
		name         string
		readme       string
		dirs         []string
		wantPatterns []string
	}{
		{"latest", installReadme("go install " + want + "@latest"), mainDirs, nil},
		{"release", installReadme("go install " + want + "@v0.1.0"), mainDirs, nil},
		{"core path", installReadme("go install github.com/ok0/go-microgateway/cmd/microgateway@latest"), mainDirs, []string{"install-path"}},
		{"no install line", installReadme(), mainDirs, []string{"install-missing"}},
		{"major only", installReadme("go install " + want + "@v1"), mainDirs, []string{"install-version"}},
		{"no v prefix", installReadme("go install " + want + "@0.1.0"), mainDirs, []string{"install-version"}},
		{"pseudo version", installReadme("go install " + want + "@v0.1.0-0.20260101000000-abcdefabcdef"), mainDirs, []string{"install-version"}},
		{"empty version", installReadme("go install " + want + "@"), mainDirs, []string{"install-version"}},
		{"no version", installReadme("go install " + want), mainDirs, []string{"install-version"}},
		{"two valid lines", installReadme("go install "+want+"@latest", "go install "+want+"@v0.1.0"), mainDirs, []string{"install-multiple"}},
		{"no main package", installReadme("go install " + want + "@latest"), nil, []string{"install-main"}},
		{"two main packages", installReadme("go install " + want + "@latest"), []string{"redis/cmd/microgateway", "redis/cmd/other"}, []string{"install-main"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			found := InstallFindings([]byte(c.readme), c.dirs)
			if got := findingPatterns(found); !slices.Equal(got, c.wantPatterns) {
				t.Errorf("patterns = %q, want %q (findings %+v)", got, c.wantPatterns, found)
			}
			for _, f := range found {
				if f.Path != "README.md" {
					t.Errorf("finding %+v has path %q, want README.md", f, f.Path)
				}
			}
		})
	}
}
