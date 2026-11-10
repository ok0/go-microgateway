package doccheck

import (
	"bytes"
	"fmt"
	"maps"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

const releaseImage = "ghcr.io/ok0/go-microgateway"

var releaseActions = []string{"actions/checkout@v7", "docker/setup-buildx-action@v4", "docker/login-action@v4", "docker/build-push-action@v7", "docker/setup-qemu-action@v4"}

var releaseSecretRE = regexp.MustCompile(`secrets\s*(?:\.\s*([A-Za-z_][A-Za-z0-9_-]*)|\[)`)

var releaseImageRE = regexp.MustCompile(`ghcr\.io/[A-Za-z0-9._/-]+`)

var releaseTagsRE = regexp.MustCompile(`^\$\{\{\s*steps\.([A-Za-z0-9_-]+)\.outputs\.([A-Za-z0-9_-]+)\s*\}\}$`)

func imageTagsProblems(stdout, stderr string, status int, want []string) []string {
	var problems []string
	if want != nil {
		if status != 0 {
			problems = append(problems, fmt.Sprintf("exit status %d, want 0", status))
		}
		if exp := strings.Join(want, "\n") + "\n"; stdout != exp {
			problems = append(problems, fmt.Sprintf("output %q, want %q", stdout, exp))
		}
		if stderr != "" {
			problems = append(problems, fmt.Sprintf("stderr %q, want empty", stderr))
		}
		return problems
	}
	if status != 1 {
		problems = append(problems, fmt.Sprintf("exit status %d, want 1", status))
	}
	if stdout != "" {
		problems = append(problems, fmt.Sprintf("printed %q for a refused tag", stdout))
	}
	if strings.TrimSpace(stderr) == "" {
		problems = append(problems, "no message on stderr for a refused tag")
	}
	return problems
}

func TestImageTags(t *testing.T) {
	requireTool(t, "sh")
	root := repoRoot(t)
	cases := []struct {
		name string
		args []string
		want []string
	}{
		{"release", []string{"v0.2.0"}, []string{"0.2.0", "0.2", "latest"}},
		{"two-digit minor", []string{"v1.10.3"}, []string{"1.10.3", "1.10", "latest"}},
		{"prerelease", []string{"v0.2.0-rc.1"}, []string{"0.2.0-rc.1"}},
		{"redis tag", []string{"redis/v0.2.0"}, nil},
		{"no v", []string{"0.2.0"}, nil},
		{"no patch", []string{"v0.2"}, nil},
		{"build part", []string{"v0.2.0+meta"}, nil},
		{"empty", []string{""}, nil},
		{"missing", nil, nil},
		{"two arguments", []string{"v0.2.0", "v0.3.0"}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cmd := exec.Command("sh", append([]string{"scripts/image-tags.sh"}, c.args...)...)
			cmd.Dir = root
			cmd.Env = scrubbedEnv()
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			status := exitStatus(t, cmd.Run())
			for _, p := range imageTagsProblems(stdout.String(), stderr.String(), status, c.want) {
				t.Error(p)
			}
		})
	}
}

func TestImageTagsProblems(t *testing.T) {
	ok3 := []string{"0.2.0", "0.2", "latest"}
	cases := []struct {
		name    string
		stdout  string
		stderr  string
		status  int
		want    []string
		wantSub string
	}{
		{"clean", "0.2.0\n0.2\nlatest\n", "", 0, ok3, ""},
		{"missing line", "0.2.0\nlatest\n", "", 0, ok3, "output"},
		{"prerelease with minor", "0.2.0-rc.1\n0.2\nlatest\n", "", 0, []string{"0.2.0-rc.1"}, "output"},
		{"failed exit", "0.2.0\n0.2\nlatest\n", "", 1, ok3, "want 0"},
		{"refused without status", "", "", 0, nil, "want 1"},
		{"refused with other status", "", "bad\n", 2, nil, "want 1"},
		{"refused silently", "", "", 1, nil, "stderr"},
		{"refused with output", "0.2.0\n", "bad\n", 1, nil, "printed"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			problems := imageTagsProblems(c.stdout, c.stderr, c.status, c.want)
			if c.wantSub == "" {
				for _, p := range problems {
					t.Error(p)
				}
				return
			}
			if !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, c.wantSub) }) {
				t.Errorf("no problem contains %q: %v", c.wantSub, problems)
			}
		})
	}
}

func TestImageTagsScript(t *testing.T) {
	src := string(readAt(t, repoRoot(t), "scripts/image-tags.sh"))
	for _, want := range []string{"#!/bin/sh", "set -eu", "check_version"} {
		if !strings.Contains(src, want) {
			t.Errorf("scripts/image-tags.sh does not contain %q", want)
		}
	}
	for _, banned := range []string{"curl", "wget", "docker", "git ", "nc "} {
		if strings.Contains(src, banned) {
			t.Errorf("scripts/image-tags.sh contains %q; the script must stay offline and docker-free", banned)
		}
	}
}

func releaseKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}

func releaseStepUses(raw any) string {
	uses, _ := mapOf(raw)["uses"].(string)
	return uses
}

func releaseRun(step map[string]any) string {
	run, _ := step["run"].(string)
	return run
}

func releaseRunHas(run string, parts ...string) bool {
	return !slices.ContainsFunc(parts, func(p string) bool { return !strings.Contains(run, p) })
}

func releaseRunAfter(steps []any, from int, parts ...string) bool {
	for i := from + 1; i < len(steps); i++ {
		if releaseRunHas(releaseRun(mapOf(steps[i])), parts...) {
			return true
		}
	}
	return false
}

func releaseWorkflowProblems(src []byte) []string {
	var wf map[string]any
	if err := yaml.Unmarshal(src, &wf); err != nil {
		return []string{"release.yml: " + err.Error()}
	}
	var problems []string

	on, isMap := wf["on"].(map[string]any)
	if !isMap || !slices.Equal(releaseKeys(on), []string{"push"}) {
		triggers := fmt.Sprint(wf["on"])
		if isMap {
			triggers = strings.Join(releaseKeys(on), ", ")
		}
		problems = append(problems, fmt.Sprintf("release workflow triggers on %q, want only a push of tags", triggers))
	}
	push := mapOf(on["push"])
	if !slices.Equal(releaseKeys(push), []string{"tags"}) {
		problems = append(problems, fmt.Sprintf("push trigger has %q, want only tags", strings.Join(releaseKeys(push), ", ")))
	}
	if !slices.Equal(listOf(push["tags"]), []any{"v*"}) {
		problems = append(problems, fmt.Sprintf("push trigger tags = %v, want [v*]", push["tags"]))
	}

	gotPerms := map[string]string{}
	for k, v := range mapOf(wf["permissions"]) {
		gotPerms[k], _ = v.(string)
	}
	if !maps.Equal(gotPerms, map[string]string{"contents": "read", "packages": "write"}) {
		problems = append(problems, fmt.Sprintf("top-level permissions = %v, want exactly contents: read and packages: write", wf["permissions"]))
	}

	for _, m := range releaseSecretRE.FindAllStringSubmatch(string(src), -1) {
		name := m[1]
		if name == "" {
			name = "["
		}
		if name != "GITHUB_TOKEN" {
			problems = append(problems, fmt.Sprintf("release workflow references secret %q; only GITHUB_TOKEN is allowed", name))
		}
	}

	jobs := mapOf(wf["jobs"])
	var steps []any
	for _, name := range releaseKeys(jobs) {
		job := mapOf(jobs[name])
		if _, ok := job["permissions"]; ok {
			problems = append(problems, fmt.Sprintf("job %s sets permissions; only the top-level contents: read and packages: write are allowed", name))
		}
		if v, ok := job["continue-on-error"]; ok && !isFalse(v) {
			problems = append(problems, fmt.Sprintf("job %s sets continue-on-error", name))
		}
		jobSteps := listOf(job["steps"])
		for _, raw := range jobSteps {
			if v, ok := mapOf(raw)["continue-on-error"]; ok && !isFalse(v) {
				problems = append(problems, fmt.Sprintf("job %s sets continue-on-error", name))
			}
		}
		for _, f := range checkoutFindings(jobSteps) {
			problems = append(problems, fmt.Sprintf("job %s: %s", name, f))
		}
		steps = append(steps, jobSteps...)
	}
	for _, raw := range steps {
		if uses := releaseStepUses(raw); uses != "" && !slices.Contains(releaseActions, uses) {
			problems = append(problems, fmt.Sprintf("release workflow uses %q, which is not an allowed action", uses))
		}
	}
	for _, want := range releaseActions {
		if !slices.ContainsFunc(steps, func(raw any) bool { return releaseStepUses(raw) == want }) {
			problems = append(problems, fmt.Sprintf("release workflow has no step using %s", want))
		}
	}

	login := slices.ContainsFunc(steps, func(raw any) bool {
		if releaseStepUses(raw) != "docker/login-action@v4" {
			return false
		}
		with := mapOf(mapOf(raw)["with"])
		return with["registry"] == any("ghcr.io") && with["username"] == any("${{ github.actor }}") && with["password"] == any("${{ secrets.GITHUB_TOKEN }}")
	})
	if !login {
		problems = append(problems, "login step does not log in to ghcr.io as github.actor with secrets.GITHUB_TOKEN")
	}

	if image := mapOf(wf["env"])["IMAGE"]; image != any(releaseImage) {
		problems = append(problems, fmt.Sprintf("workflow env IMAGE = %v, want image %s", image, releaseImage))
	}
	for _, img := range releaseImageRE.FindAllString(string(src), -1) {
		if img != releaseImage {
			problems = append(problems, fmt.Sprintf("release workflow names image %q, want only %s", img, releaseImage))
		}
	}

	var builds []int
	for i, raw := range steps {
		if releaseStepUses(raw) == "docker/build-push-action@v7" {
			builds = append(builds, i)
		}
	}
	if len(builds) != 1 {
		return append(problems, fmt.Sprintf("release workflow has %d build-push steps, want 1", len(builds)))
	}
	b := builds[0]
	with := mapOf(mapOf(steps[b])["with"])
	if with["context"] != any(".") {
		problems = append(problems, fmt.Sprintf("build step context is %v, want .", with["context"]))
	}
	if with["platforms"] != any("linux/amd64,linux/arm64") {
		problems = append(problems, fmt.Sprintf("build step platforms = %v, want linux/amd64,linux/arm64", with["platforms"]))
	}
	if push := with["push"]; push != any(true) && push != any("true") {
		problems = append(problems, "build step does not set push: true")
	}
	args, _ := with["build-args"].(string)
	var lines []string
	for _, line := range strings.Split(args, "\n") {
		lines = append(lines, strings.TrimSpace(line))
	}
	hasPrefix := func(prefix string) bool {
		return slices.ContainsFunc(lines, func(l string) bool { return strings.HasPrefix(l, prefix) })
	}
	if !slices.Contains(lines, "VERSION=${{ github.ref_name }}") || !hasPrefix("COMMIT=") || !hasPrefix("DATE=") {
		problems = append(problems, "build step build-args lack VERSION=${{ github.ref_name }}, COMMIT or DATE")
	}
	for i, raw := range steps {
		if uses := releaseStepUses(raw); (uses == "docker/setup-buildx-action@v4" || uses == "docker/login-action@v4") && i > b {
			problems = append(problems, fmt.Sprintf("%s does not precede the build step", uses))
		}
	}

	tags, _ := with["tags"].(string)
	tagsOK := false
	if m := releaseTagsRE.FindStringSubmatch(tags); m != nil {
		for j := range b {
			step := mapOf(steps[j])
			if step["id"] == any(m[1]) {
				tagsOK = releaseRunHas(releaseRun(step), `sh scripts/image-tags.sh "$GITHUB_REF_NAME"`, "$IMAGE", m[2]+"<<", "$GITHUB_OUTPUT")
			}
		}
	}
	if !tagsOK {
		problems = append(problems, "build step tags are not the output of a step running scripts/image-tags.sh")
	}

	q := -1
	for i, raw := range steps {
		if releaseStepUses(raw) == "docker/setup-qemu-action@v4" {
			q = i
			break
		}
	}
	if q >= 0 && q < b {
		problems = append(problems, "QEMU is set up before the build; emulation is only for the checks after the push")
	}
	if q < 0 {
		q = len(steps)
	}

	if !releaseRunAfter(steps, b, "docker buildx imagetools inspect", "linux/amd64", "linux/arm64") {
		problems = append(problems, "no step after the push runs docker buildx imagetools inspect requiring linux/amd64 and linux/arm64")
	}
	if !releaseRunAfter(steps, q, "linux/amd64", "linux/arm64", "docker run --rm --platform", "$IMAGE", " version", "microgateway $GITHUB_REF_NAME ") {
		problems = append(problems, "no step after the push and QEMU runs version from the pushed image for each platform and compares it with $GITHUB_REF_NAME")
	}
	if !releaseRunAfter(steps, b, "linux/amd64", "linux/arm64", "$IMAGE", "{{.Config.User}}", "65532:65532") {
		problems = append(problems, "no step after the push checks that each platform's image user is 65532:65532")
	}
	return problems
}

func TestReleaseWorkflow(t *testing.T) {
	for _, p := range releaseWorkflowProblems(readAt(t, repoRoot(t), ".github/workflows/release.yml")) {
		t.Error(p)
	}
}

type releaseMutation func(*testing.T, string) string

func swapOnce(old, with string) releaseMutation {
	return func(t *testing.T, src string) string {
		return replaceOnce(t, src, old, with)
	}
}

func TestReleaseWorkflowRules(t *testing.T) {
	src := string(readAt(t, repoRoot(t), ".github/workflows/release.yml"))
	cases := []struct {
		name   string
		mutate releaseMutation
		want   string
	}{
		{"real", nil, ""},
		{"manual dispatch", swapOnce("on:\n  push:\n", "on:\n  workflow_dispatch:\n  push:\n"), "triggers on"},
		{"pull request", swapOnce("on:\n  push:\n", "on:\n  pull_request:\n  push:\n"), "triggers on"},
		{"branch push", swapOnce("    tags: [\"v*\"]\n", "    branches: [main]\n    tags: [\"v*\"]\n"), "only tags"},
		{"every tag", swapOnce("tags: [\"v*\"]", "tags: [\"*\"]"), "want [v*]"},
		{"widened permissions", swapOnce("  packages: write\n", "  packages: write\n  id-token: write\n"), "top-level permissions"},
		{"write-all", swapOnce("permissions:\n  contents: read\n  packages: write\n", "permissions: write-all\n"), "top-level permissions"},
		{"job permissions", swapOnce("    runs-on: ubuntu-latest\n", "    runs-on: ubuntu-latest\n    permissions:\n      contents: write\n"), "sets permissions"},
		{"foreign secret", swapOnce("secrets.GITHUB_TOKEN", "secrets.GHCR_TOKEN"), "secret \"GHCR_TOKEN\""},
		{"foreign username", swapOnce("username: ${{ github.actor }}", "username: ok0"), "login step"},
		{"missing platform", swapOnce("platforms: linux/amd64,linux/arm64", "platforms: linux/amd64"), "build step platforms"},
		{"missing push", swapOnce("push: true", "push: false"), "push: true"},
		{"wrong image", swapOnce("IMAGE: ghcr.io/ok0/go-microgateway", "IMAGE: ghcr.io/ok0/other"), "image"},
		{"tags not from the script", swapOnce("tags: ${{ steps.tags.outputs.list }}", "tags: ghcr.io/ok0/go-microgateway:latest"), "scripts/image-tags.sh"},
		{"script not run", swapOnce("sh scripts/image-tags.sh \"$GITHUB_REF_NAME\"", "echo \"$GITHUB_REF_NAME\""), "scripts/image-tags.sh"},
		{"missing version check", swapOnce("\"$IMAGE@$digest\" version)", "\"$IMAGE@$digest\" help)"), "runs version"},
		{"version not compared", swapOnce("\"microgateway $GITHUB_REF_NAME \"*)", "\"microgateway \"*)"), "runs version"},
		{"one platform checked", swapOnce("for platform in linux/amd64 linux/arm64", "for platform in linux/amd64"), "runs version"},
		{"missing inspect step", swapOnce("docker buildx imagetools inspect", "docker manifest inspect"), "imagetools inspect"},
		{"inspect one platform", swapOnce("for want in linux/amd64 linux/arm64", "for want in linux/amd64"), "imagetools inspect"},
		{"missing user check", swapOnce("{{.Config.User}}", "{{.Config.Cmd}}"), "65532:65532"},
		{"missing full-history checkout", swapOnce("fetch-depth: 0", "fetch-depth: 1"), "fetch-depth: 0"},
		{"foreign action", swapOnce("docker/build-push-action@v7", "docker/build-push-action@v6"), "not an allowed action"},
		{"continue on error", swapOnce("      - name: manifest platforms\n", "      - name: manifest platforms\n        continue-on-error: true\n"), "continue-on-error"},
		{"qemu before build", func(t *testing.T, s string) string {
			s = replaceOnce(t, s, "      - uses: docker/setup-qemu-action@v4\n        with:\n          platforms: arm64\n", "")
			return replaceOnce(t, s, "      - uses: docker/build-push-action@v7\n", "      - uses: docker/setup-qemu-action@v4\n        with:\n          platforms: arm64\n      - uses: docker/build-push-action@v7\n")
		}, "QEMU is set up before the build"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mutated := src
			if c.mutate != nil {
				mutated = c.mutate(t, src)
			}
			problems := releaseWorkflowProblems([]byte(mutated))
			if c.want == "" {
				for _, p := range problems {
					t.Error(p)
				}
				return
			}
			if !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, c.want) }) {
				t.Errorf("no problem contains %q: %v", c.want, problems)
			}
		})
	}
}
