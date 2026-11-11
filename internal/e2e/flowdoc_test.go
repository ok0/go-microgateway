package e2e

import (
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/internal/doccheck"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/middleware"
	"github.com/ok0/go-microgateway/internal/proxy"
	"github.com/ok0/go-microgateway/internal/runtime"
)

func flowDocSpec() doccheck.FlowSpec {
	return doccheck.FlowSpec{
		Flow:      runtime.Flow(),
		Stages:    middleware.DefaultStages,
		Terminal:  runtime.StepProxy,
		RetryFrom: proxy.StepUpstream,
		RetryTo:   proxy.StepLoadBalancer,
		Errors: map[string]string{
			runtime.StepCanonicalPath: strconv.Itoa(http.StatusBadRequest) + " " + httperr.CodeInvalidPath,
			runtime.StepRouteMatch:    strconv.Itoa(http.StatusNotFound) + " " + httperr.CodeRouteNotFound,
		},
	}
}

func archDocSource(t *testing.T) (string, []byte) {
	t.Helper()
	root, err := doccheck.Root()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "docs", "architecture.md")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, src
}

func TestRequestFlowMatchesDocs(t *testing.T) {
	path, src := archDocSource(t)
	for _, f := range doccheck.FlowDocFindings(doccheck.Parse(path, src), flowDocSpec()) {
		t.Error(f)
	}
}

func TestRequestFlowMatchesDocsRejects(t *testing.T) {
	path, src := archDocSource(t)
	text := string(src)
	const flowStart = "flowchart TD\n  client["
	const fence = "```mermaid\n"
	at := strings.Index(text, flowStart)
	if at < 0 {
		t.Fatalf("architecture document lacks %q", flowStart)
	}
	open := strings.LastIndex(text[:at], fence)
	if open < 0 {
		t.Fatalf("architecture document has no %q before %q", fence, flowStart)
	}
	diagramOld := text[open : at+len(flowStart)]
	diagramNew := "```text\n" + text[open+len(fence):at+len(flowStart)]
	cases := []struct{ name, old, new, want string }{
		{
			"node order",
			"  timeout --> auth[\"auth, if configured\"]\n  auth --> rate_limit[\"rate_limit, if configured\"]\n  rate_limit --> cache[",
			"  timeout --> rate_limit[\"rate_limit, if configured\"]\n  rate_limit --> auth[\"auth, if configured\"]\n  auth --> cache[",
			"request-flow main path",
		},
		{"if configured", "cache[\"cache, if configured\"]", "cache[\"cache\"]", "lacks \"if configured\""},
		{"retry edge", "\n  upstream -- \"retry when the attempt fails and retries remain\" --> load_balancer", "", "no retry edge"},
		{"retry label", "-- \"retry when the attempt fails and retries remain\" -->", "-- \"again when the attempt fails\" -->", "no retry edge"},
		{"invalid path exit", "\n  canonical_path -- \"no\" --> bad_path[\"400 invalid_path\"]", "", "no error exit from \"canonical_path\""},
		{"not found code", "404 route_not_found", "404 not_found", "no error exit from \"route_match\""},
		{
			"list order",
			"`load_balancer` picks a host for the attempt, and `upstream` sends",
			"`upstream` picks a host for the attempt, and `load_balancer` sends",
			"numbered list of \"Request flow\"",
		},
		{"pipeline order", "4. `cache`\n5. `circuit_breaker`", "4. `circuit_breaker`\n5. `cache`", "numbered list of \"Default pipeline order\""},
		{"pipeline terminal", "7. `proxy`", "7. the proxy", "numbered list of \"Default pipeline order\""},
		{"no diagram", diagramOld, diagramNew, "want one mermaid diagram under \"Request flow\""},
	}
	for _, c := range cases {
		if !strings.Contains(text, c.old) {
			t.Fatalf("%s: architecture document lacks %q", c.name, c.old)
		}
		findings := doccheck.FlowDocFindings(doccheck.Parse(path, []byte(strings.Replace(text, c.old, c.new, 1))), flowDocSpec())
		found := false
		for _, f := range findings {
			if strings.Contains(f, c.want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s: no finding contains %q; findings %v", c.name, c.want, findings)
		}
	}
}
