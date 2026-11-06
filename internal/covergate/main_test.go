package main

import (
	"bytes"
	"strings"
	"testing"
)

const (
	testCore = "example.com/mod"
	testAlgo = "example.com/mod/internal/alg"
)

func profileOf(t *testing.T, s string) map[string]map[string]block {
	t.Helper()
	p, err := parseProfile(strings.NewReader(s))
	if err != nil {
		t.Fatalf("parseProfile: %v", err)
	}
	return p
}

func TestGatePasses(t *testing.T) {
	cov := profileOf(t, `mode: set
example.com/mod/a.go:1.1,2.2 10 1
example.com/mod/internal/alg/alg.go:1.1,2.2 10 1
example.com/mod/internal/alg/alg.go:3.1,4.2 10 1
`)
	res := gate(cov, []string{testAlgo}, nil, []string{testCore}, 75, 85)
	if len(res.problems) != 0 {
		t.Fatalf("problems = %v, want none", res.problems)
	}
	if res.pkgs[testAlgo] != 100 {
		t.Fatalf("algo package = %v, want 100", res.pkgs[testAlgo])
	}
	if res.modules[testCore] != 100 {
		t.Fatalf("module total = %v, want 100", res.modules[testCore])
	}
}

func TestGateAlgoBelow(t *testing.T) {
	cov := profileOf(t, `mode: set
example.com/mod/internal/alg/alg.go:1.1,2.2 849 1
example.com/mod/internal/alg/alg.go:3.1,4.2 151 0
`)
	res := gate(cov, []string{testAlgo}, nil, []string{testCore}, 75, 85)
	if len(res.problems) != 1 || !strings.Contains(res.problems[0], testAlgo) {
		t.Fatalf("problems = %v, want one problem naming %s", res.problems, testAlgo)
	}
}

func TestGateAlgoMissing(t *testing.T) {
	cov := profileOf(t, `mode: set
example.com/mod/a.go:1.1,2.2 10 1
`)
	res := gate(cov, []string{testAlgo}, nil, []string{testCore}, 75, 85)
	if len(res.problems) != 1 || !strings.Contains(res.problems[0], "no coverage data") {
		t.Fatalf("problems = %v, want one no-coverage-data problem", res.problems)
	}
}

func TestGateModuleTotalBelow(t *testing.T) {
	cov := profileOf(t, `mode: set
example.com/mod/a.go:1.1,2.2 74 1
example.com/mod/a.go:3.1,4.2 26 0
`)
	res := gate(cov, nil, nil, []string{testCore}, 75, 85)
	if len(res.problems) != 1 || !strings.Contains(res.problems[0], testCore) {
		t.Fatalf("problems = %v, want one module total problem", res.problems)
	}
}

func TestGateExcludedNotInTotal(t *testing.T) {
	cov := profileOf(t, `mode: set
example.com/mod/a.go:1.1,2.2 10 1
example.com/mod/internal/testcert/c.go:1.1,2.2 90 0
`)
	res := gate(cov, nil, []string{"example.com/mod/internal/testcert"}, []string{testCore}, 75, 85)
	if len(res.problems) != 0 {
		t.Fatalf("problems = %v, want none", res.problems)
	}
	if res.modules[testCore] != 100 {
		t.Fatalf("module total = %v, want 100", res.modules[testCore])
	}
}

func TestMergeLaterProfileWins(t *testing.T) {
	first := profileOf(t, `mode: set
example.com/mod/a.go:1.1,2.2 10 1
example.com/mod/a.go:3.1,4.2 90 0
`)
	second := profileOf(t, `mode: set
example.com/mod/a.go:1.1,2.2 10 0
example.com/mod/a.go:3.1,4.2 90 1
`)
	merged := merge([]map[string]map[string]block{first, second})
	res := gate(merged, nil, nil, nil, 0, 0)
	if res.pkgs[testCore] != 90 {
		t.Fatalf("root package = %v, want 90", res.pkgs[testCore])
	}
}

func TestParseDuplicateBlocks(t *testing.T) {
	cov := profileOf(t, `mode: count
example.com/mod/a.go:1.1,2.2 2 0
example.com/mod/a.go:1.1,2.2 2 3
`)
	blocks := cov[testCore]
	if len(blocks) != 1 {
		t.Fatalf("blocks = %v, want one block", blocks)
	}
	b := blocks["example.com/mod/a.go:1.1,2.2"]
	if b.stmts != 2 || !b.covered {
		t.Fatalf("block = %+v, want stmts 2 covered", b)
	}
	res := gate(cov, nil, nil, nil, 0, 0)
	if res.pkgs[testCore] != 100 {
		t.Fatalf("package = %v, want 100", res.pkgs[testCore])
	}
}

func TestParseMalformed(t *testing.T) {
	cases := map[string]string{
		"missing mode":    "example.com/mod/a.go:1.1,2.2 1 1\n",
		"too few fields":  "mode: set\nexample.com/mod/a.go:1.1,2.2 1\n",
		"bad block range": "mode: set\nexample.com/mod/a.go:1-1 1 1\n",
		"no file colon":   "mode: set\nexample.com/mod/a.go 1 1\n",
		"bad statements":  "mode: set\nexample.com/mod/a.go:1.1,2.2 x 1\n",
		"bad count":       "mode: set\nexample.com/mod/a.go:1.1,2.2 1 y\n",
		"empty input":     "",
	}
	for name, src := range cases {
		if _, err := parseProfile(strings.NewReader(src)); err == nil {
			t.Errorf("%s: parseProfile returned no error", name)
		}
	}
}

func TestParseMalformedReportsLine(t *testing.T) {
	_, err := parseProfile(bytes.NewReader([]byte("mode: set\nbroken\n")))
	if err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("err = %v, want an error naming line 2", err)
	}
}
