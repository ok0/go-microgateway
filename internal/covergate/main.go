// Command covergate checks Go coverage profiles against the coverage gates of the repository.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

type block struct {
	stmts   int
	covered bool
}

type result struct {
	pkgs     map[string]float64
	modules  map[string]float64
	problems []string
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run executes covergate and returns its exit status. Write errors on stdout and stderr are ignored; the status is the result.
func run(args []string, stdout, stderr io.Writer) int {
	var modules []string
	fs := flag.NewFlagSet("covergate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	total := fs.Float64("total", 75, "minimum statement coverage percent for each module")
	algoMin := fs.Float64("algo-min", 85, "minimum statement coverage percent for each algorithm package")
	algo := fs.String("algo", "", "space-separated import paths of the algorithm packages")
	exclude := fs.String("exclude", "", "space-separated import paths left out of module totals")
	fs.Func("module", "module path; repeat for each module", func(s string) error {
		modules = append(modules, s)
		return nil
	})
	if err := fs.Parse(args); err != nil {
		return 2
	}
	files := fs.Args()
	if len(files) == 0 {
		_, _ = fmt.Fprintln(stderr, "usage: covergate [flags] profile...")
		fs.PrintDefaults()
		return 2
	}

	profiles := make([]map[string]map[string]block, 0, len(files))
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "covergate: %v\n", err)
			return 2
		}
		p, err := parseProfile(bytes.NewReader(data))
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "covergate: %s: %v\n", f, err)
			return 2
		}
		profiles = append(profiles, p)
	}

	res := gate(merge(profiles), strings.Fields(*algo), strings.Fields(*exclude), modules, *total, *algoMin)

	pkgNames := make([]string, 0, len(res.pkgs))
	for p := range res.pkgs {
		pkgNames = append(pkgNames, p)
	}
	sort.Strings(pkgNames)
	for _, p := range pkgNames {
		_, _ = fmt.Fprintf(stdout, "%-70s %6.1f%%\n", p, res.pkgs[p])
	}

	modNames := make([]string, 0, len(res.modules))
	for m := range res.modules {
		modNames = append(modNames, m)
	}
	sort.Strings(modNames)
	for _, m := range modNames {
		_, _ = fmt.Fprintf(stdout, "module %s total %.1f%%\n", m, res.modules[m])
	}

	for _, p := range res.problems {
		_, _ = fmt.Fprintln(stdout, "FAIL "+p)
	}
	if len(res.problems) > 0 {
		return 1
	}
	return 0
}

func parseProfile(r io.Reader) (map[string]map[string]block, error) {
	sc := bufio.NewScanner(r)
	profile := make(map[string]map[string]block)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		text := sc.Text()
		if lineNo == 1 {
			if !strings.HasPrefix(text, "mode: ") {
				return nil, fmt.Errorf("line 1: want \"mode: <x>\", got %q", text)
			}
			continue
		}
		fields := strings.Fields(text)
		if len(fields) != 3 {
			return nil, fmt.Errorf("line %d: want \"file:range numStmts count\", got %q", lineNo, text)
		}
		key := fields[0]
		idx := strings.LastIndex(key, ":")
		if idx <= 0 || !validRange(key[idx+1:]) {
			return nil, fmt.Errorf("line %d: malformed block %q", lineNo, key)
		}
		stmts, err := strconv.Atoi(fields[1])
		if err != nil || stmts < 0 {
			return nil, fmt.Errorf("line %d: malformed statement count %q", lineNo, fields[1])
		}
		count, err := strconv.Atoi(fields[2])
		if err != nil || count < 0 {
			return nil, fmt.Errorf("line %d: malformed count %q", lineNo, fields[2])
		}
		pkg := path.Dir(key[:idx])
		blocks := profile[pkg]
		if blocks == nil {
			blocks = make(map[string]block)
			profile[pkg] = blocks
		}
		if prev, dup := blocks[key]; dup {
			prev.covered = prev.covered || count > 0
			blocks[key] = prev
			continue
		}
		blocks[key] = block{stmts: stmts, covered: count > 0}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if lineNo == 0 {
		return nil, fmt.Errorf("empty profile")
	}
	return profile, nil
}

func validRange(rng string) bool {
	parts := strings.Split(rng, ",")
	if len(parts) != 2 {
		return false
	}
	for _, p := range parts {
		nums := strings.Split(p, ".")
		if len(nums) != 2 {
			return false
		}
		for _, n := range nums {
			if _, err := strconv.Atoi(n); err != nil {
				return false
			}
		}
	}
	return true
}

func merge(profiles []map[string]map[string]block) map[string]map[string]block {
	out := make(map[string]map[string]block)
	for _, p := range profiles {
		for pkg, blocks := range p {
			out[pkg] = blocks
		}
	}
	return out
}

func gate(cov map[string]map[string]block, algo, exclude, modules []string, total, algoMin float64) result {
	res := result{pkgs: make(map[string]float64), modules: make(map[string]float64)}
	excluded := make(map[string]bool, len(exclude))
	for _, e := range exclude {
		excluded[e] = true
	}

	type counts struct{ stmts, covered int }
	modCounts := make(map[string]*counts, len(modules))
	modHasPkg := make(map[string]bool, len(modules))
	for _, m := range modules {
		modCounts[m] = &counts{}
	}

	for pkg, blocks := range cov {
		stmts, covered := sumBlocks(blocks)
		res.pkgs[pkg] = percent(stmts, covered)
		m := moduleOf(pkg, modules)
		if m == "" {
			continue
		}
		modHasPkg[m] = true
		if excluded[pkg] {
			continue
		}
		modCounts[m].stmts += stmts
		modCounts[m].covered += covered
	}

	for _, m := range modules {
		if !modHasPkg[m] {
			res.problems = append(res.problems, fmt.Sprintf("module %s: no coverage data", m))
			continue
		}
		pct := percent(modCounts[m].stmts, modCounts[m].covered)
		res.modules[m] = pct
		if pct < total {
			res.problems = append(res.problems, fmt.Sprintf("module %s: total %.1f%% < %.1f%%", m, pct, total))
		}
	}

	for _, a := range algo {
		pct, ok := res.pkgs[a]
		if !ok {
			res.problems = append(res.problems, fmt.Sprintf("algorithm package %s: no coverage data", a))
			continue
		}
		if pct < algoMin {
			res.problems = append(res.problems, fmt.Sprintf("algorithm package %s: %.1f%% < %.1f%%", a, pct, algoMin))
		}
	}

	sort.Strings(res.problems)
	return res
}

func sumBlocks(blocks map[string]block) (int, int) {
	stmts, covered := 0, 0
	for _, b := range blocks {
		stmts += b.stmts
		if b.covered {
			covered += b.stmts
		}
	}
	return stmts, covered
}

func percent(stmts, covered int) float64 {
	if stmts == 0 {
		return 100
	}
	return float64(covered) / float64(stmts) * 100
}

func moduleOf(pkg string, modules []string) string {
	best := ""
	for _, m := range modules {
		if (pkg == m || strings.HasPrefix(pkg, m+"/")) && len(m) > len(best) {
			best = m
		}
	}
	return best
}
