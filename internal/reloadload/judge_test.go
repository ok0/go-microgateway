package main

import (
	"slices"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

var (
	bA  = Behavior{Status: 200, Backend: "A"}
	bB  = Behavior{Status: 200, Backend: "B"}
	bNF = Behavior{Status: 404}
)

func series(inst string, from, to time.Time, every time.Duration, path string, b Behavior) []Record {
	var out []Record
	for s := from; s.Before(to); s = s.Add(every) {
		out = append(out, Record{Instance: inst, Path: path, Start: s, End: s.Add(time.Millisecond), Status: b.Status, Backend: b.Backend})
	}
	return out
}

func one(path string, start, end time.Time, b Behavior) Record {
	return Record{Instance: "i", Path: path, Start: start, End: end, Status: b.Status, Backend: b.Backend}
}

func weightSpec() StepSpec {
	return StepSpec{
		Name:    "weight",
		Old:     map[string]Behavior{"/svc": bA, "/new": bA},
		New:     map[string]Behavior{"/svc": bB, "/new": bB},
		Changes: true,
		Success: 1,
		Failure: 0,
	}
}

func judgeTimes() (s, tr time.Time) {
	return t0.Add(240 * time.Millisecond), t0.Add(250 * time.Millisecond)
}

func windowEnd() time.Time {
	return t0.Add(2500 * time.Millisecond)
}

// validWeightRun has the /svc and /new paths answer A before effect and B from effect on, 10 ms apart.
func validWeightRun(effect time.Time) []Record {
	var recs []Record
	for _, p := range []string{"/svc", "/new"} {
		recs = append(recs, series("i", t0, effect, 10*time.Millisecond, p, bA)...)
		recs = append(recs, series("i", effect, windowEnd(), 10*time.Millisecond, p, bB)...)
	}
	return recs
}

var (
	okCounts   = Counts{Success: 1}
	zeroCounts = Counts{}
)

func hasProblem(res StepResult, sub string) bool {
	for _, p := range res.Problems {
		if strings.Contains(p, sub) {
			return true
		}
	}
	return false
}

func wantNoProblems(t *testing.T, res StepResult) {
	t.Helper()
	if len(res.Problems) != 0 {
		t.Fatalf("problems = %q, want none", res.Problems)
	}
}

func TestJudgeStepEffectLatency(t *testing.T) {
	s, tr := judgeTimes()
	spec := weightSpec()

	res := judgeStep(spec, s, tr, validWeightRun(tr.Add(1900*time.Millisecond)), zeroCounts, okCounts)
	wantNoProblems(t, res)
	if res.Latency != 1900*time.Millisecond || !res.HasLatency {
		t.Fatalf("latency = %v (has %v), want 1.9s", res.Latency, res.HasLatency)
	}

	res = judgeStep(spec, s, tr, validWeightRun(tr.Add(2100*time.Millisecond)), zeroCounts, okCounts)
	if len(res.Problems) != 1 || !hasProblem(res, "exceeds 2s") {
		t.Fatalf("problems = %q, want only the latency problem", res.Problems)
	}
}

func TestJudgeStepTransportError(t *testing.T) {
	s, tr := judgeTimes()
	recs := validWeightRun(tr.Add(300 * time.Millisecond))
	recs = append(recs, Record{Instance: "i", Path: "/svc", Start: t0.Add(2000 * time.Millisecond), End: t0.Add(2001 * time.Millisecond), Err: "refused"})

	res := judgeStep(weightSpec(), s, tr, recs, zeroCounts, okCounts)
	if res.Failures != 1 || !hasProblem(res, "failed requests") {
		t.Fatalf("failures = %d, problems = %q", res.Failures, res.Problems)
	}
}

func TestJudgeStepBadGateway(t *testing.T) {
	s, tr := judgeTimes()
	recs := validWeightRun(tr.Add(300 * time.Millisecond))
	recs = append(recs, one("/svc", t0.Add(2000*time.Millisecond), t0.Add(2001*time.Millisecond), Behavior{Status: 502}))

	res := judgeStep(weightSpec(), s, tr, recs, zeroCounts, okCounts)
	if res.Failures != 1 {
		t.Fatalf("failures = %d, want 1 (problems %q)", res.Failures, res.Problems)
	}
}

func TestJudgeStepOldAfterEffect(t *testing.T) {
	s, tr := judgeTimes()
	recs := validWeightRun(tr.Add(300 * time.Millisecond))
	recs = append(recs, one("/svc", t0.Add(2450*time.Millisecond), t0.Add(2451*time.Millisecond), bA))

	res := judgeStep(weightSpec(), s, tr, recs, zeroCounts, okCounts)
	if len(res.Problems) == 0 || (!hasProblem(res, "only") && !hasProblem(res, "exceeds")) {
		t.Fatalf("problems = %q, want a too-few or latency problem", res.Problems)
	}
	if res.Latency < 2200*time.Millisecond {
		t.Fatalf("latency = %v, want at least 2.2s so the old record falls before the effect point", res.Latency)
	}
}

func TestJudgeStepNewBeforeTriggerStart(t *testing.T) {
	s, tr := judgeTimes()
	recs := validWeightRun(tr.Add(300 * time.Millisecond))
	recs = append(recs, one("/svc", s.Add(-3*time.Millisecond), s.Add(-time.Millisecond), bB))

	res := judgeStep(weightSpec(), s, tr, recs, zeroCounts, okCounts)
	if len(res.Problems) != 1 || !hasProblem(res, "1 requests completed before the trigger started show the new behavior") {
		t.Fatalf("problems = %q, want exactly the early-new problem", res.Problems)
	}
	if res.Failures != 0 {
		t.Fatalf("failures = %d, want 0", res.Failures)
	}
}

func TestJudgeStepOverlap(t *testing.T) {
	s, tr := judgeTimes()
	cases := []struct {
		name    string
		rec     Record
		failure bool
	}{
		{"new completed exactly at S", one("/svc", s.Add(-5*time.Millisecond), s, bB), false},
		{"new overlapping S and T", one("/svc", s.Add(-time.Millisecond), tr.Add(5*time.Millisecond), bB), false},
		{"new spanning T", one("/svc", tr.Add(-time.Millisecond), tr.Add(time.Millisecond), bB), false},
		{"old overlapping S and T", one("/svc", s.Add(2*time.Millisecond), s.Add(4*time.Millisecond), bA), false},
		{"neither overlapping S and T", one("/svc", s.Add(2*time.Millisecond), s.Add(4*time.Millisecond), Behavior{Status: 502}), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			recs := validWeightRun(tr.Add(300 * time.Millisecond))
			recs = append(recs, c.rec)
			res := judgeStep(weightSpec(), s, tr, recs, zeroCounts, okCounts)
			if c.failure {
				if res.Failures != 1 || !hasProblem(res, "failed requests") || hasProblem(res, "trigger started") {
					t.Fatalf("failures = %d, problems = %q", res.Failures, res.Problems)
				}
				return
			}
			wantNoProblems(t, res)
		})
	}
}

func TestJudgeStepTriggerTimes(t *testing.T) {
	_, tr := judgeTimes()
	spec := weightSpec()
	recs := validWeightRun(tr.Add(300 * time.Millisecond))

	res := judgeStep(spec, time.Time{}, tr, recs, zeroCounts, okCounts)
	if !hasProblem(res, "invalid trigger times") {
		t.Fatalf("zero start: problems = %q", res.Problems)
	}
	res = judgeStep(spec, tr.Add(time.Millisecond), tr, recs, zeroCounts, okCounts)
	if !hasProblem(res, "invalid trigger times") {
		t.Fatalf("start after completion: problems = %q", res.Problems)
	}
	res = judgeStep(spec, tr, tr, recs, zeroCounts, okCounts)
	wantNoProblems(t, res)
}

func TestJudgeStepTooFewAfterEffect(t *testing.T) {
	s, tr := judgeTimes()
	effect := tr.Add(300 * time.Millisecond)
	var recs []Record
	recs = append(recs, series("i", t0, effect, 10*time.Millisecond, "/svc", bA)...)
	recs = append(recs, series("i", effect, windowEnd(), 10*time.Millisecond, "/svc", bB)...)
	recs = append(recs, series("i", t0, effect, 10*time.Millisecond, "/new", bA)...)
	recs = append(recs, series("i", effect, effect.Add(9*10*time.Millisecond), 10*time.Millisecond, "/new", bB)...)

	res := judgeStep(weightSpec(), s, tr, recs, zeroCounts, okCounts)
	if !hasProblem(res, "only 9 requests to /new") {
		t.Fatalf("problems = %q, want the too-few problem for /new", res.Problems)
	}
}

func TestJudgeStepInvalidWindow(t *testing.T) {
	s, tr := judgeTimes()
	spec := StepSpec{
		Name:    "invalid",
		Old:     map[string]Behavior{"/svc": bB, "/new": bB},
		New:     map[string]Behavior{"/svc": bB, "/new": bB},
		Failure: 1,
	}
	allB := validWeightRun(t0)
	for i := range allB {
		allB[i].Status, allB[i].Backend = bB.Status, bB.Backend
	}
	cases := []struct {
		name    string
		before  Counts
		after   Counts
		recs    func() []Record
		problem string
	}{
		{"valid run", Counts{Success: 0, Failure: 0}, Counts{Success: 0, Failure: 1}, func() []Record { return allB }, ""},
		{"old answer after trigger", Counts{}, Counts{Failure: 1}, func() []Record {
			return append(append([]Record{}, allB...), one("/svc", tr.Add(100*time.Millisecond), tr.Add(101*time.Millisecond), bA))
		}, "failed requests"},
		{"failure delta 0", Counts{}, Counts{}, func() []Record { return allB }, "failure delta 0, want 1"},
		{"failure delta 2", Counts{}, Counts{Failure: 2}, func() []Record { return allB }, "failure delta 2, want 1"},
		{"success delta 1", Counts{}, Counts{Success: 1, Failure: 1}, func() []Record { return allB }, "success delta 1, want 0"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := judgeStep(spec, s, tr, c.recs(), c.before, c.after)
			if c.problem == "" {
				wantNoProblems(t, res)
				return
			}
			if !hasProblem(res, c.problem) {
				t.Fatalf("problems = %q, want one containing %q", res.Problems, c.problem)
			}
		})
	}
}

func TestJudgeStepRouteNotFoundBeforeEffect(t *testing.T) {
	s, tr := judgeTimes()
	spec := StepSpec{
		Name:    "route",
		Old:     map[string]Behavior{"/svc": bA, "/new": bNF},
		New:     map[string]Behavior{"/svc": bA, "/new": bA},
		Changes: true,
		Success: 1,
	}
	effect := tr.Add(400 * time.Millisecond)
	var recs []Record
	recs = append(recs, series("i", t0, windowEnd(), 10*time.Millisecond, "/svc", bA)...)
	recs = append(recs, series("i", t0, effect, 10*time.Millisecond, "/new", bNF)...)
	recs = append(recs, series("i", effect, windowEnd(), 10*time.Millisecond, "/new", bA)...)

	res := judgeStep(spec, s, tr, recs, zeroCounts, okCounts)
	wantNoProblems(t, res)
}

func TestJudgeStepBaseline(t *testing.T) {
	spec := StepSpec{
		Name: "baseline",
		Old:  map[string]Behavior{"/svc": bA, "/new": bNF},
		New:  map[string]Behavior{"/svc": bA, "/new": bNF},
	}
	var recs []Record
	recs = append(recs, series("i", t0, windowEnd(), 10*time.Millisecond, "/svc", bA)...)
	recs = append(recs, series("i", t0, windowEnd(), 10*time.Millisecond, "/new", bNF)...)

	res := judgeStep(spec, time.Time{}, time.Time{}, recs, zeroCounts, zeroCounts)
	wantNoProblems(t, res)

	sparse := append(series("i", t0, windowEnd(), 10*time.Millisecond, "/svc", bA), series("i", t0, t0.Add(90*time.Millisecond), 10*time.Millisecond, "/new", bNF)...)
	res = judgeStep(spec, time.Time{}, time.Time{}, sparse, zeroCounts, zeroCounts)
	if !hasProblem(res, "only 9 requests to /new after the trigger") {
		t.Fatalf("problems = %q, want the too-few problem for /new", res.Problems)
	}
}

func TestJudgeSignatures(t *testing.T) {
	const sig = "weight success=+1 failure=+0 failures=0 final=/new=200 B,/svc=200 B"
	var results []StepResult
	for _, d := range []string{"watch", "sighup", "api"} {
		for _, inst := range []string{"0", "1"} {
			results = append(results, StepResult{Driver: d, Step: "weight", Instance: inst, Signature: sig})
		}
	}
	if got := judgeSignatures(results); got != nil {
		t.Fatalf("equal signatures: got %q, want nil", got)
	}

	results[len(results)-1].Signature = "weight success=+1 failure=+0 failures=1 final=/new=200 B,/svc=200 B"
	got := judgeSignatures(results)
	if len(got) != 1 || !strings.Contains(got[0], "signature of weight differs") {
		t.Fatalf("differing api instance: got %q", got)
	}
}

func validShutdown() ShutdownObs {
	signal := t0
	return ShutdownObs{
		Driver:   "watch",
		Instance: "0",
		Signal:   signal,
		NotReady: t0.Add(60 * time.Millisecond),
		Records: []Record{
			one("/svc", t0.Add(-50*time.Millisecond), t0.Add(-49*time.Millisecond), bA),
			one("/svc", t0.Add(10*time.Millisecond), t0.Add(11*time.Millisecond), bA),
			one("/slow", t0.Add(-100*time.Millisecond), t0.Add(200*time.Millisecond), Behavior{Status: 200}),
		},
		Refused:      true,
		ExitCode:     0,
		Exited:       t0.Add(1200 * time.Millisecond),
		DrainLogged:  true,
		DrainTimeout: 5 * time.Second,
	}
}

func TestJudgeShutdown(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(o *ShutdownObs)
		problem string
	}{
		{"valid sequence", func(o *ShutdownObs) {}, ""},
		{"refused before the 503", func(o *ShutdownObs) {
			o.Records = append(o.Records, Record{Path: "/svc", Start: t0.Add(20 * time.Millisecond), End: t0.Add(21 * time.Millisecond), Err: "refused"})
		}, "refused before the 503"},
		{"slow answered 502", func(o *ShutdownObs) {
			o.Records[2].Status = 502
		}, "in-flight /slow"},
		{"exit code 1", func(o *ShutdownObs) { o.ExitCode = 1 }, "exit code 1"},
		{"request after the 503", func(o *ShutdownObs) {
			o.Records = append(o.Records, one("/svc", o.NotReady.Add(time.Millisecond), o.NotReady.Add(2*time.Millisecond), bA))
		}, "after the observed 503"},
		{"drain not logged", func(o *ShutdownObs) { o.DrainLogged = false }, "drain complete"},
		{"exit too late", func(o *ShutdownObs) { o.Exited = t0.Add(5100 * time.Millisecond) }, "did not exit within"},
		{"listener still accepting", func(o *ShutdownObs) { o.Refused = false }, "still accepts"},
		{"no readiness 503", func(o *ShutdownObs) { o.NotReady = time.Time{} }, "never reported 503"},
	}
	exact := map[string][]string{
		"slow answered 502": {
			"1 requests started before the 503 did not complete with 200",
			"in-flight /slow: 0 requests span the signal with status 200, want 1",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o := validShutdown()
			c.mutate(&o)
			got := judgeShutdown(o)
			if want, ok := exact[c.name]; ok {
				if !slices.Equal(got, want) {
					t.Fatalf("problems = %q, want exactly %q", got, want)
				}
				return
			}
			if c.problem == "" {
				if got != nil {
					t.Fatalf("problems = %q, want nil", got)
				}
				return
			}
			found := false
			for _, p := range got {
				if strings.Contains(p, c.problem) {
					found = true
				}
			}
			if !found {
				t.Fatalf("problems = %q, want one containing %q", got, c.problem)
			}
		})
	}
}
