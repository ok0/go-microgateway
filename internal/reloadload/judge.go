// Command reloadload runs a paced loopback load against hot reloads of the gateway and judges every reload.
package main

import (
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Record is one request sent by the load generator and its observed outcome. Err is empty on success, "refused" for a
// refused connection, and the error text otherwise.
type Record struct {
	Instance, Path string
	Start, End     time.Time
	Status         int
	Backend        string
	Err            string
}

// Behavior is the observable answer of one path: its status and the backend that served it. Backend is empty for the
// gateway's own 404.
type Behavior struct {
	Status  int
	Backend string
}

// StepSpec describes the expected behavior of one step before and after its trigger.
type StepSpec struct {
	Name     string
	Old, New map[string]Behavior
	Changes  bool
	Success  float64
	Failure  float64
}

// Counts holds the reload counters read from the metrics endpoint.
type Counts struct {
	Success, Failure, Unchanged float64
}

// StepResult is the judged outcome of one step on one instance.
type StepResult struct {
	Driver, Step, Instance string
	Requests, Failures     int
	Latency                time.Duration
	HasLatency             bool
	Success, Failure       float64
	Unchanged              float64
	Signature              string
	Problems               []string
}

// ShutdownObs is what the shutdown phase observed for one gateway instance.
type ShutdownObs struct {
	Driver, Instance string
	Signal, NotReady time.Time
	PollErr          string
	Records          []Record
	Refused          bool
	ExitCode         int
	Exited           time.Time
	DrainLogged      bool
	DrainTimeout     time.Duration
}

const effectLimit = 2 * time.Second

const minAfter = 10

func (b Behavior) matches(r Record) bool {
	return r.Err == "" && r.Status == b.Status && r.Backend == b.Backend
}

// String formats the behavior as its status, followed by the backend name when there is one.
func (b Behavior) String() string {
	if b.Backend == "" {
		return strconv.Itoa(b.Status)
	}
	return fmt.Sprintf("%d %s", b.Status, b.Backend)
}

func (spec StepSpec) accepts(r Record) bool {
	if b, ok := spec.Old[r.Path]; ok && b.matches(r) {
		return true
	}
	if b, ok := spec.New[r.Path]; ok && b.matches(r) {
		return true
	}
	return false
}

func outcome(r Record) string {
	if r.Err != "" {
		return r.Err
	}
	return strconv.Itoa(r.Status)
}

func recordState(r Record) string {
	if r.Err != "" {
		return "err"
	}
	return Behavior{Status: r.Status, Backend: r.Backend}.String()
}

func finalState(spec StepSpec, sorted []Record) string {
	set := map[string]bool{}
	for p := range spec.Old {
		set[p] = true
	}
	for p := range spec.New {
		set[p] = true
	}
	paths := slices.Sorted(maps.Keys(set))
	parts := make([]string, 0, len(paths))
	for _, p := range paths {
		state := "none"
		for _, r := range sorted {
			if r.Path == p {
				state = recordState(r)
			}
		}
		parts = append(parts, p+"="+state)
	}
	return strings.Join(parts, ",")
}

// judgeStep judges the records of one instance in one window. start and trigger are the trigger start and completion
// of that instance; they are used only when spec.Changes is set.
func judgeStep(spec StepSpec, start, trigger time.Time, recs []Record, before, after Counts) StepResult {
	sorted := slices.Clone(recs)
	slices.SortStableFunc(sorted, func(a, b Record) int { return a.Start.Compare(b.Start) })
	res := StepResult{
		Requests:  len(sorted),
		Success:   after.Success - before.Success,
		Failure:   after.Failure - before.Failure,
		Unchanged: after.Unchanged - before.Unchanged,
	}
	var problems []string

	triggerOK := true
	if spec.Changes && (start.IsZero() || trigger.IsZero() || start.After(trigger)) {
		triggerOK = false
		problems = append(problems, fmt.Sprintf("invalid trigger times: start %v, completion %v", start, trigger))
	}

	failures, firstBad := 0, -1
	for i, r := range sorted {
		if !spec.accepts(r) {
			failures++
			if firstBad < 0 {
				firstBad = i
			}
		}
	}
	res.Failures = failures
	if failures > 0 {
		bad := sorted[firstBad]
		problems = append(problems, fmt.Sprintf("%d failed requests (first: %s %s at +%v)",
			failures, bad.Path, outcome(bad), bad.Start.Sub(sorted[0].Start)))
	}

	if !spec.Changes {
		changed := 0
		for _, r := range sorted {
			if old, ok := spec.Old[r.Path]; ok && old.matches(r) {
				continue
			}
			if nb, ok := spec.New[r.Path]; ok && nb.matches(r) {
				changed++
			}
		}
		if changed > 0 {
			problems = append(problems, fmt.Sprintf("%d requests changed behavior", changed))
		}
		for _, p := range slices.Sorted(maps.Keys(spec.Old)) {
			n := 0
			for _, r := range sorted {
				if r.Path == p && !r.Start.Before(trigger) {
					n++
				}
			}
			if n < minAfter {
				problems = append(problems, fmt.Sprintf("only %d requests to %s after the trigger", n, p))
			}
		}
	}

	if spec.Changes && triggerOK {
		early := 0
		for _, r := range sorted {
			if !r.End.Before(start) {
				continue
			}
			if old, ok := spec.Old[r.Path]; ok && old.matches(r) {
				continue
			}
			if nb, ok := spec.New[r.Path]; ok && nb.matches(r) {
				early++
			}
		}
		if early > 0 {
			problems = append(problems, fmt.Sprintf("%d requests completed before the trigger started show the new behavior", early))
		}

		var post []Record
		for _, r := range sorted {
			if !r.Start.Before(trigger) {
				post = append(post, r)
			}
		}
		next := 0
		for i, r := range post {
			if nb, ok := spec.New[r.Path]; !ok || !nb.matches(r) {
				next = i + 1
			}
		}
		if next >= len(post) {
			problems = append(problems, "new behavior never reached")
		} else {
			effect := post[next].Start
			res.Latency = effect.Sub(trigger)
			res.HasLatency = true
			if res.Latency > effectLimit {
				problems = append(problems, fmt.Sprintf("reload latency %v exceeds 2s", res.Latency))
			}
			for _, p := range slices.Sorted(maps.Keys(spec.New)) {
				n := 0
				for _, r := range sorted {
					if r.Path == p && !r.Start.Before(effect) {
						n++
					}
				}
				if n < minAfter {
					problems = append(problems, fmt.Sprintf("only %d requests to %s after the effect point", n, p))
				}
			}
		}
	}

	if res.Success != spec.Success {
		problems = append(problems, fmt.Sprintf("success delta %g, want %g", res.Success, spec.Success))
	}
	if res.Failure != spec.Failure {
		problems = append(problems, fmt.Sprintf("failure delta %g, want %g", res.Failure, spec.Failure))
	}

	res.Signature = fmt.Sprintf("%s success=%+g failure=%+g failures=%d final=%s",
		spec.Name, res.Success, res.Failure, res.Failures, finalState(spec, sorted))
	res.Problems = problems
	return res
}

// judgeSignatures reports every step whose signatures differ between instances or between drivers.
func judgeSignatures(results []StepResult) []string {
	var steps []string
	bySteps := map[string][]StepResult{}
	for _, r := range results {
		if _, ok := bySteps[r.Step]; !ok {
			steps = append(steps, r.Step)
		}
		bySteps[r.Step] = append(bySteps[r.Step], r)
	}
	var out []string
	for _, step := range steps {
		distinct := map[string]bool{}
		var parts []string
		for _, r := range bySteps[step] {
			distinct[r.Signature] = true
			parts = append(parts, r.Driver+"/"+r.Instance+": "+r.Signature)
		}
		if len(distinct) > 1 {
			out = append(out, fmt.Sprintf("signature of %s differs: %s", step, strings.Join(parts, "; ")))
		}
	}
	return out
}

// judgeShutdown judges the graceful shutdown of one instance under load.
func judgeShutdown(o ShutdownObs) []string {
	var out []string
	if o.NotReady.IsZero() {
		out = append(out, "readiness never reported 503")
	}
	if o.PollErr != "" {
		out = append(out, "readiness poll failed before 503: "+o.PollErr)
	}

	afterNotReady, refused, other := 0, 0, 0
	for _, r := range o.Records {
		switch {
		case !o.NotReady.IsZero() && !r.Start.Before(o.NotReady):
			afterNotReady++
		case r.Err == "refused":
			refused++
		case r.Err != "" || r.Status != http.StatusOK:
			other++
		}
	}
	if afterNotReady > 0 {
		out = append(out, fmt.Sprintf("%d requests sent after the observed 503", afterNotReady))
	}
	if refused > 0 {
		out = append(out, fmt.Sprintf("%d connections refused before the 503", refused))
	}
	if other > 0 {
		out = append(out, fmt.Sprintf("%d requests started before the 503 did not complete with 200", other))
	}

	ok := 0
	for _, r := range o.Records {
		if r.Path == "/slow" && r.Start.Before(o.Signal) && r.End.After(o.Signal) && r.Err == "" && r.Status == http.StatusOK {
			ok++
		}
	}
	if ok != 1 {
		out = append(out, fmt.Sprintf("in-flight /slow: %d requests span the signal with status 200, want 1", ok))
	}

	if !o.Refused {
		out = append(out, "listener still accepts connections after exit")
	}
	if o.Exited.IsZero() || o.Exited.Sub(o.Signal) > o.DrainTimeout {
		out = append(out, fmt.Sprintf("did not exit within %v", o.DrainTimeout))
	}
	if o.ExitCode != 0 {
		out = append(out, fmt.Sprintf("exit code %d", o.ExitCode))
	}
	if !o.DrainLogged {
		out = append(out, "drain complete not logged")
	}
	return out
}
