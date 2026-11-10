package main

import (
	"cmp"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type settings struct {
	binary      string
	duration    time.Duration
	rate        int
	concurrency int
	instances   int
}

func inRange[T cmp.Ordered](name string, got, lo, hi T) error {
	if got < lo || got > hi {
		return fmt.Errorf("-%s %v is out of range: allowed %v to %v", name, got, lo, hi)
	}
	return nil
}

func parseSettings(args []string, stderr io.Writer) (settings, error) {
	var s settings
	fs := flag.NewFlagSet("reloadload", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&s.binary, "binary", "", "path to the gateway binary (bin/microgateway after make build)")
	fs.DurationVar(&s.duration, "duration", 60*time.Second, "length of the run, from 30s to 10m")
	fs.IntVar(&s.rate, "rate", 200, "total requests per second, from 1 to 1000")
	fs.IntVar(&s.concurrency, "concurrency", 8, "load workers, from 1 to 32")
	fs.IntVar(&s.instances, "instances", 2, "gateway instances per driver, from 1 to 4")
	if err := fs.Parse(args); err != nil {
		return settings{}, err
	}
	if fs.NArg() > 0 {
		return settings{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if s.binary == "" {
		return settings{}, errors.New("-binary is required (run make build)")
	}
	info, err := os.Stat(s.binary)
	if err != nil {
		return settings{}, fmt.Errorf("-binary %s: %v", s.binary, err)
	}
	if info.IsDir() {
		return settings{}, fmt.Errorf("-binary %s: %v", s.binary, errors.New("is a directory"))
	}
	if err := inRange("duration", s.duration, 30*time.Second, 10*time.Minute); err != nil {
		return settings{}, err
	}
	if err := inRange("rate", s.rate, 1, 1000); err != nil {
		return settings{}, err
	}
	if err := inRange("concurrency", s.concurrency, 1, 32); err != nil {
		return settings{}, err
	}
	if err := inRange("instances", s.instances, 1, 4); err != nil {
		return settings{}, err
	}
	return s, nil
}

const (
	stepBaseline = "baseline"
	stepRoute    = "route"
	stepWeight   = "weight"
	stepInvalid  = "invalid"
)

var stepOrder = []string{stepBaseline, stepRoute, stepWeight, stepInvalid}

// upstreams holds the base URLs of the two backends behind the svc upstream.
type upstreams struct{ A, B string }

// renderOpts selects the variant of a document. signalOnly turns off the file poll and the debounce, so that only
// SIGHUP triggers a reload.
type renderOpts struct{ signalOnly bool }

// render returns the document of one step. It panics on an unknown step.
func render(step string, ups upstreams, opts renderOpts) []byte {
	switch step {
	case stepBaseline:
		return baselineDoc(ups, opts)
	case stepRoute:
		return routeDoc(ups, opts)
	case stepWeight:
		return weightDoc(ups, opts)
	case stepInvalid:
		return invalidDoc(ups, opts)
	}
	panic("reloadload: unknown step " + step)
}

func baselineDoc(ups upstreams, opts renderOpts) []byte {
	return doc(ups, opts, 1, 0, false, false)
}

func routeDoc(ups upstreams, opts renderOpts) []byte {
	return doc(ups, opts, 1, 0, true, false)
}

func weightDoc(ups upstreams, opts renderOpts) []byte {
	return doc(ups, opts, 0, 1, true, false)
}

func invalidDoc(ups upstreams, opts renderOpts) []byte {
	return doc(ups, opts, 0, 1, true, true)
}

func doc(ups upstreams, opts renderOpts, weightA, weightB int, withNew, broken bool) []byte {
	var b strings.Builder
	drain := "5s"
	if broken {
		drain = "-1s"
	}
	b.WriteString("version: 1\n")
	b.WriteString("global:\n")
	fmt.Fprintf(&b, "  drain_timeout: %s\n", drain)
	b.WriteString("  shutdown_delay: 1s\n")
	b.WriteString("  log:\n")
	b.WriteString("    level: info\n")
	b.WriteString("    format: json\n")
	b.WriteString("    access_log: false\n")
	if opts.signalOnly {
		b.WriteString("  reload:\n")
		b.WriteString("    debounce: 1h\n")
		b.WriteString("    poll_interval: 1h\n")
	}
	b.WriteString("listeners:\n")
	b.WriteString("  - name: public\n")
	b.WriteString("    address: \"127.0.0.1:0\"\n")
	b.WriteString("admin:\n")
	b.WriteString("  enabled: true\n")
	b.WriteString("  address: \"127.0.0.1:0\"\n")
	b.WriteString("upstreams:\n")
	b.WriteString("  - name: svc\n")
	b.WriteString("    hosts:\n")
	fmt.Fprintf(&b, "      - url: \"%s\"\n", ups.A)
	fmt.Fprintf(&b, "        weight: %d\n", weightA)
	fmt.Fprintf(&b, "      - url: \"%s\"\n", ups.B)
	fmt.Fprintf(&b, "        weight: %d\n", weightB)
	b.WriteString("    load_balancer:\n")
	b.WriteString("      algorithm: weighted_round_robin\n")
	b.WriteString("routes:\n")
	b.WriteString("  - name: svc\n")
	b.WriteString("    match: {path: /svc, path_type: prefix}\n")
	b.WriteString("    upstream: svc\n")
	b.WriteString("  - name: slow\n")
	b.WriteString("    match: {path: /slow, path_type: prefix}\n")
	b.WriteString("    upstream: svc\n")
	if withNew {
		b.WriteString("  - name: new\n")
		b.WriteString("    match: {path: /new, path_type: prefix}\n")
		b.WriteString("    upstream: svc\n")
	}
	if broken {
		b.WriteString("  - name: broken\n")
		b.WriteString("    match: {path: /broken, path_type: prefix}\n")
		b.WriteString("    upstream: missing\n")
	}
	return []byte(b.String())
}

// stepSpecs returns the expectation of every step, keyed by step name.
func stepSpecs() map[string]StepSpec {
	a := Behavior{Status: 200, Backend: "A"}
	b := Behavior{Status: 200, Backend: "B"}
	nf := Behavior{Status: 404}
	return map[string]StepSpec{
		stepBaseline: {
			Name: stepBaseline,
			Old:  map[string]Behavior{"/svc": a, "/new": nf},
			New:  map[string]Behavior{"/svc": a, "/new": nf},
		},
		stepRoute: {
			Name:    stepRoute,
			Old:     map[string]Behavior{"/svc": a, "/new": nf},
			New:     map[string]Behavior{"/svc": a, "/new": a},
			Changes: true,
			Success: 1,
		},
		stepWeight: {
			Name:    stepWeight,
			Old:     map[string]Behavior{"/svc": a, "/new": a},
			New:     map[string]Behavior{"/svc": b, "/new": b},
			Changes: true,
			Success: 1,
		},
		stepInvalid: {
			Name:    stepInvalid,
			Old:     map[string]Behavior{"/svc": b, "/new": b},
			New:     map[string]Behavior{"/svc": b, "/new": b},
			Failure: 1,
		},
	}
}
