package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	s, err := parseSettings(args, stderr)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "reloadload: %v\n", err)
		return 2
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	dir, err := os.MkdirTemp("", "reloadload-")
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "reloadload: %v\n", err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	ups, closeBackends := startBackends()
	defer closeBackends()
	client := newClient(s.concurrency)

	runners := []func() driverRun{
		func() driverRun { return binaryDriver(ctx, s, "watch", ups, dir, client) },
		func() driverRun { return binaryDriver(ctx, s, "sighup", ups, dir, client) },
		func() driverRun { return apiDriver(ctx, s, ups, dir, client) },
	}
	var drivers []driverRun
	var reasons []string
	for _, r := range runners {
		if ctx.Err() != nil {
			break
		}
		drivers = append(drivers, r())
	}
	if ctx.Err() != nil {
		reasons = append(reasons, "interrupted")
	}

	var results []StepResult
	var shutdown []ShutdownObs
	for _, d := range drivers {
		for _, p := range d.problems {
			reasons = append(reasons, fmt.Sprintf("%s: %s", d.name, p))
		}
		for _, r := range d.results {
			for _, p := range r.Problems {
				reasons = append(reasons, fmt.Sprintf("%s/%s/%s: %s", r.Driver, r.Step, r.Instance, p))
			}
		}
		results = append(results, d.results...)
		shutdown = append(shutdown, d.shutdown...)
	}
	reasons = append(reasons, judgeSignatures(results)...)
	for _, o := range shutdown {
		for _, p := range judgeShutdown(o) {
			reasons = append(reasons, fmt.Sprintf("%s/shutdown/%s: %s", o.Driver, o.Instance, p))
		}
	}
	if len(results) != 3*len(stepOrder)*s.instances || len(shutdown) != 2*s.instances {
		reasons = append(reasons, "missing results")
	}

	printTables(stdout, results, shutdown)
	if len(reasons) == 0 {
		_, _ = fmt.Fprintln(stdout, "PASS")
		return 0
	}
	_, _ = fmt.Fprintf(stdout, "FAIL: %s\n", strings.Join(reasons, "; "))
	return 1
}

func printTables(w io.Writer, results []StepResult, shutdown []ShutdownObs) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "driver\tstep\tinstance\trequests\tfailures\tlatency\tsuccess\tfailure\tunchanged")
	for _, r := range results {
		lat := "-"
		if r.HasLatency {
			lat = r.Latency.Round(time.Millisecond).String()
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%s\t%+g\t%+g\t%+g\n",
			r.Driver, r.Step, r.Instance, r.Requests, r.Failures, lat, r.Success, r.Failure, r.Unchanged)
	}
	_ = tw.Flush()

	_, _ = fmt.Fprintln(tw, "driver\tinstance\tnot-ready-after\texit-after\texit-code\tdrain-complete\trefused-after-exit")
	for _, o := range shutdown {
		notReady := "-"
		if !o.NotReady.IsZero() {
			notReady = o.NotReady.Sub(o.Signal).Round(time.Millisecond).String()
		}
		exitAfter := "-"
		if !o.Exited.IsZero() {
			exitAfter = o.Exited.Sub(o.Signal).Round(time.Millisecond).String()
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%t\t%t\n",
			o.Driver, o.Instance, notReady, exitAfter, o.ExitCode, o.DrainLogged, o.Refused)
	}
	_ = tw.Flush()
}
