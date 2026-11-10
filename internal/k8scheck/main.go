// Command k8scheck checks rendered Kubernetes manifests of the gateway's Helm chart: object rules, probe and shutdown wiring, secret wiring, and microgateway validate on the rendered configuration.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ok0/go-microgateway/config"
)

const defaultsDocument = "version: 1\nlisteners:\n  - name: http\n    address: \":8080\"\nupstreams:\n  - name: api\n    hosts:\n      - url: \"http://127.0.0.1:9001\"\nroutes:\n  - name: all\n    upstream: api\n"

func lookupEnv(name string) (string, bool) {
	if name == "REDIS_PASSWORD" {
		return examplePassword, true
	}
	return "", false
}

func flat(err error) string {
	return strings.Join(strings.Fields(err.Error()), " ")
}

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

// run checks the manifests named in args and returns the exit code: 0 without findings, 1 with findings, 2 on bad usage. Write errors are ignored.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("k8scheck", flag.ContinueOnError)
	fs.SetOutput(stderr)
	binary := fs.String("binary", "", "path to the gateway binary (bin/microgateway after make build)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *binary == "" {
		_, _ = fmt.Fprintln(stderr, "k8scheck: -binary is required (run make build)")
		return 2
	}
	info, err := os.Stat(*binary)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "k8scheck: -binary %s: %v\n", *binary, err)
		return 2
	}
	if info.IsDir() {
		_, _ = fmt.Fprintf(stderr, "k8scheck: -binary %s: is a directory\n", *binary)
		return 2
	}
	files := fs.Args()
	if len(files) == 0 {
		_, _ = fmt.Fprintln(stderr, "k8scheck: no manifest files; usage: k8scheck -binary <path> <manifest.yaml>...")
		return 2
	}
	contents := make([][]byte, len(files))
	for i, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "k8scheck: read %s: %v\n", f, err)
			return 2
		}
		contents[i] = data
	}

	var lines []string
	dcfg, err := config.Parse([]byte(defaultsDocument), config.WithEnvLookup(lookupEnv))
	if err != nil {
		lines = append(lines, Finding{"Config", "defaults", "the defaults document does not parse: " + flat(err)}.Line("(defaults)"))
	} else {
		for _, f := range CheckDefaults(dcfg) {
			lines = append(lines, f.Line("(defaults)"))
		}
	}

	for i, file := range files {
		docs, err := Decode(contents[i])
		if err != nil {
			lines = append(lines, Finding{"-", "-", "manifest does not parse: " + flat(err)}.Line(file))
			continue
		}
		name, text, ok := ConfigText(docs)
		var cfg *config.Config
		if ok {
			parsed, err := config.Parse([]byte(text), config.WithEnvLookup(lookupEnv))
			if err != nil {
				lines = append(lines, Finding{"ConfigMap", name, "config does not parse: " + flat(err)}.Line(file))
			} else {
				cfg = parsed
			}
		}
		for _, f := range Check(docs, cfg) {
			lines = append(lines, f.Line(file))
		}
		if ok {
			if f := validateConfig(ctx, *binary, name, text); f != nil {
				lines = append(lines, f.Line(file))
			}
		}
	}

	for _, l := range lines {
		_, _ = fmt.Fprintln(stdout, l)
	}
	if len(lines) > 0 {
		_, _ = fmt.Fprintf(stderr, "k8scheck: %d finding(s)\n", len(lines))
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "k8scheck: no findings in %d file(s)\n", len(files))
	return 0
}
