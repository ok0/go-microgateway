package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/ok0/lychgate/config"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

const usage = `usage: lychgate <command> [flags]

commands:
  validate -config <path>   check a configuration file and report every problem
  version                   print version information
`

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "validate":
		return runValidate(args[1:], stdout, stderr)
	case "version":
		fmt.Fprintf(stdout, "lychgate %s (commit %s, built %s)\n", version, commit, date)
		return 0
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return 0
	default:
		fmt.Fprintf(stderr, "lychgate: unknown command %q\n", args[0])
		fmt.Fprint(stderr, usage)
		return 2
	}
}

func runValidate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	path := fs.String("config", "", "path to the configuration file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "lychgate validate: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	if *path == "" {
		fmt.Fprintln(stderr, "lychgate validate: -config is required")
		return 2
	}
	cfg, err := config.LoadFile(*path)
	if err != nil {
		var ve *config.ValidationError
		if errors.As(err, &ve) {
			for _, fe := range ve.Errors {
				fmt.Fprintln(stderr, fe.String())
			}
			fmt.Fprintf(stderr, "%s: %d error(s)\n", *path, len(ve.Errors))
			return 1
		}
		fmt.Fprintf(stderr, "lychgate validate: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "%s: configuration is valid (%d listeners, %d upstreams, %d routes)\n",
		*path, len(cfg.Listeners), len(cfg.Upstreams), len(cfg.Routes))
	return 0
}
