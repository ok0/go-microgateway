package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/ok0/lychgate"
	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/server"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

const usage = `usage: lychgate <command> [flags]

commands:
  validate -config <path>   check a configuration file and report every problem
  run -config <path> [-log-level L] [-log-format F]   serve the gateway until interrupted
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
	case "run":
		return runServe(ctx, args[1:], stdout, stderr, nil)
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
		return printLoadError(stderr, "validate", *path, err)
	}
	fmt.Fprintf(stdout, "%s: configuration is valid (%d listeners, %d upstreams, %d routes)\n",
		*path, len(cfg.Listeners), len(cfg.Upstreams), len(cfg.Routes))
	return 0
}

func printLoadError(stderr io.Writer, cmd, path string, err error) int {
	var ve *config.ValidationError
	if errors.As(err, &ve) {
		for _, fe := range ve.Errors {
			fmt.Fprintln(stderr, fe.String())
		}
		fmt.Fprintf(stderr, "%s: %d error(s)\n", path, len(ve.Errors))
		return 1
	}
	fmt.Fprintf(stderr, "lychgate %s: %v\n", cmd, err)
	return 1
}

func runServe(ctx context.Context, args []string, stdout, stderr io.Writer, onReady func(map[string]net.Addr)) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	path := fs.String("config", "", "path to the configuration file")
	logLevel := fs.String("log-level", "", "log level: debug, info, warn or error")
	logFormat := fs.String("log-format", "", "log format: json or text")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "lychgate run: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	if *path == "" {
		fmt.Fprintln(stderr, "lychgate run: -config is required")
		return 2
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	if set["log-level"] && !slices.Contains([]string{"debug", "info", "warn", "error"}, *logLevel) {
		fmt.Fprintf(stderr, "lychgate run: invalid -log-level %q\n", *logLevel)
		return 2
	}
	if set["log-format"] && !slices.Contains([]string{"json", "text"}, *logFormat) {
		fmt.Fprintf(stderr, "lychgate run: invalid -log-format %q\n", *logFormat)
		return 2
	}

	cfg, err := config.LoadFile(*path)
	if err != nil {
		return printLoadError(stderr, "run", *path, err)
	}
	if set["log-level"] {
		cfg.Global.Log.Level = *logLevel
	}
	if set["log-format"] {
		cfg.Global.Log.Format = *logFormat
	}
	logger := newLogger(stderr, cfg.Global.Log)

	g, err := lychgate.New(cfg, lychgate.WithLogger(logger))
	if err != nil {
		fmt.Fprintf(stderr, "lychgate run: %v\n", err)
		return 1
	}
	srv, err := server.New(cfg, g, logger)
	if err != nil {
		fmt.Fprintf(stderr, "lychgate run: %v\n", err)
		g.Close()
		return 1
	}
	if err := srv.Start(); err != nil {
		fmt.Fprintf(stderr, "lychgate run: %v\n", err)
		g.Close()
		return 1
	}

	addrs := srv.Addrs()
	for _, l := range cfg.Listeners {
		logger.Info("listening", "listener", l.Name, "address", addrs[l.Name].String())
	}
	if onReady != nil {
		onReady(addrs)
	}

	sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	code := 0
	select {
	case <-sigCtx.Done():
	case err := <-srv.Errors():
		logger.Error("listener failed", "error", err)
		code = 1
	}

	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shCtx); err != nil {
		logger.Warn("shutdown", "error", err)
	}
	g.Close()
	logger.Info("stopped")
	return code
}

func newLogger(w io.Writer, l config.Log) *slog.Logger {
	var level slog.Level
	switch l.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	if l.Format == "text" {
		return slog.New(slog.NewTextHandler(w, opts))
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}
