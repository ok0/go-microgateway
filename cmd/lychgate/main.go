package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"syscall"

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

type serveAddrs struct {
	Listeners map[string]net.Addr
	Admin     net.Addr
}

type serveHooks struct {
	onReady func(serveAddrs)
	signals <-chan os.Signal
}

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
		return runServe(ctx, args[1:], stdout, stderr, serveHooks{})
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

func runServe(ctx context.Context, args []string, stdout, stderr io.Writer, hooks serveHooks) int {
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
	var admin http.Handler
	if cfg.Admin.Enabled == nil || *cfg.Admin.Enabled {
		admin = g.AdminHandler()
	}
	srv, err := server.New(cfg, g, admin, g.SetReady, logger)
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
	if a := srv.AdminAddr(); a != nil {
		logger.Info("listening", "listener", "admin", "address", a.String())
	}
	if hooks.onReady != nil {
		hooks.onReady(serveAddrs{Listeners: addrs, Admin: srv.AdminAddr()})
	}

	sig := hooks.signals
	if sig == nil {
		ch := make(chan os.Signal, 2)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(ch)
		sig = ch
	}

	var ctxDone <-chan struct{} = ctx.Done()
	code := 0
	select {
	case <-ctx.Done():
		ctxDone = nil
	case s := <-sig:
		logger.Info("shutting down", "signal", s)
	case err := <-srv.Errors():
		logger.Error("listener failed", "error", err)
		code = 1
	}

	shCtx, force := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		select {
		case s := <-sig:
			logger.Warn("second signal; forcing shutdown", "signal", s)
			force()
		case <-ctxDone:
			force()
		case <-done:
		}
	}()
	err = srv.Shutdown(shCtx)
	close(done)
	force()
	if err != nil {
		logger.Warn("shutdown", "error", err)
		if errors.Is(err, server.ErrForcedClose) {
			code = 1
		}
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
