// Package cli implements the microgateway command: run, validate and version.
package cli

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

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	internalhooks "github.com/ok0/go-microgateway/internal/hooks"
	"github.com/ok0/go-microgateway/internal/reload"
	"github.com/ok0/go-microgateway/internal/server"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

const usage = `usage: microgateway <command> [flags]

commands:
  validate -config <path>   check a configuration file and report every problem
  run -config <path> [-log-level L] [-log-format F]   serve the gateway until interrupted
  version                   print version information
`

// Addrs holds the addresses a serving run is bound to.
type Addrs struct {
	Listeners map[string]net.Addr
	Admin     net.Addr
}

// Env is the process environment of one invocation.
type Env struct {
	Args      []string                    // command-line arguments without the program name
	Stdin     io.Reader                   // standard input; no command reads it today
	Stdout    io.Writer                   // nil discards
	Stderr    io.Writer                   // nil discards
	LookupEnv func(string) (string, bool) // resolves ${VAR}; nil means os.LookupEnv
	Signals   <-chan os.Signal            // nil: run subscribes to SIGINT, SIGTERM and SIGHUP while serving
	Ready     func(Addrs)                 // optional; called once run has bound its listeners
}

// Main runs one invocation and returns its exit code. It never exits the process. opts are passed to every
// gateway the invocation builds or validates, after the command's own options.
func Main(ctx context.Context, env Env, opts ...microgateway.Option) int {
	stdout, stderr := env.Stdout, env.Stderr
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	lookup := env.LookupEnv
	if lookup == nil {
		lookup = os.LookupEnv
	}
	args := env.Args
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "validate":
		return runValidate(args[1:], stdout, stderr, lookup, opts)
	case "run":
		return runServe(ctx, args[1:], stdout, stderr, lookup, env.Signals, env.Ready, opts)
	case "version":
		fmt.Fprintf(stdout, "microgateway %s (commit %s, built %s)\n", version, commit, date)
		return 0
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return 0
	default:
		fmt.Fprintf(stderr, "microgateway: unknown command %q\n", args[0])
		fmt.Fprint(stderr, usage)
		return 2
	}
}

func runValidate(args []string, stdout, stderr io.Writer, lookup func(string) (string, bool), opts []microgateway.Option) int {
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
		fmt.Fprintf(stderr, "microgateway validate: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	if *path == "" {
		fmt.Fprintln(stderr, "microgateway validate: -config is required")
		return 2
	}
	cfg, err := loadConfig(*path, lookup, opts)
	if err != nil {
		return printLoadError(stderr, "validate", *path, err)
	}
	fmt.Fprintf(stdout, "%s: configuration is valid (%d listeners, %d upstreams, %d routes)\n",
		*path, len(cfg.Listeners), len(cfg.Upstreams), len(cfg.Routes))
	return 0
}

// loadConfig loads the file at path and validates it against opts, reporting schema and registration problems together.
func loadConfig(path string, lookup func(string) (string, bool), opts []microgateway.Option) (*config.Config, error) {
	return config.LoadFile(path, config.WithEnvLookup(lookup), config.WithCheck(func(c *config.Config) []config.FieldError {
		err := microgateway.Validate(c, opts...)
		if err == nil {
			return nil
		}
		var ve *config.ValidationError
		if errors.As(err, &ve) {
			return ve.Errors
		}
		return []config.FieldError{{Msg: err.Error()}}
	}))
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
	fmt.Fprintf(stderr, "microgateway %s: %v\n", cmd, err)
	return 1
}

func runServe(ctx context.Context, args []string, stdout, stderr io.Writer, lookup func(string) (string, bool), signals <-chan os.Signal, ready func(Addrs), opts []microgateway.Option) int {
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
		fmt.Fprintf(stderr, "microgateway run: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	if *path == "" {
		fmt.Fprintln(stderr, "microgateway run: -config is required")
		return 2
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	if set["log-level"] && !slices.Contains([]string{"debug", "info", "warn", "error"}, *logLevel) {
		fmt.Fprintf(stderr, "microgateway run: invalid -log-level %q\n", *logLevel)
		return 2
	}
	if set["log-format"] && !slices.Contains([]string{"json", "text"}, *logFormat) {
		fmt.Fprintf(stderr, "microgateway run: invalid -log-format %q\n", *logFormat)
		return 2
	}

	cfg, err := loadConfig(*path, lookup, opts)
	if err != nil {
		return printLoadError(stderr, "run", *path, err)
	}
	logCfg := cfg.Global.Log
	if set["log-level"] {
		logCfg.Level = *logLevel
	}
	if set["log-format"] {
		logCfg.Format = *logFormat
	}
	logger := newLogger(stderr, logCfg)

	certs := server.NewCertificates(logger, clock.Real())
	gwOpts := []microgateway.Option{microgateway.WithLogger(logger), microgateway.WithEnvLookup(lookup), internalhooks.WithParticipant(certs).(microgateway.Option)}
	gwOpts = append(gwOpts, opts...)
	g, err := microgateway.New(cfg, gwOpts...)
	if err != nil {
		return printLoadError(stderr, "run", *path, err)
	}
	var admin http.Handler
	if cfg.Admin.Enabled == nil || *cfg.Admin.Enabled {
		admin = g.AdminHandler()
	}
	srv, err := server.New(cfg, g, admin, g.SetReady, logger, server.WithCertificates(certs))
	if err != nil {
		fmt.Fprintf(stderr, "microgateway run: %v\n", err)
		g.Close()
		return 1
	}
	sig := signals
	if sig == nil {
		// Subscribe before the listeners are announced so a signal sent as soon as the gateway reports ready is queued rather than terminating the process.
		ch := make(chan os.Signal, 2)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
		defer signal.Stop(ch)
		sig = ch
	}

	if err := srv.Start(); err != nil {
		fmt.Fprintf(stderr, "microgateway run: %v\n", err)
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
	if ready != nil {
		ready(Addrs{Listeners: addrs, Admin: srv.AdminAddr()})
	}

	w := reload.New(reload.Options{
		Paths:        func() []string { return watchPaths(*path, g.Config()) },
		Debounce:     cfg.Global.Reload.Debounce.Std(),
		PollInterval: cfg.Global.Reload.PollInterval.Std(),
		Clock:        clock.Real(),
		Logger:       logger,
		Apply:        func() error { return applyReload(g, srv, *path, logger) },
		Fatal:        func(err error) bool { return errors.Is(err, microgateway.ErrClosed) },
	})
	watchCtx, stopWatch := context.WithCancel(context.Background())
	watchDone := make(chan error, 1)
	go func() { watchDone <- w.Run(watchCtx) }()

	var ctxDone <-chan struct{} = ctx.Done()
	code := 0
	var stopSig os.Signal
wait:
	for {
		select {
		case <-ctx.Done():
			ctxDone = nil
			break wait
		case s := <-sig:
			if s == syscall.SIGHUP {
				logger.Info("reload requested", "signal", s)
				w.Trigger()
				continue
			}
			stopSig = s
			break wait
		case err := <-srv.Errors():
			logger.Error("listener failed", "error", err)
			code = 1
			break wait
		case err := <-watchDone:
			logger.Error("configuration watch stopped; reload unavailable", "error", err)
			watchDone = nil
		}
	}

	stopWatch()
	if watchDone != nil {
		<-watchDone
	}
	if stopSig != nil {
		logger.Info("shutting down", "signal", stopSig)
	}

	shCtx, force := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		for {
			select {
			case s := <-sig:
				if s == syscall.SIGHUP {
					logger.Info("reload ignored; shutting down")
					continue
				}
				logger.Warn("second signal; forcing shutdown", "signal", s)
				force()
				return
			case <-ctxDone:
				force()
				return
			case <-done:
				return
			}
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

func watchPaths(configPath string, cfg *config.Config) []string {
	paths := []string{configPath}
	for _, l := range cfg.Listeners {
		if l.TLS != nil {
			paths = append(paths, l.TLS.CertFile, l.TLS.KeyFile)
		}
	}
	paths = append(paths, cfg.KeyFiles()...)
	return paths
}

func applyReload(g *microgateway.Gateway, srv *server.Server, path string, logger *slog.Logger) error {
	prev := g.Config()
	if err := g.ReloadFile(path); err != nil {
		return err
	}
	cur := g.Config()
	if cur == prev {
		return nil
	}
	for _, f := range restartRequired(prev, cur) {
		logger.Warn("restart required; change not applied", "field", f)
	}
	srv.SetShutdownTimings(cur.Global.ShutdownDelay.Std(), cur.Global.DrainTimeout.Std())
	return nil
}

func restartRequired(old, cur *config.Config) []string {
	var out []string
	if listenerNamesDiffer(old.Listeners, cur.Listeners) {
		out = append(out, "listeners")
	}
	for i := range min(len(old.Listeners), len(cur.Listeners)) {
		a, b := old.Listeners[i], cur.Listeners[i]
		field := func(name string) string { return fmt.Sprintf("listeners[%d].%s", i, name) }
		if a.Name != b.Name {
			out = append(out, field("name"))
		}
		if a.Address != b.Address {
			out = append(out, field("address"))
		}
		switch {
		case (a.TLS == nil) != (b.TLS == nil):
			out = append(out, field("tls"))
		case a.TLS != nil && a.TLS.MinVersion != b.TLS.MinVersion:
			out = append(out, field("tls.min_version"))
		}
		if listenerHTTP2(a) != listenerHTTP2(b) {
			out = append(out, field("http2"))
		}
		if a.H2C != b.H2C {
			out = append(out, field("h2c"))
		}
		if a.ReadHeaderTimeout.Std() != b.ReadHeaderTimeout.Std() {
			out = append(out, field("read_header_timeout"))
		}
		if a.ReadTimeout.Std() != b.ReadTimeout.Std() {
			out = append(out, field("read_timeout"))
		}
		if a.WriteTimeout.Std() != b.WriteTimeout.Std() {
			out = append(out, field("write_timeout"))
		}
		if a.IdleTimeout.Std() != b.IdleTimeout.Std() {
			out = append(out, field("idle_timeout"))
		}
		if a.MaxHeaderBytes != b.MaxHeaderBytes {
			out = append(out, field("max_header_bytes"))
		}
	}
	if adminEnabled(old) != adminEnabled(cur) {
		out = append(out, "admin.enabled")
	}
	if old.Admin.Address != cur.Admin.Address {
		out = append(out, "admin.address")
	}
	if old.Global.Log.Level != cur.Global.Log.Level {
		out = append(out, "global.log.level")
	}
	if old.Global.Log.Format != cur.Global.Log.Format {
		out = append(out, "global.log.format")
	}
	if old.Global.Reload.Debounce.Std() != cur.Global.Reload.Debounce.Std() {
		out = append(out, "global.reload.debounce")
	}
	if old.Global.Reload.PollInterval.Std() != cur.Global.Reload.PollInterval.Std() {
		out = append(out, "global.reload.poll_interval")
	}
	return out
}

func listenerNamesDiffer(a, b []config.Listener) bool {
	if len(a) != len(b) {
		return true
	}
	for i := range a {
		if a[i].Name != b[i].Name {
			return true
		}
	}
	return false
}

func listenerHTTP2(l config.Listener) bool {
	return l.HTTP2 == nil || *l.HTTP2
}

func adminEnabled(c *config.Config) bool {
	return c.Admin.Enabled == nil || *c.Admin.Enabled
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
