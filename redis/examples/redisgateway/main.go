// Command redisgateway is an application that embeds the gateway with the Redis store type registered. Configurations
// it runs may reference stores of type redis for rate limits and the response cache.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	microgateway "github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/redis"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "redisgateway:", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to the configuration file (required)")
	listen := flag.String("listen", ":8080", "address the gateway serves traffic on")
	admin := flag.String("admin", "127.0.0.1:9901", "address the admin endpoint serves on")
	flag.Parse()
	if *configPath == "" {
		return errors.New("-config is required")
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	data, err := os.ReadFile(*configPath)
	if err != nil {
		return err
	}
	gw, err := microgateway.NewFromYAML(data, redis.Option(), microgateway.WithLogger(logger))
	if err != nil {
		return err
	}

	gwSrv := &http.Server{Addr: *listen, Handler: gw}
	adminSrv := &http.Server{Addr: *admin, Handler: gw.AdminHandler()}

	errCh := make(chan error, 2)
	go func() { errCh <- gwSrv.ListenAndServe() }()
	go func() { errCh <- adminSrv.ListenAndServe() }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGTERM)
	defer signal.Stop(sig)

	for {
		select {
		case err := <-errCh:
			return errors.Join(err, shutdown(gw, gwSrv, adminSrv))
		case s := <-sig:
			if s == syscall.SIGHUP {
				if err := gw.ReloadFile(*configPath); err != nil {
					logger.Error("reload failed", "error", err)
				} else {
					logger.Info("configuration reloaded")
				}
				continue
			}
			logger.Info("shutting down", "signal", s.String())
			return shutdown(gw, gwSrv, adminSrv)
		}
	}
}

func shutdown(gw *microgateway.Gateway, gwSrv, adminSrv *http.Server) error {
	gw.SetReady(false)
	ctx, cancel := context.WithTimeout(context.Background(), gw.Config().Global.DrainTimeout.Std())
	defer cancel()
	err := gwSrv.Shutdown(ctx)
	err = errors.Join(err, adminSrv.Shutdown(ctx))
	return errors.Join(err, gw.Close())
}
