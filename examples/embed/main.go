// Command embed runs an application that embeds the gateway. It serves the application handler on -listen and the gateway admin handler on -admin, and reloads its configuration on SIGHUP. Without -config it uses the embedded gateway.yaml.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "embed"

	microgateway "github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/store"
)

//go:embed gateway.yaml
var configYAML []byte

const maxKeySetBytes = 1 << 20

type app struct {
	gateway *microgateway.Gateway
	handler http.Handler
}

func newApp(cfg []byte, lookup func(string) (string, bool), now func() time.Time) (*app, error) {
	gw, err := microgateway.NewFromYAML(cfg,
		microgateway.WithEnvLookup(lookup),
		microgateway.WithMiddleware("audit", auditMiddleware),
		microgateway.WithStoreType("example-memory", store.CapRateLimit, limiterFactory(now)),
		microgateway.WithKeySetFetcher(newAllowListFetcher(http.DefaultClient, "auth.example.com")),
	)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/app/hello", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "hello from the application")
	})
	mux.Handle("/", gw)
	return &app{gateway: gw, handler: mux}, nil
}

func auditMiddleware(p microgateway.MiddlewareParams) (microgateway.Middleware, error) {
	var params struct {
		Header string `yaml:"header"`
	}
	if err := p.Decode(&params); err != nil {
		return nil, err
	}
	if params.Header == "" {
		return nil, errors.New("header must not be empty")
	}
	header := params.Header
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if id, ok := microgateway.IdentityFromContext(r.Context()); ok {
				w.Header().Set(header, id.Consumer)
			}
			w.Header().Set("X-Audit-Request-ID", microgateway.RequestIDFromContext(r.Context()))
			next.ServeHTTP(w, r)
		})
	}, nil
}

type allowListFetcher struct {
	client *http.Client
	hosts  map[string]bool
}

func newAllowListFetcher(c *http.Client, hosts ...string) *allowListFetcher {
	allowed := make(map[string]bool, len(hosts))
	for _, h := range hosts {
		allowed[h] = true
	}
	return &allowListFetcher{client: c, hosts: allowed}
}

func (f *allowListFetcher) FetchKeySet(ctx context.Context, provider, rawURL string) ([]byte, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("fetch key set for %s: invalid URL", provider)
	}
	if u.Scheme != "https" || !f.hosts[u.Hostname()] {
		return nil, fmt.Errorf("fetch key set for %s: scheme or host not allowed", provider)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("fetch key set for %s: invalid URL", provider)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch key set for %s: request failed", provider)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch key set for %s: status %d", provider, resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, maxKeySetBytes))
}

func main() {
	configPath := flag.String("config", "", "configuration file; empty selects the embedded configuration")
	listen := flag.String("listen", ":8080", "address of the application and gateway listener")
	adminAddr := flag.String("admin", "127.0.0.1:9901", "address of the admin listener")
	flag.Parse()
	if err := run(*configPath, *listen, *adminAddr); err != nil {
		log.Fatal(err)
	}
}

func run(configPath, listen, adminAddr string) error {
	cfg := configYAML
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return err
		}
		cfg = data
	}
	a, err := newApp(cfg, os.LookupEnv, time.Now)
	if err != nil {
		return err
	}
	appServer := &http.Server{Addr: listen, Handler: a.handler}
	adminServer := &http.Server{Addr: adminAddr, Handler: a.gateway.AdminHandler()}
	serveErr := make(chan error, 2)
	go func() { serveErr <- appServer.ListenAndServe() }()
	go func() { serveErr <- adminServer.ListenAndServe() }()

	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)

	for {
		select {
		case <-hup:
			var err error
			if configPath != "" {
				err = a.gateway.ReloadFile(configPath)
			} else {
				err = a.gateway.ReloadYAML(configYAML)
			}
			if err != nil {
				log.Printf("reload failed: %v", err)
			}
		case err := <-serveErr:
			return err
		case <-stop:
			a.gateway.SetReady(false)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := appServer.Shutdown(ctx); err != nil {
				log.Printf("application shutdown: %v", err)
			}
			if err := adminServer.Shutdown(ctx); err != nil {
				log.Printf("admin shutdown: %v", err)
			}
			return a.gateway.Close()
		}
	}
}
