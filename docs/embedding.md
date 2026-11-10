# Embedding guide

The core module `github.com/ok0/go-microgateway` is a library. A `Gateway` is an `http.Handler` built from a validated configuration, and the core module does not import a Redis client. Add it with `go get github.com/ok0/go-microgateway`. The Redis store type is in the separate module `github.com/ok0/go-microgateway/redis`, which you add only when you use Redis. The configuration keys are described in the [configuration reference](configuration.md). The stages and user middleware placement are described in the [middleware guide](middleware.md), and the cache in the [caching guide](caching.md).

The directory `examples/embed` holds a program that uses most of the features in this guide. Its configuration is `examples/embed/gateway.yaml`. Run it from the repository root with `go run ./examples/embed`. The Redis-backed program is `redis/examples/redisgateway`. The Go excerpts below are copied from these two programs, and each excerpt is contiguous in its file.

## Build a gateway from YAML

`NewFromYAML` parses a configuration document, validates it and returns a gateway that serves it. The options are the same as for `New`. When the document is not valid, `NewFromYAML` returns one `*config.ValidationError` that lists every problem, including the store and middleware types that the options do not register. Use `errors.As` to read it. `NewFromYAML` does not read files. Read the file yourself, as the example does when it is started with `-config`.

```go
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
```

## Build a gateway from a Go configuration

`New` takes a `*config.Config` that the program builds in Go, instead of YAML bytes. `New` copies the configuration, applies the defaults to the copy and validates it, so later changes to the original have no effect. A nil configuration is an error. `Gateway.Config` returns the active configuration. Treat the result as read-only.

## Validate a configuration

`Validate` checks a `*config.Config` against the options that a gateway built from it would use, and returns the problems that `New` would reject. It never calls a store factory, opens a connection or changes the configuration. A nil configuration is an error. Use it in a build pipeline or before a reload. To validate a YAML document, parse it with the `config` package and pass the result to `Validate`.

## Mount the gateway in net/http

`Gateway.ServeHTTP` serves the data traffic, so the gateway can be mounted on any `http.ServeMux` or behind other middleware. The example mounts its own application route beside the gateway on one mux and sends `/` to the gateway.

`Gateway.AdminHandler` returns the admin handler as a separate `http.Handler`. Serve it on its own listener, and never on the listener that carries public traffic. The admin handler accepts a cache purge only with a key from `admin.auth.keys` in the configuration.

`Gateway.ProbeHandler` returns the probe handler as a second `http.Handler`. It serves `GET` and `HEAD` on `/livez` and `/readyz` without a credential, and `/metrics` while `admin.probes.metrics` is true. It never serves configuration, upstream state or cache purge, so it can sit on a listener that a load balancer or orchestrator probes, while `AdminHandler` stays on a restricted one. Like `AdminHandler`, it reads the active configuration on every request, so a reload takes effect without rebuilding it. When `admin.auth.management` is `required`, or `auto` with a non-loopback `admin.address`, `AdminHandler` demands a bearer token from `admin.auth.keys` for `/config`, `/upstreams` and `/metrics`; see the [configuration reference](configuration.md) for the `admin` block.

```go
	appServer := &http.Server{Addr: listen, Handler: a.handler}
	adminServer := &http.Server{Addr: adminAddr, Handler: a.gateway.AdminHandler()}
	serveErr := make(chan error, 2)
	go func() { serveErr <- appServer.ListenAndServe() }()
	go func() { serveErr <- adminServer.ListenAndServe() }()
```

## Options

Every option is a value of the type `Option`. Options apply in the order they are given.

- `WithLogger` sets the `*slog.Logger` that the gateway logs to. The messages are listed in the [operations guide](operations.md).
- `WithEnvLookup` replaces the lookup used for `${VAR}` substitution in the configuration. Without it, substitution reads the process environment.
- `WithMiddleware` registers a middleware type that `middlewares[].type` can reference. See [User middleware](#user-middleware).
- `WithStoreType` registers a store type that `stores[].type` can reference. See [Custom store types](#custom-store-types).
- `WithKeySetFetcher` replaces the HTTP fetching of JWKS documents with a `KeySetFetcher`. A nil fetcher is an error. See [Custom key set fetcher](#custom-key-set-fetcher).
- `WithHTTPClient` sets the client that the default JWKS fetcher uses. It has no effect when `WithKeySetFetcher` is also given. A nil client is an error.

## User middleware

`Middleware` is the type `func(http.Handler) http.Handler`. A `MiddlewareFactory` builds one from a `MiddlewareParams` value, which holds the `name` and `type` of the definition and its `params`. `WithMiddleware` registers the factory under a type name. The example registers the type `audit`.

`MiddlewareParams.Decode` strictly decodes the params into a value that the factory provides. An unknown key is an error that names the key. A type mismatch names only the YAML tag and the target type. Neither error contains a parameter value. `MiddlewareParams.String`, `MiddlewareParams.GoString`, `MiddlewareParams.Format`, `MiddlewareParams.LogValue` and `MiddlewareParams.MarshalJSON` show only the name and the type, so a `MiddlewareParams` can be logged safely.

```go
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
```

The configuration refers to the middleware by name. This partial document is a fragment of the configuration; the complete one is `examples/embed/gateway.yaml`.

```yaml
middlewares:
  - name: audit
    type: audit
    params:
      header: X-Audit-Consumer

routes:
  - name: api
    match:
      path: /api/
    upstream: backend
    middlewares:
      - audit
```

A route's `middlewares` run before the built-in stages. If the route sets a `pipeline`, the middlewares it does not name run before the pipeline, and a middleware named in the pipeline runs at its position there. The [middleware guide](middleware.md) gives the full order.

## Request identity, request IDs and path parameters

`Identity` is the authenticated caller of a request, and it has the fields `Provider`, `Consumer`, `Subject`, `Claims` and `Metadata`. `IdentityFromContext` returns the identity and `true` when authentication ran for the request. It returns `false` when no authentication ran, for example on a route without an `auth` stage.

`RequestIDFromContext` returns the request ID. The gateway uses the incoming request-ID header when its value is a valid ID, and generates one otherwise. The header name is `global.request_id_header`, which defaults to `X-Request-ID`. The ID is also written to the response.

`PathParam` returns the value of a path parameter that the matched route captured, such as `{id}` in a route pattern. It returns an empty string when the route captured no parameter of that name.

## Reload

`Gateway.Reload` makes a `*config.Config` the active configuration. `Gateway.ReloadYAML` does the same with YAML bytes, and `Gateway.ReloadFile` with the document at a path. A reload makes the new configuration active when it is valid and differs from the active one. Otherwise the previous configuration stays active and the error is returned. `Gateway.Reload` copies the configuration it is given.

`Gateway.WatchFile` reloads from a path whenever its content changes. It uses the debounce and poll settings of the active configuration, and it also watches the key files that the configuration references. It blocks until the context is done and then returns nil. It returns an error at once when the path cannot be stat'ed, and `ErrClosed` when the gateway is closed while it watches.

The reload methods return `ErrClosed` after `Gateway.Close`. A reload changes the gateway's own configuration only. It does not re-bind the listeners that the program started, and it does not change the settings that need a restart. The [operations guide](operations.md) lists those settings.

The example reloads on `SIGHUP`. It reloads from the file when a path was given, and from the embedded document otherwise.

```go
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
```

## Readiness, shutdown and Close

`Gateway.SetReady` sets the value that the admin `/readyz` endpoint reports, and `Gateway.Ready` returns it. Setting the value to false does not stop the data traffic. The example sets it to false first, then shuts down its HTTP servers, then closes the gateway.

```go
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
```

`Gateway.Close` stops reloads and cancels the background cache refreshes. It releases the gateway's resources once the requests in flight and those refreshes have finished. A request that arrives after `Close` receives `503` with the code `service_unavailable`. `Close` is idempotent, and a repeated call returns the same result as the first. Call `Close` after the HTTP servers have stopped accepting requests, as the example does.

## Purge

`Gateway.Purge` removes the cached responses that a `PurgeRequest` selects. It runs the same service as the admin purge endpoint, without a credential. A `PurgeRequest` selects by exactly one of `URLs`, `Tags` or `All`, and `Soft` requests a soft purge. The [caching guide](caching.md) describes the purge modes.

A `PurgeResult` reports `Type`, `Soft`, `Accepted`, `Affected`, `AffectedKnown` and `Scope`. The scope is `instance` for the built-in memory cache, which is per instance, so call `Gateway.Purge` on every instance. The scope is `store` for a named store, and a purge of a named store is shared by every instance that uses that store.

`Gateway.Purge` checks these conditions in this order, and returns the first error that applies: `ErrClosed` after `Close`, `ErrCacheDisabled` when no route enables the cache, an error that wraps `ErrInvalidPurgeRequest` for a malformed request, an error that wraps `store.ErrUnavailable` when the cache store is down, and the error of the context. A failure part-way can leave some batches applied, and repeating the purge is safe.

## Custom key set fetcher

`KeySetFetcher` is an interface with one method, `KeySetFetcher.FetchKeySet(ctx, provider, url)`, which returns the raw JWKS JSON of a JWT provider. The gateway calls it with the configured URL after environment substitution, and it bounds each call with the provider's `jwks` timeout through the context. A panic in the fetcher is recovered and treated as a failed fetch. The gateway parses, caches and validates the keys itself, separately in each instance.

The example's fetcher allows one host over HTTPS and refuses every other URL. Its error messages do not contain the URL.

```go
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
```

## Custom store types

`WithStoreType` registers a store type that `stores[].type` can reference. Its `caps` argument lists the features the backends provide. `store.CapRateLimit` lets a rate-limit route use the store through `rate_limit.store`, and `store.CapCache` lets the response cache use it through `cache.store`.

A `StoreFactory` builds a `store.Backend` from a `StoreParams` value. `StoreParams` holds the entry's `Name`, its `Type`, `Redis` (a copy of the entry's `redis` block, or nil when the entry has none) and `Logger`, which carries the store name. `StoreParams.Decode` reads the entry's custom params. Like `MiddlewareParams`, `StoreParams.String`, `StoreParams.GoString`, `StoreParams.Format`, `StoreParams.LogValue` and `StoreParams.MarshalJSON` show only the name and the type.

The gateway calls a factory only for a store entry that a rate-limit route or the response cache references. It calls the factory when the gateway is built, and again only when the entry's type, `redis` block or params change. The gateway closes the backend that the factory returned, once, after the requests that use it have drained. A program that shares state between gateways returns a separate handle from each call.

The example's store is an in-memory rate limiter with a key cap. Its factory decodes the `max_keys` param.

```go
func limiterFactory(now func() time.Time) microgateway.StoreFactory {
	return func(p microgateway.StoreParams) (store.Backend, error) {
		params := struct {
			MaxKeys int `yaml:"max_keys"`
		}{MaxKeys: defaultMaxKeys}
		if err := p.Decode(&params); err != nil {
			return nil, err
		}
		if params.MaxKeys < 1 {
			return nil, errors.New("max_keys must be at least 1")
		}
		return newMemoryLimiter(now, params.MaxKeys).handle(), nil
	}
}
```

The configuration declares the store and a route uses it. This partial document is a fragment of `examples/embed/gateway.yaml`.

```yaml
stores:
  - name: local-limits
    type: example-memory
    params:
      max_keys: 10000

routes:
  - name: api
    match:
      path: /api/
    upstream: backend
    rate_limit:
      rate: 2
      per: 1m
      burst: 2
      store: local-limits
```

The `store/storetest` package holds conformance suites for rate-limit and cache backends. Run them against a new backend before you use it.

## Redis store

The package `github.com/ok0/go-microgateway/redis` provides `redis.Option`, which registers the store type `redis` for rate limits and for the response cache. Pass it to `NewFromYAML` or `New` like any other option. The Redis module is the only module that imports a Redis client, so a program that does not use Redis does not need it.

```go
	gw, err := microgateway.NewFromYAML(data, redis.Option(), microgateway.WithLogger(logger))
	if err != nil {
		return err
	}
```

A configuration uses the type through a `stores` entry with a `redis` block. This partial document is a fragment of `examples/redis.yaml`.

```yaml
stores:
  - name: shared
    type: redis
    redis:
      mode: standalone
      addresses: ["${REDIS_ADDR}"]
      password: "${REDIS_PASSWORD}"
      key_prefix: example-gateway
```

The Redis-backed program shuts down in the order the operations guide describes. It sets readiness to false, drains the HTTP servers within the configured drain timeout, and then closes the gateway.

```go
func shutdown(gw *microgateway.Gateway, gwSrv, adminSrv *http.Server) error {
	gw.SetReady(false)
	ctx, cancel := context.WithTimeout(context.Background(), gw.Config().Global.DrainTimeout.Std())
	defer cancel()
	err := gwSrv.Shutdown(ctx)
	err = errors.Join(err, adminSrv.Shutdown(ctx))
	return errors.Join(err, gw.Close())
}
```

The Redis settings, the failure policies and the multi-instance design are described in the [multi-instance guide](multi-instance.md). The keys are listed in the [configuration reference](configuration.md).

## Error values

The root package exports three error values. Compare them with `errors.Is`.

- `ErrClosed` is returned by `Gateway.Reload`, `Gateway.ReloadYAML`, `Gateway.ReloadFile`, `Gateway.WatchFile` and `Gateway.Purge` after `Gateway.Close`.
- `ErrCacheDisabled` is returned by `Gateway.Purge` when no route of the active configuration enables the cache. It is the cache package's error, re-exported.
- `ErrInvalidPurgeRequest` is wrapped by the error that `Gateway.Purge` returns for a malformed request. Nothing is purged in that case. It is the cache package's error, re-exported.

`Validate`, `New` and `NewFromYAML` return a `*config.ValidationError` for an invalid configuration. Use `errors.As` to read the list of problems. A store that is unavailable at purge time is reported by the wrapped `store.ErrUnavailable`.
