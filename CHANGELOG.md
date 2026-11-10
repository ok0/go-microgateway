# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

- `global.shutdown_delay` now defaults to `5s` (was `0s`). On `SIGTERM` or `SIGINT` each instance reports not ready on `/readyz` and keeps serving for 5 s before its listeners stop, so a load balancer can remove it first. Restore the previous behavior with `global.shutdown_delay: 0s`.
- `global.drain_timeout` now defaults to `20s` (was `30s`), so `shutdown_delay + drain_timeout + 5s` fits the default Kubernetes termination grace period of 30 s. Restore the previous value with `global.drain_timeout: 30s`.
- In the Go API, `config.Global.ShutdownDelay` is now a `*config.Duration` (was `config.Duration`): nil takes the default and an explicit zero is kept. Code that sets it assigns a pointer, for example `d := config.Duration(0); cfg.Global.ShutdownDelay = &d`.

### Added

- `make reload-load` (opt-in with `MICROGATEWAY_HEAVY=1`) runs gateways from `bin/microgateway` and in-process gateways under a paced loopback load. It reloads them through the file watcher, SIGHUP and the Go API, and checks graceful shutdown under load. It prints `PASS` only with zero failed requests, every reload effective within 2 s, exact reload counters and the same outcome on every path.

## [v0.1.1] - 2026-11-09

### Fixed

- The release checks read the version from `redis/go.mod` and the release tags, so `make check` and `make release-check` pass for every later release, and each release keeps its own section in this file.

## [v0.1.0] - 2026-11-06

### Added

- Routing by host, path, method and header, with per-route path rewriting and header rules.
- Upstream load balancing, DNS-discovered hosts, and passive and active health checks.
- Per-host circuit breakers, retries limited by a retry budget, and idempotency keys.
- Timeouts for requests and upstream connections.
- Authentication with JWTs and API keys stored as SHA-256 hashes, with scope and role checks.
- Rate limiting with memory and Redis stores, and explicit failure policies.
- An RFC 9111 response cache with purge by URL, tag or all entries, and request coalescing.
- Hot reload on `SIGHUP` or on a change to the configuration file, keeping the running configuration when a reload fails.
- An admin endpoint with liveness, readiness, redacted configuration, upstream status, Prometheus metrics and authenticated cache purge.
- TLS listeners, HTTP/2 over ALPN and cleartext HTTP/2 (h2c).
- The `microgateway` binary with `run`, `validate` and `version` commands.
- The Redis module, which provides the Redis-backed stores.
- Packaging: a Makefile, a Dockerfile for a distroless non-root image, and a CI workflow.
- Cache stores have a `Delete` operation that removes one stored variant without recording a purge. Custom `store.Cache` implementations must implement it; the store conformance tests cover it, and failures are counted in `store_errors` with the `delete` operation.
- `microgateway version` reports the module version when the binary was installed with `go install` at a release version, and `dev` for a build without a version.
- Release targets: `make release-prepare` sets the core version of the Redis module and of `go.work`, `make release-check` runs the read-only release checks offline, and `make release-verify` installs a published release in a clean environment (opt-in, needs the network).

### Changed

- Rewrite prefixes (`strip_prefix`, `replace_prefix.from` and `replace_prefix.to`) must be canonical paths.
- The access log has an `error` field for requests rejected because of their path.
- The Redis module requires the core module at the same release version, and no `go.mod` has a `replace` directive. A committed `go.work` builds the Redis module against the core in a checkout. Both modules are released together with one version number, tagged `vX.Y.Z` and `redis/vX.Y.Z`.
- Once a JWKS key set is loaded, a token whose `kid` is in it is verified without waiting on a key set refresh. Only the first load and a `kid` that is not in the set wait for a fetch, bounded by the JWKS fetch timeout.

### Security

- Request paths are canonicalized before routing. Ambiguous paths get `400 invalid_path`.
- Equivalent path spellings share one route, one cache entry and one rate-limit bucket.
- A 304 revalidation applies the storability rules to the merged response. A 304 that makes it non-storable, for example with `private` or `no-store`, removes the stored variant, is not shared with coalesced requests and is reported as `stored=false`.
