# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v0.2.0] - 2026-11-11

This release is the core module `v0.2.0` and the Redis module `redis/v0.2.0`. The binary is built from the Redis module and carries every change below.

### Breaking

Each entry names the setting that restores the earlier behavior. A configuration that already sets a key keeps its value and is not affected by that entry.

- `global.shutdown_delay` now defaults to `5s` (was `0s`). On `SIGTERM` or `SIGINT` each instance reports not ready on `/readyz` and keeps serving for 5 s before its listeners stop, so a load balancer can remove it first. Restore the previous behavior with `global.shutdown_delay: 0s`.
- `global.drain_timeout` now defaults to `20s` (was `30s`), so `shutdown_delay + drain_timeout + 5s` fits the default Kubernetes termination grace period of 30 s. Restore the previous value with `global.drain_timeout: 30s`.
- The binary now binds a probe listener by default on `:9902` on all interfaces. It serves only `/livez` and `/readyz` without a token (and `/metrics` when `admin.probes.metrics` is `true`), and `/readyz` returns `503` through `global.shutdown_delay` and the drain. The management endpoints stay on `admin.address` (default `127.0.0.1:9901`). Each instance binds its own probe listener. Where port 9902 conflicts or is unwanted, restore the previous behavior with `admin.probes.enabled: false`, or move it with `admin.probes.address`.
- A non-loopback `admin.address`, such as `0.0.0.0:9901` or `:9901`, now needs at least one `admin.auth.keys` entry, or the configuration is rejected at `admin.auth.keys`. On such an address `/config`, `/upstreams` and `/metrics` answer only a request whose `Authorization: Bearer` token matches a key (`401` without a token, `403` for a wrong one). `/livez`, `/readyz` and cache purge are unchanged. Restore the previous behavior with `admin.auth.management: none`, which logs a warning on every load and reload.
- In the Go API, `config.Global.ShutdownDelay` is now a `*config.Duration` (was `config.Duration`): nil takes the default and an explicit zero is kept. Code that sets it assigns a pointer, for example `d := config.Duration(0); cfg.Global.ShutdownDelay = &d`. This changes Go source only: it has no YAML setting to restore, and YAML configurations are not affected by the type change.

To restore the earlier defaults (no shutdown delay, a 30 s drain, no probe listener, and management endpoints without a token on any `admin.address`), merge this block into the configuration:

```yaml
global:
  shutdown_delay: 0s
  drain_timeout: 30s
admin:
  probes:
    enabled: false
  auth:
    management: none
```

### Added

- `make reload-load` (opt-in with `MICROGATEWAY_HEAVY=1`) runs gateways from `bin/microgateway` and in-process gateways under a paced loopback load. It reloads them through the file watcher, SIGHUP and the Go API, and checks graceful shutdown under load. It prints `PASS` only with zero failed requests, every reload effective within 2 s, exact reload counters and the same outcome on every path.
- `admin.probes.enabled` (default `true`), `admin.probes.address` (default `:9902`) and `admin.probes.metrics` (default `false`) configure the probe endpoint, and `admin.auth.management` (`auto`, `required` or `none`, default `auto`) selects when the management endpoints need a token. `Gateway.ProbeHandler` serves `/livez` and `/readyz`, and `/metrics` when `admin.probes.metrics` is `true`, without a token. The Go API adds `config.AdminProbes`, `config.Admin.Probes`, `config.AdminAuth.Management`, `config.IsLoopbackAddress` and `(*config.Admin).ManagementAuthRequired`.
- Pushing a core tag `vX.Y.Z` publishes the container image `ghcr.io/ok0/go-microgateway` for `linux/amd64` and `linux/arm64` in one manifest, tagged `X.Y.Z`, `X.Y` and `latest` (a prerelease gets only its full version). After the push, the release workflow checks both platforms, the version and the non-root user. `make workflow-lint` runs actionlint over the workflows.
- A Helm chart in `deploy/helm/microgateway` runs the gateway as two or more stateless replicas with a Deployment, a Service, a ConfigMap that holds the gateway configuration, a PodDisruptionBudget and a HorizontalPodAutoscaler. The chart version and `appVersion` are the release version without the `v`, and its default image is `ghcr.io/ok0/go-microgateway:<appVersion>`. The Kubernetes guide `docs/kubernetes.md` covers its values, Redis wiring, probes, shutdown timing and configuration updates.
- `make k8s-check` lints and renders the chart with its default values and with `deploy/helm/microgateway/ci/redis-values.yaml`, validates both renders against the Kubernetes 1.35.0 schemas with kubeconform, and checks them with `internal/k8scheck`, which also runs `bin/microgateway validate` on each rendered configuration. With `HELM3` set it also checks that Helm 3 renders the same bytes as Helm 4. It needs the network and is not part of `make check`.
- CI adds the `k8s` job, which runs `make k8s-check` with Helm 3 and Helm 4, and the `actionlint` job, which runs `make workflow-lint`. The `docker` job also checks that the image user `65532:65532` is `nonroot`.

### Changed

- The container image runs as the numeric user `65532:65532` (was `nonroot`) and exposes `8080` and the probe port `9902` (was `9901`). The management port stays on loopback and is not exposed. The Dockerfile cross-compiles on the build platform.

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
