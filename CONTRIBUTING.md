# Contributing

This guide describes how to build, test and check changes to go-microgateway, and the checks that are run before a release.

## Prerequisites

- Go 1.27 or later. CI tests the `go.mod` floor, Go 1.27.0, and the current stable release; every other job uses go1.27.3, which you reproduce with `GOTOOLCHAIN=go1.27.3`. Releases are built with Go 1.27.
- Docker, optional. It is needed only for `make docker`.
- Redis 6.2 or later, optional. It is needed only for the Redis integration tests, which CI runs on Redis 6.2 and 7.4.
- golangci-lint v2.14.0, for `make lint`.
- The network, for `make workflow-lint`. It runs the pinned actionlint with `go run`, so nothing is added to a `go.mod`.
- Helm v4.3.1 and kubeconform v0.8.0, for `make k8s-check`, which also needs the network for the Kubernetes 1.35.0 schemas. Set `HELM` or `KUBECONFORM` when the tool is not on `PATH`. Helm v3.22.1, set as `HELM3`, adds the Helm 3 comparison that CI runs.

## Building and testing

The repository holds two Go modules: the core module at the root and the Redis module in `redis/`. Core commands and Redis module commands run in different modes; see [Command modes](#command-modes).

The Makefile wraps the common commands:

- `make build` builds the binary into `bin/microgateway`.
- `make test` runs the tests of both modules.
- `make race` runs the tests with the race detector.
- `make vet` runs `go vet` in both modules.
- `make cover` runs the tests with coverage and enforces the coverage gate.
- `make cover-ci` runs `make cover` with verbose redis tests and keeps the log in `bin/cover/cover.log`; `make cover-times` then lists its slowest tests. CI's coverage job uses them.
- `make lint` runs golangci-lint v2.14.0 in both modules.
- `make workflow-lint` runs actionlint over every workflow in `.github/workflows/`. It is not part of `make check`; CI runs it in the `actionlint` job.
- `make k8s-check` builds `bin/microgateway`, lints and renders the Helm chart with its default values and with `deploy/helm/microgateway/ci/redis-values.yaml` into `bin/k8s/`, validates both renders against the Kubernetes 1.35.0 schemas with kubeconform, and checks them with `internal/k8scheck`, which also runs `bin/microgateway validate` on each rendered configuration. It then checks the render failures, the merging of `config` and the install notes. With `HELM3` set it also checks that Helm 3 lints the chart and renders the same bytes as Helm 4. It is not part of `make check`; CI runs it in the `k8s` job.
- `make tidy-check` checks that `go mod tidy` changes nothing in either module; see [Command modes](#command-modes) for the Redis module.
- `make hygiene` runs the repository hygiene checks, which include the copied-source check.
- `make check` runs `vet`, `cover`, `lint`, `tidy-check` and `hygiene`.

The same commands by hand, from the repository root for the core module and from `redis/` for the Redis module:

```sh
GOWORK=off go build ./...
GOWORK=off go vet ./...
GOWORK=off go test ./...
cd redis && go test ./...
```

The repository tests read the git history and tags. `TestCommitMessageHygiene` scans every commit and `TestReleaseCheck` checks the release tag pairs, so both need a full clone with all history and all tags. A shallow clone (`git clone --depth 1`, or `actions/checkout` without `fetch-depth: 0`) has one commit and few or no tags; on it both tests fail and name the fix. Run `git fetch --unshallow --tags` to complete a shallow clone. CI checks out every job with `fetch-depth: 0`.

## Command modes

Core commands run from the repository root with `GOWORK=off`. The core builds from its own `go.mod` alone, as a consumer's build does, and the dependency check proves that the core has no Redis dependency.

Redis module commands run from `redis/` with the workspace on. The committed `go.work` uses `.` and `./redis`, and maps the core version that `redis/go.mod` requires to the checkout. The Redis module therefore builds and tests against the working-tree core, without a published core or network. `go` finds `go.work` by itself from `redis/`, and the Makefile sets `GOWORK=$(CURDIR)/go.work` explicitly. If your environment sets `GOWORK=off`, set `GOWORK` to the repository's `go.work` for Redis module commands.

Never run a `./...` pattern from the root in workspace mode. It then spans both modules, and core checks would include the Redis module and its client. `GOFLAGS=-mod=mod` is refused in workspace mode. `go.work.sum` is ignored and never committed.

A Redis module command with `GOWORK=off` resolves the core from the module proxy, so it works only for a published core version.

`make tidy-check` runs `go mod tidy -diff` in the core and `scripts/tidy-redis.sh` for the Redis module. The script tidies a temporary copy of `redis/go.mod` and `redis/go.sum` against the checkout, offline, and ignores the core's own `go.sum` lines.

No tagging, pushing or release steps are described here.

## Test rules

- Time comes from the injected clock. A test must not decide anything from the wall clock, and must not call `time.Sleep` to wait for a result.
- Waits for an asynchronous result go through `internal/testwait`.
- Tests of the gateway as a whole live in `internal/e2e`.
- HTTP/2 server limits are tested under `testing/synctest`.

## Two-module rule

The core module never imports the Redis module or a Redis client. The Redis store, `redis.Option()` and the `microgateway` binary that links the Redis store live only in `redis/`.

## No copied or vendored source

Do not copy or vendor source code from other packages, including code from a fork or a snippet from another project. If a dependency is missing a feature, propose an alternative instead. The hygiene checks reject files that carry the text of a known open-source license grant, except the root `LICENSE` file.

## Doc comments and lint

Every exported identifier has a doc comment. Lint runs through `make lint` with golangci-lint v2.14.0 in both modules.

## Commit messages

Commit messages follow Conventional Commits, for example `feat(cache): ...`, `fix(upstream): ...` or `docs: ...`.

## Heavy checks

Some checks are opt-in because they are slow, need a real network, or need an external service. They are skipped unless their environment variable is set:

- `MICROGATEWAY_HEAVY=1` enables the long benchmark and fuzz runs, the soak tests, the integration tests, the release install check and the hot reload load check.
- `MICROGATEWAY_SOAK` sets the soak duration, for example `60s`.
- `RELOAD_LOAD_DURATION`, `RELOAD_LOAD_RATE`, `RELOAD_LOAD_CONCURRENCY` and `RELOAD_LOAD_INSTANCES` set the length (default 60s, 30s to 10m), total request rate (default 200 per second, at most 1000), workers (default 8, at most 32) and gateway count (default 2, at most 4) of `make reload-load`.
- `MICROGATEWAY_LOAD=1` enables the load tests.
- `MICROGATEWAY_CACHE_FULL=1` enables the full cache test matrix in `redis`.
- `MICROGATEWAY_REDIS_ADDR` sets the address of a Redis 6.2 or later server for the integration tests.
- `MICROGATEWAY_REDIS_CLUSTER` and `MICROGATEWAY_REDIS_SENTINEL` select the cluster and Sentinel topologies of the integration tests.
- The `realnet` build tag enables the tests that use the real network, including the default-exposure test, which needs a non-loopback IPv4 interface address and fails without one.

`make perf`, `make soak`, `make fuzz`, `make integration`, `make release-verify`, `make reload-load` and `make realnet` refuse to run without the variables they need and say which one is missing.

## Release verification

Run these checks before a release. The heavy checks are not part of `make check` and are run by the owner of the release. Record the results in the CHANGELOG entry of the release.

| Check | Command | Environment | Expected result | Heavy |
|-------|---------|-------------|-----------------|-------|
| 1. Benchmarks | `make perf` with `MICROGATEWAY_HEAVY=1` | Local machine, Go 1.27 | `overhead-p50-ns` below 150000 for the full chain, below 50000 for a cache hit, and router below 2000 ns/op | Yes |
| 2. Soaks | `make soak` with `MICROGATEWAY_HEAVY=1 MICROGATEWAY_SOAK=60s` | Local machine | `TestSoakFullChain` and `TestSoakCacheMixed` pass, and the goroutine and heap targets hold | Yes |
| 3. Fuzzing | `make fuzz` with `MICROGATEWAY_HEAVY=1` | Local machine | Every `Fuzz` target runs for 30 s with no failure | Yes |
| 4. Idle-timer stress | `GOWORK=off go test -race -count=20 -run 'TestCloseStopsIdleTimers\|TestCloseInUseConnClosedByReadLoop' ./internal/upstream`, run while three other `GOWORK=off go test -race -count=1 ./internal/upstream` runs execute concurrently | Local machine | Zero failures | Yes |
| 5. Cache and purge load | `MICROGATEWAY_LOAD=1 GOWORK=off go test -race -count=1 -run 'TestCoalesceScale\|TestCacheMixedLoad\|TestPurgeLoad' ./internal/cache`; the same variables with `-run 'TestPurgeMarkerCapDefault\|TestInstancesMixedLoad\|TestHostGaugesChurnLoad' ./internal/e2e`; `MICROGATEWAY_LOAD=1 go test -race -count=1 -run TestMixedMultiInstanceLoad .` in `redis`; `MICROGATEWAY_CACHE_FULL=1 go test -count=1 ./...` in `redis`; `GOWORK=off go test -race -count=20 ./internal/cache ./internal/coalesce ./internal/memstore ./store/... ./internal/storeguard`; and the full-length `go test -run '^$' -bench . ./internal/cache ./internal/memstore` plus the same in `redis` | Local machine | Every command passes | Yes |
| 6. Server and HTTP/2 | `GOWORK=off go test -race -count=20 ./internal/server ./internal/memnet`; three concurrent `GOWORK=off go test -race -count=3 ./internal/e2e`; and `go test -race -count=3` of `./internal/cli` with `./redis/cmd/microgateway` and `./redis`, run from `redis/` at the same time | Local machine | Every command passes | Yes |
| 7. Real Redis | `make integration` with `MICROGATEWAY_HEAVY=1 MICROGATEWAY_REDIS_ADDR=<host:port>`; `go test -race -count=5 -tags integration ./...` in `redis` | Redis 6.2 or later, empty database such as 15 | `TestIntegrationSuite` and `TestIntegrationTwoInstances` pass; cluster and Sentinel runs pass with `MICROGATEWAY_REDIS_CLUSTER` and `MICROGATEWAY_REDIS_SENTINEL` set | Yes |
| 8. Diagrams | Render the nine Mermaid diagrams in the README and the guides | A GitHub-compatible Mermaid renderer | Every diagram renders without a syntax error | Yes (manual) |
| 9. Real network | `make realnet`, and `GOWORK=off go test -race -count=5 -tags realnet ./internal/e2e ./internal/cli`, and `-run TestConnectTimeoutRealClockTCP` | Local machine with the VPN off and a non-loopback IPv4 interface address | Every command passes | Yes |
| 10. Container | `make docker`, then `docker run --rm microgateway:<version> version`, then a run with a mounted configuration | Docker | The version output matches `bin/microgateway version`, and the process runs as the non-root user `65532:65532` | Yes |
| 11. Lint and toolchain | `make lint` with golangci-lint v2.14.0 in both modules; build and test with a Go 1.27 toolchain | Go 1.27 toolchain | Every command passes | No |
| 12. Release | The steps in [Releasing](#releasing), after all checks pass | Repository owner | The core tag `vX.Y.Z`, then a commit with the core's lines in `redis/go.sum`, then the tag `redis/vX.Y.Z`, all pushed | No |
| 13. Hot reload under load | `make reload-load` with `MICROGATEWAY_HEAVY=1` | Local machine | `PASS` with zero failed requests and every reload latency at most 2 s, for the file watch, SIGHUP and API drivers and the shutdown under load | Yes |
| 14. Workflow lint | `make workflow-lint` | Go 1.27 and the network | actionlint reports no problem in any workflow | No |
| 15. Kubernetes artifacts | `make k8s-check`, with `HELM3` set to Helm v3.22.1 | Helm v4.3.1, kubeconform v0.8.0, Go 1.27 and the network; the target builds the binary | Lint, render, schema validation and the manifest checks pass for the default and the Redis values, and Helm 3 renders the same bytes | No |
| 16. Published image | After the core tag is pushed and its release workflow succeeded: `docker buildx imagetools inspect ghcr.io/ok0/go-microgateway:<version>`, `docker run --rm ghcr.io/ok0/go-microgateway:<version> version` and `docker image inspect --format '{{.Config.User}}' ghcr.io/ok0/go-microgateway:<version>`, where `<version>` is `X.Y.Z` without the `v` | Docker; the package is public (see [Releasing](#releasing)) | The manifest lists `linux/amd64` and `linux/arm64`, `version` prints `vX.Y.Z`, and the user is `65532:65532`, so the image runs as non-root | Yes (manual) |

Each check names whether it is heavy. Checks marked heavy need the environment they name and are not run in continuous integration.

## Releasing

The core module and the Redis module are released together with one version number. Release `vX.Y.Z` is the tag `vX.Y.Z` for the core module and the tag `redis/vX.Y.Z` for the Redis module, and `redis/go.mod` at `redis/vX.Y.Z` requires the core at `vX.Y.Z`. The first release is `v0.1.0`. No `replace` directive is ever committed.

Steps 3 to 8 are user steps for the repository owner. Automated agents never tag, push or add remotes.

1. Run `make release-prepare VERSION=vX.Y.Z`. It sets the core version that `redis/go.mod` requires and the core version that `go.work` replaces to `vX.Y.Z`, together and offline, and creates no tag.
2. Move the `Unreleased` entries of `CHANGELOG.md` under a new `## [vX.Y.Z] - YYYY-MM-DD` heading and leave an empty `## [Unreleased]` heading above it. Run `make check`, `make workflow-lint` and `make release-check VERSION=vX.Y.Z`, then commit.
3. User step: tag that commit for the core and push the tag with `git tag vX.Y.Z` and `git push origin vX.Y.Z`. Pushing the core tag starts the release workflow `.github/workflows/release.yml`. It builds the image for `linux/amd64` and `linux/arm64` and publishes one manifest to `ghcr.io/ok0/go-microgateway`, tagged with the lines that `sh scripts/image-tags.sh vX.Y.Z` prints: `X.Y.Z`, `X.Y` and `latest` (a prerelease gets only its full version). It fails unless the published manifest lists both platforms and each platform's image runs as `65532:65532` and prints `vX.Y.Z`. It uses only the workflow's own token.
4. User step: in `redis/`, run `GOWORK=off go mod tidy`, which resolves the published core and adds its lines to `redis/go.sum`, and commit.
5. User step: tag that commit for the Redis module and push it with the branch with `git tag redis/vX.Y.Z` and `git push origin main redis/vX.Y.Z`.
6. User step: run `MICROGATEWAY_HEAVY=1 make release-verify VERSION=vX.Y.Z`. It needs the network and free ports 8080, 9901 and 9902. It installs with the README command from an empty `GOPATH` and module cache outside the checkout, checks that `microgateway version` prints `vX.Y.Z`, and validates and runs `examples/redis.yaml`.
7. User step, once, after the first image is published: make the package public. New GitHub packages are private, so anonymous pulls of `ghcr.io/ok0/go-microgateway` fail until then. In the package settings, change the visibility to public; this cannot be undone.
8. User step: check the published image (check 16 of [Release verification](#release-verification)). `docker buildx imagetools inspect ghcr.io/ok0/go-microgateway:X.Y.Z` lists `linux/amd64` and `linux/arm64`. `docker run --rm ghcr.io/ok0/go-microgateway:X.Y.Z version` prints `vX.Y.Z`. `docker image inspect --format '{{.Config.User}}' ghcr.io/ok0/go-microgateway:X.Y.Z` prints `65532:65532`, so the image runs as non-root.

The core is tagged first because `redis/go.sum` must record the hash of the published core, which exists only once the core tag is pushed. Between steps 1 and 4, `redis/go.sum` lacks the core's lines; workspace builds do not need them. `make release-check` is read-only and offline: it checks the versions in `redis/go.mod` and `go.work`, the module layout, the pairing of the core and Redis tags, the README install command, and the CHANGELOG: its section order and format, a section for every core tag, and a section for `vX.Y.Z`. `make check` does not need the new section, so it also passes right after step 1.

Release `v0.2.0` changes defaults; its migration guide is the `Breaking` subsection of the `[v0.2.0]` section of [CHANGELOG.md](CHANGELOG.md), which lists each changed default with the setting that restores it. Its steps, in order:

- Steps 1 and 2 are done and committed: `make release-prepare VERSION=v0.2.0`, the `[v0.2.0]` section dated with the commit date, `make check` and `make release-check VERSION=v0.2.0`. If you tag on a later day, change the date of the `[v0.2.0]` heading to the tag date, run `make release-check VERSION=v0.2.0` again and commit before tagging.
- User step: `git tag v0.2.0` and `git push origin v0.2.0`. This push triggers the first publication of `ghcr.io/ok0/go-microgateway`, tagged `0.2.0`, `0.2` and `latest`.
- User step: in `redis/`, run `GOWORK=off go mod tidy` and commit the core's `v0.2.0` lines in `redis/go.sum`.
- User step: `git tag redis/v0.2.0` and `git push origin main redis/v0.2.0`.
- User step: `MICROGATEWAY_HEAVY=1 make release-verify VERSION=v0.2.0`.
- User step, once, because `v0.2.0` publishes the first image: make the package public (step 7), then check the image `0.2.0` (step 8).
