# Contributing

This guide describes how to build, test and check changes to go-microgateway, and the checks that are run before a release.

## Prerequisites

- Go 1.27 or later. CI tests the `go.mod` floor, Go 1.27.0, and the current stable release; every other job uses go1.27.3, which you reproduce with `GOTOOLCHAIN=go1.27.3`. Releases are built with Go 1.27.
- Docker, optional. It is needed only for `make docker`.
- Redis 6.2 or later, optional. It is needed only for the Redis integration tests, which CI runs on Redis 6.2 and 7.4.
- golangci-lint v2.14.0, for `make lint`.

## Building and testing

The repository holds two Go modules: the core module at the root and the Redis module in `redis/`. Run every Go command with `GOWORK=off`. No `go.work` file is committed, and changes must not depend on one.

The Makefile wraps the common commands:

- `make build` builds the binary into `bin/microgateway`.
- `make test` runs the tests of both modules.
- `make race` runs the tests with the race detector.
- `make vet` runs `go vet` in both modules.
- `make cover` runs the tests with coverage and enforces the coverage gate.
- `make lint` runs golangci-lint v2.14.0 in both modules.
- `make tidy-check` checks that `go mod tidy` changes nothing in either module.
- `make hygiene` runs the repository hygiene checks, which include the copied-source check.
- `make check` runs `vet`, `cover`, `lint`, `tidy-check` and `hygiene`.

The same commands by hand, from the repository root and from `redis/`:

```sh
GOWORK=off go build ./...
GOWORK=off go test ./...
GOWORK=off go vet ./...
cd redis && GOWORK=off go test ./...
```

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

- `MICROGATEWAY_HEAVY=1` enables the long benchmark and fuzz runs, the soak tests and the integration tests.
- `MICROGATEWAY_SOAK` sets the soak duration, for example `60s`.
- `MICROGATEWAY_LOAD=1` enables the load tests.
- `MICROGATEWAY_CACHE_FULL=1` enables the full cache test matrix in `redis`.
- `MICROGATEWAY_REDIS_ADDR` sets the address of a Redis 6.2 or later server for the integration tests.
- `MICROGATEWAY_REDIS_CLUSTER` and `MICROGATEWAY_REDIS_SENTINEL` select the cluster and Sentinel topologies of the integration tests.
- The `realnet` build tag enables the tests that use the real network.

`make perf`, `make soak`, `make fuzz`, `make integration` and `make realnet` refuse to run without the variables they need and say which one is missing.

## Release verification

Run these checks before a release. The heavy checks are not part of `make check` and are run by the owner of the release. Record the results in the CHANGELOG entry of the release.

| Check | Command | Environment | Expected result | Heavy |
|-------|---------|-------------|-----------------|-------|
| 1. Benchmarks | `make perf` with `MICROGATEWAY_HEAVY=1` | Local machine, Go 1.27 | `overhead-p50-ns` below 150000 for the full chain, below 50000 for a cache hit, and router below 2000 ns/op | Yes |
| 2. Soaks | `make soak` with `MICROGATEWAY_HEAVY=1 MICROGATEWAY_SOAK=60s` | Local machine | `TestSoakFullChain` and `TestSoakCacheMixed` pass, and the goroutine and heap targets hold | Yes |
| 3. Fuzzing | `make fuzz` with `MICROGATEWAY_HEAVY=1` | Local machine | Every `Fuzz` target runs for 30 s with no failure | Yes |
| 4. Idle-timer stress | `GOWORK=off go test -race -count=20 -run 'TestCloseStopsIdleTimers\|TestCloseInUseConnClosedByReadLoop' ./internal/upstream`, run while three other `GOWORK=off go test -race ./internal/upstream` runs execute concurrently | Local machine | Zero failures | Yes |
| 5. Cache and purge load | `MICROGATEWAY_LOAD=1 GOWORK=off go test -race -count=1 -run 'TestCoalesceScale\|TestCacheMixedLoad\|TestPurgeLoad' ./internal/cache`; the same variables with `-run 'TestPurgeMarkerCapDefault\|TestInstancesMixedLoad\|TestHostGaugesChurnLoad' ./internal/e2e`; `MICROGATEWAY_LOAD=1 GOWORK=off go test -race -count=1 -run TestMixedMultiInstanceLoad .` in `redis`; `MICROGATEWAY_CACHE_FULL=1 GOWORK=off go test -count=1 ./...` in `redis`; `GOWORK=off go test -race -count=20 ./internal/cache ./internal/coalesce ./internal/memstore ./store/... ./internal/storeguard`; and the full-length `go test -run '^$' -bench . ./internal/cache ./internal/memstore` plus the same in `redis` | Local machine | Every command passes | Yes |
| 6. Server and HTTP/2 | `GOWORK=off go test -race -count=20 ./internal/server ./internal/memnet`; three concurrent `GOWORK=off go test -race -count=3 ./internal/e2e`; and `go test -race -count=3` of `./internal/cli` with `./redis/cmd/microgateway` and `./redis`, run from `redis/` at the same time | Local machine | Every command passes | Yes |
| 7. Real Redis | `make integration` with `MICROGATEWAY_HEAVY=1 MICROGATEWAY_REDIS_ADDR=<host:port>`; `GOWORK=off go test -race -count=5 -tags integration ./...` in `redis` | Redis 6.2 or later, empty database such as 15 | `TestIntegrationSuite` and `TestIntegrationTwoInstances` pass; cluster and Sentinel runs pass with `MICROGATEWAY_REDIS_CLUSTER` and `MICROGATEWAY_REDIS_SENTINEL` set | Yes |
| 8. Diagrams | Render the nine Mermaid diagrams in the README and the guides | A GitHub-compatible Mermaid renderer | Every diagram renders without a syntax error | Yes (manual) |
| 9. Real network | `make realnet`, and `GOWORK=off go test -race -count=5 -tags realnet ./internal/e2e`, and `-run TestConnectTimeoutRealClockTCP` | Local machine with the VPN off | Every command passes | Yes |
| 10. Container | `make docker`, then `docker run --rm microgateway:<version> version`, then a run with a mounted configuration | Docker | The version output matches `bin/microgateway version`, and the process runs as a non-root user | Yes |
| 11. Lint and toolchain | `make lint` with golangci-lint v2.14.0 in both modules; build and test with a Go 1.27 toolchain | Go 1.27 toolchain | Every command passes | No |
| 12. Tags | After all checks pass, the owner may create the local tags `v0.1.0` for the core module and `redis/v0.1.0` for the Redis module | Repository owner | Local tags exist for both modules | No |

Each check names whether it is heavy. Checks marked heavy need the environment they name and are not run in continuous integration.
