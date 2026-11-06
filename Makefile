GOLANGCI_LINT_VERSION := v2.14.0
GOLANGCI_LINT ?= golangci-lint
FUZZTIME ?= 30s
MICROGATEWAY_SOAK ?= 60s
GO := GOWORK=off go
CLI := github.com/ok0/go-microgateway/internal/cli
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE ?= $(shell TZ=UTC git log -1 --format=%cd --date=format-local:%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo unknown)
LDFLAGS := -s -w -X $(CLI).version=$(VERSION) -X $(CLI).commit=$(COMMIT) -X $(CLI).date=$(DATE)
CORE := github.com/ok0/go-microgateway
COVER_ALGO := $(CORE)/internal/lb $(CORE)/internal/health $(CORE)/internal/breaker $(CORE)/internal/retry $(CORE)/internal/auth $(CORE)/internal/authkey $(CORE)/internal/ratelimit $(CORE)/internal/router $(CORE)/store $(CORE)/internal/memstore $(CORE)/internal/storeguard $(CORE)/internal/cache $(CORE)/internal/httpcache $(CORE)/internal/coalesce $(CORE)/redis
COVER_EXCLUDE := $(CORE)/internal/teststore $(CORE)/internal/testcert $(CORE)/internal/leakcheck
REDIS_COVERFLAGS ?=

.DEFAULT_GOAL := help
.PHONY: help build test race vet cover cover-ci cover-times lint tidy-check realnet bench require-heavy require-redis fuzz soak perf integration docker hygiene check

help: ## list the targets
	@grep -E '^[A-Za-z0-9_.-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "%-12s %s\n", $$1, $$2}'

build: ## build bin/microgateway
	cd redis && CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o ../bin/microgateway ./cmd/microgateway

test: ## unit tests of both modules
	$(GO) test ./...
	cd redis && $(GO) test ./...

race: ## unit tests of both modules with the race detector
	$(GO) test -race ./...
	cd redis && $(GO) test -race ./...

vet: ## go vet of both modules
	$(GO) vet ./...
	cd redis && $(GO) vet ./...

cover: ## race tests with the coverage gate
	mkdir -p bin/cover
	$(GO) test -race -covermode=atomic -coverprofile=bin/cover/core.out $$($(GO) list ./... | grep -v -x -e '$(CORE)' -e '$(CORE)/internal/e2e')
	$(GO) test -race -covermode=atomic -coverpkg=$(CORE) -coverprofile=bin/cover/root.out . ./internal/e2e
	cd redis && $(GO) test -race -covermode=atomic -coverprofile=../bin/cover/redis.out $(REDIS_COVERFLAGS) ./...
	$(GO) run ./internal/covergate -total 75 -algo-min 85 -algo '$(COVER_ALGO)' -exclude '$(COVER_EXCLUDE)' -module $(CORE) -module $(CORE)/redis bin/cover/core.out bin/cover/root.out bin/cover/redis.out

cover-ci: ## cover with verbose redis tests, logged to bin/cover/cover.log
	mkdir -p bin/cover
	bash -o pipefail -c '$(MAKE) cover REDIS_COVERFLAGS=-v 2>&1 | tee bin/cover/cover.log'

cover-times: ## the slowest top-level tests in bin/cover/cover.log
	grep -E '^--- (PASS|FAIL|SKIP): ' bin/cover/cover.log | sed -E 's/^--- [A-Z]+: ([^ ]+) \(([0-9.]+)s\)$$/\2 \1/' | sort -rn | head -40

lint: ## golangci-lint v2.14.0 in both modules
	@command -v $(GOLANGCI_LINT) >/dev/null 2>&1 || { echo "golangci-lint $(GOLANGCI_LINT_VERSION) is required: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)"; exit 1; }
	$(GOLANGCI_LINT) run --config $(CURDIR)/.golangci.yml ./...
	cd redis && $(GOLANGCI_LINT) run --config $(CURDIR)/.golangci.yml ./...

tidy-check: ## go mod tidy -diff in both modules
	$(GO) mod tidy -diff
	cd redis && $(GO) mod tidy -diff

realnet: ## tests against real TCP sockets
	$(GO) test -race -count=1 -tags realnet ./internal/e2e

bench: ## every benchmark once, no thresholds
	$(GO) test -run '^$$' -bench . -benchtime=1x ./...
	cd redis && $(GO) test -run '^$$' -bench . -benchtime=1x ./...

require-heavy:
	@test "$(MICROGATEWAY_HEAVY)" = 1 || { echo "this target is heavy and opt-in: set MICROGATEWAY_HEAVY=1"; exit 1; }

require-redis:
	@test -n "$(MICROGATEWAY_REDIS_ADDR)" || { echo "set MICROGATEWAY_REDIS_ADDR=host:port (Redis 6.2 or later, empty database)"; exit 1; }

fuzz: require-heavy ## every Fuzz target for FUZZTIME each
	FUZZTIME=$(FUZZTIME) sh scripts/fuzz.sh

soak: require-heavy ## soak tests for MICROGATEWAY_SOAK each
	MICROGATEWAY_SOAK=$(MICROGATEWAY_SOAK) $(GO) test -count=1 -v -timeout 30m -run '^TestSoak(FullChain|CacheMixed)$$' ./internal/e2e

perf: require-heavy ## benchmarks of the gateway and the router
	MICROGATEWAY_HEAVY=1 $(GO) test -count=1 -run '^$$' -bench '^(BenchmarkFullChain|BenchmarkCacheHit)$$' -benchtime=20000x ./internal/e2e
	MICROGATEWAY_HEAVY=1 $(GO) test -count=1 -run '^$$' -bench '^BenchmarkLookup1000$$' -benchtime=1s ./internal/router

integration: require-heavy require-redis ## Redis integration tests
	cd redis && MICROGATEWAY_REDIS_ADDR=$(MICROGATEWAY_REDIS_ADDR) $(GO) test -race -count=1 -tags integration ./...

docker: ## build the container image
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg DATE=$(DATE) -t microgateway:$(VERSION) .

hygiene: ## repository, commit, remote, dependency and release checks
	$(GO) test -count=1 -v -run '^(TestTrackedFileHygiene|TestCommitMessageHygiene|TestRemotes|TestTrackedGoLines|TestModuleDependencies|TestIgnoreFiles|TestLicense|TestNoCopiedSource|TestCoreHasNoRedisDependency)$$' ./internal/doccheck ./internal/cli

check: vet cover lint tidy-check hygiene ## vet, race tests with the coverage gate, lint, tidy-check, hygiene; nothing heavy
