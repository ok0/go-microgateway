# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

### Changed

- Rewrite prefixes (`strip_prefix`, `replace_prefix.from` and `replace_prefix.to`) must be canonical paths.
- The access log has an `error` field for requests rejected because of their path.

### Security

- Request paths are canonicalized before routing. Ambiguous paths get `400 invalid_path`.
- Equivalent path spellings share one route, one cache entry and one rate-limit bucket.
