# Security policy

## Supported versions

Security fixes are made on `main`. The latest release line receives fixes once a release exists; no version has been released yet.

## Reporting a vulnerability

Report suspected vulnerabilities privately through GitHub private vulnerability reporting on the repository's Security tab. Do not open a public issue or pull request for a suspected vulnerability.

## What to include

- The affected version or commit, and the module (the gateway in the root module or the Redis module in `redis/`).
- A configuration that reproduces the problem, with secrets removed.
- The request and response, or the log lines, that show the behavior.
- The impact you observed and the conditions it needs.

## What to expect

We acknowledge a report, assess its impact, prepare a fix, and coordinate disclosure with the reporter before the fix is published. We do not commit to fixed response times.

## Scope

In scope: the gateway code in both modules (the root module and `redis/`).

Out of scope: the operator's Redis servers, upstream services, and configuration secrets, which the operator manages.

## Secure defaults

- JWT verification pins the algorithm per key, and `alg=none` is never accepted.
- Identity and forwarding headers are stripped on every route.
- Credentials are removed before proxying unless `forward_credentials` is set.
- YAML is decoded strictly: unknown keys are rejected.
- The admin listener binds to loopback (`127.0.0.1:9901`) by default.
- API keys and admin tokens are stored only as SHA-256 hashes.
