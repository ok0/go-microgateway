# Configuration reference

A microgateway configuration is one YAML document. The gateway decodes it strictly: unknown keys are rejected, and every problem is reported before the gateway starts. Each top-level key has its own section below. Every key of the document appears in exactly one table row.

Each table has these columns:

- **Key** is the path of the key. `[]` marks a list of structs, so `routes[].cache.max_ttl` is the `max_ttl` key of each entry of `routes`. A map is documented as one path and its entries are not listed.
- **Type** is the YAML type of the value.
- **Default** is the value applied when the key is absent. `none` means that defaulting leaves the zero value, and `required` means that the key must be set. `depends on` introduces a rule that selects the default.
- **Constraints** lists the rules that `microgateway validate` checks.
- **Meaning** describes the key.

A change to one of the following keys is not applied by a reload and needs a restart: `listeners[]` `name`, `address`, `tls`, `http2`, `h2c`, `read_header_timeout`, `read_timeout`, `write_timeout`, `idle_timeout` and `max_header_bytes`; `admin.enabled`; `admin.address`; `global.log.level`; `global.log.format`; `global.reload.debounce`; and `global.reload.poll_interval`.

## version

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `version` | integer | required | must be `1` | Schema version of the document. |

## global

Process-wide settings. The defaults of the `log` and `reload` blocks apply when the blocks are omitted.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `global.request_id_header` | string | `X-Request-ID` | must not be empty | Header that carries the request identifier. |
| `global.trusted_proxies` | list of strings | none | each entry is an IP address or a CIDR prefix | Addresses or prefixes whose forwarding headers are trusted. |
| `global.strip_identity_headers` | list of strings | `[X-Consumer-ID, X-Consumer-Metadata, X-User-ID, X-Auth-Subject, X-Auth-Claims]` | applied only when the key is absent; an explicit empty list keeps no default | Headers removed from inbound requests. |
| `global.drain_timeout` | duration | `30s` | must be positive | Bounds how long shutdown waits for in-flight requests. |
| `global.shutdown_delay` | duration | none | must not be negative | How long the gateway keeps serving after it starts shutting down. |

### global.log

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `global.log.level` | string | `info` | one of `debug`, `info`, `warn`, `error` | Log level. |
| `global.log.format` | string | `json` | one of `json`, `text` | Format of each log line. |
| `global.log.access_log` | boolean | `true` | `true` or `false` | Enables one access log line per request. |

### global.reload

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `global.reload.debounce` | duration | `250ms` | must be positive | Quiet period that coalesces bursts of change events. |
| `global.reload.poll_interval` | duration | `5s` | must be positive | How often the configuration file is checked for changes. |

## listeners

Each entry is one address the gateway accepts connections on. The `tls` block is optional. Its keys are required when the block is present.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `listeners[].name` | string | required | non-empty and unique among the listeners | Identifies the listener. |
| `listeners[].address` | string | required | a valid `host:port` address, for example `:8080` | Address the listener binds to. |
| `listeners[].http2` | boolean | `true` | `true` or `false`; applied when the key is absent | Enables HTTP/2 on the listener. |
| `listeners[].h2c` | boolean | none | `true` or `false` | Enables cleartext HTTP/2 when TLS is not used. |
| `listeners[].read_header_timeout` | duration | `10s` | must be positive | Bounds reading the request headers. |
| `listeners[].read_timeout` | duration | none | must not be negative; `0` means no limit | Bounds reading an entire request. |
| `listeners[].write_timeout` | duration | none | must not be negative; `0` means no limit | Bounds writing a response. |
| `listeners[].idle_timeout` | duration | `120s` | must be positive | Bounds how long an idle keep-alive connection stays open. |
| `listeners[].max_header_bytes` | integer | `1048576` | must be positive | Bounds the size of the request headers. |

### listeners[].tls

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `listeners[].tls.cert_file` | string | required | required when `tls` is set; relative paths resolve against the configuration directory | PEM certificate chain. |
| `listeners[].tls.key_file` | string | required | required when `tls` is set; relative paths resolve against the configuration directory | PEM private key. |
| `listeners[].tls.min_version` | string | `"1.2"` | one of `1.2`, `1.3`; applied when `tls` is set | Minimum TLS version. |

## admin

The administrative endpoint is configured here. Its mutating endpoints, such as cache purge, are present only when `admin.auth.keys` lists at least one key.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `admin.enabled` | boolean | `true` | `true` or `false`; applied when the key is absent | Turns the administrative endpoint on. |
| `admin.address` | string | `127.0.0.1:9901` | a valid `host:port` address when `enabled` is `true` | Address the administrative endpoint binds to. |

### admin.auth.keys[]

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `admin.auth.keys[].id` | string | required | non-empty and unique among the keys | Identifies the key. |
| `admin.auth.keys[].hash` | string (secret) | required | `sha256:` followed by 64 hexadecimal digits; unique among the keys | SHA-256 digest of the bearer token. |

## stores

Named stores hold state that rate limits and the response cache can share across instances. Each store is referenced by its `name`.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `stores[].name` | string | required | matches `^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`; unique among the stores | Name that rate limits and the cache reference. |
| `stores[].type` | string | required | `redis`, or a type the embedding program registers | Store implementation. |
| `stores[].params` | map (secret values) | none | keys must be strings; not valid for type `redis` | Settings of a registered store type. |

### stores[].guard

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `stores[].guard.consecutive_failures` | integer | `3` | must be at least 1 | Failures in a row that open the guard. |
| `stores[].guard.open_timeout` | duration | `2s` | must be positive | How long the guard stays open before the store is tried again. |
| `stores[].guard.max_inflight` | integer | `1024` | must be at least 1 | Maximum number of concurrent store calls. |

### stores[].redis

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `stores[].redis.mode` | string | `standalone` | one of `standalone`, `sentinel`, `cluster` | Topology of the Redis deployment. |
| `stores[].redis.addresses` | list of strings | required | at least one `host:port` without a scheme or userinfo; `standalone` takes exactly one | Server addresses. |
| `stores[].redis.master_name` | string | depends on `mode`: required for `sentinel`, none otherwise | required in `sentinel` mode; only valid in `sentinel` mode | Name of the master that sentinel monitors. |
| `stores[].redis.username` | string | none | none | User name for the Redis servers. |
| `stores[].redis.password` | string (secret) | none | none | Password for the Redis servers. |
| `stores[].redis.sentinel_username` | string | none | only valid in `sentinel` mode | User name for the sentinel servers. |
| `stores[].redis.sentinel_password` | string (secret) | none | only valid in `sentinel` mode | Password for the sentinel servers. |
| `stores[].redis.db` | integer | none | must not be negative; must be `0` in `cluster` mode | Logical database index. |
| `stores[].redis.dial_timeout` | duration | `1s` | must be positive | Bounds establishing a connection. |
| `stores[].redis.read_timeout` | duration | `500ms` | must be positive | Bounds each read. |
| `stores[].redis.write_timeout` | duration | `500ms` | must be positive | Bounds each write. |
| `stores[].redis.pool_size` | integer | none | must not be negative; `0` keeps the client default | Size of the connection pool. |
| `stores[].redis.min_idle_conns` | integer | none | must not be negative and must not exceed `pool_size` | Idle connections kept open. |
| `stores[].redis.key_prefix` | string | `microgateway` | must not be empty and must not contain `{` or `}` | Prefix of every key the store writes. |

### stores[].redis.tls

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `stores[].redis.tls.enabled` | boolean | none | `true` or `false` | Enables TLS towards the Redis servers. |
| `stores[].redis.tls.ca_file` | string | none | only valid when `enabled` is `true` | PEM file of the certificate authorities that verify the servers. |
| `stores[].redis.tls.cert_file` | string | none | only valid when `enabled` is `true`; set together with `key_file` | PEM client certificate. |
| `stores[].redis.tls.key_file` | string | none | only valid when `enabled` is `true`; set together with `cert_file` | PEM client private key. |
| `stores[].redis.tls.server_name` | string | none | only valid when `enabled` is `true` | Server name used to verify the certificate. |
| `stores[].redis.tls.insecure_skip_verify` | boolean | none | only valid when `enabled` is `true` | Disables verification of the server certificate. |

## cache

The response cache is shared by every route that enables caching. The defaults apply when the block is omitted.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `cache.name` | string | `microgateway` | 1 to 128 printable ASCII characters | Identifier of the cache in the `Cache-Status` response header. |
| `cache.store` | string | none | empty uses the built-in memory store of each instance; otherwise names an entry of `stores` that supports caching | Store that holds the cache entries. |
| `cache.max_body_bytes` | integer | `1048576` | 1 to 67108864 | Largest response body that is stored. |
| `cache.store_timeout` | duration | `100ms` | must be positive | Bounds each call to the cache store. |
| `cache.tag_header` | string | `Surrogate-Key` | a valid header field name | Header of upstream responses that carries cache tags. |
| `cache.tag_delimiter` | string | `space` | one of `space`, `comma` | Separator between the tags in the tag header. |
| `cache.strip_tag_header` | boolean | `true` | `true` or `false` | Removes the tag header from responses sent to clients. |
| `cache.status_key` | boolean | none | `true` or `false` | Adds a digest of the cache key to the `Cache-Status` member. |
| `cache.max_background` | integer | `64` | must be at least 1 | Maximum number of concurrent background fetches. |

### cache.memory

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `cache.memory.max_bytes` | integer | `67108864` | must be at least 1; divided by `shards` must be at least `max_body_bytes` | Capacity of the built-in memory store. |
| `cache.memory.shards` | integer | `16` | 1 to 1024 | Number of shards the memory store is split into. |

### cache.coalescing

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `cache.coalescing.enabled` | boolean | `true` | `true` or `false` | Lets concurrent misses for one key share a single upstream fetch. |
| `cache.coalescing.timeout` | duration | `5s` | must be positive | Bounds how long a waiting request waits for the shared fetch. |
| `cache.coalescing.max_keys` | integer | `10000` | must be at least 1 | Maximum number of keys with a shared fetch in progress. |
| `cache.coalescing.max_waiters` | integer | `1024` | must be at least 1 | Maximum number of requests waiting for one shared fetch. |

## upstreams

Each upstream has either a static list of `hosts` or one `dns` block. The two are mutually exclusive, and one of them is required.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `upstreams[].name` | string | required | non-empty and unique among the upstreams | Name that routes refer to. |
| `upstreams[].http2` | boolean | none | `true` or `false` | Enables HTTP/2 towards the backends. |

### upstreams[].hosts[]

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `upstreams[].hosts[].url` | string (secret URL) | required | an `http` or `https` URL with a host; userinfo and query values are redacted in output | Base URL of a backend. |
| `upstreams[].hosts[].weight` | integer | `1` | must not be negative; `0` means the host is never selected | Relative share of traffic. |

### upstreams[].dns

A DNS upstream resolves `hostname` to A and AAAA records, and uses at most 256 addresses. Each address gets weight 1.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `upstreams[].dns.hostname` | string | required | non-empty | Name that is resolved to find the backends. |
| `upstreams[].dns.port` | integer | required | 1 to 65535 | Port of the backends. |
| `upstreams[].dns.scheme` | string | `http` | one of `http`, `https` | Scheme used towards the backends. |
| `upstreams[].dns.refresh_interval` | duration | `30s` | must be at least 1s | How often the name is resolved again. |

### upstreams[].load_balancer

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `upstreams[].load_balancer.algorithm` | string | `p2c` | one of `p2c`, `round_robin`, `weighted_round_robin`, `random`, `consistent_hash` | Algorithm that spreads requests across the backends. |
| `upstreams[].load_balancer.bounded_load_factor` | float | `1.25` | must be at least 1 | Caps the load of one backend relative to the average under consistent hashing. |
| `upstreams[].load_balancer.virtual_nodes` | integer | `160` | must be at least 1 | Ring points per backend under consistent hashing. |

### upstreams[].load_balancer.hash

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `upstreams[].load_balancer.hash.source` | string | depends on `algorithm`: required for `consistent_hash` | one of `header`, `cookie`, `ip`, `path` | Request attribute that is hashed; `path` hashes the canonical request path. |
| `upstreams[].load_balancer.hash.name` | string | none | required for the `header` and `cookie` sources | Name of the header or cookie that is hashed. |

### upstreams[].timeouts

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `upstreams[].timeouts.connect` | duration | `5s` | none | Bounds establishing a connection to a backend. |
| `upstreams[].timeouts.tls_handshake` | duration | `10s` | none | Bounds the TLS handshake with a backend. |
| `upstreams[].timeouts.response_header` | duration | `30s` | none | Bounds the wait for response headers. |
| `upstreams[].timeouts.idle` | duration | `90s` | none | Bounds how long an idle pooled connection is kept. |

### upstreams[].pool

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `upstreams[].pool.max_idle_per_host` | integer | `64` | must not be negative | Idle connections kept per backend. |
| `upstreams[].pool.max_conns_per_host` | integer | none | must not be negative; `0` means no limit | Open connections allowed per backend. |

### upstreams[].passive_health

The block is optional. Its defaults apply when the block is present.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `upstreams[].passive_health.consecutive_failures` | integer | `5` | must be at least 1 | Failures in a row that eject a backend. |
| `upstreams[].passive_health.base_ejection_time` | duration | `30s` | none | Duration of the first ejection. |
| `upstreams[].passive_health.max_ejection_time` | duration | `300s` | must be at least `base_ejection_time` | Upper bound of the ejection duration as it grows. |
| `upstreams[].passive_health.max_ejection_percent` | integer | `50` | 0 to 100; `0` disables ejection; applied when the key is absent | Share of backends that may be ejected at the same time. |

### upstreams[].active_health

The block is optional. Its defaults apply when the block is present.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `upstreams[].active_health.path` | string | `/health` | must start with `/` | Request path that is probed. |
| `upstreams[].active_health.interval` | duration | `10s` | none | Time between probes. |
| `upstreams[].active_health.jitter` | float | `0.1` | in [0, 1); `0` disables jitter; applied when the key is absent | Randomizes the probe timing by up to this fraction. |
| `upstreams[].active_health.timeout` | duration | `2s` | must be less than `interval` | Bounds one probe. |
| `upstreams[].active_health.healthy_threshold` | integer | `2` | must be at least 1; applied when the key is absent | Successful probes needed to mark a backend healthy. |
| `upstreams[].active_health.unhealthy_threshold` | integer | `3` | must be at least 1; applied when the key is absent | Failed probes needed to mark a backend unhealthy. |
| `upstreams[].active_health.expected_statuses` | list of strings | `["200-399"]` | each entry is `NNN` or `NNN-NNN`; applied only when the key is absent | Status codes that count as a healthy probe. |

### upstreams[].circuit_breaker

The block is optional. Its defaults apply when the block is present.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `upstreams[].circuit_breaker.window` | duration | `10s` | the window divided by `buckets` must be at least 1ms | Sliding window over which failures are counted. |
| `upstreams[].circuit_breaker.buckets` | integer | `10` | 1 to 1000 | Number of slices the window is divided into. |
| `upstreams[].circuit_breaker.min_requests` | integer | `20` | must be at least 1 | Requests needed in the window before the failure ratio is evaluated. |
| `upstreams[].circuit_breaker.failure_ratio` | float | `0.5` | in (0, 1] | Opens the breaker when failures reach this fraction of requests. |
| `upstreams[].circuit_breaker.consecutive_failures` | integer | `5` | must not be negative; `0` disables the rule | Opens the breaker after this many failures in a row. |
| `upstreams[].circuit_breaker.open_timeout` | duration | `30s` | none | How long the breaker stays open before it probes again. |
| `upstreams[].circuit_breaker.half_open_probes` | integer | `3` | must be at least 1 and at least `success_threshold` | Trial requests allowed while the breaker is half open. |
| `upstreams[].circuit_breaker.success_threshold` | integer | `2` | must be at least 1 | Successful trial requests needed to close the breaker. |

## auth

The `auth` block declares the providers that routes reference by `name`. Providers of both kinds share one namespace of names.

### auth.jwt[]

A JWT provider needs `keys`, `jwks`, or both. Static HS keys cannot be combined with asymmetric keys or with `jwks`.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `auth.jwt[].name` | string | required | non-empty and unique across all providers | Provider name that routes reference. |
| `auth.jwt[].issuer` | string | required | non-empty | Issuer the token must carry. |
| `auth.jwt[].audiences` | list of strings | required | at least one entry, and no empty entry | Audiences one of which the token must carry. |
| `auth.jwt[].leeway` | duration | `30s` | must not be negative; at most `5m`; `0` means no leeway; applied when the key is absent | Clock skew tolerated when the token times are checked. |
| `auth.jwt[].header` | string | `Authorization` | a valid header field name | Header that carries the token. |
| `auth.jwt[].scope_claim` | string | `scope` | must not be empty | Claim that holds the scopes. |
| `auth.jwt[].roles_claim` | string | `roles` | must not be empty | Claim that holds the roles. |
| `auth.jwt[].forward_claims` | map of strings | none | values are valid header field names | Maps claim names to the upstream headers that receive their values. |
| `auth.jwt[].max_token_bytes` | integer | `4096` | must be positive; applied when the key is absent | Largest token that is accepted. |
| `auth.jwt[].forward_credentials` | boolean | `false` | `true` or `false`; applied when the key is absent | Keeps the token on the upstream request. |

### auth.jwt[].keys[]

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `auth.jwt[].keys[].kid` | string | depends on the provider's key count: none for a provider with one key | required when the provider has more than one key; unique within the provider | Key identifier matched against the token header. |
| `auth.jwt[].keys[].algorithm` | string | required | one of `HS256`, `HS384`, `HS512`, `RS256`, `RS384`, `RS512`, `ES256`, `ES384`, `ES512`, `EdDSA` | Signing algorithm that the key verifies. |
| `auth.jwt[].keys[].secret` | string (secret) | depends on the key material given: none for the two not given | exactly one of `secret`, `public_key`, `public_key_file` per key; only valid with HS algorithms | Shared secret of an HS algorithm. |
| `auth.jwt[].keys[].public_key` | string | depends on the key material given: none for the two not given | exactly one of `secret`, `public_key`, `public_key_file` per key; not valid with HS algorithms | PEM-encoded public key given inline. |
| `auth.jwt[].keys[].public_key_file` | string | depends on the key material given: none for the two not given | exactly one of `secret`, `public_key`, `public_key_file` per key; not valid with HS algorithms | Path of a PEM-encoded public key file; see Key files. |

### auth.jwt[].jwks

The block is optional. Its defaults apply when the block is present.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `auth.jwt[].jwks.url` | string (secret URL) | required | an `http` or `https` URL; userinfo and query values are redacted in output | Location of the remote key set. |
| `auth.jwt[].jwks.algorithms` | list of strings | required | at least one entry; each one of `RS256`, `RS384`, `RS512`, `ES256`, `ES384`, `ES512`, `EdDSA`; no duplicates | Asymmetric algorithms accepted from the key set. |
| `auth.jwt[].jwks.ttl` | duration | `10m` | must be positive | How long fetched keys are used before they are refreshed. |
| `auth.jwt[].jwks.min_refresh_interval` | duration | `30s` | must be positive | Minimum time after a fetch attempt before a failed fetch is retried or an unknown `kid` starts a refresh. |
| `auth.jwt[].jwks.timeout` | duration | `5s` | must be positive | Bounds one fetch of the key set. |

Which requests wait for a fetch and which are served from the cached set is described in [JWKS key sets](middleware.md#jwks-key-sets).

### auth.api_keys[]

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `auth.api_keys[].name` | string | required | non-empty and unique across all providers | Provider name that routes reference. |
| `auth.api_keys[].header` | string | `X-API-Key` | a valid header field name; applied when the key is absent; an explicit empty string disables the header and requires `query` | Header that carries the key. |
| `auth.api_keys[].query` | string | none | required when `header` is an empty string | Query parameter that carries the key. |
| `auth.api_keys[].key_file` | string | none | `keys` or `key_file` is required; see Key files | Path of a file that lists keys in the format of `keys[]`. |
| `auth.api_keys[].consumer_header` | string | `X-Consumer-ID` | a valid header field name | Header that receives the consumer name. |
| `auth.api_keys[].metadata_headers` | map of strings | none | values are valid header field names | Maps metadata field names to the headers that receive their values. |
| `auth.api_keys[].forward_credentials` | boolean | `false` | `true` or `false`; applied when the key is absent | Keeps the key on the upstream request. |

### auth.api_keys[].keys[]

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `auth.api_keys[].keys[].id` | string | required | non-empty; unique within the provider and across its `key_file` | Identifies the key. |
| `auth.api_keys[].keys[].hash` | string (secret) | required | `sha256:` followed by 64 hexadecimal digits; unique within the provider and across its `key_file` | SHA-256 digest of the key. |
| `auth.api_keys[].keys[].consumer` | string | required | non-empty | Client that the key belongs to. |
| `auth.api_keys[].keys[].metadata` | map of strings | none | none | Extra values forwarded with the consumer. |
| `auth.api_keys[].keys[].scopes` | list of strings | none | no empty entry | Scopes granted to the consumer of the key. |
| `auth.api_keys[].keys[].roles` | list of strings | none | no empty entry | Roles granted to the consumer of the key. |

## middlewares

Named middleware instances that routes reference in `middlewares` or `pipeline`. The parameters of the built-in types `cors`, `headers` and `request_size_limit` are documented in [docs/middleware.md](middleware.md).

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `middlewares[].name` | string | required | non-empty and unique; not the name of a built-in stage (`timeout`, `auth`, `rate_limit`, `cache`, `circuit_breaker`, `retry`) | Name that routes reference. |
| `middlewares[].type` | string | required | `cors`, `headers`, `request_size_limit`, or a type the embedding program registers | Middleware implementation. |
| `middlewares[].params` | map (secret values) | none | keys must be strings; built-in types are checked against their own parameters | Implementation settings. |

## routes

Each route matches requests and forwards them to one upstream. `priority` orders the routes that could match the same request.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `routes[].name` | string | required | non-empty and unique among the routes | Identifies the route. |
| `routes[].priority` | integer | none | none | Orders the routes that could match the same request. |
| `routes[].upstream` | string | required | names an entry of `upstreams` | Upstream that matching requests are forwarded to. |
| `routes[].preserve_host` | boolean | none | `true` or `false` | Forwards the original `Host` header instead of the upstream host. |
| `routes[].timeout` | duration | none | must not be negative; `0` means no limit | Bounds the whole exchange with the upstream. |
| `routes[].middlewares` | list of strings | none | each entry names a defined middleware; no duplicates | Middlewares applied to the route before the pipeline. |
| `routes[].pipeline` | list of strings | none | each entry is a built-in stage or a middleware name; `auth` comes before `cache`; `cache` comes before `circuit_breaker` and `retry`; `retry` comes last; `auth` is required when `auth.mode` is not `none`; `rate_limit` is required when the route has a `rate_limit` block | Order of the stages and middlewares of the route. When absent, the route runs its middlewares and then `timeout`, `auth`, `rate_limit`, `cache`, `circuit_breaker`, `retry`. |

### routes[].match

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `routes[].match.hosts` | list of strings | none | host names or `*.` wildcards | Hosts the route matches; an empty list matches any host. |
| `routes[].match.path` | string | `/` | a valid path that starts with `/` | Path to match. |
| `routes[].match.path_type` | string | `prefix` | one of `exact`, `prefix`, `pattern` | How the path is compared. \`exact\` compares the canonical request path verbatim. \`prefix\` matches on segment boundaries and ignores a trailing slash in the prefix, so \`/public/\` and \`/public\` both match \`/public\`, \`/public/\` and \`/public/x\`, and neither matches \`/publicx\`. |
| `routes[].match.methods` | list of strings | none | uppercase HTTP method tokens | Methods the route matches; an empty list matches any method. |
| `routes[].match.headers` | map of strings (secret values) | none | header names; values are secrets | Header values that the request must carry. |

### routes[].rewrite

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `routes[].rewrite.strip_prefix` | string | none | must start with `/`; must be a canonical path (see [Request paths](#request-paths)); mutually exclusive with `replace_prefix` | Prefix removed from the path before forwarding; it is compared with the canonical request path. |
| `routes[].rewrite.replace_prefix.from` | string | none | must start with `/`; must be a canonical path (see [Request paths](#request-paths)); set together with `to`, or both empty | Path prefix that is replaced; it is compared with the canonical request path. |
| `routes[].rewrite.replace_prefix.to` | string | none | must be a canonical path (see [Request paths](#request-paths)); set together with `from`, or both empty | Prefix that replaces `from`. |

### routes[].request_headers

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `routes[].request_headers.add` | map of strings (secret values) | none | header names; values are secrets | Appends values to the named request headers. |
| `routes[].request_headers.set` | map of strings (secret values) | none | header names; values are secrets | Replaces the named request headers. |
| `routes[].request_headers.remove` | list of strings | none | header names | Deletes the named request headers. |

### routes[].response_headers

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `routes[].response_headers.add` | map of strings (secret values) | none | header names; values are secrets | Appends values to the named response headers. |
| `routes[].response_headers.set` | map of strings (secret values) | none | header names; values are secrets | Replaces the named response headers. |
| `routes[].response_headers.remove` | list of strings | none | header names | Deletes the named response headers. |

### routes[].auth

The block is optional. Its defaults apply when the block is present.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `routes[].auth.mode` | string | `none` | one of `none`, `jwt`, `apikey`, `any` | Kinds of provider that authenticate the route. |
| `routes[].auth.providers` | list of strings | none | empty when `mode` is `none`; at least one name otherwise; each name is a defined provider of a matching kind | Providers that the route accepts. |
| `routes[].auth.allow_anonymous` | boolean | none | not set when `mode` is `none`; cannot be combined with `require` | Lets requests without valid credentials through. |
| `routes[].auth.require.scopes` | list of strings | none | empty when `mode` is `none`; no empty entry | Scopes that the identity must hold. |
| `routes[].auth.require.roles` | list of strings | none | empty when `mode` is `none`; no empty entry | Roles that the identity must hold. |
| `routes[].auth.require.claims` | map of values | none | empty when `mode` is `none`; claim names must not be empty; values must be strings, numbers or booleans | Claim values that the identity must match. For API keys the claims are matched against metadata. |

### routes[].rate_limit

The block is optional. A value that is set explicitly to zero or to a negative number is an error and is never replaced by a default. Defaults that depend on another key are written as `depends on`.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `routes[].rate_limit.algorithm` | string | `token_bucket` | one of `token_bucket`, `sliding_window` | Algorithm that counts the requests. |
| `routes[].rate_limit.rate` | integer | depends on `algorithm`: required for `token_bucket` | at least 1; only valid with `token_bucket` | Tokens added per `per` interval. |
| `routes[].rate_limit.per` | duration | depends on `algorithm`: `1s` for `token_bucket` | must be positive; only valid with `token_bucket` | Interval that `rate` is counted over. |
| `routes[].rate_limit.burst` | integer | depends on `algorithm`: equal to `rate` for `token_bucket` when `rate` is at least 1 | at least 1; only valid with `token_bucket` | Size of the token bucket. |
| `routes[].rate_limit.limit` | integer | depends on `algorithm`: required for `sliding_window` | at least 1; only valid with `sliding_window` | Requests allowed in each `window`. |
| `routes[].rate_limit.window` | duration | depends on `algorithm`: `1m` for `sliding_window` | must be positive and at most 24h; only valid with `sliding_window` | Window that `limit` is counted over. |
| `routes[].rate_limit.key.source` | string | `ip` | one of `ip`, `consumer`, `header`, `route`; `consumer` requires route authentication | Attribute that the limit is applied per. |
| `routes[].rate_limit.key.name` | string | depends on `source`: required for `header` | required when `source` is `header`; only valid with `source` `header` | Name of the header that the limit is applied per. |
| `routes[].rate_limit.max_keys` | integer | `100000` | must be at least 1 | Number of keys kept in memory per instance. |
| `routes[].rate_limit.store` | string | none | names an entry of `stores` | Store that holds the counters; empty keeps the counters in memory per instance. |
| `routes[].rate_limit.on_store_error` | string | depends on `store`: `local` when a store is set | one of `local`, `allow`, `deny`; only valid with `store` | Behavior when the store fails: `local` uses the in-memory limiter, `allow` admits the request and logs an error, `deny` answers with status 503. |
| `routes[].rate_limit.store_timeout` | duration | depends on `store`: `50ms` when a store is set | must be positive; only valid with `store` | Bounds each call to the store. |

### routes[].cache

The block is optional. The route uses the cache only when `enabled` is `true`.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `routes[].cache.enabled` | boolean | none | `true` or `false` | Enables response caching for the route. |
| `routes[].cache.default_ttl` | duration | none | not negative; not more than `max_ttl`; at most 365 days | Freshness lifetime of a stored response when the upstream response sets none. |
| `routes[].cache.max_ttl` | duration | `24h` | must be positive; at most 365 days; applied when the block is present | Upper bound of the freshness lifetime of a stored response. |
| `routes[].cache.keep` | duration | none | not negative; at most 365 days | How long a stored response is kept past its freshness lifetime so that it can be revalidated. |
| `routes[].cache.stale_while_revalidate` | duration | none | not negative; at most 365 days | How long a stale response may be served while it is refreshed in the background. |
| `routes[].cache.stale_if_error` | duration | none | not negative; at most 365 days | How long a stale response may be served when the upstream fails. |
| `routes[].cache.statuses` | list of integers | `[200, 203, 204, 300, 301, 308, 404, 405, 410, 414, 501]` | not empty; each a final status from 200 to 599 other than 206 and 304; no duplicates | Response statuses that may be stored. |

### routes[].circuit_breaker

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `routes[].circuit_breaker.enabled` | boolean | `true` | `true` or `false`; applied when the block is present | Enables the upstream circuit breaker for the route. |

### routes[].retry

The block is optional. Its defaults apply when the block is present.

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `routes[].retry.attempts` | integer | `3` | 1 to 10; applied when the key is absent | Total number of tries, including the first. |
| `routes[].retry.base_delay` | duration | `25ms` | none | First backoff delay. |
| `routes[].retry.max_delay` | duration | `250ms` | must be at least `base_delay` | Upper bound of the backoff delay. |
| `routes[].retry.on_statuses` | list of integers | `[502, 503, 504]` | each between 400 and 599; applied only when the key is absent | Response statuses that trigger a retry. |
| `routes[].retry.methods` | list of strings | `[GET, HEAD, OPTIONS, PUT, DELETE]` | uppercase HTTP method tokens; applied only when the key is absent | Methods that may be retried. |
| `routes[].retry.max_body_bytes` | integer | `65536` | must not be negative; applied when the key is absent; `0` disables body buffering | Largest request body that is buffered for retries. With `0`, a request with a body is retried only on a connect failure before any byte is sent. |

### routes[].retry.idempotency_key

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `routes[].retry.idempotency_key.enabled` | boolean | none | `true` or `false` | Lets methods that are not listed in `methods` be retried when the request carries a non-empty idempotency key header. |
| `routes[].retry.idempotency_key.header` | string | `Idempotency-Key` | required when `enabled` is `true` | Header that carries the idempotency key. |

### routes[].retry.budget

| Key | Type | Default | Constraints | Meaning |
|---|---|---|---|---|
| `routes[].retry.budget.ratio` | float | `0.1` | in [0, 1]; applied when the key is absent; `0` is kept | Share of requests that may be retried. |
| `routes[].retry.budget.min_per_second` | float | `10` | a finite number not less than 0; applied when the key is absent; `0` is kept | Retry rate that is always allowed, regardless of traffic. |
| `routes[].retry.budget.window` | duration | `10s` | none | Period over which the budget is measured. |

## Environment substitution

String values may reference environment variables. Substitution runs after the document is decoded, so it cannot change the structure of the document.

- `${NAME}` is replaced by the value of `NAME`. `NAME` must be set. An empty value is allowed and produces an empty string.
- `${NAME:-DEFAULT}` is replaced by the value of `NAME` when it is set and not empty. Otherwise it is replaced by `DEFAULT`, which may itself contain references.
- `$$` produces a literal `$`. Any other `$` is literal.
- `NAME` must match `^[A-Za-z_][A-Za-z0-9_]*$`. A reference without a closing `}` is an error.
- A substituted value is inserted verbatim and is never expanded again.

Substitution applies to every string value: string fields, string list entries, string map values, and the secret fields, such as `hash`, `password`, `secret` and the URLs. Map keys are never substituted. Numbers, booleans and durations are not substituted, so a duration cannot be written as `${VAR}`.

A reference to a variable that is not set and has no default is reported at the path of the value, for example:

```text
upstreams[0].hosts[0].url (line 8): environment variable "CATALOG_URL" is not set
```

## Key files

The following fields name files that are read when the configuration is loaded or validated:

- `listeners[].tls.cert_file` and `listeners[].tls.key_file` name the PEM certificate chain and the PEM private key.
- `auth.jwt[].keys[].public_key_file` names a PEM-encoded public key.
- `auth.api_keys[].key_file` names a YAML file with a `keys` list. Each entry has the fields of `auth.api_keys[].keys[]`.

A relative path resolves against the directory that contains the configuration file. Each file must be a regular file of at most 1 MiB. The keys of a key file are checked together with the inline keys of the same provider, so a duplicate `id` or `hash` across the two is reported.

## Durations

A duration is a YAML string in the syntax of Go's `time.ParseDuration`: a number followed by a unit, such as `250ms`, `5s`, `5m` or `24h`. Units can be combined, such as `1h30m`. The accepted units are `ns`, `us`, `µs`, `ms`, `s`, `m` and `h`. The value `0` is accepted without a unit. A number without a unit, such as `5`, and a unit such as `d` are rejected, and so is a value with a space, such as `1 minute`.

## Strict decoding

Unknown keys are rejected. A key that is not in the tables above is reported at its line, for example:

```text
retries (line 2): unknown field "retries"
```

## Request paths

Before routing, the gateway derives one canonical path from the request path. Routing, route stages, rewriting, the forwarded path, the cache key and the load balancer's `path` hash all use this canonical path. The access log keeps the path the client sent.

A request whose path breaks the path rules is rejected with `400` and the code `invalid_path`, before any route policy runs. The path rules reject:

- dot segments `.` and `..` in any encoding, including a dot segment before a `;`, such as `..;x`;
- encoded separators `%2F` and `%5C` in any letter case, and a literal backslash;
- control bytes, literal or percent-encoded;
- invalid percent escapes;
- a path that does not start with `/`, except the `OPTIONS *` request form, which matches no route.

The gateway normalizes every other path:

- escapes of unreserved bytes are decoded, so `/a%7Ex` is `/a~x`;
- other escapes are written with upper-case hex digits;
- bytes outside the path character set are escaped;
- repeated slashes are merged, so `/admin//x` is `/admin/x`.

Paths that differ only in the spelling of unreserved bytes or in escape case, or that differ only in repeated slashes, reach the same route and share one cache entry and rate-limit bucket. The path `/a%3Bb` is a different path from `/a;b`. Upstreams receive the merged slashes and the normalized escapes. There is no setting that relaxes these rules. Clients that send `%2F`, `%5C` or dot segments receive `400`.

Rewrite prefixes (`strip_prefix`, `replace_prefix.from` and `replace_prefix.to`) must already be canonical paths, so that they can be compared with the canonical request path.

## Validation errors

`microgateway validate -config <file>` checks a file and reports every problem in one run. Each problem is one line of the form `path (line N): message`. List entries appear as `[index]` in the path. The last line gives the file name and the number of errors. The command exits with status 1 when the file has errors, and with status 0 when the file is valid. A valid file is reported with its counts of listeners, upstreams and routes.

Rejected configuration:

```yaml
version: 1

global:
  drain_timeout: -5s

listeners:
  - name: public
    address: ":8080"
    read_timeout: 1 minute

upstreams:
  - name: catalog
    hosts:
      - url: "http://127.0.0.1:9001"
    load_balancer:
      algorithm: round-robin
```

`microgateway validate -config rejected.yaml` reports:

```text
global.drain_timeout (line 4): must be positive
listeners[0].read_timeout (line 9): invalid duration "1 minute" (use a value such as "250ms" or "5s")
upstreams[0].load_balancer.algorithm (line 16): must be one of p2c, round_robin, weighted_round_robin, random, consistent_hash
rejected.yaml: 3 error(s)
```

Valid configuration:

```yaml
version: 1

listeners:
  - name: public
    address: ":8080"

upstreams:
  - name: catalog
    hosts:
      - url: "http://127.0.0.1:9001"

routes:
  - name: catalog
    match:
      path: /catalog/
    upstream: catalog
```
