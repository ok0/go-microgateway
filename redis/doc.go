// Package redis implements a Redis backend for store.RateLimiter.
//
// Gateway instances that share one Redis deployment enforce one global limit:
// every instance reads and writes the same counters. Option registers the
// store type "redis" with the gateway, NewFromConfig builds a Store from a
// stores[] configuration entry, and New wraps a client the caller owns.
//
// Redis 6.2 or later is required because the scripts depend on script effects
// replication.
//
// # Usage
//
// In YAML, declare a store with type redis and reference it from the rate
// limit of a route through routes[].rate_limit.store. Keys are kept in the
// store named there; an empty value keeps counters in memory per instance.
//
// In Go, pass the option to the gateway constructor:
//
//	gw, err := microgateway.New(cfg, redis.Option())
//
// # Semantics
//
// The backend follows the contract of the store package exactly: the same
// Take calls produce the same Decision values and the same persisted state as
// the memory backend, given the same clock readings. The clock is the Redis
// server clock, read with TIME inside the script that decides and persists the
// request, so the decision and the write are one atomic step. A denied request
// writes nothing.
//
// # Keys and values
//
// A token bucket key is <key_prefix>:rl:{<key>}. Its value is the theoretical
// arrival time as a decimal count of Unix nanoseconds, written as the
// millisecond part followed by six digits of sub-millisecond nanoseconds.
//
// A sliding window key is <key_prefix>:rl:{<key>}:sw. Its value holds three
// decimal integers separated by spaces: the index of the current window, the
// count of the previous window and the count of the current window. The window
// state is a single key, so the whole state of one limit moves together.
//
// The key is the limiter identifier joined with the client key. The braces
// place the whole key in one hash slot, so every script call touches exactly
// one key and works in Redis Cluster.
//
// Every write is followed by PEXPIREAT at the contract expiry, rounded up to
// the next whole millisecond, so expiry never changes a decision. Keys are
// namespaced by key_prefix. Operators rely on the TTL to remove stale keys;
// the backend never scans or purges keys.
//
// # Exactness
//
// Scripts compute with Lua doubles. Clock readings are converted to Unix
// microseconds, which stay below 2^53 until the year 2255. Token bucket state
// more than about 104 days ahead of the server clock is not exact. That state
// arises only if the Redis clock steps back by that much.
//
// # Scripts and cluster
//
// Scripts are sent with EVALSHA and loaded with EVAL when the server replies
// NOSCRIPT. Cluster mode disables go-redis redirect handling: a MOVED or ASK
// reply during resharding fails that one call with store.ErrUnavailable, and
// the client reloads the slot map before the next call.
//
// # Failures
//
// The client does not retry commands. A retried script after an ambiguous
// failure could consume a request twice. Each call is bounded by the dial,
// read and write timeouts configured for the store, and by the context passed
// to Take.
//
// Errors from Take wrap store.ErrUnavailable and carry only a fixed category
// such as "timeout" or "server error WRONGTYPE"; they never contain a server
// reply, an address, a key or a password. Every timeout, including expiry of
// the context deadline, has the category "timeout". A server error carries the
// code of the innermost Redis error, also when Redis wraps an error raised
// inside a script. Secrets are redacted in formatting and logging. When the
// caller cancels the context, Take returns the context error unwrapped.
//
// The go-redis pool stops dialing after pool_size consecutive dial failures
// and returns the last dial error until a background dial, attempted once per
// second, succeeds.
//
// # Testing
//
// Default tests use miniredis only. Running with the integration build tag and
// MICROGATEWAY_REDIS_ADDR=host:port (Redis 6.2 or later, database 15, which
// must be empty) runs the authoritative suite. MICROGATEWAY_REDIS_SENTINEL set
// to <master>@host:port[,host:port] enables the sentinel cases, and
// MICROGATEWAY_REDIS_CLUSTER set to host:port[,...] enables the cluster cases;
// without them those cases are skipped. A local server can be started with
//
//	docker run -d --rm --name mg-redis -p 6379:6379 redis:7.4-alpine
//
// # Test server fidelity
//
// Differences between miniredis and Redis that the tests depend on:
//
//   - gopher-lua formats integer-valued numbers in full through tostring, while
//     Redis Lua formats them with %.14g. The scripts never call tostring and
//     never convert numbers to strings implicitly; every number is formatted
//     with string.format('%.0f'). Covered by TestIntegrationSuite/Fidelity/NumberFormatting.
//   - miniredis computes PTTL (and the remaining TTL used for eviction) against
//     its sub-millisecond clock, while Redis measures when − now on the
//     millisecond-truncated clock: with the server clock 300 µs past a
//     millisecond, PEXPIREAT now_ms+201 reads back as PTTL 200 on miniredis and
//     201 on Redis. The scripts never read TTLs; they only set an absolute
//     millisecond expiry with PEXPIREAT, which both servers store exactly.
//     Default tests therefore assert absolute expiry through PEXPIRETIME, and the
//     miniredis probe asserts the miniredis value explicitly. Covered by
//     TestIntegrationSuite/Fidelity/PttlRounding,
//     TestIntegrationSuite/Fidelity/PexpireatAbsolute and
//     TestIntegrationSuite/InjectedTTL.
//   - An error raised by redis.call inside a script reaches the client in three
//     shapes: Redis 7 keeps the original code (WRONGTYPE Operation against a key
//     holding the wrong kind of value script: <sha>, on @user_script:1.), Redis
//     6.2 wraps it as ERR Error running script (call to f_<sha>):
//     @user_script:1: WRONGTYPE …, and miniredis wraps every runtime error as ERR
//     Error compiling script (new function): <string>:1: WRONGTYPE …. Take
//     reports the innermost error code for all three, so the miniredis tests
//     assert server error WRONGTYPE unchanged. Covered by
//     TestIntegrationSuite/Fidelity/ErrorPropagation and
//     TestIntegrationSuite/ServerTime/WrongType.
package redis
