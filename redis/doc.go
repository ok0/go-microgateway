// Package redis implements Redis backends for store.RateLimiter and store.Cache.
//
// Gateway instances that share one Redis deployment enforce one global limit
// and share one response cache: every instance reads and writes the same
// counters, entries and purge state. Option registers the store type "redis"
// with the gateway for both capabilities, NewFromConfig builds a Store from a
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
// The response cache uses a store of the same type, referenced by cache.store.
// Gateways that reference one store share entries and purges: a purge accepted
// by any of them is seen by every one on its next lookup. Without a store, the
// cache is kept in memory per instance and a purge affects only the instance
// that received it.
//
// In Go, pass the option to the gateway constructor:
//
//	gw, err := microgateway.New(cfg, redis.Option())
//
// # Semantics
//
// The rate limit backend follows the contract of the store package exactly: the
// same Take calls produce the same Decision values and the same persisted state
// as the memory backend, given the same clock readings. The clock is the Redis
// server clock, read with TIME inside the script that decides and persists the
// request, so the decision and the write are one atomic step. A denied request
// writes nothing.
//
// # Rate limit keys and values
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
// # Cache keys and values
//
// A primary key is hashed with SHA-256; the first 32 lower-case hexadecimal
// characters of the digest, h, name its keys. The first two characters of h
// select one of 256 buckets. The keys of a primary and the two epoch keys of its
// bucket share the hash tag {bucket}, so every cache script touches one slot.
// With P the key_prefix:
//
//   - P:c:{bb}:i:h is the index of the primary: the Vary names of its latest Put,
//     its key generation, its purge-until time, one record per variant and the
//     fills that are pending.
//   - P:c:{bb}:m:h is the purge marker of the primary: its key generation and its
//     purge-until time.
//   - P:c:{bb}:e:h:i, for slots i from 0 to 31, holds one entry: its fill
//     identifier, metadata, body, tags, stored time and deadlines.
//   - P:c:{bb}:p:h:fid holds a pending fill until its commit moves it into a slot.
//   - P:c:{bb}:eh and P:c:{bb}:es are the hard and the soft epoch of the bucket.
//     Both hold the same content, so one lost key is recovered from the other.
//   - P:c:t:{th}:s and P:c:t:{th}:sr hold the primaries of a tag, one field per
//     member with its deadline. Both hold the same content. P:c:t:{th}:m and
//     P:c:t:{th}:r hold the purge marker of the tag, again in two identical keys.
//     th is the SHA-256 of the tag, truncated in the same way as h.
//   - P:c:{q}:tq and P:c:{q}:tr hold the counter of tag purges, in two identical
//     keys.
//
// A key always carries an expiry no earlier than its lifetime and at most one
// millisecond later. An entry expires at its deadline. The index expires at the
// latest deadline of its live variants and pending fills, or at its purge-until
// time, and it is recomputed on every write. A purge marker expires at its
// purge-until time. A tag set and its replica expire at the latest deadline of
// their registrations, and a tag marker at the latest retention of the purges of
// its tag. Bucket epochs expire at the latest of the retention of the purges that
// wrote them and the deadlines of the fills committed in their bucket, and the tag
// purge counter at the latest retention of a tag purge. Lifetimes only grow,
// except that of the index.
//
// # Cache semantics
//
// The clock of the cache is the Redis server clock. Entry.StoredAt and
// Lookup.Now are TIME readings taken inside the operation, in microseconds, and
// no clock of a gateway takes part in a decision. An entry is returned by Get
// while now < StoredAt + ttl. The deadline is kept exact: it is the microsecond
// reading plus the sub-microsecond part of ttl, and PEXPIREAT rounds it up to
// the next whole millisecond. Lookup.Now and the deadlines are compared on the
// same clock, so the age of an entry is the same on every instance.
//
// A variant is visible when its index record is alive, when it belongs to the
// current hard epoch of its bucket and when its slot holds the same fill. It is
// stale when a soft purge covers it. The Vary names of a primary and the
// selection made by Get follow its live variants only. Malformed stored data is
// treated as absent.
//
// A fill token is read atomically with the lookup. It records the server time,
// the generation of the primary, the epochs of the bucket and the tag purge
// counter, so the refusal of a fill depends only on the order of operations and
// on Redis time. Put refuses a fill, storing nothing and changing no Vary record,
// when a hard or soft purge that covers the entry began after the token was
// issued. A purge of keys or of all entries is decided by comparing generations
// and epochs, so that refusal is exact in call order. A purge of tags refuses a
// fill whose token was issued within one second (store.PurgeTolerance) after a
// purge of one of its tags, and a token that carries no tag purge counter or that
// predates the counter of a tag marker.
//
// Put runs in four steps: the pending fill is written; the fill is registered
// under each of its tags; the tag markers are checked; the fill is committed into
// a slot of its primary. Each step is atomic on its own keys, and the commit
// applies the refusal rules again, so a purge that begins between two steps
// still refuses the fill.
//
// # Cache purge
//
// PurgeResult.Accepted counts the distinct selectors of a purge, and Affected
// counts the entries changed when AffectedKnown is true, as the store contract
// defines it. A purge of all entries may not know its count.
//
// A purge runs in batches of 100 primaries, one pipeline per batch, and a tag
// purge pages each tag set with HSCAN and COUNT 100 until the cursor returns to 0.
// The members of a tag set are never read with HGETALL or SMEMBERS, so no purge
// reads a whole set at once. A live variant listed under both keys of a tag set
// is counted once.
//
// A purge that fails part-way returns an error wrapping store.ErrUnavailable,
// which the admin endpoint answers with 503. Purges are idempotent, so repeating
// the purge after the failure is safe and reports the entries it still changes.
//
// # Cache exactness and limits
//
// Each bookkeeping value is kept in two keys of one slot, written in one script,
// and readers take the union of the two. Losing one key of a pair, by eviction or
// by expiry, changes no lookup, no purge coverage and no refusal: a lost index or
// entry is a miss, and a lost pending fill or fill registration refuses the fill.
// Losing both keys of one pair is not covered. Such a loss can let a variant that
// a purge covered be served again. Both keys of a pair carry the same lifetime,
// so no single eviction or expiry causes that loss.
//
// On a cluster, the keys of one tag and of one bucket may sit on nodes whose
// clocks differ. The cluster client adds one second to the lifetimes of tag sets,
// the tag purge counter and the bucket epochs, and to the tag pruning check, so
// those keys outlive the purge tolerance on any node. Single-server lifetimes are
// exact.
//
// The cache accepts server clock readings up to 8912591253740990 microseconds
// after the Unix epoch, which is June 2252. The bound leaves room for
// the largest storage TTL and the cluster allowance, so every sum stays an exact
// integer in Lua. Past it, an operation fails with an error wrapping
// store.ErrUnavailable.
//
// When the Redis clock steps back, an entry whose deadline has passed but which
// Redis has not removed yet is visible again until its deadline passes again.
// Purges stay in effect across a backward step, because their markers keep their
// own deadlines.
//
// # Scripts and cluster
//
// Scripts are sent with EVALSHA and loaded with EVAL when the server replies
// NOSCRIPT. Cluster mode disables go-redis redirect handling: a MOVED or ASK
// reply during resharding fails that one call with store.ErrUnavailable, and
// the client reloads the slot map before the next call. Every script of the
// cache declares all of its keys in KEYS and touches keys of one slot only.
//
// # Failures
//
// The client does not retry commands. A retried script after an ambiguous
// failure could consume a request twice. Each call is bounded by the dial,
// read and write timeouts configured for the store and by the context passed
// to Take. The dial timeout covers the TLS handshake; the read and write
// timeouts bound each read and write on the connection. They run on the clock
// of the gateway that built the store, or on the real clock for a Store from
// NewFromConfig. When the context ends first, Take returns at once and the
// abandoned call ends at those timeouts.
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
// must be empty) runs the authoritative suite, including the cache conformance,
// server clock, script flush, purge loss, large tag and two-gateway cases.
// MICROGATEWAY_REDIS_SENTINEL set to <master>@host:port[,host:port] enables the
// sentinel cases, and MICROGATEWAY_REDIS_CLUSTER set to host:port[,...] enables
// the cluster cases; without them those cases are skipped. A local server can be
// started with
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
//   - HSCAN ignores COUNT in miniredis: it returns the whole hash in one reply with
//     cursor 0, while Redis returns pages of about COUNT fields. The tag purge
//     depends only on the guarantees of SCAN, which hold on both servers. Default
//     tests answer HSCAN from a test proxy that pages the hash at the wire, and
//     the integration counterparts run on Redis: TestIntegrationSuite/Fidelity/HscanCoversAll
//     requires at least two pages, and TestIntegrationSuite/CacheLargeTag purges
//     a tag set of more than one page.
//   - miniredis does not reject a script whose keys lie in different slots, which
//     Redis does with CROSSSLOT. Default tests declare every key of a script in
//     KEYS through a guard that rejects an undeclared key and a command that cannot
//     be proven single-slot, and they audit the slots of every script on the wire.
//     The integration counterparts are TestIntegrationSuite/Fidelity/ScriptKeyGuard
//     and the slot cases of the cluster, TestIntegrationSuite/Cluster/CacheSlotCrossing.
//   - miniredis removes keys at expiry only when the test calls FastForward; SetTime
//     alone never expires a key. SetTime backwards keeps the remaining TTL of each
//     key, so its absolute expiry moves back with the clock, while Redis keeps the
//     absolute expiry. Covered by TestIntegrationSuite/CacheServerTime, which
//     checks the expiry instant on the server clock, and TestIntegrationSuite/Fidelity/ExpiryInstant.
//   - RENAME gives the destination the expiry of the source and drops the expiry
//     of the overwritten destination. The probe checks this on the server it runs
//     against. Covered by TestIntegrationSuite/Fidelity/RenameKeepsTTL.
//   - PEXPIRETIME exists from Redis 7.0. The tests that read absolute expiries
//     through it run on miniredis and on Redis 7 and later. On Redis 6.2 the
//     probe that checks it is skipped, and the server clock case omits its
//     PEXPIRETIME check. Covered by TestIntegrationSuite/Fidelity/PexpireatAbsolute
//     and TestIntegrationSuite/CacheServerTime.
//   - TIME on miniredis reads the clock that the test sets. The cache takes every
//     time reading from TIME. Covered by TestIntegrationSuite/Fidelity/Time and
//     TestIntegrationSuite/CacheServerTime, which reads the real server clock.
//   - The tests flush the script cache and check that a later call reloads each
//     script. Covered by TestIntegrationSuite/Fidelity/EvalshaNoscript and
//     TestIntegrationSuite/CacheScriptFlush.
//   - A script passes a 1 MiB argument through unchanged. Covered by
//     TestIntegrationSuite/Fidelity/LargeArgv.
//
// The cache uses neither SSCAN nor HINCRBY.
package redis
