// Package coalesce shares one in-flight call among concurrent callers of the same key. One caller per key leads and
// runs the call; the others wait, each bounded by its own context and a timeout on an injected clock. Bounds: keys in
// flight and waiters per key; when a bound is reached the caller is told not to coalesce. A panic in the leader is
// recovered and delivered to waiters as an error. State is per process: there is no cross-instance coordination.
package coalesce
