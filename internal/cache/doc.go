// Package cache is the response cache stage of a route pipeline. It answers GET
// and HEAD requests from stored responses that RFC 9111 allows to be reused,
// forwards the remaining requests to the origin, and stores the responses the
// origin returns.
//
// One URL is one key. The primary key is derived from the method, scheme, host,
// path and query of the request alone, so the key does not depend on the route.
// Changes to a route's upstream, rewrite or header rules do not flush entries.
// The scheme is https only for TLS connections to the gateway. X-Forwarded-*
// and Forwarded headers are ignored, so behind a TLS-terminating load balancer
// keys and purge URLs use http.
//
// The built-in memory cache is per instance: instances do not share entries,
// and a purge reaches only the instance that receives it. A named store that
// supports caching is shared by all instances that use it. Concurrent requests
// that need the same entry from the origin share one upstream request: the first
// leads and streams the response to its client, the others wait for the complete
// response, bounded by cache.coalescing.timeout, and are released to forward on
// their own when the response cannot be shared. Coalescing is per instance; N
// instances may each send one request for the same entry. The leader's upstream
// request does not end when its client disconnects; it is bounded by the route
// timeout.
//
// The stored value in Cache-Status is a prediction made when the response
// headers are written. The metrics are authoritative for what was stored.
//
// Cache-Status is added to every response that passes through the stage.
// Responses produced by outer stages, such as auth or rate_limit rejections,
// do not carry it.
//
// Inside a stale-while-revalidate window a stale entry is served at once and
// refreshed in the background: at most one refresh per entry and
// cache.max_background refreshes per instance; a refresh is skipped, never
// queued, when either bound is reached. A refresh is a conditional request
// derived from the triggering request without its Cache-Control, Pragma and
// precondition fields; it forwards that request's credentials.
//
// When a forward for a stored entry fails with 500, 502, 503 or 504 (including
// the gateway's own errors for connection failures, header timeouts and open
// circuit breakers) and the entry is inside its stale-if-error window, the
// failed response is discarded before any of it reaches the client and the
// stored entry is served; Age and ttl are those of the lookup. Soft-purged
// entries are never served stale; they are revalidated.
package cache
