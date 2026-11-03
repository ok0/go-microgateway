package config

import (
	"fmt"
	"math"
	"slices"
	"time"
)

const maxCacheDuration = 365 * 24 * time.Hour

const maxCacheBodyBytes = 64 << 20

func defInt64Ptr(p **int64, v int64) {
	if *p == nil {
		*p = &v
	}
}

func (c *Cache) applyDefaults() {
	defStrPtr(&c.Name, "microgateway")
	defInt64Ptr(&c.Memory.MaxBytes, 67108864)
	defIntPtr(&c.Memory.Shards, 16)
	defInt64Ptr(&c.MaxBodyBytes, 1048576)
	defDurPtr(&c.StoreTimeout, 100*time.Millisecond)
	defStrPtr(&c.TagHeader, "Surrogate-Key")
	defStr(&c.TagDelimiter, "space")
	defBool(&c.StripTagHeader, true)
	defBool(&c.Coalescing.Enabled, true)
	defDurPtr(&c.Coalescing.Timeout, 5*time.Second)
	defIntPtr(&c.Coalescing.MaxKeys, 10000)
	defIntPtr(&c.Coalescing.MaxWaiters, 1024)
	defIntPtr(&c.MaxBackground, 64)
}

func (rc *RouteCache) applyDefaults() {
	defDurPtr(&rc.MaxTTL, 24*time.Hour)
	if rc.Statuses == nil {
		rc.Statuses = []int{200, 203, 204, 300, 301, 308, 404, 405, 410, 414, 501}
	}
}

func validCacheName(s string) bool {
	if len(s) < 1 || len(s) > 128 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7E {
			return false
		}
	}
	return true
}

func checkCache(ck *checker, c *Cache, storeNames map[string]bool) {
	if c.Name != nil && !validCacheName(*c.Name) {
		ck.add("cache.name", "must be 1-128 printable ASCII characters")
	}
	if c.Store != "" && !storeNames[c.Store] {
		ck.add("cache.store", fmt.Sprintf("store %q is not defined in stores", c.Store))
	}
	maxBytesOK := c.Memory.MaxBytes != nil && *c.Memory.MaxBytes >= 1
	if c.Memory.MaxBytes != nil && !maxBytesOK {
		ck.add("cache.memory.max_bytes", "must be >= 1")
	}
	shardsOK := c.Memory.Shards != nil && *c.Memory.Shards >= 1 && *c.Memory.Shards <= 1024
	if c.Memory.Shards != nil && !shardsOK {
		ck.add("cache.memory.shards", "must be between 1 and 1024")
	}
	bodyOK := c.MaxBodyBytes != nil && *c.MaxBodyBytes >= 1 && *c.MaxBodyBytes <= maxCacheBodyBytes
	if c.MaxBodyBytes != nil && !bodyOK {
		ck.add("cache.max_body_bytes", fmt.Sprintf("must be between 1 and %d", maxCacheBodyBytes))
	}
	if c.StoreTimeout != nil && *c.StoreTimeout <= 0 {
		ck.add("cache.store_timeout", "must be positive")
	}
	if c.TagHeader != nil && !validHeaderName(*c.TagHeader) {
		ck.add("cache.tag_header", "must be a valid header field name")
	}
	if !slices.Contains([]string{"space", "comma"}, c.TagDelimiter) {
		ck.add("cache.tag_delimiter", "must be one of space, comma")
	}
	if t := c.Coalescing.Timeout; t != nil && *t <= 0 {
		ck.add("cache.coalescing.timeout", "must be positive")
	}
	if v := c.Coalescing.MaxKeys; v != nil && *v < 1 {
		ck.add("cache.coalescing.max_keys", "must be >= 1")
	}
	if v := c.Coalescing.MaxWaiters; v != nil && *v < 1 {
		ck.add("cache.coalescing.max_waiters", "must be >= 1")
	}
	if v := c.MaxBackground; v != nil && *v < 1 {
		ck.add("cache.max_background", "must be >= 1")
	}
	if c.Store == "" && maxBytesOK && shardsOK && bodyOK && *c.Memory.MaxBytes/int64(*c.Memory.Shards) < *c.MaxBodyBytes {
		ck.add("cache.memory.max_bytes", fmt.Sprintf("divided by memory.shards must be at least max_body_bytes (%d)", *c.MaxBodyBytes))
	}
}

func checkRouteCache(ck *checker, p string, rc *RouteCache) {
	durOK := func(name string, d Duration) bool {
		switch {
		case d < 0:
			ck.add(p+"."+name, "must not be negative")
		case d.Std() > maxCacheDuration:
			ck.add(p+"."+name, "must not exceed 365 days")
		default:
			return true
		}
		return false
	}
	defaultOK := durOK("default_ttl", rc.DefaultTTL)
	durOK("keep", rc.Keep)
	durOK("stale_while_revalidate", rc.StaleWhileRevalidate)
	durOK("stale_if_error", rc.StaleIfError)
	maxOK := false
	if m := rc.MaxTTL; m != nil {
		switch {
		case *m <= 0:
			ck.add(p+".max_ttl", "must be positive")
		case m.Std() > maxCacheDuration:
			ck.add(p+".max_ttl", "must not exceed 365 days")
		default:
			maxOK = true
		}
	}
	if defaultOK && maxOK && rc.DefaultTTL > *rc.MaxTTL {
		ck.add(p+".default_ttl", "must not exceed max_ttl")
	}
	if len(rc.Statuses) == 0 {
		ck.add(p+".statuses", "must not be empty")
	}
	seen := map[int]bool{}
	for j, s := range rc.Statuses {
		sp := fmt.Sprintf("%s.statuses[%d]", p, j)
		switch {
		case s < 200 || s > 599 || s == 206 || s == 304:
			ck.add(sp, "must be a final status from 200 to 599 other than 206 and 304")
		case seen[s]:
			ck.add(sp, fmt.Sprintf("duplicate status %d", s))
		}
		seen[s] = true
	}
}

// MaxStorageTTL returns the longest time any entry of an enabled route may be retained: its max_ttl plus the larger
// of stale_while_revalidate and stale_if_error plus keep, where a nil max_ttl counts as 24h and negative components
// count as 0. The sum saturates at the largest time.Duration. It returns 0 when no route enables caching. The
// receiver is not used and may be nil.
func (c *Cache) MaxStorageTTL(routes []Route) time.Duration {
	var best int64
	for i := range routes {
		rc := routes[i].Cache
		if rc == nil || !rc.Enabled {
			continue
		}
		maxTTL := int64(24 * time.Hour)
		if rc.MaxTTL != nil {
			maxTTL = int64(*rc.MaxTTL)
		}
		total := nonNegative(maxTTL)
		total = satAdd(total, max(nonNegative(int64(rc.StaleWhileRevalidate)), nonNegative(int64(rc.StaleIfError))))
		total = satAdd(total, nonNegative(int64(rc.Keep)))
		best = max(best, total)
	}
	return time.Duration(best)
}

func nonNegative(v int64) int64 {
	return max(v, 0)
}

func satAdd(a, b int64) int64 {
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}
