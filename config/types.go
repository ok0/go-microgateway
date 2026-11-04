// Package config defines the microgateway configuration schema, parses it strictly from YAML, and validates it.
package config

// Config is the root of a microgateway configuration document.
type Config struct {
	// Version is the schema version; it must be 1.
	Version int `yaml:"version" json:"version"`
	// Global holds process-wide settings.
	Global Global `yaml:"global" json:"global"`
	// Listeners are the addresses the gateway accepts connections on.
	Listeners []Listener `yaml:"listeners" json:"listeners"`
	// Admin configures the administrative endpoint.
	Admin Admin `yaml:"admin" json:"admin"`
	// Upstreams are the backend pools that routes forward to.
	Upstreams []Upstream `yaml:"upstreams" json:"upstreams"`
	// Stores are the named backing stores that rate limits and the response cache reference by name.
	Stores []Store `yaml:"stores" json:"stores"`
	// Cache holds the response cache settings shared by every route that enables caching; defaults apply when the block is omitted.
	Cache Cache `yaml:"cache" json:"cache"`
	// Auth declares the authentication providers.
	Auth Auth `yaml:"auth" json:"auth"`
	// Middlewares are named middleware instances referenced by routes.
	Middlewares []Middleware `yaml:"middlewares" json:"middlewares"`
	// Routes map incoming requests to upstreams.
	Routes []Route    `yaml:"routes" json:"routes"`
	lines  *lineIndex // source line index of the parsed document; nil for configurations built in Go
}

// Global holds process-wide settings. Fields left at their zero value take the defaults noted on each field.
type Global struct {
	// RequestIDHeader is the header that carries the request identifier. Default "X-Request-ID".
	RequestIDHeader string `yaml:"request_id_header" json:"request_id_header"`
	// TrustedProxies lists addresses or CIDR prefixes whose forwarding headers are trusted.
	TrustedProxies []string `yaml:"trusted_proxies" json:"trusted_proxies"`
	// StripIdentityHeaders lists headers removed from inbound requests. Default [X-Consumer-ID, X-Consumer-Metadata, X-User-ID, X-Auth-Subject, X-Auth-Claims], applied only when nil.
	StripIdentityHeaders []string `yaml:"strip_identity_headers" json:"strip_identity_headers"`
	// DrainTimeout bounds how long shutdown waits for in-flight requests. Default 30s.
	DrainTimeout Duration `yaml:"drain_timeout" json:"drain_timeout"`
	// ShutdownDelay is how long the gateway keeps serving after it starts shutting down. Default 0.
	ShutdownDelay Duration `yaml:"shutdown_delay" json:"shutdown_delay"`
	// Log configures logging.
	Log Log `yaml:"log" json:"log"`
	// Reload configures configuration reloading.
	Reload Reload `yaml:"reload" json:"reload"`
}

// Log configures logging. Fields left at their zero value take the defaults noted on each field.
type Log struct {
	// Level is one of debug, info, warn, error. Default "info".
	Level string `yaml:"level" json:"level"`
	// Format is one of json, text. Default "json".
	Format string `yaml:"format" json:"format"`
	// AccessLog enables per-request access log lines. Default true when nil.
	AccessLog *bool `yaml:"access_log" json:"access_log"`
}

// Reload configures configuration reloading. Fields left at their zero value take the defaults noted on each field.
type Reload struct {
	// Debounce is the quiet period that coalesces bursts of change events. Default 250ms.
	Debounce Duration `yaml:"debounce" json:"debounce"`
	// PollInterval is how often the file is checked for changes. Default 5s.
	PollInterval Duration `yaml:"poll_interval" json:"poll_interval"`
}

// Listener is one address the gateway accepts connections on. Fields left at their zero value take the defaults noted on each field.
type Listener struct {
	// Name identifies the listener and must be unique.
	Name string `yaml:"name" json:"name"`
	// Address is a host:port pair to listen on.
	Address string `yaml:"address" json:"address"`
	// TLS enables TLS on the listener when set.
	TLS *TLS `yaml:"tls" json:"tls"`
	// HTTP2 enables HTTP/2. Default true when nil.
	HTTP2 *bool `yaml:"http2" json:"http2"`
	// H2C enables cleartext HTTP/2 when TLS is not used.
	H2C bool `yaml:"h2c" json:"h2c"`
	// ReadHeaderTimeout bounds reading request headers. Default 10s.
	ReadHeaderTimeout Duration `yaml:"read_header_timeout" json:"read_header_timeout"`
	// ReadTimeout bounds reading an entire request. Default 0, meaning no limit.
	ReadTimeout Duration `yaml:"read_timeout" json:"read_timeout"`
	// WriteTimeout bounds writing a response. Default 0, meaning no limit.
	WriteTimeout Duration `yaml:"write_timeout" json:"write_timeout"`
	// IdleTimeout bounds keep-alive idle time. Default 120s.
	IdleTimeout Duration `yaml:"idle_timeout" json:"idle_timeout"`
	// MaxHeaderBytes bounds the size of request headers. Default 1048576.
	MaxHeaderBytes int `yaml:"max_header_bytes" json:"max_header_bytes"`
}

// TLS holds the certificate material for a listener. Fields left at their zero value take the defaults noted on each field.
type TLS struct {
	// CertFile is the path to the PEM certificate chain.
	CertFile string `yaml:"cert_file" json:"cert_file"`
	// KeyFile is the path to the PEM private key.
	KeyFile string `yaml:"key_file" json:"key_file"`
	// MinVersion is the minimum TLS version, 1.2 or 1.3. Default "1.2".
	MinVersion string `yaml:"min_version" json:"min_version"`
}

// Admin configures the administrative endpoint. Fields left at their zero value take the defaults noted on each field.
type Admin struct {
	// Enabled turns the administrative endpoint on. Default true when nil.
	Enabled *bool `yaml:"enabled" json:"enabled"`
	// Address is the host:port the endpoint listens on. Default "127.0.0.1:9901".
	Address string `yaml:"address" json:"address"`
	// Auth holds the bearer tokens accepted by mutating admin endpoints. Empty disables them.
	Auth AdminAuth `yaml:"auth" json:"auth"`
}

// AdminAuth lists the credentials of the mutating admin endpoints (cache purge). With no keys those endpoints are absent.
type AdminAuth struct {
	// Keys lists the accepted bearer tokens, stored as hashes.
	Keys []AdminKey `yaml:"keys" json:"keys"`
}

// AdminKey is one accepted admin bearer token, stored as a hash.
type AdminKey struct {
	// ID identifies the key in logs and must be unique.
	ID string `yaml:"id" json:"id"`
	// Hash is the sha256 digest of the token in the form sha256:<64 hex digits>, unique among the keys. Reveal returns the value; every formatting and encoding path redacts it.
	Hash Secret `yaml:"hash" json:"hash"`
}

// Upstream is a backend pool. Exactly one of Hosts or DNS must be set. Fields left at their zero value take the defaults noted on each field.
type Upstream struct {
	// Name identifies the upstream and must be unique.
	Name string `yaml:"name" json:"name"`
	// Hosts lists static backend addresses.
	Hosts []Host `yaml:"hosts" json:"hosts"`
	// DNS discovers backends by resolving a hostname.
	DNS *DNS `yaml:"dns" json:"dns"`
	// LoadBalancer selects how requests are spread across backends.
	LoadBalancer LoadBalancer `yaml:"load_balancer" json:"load_balancer"`
	// Timeouts bounds connection and response phases.
	Timeouts Timeouts `yaml:"timeouts" json:"timeouts"`
	// Pool controls the connection pool to each backend.
	Pool Pool `yaml:"pool" json:"pool"`
	// HTTP2 enables HTTP/2 towards the backends.
	HTTP2 bool `yaml:"http2" json:"http2"`
	// PassiveHealth ejects backends based on observed failures when set.
	PassiveHealth *PassiveHealth `yaml:"passive_health" json:"passive_health"`
	// ActiveHealth probes backends on a schedule when set.
	ActiveHealth *ActiveHealth `yaml:"active_health" json:"active_health"`
	// CircuitBreaker stops traffic to the upstream after repeated failures when set.
	CircuitBreaker *CircuitBreaker `yaml:"circuit_breaker" json:"circuit_breaker"`
}

// Host is one static backend address. Fields left at their zero value take the defaults noted on each field.
type Host struct {
	// URL is the backend base URL, with an http or https scheme. Reveal returns the value; every formatting and encoding path redacts its userinfo and query values.
	URL SecretURL `yaml:"url" json:"url"`
	// Weight is the relative share of traffic. Default 1 when absent; 0 means the host is never selected.
	Weight *int `yaml:"weight" json:"weight"`
}

// DNS describes a backend discovered through DNS; hosts get weight 1, A/AAAA only, at most 256 addresses. Fields left at their zero value take the defaults noted on each field.
type DNS struct {
	// Hostname is the name to resolve.
	Hostname string `yaml:"hostname" json:"hostname"`
	// Port is the backend port.
	Port int `yaml:"port" json:"port"`
	// Scheme is http or https. Default "http".
	Scheme string `yaml:"scheme" json:"scheme"`
	// RefreshInterval is how often the name is resolved again. Default 30s; minimum 1s.
	RefreshInterval Duration `yaml:"refresh_interval" json:"refresh_interval"`
}

// LoadBalancer selects the balancing algorithm. Fields left at their zero value take the defaults noted on each field.
type LoadBalancer struct {
	// Algorithm is one of p2c, round_robin, weighted_round_robin, random, consistent_hash. Default "p2c".
	Algorithm string `yaml:"algorithm" json:"algorithm"`
	// Hash configures the key used by consistent hashing.
	Hash Hash `yaml:"hash" json:"hash"`
	// BoundedLoadFactor caps per-backend load relative to the average under consistent hashing. Default 1.25.
	BoundedLoadFactor float64 `yaml:"bounded_load_factor" json:"bounded_load_factor"`
	// VirtualNodes is the number of ring points per backend. Default 160.
	VirtualNodes int `yaml:"virtual_nodes" json:"virtual_nodes"`
}

// Hash selects the request attribute hashed for consistent hashing.
type Hash struct {
	// Source is one of header, cookie, ip, path.
	Source string `yaml:"source" json:"source"`
	// Name is the header or cookie name, required for header and cookie sources.
	Name string `yaml:"name" json:"name"`
}

// Timeouts bounds the phases of an upstream exchange. Fields left at their zero value take the defaults noted on each field.
type Timeouts struct {
	// Connect bounds establishing a connection. Default 5s.
	Connect Duration `yaml:"connect" json:"connect"`
	// TLSHandshake bounds the TLS handshake. Default 10s.
	TLSHandshake Duration `yaml:"tls_handshake" json:"tls_handshake"`
	// ResponseHeader bounds the wait for response headers. Default 30s.
	ResponseHeader Duration `yaml:"response_header" json:"response_header"`
	// Idle bounds how long an idle pooled connection is kept. Default 90s.
	Idle Duration `yaml:"idle" json:"idle"`
}

// Pool controls connection pooling per backend. Fields left at their zero value take the defaults noted on each field.
type Pool struct {
	// MaxIdlePerHost is the number of idle connections kept per backend. Default 64.
	MaxIdlePerHost int `yaml:"max_idle_per_host" json:"max_idle_per_host"`
	// MaxConnsPerHost caps open connections per backend. Default 0, meaning no limit.
	MaxConnsPerHost int `yaml:"max_conns_per_host" json:"max_conns_per_host"`
}

// PassiveHealth ejects a backend after observed failures. Fields left at their zero value take the defaults noted on each field.
type PassiveHealth struct {
	// ConsecutiveFailures is the failure count that triggers ejection. Default 5.
	ConsecutiveFailures int `yaml:"consecutive_failures" json:"consecutive_failures"`
	// BaseEjectionTime is the first ejection duration. Default 30s.
	BaseEjectionTime Duration `yaml:"base_ejection_time" json:"base_ejection_time"`
	// MaxEjectionTime caps the ejection duration as it grows. Default 300s.
	MaxEjectionTime Duration `yaml:"max_ejection_time" json:"max_ejection_time"`
	// MaxEjectionPercent caps the share of backends that may be ejected at once. Default 50 when absent; 0 disables ejection; must be 0..100.
	MaxEjectionPercent *int `yaml:"max_ejection_percent" json:"max_ejection_percent"`
}

// ActiveHealth probes backends on a schedule. Pointer fields take their default only when absent; other fields left at their zero value take the defaults noted on each field.
type ActiveHealth struct {
	// Path is the request path probed. Default "/health".
	Path string `yaml:"path" json:"path"`
	// Interval is the time between probes. Default 10s.
	Interval Duration `yaml:"interval" json:"interval"`
	// Jitter randomizes probe timing by up to this fraction. Default 0.1 when absent; 0 disables jitter; must be in [0, 1).
	Jitter *float64 `yaml:"jitter" json:"jitter"`
	// Timeout bounds a single probe. Default 2s.
	Timeout Duration `yaml:"timeout" json:"timeout"`
	// HealthyThreshold is the successes needed to mark a backend healthy. Default 2 when absent; must be >= 1.
	HealthyThreshold *int `yaml:"healthy_threshold" json:"healthy_threshold"`
	// UnhealthyThreshold is the failures needed to mark a backend unhealthy. Default 3 when absent; must be >= 1.
	UnhealthyThreshold *int `yaml:"unhealthy_threshold" json:"unhealthy_threshold"`
	// ExpectedStatuses lists accepted status codes as "NNN" or "NNN-NNN". Default ["200-399"], applied only when nil.
	ExpectedStatuses []string `yaml:"expected_statuses" json:"expected_statuses"`
}

// CircuitBreaker stops traffic after repeated failures. Pointer fields take their default only when absent; other fields left at their zero value take the defaults noted on each field.
type CircuitBreaker struct {
	// Window is the sliding window over which failures are counted. Default 10s.
	Window Duration `yaml:"window" json:"window"`
	// Buckets is the number of slices the window is divided into. Default 10 when absent; 1-1000; window/buckets must be at least 1ms.
	Buckets *int `yaml:"buckets" json:"buckets"`
	// MinRequests is the request count needed in the window before the ratio is evaluated. Default 20 when absent; must be >= 1.
	MinRequests *int `yaml:"min_requests" json:"min_requests"`
	// FailureRatio opens the breaker when failures reach this fraction. Default 0.5 when absent; must be in (0, 1].
	FailureRatio *float64 `yaml:"failure_ratio" json:"failure_ratio"`
	// ConsecutiveFailures opens the breaker after this many failures in a row. Default 5 when absent; 0 disables the consecutive rule.
	ConsecutiveFailures *int `yaml:"consecutive_failures" json:"consecutive_failures"`
	// OpenTimeout is how long the breaker stays open before probing. Default 30s.
	OpenTimeout Duration `yaml:"open_timeout" json:"open_timeout"`
	// HalfOpenProbes is the number of trial requests allowed while half open. Default 3 when absent; must be >= 1 and >= success_threshold.
	HalfOpenProbes *int `yaml:"half_open_probes" json:"half_open_probes"`
	// SuccessThreshold is the successful probes needed to close the breaker. Default 2 when absent; must be >= 1.
	SuccessThreshold *int `yaml:"success_threshold" json:"success_threshold"`
}

// Auth declares the authentication providers available to routes.
type Auth struct {
	// JWT lists JSON Web Token providers.
	JWT []JWTProvider `yaml:"jwt" json:"jwt"`
	// APIKeys lists API key providers.
	APIKeys []APIKeyProvider `yaml:"api_keys" json:"api_keys"`
}

// JWTProvider verifies bearer tokens. Pointer fields take their default only when absent; other fields left at their zero value take the defaults noted on each field.
type JWTProvider struct {
	// Name identifies the provider; names are unique across all providers.
	Name string `yaml:"name" json:"name"`
	// Issuer is the required token issuer.
	Issuer string `yaml:"issuer" json:"issuer"`
	// Audiences lists accepted token audiences; at least one is required.
	Audiences []string `yaml:"audiences" json:"audiences"`
	// Leeway tolerates clock skew when checking token times. Default 30s when absent; 0 means no leeway; max 5m.
	Leeway *Duration `yaml:"leeway" json:"leeway"`
	// Header is the header carrying the token. Default "Authorization".
	Header string `yaml:"header" json:"header"`
	// Keys lists static verification keys.
	Keys []JWTKey `yaml:"keys" json:"keys"`
	// JWKS fetches verification keys from a remote key set when set.
	JWKS *JWKS `yaml:"jwks" json:"jwks"`
	// ScopeClaim is the claim holding scopes. Default "scope".
	ScopeClaim string `yaml:"scope_claim" json:"scope_claim"`
	// RolesClaim is the claim holding roles. Default "roles".
	RolesClaim string `yaml:"roles_claim" json:"roles_claim"`
	// ForwardClaims maps claim names to the upstream header that carries their values.
	ForwardClaims map[string]string `yaml:"forward_claims" json:"forward_claims"`
	// MaxTokenBytes bounds the size of a presented token. Default 4096 when absent; must be positive.
	MaxTokenBytes *int `yaml:"max_token_bytes" json:"max_token_bytes"`
	// ForwardCredentials keeps the token on the upstream request. Default false when absent.
	ForwardCredentials *bool `yaml:"forward_credentials" json:"forward_credentials"`
}

// JWTKey is one verification key. Exactly one of Secret, PublicKey or PublicKeyFile must be set.
type JWTKey struct {
	// KID is the key identifier matched against the token header; required when there is more than one key.
	KID string `yaml:"kid" json:"kid"`
	// Algorithm is the signing algorithm the key verifies.
	Algorithm string `yaml:"algorithm" json:"algorithm"`
	// Secret is the shared secret for HS algorithms. Reveal returns the value; every formatting and encoding path redacts it.
	Secret Secret `yaml:"secret" json:"secret"`
	// PublicKey is a PEM encoded public key for asymmetric algorithms.
	PublicKey string `yaml:"public_key" json:"public_key"`
	// PublicKeyFile is a path to a PEM encoded public key.
	PublicKeyFile string `yaml:"public_key_file" json:"public_key_file"`
	// LoadedPublicKey is the PEM content read from PublicKeyFile when the configuration is loaded or validated; any value set by the caller is replaced.
	LoadedPublicKey string `yaml:"-" json:"loaded_public_key,omitempty"`
}

// JWKS describes a remote JSON Web Key Set. Pointer fields take their default only when absent; other fields left at their zero value take the defaults noted on each field.
type JWKS struct {
	// URL is the http or https location of the key set. Reveal returns the value; every formatting and encoding path redacts its userinfo and query values.
	URL SecretURL `yaml:"url" json:"url"`
	// Algorithms lists the asymmetric algorithms accepted from the key set; required.
	Algorithms []string `yaml:"algorithms" json:"algorithms"`
	// TTL is how long fetched keys are used before refresh. Default 10m when absent; must be positive.
	TTL *Duration `yaml:"ttl" json:"ttl"`
	// MinRefreshInterval is the minimum time between refreshes. Default 30s when absent; must be positive.
	MinRefreshInterval *Duration `yaml:"min_refresh_interval" json:"min_refresh_interval"`
	// Timeout bounds a key set fetch. Default 5s when absent; must be positive.
	Timeout *Duration `yaml:"timeout" json:"timeout"`
}

// APIKeyProvider authenticates requests by API key. Pointer fields take their default only when absent; other fields left at their zero value take the defaults noted on each field.
type APIKeyProvider struct {
	// Name identifies the provider; names are unique across all providers.
	Name string `yaml:"name" json:"name"`
	// Header is the header carrying the key. Default "X-API-Key" when absent; an explicit "" disables the header and requires Query.
	Header *string `yaml:"header" json:"header"`
	// Query is the query parameter carrying the key as an alternative to Header.
	Query string `yaml:"query" json:"query"`
	// KeyFile is a path to a file whose keys are loaded into FileKeys when the configuration is loaded or validated.
	KeyFile string `yaml:"key_file" json:"key_file"`
	// Keys lists inline keys.
	Keys []APIKey `yaml:"keys" json:"keys"`
	// ConsumerHeader is the header that receives the consumer name. Default "X-Consumer-ID".
	ConsumerHeader string `yaml:"consumer_header" json:"consumer_header"`
	// MetadataHeaders maps metadata field names to the headers that receive their values.
	MetadataHeaders map[string]string `yaml:"metadata_headers" json:"metadata_headers"`
	// ForwardCredentials keeps the key on the upstream request. Default false when absent.
	ForwardCredentials *bool `yaml:"forward_credentials" json:"forward_credentials"`
	// FileKeys holds the keys read from KeyFile when the configuration is loaded or validated; replaced on every load.
	FileKeys []APIKey `yaml:"-" json:"file_keys,omitempty"`
}

// APIKey is one accepted API key, stored as a hash.
type APIKey struct {
	// ID identifies the key and must be unique within its provider.
	ID string `yaml:"id" json:"id"`
	// Hash is the key digest in the form sha256:<64 hex digits>. Reveal returns the value; every formatting and encoding path redacts it.
	Hash Secret `yaml:"hash" json:"hash"`
	// Consumer names the client the key belongs to.
	Consumer string `yaml:"consumer" json:"consumer"`
	// Metadata holds extra values forwarded with the consumer.
	Metadata map[string]string `yaml:"metadata" json:"metadata"`
	// Scopes lists the scopes granted to the consumer of the key.
	Scopes []string `yaml:"scopes" json:"scopes"`
	// Roles lists the roles granted to the consumer of the key.
	Roles []string `yaml:"roles" json:"roles"`
}

// APIKeyFile is the format of an API key file referenced by key_file.
type APIKeyFile struct {
	// Keys lists the keys in the file.
	Keys []APIKey `yaml:"keys" json:"keys"`
}

// Middleware is a named middleware instance that routes can reference.
type Middleware struct {
	// Name identifies the middleware and must be unique.
	Name string `yaml:"name" json:"name"`
	// Type selects the middleware implementation.
	Type string `yaml:"type" json:"type"`
	// Params carries implementation specific settings. Params are secret: every leaf is redacted when formatted or encoded; keys are kept.
	Params SecretParams `yaml:"params" json:"params"`
}

// Route maps matching requests to an upstream. Fields left at their zero value take the defaults noted on each field.
type Route struct {
	// Name identifies the route and must be unique.
	Name string `yaml:"name" json:"name"`
	// Match selects the requests the route handles.
	Match Match `yaml:"match" json:"match"`
	// Priority orders routes that could match the same request.
	Priority int `yaml:"priority" json:"priority"`
	// Upstream names the upstream requests are forwarded to.
	Upstream string `yaml:"upstream" json:"upstream"`
	// PreserveHost forwards the original Host header instead of the upstream host.
	PreserveHost bool `yaml:"preserve_host" json:"preserve_host"`
	// Rewrite changes the request path before forwarding.
	Rewrite Rewrite `yaml:"rewrite" json:"rewrite"`
	// RequestHeaders edits headers of the forwarded request.
	RequestHeaders HeaderOps `yaml:"request_headers" json:"request_headers"`
	// ResponseHeaders edits headers of the response.
	ResponseHeaders HeaderOps `yaml:"response_headers" json:"response_headers"`
	// Timeout bounds the whole exchange. Default 0, meaning none.
	Timeout Duration `yaml:"timeout" json:"timeout"`
	// Auth configures authentication for the route when set.
	Auth *RouteAuth `yaml:"auth" json:"auth"`
	// RateLimit configures request rate limiting when set.
	RateLimit *RateLimit `yaml:"rate_limit" json:"rate_limit"`
	// Cache configures response caching for the route when set.
	Cache *RouteCache `yaml:"cache" json:"cache"`
	// CircuitBreaker enables the upstream circuit breaker for this route when set.
	CircuitBreaker *RouteBreaker `yaml:"circuit_breaker" json:"circuit_breaker"`
	// Retry configures retries of failed upstream exchanges when set.
	Retry *Retry `yaml:"retry" json:"retry"`
	// Middlewares lists top-level middleware names applied to the route.
	Middlewares []string `yaml:"middlewares" json:"middlewares"`
	// Pipeline orders the built-in stages and middlewares for the route; cache must come before circuit_breaker and retry; retry must be last when present.
	Pipeline []string `yaml:"pipeline" json:"pipeline"`
}

// Match selects requests by host, path, method and headers. Fields left at their zero value take the defaults noted on each field.
type Match struct {
	// Hosts lists host names or "*." wildcards; empty matches any host.
	Hosts []string `yaml:"hosts" json:"hosts"`
	// Path is the path to match. Default "/".
	Path string `yaml:"path" json:"path"`
	// PathType is one of exact, prefix, pattern. Default "prefix".
	PathType string `yaml:"path_type" json:"path_type"`
	// Methods lists HTTP methods; empty matches any method.
	Methods []string `yaml:"methods" json:"methods"`
	// Headers lists header values the request must carry. Values are secrets: Reveal returns the value; every formatting and encoding path redacts it.
	Headers map[string]Secret `yaml:"headers" json:"headers"`
}

// Rewrite changes the request path before it is forwarded.
type Rewrite struct {
	// StripPrefix removes this prefix from the path.
	StripPrefix string `yaml:"strip_prefix" json:"strip_prefix"`
	// ReplacePrefix replaces one path prefix with another.
	ReplacePrefix ReplacePrefix `yaml:"replace_prefix" json:"replace_prefix"`
}

// ReplacePrefix replaces the From prefix of the path with To.
type ReplacePrefix struct {
	// From is the prefix to replace.
	From string `yaml:"from" json:"from"`
	// To is the replacement prefix.
	To string `yaml:"to" json:"to"`
}

// HeaderOps edits headers. Add and Set map header names to values; Remove lists header names. Values are secrets: Reveal returns the value; every formatting and encoding path redacts it.
type HeaderOps struct {
	// Add appends values to the named headers.
	Add map[string]Secret `yaml:"add" json:"add"`
	// Set replaces the named headers.
	Set map[string]Secret `yaml:"set" json:"set"`
	// Remove deletes the named headers.
	Remove []string `yaml:"remove" json:"remove"`
}

// RouteAuth configures authentication for a route. Fields left at their zero value take the defaults noted on each field.
type RouteAuth struct {
	// Mode is one of none, jwt, apikey, any. Default "none".
	Mode string `yaml:"mode" json:"mode"`
	// Providers lists the provider names accepted for the route.
	Providers []string `yaml:"providers" json:"providers"`
	// AllowAnonymous lets requests without valid credentials through.
	AllowAnonymous bool `yaml:"allow_anonymous" json:"allow_anonymous"`
	// Require lists the claims a verified identity must satisfy.
	Require Require `yaml:"require" json:"require"`
}

// Require lists conditions a verified identity must meet.
type Require struct {
	// Scopes lists scopes the identity must hold.
	Scopes []string `yaml:"scopes" json:"scopes"`
	// Roles lists roles the identity must hold.
	Roles []string `yaml:"roles" json:"roles"`
	// Claims maps claim names to the expected value: a string, number or boolean. For API keys the claim is matched against metadata.
	Claims map[string]any `yaml:"claims" json:"claims"`
}

// RateLimit configures request rate limiting. Pointer fields distinguish absent from explicit zero: an absent field takes its default, while an explicit zero or negative value is a validation error and is never replaced by a default.
type RateLimit struct {
	Algorithm    string       `yaml:"algorithm" json:"algorithm"` // token_bucket | sliding_window. Default "token_bucket".
	Rate         *int64       `yaml:"rate" json:"rate"`           // tokens per Per; required for token_bucket; >= 1.
	Per          *Duration    `yaml:"per" json:"per"`             // token_bucket only. Default 1s when absent; > 0.
	Burst        *int64       `yaml:"burst" json:"burst"`         // token_bucket only. Default equal to Rate when absent; >= 1.
	Limit        *int64       `yaml:"limit" json:"limit"`         // requests per Window; required for sliding_window; >= 1.
	Window       *Duration    `yaml:"window" json:"window"`       // sliding_window only. Default 1m when absent; > 0.
	Key          RateLimitKey `yaml:"key" json:"key"`
	MaxKeys      *int         `yaml:"max_keys" json:"max_keys"`             // keys kept in memory per instance. Default 100000 when absent; >= 1.
	Store        string       `yaml:"store" json:"store"`                   // named store holding the counters; empty keeps them in memory per instance.
	OnStoreError string       `yaml:"on_store_error" json:"on_store_error"` // local|allow|deny when the store fails. Default local when store is set.
	StoreTimeout *Duration    `yaml:"store_timeout" json:"store_timeout"`   // bound on each store call. Default 50ms when store is set; > 0.
}

// Store declares a named backing store that rate limits can reference.
type Store struct {
	Name   string       `yaml:"name" json:"name"`
	Type   string       `yaml:"type" json:"type"`
	Redis  *RedisStore  `yaml:"redis" json:"redis"`
	Guard  StoreGuard   `yaml:"guard" json:"guard"`
	Params SecretParams `yaml:"params" json:"params"`
}

// StoreGuard bounds how a store is called. Pointer fields take their default only when absent.
type StoreGuard struct {
	ConsecutiveFailures *int      `yaml:"consecutive_failures" json:"consecutive_failures"`
	OpenTimeout         *Duration `yaml:"open_timeout" json:"open_timeout"`
	MaxInflight         *int      `yaml:"max_inflight" json:"max_inflight"`
}

// RedisStore configures a Redis backed store. Fields left at their zero value take the defaults noted in the store checks.
type RedisStore struct {
	Mode             string    `yaml:"mode" json:"mode"`
	Addresses        []string  `yaml:"addresses" json:"addresses"`
	MasterName       string    `yaml:"master_name" json:"master_name"`
	Username         string    `yaml:"username" json:"username"`
	Password         Secret    `yaml:"password" json:"password"`
	SentinelUsername string    `yaml:"sentinel_username" json:"sentinel_username"`
	SentinelPassword Secret    `yaml:"sentinel_password" json:"sentinel_password"`
	DB               int       `yaml:"db" json:"db"`
	TLS              *RedisTLS `yaml:"tls" json:"tls"`
	DialTimeout      *Duration `yaml:"dial_timeout" json:"dial_timeout"`
	ReadTimeout      *Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout     *Duration `yaml:"write_timeout" json:"write_timeout"`
	PoolSize         int       `yaml:"pool_size" json:"pool_size"`
	MinIdleConns     int       `yaml:"min_idle_conns" json:"min_idle_conns"`
	KeyPrefix        *string   `yaml:"key_prefix" json:"key_prefix"`
}

// RedisTLS configures TLS towards Redis servers.
type RedisTLS struct {
	Enabled            bool   `yaml:"enabled" json:"enabled"`
	CAFile             string `yaml:"ca_file" json:"ca_file"`
	CertFile           string `yaml:"cert_file" json:"cert_file"`
	KeyFile            string `yaml:"key_file" json:"key_file"`
	ServerName         string `yaml:"server_name" json:"server_name"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify" json:"insecure_skip_verify"`
}

// RateLimitKey selects the attribute limits are applied per. Fields left at their zero value take the defaults noted on each field.
type RateLimitKey struct {
	// Source is one of ip, consumer, header, route. Default "ip".
	Source string `yaml:"source" json:"source"`
	// Name is the header name. It is required when Source is header and is only valid with Source header.
	Name string `yaml:"name" json:"name"`
}

// RouteBreaker enables the circuit breaker for a route. Fields left at their zero value take the defaults noted on each field.
type RouteBreaker struct {
	// Enabled turns the breaker on. Default true when nil.
	Enabled *bool `yaml:"enabled" json:"enabled"`
}

// Retry configures retries of failed exchanges. Pointer fields take their default only when absent; other fields left at their zero value take the defaults noted on each field.
type Retry struct {
	// Attempts is the total number of tries including the first. Default 3 when absent; 1-10.
	Attempts *int `yaml:"attempts" json:"attempts"`
	// BaseDelay is the first backoff delay. Default 25ms.
	BaseDelay Duration `yaml:"base_delay" json:"base_delay"`
	// MaxDelay caps the backoff delay. Default 250ms.
	MaxDelay Duration `yaml:"max_delay" json:"max_delay"`
	// OnStatuses lists response statuses that trigger a retry; statuses 400-599. Default [502, 503, 504], applied only when nil.
	OnStatuses []int `yaml:"on_statuses" json:"on_statuses"`
	// Methods lists methods that may be retried. Default [GET, HEAD, OPTIONS, PUT, DELETE], applied only when nil.
	Methods []string `yaml:"methods" json:"methods"`
	// IdempotencyKey controls the idempotency key header sent with retries.
	IdempotencyKey IdempotencyKey `yaml:"idempotency_key" json:"idempotency_key"`
	// Budget limits how many retries may be issued relative to traffic.
	Budget RetryBudget `yaml:"budget" json:"budget"`
	// MaxBodyBytes is the largest request body that is buffered for retry. Default 65536 when absent; 0 disables body buffering, so requests with a body are only retried on a connect failure before any byte was sent.
	MaxBodyBytes *int64 `yaml:"max_body_bytes" json:"max_body_bytes"`
}

// IdempotencyKey controls the idempotency key header. Fields left at their zero value take the defaults noted on each field.
type IdempotencyKey struct {
	// Enabled lets methods outside methods be retried when the request carries a non-empty Header.
	Enabled bool `yaml:"enabled" json:"enabled"`
	// Header is the header name for the key. Default "Idempotency-Key".
	Header string `yaml:"header" json:"header"`
}

// RetryBudget bounds retries as a share of traffic. Fields left at their zero value take the defaults noted on each field.
type RetryBudget struct {
	// Ratio is the retry share of requests, in [0, 1]. Default 0.1 when absent; 0 is kept.
	Ratio *float64 `yaml:"ratio" json:"ratio"`
	// MinPerSecond is the retry rate always allowed regardless of traffic. Default 10 when absent; 0 is kept.
	MinPerSecond *float64 `yaml:"min_per_second" json:"min_per_second"`
	// Window is the period the budget is measured over. Default 10s.
	Window Duration `yaml:"window" json:"window"`
}

// Cache configures the response cache. Pointer fields distinguish absent from explicit zero: an absent field takes its default, while an explicit zero or empty value is a validation error and is never replaced by a default.
type Cache struct {
	Name           *string         `yaml:"name" json:"name"`                         // Cache-Status identifier. Default "microgateway"; 1-128 printable ASCII.
	Store          string          `yaml:"store" json:"store"`                       // stores[].name; empty = built-in memory per instance (not shared between instances); a named store must support caching.
	Memory         CacheMemory     `yaml:"memory" json:"memory"`                     // Built-in memory store settings; used when store is empty, per instance.
	MaxBodyBytes   *int64          `yaml:"max_body_bytes" json:"max_body_bytes"`     // Default 1048576; 1..67108864.
	StoreTimeout   *Duration       `yaml:"store_timeout" json:"store_timeout"`       // Default 100ms; > 0.
	TagHeader      *string         `yaml:"tag_header" json:"tag_header"`             // Default "Surrogate-Key"; valid header name.
	TagDelimiter   string          `yaml:"tag_delimiter" json:"tag_delimiter"`       // space | comma. Default space.
	StripTagHeader *bool           `yaml:"strip_tag_header" json:"strip_tag_header"` // Default true.
	StatusKey      bool            `yaml:"status_key" json:"status_key"`             // Default false.
	Coalescing     CacheCoalescing `yaml:"coalescing" json:"coalescing"`             // Request coalescing settings for concurrent misses.
	MaxBackground  *int            `yaml:"max_background" json:"max_background"`     // Default 64; >= 1.
}

// CacheMemory configures the built-in in-memory cache store. Pointer fields distinguish absent from explicit zero.
type CacheMemory struct {
	MaxBytes *int64 `yaml:"max_bytes" json:"max_bytes"` // Default 67108864; >= 1.
	Shards   *int   `yaml:"shards" json:"shards"`       // Default 16; 1..1024.
}

// CacheCoalescing configures request coalescing for the response cache. Pointer fields distinguish absent from explicit zero.
type CacheCoalescing struct {
	Enabled    *bool     `yaml:"enabled" json:"enabled"`         // Default true.
	Timeout    *Duration `yaml:"timeout" json:"timeout"`         // Default 5s; > 0.
	MaxKeys    *int      `yaml:"max_keys" json:"max_keys"`       // Default 10000; >= 1.
	MaxWaiters *int      `yaml:"max_waiters" json:"max_waiters"` // Default 1024; >= 1.
}

// RouteCache enables response caching for a route. Pointer fields distinguish absent from explicit zero. Every duration is at most 365 days.
type RouteCache struct {
	Enabled              bool      `yaml:"enabled" json:"enabled"`                               // Default false.
	DefaultTTL           Duration  `yaml:"default_ttl" json:"default_ttl"`                       // Default 0; 0..max_ttl.
	MaxTTL               *Duration `yaml:"max_ttl" json:"max_ttl"`                               // Default 24h; > 0.
	Keep                 Duration  `yaml:"keep" json:"keep"`                                     // Default 0; >= 0.
	StaleWhileRevalidate Duration  `yaml:"stale_while_revalidate" json:"stale_while_revalidate"` // Default 0; >= 0.
	StaleIfError         Duration  `yaml:"stale_if_error" json:"stale_if_error"`                 // Default 0; >= 0.
	Statuses             []int     `yaml:"statuses" json:"statuses"`                             // Default [200 203 204 300 301 308 404 405 410 414 501]; non-empty when set.
}

// BuiltinStages lists the stage names a route pipeline may use without a middleware definition.
var BuiltinStages = []string{"timeout", "auth", "rate_limit", "cache", "circuit_breaker", "retry"}
