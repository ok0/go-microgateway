// Package config defines the lychgate configuration schema, parses it strictly from YAML, and validates it.
package config

// Config is the root of a lychgate configuration document.
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
	// Auth declares the authentication providers.
	Auth Auth `yaml:"auth" json:"auth"`
	// Middlewares are named middleware instances referenced by routes.
	Middlewares []Middleware `yaml:"middlewares" json:"middlewares"`
	// Routes map incoming requests to upstreams.
	Routes []Route `yaml:"routes" json:"routes"`
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
	// URL is the backend base URL, with an http or https scheme.
	URL string `yaml:"url" json:"url"`
	// Weight is the relative share of traffic. Default 1 when absent; 0 means the host is never selected.
	Weight *int `yaml:"weight" json:"weight"`
}

// DNS describes a backend discovered through DNS. Fields left at their zero value take the defaults noted on each field.
type DNS struct {
	// Hostname is the name to resolve.
	Hostname string `yaml:"hostname" json:"hostname"`
	// Port is the backend port.
	Port int `yaml:"port" json:"port"`
	// Scheme is http or https. Default "http".
	Scheme string `yaml:"scheme" json:"scheme"`
	// RefreshInterval is how often the name is resolved again. Default 30s.
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
	// MaxEjectionPercent caps the share of backends that may be ejected at once. Default 50.
	MaxEjectionPercent int `yaml:"max_ejection_percent" json:"max_ejection_percent"`
}

// ActiveHealth probes backends on a schedule. Fields left at their zero value take the defaults noted on each field.
type ActiveHealth struct {
	// Path is the request path probed. Default "/health".
	Path string `yaml:"path" json:"path"`
	// Interval is the time between probes. Default 10s.
	Interval Duration `yaml:"interval" json:"interval"`
	// Jitter randomizes probe timing by up to this fraction. Default 0.1.
	Jitter float64 `yaml:"jitter" json:"jitter"`
	// Timeout bounds a single probe. Default 2s.
	Timeout Duration `yaml:"timeout" json:"timeout"`
	// HealthyThreshold is the successes needed to mark a backend healthy. Default 2.
	HealthyThreshold int `yaml:"healthy_threshold" json:"healthy_threshold"`
	// UnhealthyThreshold is the failures needed to mark a backend unhealthy. Default 3.
	UnhealthyThreshold int `yaml:"unhealthy_threshold" json:"unhealthy_threshold"`
	// ExpectedStatuses lists accepted status codes as "NNN" or "NNN-NNN". Default ["200-399"], applied only when nil.
	ExpectedStatuses []string `yaml:"expected_statuses" json:"expected_statuses"`
}

// CircuitBreaker stops traffic after repeated failures. Fields left at their zero value take the defaults noted on each field.
type CircuitBreaker struct {
	// Window is the sliding window over which failures are counted. Default 10s.
	Window Duration `yaml:"window" json:"window"`
	// Buckets is the number of slices the window is divided into. Default 10.
	Buckets int `yaml:"buckets" json:"buckets"`
	// MinRequests is the request count needed in the window before the ratio is evaluated. Default 20.
	MinRequests int `yaml:"min_requests" json:"min_requests"`
	// FailureRatio opens the breaker when failures reach this fraction. Default 0.5.
	FailureRatio float64 `yaml:"failure_ratio" json:"failure_ratio"`
	// ConsecutiveFailures opens the breaker after this many failures in a row. Default 5.
	ConsecutiveFailures int `yaml:"consecutive_failures" json:"consecutive_failures"`
	// OpenTimeout is how long the breaker stays open before probing. Default 30s.
	OpenTimeout Duration `yaml:"open_timeout" json:"open_timeout"`
	// HalfOpenProbes is the number of trial requests allowed while half open. Default 3.
	HalfOpenProbes int `yaml:"half_open_probes" json:"half_open_probes"`
	// SuccessThreshold is the successful probes needed to close the breaker. Default 2.
	SuccessThreshold int `yaml:"success_threshold" json:"success_threshold"`
}

// Auth declares the authentication providers available to routes.
type Auth struct {
	// JWT lists JSON Web Token providers.
	JWT []JWTProvider `yaml:"jwt" json:"jwt"`
	// APIKeys lists API key providers.
	APIKeys []APIKeyProvider `yaml:"api_keys" json:"api_keys"`
}

// JWTProvider verifies bearer tokens. Fields left at their zero value take the defaults noted on each field.
type JWTProvider struct {
	// Name identifies the provider; names are unique across all providers.
	Name string `yaml:"name" json:"name"`
	// Issuer is the required token issuer.
	Issuer string `yaml:"issuer" json:"issuer"`
	// Audiences lists accepted token audiences; at least one is required.
	Audiences []string `yaml:"audiences" json:"audiences"`
	// Leeway tolerates clock skew when checking token times. Default 30s.
	Leeway Duration `yaml:"leeway" json:"leeway"`
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
}

// JWTKey is one verification key. Exactly one of Secret, PublicKey or PublicKeyFile must be set.
type JWTKey struct {
	// KID is the key identifier matched against the token header; required when there is more than one key.
	KID string `yaml:"kid" json:"kid"`
	// Algorithm is the signing algorithm the key verifies.
	Algorithm string `yaml:"algorithm" json:"algorithm"`
	// Secret is the shared secret for HS algorithms.
	Secret string `yaml:"secret" json:"secret"`
	// PublicKey is a PEM encoded public key for asymmetric algorithms.
	PublicKey string `yaml:"public_key" json:"public_key"`
	// PublicKeyFile is a path to a PEM encoded public key.
	PublicKeyFile string `yaml:"public_key_file" json:"public_key_file"`
}

// JWKS describes a remote JSON Web Key Set. Fields left at their zero value take the defaults noted on each field.
type JWKS struct {
	// URL is the http or https location of the key set.
	URL string `yaml:"url" json:"url"`
	// TTL is how long fetched keys are used before refresh. Default 5m.
	TTL Duration `yaml:"ttl" json:"ttl"`
	// MinRefreshInterval is the minimum time between refreshes. Default 30s.
	MinRefreshInterval Duration `yaml:"min_refresh_interval" json:"min_refresh_interval"`
	// Timeout bounds a key set fetch. Default 5s.
	Timeout Duration `yaml:"timeout" json:"timeout"`
}

// APIKeyProvider authenticates requests by API key. Fields left at their zero value take the defaults noted on each field.
type APIKeyProvider struct {
	// Name identifies the provider; names are unique across all providers.
	Name string `yaml:"name" json:"name"`
	// Header is the header carrying the key. Default "X-API-Key".
	Header string `yaml:"header" json:"header"`
	// Query is the query parameter carrying the key as an alternative to Header.
	Query string `yaml:"query" json:"query"`
	// KeyFile is a path to a file whose keys are added to Keys.
	KeyFile string `yaml:"key_file" json:"key_file"`
	// Keys lists inline keys.
	Keys []APIKey `yaml:"keys" json:"keys"`
	// ConsumerHeader is the header that receives the consumer name. Default "X-Consumer-ID".
	ConsumerHeader string `yaml:"consumer_header" json:"consumer_header"`
	// MetadataHeaders maps metadata field names to the headers that receive their values.
	MetadataHeaders map[string]string `yaml:"metadata_headers" json:"metadata_headers"`
}

// APIKey is one accepted API key, stored as a hash.
type APIKey struct {
	// ID identifies the key and must be unique within its provider.
	ID string `yaml:"id" json:"id"`
	// Hash is the key digest in the form sha256:<64 hex digits>.
	Hash string `yaml:"hash" json:"hash"`
	// Consumer names the client the key belongs to.
	Consumer string `yaml:"consumer" json:"consumer"`
	// Metadata holds extra values forwarded with the consumer.
	Metadata map[string]string `yaml:"metadata" json:"metadata"`
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
	// Params carries implementation specific settings.
	Params map[string]any `yaml:"params" json:"params"`
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
	// CircuitBreaker enables the upstream circuit breaker for this route when set.
	CircuitBreaker *RouteBreaker `yaml:"circuit_breaker" json:"circuit_breaker"`
	// Retry configures retries of failed upstream exchanges when set.
	Retry *Retry `yaml:"retry" json:"retry"`
	// Middlewares lists top-level middleware names applied to the route.
	Middlewares []string `yaml:"middlewares" json:"middlewares"`
	// Pipeline orders the built-in stages and middlewares for the route; retry must be last when present.
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
	// Headers lists header values the request must carry.
	Headers map[string]string `yaml:"headers" json:"headers"`
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

// HeaderOps edits headers. Add and Set map header names to values; Remove lists header names.
type HeaderOps struct {
	// Add appends values to the named headers.
	Add map[string]string `yaml:"add" json:"add"`
	// Set replaces the named headers.
	Set map[string]string `yaml:"set" json:"set"`
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
	// Claims lists claim values the identity must carry.
	Claims map[string]string `yaml:"claims" json:"claims"`
}

// RateLimit configures request rate limiting. Fields left at their zero value take the defaults noted on each field.
type RateLimit struct {
	// Algorithm is token_bucket or sliding_window. Default "token_bucket".
	Algorithm string `yaml:"algorithm" json:"algorithm"`
	// Rate is the token refill rate per Per, used by token_bucket.
	Rate float64 `yaml:"rate" json:"rate"`
	// Per is the period Rate is measured over. Default 1s.
	Per Duration `yaml:"per" json:"per"`
	// Burst is the bucket size for token_bucket. Default max(1, ceil(rate)) for token_bucket.
	Burst int `yaml:"burst" json:"burst"`
	// Limit is the request count per Window, used by sliding_window.
	Limit int `yaml:"limit" json:"limit"`
	// Window is the period Limit applies to, used by sliding_window. Default 1m.
	Window Duration `yaml:"window" json:"window"`
	// Key selects what limits are applied per.
	Key RateLimitKey `yaml:"key" json:"key"`
	// MaxKeys bounds how many distinct keys are tracked. Default 100000.
	MaxKeys int `yaml:"max_keys" json:"max_keys"`
	// IdleTTL is how long an unused key is kept. Default 10m.
	IdleTTL Duration `yaml:"idle_ttl" json:"idle_ttl"`
}

// RateLimitKey selects the attribute limits are applied per. Fields left at their zero value take the defaults noted on each field.
type RateLimitKey struct {
	// Source is one of ip, consumer, header, route. Default "ip".
	Source string `yaml:"source" json:"source"`
	// Name is the header name, required when Source is header.
	Name string `yaml:"name" json:"name"`
}

// RouteBreaker enables the circuit breaker for a route. Fields left at their zero value take the defaults noted on each field.
type RouteBreaker struct {
	// Enabled turns the breaker on. Default true when nil.
	Enabled *bool `yaml:"enabled" json:"enabled"`
}

// Retry configures retries of failed exchanges. Fields left at their zero value take the defaults noted on each field.
type Retry struct {
	// Attempts is the total number of tries, between 1 and 10. Default 3.
	Attempts int `yaml:"attempts" json:"attempts"`
	// BaseDelay is the first backoff delay. Default 25ms.
	BaseDelay Duration `yaml:"base_delay" json:"base_delay"`
	// MaxDelay caps the backoff delay. Default 250ms.
	MaxDelay Duration `yaml:"max_delay" json:"max_delay"`
	// OnStatuses lists response statuses that trigger a retry. Default [502, 503, 504], applied only when nil.
	OnStatuses []int `yaml:"on_statuses" json:"on_statuses"`
	// Methods lists methods that may be retried. Default [GET, HEAD, OPTIONS, PUT, DELETE], applied only when nil.
	Methods []string `yaml:"methods" json:"methods"`
	// IdempotencyKey controls the idempotency key header sent with retries.
	IdempotencyKey IdempotencyKey `yaml:"idempotency_key" json:"idempotency_key"`
	// Budget limits how many retries may be issued relative to traffic.
	Budget RetryBudget `yaml:"budget" json:"budget"`
	// MaxBodyBytes is the largest request body that is buffered for retry. Default 65536.
	MaxBodyBytes int64 `yaml:"max_body_bytes" json:"max_body_bytes"`
}

// IdempotencyKey controls the idempotency key header. Fields left at their zero value take the defaults noted on each field.
type IdempotencyKey struct {
	// Enabled sends an idempotency key header with retried requests.
	Enabled bool `yaml:"enabled" json:"enabled"`
	// Header is the header name for the key. Default "Idempotency-Key".
	Header string `yaml:"header" json:"header"`
}

// RetryBudget bounds retries as a share of traffic. Fields left at their zero value take the defaults noted on each field.
type RetryBudget struct {
	// Ratio is the retry share of requests, in [0, 1]. Default 0.1.
	Ratio float64 `yaml:"ratio" json:"ratio"`
	// MinPerSecond is the retry rate always allowed regardless of traffic. Default 10.
	MinPerSecond float64 `yaml:"min_per_second" json:"min_per_second"`
	// Window is the period the budget is measured over. Default 10s.
	Window Duration `yaml:"window" json:"window"`
}

// BuiltinStages lists the stage names a route pipeline may use without a middleware definition.
var BuiltinStages = []string{"timeout", "auth", "rate_limit", "circuit_breaker", "retry"}
