package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"net"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

var storeNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

func (s *Store) applyDefaults() {
	defIntPtr(&s.Guard.ConsecutiveFailures, 3)
	defDurPtr(&s.Guard.OpenTimeout, 2*time.Second)
	defIntPtr(&s.Guard.MaxInflight, 1024)
	if r := s.Redis; r != nil {
		defStr(&r.Mode, "standalone")
		defDurPtr(&r.DialTimeout, time.Second)
		defDurPtr(&r.ReadTimeout, 500*time.Millisecond)
		defDurPtr(&r.WriteTimeout, 500*time.Millisecond)
		defStrPtr(&r.KeyPrefix, "microgateway")
	}
}

// checkStores validates every store and returns the set of store names. Messages never include addresses,
// passwords or parameter values.
func checkStores(ck *checker, stores []Store) map[string]bool {
	names := map[string]bool{}
	for i := range stores {
		s := &stores[i]
		p := fmt.Sprintf("stores[%d]", i)
		checkName(ck, p+".name", s.Name, names)
		if s.Name != "" && !storeNamePattern.MatchString(s.Name) {
			ck.add(p+".name", "must match ^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
		}
		if s.Type == "" {
			ck.add(p+".type", "is required")
		}
		switch {
		case s.Type == "redis" && s.Redis == nil:
			ck.add(p+".redis", "is required for type redis")
		case s.Type != "" && s.Type != "redis" && s.Redis != nil:
			ck.add(p+".redis", "is only valid with type redis")
		}
		if s.Type == "redis" && !s.Params.IsZero() {
			ck.add(p+".params", "is not valid for type redis")
		}
		if s.Redis != nil {
			checkRedis(ck, p+".redis", s.Redis)
		}
		checkStoreGuard(ck, p+".guard", &s.Guard)
	}
	return names
}

func checkStoreGuard(ck *checker, p string, g *StoreGuard) {
	if *g.ConsecutiveFailures < 1 {
		ck.add(p+".consecutive_failures", "must be >= 1")
	}
	requirePositive(ck, p+".open_timeout", *g.OpenTimeout)
	if *g.MaxInflight < 1 {
		ck.add(p+".max_inflight", "must be >= 1")
	}
}

func checkRedis(ck *checker, p string, r *RedisStore) {
	if !slices.Contains([]string{"standalone", "sentinel", "cluster"}, r.Mode) {
		ck.add(p+".mode", "must be one of standalone, sentinel, cluster")
	}
	switch {
	case len(r.Addresses) == 0:
		ck.add(p+".addresses", "at least one address is required")
	case r.Mode == "standalone" && len(r.Addresses) > 1:
		ck.add(p+".addresses", "standalone mode takes exactly one address")
	}
	seen := map[string]bool{}
	for k, addr := range r.Addresses {
		checkRedisAddress(ck, fmt.Sprintf("%s.addresses[%d]", p, k), addr, seen)
	}

	switch {
	case r.Mode == "sentinel" && r.MasterName == "":
		ck.add(p+".master_name", "is required in sentinel mode")
	case r.Mode != "sentinel" && r.MasterName != "":
		ck.add(p+".master_name", "is only valid in sentinel mode")
	}
	if r.Mode != "sentinel" && r.SentinelUsername != "" {
		ck.add(p+".sentinel_username", "is only valid in sentinel mode")
	}
	if r.Mode != "sentinel" && !r.SentinelPassword.IsZero() {
		ck.add(p+".sentinel_password", "is only valid in sentinel mode")
	}

	if r.DB < 0 {
		ck.add(p+".db", "must be >= 0")
	}
	if r.Mode == "cluster" && r.DB != 0 {
		ck.add(p+".db", "must be 0 in cluster mode")
	}

	if t := r.TLS; t != nil {
		if (t.CertFile == "") != (t.KeyFile == "") {
			ck.add(p+".tls", "cert_file and key_file must be set together")
		}
		if !t.Enabled {
			for _, f := range []struct {
				name string
				set  bool
			}{
				{"ca_file", t.CAFile != ""},
				{"cert_file", t.CertFile != ""},
				{"key_file", t.KeyFile != ""},
				{"server_name", t.ServerName != ""},
				{"insecure_skip_verify", t.InsecureSkipVerify},
			} {
				if f.set {
					ck.add(p+".tls."+f.name, "is only valid when tls.enabled is true")
				}
			}
		}
	}

	requirePositive(ck, p+".dial_timeout", *r.DialTimeout)
	requirePositive(ck, p+".read_timeout", *r.ReadTimeout)
	requirePositive(ck, p+".write_timeout", *r.WriteTimeout)

	if r.PoolSize < 0 {
		ck.add(p+".pool_size", "must be >= 0")
	}
	if r.MinIdleConns < 0 {
		ck.add(p+".min_idle_conns", "must be >= 0")
	}
	if r.PoolSize > 0 && r.MinIdleConns > r.PoolSize {
		ck.add(p+".min_idle_conns", "must not exceed pool_size")
	}

	switch {
	case *r.KeyPrefix == "":
		ck.add(p+".key_prefix", "must not be empty")
	case strings.ContainsAny(*r.KeyPrefix, "{}"):
		ck.add(p+".key_prefix", "must not contain { or }")
	}
}

// checkRedisAddress reports a problem with one host:port address. Messages never echo the address.
func checkRedisAddress(ck *checker, p, addr string, seen map[string]bool) {
	switch {
	case strings.Contains(addr, "://"):
		ck.add(p, "must be host:port without a scheme")
		return
	case strings.Contains(addr, "@"):
		ck.add(p, "must not contain userinfo")
		return
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		ck.add(p, "must be host:port")
		return
	}
	if host == "" {
		ck.add(p, "host is required")
		return
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n < 1 {
		ck.add(p, "port must be between 1 and 65535")
		return
	}
	if seen[addr] {
		ck.add(p, "duplicate address")
		return
	}
	seen[addr] = true
}

// BackendFingerprint returns the lowercase hex SHA-256 that identifies the backend a store connects to. It covers
// the type, the Redis block including both passwords, and the parameters; it excludes Name and Guard. Secret
// values enter the digest, and no secret value is written in the clear.
func (s *Store) BackendFingerprint() string {
	h := sha256.New()
	writeFingerprintPart(h, s.Type)
	writeFingerprintPart(h, s.Redis)
	if s.Redis != nil {
		writeLengthPrefixed(h, s.Redis.Password.Reveal())
		writeLengthPrefixed(h, s.Redis.SentinelPassword.Reveal())
	}
	writeFingerprintPart(h, s.Params.Reveal())
	return hex.EncodeToString(h.Sum(nil))
}

// hash.Hash writes never fail, so their errors are ignored.
func writeFingerprintPart(h hash.Hash, v any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		_, _ = fmt.Fprintf(h, "unencodable %T\n", v)
		return
	}
	h.Write(buf.Bytes())
}

// hash.Hash writes never fail, so their errors are ignored.
func writeLengthPrefixed(h hash.Hash, s string) {
	_, _ = fmt.Fprintf(h, "%d:%s;", len(s), s)
}

// Clone returns a deep copy of r that shares no mutable data with r. It returns nil for nil.
func (r *RedisStore) Clone() *RedisStore {
	if r == nil {
		return nil
	}
	return deepCopy(reflect.ValueOf(r)).Interface().(*RedisStore)
}
