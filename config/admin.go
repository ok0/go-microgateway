package config

import (
	"fmt"
	"net"
	"strings"
)

// checkAdminAuth reports empty or duplicate key ids and malformed or duplicate key hashes. Messages never contain a hash.
func checkAdminAuth(ck *checker, a *AdminAuth) {
	ids := map[string]bool{}
	hashes := map[string]bool{}
	for k := range a.Keys {
		key := &a.Keys[k]
		kp := fmt.Sprintf("admin.auth.keys[%d]", k)
		switch {
		case key.ID == "":
			ck.add(kp+".id", "is required")
		case ids[key.ID]:
			ck.add(kp+".id", fmt.Sprintf("duplicate id %q", key.ID))
		default:
			ids[key.ID] = true
		}
		if !apiKeyHash.MatchString(key.Hash.Reveal()) {
			ck.add(kp+".hash", "must have the form sha256:<64 hex digits>")
		} else {
			h := strings.ToLower(key.Hash.Reveal())
			if hashes[h] {
				ck.add(kp+".hash", "duplicate hash")
			}
			hashes[h] = true
		}
	}
}

// IsLoopbackAddress reports whether addr is a host:port address, with a port from 0 to 65535, whose host is localhost or a loopback IP literal (127.0.0.0/8 or ::1).
// An empty host, an unspecified address such as 0.0.0.0 or ::, every other name, and a string that is not such an address are not loopback.
func IsLoopbackAddress(addr string) bool {
	if !validAddress(addr) {
		return false
	}
	host, _, _ := net.SplitHostPort(addr)
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// ManagementAuthRequired reports whether /config, /upstreams and /metrics need a bearer token from a.Auth.Keys:
// always for "required", never for "none", and for "auto" (or unset) when a.Address is not a loopback address. Any other mode requires one.
func (a *Admin) ManagementAuthRequired() bool {
	switch a.Auth.Management {
	case "none":
		return false
	case "auto", "":
		return !IsLoopbackAddress(a.Address)
	default:
		return true
	}
}

// checkAdminExposure reports an invalid probe address, an unknown management mode, and management endpoints that would need a key when none is configured. Messages never contain a hash.
func checkAdminExposure(ck *checker, a *Admin) {
	if a.Probes.Enabled != nil && *a.Probes.Enabled && !validAddress(a.Probes.Address) {
		ck.add("admin.probes.address", fmt.Sprintf("invalid address %q", a.Probes.Address))
	}
	switch a.Auth.Management {
	case "auto", "required", "none":
	default:
		ck.add("admin.auth.management", "must be one of auto, required, none")
		return
	}
	if a.Enabled == nil || !*a.Enabled || len(a.Auth.Keys) > 0 {
		return
	}
	if a.Auth.Management == "required" {
		ck.add("admin.auth.keys", "at least one key is needed when admin.auth.management is required")
	} else if a.Auth.Management == "auto" && validAddress(a.Address) && !IsLoopbackAddress(a.Address) {
		ck.add("admin.auth.keys", fmt.Sprintf("management endpoints on non-loopback address %q need at least one key, or set admin.auth.management: none", a.Address))
	}
}
