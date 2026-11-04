package config

import (
	"fmt"
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
