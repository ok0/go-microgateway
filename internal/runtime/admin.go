package runtime

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/ok0/go-microgateway/config"
)

// decodeKeyHash returns the sha256 digest named by s, which has the form sha256:<64 hex digits>. It reports false
// when s has another form.
func decodeKeyHash(s string) ([32]byte, bool) {
	var d [32]byte
	b, err := hex.DecodeString(strings.ToLower(strings.TrimPrefix(s, "sha256:")))
	if err != nil || len(b) != len(d) {
		return d, false
	}
	copy(d[:], b)
	return d, true
}

// adminKeys returns the ids and sha256 digests of the admin keys of cfg, index-aligned. It returns nil slices when
// cfg has no admin keys. An invalid hash is reported by its index only.
func adminKeys(cfg *config.Config) ([]string, [][32]byte, error) {
	keys := cfg.Admin.Auth.Keys
	if len(keys) == 0 {
		return nil, nil, nil
	}
	ids := make([]string, 0, len(keys))
	hashes := make([][32]byte, 0, len(keys))
	for i := range keys {
		d, ok := decodeKeyHash(keys[i].Hash.Reveal())
		if !ok {
			return nil, nil, fmt.Errorf("admin.auth.keys[%d]: invalid hash", i)
		}
		ids = append(ids, keys[i].ID)
		hashes = append(hashes, d)
	}
	return ids, hashes, nil
}
