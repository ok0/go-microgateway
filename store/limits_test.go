package store_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/store"
)

func atLeast(have, need int64) bool { return have >= need }

func ceilingDoc(dur, body string) string {
	doc := `version: 1
listeners:
  - name: l
    address: "127.0.0.1:0"
upstreams:
  - name: up
    hosts:
      - url: "http://127.0.0.1:1"
routes:
  - name: r
    match:
      path: /x
    upstream: up
    cache:
      enabled: true
      max_ttl: ` + dur + `
      default_ttl: ` + dur + `
      keep: ` + dur + `
      stale_while_revalidate: ` + dur + `
      stale_if_error: ` + dur + `
`
	if body != "" {
		doc += `cache:
  max_body_bytes: ` + body + `
  memory:
    max_bytes: 1073741824
    shards: 16
`
	}
	return doc
}

func mustParse(t *testing.T, doc string) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(doc))
	if err != nil {
		t.Fatalf("config.Parse() error = %v", err)
	}
	return cfg
}

func TestLimitsCoverProducers(t *testing.T) {
	t.Parallel()
	h := make(http.Header)
	h.Set("Cache-Control", "stale-while-revalidate=999999999999, stale-if-error=999999999999")
	sw, se := httpcache.StaleWindows(h, httpcache.Options{})
	if sw != httpcache.MaxWindow || se != httpcache.MaxWindow {
		t.Fatalf("StaleWindows() = %v, %v; want both %v", sw, se, httpcache.MaxWindow)
	}
	rulesTTL := httpcache.StorageTTL(httpcache.MaxWindow, sw, se, httpcache.MaxWindow, true)
	cfg := mustParse(t, ceilingDoc("8760h", ""))
	configTTL := (*config.Cache)(nil).MaxStorageTTL(cfg.Routes)
	mustParse(t, ceilingDoc("8760h", "67108864"))
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	sec, ok := httpcache.SecondaryKey([]string{"accept-encoding"}, req)
	if !ok {
		t.Fatal("SecondaryKey() ok = false, want true")
	}
	tests := []struct {
		name       string
		have, need int64
	}{
		{name: "vary names", have: store.MaxVaryNames, need: httpcache.MaxVaryNames},
		{name: "tags", have: store.MaxTags, need: httpcache.MaxTags},
		{name: "tag bytes", have: store.MaxTagBytes, need: httpcache.MaxTagBytes},
		{name: "initial age", have: int64(store.MaxInitialAge), need: int64(httpcache.MaxDelta)},
		{name: "storage ttl (rules)", have: int64(store.MaxTTL), need: int64(rulesTTL)},
		{name: "storage ttl (config)", have: int64(store.MaxTTL), need: int64(configTTL)},
		{name: "body bytes", have: store.MaxBodyBytes, need: 67108864},
		{name: "secondary key", have: store.MaxSecondaryBytes, need: int64(len(sec))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if !atLeast(tt.have, tt.need) {
				t.Errorf("store limit %d does not cover producer value %d", tt.have, tt.need)
			}
			if atLeast(tt.need-1, tt.need) {
				t.Errorf("atLeast(%d, %d) = true, want false", tt.need-1, tt.need)
			}
		})
	}
}

func TestConfigCeilings(t *testing.T) {
	t.Parallel()
	if _, err := config.Parse([]byte(ceilingDoc("8760h0m0.000000001s", ""))); err == nil {
		t.Error("config.Parse() with storage ttl above 3*365 days succeeded, want error")
	}
	if _, err := config.Parse([]byte(ceilingDoc("8760h", "67108865"))); err == nil {
		t.Error("config.Parse() with max_body_bytes above 64 MiB succeeded, want error")
	}
}
