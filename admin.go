package microgateway

import (
	"context"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/admin"
	"github.com/ok0/go-microgateway/internal/cache"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/upstream"
)

type adminSource struct {
	g *Gateway
}

func (s adminSource) Ready() bool {
	return s.g.Ready()
}

func (s adminSource) RedactedConfig() *config.Config {
	return s.g.Config().Redacted()
}

func (s adminSource) Upstreams() []upstream.Status {
	snap := s.g.cur.Load().snap
	out := make([]upstream.Status, 0, len(snap.Config.Upstreams))
	for _, u := range snap.Config.Upstreams {
		out = append(out, snap.Upstreams[u.Name].Status())
	}
	return out
}

func (s adminSource) Metrics() *metrics.Registry {
	s.g.seriesMu.Lock()
	defer s.g.seriesMu.Unlock()
	snap := s.g.cur.Load().snap
	for _, u := range snap.Config.Upstreams {
		snap.Upstreams[u.Name].RefreshHealth()
	}
	snap.RefreshRateLimitKeys()
	snap.RefreshCacheGauges()
	return s.g.metrics
}

func (s adminSource) ManagementAuth() (bool, []string, [][32]byte) {
	snap := s.g.cur.Load().snap
	a := snap.Config.Admin
	a.Address = s.g.adminAddr
	ids, hashes := snap.AdminKeys()
	return a.ManagementAuthRequired(), ids, hashes
}

func (s adminSource) ProbeMetrics() bool {
	m := s.g.cur.Load().snap.Config.Admin.Probes.Metrics
	return m != nil && *m
}

// warnOpenManagement logs a warning when cfg leaves the management endpoints unauthenticated on a non-loopback address.
func (g *Gateway) warnOpenManagement(cfg *config.Config) {
	a := cfg.Admin
	if a.Enabled == nil || !*a.Enabled || a.Auth.Management != "none" {
		return
	}
	addr := g.adminAddr
	if config.IsLoopbackAddress(addr) {
		addr = a.Address
	}
	if config.IsLoopbackAddress(addr) {
		return
	}
	g.logger.Warn("admin management endpoints are unauthenticated on a non-loopback address", "field", "admin.auth.management", "address", addr)
}

func (s adminSource) PurgeAccess() admin.PurgeAccess {
	snap := s.g.cur.Load().snap
	ids, hashes := snap.AdminKeys()
	return admin.PurgeAccess{KeyIDs: ids, Hashes: hashes, Cache: snap.Purger() != nil}
}

func (s adminSource) Purge(ctx context.Context, req cache.PurgeRequest, keyID string) (cache.PurgeResult, error) {
	return s.g.purge(ctx, req, keyID)
}
