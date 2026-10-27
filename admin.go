package lychgate

import (
	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/metrics"
	"github.com/ok0/lychgate/internal/upstream"
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
	snap := s.g.snap.Load()
	out := make([]upstream.Status, 0, len(snap.Config.Upstreams))
	for _, u := range snap.Config.Upstreams {
		out = append(out, upstream.Status{Name: u.Name, Hosts: snap.Upstreams[u.Name].Snapshot()})
	}
	return out
}

func (s adminSource) Metrics() *metrics.Registry {
	return s.g.metrics
}
