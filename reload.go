package lychgate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/metrics"
	"github.com/ok0/lychgate/internal/reload"
	"github.com/ok0/lychgate/internal/runtime"
)

type reloadSeries struct {
	success, failure, unchanged *metrics.Counter
	lastSuccess                 *metrics.Gauge
}

func registerReloadSeries(r *metrics.Registry) (*reloadSeries, error) {
	c, err := r.Counter("lychgate_config_reloads_total", "Configuration reload attempts, by result.", "result")
	if err != nil {
		return nil, err
	}
	g, err := r.Gauge("lychgate_config_last_reload_success_timestamp_seconds", "Unix time of the last successful configuration load, initial or reload.")
	if err != nil {
		return nil, err
	}
	return &reloadSeries{
		success:     c.With("success"),
		failure:     c.With("failure"),
		unchanged:   c.With("unchanged"),
		lastSuccess: g.With(),
	}, nil
}

// Reload makes cfg the active configuration when it is valid and differs from the active one. cfg is copied, so later
// changes to it have no effect. On failure the previous configuration remains active and the error is returned.
func (g *Gateway) Reload(cfg *config.Config) error {
	if cfg == nil {
		return errors.New("lychgate: nil config")
	}
	return g.reload("api", func() (*config.Config, error) {
		c := cfg.Clone()
		if err := c.Validate(); err != nil {
			return nil, err
		}
		return c, nil
	})
}

// ReloadYAML parses data as a configuration document and makes it the active configuration when it is valid and
// differs from the active one. On failure the previous configuration remains active and the error is returned.
func (g *Gateway) ReloadYAML(data []byte) error {
	return g.reload("yaml", func() (*config.Config, error) {
		return config.Parse(data, g.parseOptions()...)
	})
}

// ReloadFile reads the configuration document at path and makes it the active configuration when it is valid and
// differs from the active one. On failure the previous configuration remains active and the error is returned.
func (g *Gateway) ReloadFile(path string) error {
	return g.reload("file", func() (*config.Config, error) {
		return config.LoadFile(path, g.parseOptions()...)
	})
}

// WatchFile reloads the configuration from path whenever its content changes, using the reload settings of the active
// configuration. It blocks until ctx is done and then returns nil. It returns an error at once when path cannot be
// stat'ed, and ErrClosed when the gateway is closed while watching.
func (g *Gateway) WatchFile(ctx context.Context, path string) error {
	g.mu.Lock()
	closed := g.closed
	g.mu.Unlock()
	if closed {
		return ErrClosed
	}
	rc := g.Config().Global.Reload
	return reload.New(reload.Options{
		Paths:        func() []string { return []string{path} },
		Debounce:     rc.Debounce.Std(),
		PollInterval: rc.PollInterval.Std(),
		Clock:        g.clk,
		Logger:       g.logger,
		Apply:        func() error { return g.ReloadFile(path) },
		Fatal:        func(err error) bool { return errors.Is(err, ErrClosed) },
	}).Run(ctx)
}

func (g *Gateway) reload(source string, load func() (*config.Config, error)) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return ErrClosed
	}
	start := g.clk.Now()
	cfg, err := load()
	if err != nil {
		return g.failed(source, start, err)
	}
	digest, applies, err := g.prepare(cfg)
	if err != nil {
		return g.failed(source, start, err)
	}
	old := g.cur.Load()
	fp := combine(cfg.Fingerprint(), digest)
	if fp == old.fingerprint {
		g.reloads.unchanged.Inc()
		g.logger.Debug("configuration unchanged", "source", source, "fingerprint", short(fp))
		return nil
	}
	gen, err := g.build(cfg, fp)
	if err != nil {
		return g.failed(source, start, err)
	}
	g.install(gen, applies)
	if err := old.retire(); err != nil {
		g.logger.Warn("releasing retired configuration failed", "error", err)
	}
	g.reloads.success.Inc()
	g.reloads.lastSuccess.Set(float64(g.clk.Now().Unix()))
	rd := diffNamed(old.snap.Config.Routes, cfg.Routes, routeName)
	ud := diffNamed(old.snap.Config.Upstreams, cfg.Upstreams, upstreamName)
	g.logger.Info("configuration reloaded",
		"source", source,
		"old_fingerprint", short(old.fingerprint),
		"new_fingerprint", short(fp),
		"routes_added", rd.added,
		"routes_removed", rd.removed,
		"routes_changed", rd.changed,
		"upstreams_added", ud.added,
		"upstreams_removed", ud.removed,
		"upstreams_changed", ud.changed,
		"duration_ms", g.clk.Since(start).Milliseconds(),
	)
	return nil
}

func (g *Gateway) failed(source string, start time.Time, err error) error {
	g.reloads.failure.Inc()
	attrs := []any{"source", source}
	var ve *config.ValidationError
	if errors.As(err, &ve) {
		msgs := make([]string, 0, len(ve.Errors))
		for _, fe := range ve.Errors {
			msgs = append(msgs, fe.String())
		}
		attrs = append(attrs, "errors", msgs)
	} else {
		attrs = append(attrs, "error", err.Error())
	}
	attrs = append(attrs, "duration_ms", g.clk.Since(start).Milliseconds())
	g.logger.Warn("configuration reload failed; previous configuration remains active", attrs...)
	return err
}

func (g *Gateway) prepare(cfg *config.Config) (string, []func(), error) {
	if len(g.parts) == 0 {
		return "", nil, nil
	}
	h := sha256.New()
	applies := make([]func(), 0, len(g.parts))
	for _, p := range g.parts {
		digest, apply, err := p.Prepare(cfg)
		if err != nil {
			return "", nil, err
		}
		h.Write([]byte(digest + "\n"))
		applies = append(applies, apply)
	}
	return hex.EncodeToString(h.Sum(nil)), applies, nil
}

func combine(cfgFP, digest string) string {
	if digest == "" {
		return cfgFP
	}
	sum := sha256.Sum256([]byte(cfgFP + "\n" + digest))
	return hex.EncodeToString(sum[:])
}

func (g *Gateway) build(cfg *config.Config, fp string) (*generation, error) {
	tx := g.state.Begin()
	snap, err := runtime.Build(cfg, g.deps, tx)
	if err != nil {
		tx.Abort()
		return nil, err
	}
	lease := tx.Commit()
	return &generation{
		snap:        snap,
		fingerprint: fp,
		release: func() error {
			err := lease.Release()
			g.live.Add(-1)
			return err
		},
		logger: g.logger,
	}, nil
}

func (g *Gateway) install(gen *generation, applies []func()) {
	g.live.Add(1)
	gen.snap.Activate()
	g.cur.Store(gen)
	for _, apply := range applies {
		apply()
	}
}

func (g *Gateway) parseOptions() []config.ParseOption {
	if g.lookup != nil {
		return []config.ParseOption{config.WithEnvLookup(g.lookup)}
	}
	return nil
}

func short(fp string) string {
	if len(fp) > 12 {
		return fp[:12]
	}
	return fp
}

type nameDiff struct{ added, removed, changed int }

func diffNamed[T any](old, cur []T, name func(T) string) nameDiff {
	oldByName := make(map[string]T, len(old))
	for _, v := range old {
		oldByName[name(v)] = v
	}
	var d nameDiff
	seen := make(map[string]bool, len(cur))
	for _, v := range cur {
		n := name(v)
		seen[n] = true
		o, ok := oldByName[n]
		if !ok {
			d.added++
			continue
		}
		oj, errO := json.Marshal(o)
		cj, errC := json.Marshal(v)
		if errO != nil || errC != nil || string(oj) != string(cj) {
			d.changed++
		}
	}
	for n := range oldByName {
		if !seen[n] {
			d.removed++
		}
	}
	return d
}

func routeName(r config.Route) string {
	return r.Name
}

func upstreamName(u config.Upstream) string {
	return u.Name
}
