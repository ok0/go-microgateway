package upstream

import (
	"context"
	"log/slog"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/clock"
)

const maxDNSHosts = 256

const maxDNSLookup = 5 * time.Second

type Resolver interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

type dnsResult struct {
	seq  uint64
	urls []string
}

type dnsWatcher struct {
	name    string
	cfg     config.DNS
	res     Resolver
	clk     clock.Clock
	logger  *slog.Logger
	metrics *Metrics

	mu   sync.Mutex
	cur  dnsResult
	fn   func(dnsResult)
	stop context.CancelFunc
	done chan struct{}
}

func newDNSWatcher(name string, cfg config.DNS, d Deps) *dnsWatcher {
	ctx, stop := context.WithCancel(context.Background())
	w := &dnsWatcher{
		name:    name,
		cfg:     cfg,
		res:     d.Resolver,
		clk:     d.Clock,
		logger:  d.Logger,
		metrics: d.Metrics,
		stop:    stop,
		done:    make(chan struct{}),
	}
	w.refresh(ctx)
	go w.run(ctx)
	return w
}

func (w *dnsWatcher) run(ctx context.Context) {
	defer close(w.done)
	for {
		wake := make(chan struct{})
		t := w.clk.AfterFunc(w.cfg.RefreshInterval.Std(), func() { close(wake) })
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-wake:
		}
		w.refresh(ctx)
	}
}

func (w *dnsWatcher) lookup(parent context.Context) ([]string, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	t := w.clk.AfterFunc(min(w.cfg.RefreshInterval.Std(), maxDNSLookup), cancel)
	defer t.Stop()
	return w.res.LookupHost(ctx, w.cfg.Hostname)
}

func (w *dnsWatcher) refresh(ctx context.Context) {
	addrs, err := w.lookup(ctx)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		w.metrics.DNSRefresh.With(w.name, "error").Inc()
		w.logger.Warn("upstream dns lookup failed", "upstream", w.name, "hostname", w.cfg.Hostname, "error", err)
		return
	}
	urls := w.parse(addrs)
	if len(urls) == 0 {
		w.metrics.DNSRefresh.With(w.name, "empty").Inc()
		w.logger.Warn("upstream dns lookup returned no addresses", "upstream", w.name, "hostname", w.cfg.Hostname)
		return
	}
	w.metrics.DNSRefresh.With(w.name, "success").Inc()

	w.mu.Lock()
	if slices.Equal(urls, w.cur.urls) {
		w.mu.Unlock()
		return
	}
	w.cur = dnsResult{seq: w.cur.seq + 1, urls: urls}
	res, fn := w.cur, w.fn
	w.mu.Unlock()
	if fn != nil {
		fn(res)
	}
}

func (w *dnsWatcher) parse(answers []string) []string {
	seen := make(map[netip.Addr]struct{}, len(answers))
	ips := make([]netip.Addr, 0, len(answers))
	for _, s := range answers {
		a, err := netip.ParseAddr(s)
		if err != nil {
			continue
		}
		if _, dup := seen[a]; dup {
			continue
		}
		seen[a] = struct{}{}
		ips = append(ips, a)
	}
	slices.SortFunc(ips, netip.Addr.Compare)
	if len(ips) > maxDNSHosts {
		w.logger.Warn("upstream dns answer truncated", "upstream", w.name, "hostname", w.cfg.Hostname, "addresses", len(ips), "limit", maxDNSHosts)
		ips = ips[:maxDNSHosts]
	}
	port := strconv.Itoa(w.cfg.Port)
	urls := make([]string, 0, len(ips))
	for _, a := range ips {
		urls = append(urls, w.cfg.Scheme+"://"+net.JoinHostPort(a.String(), port))
	}
	return urls
}

func (w *dnsWatcher) bind(fn func(dnsResult)) dnsResult {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.fn = fn
	return w.cur
}

func (w *dnsWatcher) Close() error {
	w.stop()
	<-w.done
	return nil
}
