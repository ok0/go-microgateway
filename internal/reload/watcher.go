// Package reload watches configuration files and runs a reload attempt when their content changes.
package reload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/ok0/go-microgateway/internal/clock"
)

// Options configures a Watcher.
type Options struct {
	Paths        func() []string             // files whose contents decide a reload; called at start and after every attempt; the first is the configuration file
	Debounce     time.Duration               // quiet period; must be > 0
	PollInterval time.Duration               // must be > 0
	Clock        clock.Clock                 // nil → clock.Real()
	Logger       *slog.Logger                // nil → discard
	Apply        func() error                // one reload attempt
	Fatal        func(error) bool            // optional; Run returns an Apply error for which it reports true
	newEvents    func() (eventSource, error) // test hook; nil → newFSEvents
}

// Watcher runs a reload attempt when the watched configuration files change. Changes are detected from file
// events and periodic polling, debounced, and applied one attempt at a time.
type Watcher struct {
	o       Options
	trigger chan struct{}
}

type fileStat struct {
	exists bool
	size   int64
	mtime  int64
	target string
}

type record struct {
	stat   fileStat
	digest string
}

// New returns a Watcher configured by o. Nil Clock, Logger and event source hooks take their defaults: the real
// clock, a logger that discards output, and fsnotify.
func New(o Options) *Watcher {
	if o.Clock == nil {
		o.Clock = clock.Real()
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.newEvents == nil {
		o.newEvents = newFSEvents
	}
	return &Watcher{o: o, trigger: make(chan struct{}, 1)}
}

// Trigger requests a reload attempt even if the watched content has not changed. Requests made while one is
// already pending are coalesced into it.
func (w *Watcher) Trigger() {
	select {
	case w.trigger <- struct{}{}:
	default:
	}
}

// Run watches the paths returned by Options.Paths and calls Options.Apply when their content changes. It makes one
// attempt at start, and returns an error without attempting if the first path does not exist. Run returns nil when
// ctx is done. It returns an Apply error only when Options.Fatal reports that error as fatal.
func (w *Watcher) Run(ctx context.Context) error {
	o := w.o
	clk := o.Clock

	paths := absAll(o.Paths())
	if _, err := os.Stat(paths[0]); err != nil {
		return fmt.Errorf("watch %s: %w", paths[0], err)
	}
	if ctx.Err() != nil {
		return nil
	}

	var src eventSource
	if es, err := o.newEvents(); err != nil {
		o.Logger.Warn("file events unavailable; polling only", "error", err, "poll_interval", o.PollInterval)
	} else {
		src = es
		defer src.Close()
	}
	var events <-chan fsnotify.Event
	var errs <-chan error
	if src != nil {
		events = src.Events()
		errs = src.Errors()
	}

	recs := map[string]record{}
	var names, pathDirs, watchDirs map[string]bool
	var pending bool
	var first, deadline time.Time
	var timer clock.Timer
	fire := make(chan struct{}, 1)

	resync := func(ps []string) {
		names = map[string]bool{}
		pathDirs = map[string]bool{}
		dirs := map[string]bool{}
		for _, p := range ps {
			names[p] = true
			pathDirs[filepath.Dir(p)] = true
			dirs[p] = true
			dirs[filepath.Dir(p)] = true
			t, err := filepath.EvalSymlinks(p)
			if err == nil && t != p {
				names[t] = true
				dirs[filepath.Dir(t)] = true
			}
		}
		watchDirs = dirs
		if src != nil {
			src.Watch(sortedKeys(dirs))
		}
	}

	refreshStats := func() {
		for _, p := range paths {
			r := recs[p]
			r.stat = statOf(p)
			recs[p] = r
		}
	}

	attempt := func() error {
		for _, p := range paths {
			recs[p] = snapshot(p)
		}
		err := o.Apply()
		newPaths := absAll(o.Paths())
		present := make(map[string]bool, len(newPaths))
		for _, p := range newPaths {
			present[p] = true
		}
		for p := range recs {
			if !present[p] {
				delete(recs, p)
			}
		}
		for _, p := range newPaths {
			if _, ok := recs[p]; !ok {
				recs[p] = snapshot(p)
			}
		}
		paths = newPaths
		resync(paths)
		return err
	}

	fatal := func(err error) bool {
		return err != nil && o.Fatal != nil && o.Fatal(err)
	}

	schedule := func() {
		now := clk.Now()
		if !pending {
			pending = true
			first = now
		}
		deadline = now.Add(o.Debounce)
		if maxWait := first.Add(10 * o.Debounce); maxWait.Before(deadline) {
			deadline = maxWait
		}
		d := deadline.Sub(now)
		if d <= 0 {
			if timer != nil {
				timer.Stop()
			}
			select {
			case fire <- struct{}{}:
			default:
			}
			return
		}
		if timer == nil {
			timer = clk.AfterFunc(d, func() {
				select {
				case fire <- struct{}{}:
				default:
				}
			})
		} else {
			timer.Reset(d)
		}
	}

	ticker := clk.NewTicker(o.PollInterval)
	defer ticker.Stop()
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	if ctx.Err() != nil {
		return nil
	}
	resync(paths)
	if err := attempt(); fatal(err) {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case ev := <-events:
			if relevant(ev, names, pathDirs) || entryChanged(ev, watchDirs) {
				resync(paths)
				schedule()
			}

		case err := <-errs:
			if errors.Is(err, fsnotify.ErrEventOverflow) {
				if contentChanged(paths, recs) {
					schedule()
				}
			} else {
				o.Logger.Debug("file event error", "error", err)
			}

		case <-ticker.C():
			resync(paths)
			statChanged := false
			for _, p := range paths {
				if statOf(p) != recs[p].stat {
					statChanged = true
					break
				}
			}
			if statChanged {
				if contentChanged(paths, recs) {
					schedule()
				} else {
					refreshStats()
				}
			}

		case <-fire:
			if ctx.Err() != nil {
				return nil
			}
			if !pending || clk.Now().Before(deadline) {
				continue
			}
			pending = false
			if contentChanged(paths, recs) {
				if err := attempt(); fatal(err) {
					return err
				}
			} else {
				refreshStats()
				o.Logger.Debug("watched files unchanged; reload skipped")
			}

		case <-w.trigger:
			if ctx.Err() != nil {
				return nil
			}
			if timer != nil {
				timer.Stop()
			}
			pending = false
			if err := attempt(); fatal(err) {
				return err
			}
		}
	}
}

func absAll(ps []string) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		a, err := filepath.Abs(p)
		if err != nil {
			a = p
		}
		out = append(out, filepath.Clean(a))
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func statOf(p string) fileStat {
	fi, err := os.Stat(p)
	if err != nil {
		return fileStat{}
	}
	target, err := filepath.EvalSymlinks(p)
	if err != nil {
		target = ""
	}
	return fileStat{exists: true, size: fi.Size(), mtime: fi.ModTime().UnixNano(), target: target}
}

func digestOf(p string) string {
	b, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return "missing"
	}
	if err != nil {
		return "error: " + err.Error()
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func snapshot(p string) record {
	st := statOf(p)
	return record{stat: st, digest: digestOf(p)}
}

func contentChanged(paths []string, recs map[string]record) bool {
	for _, p := range paths {
		r, ok := recs[p]
		if !ok || digestOf(p) != r.digest {
			return true
		}
	}
	return false
}

func entryChanged(ev fsnotify.Event, dirs map[string]bool) bool {
	return ev.Op&^fsnotify.Chmod != 0 && dirs[filepath.Dir(filepath.Clean(ev.Name))]
}

func relevant(ev fsnotify.Event, names, pathDirs map[string]bool) bool {
	if ev.Op&^fsnotify.Chmod == 0 {
		return false
	}
	n := filepath.Clean(ev.Name)
	if names[n] {
		return true
	}
	return strings.HasPrefix(filepath.Base(n), "..") && pathDirs[filepath.Dir(n)]
}
