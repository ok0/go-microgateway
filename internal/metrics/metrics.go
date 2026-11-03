// Package metrics provides counters, gauges and histograms with labels and a Prometheus text-format encoder.
package metrics

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// ErrInvalid is wrapped by errors returned for malformed metric definitions.
var ErrInvalid = errors.New("metrics: invalid definition")

// ErrConflict is wrapped by errors returned when a name is already registered with a different definition.
var ErrConflict = errors.New("metrics: conflicting registration")

// ContentType is the media type of the Prometheus text exposition format.
const ContentType = "text/plain; version=0.0.4; charset=utf-8"

// DefaultBuckets returns a fresh slice of latency bucket upper bounds in seconds.
func DefaultBuckets() []float64 {
	return []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
}

type kind int

const (
	kindCounter kind = iota
	kindGauge
	kindHistogram
)

func (k kind) String() string {
	switch k {
	case kindCounter:
		return "counter"
	case kindGauge:
		return "gauge"
	}
	return "histogram"
}

var histogramSuffixes = []string{"_bucket", "_sum", "_count"}

// Registry holds metric families and serves their current values.
type Registry struct {
	mu   sync.Mutex
	fams map[string]*family
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{fams: make(map[string]*family)}
}

type family struct {
	name    string
	help    string
	kind    kind
	labels  []string
	buckets []float64
	series  sync.Map
	vec     any
}

type series struct {
	values []string
	bits   atomic.Uint64
	counts []atomic.Uint64
	bounds []float64
}

func (s *series) float() float64 {
	return math.Float64frombits(s.bits.Load())
}

// Counter registers or retrieves a counter family with the given label names.
func (r *Registry) Counter(name, help string, labels ...string) (*CounterVec, error) {
	f, err := r.register(kindCounter, name, help, labels, nil)
	if err != nil {
		return nil, err
	}
	return f.vec.(*CounterVec), nil
}

// Registered reports whether a family named name exists in the registry.
func (r *Registry) Registered(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.fams[name]
	return ok
}

// Gauge registers or retrieves a gauge family with the given label names.
func (r *Registry) Gauge(name, help string, labels ...string) (*GaugeVec, error) {
	f, err := r.register(kindGauge, name, help, labels, nil)
	if err != nil {
		return nil, err
	}
	return f.vec.(*GaugeVec), nil
}

// Histogram registers or retrieves a histogram family with the given finite bucket upper bounds and label names.
// A single trailing +Inf bound is accepted and dropped; the +Inf bucket is always implicit.
func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) (*HistogramVec, error) {
	f, err := r.register(kindHistogram, name, help, labels, buckets)
	if err != nil {
		return nil, err
	}
	return f.vec.(*HistogramVec), nil
}

func (r *Registry) register(k kind, name, help string, labels []string, buckets []float64) (*family, error) {
	if !validName(name) {
		return nil, fmt.Errorf("%w: metric name %q", ErrInvalid, name)
	}
	for i, l := range labels {
		if !validLabel(l) || strings.HasPrefix(l, "__") {
			return nil, fmt.Errorf("%w: label name %q", ErrInvalid, l)
		}
		if k == kindHistogram && l == "le" {
			return nil, fmt.Errorf("%w: label name %q is reserved for histograms", ErrInvalid, l)
		}
		if slices.Contains(labels[:i], l) {
			return nil, fmt.Errorf("%w: duplicate label name %q", ErrInvalid, l)
		}
	}
	if k == kindHistogram {
		b, err := normalizeBuckets(buckets)
		if err != nil {
			return nil, err
		}
		buckets = b
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if f, ok := r.fams[name]; ok {
		if f.kind == k && f.help == help && slices.Equal(f.labels, labels) && slices.Equal(f.buckets, buckets) {
			return f, nil
		}
		return nil, fmt.Errorf("%w: %q is already registered with a different definition", ErrConflict, name)
	}
	if k == kindHistogram {
		for _, s := range histogramSuffixes {
			if _, ok := r.fams[name+s]; ok {
				return nil, fmt.Errorf("%w: histogram %q collides with existing family %q", ErrConflict, name, name+s)
			}
		}
	} else {
		for _, s := range histogramSuffixes {
			base, ok := strings.CutSuffix(name, s)
			if !ok {
				continue
			}
			if hf, ok := r.fams[base]; ok && hf.kind == kindHistogram {
				return nil, fmt.Errorf("%w: %q collides with series of histogram %q", ErrConflict, name, base)
			}
		}
	}

	f := &family{name: name, help: help, kind: k, labels: slices.Clone(labels), buckets: buckets}
	switch k {
	case kindCounter:
		f.vec = &CounterVec{f: f}
	case kindGauge:
		f.vec = &GaugeVec{f: f}
	default:
		f.vec = &HistogramVec{f: f}
	}
	r.fams[name] = f
	return f, nil
}

func normalizeBuckets(b []float64) ([]float64, error) {
	if n := len(b); n > 0 && math.IsInf(b[n-1], 1) {
		b = b[:n-1]
	}
	if len(b) == 0 {
		return nil, fmt.Errorf("%w: histogram needs at least one finite bucket bound", ErrInvalid)
	}
	for i, v := range b {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("%w: bucket bound %v is not finite", ErrInvalid, v)
		}
		if i > 0 && v <= b[i-1] {
			return nil, fmt.Errorf("%w: bucket bounds must be strictly increasing", ErrInvalid)
		}
	}
	return slices.Clone(b), nil
}

func validName(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if isLetter(c) || c == ':' || (i > 0 && isDigit(c)) {
			continue
		}
		return false
	}
	return s != ""
}

func validLabel(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if isLetter(c) || (i > 0 && isDigit(c)) {
			continue
		}
		return false
	}
	return s != ""
}

func isLetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

// CounterVec is a counter family; With selects the series for one label-value combination.
type CounterVec struct{ f *family }

// With returns the counter series for the given label values, in label-name order.
func (v *CounterVec) With(values ...string) *Counter {
	return &Counter{s: v.f.with(values)}
}

// GaugeVec is a gauge family; With selects the series for one label-value combination.
type GaugeVec struct{ f *family }

// With returns the gauge series for the given label values, in label-name order.
func (v *GaugeVec) With(values ...string) *Gauge {
	return &Gauge{s: v.f.with(values)}
}

// DeleteFunc removes every series whose label values satisfy del and returns the number removed.
// Deleted series disappear from the exposition, and a later With creates a new series at 0.
// Handles obtained earlier keep pointing at the orphaned series.
func (v *GaugeVec) DeleteFunc(del func(values []string) bool) int {
	n := 0
	v.f.series.Range(func(key, val any) bool {
		if del(slices.Clone(val.(*series).values)) {
			v.f.series.Delete(key)
			n++
		}
		return true
	})
	return n
}

// HistogramVec is a histogram family; With selects the series for one label-value combination.
type HistogramVec struct{ f *family }

// With returns the histogram series for the given label values, in label-name order.
func (v *HistogramVec) With(values ...string) *Histogram {
	return &Histogram{s: v.f.with(values)}
}

func (f *family) with(values []string) *series {
	if len(values) != len(f.labels) {
		panic(fmt.Sprintf("metrics: %s: got %d label values, want %d", f.name, len(values), len(f.labels)))
	}
	key := strings.Join(values, "\xff")
	if s, ok := f.series.Load(key); ok {
		return s.(*series)
	}
	s := &series{values: slices.Clone(values)}
	if f.kind == kindHistogram {
		s.bounds = f.buckets
		s.counts = make([]atomic.Uint64, len(f.buckets)+1)
	}
	actual, _ := f.series.LoadOrStore(key, s)
	return actual.(*series)
}

// Counter is a monotonically increasing value.
type Counter struct{ s *series }

// Inc adds one to the counter.
func (c *Counter) Inc() { c.Add(1) }

// Add adds v to the counter. It panics if v is negative or NaN.
func (c *Counter) Add(v float64) {
	if v < 0 || math.IsNaN(v) {
		panic("metrics: counter cannot decrease or be NaN")
	}
	addFloat(&c.s.bits, v)
}

// Value returns the current counter value.
func (c *Counter) Value() float64 { return c.s.float() }

// Gauge is a value that can go up and down.
type Gauge struct{ s *series }

// Set sets the gauge to v.
func (g *Gauge) Set(v float64) { g.s.bits.Store(math.Float64bits(v)) }

// Add adds v to the gauge.
func (g *Gauge) Add(v float64) { addFloat(&g.s.bits, v) }

// Inc adds one to the gauge.
func (g *Gauge) Inc() { g.Add(1) }

// Dec subtracts one from the gauge.
func (g *Gauge) Dec() { g.Add(-1) }

// Value returns the current gauge value.
func (g *Gauge) Value() float64 { return g.s.float() }

// Histogram records observations into buckets and keeps their sum.
type Histogram struct{ s *series }

// Observe records one observation v.
func (h *Histogram) Observe(v float64) {
	i := sort.SearchFloat64s(h.s.bounds, v)
	h.s.counts[i].Add(1)
	addFloat(&h.s.bits, v)
}

// Count returns the total number of observations.
func (h *Histogram) Count() uint64 {
	var n uint64
	for i := range h.s.counts {
		n += h.s.counts[i].Load()
	}
	return n
}

// Sum returns the sum of all observations.
func (h *Histogram) Sum() float64 { return h.s.float() }

func addFloat(bits *atomic.Uint64, v float64) {
	for {
		old := bits.Load()
		next := math.Float64bits(math.Float64frombits(old) + v)
		if bits.CompareAndSwap(old, next) {
			return
		}
	}
}
