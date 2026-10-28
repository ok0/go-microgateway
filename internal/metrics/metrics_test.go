package metrics_test

import (
	"bytes"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/ok0/lychgate/internal/metrics"
)

type family struct {
	typ     string
	help    string
	samples []sample
}

type sample struct {
	name   string
	labels []label
	value  float64
}

type label struct{ key, value string }

type hseries struct {
	key       []string
	hasLe     bool
	prevLe    float64
	prevCount float64
	infCount  float64
	sawInf    bool
	sawSum    bool
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func assertPanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	fn()
}

func parseText(t *testing.T, s string) map[string]family {
	t.Helper()
	out := make(map[string]family)
	if s == "" {
		return out
	}
	if !strings.HasSuffix(s, "\n") {
		t.Fatal("output does not end with a newline")
	}
	lines := strings.Split(s[:len(s)-1], "\n")
	var cur, prev string
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if strings.HasPrefix(line, "# HELP ") {
			name, help, ok := strings.Cut(strings.TrimPrefix(line, "# HELP "), " ")
			if !ok || !validMetricName(name) {
				t.Fatalf("malformed HELP line %q", line)
			}
			if prev != "" && name <= prev {
				t.Fatalf("family %q is not strictly after %q", name, prev)
			}
			checkHelp(t, help)
			if i+1 >= len(lines) || !strings.HasPrefix(lines[i+1], "# TYPE ") {
				t.Fatalf("HELP line %q is not followed by a TYPE line", line)
			}
			i++
			tname, typ, ok := strings.Cut(strings.TrimPrefix(lines[i], "# TYPE "), " ")
			if !ok || tname != name {
				t.Fatalf("TYPE line %q does not match family %q", lines[i], name)
			}
			switch typ {
			case "counter", "gauge", "histogram":
			default:
				t.Fatalf("unknown type %q for family %q", typ, name)
			}
			out[name] = family{typ: typ, help: help}
			cur, prev = name, name
			continue
		}
		if strings.HasPrefix(line, "#") {
			t.Fatalf("unexpected comment line %q", line)
		}
		if cur == "" {
			t.Fatalf("sample %q appears before any family", line)
		}
		name, labels, value := parseSample(t, line)
		f := out[cur]
		if f.typ == "histogram" {
			if name != cur+"_bucket" && name != cur+"_sum" && name != cur+"_count" {
				t.Fatalf("sample %q does not belong to histogram %q", name, cur)
			}
		} else if name != cur {
			t.Fatalf("sample %q does not belong to family %q", name, cur)
		}
		f.samples = append(f.samples, sample{name: name, labels: labels, value: value})
		out[cur] = f
	}
	for name, f := range out {
		checkFamily(t, name, f)
	}
	return out
}

func checkFamily(t *testing.T, name string, f family) {
	t.Helper()
	if f.typ == "histogram" {
		checkHistogram(t, name, f.samples)
		return
	}
	var prev []string
	for i, s := range f.samples {
		key := labelValues(s.labels, "")
		if i > 0 && compareKeys(key, prev) <= 0 {
			t.Fatalf("family %q: series %v is not strictly after %v", name, key, prev)
		}
		prev = key
	}
}

func checkHistogram(t *testing.T, name string, samples []sample) {
	t.Helper()
	var prevKey []string
	havePrev := false
	var cur *hseries
	for _, s := range samples {
		key := labelValues(s.labels, "le")
		if cur != nil && !slices.Equal(cur.key, key) {
			t.Fatalf("histogram %q: series %v ended before its _count", name, cur.key)
		}
		switch s.name {
		case name + "_bucket":
			if cur == nil {
				if havePrev && compareKeys(key, prevKey) <= 0 {
					t.Fatalf("histogram %q: series %v is not strictly after %v", name, key, prevKey)
				}
				cur = &hseries{key: key}
			}
			le, ok := leValue(s.labels)
			if !ok {
				t.Fatalf("histogram %q: bucket without le label", name)
			}
			if cur.sawSum {
				t.Fatalf("histogram %q: bucket after _sum", name)
			}
			if cur.hasLe && le <= cur.prevLe {
				t.Fatalf("histogram %q: le values are not strictly increasing", name)
			}
			if s.value < cur.prevCount {
				t.Fatalf("histogram %q: bucket counts decrease", name)
			}
			cur.hasLe, cur.prevLe, cur.prevCount = true, le, s.value
			if math.IsInf(le, 1) {
				cur.sawInf, cur.infCount = true, s.value
			}
		case name + "_sum":
			if cur == nil || !cur.sawInf || cur.sawSum {
				t.Fatalf("histogram %q: unexpected _sum", name)
			}
			cur.sawSum = true
		case name + "_count":
			if cur == nil || !cur.sawSum {
				t.Fatalf("histogram %q: unexpected _count", name)
			}
			if s.value != cur.infCount {
				t.Fatalf("histogram %q: _count %v differs from +Inf bucket %v", name, s.value, cur.infCount)
			}
			prevKey, havePrev = cur.key, true
			cur = nil
		default:
			t.Fatalf("histogram %q: unexpected sample %q", name, s.name)
		}
	}
	if cur != nil {
		t.Fatalf("histogram %q: series %v has no _count", name, cur.key)
	}
}

func labelValues(labels []label, skip string) []string {
	var out []string
	for _, l := range labels {
		if l.key != skip {
			out = append(out, l.value)
		}
	}
	return out
}

func leValue(labels []label) (float64, bool) {
	for _, l := range labels {
		if l.key == "le" {
			v, err := strconv.ParseFloat(l.value, 64)
			return v, err == nil
		}
	}
	return 0, false
}

func compareKeys(a, b []string) int {
	for i := range min(len(a), len(b)) {
		if c := strings.Compare(a[i], b[i]); c != 0 {
			return c
		}
	}
	return len(a) - len(b)
}

func checkHelp(t *testing.T, s string) {
	t.Helper()
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			continue
		}
		if i+1 >= len(s) || (s[i+1] != '\\' && s[i+1] != 'n') {
			t.Fatalf("invalid escape in HELP text %q", s)
		}
		i++
	}
}

func parseSample(t *testing.T, line string) (string, []label, float64) {
	t.Helper()
	i := 0
	for i < len(line) && isNameByte(line[i]) {
		i++
	}
	name := line[:i]
	if !validMetricName(name) {
		t.Fatalf("sample %q: invalid metric name", line)
	}
	var labels []label
	if i < len(line) && line[i] == '{' {
		i++
		for i < len(line) && line[i] != '}' {
			if len(labels) > 0 {
				if line[i] != ',' {
					t.Fatalf("sample %q: expected ',' between labels", line)
				}
				i++
			}
			start := i
			for i < len(line) && isNameByte(line[i]) && line[i] != ':' {
				i++
			}
			key := line[start:i]
			if !validLabelName(key) || i >= len(line) || line[i] != '=' {
				t.Fatalf("sample %q: malformed label name", line)
			}
			i++
			if i >= len(line) || line[i] != '"' {
				t.Fatalf("sample %q: label value must be quoted", line)
			}
			i++
			var sb strings.Builder
			for {
				if i >= len(line) {
					t.Fatalf("sample %q: unterminated label value", line)
				}
				c := line[i]
				if c == '"' {
					i++
					break
				}
				if c == '\\' {
					if i+1 >= len(line) {
						t.Fatalf("sample %q: dangling escape", line)
					}
					switch line[i+1] {
					case '\\':
						sb.WriteByte('\\')
					case '"':
						sb.WriteByte('"')
					case 'n':
						sb.WriteByte('\n')
					default:
						t.Fatalf("sample %q: invalid escape", line)
					}
					i += 2
					continue
				}
				sb.WriteByte(c)
				i++
			}
			labels = append(labels, label{key: key, value: sb.String()})
		}
		if i >= len(line) {
			t.Fatalf("sample %q: missing '}'", line)
		}
		i++
	}
	if i >= len(line) || line[i] != ' ' {
		t.Fatalf("sample %q: expected a single space before the value", line)
	}
	raw := line[i+1:]
	if raw == "" || strings.Contains(raw, " ") {
		t.Fatalf("sample %q: malformed value", line)
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		t.Fatalf("sample %q: %v", line, err)
	}
	return name, labels, v
}

func isNameByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == ':'
}

func validMetricName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		letter := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == ':'
		if !letter && !(i > 0 && c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func validLabelName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		letter := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
		if !letter && !(i > 0 && c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func TestGolden(t *testing.T) {
	r := metrics.NewRegistry()
	c, err := r.Counter("b_total", "Help with \\ and\nnewline.", "path")
	must(t, err)
	c.With("a\"b\\c\nd").Add(2)
	c.With("x").Inc()
	g, err := r.Gauge("a_gauge", "A gauge.")
	must(t, err)
	g.With().Set(-1.5)
	h, err := r.Histogram("c_seconds", "Latency.", []float64{0.1, 1}, "route")
	must(t, err)
	hh := h.With("r1")
	for _, v := range []float64{0.0625, 0.5, 2} {
		hh.Observe(v)
	}

	var buf bytes.Buffer
	must(t, r.WriteText(&buf))
	want := `# HELP a_gauge A gauge.
# TYPE a_gauge gauge
a_gauge -1.5
# HELP b_total Help with \\ and\nnewline.
# TYPE b_total counter
b_total{path="a\"b\\c\nd"} 2
b_total{path="x"} 1
# HELP c_seconds Latency.
# TYPE c_seconds histogram
c_seconds_bucket{route="r1",le="0.1"} 1
c_seconds_bucket{route="r1",le="1"} 2
c_seconds_bucket{route="r1",le="+Inf"} 3
c_seconds_sum{route="r1"} 2.5625
c_seconds_count{route="r1"} 3
`
	if got := buf.String(); got != want {
		t.Fatalf("exposition mismatch\n--- got\n%s--- want\n%s", got, want)
	}
	parseText(t, buf.String())
}

func TestRegisterInvalid(t *testing.T) {
	cases := []struct {
		name string
		fn   func(r *metrics.Registry) error
	}{
		{"empty name", func(r *metrics.Registry) error { _, err := r.Counter("", "h"); return err }},
		{"name starts with digit", func(r *metrics.Registry) error { _, err := r.Counter("1a", "h"); return err }},
		{"name with dash", func(r *metrics.Registry) error { _, err := r.Gauge("a-b", "h"); return err }},
		{"reserved double underscore label", func(r *metrics.Registry) error { _, err := r.Counter("x", "h", "__x"); return err }},
		{"label with space", func(r *metrics.Registry) error { _, err := r.Counter("x", "h", "a b"); return err }},
		{"duplicate label", func(r *metrics.Registry) error { _, err := r.Counter("x", "h", "x", "x"); return err }},
		{"histogram le label", func(r *metrics.Registry) error {
			_, err := r.Histogram("x", "h", []float64{1}, "le")
			return err
		}},
		{"nil buckets", func(r *metrics.Registry) error { _, err := r.Histogram("x", "h", nil); return err }},
		{"equal buckets", func(r *metrics.Registry) error {
			_, err := r.Histogram("x", "h", []float64{1, 1})
			return err
		}},
		{"decreasing buckets", func(r *metrics.Registry) error {
			_, err := r.Histogram("x", "h", []float64{2, 1})
			return err
		}},
		{"NaN bucket", func(r *metrics.Registry) error {
			_, err := r.Histogram("x", "h", []float64{math.NaN()})
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn(metrics.NewRegistry())
			if !errors.Is(err, metrics.ErrInvalid) {
				t.Fatalf("got %v, want ErrInvalid", err)
			}
		})
	}
}

func TestReRegister(t *testing.T) {
	r := metrics.NewRegistry()
	v1, err := r.Counter("re_total", "help", "a")
	must(t, err)
	v2, err := r.Counter("re_total", "help", "a")
	must(t, err)
	if v1 != v2 {
		t.Fatal("identical Counter registration returned a different vector")
	}

	conflicts := []struct {
		name string
		fn   func() error
	}{
		{"different help", func() error { _, err := r.Counter("re_total", "other", "a"); return err }},
		{"different labels", func() error { _, err := r.Counter("re_total", "help", "b"); return err }},
		{"different kind", func() error { _, err := r.Gauge("re_total", "help", "a"); return err }},
		{"counter named like histogram bucket", func() error {
			_, err := r.Histogram("rh", "help", []float64{1})
			if err != nil {
				return err
			}
			_, err = r.Counter("rh_bucket", "help")
			return err
		}},
		{"histogram named like counter total", func() error {
			_, err := r.Counter("rq_count", "help")
			if err != nil {
				return err
			}
			_, err = r.Histogram("rq", "help", []float64{1})
			return err
		}},
	}
	for _, tc := range conflicts {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.fn(); !errors.Is(err, metrics.ErrConflict) {
				t.Fatalf("got %v, want ErrConflict", err)
			}
		})
	}

	h1, err := r.Histogram("re_hist", "help", []float64{1, 2}, "x")
	must(t, err)
	h2, err := r.Histogram("re_hist", "help", []float64{1, 2}, "x")
	must(t, err)
	if h1 != h2 {
		t.Fatal("identical Histogram registration returned a different vector")
	}
	if _, err := r.Histogram("re_hist", "help", []float64{1, 3}, "x"); !errors.Is(err, metrics.ErrConflict) {
		t.Fatalf("different buckets: got %v, want ErrConflict", err)
	}
}

func TestConcurrent(t *testing.T) {
	r := metrics.NewRegistry()
	c, err := r.Counter("cc_total", "c", "k")
	must(t, err)
	g, err := r.Gauge("cc_gauge", "g")
	must(t, err)
	h, err := r.Histogram("cc_seconds", "h", []float64{1})
	must(t, err)

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				c.With("a").Inc()
				g.With().Add(1)
				h.With().Observe(0.5)
			}
		}()
	}
	wg.Wait()

	if v := c.With("a").Value(); v != 32000 {
		t.Fatalf("counter = %v, want 32000", v)
	}
	if v := g.With().Value(); v != 32000 {
		t.Fatalf("gauge = %v, want 32000", v)
	}
	hs := h.With()
	if n := hs.Count(); n != 32000 {
		t.Fatalf("histogram count = %d, want 32000", n)
	}
	if s := hs.Sum(); s != 16000 {
		t.Fatalf("histogram sum = %v, want 16000", s)
	}

	var buf bytes.Buffer
	must(t, r.WriteText(&buf))
	parseText(t, buf.String())
}

func TestWithArityPanics(t *testing.T) {
	r := metrics.NewRegistry()
	c, err := r.Counter("ar_total", "h", "a")
	must(t, err)
	assertPanics(t, func() { c.With("x", "y") })
	assertPanics(t, func() { c.With() })
}

func TestCounterNegativePanics(t *testing.T) {
	r := metrics.NewRegistry()
	c, err := r.Counter("neg_total", "h")
	must(t, err)
	assertPanics(t, func() { c.With().Add(-1) })
	assertPanics(t, func() { c.With().Add(math.NaN()) })
}

func TestHandler(t *testing.T) {
	r := metrics.NewRegistry()
	c, err := r.Counter("hd_total", "h")
	must(t, err)
	c.With().Inc()

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if ct := rec.Header().Get("Content-Type"); ct != metrics.ContentType {
		t.Fatalf("Content-Type = %q, want %q", ct, metrics.ContentType)
	}
	var buf bytes.Buffer
	must(t, r.WriteText(&buf))
	if rec.Body.String() != buf.String() {
		t.Fatalf("handler body differs from WriteText output\n%s\nvs\n%s", rec.Body.String(), buf.String())
	}
}

func TestGaugeDeleteFunc(t *testing.T) {
	r := metrics.NewRegistry()
	g, err := r.Gauge("g", "h", "a", "b")
	must(t, err)
	for _, v := range [][]string{{"x", "1"}, {"x", "2"}, {"y", "1"}} {
		g.With(v...).Set(5)
	}

	if n := g.DeleteFunc(func(v []string) bool { return v[0] == "x" }); n != 2 {
		t.Fatalf("DeleteFunc removed %d series, want 2", n)
	}

	var buf bytes.Buffer
	must(t, r.WriteText(&buf))
	want := "# HELP g h\n# TYPE g gauge\ng{a=\"y\",b=\"1\"} 5\n"
	if got := buf.String(); got != want {
		t.Fatalf("exposition after delete\n--- got\n%s--- want\n%s", got, want)
	}

	if v := g.With("x", "1").Value(); v != 0 {
		t.Fatalf("recreated series value = %v, want 0", v)
	}
	buf.Reset()
	must(t, r.WriteText(&buf))
	want = "# HELP g h\n# TYPE g gauge\ng{a=\"x\",b=\"1\"} 0\ng{a=\"y\",b=\"1\"} 5\n"
	if got := buf.String(); got != want {
		t.Fatalf("exposition after recreate\n--- got\n%s--- want\n%s", got, want)
	}
}

func TestGaugeDeleteFuncConcurrent(t *testing.T) {
	r := metrics.NewRegistry()
	g, err := r.Gauge("gc", "h", "a", "b")
	must(t, err)
	for _, v := range [][]string{{"x", "1"}, {"x", "2"}} {
		g.With(v...).Set(1)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 1000 {
			g.With("y", "1").Set(float64(i))
		}
	}()
	deleted := 0
	for range 100 {
		deleted += g.DeleteFunc(func(v []string) bool { return v[0] == "x" })
		g.With("x", "1").Set(1)
	}
	wg.Wait()

	if deleted < 1 {
		t.Fatalf("DeleteFunc removed %d series, want at least 1", deleted)
	}
	if v := g.With("y", "1").Value(); v != 999 {
		t.Fatalf("y series = %v, want 999", v)
	}
}
