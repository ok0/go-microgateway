package metrics

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

var (
	helpEscaper  = strings.NewReplacer(`\`, `\\`, "\n", `\n`)
	labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
)

// WriteText writes every family in the Prometheus text exposition format, sorted by family name and then by label values.
func (r *Registry) WriteText(w io.Writer) error {
	r.mu.Lock()
	fams := make([]*family, 0, len(r.fams))
	for _, f := range r.fams {
		fams = append(fams, f)
	}
	r.mu.Unlock()
	slices.SortFunc(fams, func(a, b *family) int { return strings.Compare(a.name, b.name) })

	bw := bufio.NewWriter(w)
	for _, f := range fams {
		writeFamily(bw, f)
	}
	return bw.Flush()
}

// Handler returns an http.Handler that serves the registry in the Prometheus text exposition format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", ContentType)
		_ = r.WriteText(w)
	})
}

func writeFamily(bw *bufio.Writer, f *family) {
	fmt.Fprintf(bw, "# HELP %s %s\n", f.name, helpEscaper.Replace(f.help))
	fmt.Fprintf(bw, "# TYPE %s %s\n", f.name, f.kind)
	for _, s := range sortedSeries(f) {
		body := labelBody(f.labels, s.values)
		if f.kind == kindHistogram {
			writeHistogram(bw, f.name, body, s)
			continue
		}
		fmt.Fprintf(bw, "%s%s %s\n", f.name, braces(body), formatFloat(s.float()))
	}
}

func writeHistogram(bw *bufio.Writer, name, body string, s *series) {
	var cum uint64
	for i, bound := range s.bounds {
		cum += s.counts[i].Load()
		fmt.Fprintf(bw, "%s_bucket%s %d\n", name, braces(join(body, `le="`+formatFloat(bound)+`"`)), cum)
	}
	cum += s.counts[len(s.bounds)].Load()
	fmt.Fprintf(bw, "%s_bucket%s %d\n", name, braces(join(body, `le="+Inf"`)), cum)
	fmt.Fprintf(bw, "%s_sum%s %s\n", name, braces(body), formatFloat(s.float()))
	fmt.Fprintf(bw, "%s_count%s %d\n", name, braces(body), cum)
}

func sortedSeries(f *family) []*series {
	var ss []*series
	f.series.Range(func(_, v any) bool {
		ss = append(ss, v.(*series))
		return true
	})
	slices.SortFunc(ss, func(a, b *series) int { return compareValues(a.values, b.values) })
	return ss
}

func compareValues(a, b []string) int {
	for i := range min(len(a), len(b)) {
		if c := strings.Compare(a[i], b[i]); c != 0 {
			return c
		}
	}
	return len(a) - len(b)
}

func labelBody(names, values []string) string {
	var sb strings.Builder
	for i, n := range names {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(n)
		sb.WriteString(`="`)
		sb.WriteString(labelEscaper.Replace(values[i]))
		sb.WriteByte('"')
	}
	return sb.String()
}

func join(a, b string) string {
	if a == "" {
		return b
	}
	return a + "," + b
}

func braces(body string) string {
	if body == "" {
		return ""
	}
	return "{" + body + "}"
}

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
