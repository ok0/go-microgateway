package doccheck

import (
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// FlowSource is the node id of the request source that starts the main path of the request-flow diagram.
const FlowSource = "client"

// Section headings of the architecture document that the flow checks read.
const (
	// LayersHeading is the heading of the section that holds the layers diagram.
	LayersHeading = "Layers"
	// RequestFlowHeading is the heading of the section that holds the request-flow diagram.
	RequestFlowHeading = "Request flow"
	// PipelineOrderHeading is the heading of the section that lists the default pipeline order.
	PipelineOrderHeading = "Default pipeline order"
)

// Edge is a flowchart edge: source and target node ids, its label (empty for an unlabeled edge) and its 1-based line in the diagram body.
type Edge struct {
	From, To, Label string
	Line            int
}

// Flowchart is the nodes and edges of a Mermaid flowchart: the label of each node by id ("" when the node has no label) and the edges in source order.
type Flowchart struct {
	Labels map[string]string
	Edges  []Edge
}

// FlowSpec is what the request-flow sections of the architecture document must show.
type FlowSpec struct {
	Flow      []string          // step ids in request order
	Stages    []string          // default pipeline stage names in order
	Terminal  string            // id that ends the Default pipeline order list
	RetryFrom string            // retry back edge source
	RetryTo   string            // retry back edge target
	Errors    map[string]string // step id -> text the error-exit target label must contain
}

var (
	flowNodeRE = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)(?:\["([^"]*)"\]|\{"([^"]*)"\}|\("([^"]*)"\)|\[([^\]"]*)\])?`)
	flowOpRE   = regexp.MustCompile(`^(?:--\s*"([^"]*)"\s*-->|-->\|([^|]*)\||-->)\s*`)
	wordRE     = regexp.MustCompile(`[A-Za-z0-9_]+`)
	itemRE     = regexp.MustCompile(`^[0-9]+\. (.*)$`)
)

// ParseFlowchart parses the body of a Mermaid flowchart. It accepts node declarations with optional labels, edges with optional labels and chained edges, and skips the directives that do not change the graph.
func ParseFlowchart(body string) (*Flowchart, error) {
	fc := &Flowchart{Labels: map[string]string{}}
	header := false
	for i, raw := range strings.Split(body, "\n") {
		n := i + 1
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "%%") {
			continue
		}
		if !header {
			if f := strings.Fields(line); f[0] != "flowchart" && f[0] != "graph" {
				return nil, fmt.Errorf("doccheck: line %d: not a flowchart", n)
			}
			header = true
			continue
		}
		if line == "end" || skipFlowLine(line) {
			continue
		}
		if err := fc.statement(line, n); err != nil {
			return nil, err
		}
	}
	if !header {
		return nil, fmt.Errorf("doccheck: no flowchart header")
	}
	return fc, nil
}

func skipFlowLine(line string) bool {
	for _, p := range []string{"subgraph ", "direction ", "classDef ", "class ", "style ", "linkStyle "} {
		if strings.HasPrefix(line, p) {
			return true
		}
	}
	return false
}

func (f *Flowchart) statement(text string, n int) error {
	id, label, rest, ok := parseFlowNode(text)
	if !ok {
		return fmt.Errorf("doccheck: line %d: cannot parse %q", n, text)
	}
	if err := f.addNode(id, label, n); err != nil {
		return err
	}
	prev := id
	for {
		rest = strings.TrimLeft(rest, " \t")
		if rest == "" {
			return nil
		}
		m := flowOpRE.FindStringSubmatch(rest)
		if m == nil {
			return fmt.Errorf("doccheck: line %d: cannot parse %q", n, text)
		}
		edgeLabel := m[1]
		if edgeLabel == "" {
			edgeLabel = m[2]
		}
		rest = rest[len(m[0]):]
		id, label, rest, ok = parseFlowNode(rest)
		if !ok {
			return fmt.Errorf("doccheck: line %d: cannot parse %q", n, text)
		}
		if err := f.addNode(id, label, n); err != nil {
			return err
		}
		f.Edges = append(f.Edges, Edge{From: prev, To: id, Label: edgeLabel, Line: n})
		prev = id
	}
}

func parseFlowNode(s string) (id, label, rest string, ok bool) {
	m := flowNodeRE.FindStringSubmatch(s)
	if m == nil {
		return "", "", "", false
	}
	for _, g := range m[2:] {
		if g != "" {
			label = g
			break
		}
	}
	return m[1], label, s[len(m[0]):], true
}

func (f *Flowchart) addNode(id, label string, n int) error {
	old, ok := f.Labels[id]
	if label == "" {
		if !ok {
			f.Labels[id] = ""
		}
		return nil
	}
	if ok && old != "" && old != label {
		return fmt.Errorf("doccheck: line %d: node %q has two labels", n, id)
	}
	f.Labels[id] = label
	return nil
}

// MainPath returns the node ids of the single path formed by the unlabeled edges, in walk order. It fails when the unlabeled edges do not form exactly one path from one start node.
func (f *Flowchart) MainPath() ([]string, error) {
	next := map[string]string{}
	incoming := map[string]bool{}
	var unlabeled []Edge
	for _, e := range f.Edges {
		if e.Label != "" {
			continue
		}
		if _, dup := next[e.From]; dup {
			return nil, fmt.Errorf("doccheck: node %q has two outgoing unlabeled edges", e.From)
		}
		if incoming[e.To] {
			return nil, fmt.Errorf("doccheck: node %q has two incoming unlabeled edges", e.To)
		}
		next[e.From] = e.To
		incoming[e.To] = true
		unlabeled = append(unlabeled, e)
	}
	var starts []string
	for _, e := range unlabeled {
		if !incoming[e.From] && !slices.Contains(starts, e.From) {
			starts = append(starts, e.From)
		}
	}
	if len(starts) != 1 {
		return nil, fmt.Errorf("doccheck: want one start node, found %d", len(starts))
	}
	path := []string{starts[0]}
	cur := starts[0]
	for used := 0; used < len(unlabeled); used++ {
		nxt, ok := next[cur]
		if !ok {
			break
		}
		path = append(path, nxt)
		cur = nxt
	}
	if len(path)-1 != len(unlabeled) {
		return nil, fmt.Errorf("doccheck: unlabeled edges do not form one path")
	}
	return path, nil
}

// CodeSpans returns the contents of each backtick code span in s, in order, without the backticks.
func CodeSpans(s string) []string {
	var out []string
	for _, m := range codeSpanRE.FindAllString(s, -1) {
		out = append(out, strings.Trim(m, "`"))
	}
	return out
}

// SectionItems returns the numbered-list items under the first heading whose text is heading. A continuation line indented by two or more spaces joins the item it follows, and fenced lines are ignored. It returns nil when the heading is absent.
func SectionItems(d *Doc, heading string) []string {
	idx := -1
	for i, h := range d.Headings {
		if h.Text == heading {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil
	}
	lo := d.Headings[idx].Line
	hi := len(d.Lines)
	if idx+1 < len(d.Headings) {
		hi = d.Headings[idx+1].Line - 1
	}
	var items []string
	inItem, fenced := false, false
	var fenceChar byte
	fenceLen := 0
	for i := lo; i < hi; i++ {
		line := d.Lines[i]
		if fenced {
			if isFenceClose(line, fenceChar, fenceLen) {
				fenced = false
			}
			continue
		}
		if c, n, _, ok := fenceOpen(line); ok {
			fenced, fenceChar, fenceLen = true, c, n
			inItem = false
			continue
		}
		if strings.TrimSpace(line) == "" {
			inItem = false
			continue
		}
		if m := itemRE.FindStringSubmatch(line); m != nil {
			items = append(items, m[1])
			inItem = true
			continue
		}
		if inItem && strings.HasPrefix(line, "  ") {
			items[len(items)-1] += " " + strings.TrimSpace(line)
			continue
		}
		inItem = false
	}
	return items
}

// FlowDocFindings checks the request-flow diagram and the numbered lists of the architecture document d against spec. It returns sorted findings prefixed with the document path.
func FlowDocFindings(d *Doc, spec FlowSpec) []string {
	var out []string
	add := func(format string, args ...any) {
		out = append(out, d.Path+": "+fmt.Sprintf(format, args...))
	}
	var diagrams []Fence
	for _, f := range d.Fences {
		if f.Lang == "mermaid" && f.Heading == RequestFlowHeading {
			diagrams = append(diagrams, f)
		}
	}
	if len(diagrams) != 1 {
		add("want one mermaid diagram under %q, found %d", RequestFlowHeading, len(diagrams))
	} else if fc, err := ParseFlowchart(diagrams[0].Body); err != nil {
		add("request-flow diagram: %v", err)
	} else {
		out = append(out, flowDiagramFindings(d.Path, fc, spec)...)
	}
	var spans []string
	for _, item := range SectionItems(d, RequestFlowHeading) {
		spans = append(spans, CodeSpans(item)...)
	}
	var named []string
	for _, s := range spans {
		if slices.Contains(spec.Flow, s) {
			named = append(named, s)
		}
	}
	if !slices.Equal(named, spec.Flow) {
		add("numbered list of %q names %v, want %v", RequestFlowHeading, named, spec.Flow)
	}
	var terms []string
	for _, item := range SectionItems(d, PipelineOrderHeading) {
		term := ""
		if s := CodeSpans(item); len(s) > 0 {
			term = s[0]
		}
		terms = append(terms, term)
	}
	want := append(slices.Clone(spec.Stages), spec.Terminal)
	if !slices.Equal(terms, want) {
		add("numbered list of %q = %v, want %v", PipelineOrderHeading, terms, want)
	}
	sort.Strings(out)
	return out
}

func flowDiagramFindings(path string, fc *Flowchart, spec FlowSpec) []string {
	var out []string
	add := func(format string, args ...any) {
		out = append(out, path+": "+fmt.Sprintf(format, args...))
	}
	main, err := fc.MainPath()
	if err != nil {
		add("request-flow main path: %v", err)
	}
	want := append([]string{FlowSource}, spec.Flow...)
	if err == nil && !slices.Equal(main, want) {
		add("request-flow main path = %v, want %v", main, want)
	}
	for _, s := range spec.Stages {
		if !strings.Contains(fc.Labels[s], "if configured") {
			add("request-flow node %q lacks \"if configured\"", s)
		}
	}
	retry := false
	for _, e := range fc.Edges {
		if e.From == spec.RetryFrom && e.To == spec.RetryTo && strings.Contains(e.Label, "retry") {
			retry = true
			break
		}
	}
	if !retry {
		add("request-flow diagram has no retry edge from %q to %q labelled \"retry\"", spec.RetryFrom, spec.RetryTo)
	}
	froms := make([]string, 0, len(spec.Errors))
	for from := range spec.Errors {
		froms = append(froms, from)
	}
	sort.Strings(froms)
	for _, from := range froms {
		text := spec.Errors[from]
		found := false
		for _, e := range fc.Edges {
			if e.From == from && e.Label != "" && strings.Contains(fc.Labels[e.To], text) {
				found = true
				break
			}
		}
		if !found {
			add("request-flow diagram has no error exit from %q to %q", from, text)
		}
	}
	return out
}

// FlowCopyFindings checks that the request flow is drawn only in the architecture document arch. It returns sorted findings: the architecture document must hold exactly the layers and request-flow diagrams, no other mermaid diagram may draw three or more step ids as nodes or name three consecutive steps in flow order, and no other line may name every stage in order.
func FlowCopyFindings(docs []*Doc, arch string, ids, stages []string) []string {
	arch = filepath.Clean(arch)
	var out []string
	var archDoc *Doc
	for _, d := range docs {
		if filepath.Clean(d.Path) == arch {
			archDoc = d
			break
		}
	}
	if archDoc == nil {
		out = append(out, arch+": missing")
	} else {
		var headings []string
		for _, f := range archDoc.Fences {
			if f.Lang == "mermaid" {
				headings = append(headings, f.Heading)
			}
		}
		if len(headings) != 2 || !slices.Contains(headings, LayersHeading) || !slices.Contains(headings, RequestFlowHeading) {
			out = append(out, fmt.Sprintf("%s: want one mermaid diagram under %q and one under %q, found %v", arch, LayersHeading, RequestFlowHeading, headings))
		}
	}
	for _, d := range docs {
		isArch := filepath.Clean(d.Path) == arch
		for _, f := range d.Fences {
			if f.Lang != "mermaid" {
				continue
			}
			n := countStepIDs(f.Body, ids)
			if isArch && f.Heading == RequestFlowHeading {
				if n < 3 {
					out = append(out, fmt.Sprintf("%s:%d: request-flow diagram names %d step ids", d.Path, f.Line, n))
				}
			} else {
				out = append(out, copyFindings(d.Path, f.Line, f.Body, ids, arch)...)
			}
		}
		if isArch {
			continue
		}
		for i, line := range d.Lines {
			if namesInOrder(wordRE.FindAllString(line, -1), stages) {
				out = append(out, fmt.Sprintf("%s:%d: line names every default stage in order; link to %s#%s instead", d.Path, i+1, filepath.Base(arch), Anchor(PipelineOrderHeading)))
			}
		}
	}
	if len(ids) < 3 || len(stages) < 2 {
		out = append(out, "flow copy check needs at least three step ids and two stages")
	}
	sort.Strings(out)
	return out
}

// copyFindings returns the findings for a mermaid fence outside the architecture document arch. A fence copies the request flow when it draws three or more step ids as nodes, or names three consecutive steps in flow order anywhere in its body, labels included. Common words used as step ids do not make a different diagram a copy.
func copyFindings(path string, line int, body string, ids []string, arch string) []string {
	var out []string
	nodes := 0
	if fc, err := ParseFlowchart(body); err == nil {
		for _, id := range ids {
			if _, ok := fc.Labels[id]; ok {
				nodes++
			}
		}
	} else {
		nodes = countStepIDs(body, ids)
	}
	if nodes >= 3 {
		out = append(out, fmt.Sprintf("%s:%d: mermaid diagram draws %d request-flow step ids as nodes; the request flow is drawn only in %s", path, line, nodes, arch))
	}
	var seq []string
	for _, tok := range wordRE.FindAllString(body, -1) {
		if slices.Contains(ids, tok) {
			seq = append(seq, tok)
		}
	}
	for i := 0; i+3 <= len(ids); i++ {
		if namesInOrder(seq, ids[i:i+3]) {
			out = append(out, fmt.Sprintf("%s:%d: mermaid diagram names consecutive request-flow steps %q, %q, %q in order; the request flow is drawn only in %s", path, line, ids[i], ids[i+1], ids[i+2], arch))
			break
		}
	}
	return out
}

func countStepIDs(body string, ids []string) int {
	seen := map[string]bool{}
	for _, tok := range wordRE.FindAllString(body, -1) {
		if slices.Contains(ids, tok) {
			seen[tok] = true
		}
	}
	return len(seen)
}

func namesInOrder(tokens, names []string) bool {
	i := 0
	for _, t := range tokens {
		if i < len(names) && t == names[i] {
			i++
		}
	}
	return i == len(names)
}

// ReadmeArchitectureFindings checks that the README points to the architecture document target and holds no architecture content of its own. It returns sorted findings prefixed with the README path.
func ReadmeArchitectureFindings(readme *Doc, target string) []string {
	var out []string
	add := func(format string, args ...any) {
		out = append(out, readme.Path+": "+fmt.Sprintf(format, args...))
	}
	for _, h := range readme.Headings {
		if h.Text == "Architecture overview" || h.Text == "Request flow" {
			add("heading %q at line %d; that content lives in %s", h.Text, h.Line, target)
		}
	}
	for _, f := range readme.Fences {
		if f.Lang == "mermaid" {
			add("mermaid diagram at line %d; diagrams live in %s", f.Line, target)
		}
	}
	docStart, docEnd := 0, 0
	for i, h := range readme.Headings {
		if h.Level == 2 && h.Text == "Documentation" {
			docStart, docEnd = h.Line, len(readme.Lines)
			for _, next := range readme.Headings[i+1:] {
				if next.Level <= 2 {
					docEnd = next.Line - 1
					break
				}
			}
			break
		}
	}
	var pointers []int
	for _, l := range readme.Links {
		if l.Line >= docStart && l.Line <= docEnd {
			continue
		}
		if l.Target == target || strings.HasPrefix(l.Target, target+"#") {
			pointers = append(pointers, l.Line)
		}
	}
	if len(pointers) != 1 {
		add("want exactly one link to %s outside Documentation, found %d", target, len(pointers))
	} else {
		line := pointers[0]
		isText := func(n int) bool {
			t := readme.Lines[n-1]
			return strings.TrimSpace(t) != "" && !headingRE.MatchString(t)
		}
		first, last := line, line
		for first > 1 && isText(first-1) {
			first--
		}
		for last < len(readme.Lines) && isText(last+1) {
			last++
		}
		if n := last - first + 1; n > 2 {
			add("pointer paragraph at line %d has %d lines, want at most 2", first, n)
		}
	}
	var h2 []Heading
	for _, h := range readme.Headings {
		if h.Level == 2 {
			h2 = append(h2, h)
		}
	}
	firstOf := func(text string) (Heading, bool) {
		for _, h := range h2 {
			if h.Text == text {
				return h, true
			}
		}
		return Heading{}, false
	}
	for _, name := range []string{"Install", "Quick start", "Module layout", "Dependencies"} {
		if _, ok := firstOf(name); !ok {
			add("missing heading %q", name)
		}
	}
	inst, okInst := firstOf("Install")
	qs, okQS := firstOf("Quick start")
	if okInst && okQS && inst.Line > qs.Line {
		add("%q must come before %q", "Install", "Quick start")
	}
	if okQS {
		for _, h := range h2 {
			if h.Line < qs.Line && h.Text != "Features" && h.Text != "Requirements" && h.Text != "Install" {
				add("heading %q comes before %q and %q", h.Text, "Install", "Quick start")
			}
		}
	}
	sort.Strings(out)
	return out
}
