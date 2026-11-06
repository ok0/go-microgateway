package e2e

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/doccheck"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/teststore"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

// docFiles loads the README and every markdown file under docs.
func docFiles(t *testing.T) []*doccheck.Doc {
	t.Helper()
	root, err := doccheck.Root()
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(root, "docs", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	paths = append(paths, filepath.Join(root, "README.md"))
	var docs []*doccheck.Doc
	for _, p := range paths {
		d, err := doccheck.Load(p)
		if err != nil {
			t.Fatal(err)
		}
		docs = append(docs, d)
	}
	return docs
}

var (
	docMetricToken  = regexp.MustCompile(`microgateway_[a-z0-9_]+`)
	metricTypeLine  = regexp.MustCompile(`^# TYPE (\S+) (\S+)$`)
	metricLabelName = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)="`)
)

type metricDoc struct {
	typ    string
	labels []string
}

type metricSeen struct {
	typ      string
	labels   map[string]bool
	sampled  bool
	positive bool
}

// documentedMetrics returns the family table rows of every document (header | Metric | Type | Labels | Meaning |).
func documentedMetrics(t *testing.T, docs []*doccheck.Doc) map[string]metricDoc {
	t.Helper()
	header := []string{"Metric", "Type", "Labels", "Meaning"}
	out := map[string]metricDoc{}
	for _, d := range docs {
		for _, tbl := range d.Tables {
			if !slices.Equal(tbl.Header, header) {
				continue
			}
			for _, row := range tbl.Rows {
				if len(row) != 4 {
					t.Fatalf("%s:%d: metric row has %d cells, want 4", d.Path, tbl.Line, len(row))
				}
				name := strings.Trim(row[0], "`")
				if _, dup := out[name]; dup {
					t.Errorf("metric %s is documented twice", name)
				}
				var labels []string
				if row[2] != "none" {
					for _, l := range strings.Split(row[2], ", ") {
						labels = append(labels, strings.Trim(l, "`"))
					}
				}
				out[name] = metricDoc{typ: row[1], labels: labels}
			}
		}
	}
	return out
}

// observedMetrics parses the exposition texts into families: the declared type, the label names used by the samples
// (le excluded), whether any sample exists, and whether a counter sample or a histogram _count is above zero.
func observedMetrics(t *testing.T, texts []string) map[string]*metricSeen {
	t.Helper()
	fams := map[string]*metricSeen{}
	for _, text := range texts {
		for _, line := range strings.Split(text, "\n") {
			m := metricTypeLine.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			if f, ok := fams[m[1]]; ok && f.typ != m[2] {
				t.Errorf("family %s declared as %s and %s", m[1], f.typ, m[2])
				continue
			}
			if fams[m[1]] == nil {
				fams[m[1]] = &metricSeen{typ: m[2], labels: map[string]bool{}}
			}
		}
	}
	for _, text := range texts {
		for _, line := range strings.Split(text, "\n") {
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			open := strings.IndexAny(line, "{ ")
			if open < 0 {
				t.Fatalf("malformed sample %q", line)
			}
			name := line[:open]
			var labels string
			if line[open] == '{' {
				end := strings.LastIndexByte(line, '}')
				if end < open {
					t.Fatalf("malformed sample %q", line)
				}
				labels = line[open+1 : end]
			}
			v, err := strconv.ParseFloat(line[strings.LastIndexByte(line, ' ')+1:], 64)
			if err != nil {
				t.Fatalf("sample value of %q: %v", line, err)
			}
			base, suffix := name, ""
			if fams[name] == nil {
				for _, s := range []string{"_bucket", "_sum", "_count"} {
					if b, ok := strings.CutSuffix(name, s); ok && fams[b] != nil && fams[b].typ == "histogram" {
						base, suffix = b, s
						break
					}
				}
			}
			f := fams[base]
			if f == nil {
				t.Errorf("sample of undeclared family %s", name)
				continue
			}
			f.sampled = true
			for _, m := range metricLabelName.FindAllStringSubmatch(labels, -1) {
				if m[1] != "le" {
					f.labels[m[1]] = true
				}
			}
			if suffix == "_bucket" || suffix == "_sum" {
				continue
			}
			if v > 0 {
				f.positive = true
			}
		}
	}
	return fams
}

func checkMetricFamilies(t *testing.T, documented map[string]metricDoc, seen map[string]*metricSeen) {
	t.Helper()
	names := make([]string, 0, len(documented))
	for name := range documented {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		want := documented[name]
		got, ok := seen[name]
		if !ok {
			t.Errorf("documented family %s is not exposed by the scenario", name)
			continue
		}
		if got.typ != want.typ {
			t.Errorf("family %s type = %s, documented %s", name, got.typ, want.typ)
		}
		var gotLabels []string
		for l := range got.labels {
			gotLabels = append(gotLabels, l)
		}
		sort.Strings(gotLabels)
		wantLabels := slices.Clone(want.labels)
		sort.Strings(wantLabels)
		if !slices.Equal(gotLabels, wantLabels) {
			t.Errorf("family %s labels = %v, documented %v", name, gotLabels, wantLabels)
		}
		switch want.typ {
		case "counter", "histogram":
			if !got.positive {
				t.Errorf("family %s has no sample above zero", name)
			}
		case "gauge":
			if !got.sampled {
				t.Errorf("gauge %s has no sample", name)
			}
		}
	}
	exposed := make([]string, 0, len(seen))
	for name := range seen {
		exposed = append(exposed, name)
	}
	sort.Strings(exposed)
	for _, name := range exposed {
		if _, ok := documented[name]; !ok {
			t.Errorf("exposed family %s is not documented", name)
		}
	}
}

func checkMetricTokens(t *testing.T, docs []*doccheck.Doc, documented map[string]metricDoc) {
	t.Helper()
	for _, d := range docs {
		for i, line := range d.Lines {
			for _, tok := range docMetricToken.FindAllString(line, -1) {
				if _, ok := documented[tok]; !ok {
					t.Errorf("%s:%d: %s is not a documented metric family", d.Path, i+1, tok)
				}
			}
		}
	}
}

type metricSnap func(g *microgateway.Gateway)

func metricScenarioBasic(t *testing.T, n *memNetwork, snap metricSnap) {
	be := n.serve(t, echoHandler("basic"))
	g := n.gateway(t, oneRouteDoc("", be.URL))
	if rec := rlGet(t, g, "http://example.com/basic", "192.0.2.1:1000", nil); rec.Code != http.StatusOK {
		t.Fatalf("basic GET = %d, want 200", rec.Code)
	}
	if err := g.ReloadYAML([]byte(oneRouteDoc("global:\n  drain_timeout: 31s\n", be.URL))); err != nil {
		t.Fatal(err)
	}
	snap(g)
}

func metricScenarioUpstream(t *testing.T, n *memNetwork, snap metricSnap) {
	r, hosts := newPassiveRig(t, n, 4, 50)
	for _, h := range hosts[:3] {
		h.setStatus(http.StatusServiceUnavailable)
	}
	for range 40 {
		r.get("/api/x")
	}
	snap(r.g)

	probe := n.serve(t, echoHandler("probe"))
	activeFake := clock.NewFake(time.Unix(0, 0))
	ga := n.gateway(t, miDoc(miTimeouts+miRR+miActive, []string{probe.URL}), withClock(activeFake))
	completed := miActiveChecks(t, ga)
	waitProbes(t, activeFake, completed, 0, 1)
	activeFake.Advance(10 * time.Second)
	waitProbes(t, activeFake, completed, 1, 1)
	snap(ga)

	n.serveAt(t, "10.0.0.1:8080", echoHandler("d1"))
	n.serveAt(t, "10.0.0.2:8080", echoHandler("d2"))
	fr := &fakeResolver{}
	fr.set([]string{"10.0.0.1"})
	dnsFake := clock.NewFake(time.Unix(0, 0))
	dnsDoc := "version: 1\n" + listenersYAML + "upstreams:\n  - name: up\n" + miTimeouts +
		"    dns:\n      hostname: svc.internal\n      port: 8080\n      scheme: http\n      refresh_interval: 5s\n" +
		"routes:\n  - name: all\n    upstream: up\n"
	gd := n.gateway(t, dnsDoc, withClock(dnsFake), withResolver(fr))
	dnsFake.BlockUntil(1)
	fr.set([]string{"10.0.0.2"})
	dnsFake.Advance(5 * time.Second)
	refreshed := []string{"http://10.0.0.2:8080"}
	eventually(t, "DNS refresh to 10.0.0.2", func() bool { return slices.Equal(miHostURLs(t, gd), refreshed) }, nil)
	snap(gd)

	bkBackend := newRSBackend(t, n, rsStatusSeq(503))
	bkFake := clock.NewFake(time.Now())
	gb := n.gateway(t, rsDoc{hosts: []string{bkBackend.URL}, breaker: rsBreaker}.yaml(), withClock(bkFake))
	bts := n.serve(t, gb)
	bclient := n.client(t, nil)
	for range 3 {
		bkGet(t, bclient, bts.URL+"/", http.StatusServiceUnavailable)
	}
	code, _, body := rsGet(t, bclient, bts.URL+"/", http.MethodGet, nil, nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("open breaker request = %d, want 503", code)
	}
	bkCircuitOpen(t, body)
	snap(gb)

	rb := newRSBackend(t, n, rsStatusSeq(502, 200))
	rf := clock.NewFake(time.Now())
	renv := reServe(t, n, rf, reSingle([]string{rb.URL}, "retry: {}"))
	rd := reDrive(t, rf, 250*time.Millisecond)
	reGet(t, renv, http.MethodGet, "/", nil, nil)
	rd.stop()
	snap(renv.g)

	bb := newRSBackend(t, n, rsStatusSeq(503))
	bf := clock.NewFake(time.Now())
	benv := reServe(t, n, bf, reSingle([]string{bb.URL}, "retry:\n      attempts: 2\n      base_delay: 1ns\n      max_delay: 1ns\n      budget:\n        ratio: 0.1\n        min_per_second: 0\n        window: 10s\n"))
	reGet(t, benv, http.MethodGet, "/", nil, nil)
	snap(benv.g)
}

func metricScenarioAuth(t *testing.T, snap metricSnap) {
	e := newAuEnv(t)
	k := auTestKeys(t)
	be, _ := auBackend(e)
	f := &auFetcher{}
	f.set(auJWKS(auJWK("k1", "RS256", &k.rsa1.PublicKey)), nil)
	g := e.gateway(auJWKSDoc(be.URL, jwksTestURL, "", "1", true),
		microgateway.WithKeySetFetcher(f), microgateway.WithHTTPClient(auFailingClient(t)))
	if rr := e.get(g, "/jwt", nil); rr.Code != http.StatusUnauthorized {
		t.Fatalf("request without token = %d, want 401", rr.Code)
	}
	tok := e.mint("RS256", "k1", k.rsa1, e.claims(nil))
	if rr := e.get(g, "/jwt", http.Header{"Authorization": []string{"Bearer " + tok}}); rr.Code != http.StatusOK {
		t.Fatalf("request with token = %d, want 200", rr.Code)
	}
	snap(g)
}

func metricScenarioRateLimit(t *testing.T, n *memNetwork, snap metricSnap) {
	be := n.serve(t, echoHandler("rl"))
	g := n.gateway(t, rlDoc(be.URL, "", stRoute("rl", smRate("", 1, 1))), withClock(rlFake()))
	rlGet(t, g, "/rl", smClient, nil)
	if rec := rlGet(t, g, "/rl", smClient, nil); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second request = %d, want 429", rec.Code)
	}
	snap(g)
}

func metricScenarioStore(t *testing.T, n *memNetwork, snap metricSnap) {
	be := n.serve(t, echoHandler("store"))
	fake := rlFake()
	shared := stShared(t, fake)
	var calls atomic.Int64
	g := n.gateway(t, stDoc(be.URL, stStore("shared", ""), stRoute("rl", spRateLimit("local", 3, spStoreTimeout))),
		withClock(fake), stOption(shared, &calls))
	rlGet(t, g, "/rl", smClient, nil)
	shared.SetMode(teststore.Fail, fmt.Errorf("store down: %w", store.ErrUnavailable))
	for range 3 {
		rlGet(t, g, "/rl", smClient, nil)
	}
	snap(g)
}

func metricScenarioCache(t *testing.T, n *memNetwork, snap metricSnap) {
	scr := &cacheScript{}
	scr.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
	be := n.serve(t, scr)
	fake := clock.NewFake(cacheStart)
	shared := teststore.NewSharedCache(fake, 1<<20)
	t.Cleanup(func() { _ = shared.Close() })
	g := n.gateway(t, cacheSharedDoc(be.URL, "  store_timeout: 100ms\n", cacheRouteYAML("r")),
		withClock(fake), cacheSharedOption("shared", shared))
	cacheDo(g, http.MethodGet, "http://example.com/a", nil)
	cacheDo(g, http.MethodGet, "http://example.com/a", nil)
	shared.SetOpMode("put", teststore.Fail, fmt.Errorf("put down: %w", store.ErrUnavailable))
	cacheDo(g, http.MethodGet, "http://example.com/b", nil)
	snap(g)

	o := newFlightOrigin()
	o.set(http.StatusOK, coalesceHot(), "x")
	o.hold()
	cg, _ := coalesceGateway(t, n, o, "", cacheRouteYAML("r"))
	recs := make(chan *httptest.ResponseRecorder, 3)
	for range 3 {
		go func() { recs <- cacheDo(cg, http.MethodGet, "http://example.com/hot", nil) }()
	}
	testwait.Recv(t, "leader at origin", o.arrived, nil)
	awaitWaiters(t, cg, 2)
	o.release()
	for range 3 {
		testwait.Recv(t, "coalesced response", recs, nil)
	}
	snap(cg)

	so := newFlightOrigin()
	so.set(http.StatusOK, staleSWRHeader(), "x")
	sg, sfake := coalesceGateway(t, n, so, "", cacheRouteYAML("r"))
	cacheDo(sg, http.MethodGet, "http://example.com/s", nil)
	sfake.Advance(61 * time.Second)
	cacheDo(sg, http.MethodGet, "http://example.com/s", nil)
	eventually(t, "background revalidation", func() bool {
		total := 0.0
		for _, res := range staleBackgroundResults {
			total += rsMetricOr0(t, sg, "microgateway_cache_background_revalidations_total", "route", "r", "result", res)
		}
		return total == 1
	}, nil)
	snap(sg)

	ev := &cacheScript{}
	ev.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, strings.Repeat("x", 2000))
	evBe := n.serve(t, ev)
	eg := n.gateway(t, cacheGatewayDoc(evBe.URL, "cache:\n  max_body_bytes: 2048\n  memory:\n    max_bytes: 4096\n    shards: 1\n", cacheRouteYAML("r")),
		withClock(clock.NewFake(cacheStart)))
	for _, p := range []string{"/1", "/2", "/3"} {
		cacheDo(eg, http.MethodGet, "http://example.com"+p, nil)
	}
	snap(eg)

	pg, _, _ := purgeFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	cacheDo(pg, http.MethodGet, "http://example.com/p", nil)
	if rec := adminReq(pg, http.MethodPost, "/cache/purge", "Bearer "+purgeTokA, `{"all":true}`); rec.Code != http.StatusOK {
		t.Fatalf("purge = %d, want 200", rec.Code)
	}
	snap(pg)
}

// TestDocumentedMetricFamilies drives every metric family through public surfaces and checks the family table of the
// documentation against the exposition: names, types, label names, samples, and that no other family is exposed.
func TestDocumentedMetricFamilies(t *testing.T) {
	var texts []string
	snap := func(g *microgateway.Gateway) {
		texts = append(texts, metricsText(t, g))
	}
	n := newMemNetwork()
	metricScenarioBasic(t, n, snap)
	metricScenarioUpstream(t, n, snap)
	metricScenarioAuth(t, snap)
	metricScenarioRateLimit(t, n, snap)
	metricScenarioStore(t, n, snap)
	metricScenarioCache(t, n, snap)

	docs := docFiles(t)
	documented := documentedMetrics(t, docs)
	if len(documented) == 0 {
		t.Fatal("no metric family table in the documentation")
	}
	checkMetricFamilies(t, documented, observedMetrics(t, texts))
	checkMetricTokens(t, docs, documented)
}

type errorPair struct {
	status int
	code   string
}

// errorSink collects the (status, code) pairs of error responses.
type errorSink map[errorPair]bool

func (s errorSink) add(t *testing.T, status int, body []byte) {
	t.Helper()
	var out struct {
		Error apiError `json:"error"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("error body %q: %v", body, err)
	}
	if out.Error.Status != status {
		t.Fatalf("body status = %d, response status = %d", out.Error.Status, status)
	}
	s[errorPair{status, out.Error.Code}] = true
}

func (s errorSink) addRec(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	s.add(t, rec.Code, rec.Body.Bytes())
}

// documentedErrors returns the rows of the table under the heading Error responses.
func documentedErrors(t *testing.T, docs []*doccheck.Doc) map[errorPair]bool {
	t.Helper()
	header := []string{"Status", "Code", "Cause"}
	out := map[errorPair]bool{}
	for _, d := range docs {
		for _, tbl := range d.Tables {
			if tbl.Heading != "Error responses" || !slices.Equal(tbl.Header, header) {
				continue
			}
			for _, row := range tbl.Rows {
				if len(row) != 3 {
					t.Fatalf("%s:%d: error row has %d cells, want 3", d.Path, tbl.Line, len(row))
				}
				status, err := strconv.Atoi(row[0])
				if err != nil {
					t.Fatalf("%s:%d: status %q is not an integer", d.Path, tbl.Line, row[0])
				}
				out[errorPair{status, strings.Trim(row[1], "`")}] = true
			}
		}
	}
	return out
}

func adminReq(g *microgateway.Gateway, method, path, authorization, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	rec := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(rec, req)
	return rec
}

// TestDocumentedErrorCodes triggers every error code of the gateway and checks the Error responses table: each row
// is observed with its status, every observed pair is documented, and the codes are exactly the httperr constants
// except client_closed_request, which has no body.
func TestDocumentedErrorCodes(t *testing.T) {
	sink := errorSink{}
	n := newMemNetwork()
	okBe := backend(t, "ok")

	api := gwWith(t, "version: 1\n"+listenersYAML+fmt.Sprintf("upstreams:\n  - name: up\n    hosts:\n      - url: %q\nroutes:\n  - name: api\n    match:\n      path: /api\n    upstream: up\n", okBe.URL))
	sink.addRec(t, rlGet(t, api, "http://example.com/nope", "192.0.2.1:1000", nil))
	sink.addRec(t, rlGet(t, api, "http://example.com/api/%2e%2e/x", "192.0.2.1:1000", nil))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedURL := "http://" + ln.Addr().String()
	_ = ln.Close()
	refused := gwWith(t, "version: 1\n"+listenersYAML+fmt.Sprintf("upstreams:\n  - name: up\n    hosts:\n      - url: %q\nroutes:\n  - name: all\n    upstream: up\n", closedURL))
	sink.addRec(t, rlGet(t, refused, "http://example.com/", "192.0.2.1:1000", nil))

	hang := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(hang.Close)
	timeFake := clock.NewFake(time.Now())
	timed := gwWith(t, "version: 1\n"+listenersYAML+fmt.Sprintf("upstreams:\n  - name: up\n    hosts:\n      - url: %q\n    timeouts:\n      connect: 1h\n      response_header: 1h\nroutes:\n  - name: all\n    upstream: up\n    timeout: 50ms\n", hang.URL), withClock(timeFake))
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		timed.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		done <- rec
	}()
	var timeoutRec *httptest.ResponseRecorder
	eventuallyAdvancing(t, "route timeout response", timeFake, 100*time.Millisecond, func() bool {
		select {
		case timeoutRec = <-done:
			return true
		default:
			return false
		}
	}, nil)
	sink.addRec(t, timeoutRec)

	zero := gwWith(t, "version: 1\n"+listenersYAML+fmt.Sprintf("upstreams:\n  - name: up\n    hosts:\n      - url: %q\n        weight: 0\nroutes:\n  - name: all\n    upstream: up\n", okBe.URL))
	sink.addRec(t, rlGet(t, zero, "http://example.com/", "192.0.2.1:1000", nil))

	cb := newRSBackend(t, n, rsStatusSeq(503))
	cf := clock.NewFake(time.Now())
	cg := n.gateway(t, rsDoc{hosts: []string{cb.URL}, breaker: rsBreaker}.yaml(), withClock(cf))
	cts := n.serve(t, cg)
	cclient := n.client(t, nil)
	for range 3 {
		bkGet(t, cclient, cts.URL+"/", http.StatusServiceUnavailable)
	}
	code, _, cbody := rsGet(t, cclient, cts.URL+"/", http.MethodGet, nil, nil)
	sink.add(t, code, cbody)

	authGW := gwWith(t, auDoc(okBe.URL, amdAuthYAML(auHMAC("HS256"), auHash("example-key")), amdRoute("api", "/api", "    auth:\n      mode: any\n      providers: [jwt1, keys1]\n")))
	sink.addRec(t, rlGet(t, authGW, "http://example.com/api", "192.0.2.1:1000", nil))

	admin := gwWith(t, "version: 1\n"+listenersYAML+purgeAdminYAML(purgeTokA)+fmt.Sprintf("upstreams:\n  - name: up\n    hosts:\n      - url: %q\nroutes:\n  - name: all\n    upstream: up\n", okBe.URL))
	sink.addRec(t, adminReq(admin, http.MethodPost, "/cache/purge", "Bearer wrong-token", `{"all":true}`))
	sink.addRec(t, adminReq(admin, http.MethodGet, "/cache/purge", "Bearer "+purgeTokA, ""))
	sink.addRec(t, adminReq(admin, http.MethodPost, "/cache/purge", "Bearer "+purgeTokA, `{"all":true}`))
	sink.addRec(t, adminReq(admin, http.MethodGet, "/nope", "", ""))

	pf, _, _ := purgeFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	sink.addRec(t, adminReq(pf, http.MethodPost, "/cache/purge", "Bearer "+purgeTokA, "not json"))

	sc := teststore.NewSharedCache(clock.NewFake(cacheStart), 1<<20)
	t.Cleanup(func() { _ = sc.Close() })
	sc.SetOpMode("purge", teststore.Fail, fmt.Errorf("purge down: %w", store.ErrUnavailable))
	scBe := n.serve(t, echoHandler("sc"))
	scg := n.gateway(t, cacheGatewayDoc(scBe.URL, purgeAdminYAML(purgeTokA)+"stores:\n  - name: sc\n    type: shared\ncache:\n  store: sc\n", cacheRouteYAML("r")), cacheSharedOption("shared", sc))
	sink.addRec(t, adminReq(scg, http.MethodPost, "/cache/purge", "Bearer "+purgeTokA, `{"all":true}`))

	closed := gwWith(t, oneRouteDoc("", okBe.URL))
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	sink.addRec(t, rlGet(t, closed, "http://example.com/", "192.0.2.1:1000", nil))

	rbe := n.serve(t, echoHandler("rl"))
	rg := n.gateway(t, rlDoc(rbe.URL, "", stRoute("rl", smRate("", 1, 1))), withClock(rlFake()))
	rlGet(t, rg, "/rl", smClient, nil)
	sink.addRec(t, rlGet(t, rg, "/rl", smClient, nil))

	df := rlFake()
	dshared := stShared(t, df)
	var dcalls atomic.Int64
	dg := n.gateway(t, stDoc(rbe.URL, stStore("shared", ""), stRoute("rl", spRateLimit("deny", 3, spStoreTimeout))),
		withClock(df), stOption(dshared, &dcalls))
	dshared.SetMode(teststore.Fail, fmt.Errorf("store down: %w", store.ErrUnavailable))
	sink.addRec(t, rlGet(t, dg, "/rl", smClient, nil))

	sized := gwWith(t, builtinDoc(okBe.URL, "request_size_limit", "    params:\n      max_bytes: 16\n"))
	srec := httptest.NewRecorder()
	sized.ServeHTTP(srec, httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(strings.Repeat("a", 17))))
	sink.addRec(t, srec)

	boom := func(microgateway.MiddlewareParams) (microgateway.Middleware, error) {
		return func(http.Handler) http.Handler {
			return http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") })
		}, nil
	}
	panicGW := gwWith(t, routeDoc(okBe.URL, "    middlewares: [m]\n")+"middlewares:\n  - name: m\n    type: boom\n", microgateway.WithMiddleware("boom", boom))
	sink.addRec(t, rlGet(t, panicGW, "http://example.com/x", "192.0.2.1:1000", nil))

	cached, _, _ := cacheFixture(t, "", http.Header{"Cache-Control": {"max-age=60"}})
	sink.addRec(t, cacheDo(cached, http.MethodGet, "http://example.com/x", http.Header{"Cache-Control": {"only-if-cached"}}))

	docs := docFiles(t)
	table := documentedErrors(t, docs)
	if len(table) == 0 {
		t.Fatal("no Error responses table in the documentation")
	}
	codes := []string{
		httperr.CodeRouteNotFound,
		httperr.CodeUnauthorized,
		httperr.CodeForbidden,
		httperr.CodePayloadTooLarge,
		httperr.CodeRateLimited,
		httperr.CodeRateLimitUnavailable,
		httperr.CodeBadGateway,
		httperr.CodeCircuitOpen,
		httperr.CodeNoHealthyUpstream,
		httperr.CodeGatewayTimeout,
		httperr.CodeInternal,
		httperr.CodeNotFound,
		httperr.CodeMethodNotAllowed,
		httperr.CodeUnavailable,
		httperr.CodeNotCached,
		httperr.CodeBadRequest,
		httperr.CodeStoreUnavailable,
		httperr.CodeCacheNotConfigured,
		httperr.CodeInvalidPath,
	}
	tableCodes := map[string]bool{}
	for p := range table {
		tableCodes[p.code] = true
		if !sink[p] {
			t.Errorf("documented error %d %s was not observed", p.status, p.code)
		}
	}
	for _, c := range codes {
		if !tableCodes[c] {
			t.Errorf("error code %s has no row in the Error responses table", c)
		}
	}
	for p := range sink {
		if !table[p] {
			t.Errorf("observed error %d %s is not documented", p.status, p.code)
		}
		if !slices.Contains(codes, p.code) {
			t.Errorf("observed error code %s is not an httperr code", p.code)
		}
	}
	for p := range table {
		if !slices.Contains(codes, p.code) {
			t.Errorf("documented error code %s is not an httperr code", p.code)
		}
	}
}
