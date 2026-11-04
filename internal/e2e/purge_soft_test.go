package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/testwait"
)

const (
	psURL          = "http://h/p"
	psLastModified = "Fri, 02 Jan 2026 03:04:05 GMT"
)

// psValidator describes the validator of a stored entry: the origin header that carries it, the request header a
// revalidation sends and the value that header must have. An empty cond is an entry without a validator.
type psValidator struct {
	name   string
	header http.Header
	cond   string
	value  string
}

var (
	psETag       = psValidator{name: "etag", header: http.Header{"Etag": {`"v1"`}}, cond: "If-None-Match", value: `"v1"`}
	psLM         = psValidator{name: "last-modified", header: http.Header{"Last-Modified": {psLastModified}}, cond: "If-Modified-Since", value: psLastModified}
	psNone       = psValidator{name: "none"}
	psValidators = []psValidator{psETag, psLM, psNone}
	psSelectors  = []string{"url", "tag", "all"}
)

// psOrigin returns the origin header of an entry with validator v: a 60s lifetime, the tag tg and v's header.
func psOrigin(v psValidator) http.Header {
	h := http.Header{"Cache-Control": {"max-age=60"}, "Surrogate-Key": {"tg"}}
	for k, vals := range v.header {
		h[k] = vals
	}
	return h
}

// psPurgeJSON returns the purge body that selects typ (url, tag or all) in the hard or the soft mode.
func psPurgeJSON(typ string, soft bool) string {
	switch typ {
	case "url":
		return fmt.Sprintf(`{"urls":[%q],"soft":%t}`, psURL, soft)
	case "tag":
		return fmt.Sprintf(`{"tags":["tg"],"soft":%t}`, soft)
	default:
		return fmt.Sprintf(`{"all":true,"soft":%t}`, soft)
	}
}

// psReply is the reply of a successful single-selector purge of typ with affected entries.
func psReply(typ string, soft bool, affected int) purgeReply {
	return purgeReply{Type: typ, Soft: soft, Accepted: 1, Affected: intp(affected), Scope: "instance"}
}

// psFill sends a GET of psURL, waits for its origin request and fails t unless the response is stored.
func psFill(t *testing.T, g *microgateway.Gateway, o *flightOrigin) {
	t.Helper()
	st := purgeLast(t, purgeGet(t, g, psURL, nil))
	testwait.Recv(t, "fill at origin", o.arrived, nil)
	if !st.Stored {
		t.Fatalf("fill %+v, want stored", st)
	}
}

// psRead returns the body of resp.
func psRead(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// psCounts fails t unless the origin has received calls requests, cond of them conditional.
func psCounts(t *testing.T, o *flightOrigin, calls, cond int) {
	t.Helper()
	gotCalls, gotCond, gotUncond := o.counts()
	if gotCalls != calls || gotCond != cond || gotUncond != calls-cond {
		t.Fatalf("origin requests %d (conditional %d, unconditional %d), want %d (conditional %d)", gotCalls, gotCond, gotUncond, calls, cond)
	}
}

// psNotStale fails t when st says the response was served from a stale window.
func psNotStale(t *testing.T, st httpcache.Status) {
	t.Helper()
	if st.Detail == "stale-while-revalidate" || st.Detail == "stale-if-error" {
		t.Fatalf("status %+v serves a stale entry", st)
	}
}

// psPurged returns a gateway whose entry for psURL with validator v is soft-purged by a purge of typ.
func psPurged(t *testing.T, typ string, v psValidator) (*microgateway.Gateway, *flightOrigin) {
	t.Helper()
	g, _, origin := purgeFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	origin.set(http.StatusOK, psOrigin(v), "old")
	psFill(t, g, origin)
	rep := purgeOK(t, purgeDo(t, g, purgeTokA, psPurgeJSON(typ, true)))
	if want := psReply(typ, true, 1); !reflect.DeepEqual(rep, want) {
		t.Fatalf("soft %s purge reply = %+v, want %+v", typ, rep, want)
	}
	return g, origin
}

// psNetFixture returns a network with a gateway for psURL whose origin is the returned flightOrigin. With breaker, the
// upstream has the circuit breaker of rsBreaker, which opens after three consecutive failures.
func psNetFixture(t *testing.T, breaker bool) (*memNetwork, *testServer, *microgateway.Gateway, *flightOrigin) {
	t.Helper()
	n := newMemNetwork()
	origin := newFlightOrigin()
	be := n.serve(t, origin)
	doc := cacheGatewayDoc(be.URL, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
	if breaker {
		doc = strings.Replace(doc, "    hosts:\n", "    circuit_breaker:\n"+rsBreaker+"    hosts:\n", 1)
	}
	return n, be, n.gateway(t, doc, withClock(clock.NewFake(cacheStart))), origin
}

func TestPurgeSoft(t *testing.T) {
	t.Parallel()
	for _, typ := range psSelectors {
		for _, v := range psValidators {
			t.Run(typ+"/"+v.name, func(t *testing.T) {
				t.Parallel()
				wantCond := 0
				if v.cond != "" {
					wantCond = 1
				}
				if v.cond != "" {
					t.Run("304", func(t *testing.T) {
						t.Parallel()
						g, origin := psPurged(t, typ, v)
						origin.set(http.StatusNotModified, psOrigin(v), "")
						resp := purgeGet(t, g, psURL, nil)
						st := purgeLast(t, resp)
						body := psRead(t, resp)
						if resp.StatusCode != http.StatusOK || body != "old" {
							t.Fatalf("client response %d %q, want 200 old", resp.StatusCode, body)
						}
						if !st.Hit || st.Fwd != httpcache.FwdStale || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified || st.Detail != "" {
							t.Fatalf("status %+v, want hit with fwd=stale and fwd-status=304", st)
						}
						if got := origin.lastHeader().Get(v.cond); got != v.value {
							t.Fatalf("origin %s = %q, want %q", v.cond, got, v.value)
						}
						psCounts(t, origin, 2, 1)

						later := purgeGet(t, g, psURL, nil)
						laterSt := purgeLast(t, later)
						if laterBody := psRead(t, later); !laterSt.Hit || laterBody != "old" {
							t.Fatalf("later status %+v body %q, want hit with old", laterSt, laterBody)
						}
						psCounts(t, origin, 2, 1)
					})
				}
				t.Run("200", func(t *testing.T) {
					t.Parallel()
					g, origin := psPurged(t, typ, v)
					origin.set(http.StatusOK, psOrigin(v), "new")
					resp := purgeGet(t, g, psURL, nil)
					st := purgeLast(t, resp)
					body := psRead(t, resp)
					if resp.StatusCode != http.StatusOK || body != "new" {
						t.Fatalf("client response %d %q, want 200 new", resp.StatusCode, body)
					}
					if st.Hit || !st.Stored || st.Fwd != httpcache.FwdStale {
						t.Fatalf("status %+v, want a stored fwd=stale miss", st)
					}
					psNotStale(t, st)
					psCounts(t, origin, 2, wantCond)

					later := purgeGet(t, g, psURL, nil)
					laterSt := purgeLast(t, later)
					if laterBody := psRead(t, later); !laterSt.Hit || laterBody != "new" {
						t.Fatalf("later status %+v body %q, want hit with new", laterSt, laterBody)
					}
					psCounts(t, origin, 2, wantCond)
				})
			})
		}
	}

	t.Run("stale windows are not served", func(t *testing.T) {
		t.Parallel()
		g, fake, origin := purgeFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
		header := http.Header{"Cache-Control": {"max-age=60, stale-while-revalidate=600, stale-if-error=600"}, "Etag": {`"v1"`}}
		origin.set(http.StatusOK, header, "old")
		psFill(t, g, origin)
		fake.Advance(61 * time.Second)
		if rep := purgeOK(t, purgeDo(t, g, purgeTokA, psPurgeJSON("url", true))); !reflect.DeepEqual(rep, psReply("url", true, 1)) {
			t.Fatalf("soft purge reply = %+v", rep)
		}

		e := decodeErr(t, purgeGet(t, g, psURL, http.Header{"Cache-Control": {"only-if-cached"}}))
		if e.Status != http.StatusGatewayTimeout || e.Code != httperr.CodeNotCached {
			t.Fatalf("only-if-cached error = %+v, want 504 %q", e, httperr.CodeNotCached)
		}
		psCounts(t, origin, 1, 0)

		origin.set(http.StatusOK, header, "new")
		resp := purgeGet(t, g, psURL, http.Header{"Cache-Control": {"max-stale"}})
		st := purgeLast(t, resp)
		body := psRead(t, resp)
		if body != "new" || st.Hit {
			t.Fatalf("max-stale status %+v body %q, want a forwarded miss with new", st, body)
		}
		psNotStale(t, st)
		psCounts(t, origin, 2, 1)
		if n := staleRequests(t, g, "stale"); n != 0 {
			t.Fatalf("stale requests = %v, want 0", n)
		}
	})

	t.Run("soft then hard removes the entry", func(t *testing.T) {
		t.Parallel()
		g, origin := psPurged(t, "url", psETag)
		rep := purgeOK(t, purgeDo(t, g, purgeTokA, psPurgeJSON("url", false)))
		if rep.Type != "url" || rep.Soft || rep.Accepted != 1 || rep.Scope != "instance" {
			t.Fatalf("hard purge reply = %+v", rep)
		}
		st := purgeLast(t, purgeGet(t, g, psURL, nil))
		if st.Hit || st.Fwd != httpcache.FwdURIMiss {
			t.Fatalf("status after hard purge %+v, want uri-miss", st)
		}
		psCounts(t, origin, 2, 0)
	})

	t.Run("hard then soft changes nothing", func(t *testing.T) {
		t.Parallel()
		g, _, origin := purgeFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
		origin.set(http.StatusOK, psOrigin(psETag), "old")
		psFill(t, g, origin)
		if rep := purgeOK(t, purgeDo(t, g, purgeTokA, psPurgeJSON("url", false))); !reflect.DeepEqual(rep, psReply("url", false, 1)) {
			t.Fatalf("hard purge reply = %+v", rep)
		}
		if rep := purgeOK(t, purgeDo(t, g, purgeTokA, psPurgeJSON("url", true))); !reflect.DeepEqual(rep, psReply("url", true, 0)) {
			t.Fatalf("soft purge after hard reply = %+v", rep)
		}
		st := purgeLast(t, purgeGet(t, g, psURL, nil))
		if st.Hit || st.Fwd != httpcache.FwdURIMiss {
			t.Fatalf("status after hard then soft %+v, want uri-miss", st)
		}
		psCounts(t, origin, 2, 0)
	})

	t.Run("soft purge of an absent entry affects nothing", func(t *testing.T) {
		t.Parallel()
		g, _, _ := purgeFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
		rep := purgeOK(t, purgeDo(t, g, purgeTokA, `{"urls":["http://h/absent"],"soft":true}`))
		if !reflect.DeepEqual(rep, psReply("url", true, 0)) {
			t.Fatalf("absent soft purge reply = %+v", rep)
		}
	})
}

func TestPurgeSoftCoalesced(t *testing.T) {
	t.Parallel()
	g, origin := psPurged(t, "url", psETag)
	origin.set(http.StatusOK, psOrigin(psETag), "new")
	origin.hold()
	const n = 100
	chans := make([]<-chan *httptest.ResponseRecorder, n)
	for i := range chans {
		chans[i] = coalesceAsync(context.Background(), g, psURL, nil)
	}
	testwait.Recv(t, "leader at origin", origin.arrived, nil)
	testwait.Until(t, "99 waiters parked", func() bool {
		return coalesceStats(g).Waiters == n-1
	}, func() string {
		return fmt.Sprintf("coalesce waiters %d", coalesceStats(g).Waiters)
	})
	origin.release()

	collapsed := 0
	for _, ch := range chans {
		rec := coalesceRecv(t, ch)
		if rec.Code != http.StatusOK || rec.Body.String() != "new" {
			t.Fatalf("response %d %q, want 200 new", rec.Code, rec.Body.String())
		}
		if cacheLast(t, rec).Collapsed {
			collapsed++
		}
	}
	if collapsed != n-1 {
		t.Fatalf("collapsed responses = %d, want %d", collapsed, n-1)
	}
	psCounts(t, origin, 2, 1)
}

func TestPurgeSoftUpstreamFailure(t *testing.T) {
	t.Parallel()
	for _, code := range []int{http.StatusInternalServerError, http.StatusServiceUnavailable} {
		t.Run("status "+strconv.Itoa(code), func(t *testing.T) {
			t.Parallel()
			g, origin := psPurged(t, "url", psETag)
			origin.set(code, nil, "fail")
			for i := 1; i <= 2; i++ {
				resp := purgeGet(t, g, psURL, nil)
				st := purgeLast(t, resp)
				if body := psRead(t, resp); resp.StatusCode != code || body != "fail" || st.Hit {
					t.Fatalf("attempt %d: response %d %q status %+v, want %d fail miss", i, resp.StatusCode, body, st, code)
				}
				if got := origin.lastHeader().Get("If-None-Match"); got != `"v1"` {
					t.Fatalf("attempt %d: origin If-None-Match %q, want \"v1\"", i, got)
				}
				psCounts(t, origin, 1+i, i)
			}
		})
	}

	t.Run("connection failure", func(t *testing.T) {
		t.Parallel()
		n, be, g, origin := psNetFixture(t, false)
		h := psOrigin(psETag)
		h.Set("Connection", "close")
		origin.set(http.StatusOK, h, "old")
		psFill(t, g, origin)
		if rep := purgeOK(t, purgeDo(t, g, purgeTokA, psPurgeJSON("url", true))); !reflect.DeepEqual(rep, psReply("url", true, 1)) {
			t.Fatalf("soft purge reply = %+v", rep)
		}
		n.refuse(t, be)
		for i := 1; i <= 2; i++ {
			before := n.dialCount(be.Addr)
			e := decodeErr(t, purgeGet(t, g, psURL, nil))
			if e.Status != http.StatusBadGateway || e.Code != httperr.CodeBadGateway {
				t.Fatalf("attempt %d: error = %+v, want 502 %q", i, e, httperr.CodeBadGateway)
			}
			if d := n.dialCount(be.Addr) - before; d != 1 {
				t.Fatalf("attempt %d: dials = %d, want 1 revalidation dial", i, d)
			}
		}
	})

	t.Run("open breaker", func(t *testing.T) {
		t.Parallel()
		_, _, g, origin := psNetFixture(t, true)
		origin.set(http.StatusOK, psOrigin(psETag), "old")
		psFill(t, g, origin)
		if rep := purgeOK(t, purgeDo(t, g, purgeTokA, psPurgeJSON("url", true))); !reflect.DeepEqual(rep, psReply("url", true, 1)) {
			t.Fatalf("soft purge reply = %+v", rep)
		}
		origin.set(http.StatusServiceUnavailable, nil, "fail")
		for i := 1; i <= 3; i++ {
			resp := purgeGet(t, g, psURL, nil)
			if body := psRead(t, resp); resp.StatusCode != http.StatusServiceUnavailable || body != "fail" {
				t.Fatalf("attempt %d: response %d %q, want 503 fail", i, resp.StatusCode, body)
			}
			psCounts(t, origin, 1+i, i)
		}
		if st := rsStatus(t, g); st.BreakerState != "open" {
			t.Fatalf("breaker state = %q, want open", st.BreakerState)
		}
		e := decodeErr(t, purgeGet(t, g, psURL, nil))
		if e.Status != http.StatusServiceUnavailable || e.Code != httperr.CodeCircuitOpen {
			t.Fatalf("open breaker error = %+v, want 503 %q", e, httperr.CodeCircuitOpen)
		}
		psCounts(t, origin, 4, 3)
	})
}

func TestPurgeFillRace(t *testing.T) {
	t.Parallel()
	for _, typ := range psSelectors {
		for _, soft := range []bool{false, true} {
			mode := "hard"
			if soft {
				mode = "soft"
			}
			t.Run(typ+"/"+mode, func(t *testing.T) {
				t.Parallel()
				g, _, origin := purgeFixture(t, purgeAdminYAML(purgeTokA), cacheRouteYAML("r"))
				origin.set(http.StatusOK, psOrigin(psETag), "new")
				origin.hold()
				ch := coalesceAsync(context.Background(), g, psURL, nil)
				testwait.Recv(t, "fill at origin", origin.arrived, nil)
				before := psMemEntries(t, g)

				if rep := purgeOK(t, purgeDo(t, g, purgeTokA, psPurgeJSON(typ, soft))); !reflect.DeepEqual(rep, psReply(typ, soft, 0)) {
					t.Fatalf("purge reply during the fill = %+v", rep)
				}
				origin.release()

				rec := coalesceRecv(t, ch)
				st := cacheLast(t, rec)
				if rec.Code != http.StatusOK || rec.Body.String() != "new" {
					t.Fatalf("fill response %d %q, want 200 new", rec.Code, rec.Body.String())
				}
				if st.Hit {
					t.Fatalf("fill status %+v, want a miss", st)
				}
				if refused := rsMetricOr0(t, g, "microgateway_cache_stores_total", "route", "r", "result", "refused"); refused != 1 {
					t.Fatalf("refused stores = %v, want 1", refused)
				}
				if stored := rsMetricOr0(t, g, "microgateway_cache_stores_total", "route", "r", "result", "stored"); stored != 0 {
					t.Fatalf("stored stores = %v, want 0", stored)
				}
				if after := psMemEntries(t, g); after != before || after != 0 {
					t.Fatalf("memory entries before %d after %d, want 0 and 0", before, after)
				}

				next := purgeLast(t, purgeGet(t, g, psURL, nil))
				if next.Hit || next.Fwd != httpcache.FwdURIMiss {
					t.Fatalf("next status %+v, want uri-miss", next)
				}
				psCounts(t, origin, 2, 0)
			})
		}
	}
}

// psMemEntries returns the number of entries of the built-in memory cache of g.
func psMemEntries(t *testing.T, g *microgateway.Gateway) int {
	t.Helper()
	mc := inspect(g).MemCache
	if mc == nil {
		t.Fatal("built-in memory cache state is not exposed")
	}
	return mc.Entries
}
