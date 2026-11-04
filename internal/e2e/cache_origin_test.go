package e2e

import (
	"io"
	"net/http"
	"sync"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/coalesce"
)

// flightOrigin is an origin whose responses the test sets. It counts its requests, reports each arrival on arrived,
// and holds requests at a gate while one is set. A held request whose context ends is reported on gone.
type flightOrigin struct {
	mu            sync.Mutex
	status        int
	header        http.Header
	body          string
	begin         http.HandlerFunc
	gate          chan struct{}
	arrived       chan struct{}
	gone          chan struct{}
	last          http.Header
	calls         int
	conditional   int
	unconditional int
}

// newFlightOrigin returns a flightOrigin that answers 200 with an empty body until the test sets a response.
func newFlightOrigin() *flightOrigin {
	return &flightOrigin{
		status:  http.StatusOK,
		arrived: make(chan struct{}, 1024),
		gone:    make(chan struct{}, 1024),
	}
}

// set sets the status, header and body of the responses that begin does not replace.
func (o *flightOrigin) set(status int, header http.Header, body string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.status, o.header, o.body = status, header, body
}

// setBegin sets a handler that writes every response in place of the status, header and body; nil restores them.
func (o *flightOrigin) setBegin(h http.HandlerFunc) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.begin = h
}

// hold makes the requests that arrive from now on wait at a gate until release.
func (o *flightOrigin) hold() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.gate = make(chan struct{})
}

// release lets the requests waiting at the gate proceed and stops holding new ones.
func (o *flightOrigin) release() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.gate != nil {
		close(o.gate)
		o.gate = nil
	}
}

// counts returns the number of requests, the conditional ones and the unconditional ones.
func (o *flightOrigin) counts() (calls, cond, uncond int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls, o.conditional, o.unconditional
}

// lastHeader returns the request header of the most recent request.
func (o *flightOrigin) lastHeader() http.Header {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.last
}

func (o *flightOrigin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	o.mu.Lock()
	o.calls++
	o.last = r.Header.Clone()
	if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
		o.conditional++
	} else {
		o.unconditional++
	}
	gate, status, header, body, begin := o.gate, o.status, o.header, o.body, o.begin
	o.mu.Unlock()
	o.arrived <- struct{}{}
	if gate != nil {
		select {
		case <-gate:
		case <-r.Context().Done():
			o.gone <- struct{}{}
			return
		}
	}
	if begin != nil {
		begin(w, r)
		return
	}
	writeOrigin(w, status, header, body)
}

// writeOrigin writes one response with status, header and body. A Date the origin does not set is suppressed.
func writeOrigin(w http.ResponseWriter, status int, header http.Header, body string) {
	if _, ok := header["Date"]; !ok {
		w.Header()["Date"] = nil
	}
	for k, v := range header {
		w.Header()[k] = v
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

// coalesceStats returns the coalescing state of the active generation of g, or the zero Stats when g does not coalesce.
func coalesceStats(g *microgateway.Gateway) coalesce.Stats {
	if cs := inspect(g).Coalesce; cs != nil {
		return *cs
	}
	return coalesce.Stats{}
}
