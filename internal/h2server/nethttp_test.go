package h2server_test

import (
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/h2server"
)

func TestOptionsFor(t *testing.T) {
	connState := func(net.Conn, http.ConnState) {}
	logger := log.New(io.Discard, "", 0)
	h2cfg := &http.HTTP2Config{}

	tests := []struct {
		name string
		hs   *http.Server
		want h2server.Options
	}{
		{
			name: "idle timeout set",
			hs: &http.Server{
				ReadTimeout:    4 * time.Second,
				WriteTimeout:   5 * time.Second,
				IdleTimeout:    6 * time.Second,
				MaxHeaderBytes: 4096,
			},
			want: h2server.Options{
				MaxHeaderBytes:      4096,
				MaxHeaderValueCount: 500,
				ReadTimeout:         4 * time.Second,
				WriteTimeout:        5 * time.Second,
				IdleTimeout:         6 * time.Second,
				SendPingTimeout:     4 * time.Second,
			},
		},
		{
			name: "idle timeout falls back to read timeout",
			hs:   &http.Server{ReadTimeout: 3 * time.Second},
			want: h2server.Options{
				MaxHeaderValueCount: 500,
				ReadTimeout:         3 * time.Second,
				IdleTimeout:         3 * time.Second,
				SendPingTimeout:     3 * time.Second,
			},
		},
		{
			name: "explicit fields pass through",
			hs: &http.Server{
				MaxHeaderValueCount:   7,
				DisableClientPriority: true,
				HTTP2:                 h2cfg,
				ConnState:             connState,
				ErrorLog:              logger,
			},
			want: h2server.Options{
				MaxHeaderValueCount:   7,
				DisableClientPriority: true,
				HTTP2:                 h2cfg,
				ConnState:             connState,
				ErrorLog:              logger,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := h2server.OptionsFor(tc.hs)

			if got.HTTP2 != tc.want.HTTP2 {
				t.Errorf("HTTP2 = %p, want %p", got.HTTP2, tc.want.HTTP2)
			}
			if got.ErrorLog != tc.want.ErrorLog {
				t.Errorf("ErrorLog = %p, want %p", got.ErrorLog, tc.want.ErrorLog)
			}
			if (got.ConnState == nil) != (tc.want.ConnState == nil) {
				t.Errorf("ConnState nil = %v, want %v", got.ConnState == nil, tc.want.ConnState == nil)
			} else if got.ConnState != nil &&
				reflect.ValueOf(got.ConnState).Pointer() != reflect.ValueOf(tc.want.ConnState).Pointer() {
				t.Errorf("ConnState is not the server's ConnState hook")
			}

			got.HTTP2, got.ErrorLog, got.ConnState = nil, nil, nil
			want := tc.want
			want.HTTP2, want.ErrorLog, want.ConnState = nil, nil, nil
			if !reflect.DeepEqual(got, want) {
				t.Errorf("OptionsFor = %+v, want %+v", got, want)
			}
		})
	}
}

func TestAttach(t *testing.T) {
	hs := &http.Server{}
	s := h2server.New(nil, h2server.Options{})
	if err := s.Attach(hs); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if len(hs.TLSNextProto) != 2 {
		t.Errorf("TLSNextProto has %d keys, want 2", len(hs.TLSNextProto))
	}
	for _, key := range []string{"h2", "unencrypted_http2"} {
		if _, ok := hs.TLSNextProto[key]; !ok {
			t.Errorf("TLSNextProto[%q] is not set", key)
		}
	}
	if hs.TLSConfig != nil {
		t.Errorf("Attach set hs.TLSConfig")
	}
	if err := s.Attach(hs); err == nil {
		t.Errorf("second Attach returned nil, want an error")
	}
}
