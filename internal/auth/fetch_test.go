package auth

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) {
	return 0, errors.New("connection reset")
}

func respond(status int, body io.Reader) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(body),
	}
}

func fetcherWith(rt rtFunc) Fetcher {
	return NewHTTPFetcher(&http.Client{Transport: rt})
}

func TestFetchKeySet(t *testing.T) {
	const url = "https://u:pw@idp.example/jwks?token=s3cret"
	cases := []struct {
		name    string
		rt      rtFunc
		wantErr string
		wantLen int
	}{
		{
			name: "ok",
			rt: func(r *http.Request) (*http.Response, error) {
				if r.Header.Get("Accept") != "application/json" {
					return nil, errors.New("missing accept header")
				}
				return respond(http.StatusOK, strings.NewReader(`{"keys":[]}`)), nil
			},
			wantLen: len(`{"keys":[]}`),
		},
		{
			name: "non 200 status",
			rt: func(*http.Request) (*http.Response, error) {
				return respond(http.StatusInternalServerError, strings.NewReader("oops")), nil
			},
			wantErr: "status 500",
		},
		{
			name: "body over limit",
			rt: func(*http.Request) (*http.Response, error) {
				return respond(http.StatusOK, bytes.NewReader(make([]byte, MaxKeySetBytes+1))), nil
			},
			wantErr: "exceeds",
		},
		{
			name: "body at limit",
			rt: func(*http.Request) (*http.Response, error) {
				return respond(http.StatusOK, bytes.NewReader(make([]byte, MaxKeySetBytes))), nil
			},
			wantLen: MaxKeySetBytes,
		},
		{
			name: "transport error mentioning the url",
			rt: func(*http.Request) (*http.Response, error) {
				return nil, errors.New("dial " + url + ": refused")
			},
			wantErr: "jwks request failed",
		},
		{
			name: "body read error",
			rt: func(*http.Request) (*http.Response, error) {
				return respond(http.StatusOK, errReader{}), nil
			},
			wantErr: "body read failed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := fetcherWith(tc.rt).FetchKeySet(t.Context(), "idp", url)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
				}
				if strings.Contains(err.Error(), "s3cret") || strings.Contains(err.Error(), "u:pw") {
					t.Fatalf("error leaks url: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("FetchKeySet: %v", err)
			}
			if len(data) != tc.wantLen {
				t.Fatalf("body length = %d, want %d", len(data), tc.wantLen)
			}
		})
	}
}

func TestFetchKeySetPassesContext(t *testing.T) {
	rt := func(r *http.Request) (*http.Response, error) {
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		return respond(http.StatusOK, strings.NewReader("{}")), nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := fetcherWith(rt).FetchKeySet(ctx, "idp", "https://idp.example/jwks")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestFetchKeySetInvalidURL(t *testing.T) {
	_, err := fetcherWith(nil).FetchKeySet(t.Context(), "idp", "://bad")
	if err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("err = %v, want invalid request", err)
	}
}

func TestNewHTTPFetcherDefault(t *testing.T) {
	f, ok := NewHTTPFetcher(nil).(*httpFetcher)
	if !ok || f.client == nil || f.client.Transport == nil {
		t.Fatal("NewHTTPFetcher(nil) did not build a usable client")
	}
}

func TestRedact(t *testing.T) {
	const raw = "https://u:pw@idp.example/jwks?token=s3cret"
	got := redact("get "+raw+" and u:pw and token=s3cret", raw)
	if strings.Contains(got, "s3cret") || strings.Contains(got, "pw") {
		t.Fatalf("redact left secrets: %q", got)
	}
	if got := redact("plain text", "://bad url"); got != "plain text" {
		t.Fatalf("redact with unparsable url = %q", got)
	}
}
