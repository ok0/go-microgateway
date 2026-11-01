package auth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Fetcher interface {
	FetchKeySet(ctx context.Context, provider, keySetURL string) ([]byte, error)
}

const MaxKeySetBytes = 1 << 20

type httpFetcher struct {
	client *http.Client
	dialer *net.Dialer
}

// With a nil client the only bound is the caller's context (the key set's fetch
// timeout on its clock).
func NewHTTPFetcher(c *http.Client) Fetcher {
	if c != nil {
		return &httpFetcher{client: c}
	}
	d := &net.Dialer{KeepAlive: 30 * time.Second}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = d.DialContext
	t.TLSHandshakeTimeout = 0
	t.IdleConnTimeout = 0
	t.ResponseHeaderTimeout = 0
	t.ExpectContinueTimeout = 0
	t.DisableKeepAlives = true
	return &httpFetcher{client: &http.Client{Transport: t}, dialer: d}
}

func (f *httpFetcher) FetchKeySet(ctx context.Context, _, keySetURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, keySetURL, nil)
	if err != nil {
		return nil, errors.New("jwks request is invalid")
	}
	req.Header.Set("Accept", "application/json")

	resp, err := f.client.Do(req)
	if err != nil {
		var ue *url.Error
		inner := err
		if errors.As(err, &ue) {
			inner = ue.Err
		}
		if errors.Is(inner, context.Canceled) || errors.Is(inner, context.DeadlineExceeded) {
			return nil, fmt.Errorf("jwks request failed: %w", inner)
		}
		return nil, fmt.Errorf("jwks request failed: %s", redact(inner.Error(), keySetURL))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		_, _ = io.CopyN(io.Discard, resp.Body, 4096)
		return nil, fmt.Errorf("jwks endpoint returned status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxKeySetBytes+1))
	if err != nil {
		return nil, errors.New("jwks body read failed")
	}
	if len(data) > MaxKeySetBytes {
		return nil, fmt.Errorf("jwks response exceeds %d bytes", MaxKeySetBytes)
	}
	return data, nil
}

func redact(msg, rawURL string) string {
	msg = strings.ReplaceAll(msg, rawURL, "REDACTED")
	u, err := url.Parse(rawURL)
	if err != nil {
		return msg
	}
	if u.User != nil {
		msg = strings.ReplaceAll(msg, u.User.String(), "REDACTED")
	}
	if u.RawQuery != "" {
		msg = strings.ReplaceAll(msg, u.RawQuery, "REDACTED")
	}
	return msg
}
