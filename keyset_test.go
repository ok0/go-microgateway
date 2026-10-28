package lychgate

import (
	"testing"

	"github.com/ok0/lychgate/config"
)

func TestWithKeySetFetcherNil(t *testing.T) {
	g, err := NewFromYAML([]byte("version: 1\n"), WithKeySetFetcher(nil))
	if err == nil || err.Error() != "lychgate: WithKeySetFetcher: nil fetcher" {
		t.Fatalf("NewFromYAML error = %v, want nil fetcher error", err)
	}
	if g != nil {
		t.Fatal("NewFromYAML returned a gateway with an error")
	}
	g, err = New(&config.Config{}, WithKeySetFetcher(nil))
	if err == nil || err.Error() != "lychgate: WithKeySetFetcher: nil fetcher" {
		t.Fatalf("New error = %v, want nil fetcher error", err)
	}
	if g != nil {
		t.Fatal("New returned a gateway with an error")
	}
}

func TestWithHTTPClientNil(t *testing.T) {
	g, err := NewFromYAML([]byte("version: 1\n"), WithHTTPClient(nil))
	if err == nil || err.Error() != "lychgate: WithHTTPClient: nil client" {
		t.Fatalf("NewFromYAML error = %v, want nil client error", err)
	}
	if g != nil {
		t.Fatal("NewFromYAML returned a gateway with an error")
	}
	g, err = New(&config.Config{}, WithHTTPClient(nil))
	if err == nil || err.Error() != "lychgate: WithHTTPClient: nil client" {
		t.Fatalf("New error = %v, want nil client error", err)
	}
	if g != nil {
		t.Fatal("New returned a gateway with an error")
	}
}
