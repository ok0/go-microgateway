package reqctx

import (
	"context"
	"net/http"
	"net/netip"
	"reflect"
	"strings"
	"testing"
)

func TestIdentity(t *testing.T) {
	bg := context.Background()
	if id, ok := IdentityFrom(bg); ok || !reflect.DeepEqual(id, Identity{}) {
		t.Fatalf("IdentityFrom(background) = (%+v, %v), want zero value and false", id, ok)
	}

	want := Identity{
		Provider: "jwt",
		Consumer: "billing",
		Subject:  "user-1",
		Claims:   map[string]any{"scope": "read"},
		Metadata: map[string]string{"tier": "gold"},
	}
	ctx := WithIdentity(bg, want)
	got, ok := IdentityFrom(ctx)
	if !ok {
		t.Fatal("IdentityFrom reported absent after WithIdentity")
	}
	if got.Provider != want.Provider || got.Consumer != want.Consumer || got.Subject != want.Subject ||
		!reflect.DeepEqual(got.Claims, want.Claims) || !reflect.DeepEqual(got.Metadata, want.Metadata) {
		t.Errorf("IdentityFrom = %+v, want %+v", got, want)
	}
}

func TestInfo(t *testing.T) {
	bg := context.Background()
	if got := InfoFrom(bg); got != nil {
		t.Fatalf("InfoFrom(background) = %v, want nil", got)
	}

	p := &Info{Route: "users", Upstream: "u1"}
	ctx := WithInfo(bg, p)
	if got := InfoFrom(ctx); got != p {
		t.Errorf("InfoFrom = %p, want %p", got, p)
	}
}

func TestContextValues(t *testing.T) {
	bg := context.Background()
	if RequestID(bg) != "" || Route(bg) != "" || len(Params(bg)) != 0 ||
		ParamValue(bg, "id") != "" || ClientIP(bg) != (netip.Addr{}) {
		t.Fatal("getters on background context must return zero values")
	}

	ctx := WithRequestID(bg, "rid-1")
	if got := RequestID(ctx); got != "rid-1" {
		t.Errorf("RequestID = %q", got)
	}

	ctx = WithRoute(ctx, "users")
	if got := Route(ctx); got != "users" {
		t.Errorf("Route = %q", got)
	}

	ctx = WithParams(ctx, []Param{{Name: "id", Value: "7"}, {Name: "ver", Value: "v2"}})
	if got := Params(ctx); len(got) != 2 || got[0] != (Param{"id", "7"}) || got[1] != (Param{"ver", "v2"}) {
		t.Errorf("Params = %v", got)
	}
	if got := ParamValue(ctx, "id"); got != "7" {
		t.Errorf("ParamValue(id) = %q", got)
	}
	if got := ParamValue(ctx, "missing"); got != "" {
		t.Errorf("ParamValue(missing) = %q", got)
	}

	ip := netip.MustParseAddr("192.0.2.9")
	ctx = WithClientIP(ctx, ip)
	if got := ClientIP(ctx); got != ip {
		t.Errorf("ClientIP = %v", got)
	}
}

func TestParseTrusted(t *testing.T) {
	tp, err := ParseTrusted([]string{"10.0.0.0/8", "127.0.0.1", "::1"})
	if err != nil {
		t.Fatalf("ParseTrusted: %v", err)
	}
	if !tp.Contains(netip.MustParseAddr("10.9.9.9")) || !tp.Contains(netip.MustParseAddr("127.0.0.1")) || !tp.Contains(netip.MustParseAddr("::1")) {
		t.Error("expected trusted addresses to be contained")
	}

	_, err = ParseTrusted([]string{"nope"})
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Errorf("ParseTrusted(nope) error = %v", err)
	}
}

func TestClientIP(t *testing.T) {
	tp, err := ParseTrusted([]string{"10.0.0.0/8", "127.0.0.1"})
	if err != nil {
		t.Fatalf("ParseTrusted: %v", err)
	}

	tests := []struct {
		name       string
		remoteAddr string
		xff        []string
		want       string
	}{
		{"untrusted peer ignores xff", "203.0.113.5:1000", []string{"1.1.1.1"}, "203.0.113.5"},
		{"trusted peer, untrusted hop", "127.0.0.1:1", []string{"1.1.1.1, 10.0.0.2"}, "1.1.1.1"},
		{"all hops trusted yields leftmost", "127.0.0.1:1", []string{"10.0.0.3"}, "10.0.0.3"},
		{"garbage hop stops walk", "127.0.0.1:1", []string{"garbage, 10.0.0.2"}, "10.0.0.2"},
		{"trusted peer without xff", "127.0.0.1:1", nil, "127.0.0.1"},
		{"multiple xff lines", "127.0.0.1:1", []string{"1.1.1.1", "2.2.2.2"}, "2.2.2.2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, "http://gw.test/", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.RemoteAddr = tt.remoteAddr
			for _, v := range tt.xff {
				req.Header.Add("X-Forwarded-For", v)
			}
			if got := tp.ClientIP(req).String(); got != tt.want {
				t.Errorf("ClientIP = %s, want %s", got, tt.want)
			}
		})
	}
}
