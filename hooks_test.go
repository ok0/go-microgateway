package microgateway

import (
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/hooks"
)

func TestHooksWithClock(t *testing.T) {
	fake := clock.NewFake(time.Unix(1_700_000_000, 0))
	opt, ok := hooks.WithClock(fake).(Option)
	if !ok {
		t.Fatal("hooks.WithClock did not return a microgateway.Option")
	}
	var o options
	opt(&o)
	if o.clk != clock.Clock(fake) {
		t.Fatalf("option clock = %v, want the fake clock", o.clk)
	}
}
