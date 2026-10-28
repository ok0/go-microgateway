package store

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestLimitValidate(t *testing.T) {
	tests := []struct {
		name    string
		limit   Limit
		wantErr bool
	}{
		{name: "valid token bucket", limit: Limit{Algorithm: TokenBucket, Count: 5, Period: time.Second, Burst: 3}},
		{name: "valid sliding window", limit: Limit{Algorithm: SlidingWindow, Count: 10, Period: time.Minute}},
		{name: "boundary valid token bucket emission interval 1µs", limit: Limit{Algorithm: TokenBucket, Count: 1000, Period: time.Millisecond, Burst: 1}},
		{name: "boundary valid token bucket T*Burst equals 24h", limit: Limit{Algorithm: TokenBucket, Count: 1, Period: time.Hour, Burst: 24}},
		{name: "boundary valid sliding window period 24h", limit: Limit{Algorithm: SlidingWindow, Count: 1, Period: 24 * time.Hour}},

		{name: "invalid algorithm zero", limit: Limit{Algorithm: Algorithm(0), Count: 1, Period: time.Second, Burst: 1}, wantErr: true},
		{name: "invalid algorithm three", limit: Limit{Algorithm: Algorithm(3), Count: 1, Period: time.Second, Burst: 1}, wantErr: true},
		{name: "invalid count zero", limit: Limit{Algorithm: TokenBucket, Count: 0, Period: time.Second, Burst: 1}, wantErr: true},
		{name: "invalid count negative", limit: Limit{Algorithm: TokenBucket, Count: -1, Period: time.Second, Burst: 1}, wantErr: true},
		{name: "invalid period zero", limit: Limit{Algorithm: TokenBucket, Count: 1, Period: 0, Burst: 1}, wantErr: true},
		{name: "invalid period negative", limit: Limit{Algorithm: TokenBucket, Count: 1, Period: -time.Second, Burst: 1}, wantErr: true},
		{name: "invalid token bucket burst zero", limit: Limit{Algorithm: TokenBucket, Count: 1, Period: time.Second, Burst: 0}, wantErr: true},
		{name: "invalid token bucket burst negative", limit: Limit{Algorithm: TokenBucket, Count: 1, Period: time.Second, Burst: -1}, wantErr: true},
		{name: "invalid emission interval 999ns", limit: Limit{Algorithm: TokenBucket, Count: 1001, Period: time.Millisecond, Burst: 1}, wantErr: true},
		{name: "invalid sliding window emission interval 999ns", limit: Limit{Algorithm: SlidingWindow, Count: 1001, Period: time.Millisecond}, wantErr: true},
		{name: "invalid token bucket T*Burst exceeds 24h", limit: Limit{Algorithm: TokenBucket, Count: 1, Period: time.Hour, Burst: 25}, wantErr: true},
		{name: "invalid token bucket overflow guard", limit: Limit{Algorithm: TokenBucket, Count: 1, Period: time.Duration(math.MaxInt64), Burst: math.MaxInt64}, wantErr: true},
		{name: "invalid sliding window period exceeds 24h", limit: Limit{Algorithm: SlidingWindow, Count: 1, Period: 24*time.Hour + time.Nanosecond}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.limit.Validate()
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Validate() = nil, want error")
			}
			if !errors.Is(err, ErrInvalidLimit) {
				t.Errorf("Validate() error %v does not wrap ErrInvalidLimit", err)
			}
			if errors.Is(err, ErrUnavailable) {
				t.Errorf("Validate() error %v wraps ErrUnavailable", err)
			}
		})
	}
}

func TestAlgorithmString(t *testing.T) {
	tests := []struct {
		alg  Algorithm
		want string
	}{
		{TokenBucket, "token_bucket"},
		{SlidingWindow, "sliding_window"},
		{Algorithm(0), "Algorithm(0)"},
		{Algorithm(9), "Algorithm(9)"},
	}
	for _, tt := range tests {
		if got := tt.alg.String(); got != tt.want {
			t.Errorf("Algorithm(%d).String() = %q, want %q", uint8(tt.alg), got, tt.want)
		}
	}
}

func TestSentinelsDistinct(t *testing.T) {
	if ErrInvalidLimit == ErrUnavailable {
		t.Fatal("ErrInvalidLimit and ErrUnavailable must be distinct")
	}
	if got := ErrUnavailable.Error(); got != "store: unavailable" {
		t.Errorf("ErrUnavailable.Error() = %q, want %q", got, "store: unavailable")
	}
	if got := ErrInvalidLimit.Error(); got != "store: invalid limit" {
		t.Errorf("ErrInvalidLimit.Error() = %q, want %q", got, "store: invalid limit")
	}
	if CapRateLimit != 1 {
		t.Errorf("CapRateLimit = %d, want 1", CapRateLimit)
	}
}
