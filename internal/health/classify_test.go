package health

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
)

type headerTimeoutErr struct{}

func (headerTimeoutErr) Error() string   { return "net/http: timeout awaiting response headers" }
func (headerTimeoutErr) Timeout() bool   { return true }
func (headerTimeoutErr) Temporary() bool { return true }

var _ net.Error = headerTimeoutErr{}

func TestClassify(t *testing.T) {
	live := context.Background()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	deadline, dcancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer dcancel()
	dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}

	cases := []struct {
		name string
		ctx  context.Context
		code int
		err  error
		want Outcome
	}{
		{"200", live, 200, nil, Success},
		{"101", live, 101, nil, Success},
		{"302", live, 302, nil, Success},
		{"404", live, 404, nil, Ignored},
		{"429", live, 429, nil, Ignored},
		{"500", live, 500, nil, Failure},
		{"503", live, 503, nil, Failure},
		{"dial error", live, 0, dialErr, Failure},
		{"response header timeout", live, 0, headerTimeoutErr{}, Failure},
		{"canceled request", canceled, 0, errors.New("context canceled"), Ignored},
		{"deadline request", deadline, 0, context.DeadlineExceeded, Failure},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var resp *http.Response
			if tc.err == nil {
				resp = &http.Response{StatusCode: tc.code}
			}
			if got := Classify(tc.ctx, resp, tc.err); got != tc.want {
				t.Fatalf("Classify = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestOutcomeString(t *testing.T) {
	cases := map[Outcome]string{
		Success:      "success",
		Failure:      "failure",
		Ignored:      "ignored",
		Outcome(200): "unknown",
	}
	for o, want := range cases {
		if got := o.String(); got != want {
			t.Fatalf("Outcome(%d).String() = %q, want %q", uint8(o), got, want)
		}
	}
}
