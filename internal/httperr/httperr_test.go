package httperr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ok0/lychgate/internal/reqctx"
)

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestWrite(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(reqctx.WithRequestID(req.Context(), "abc"))

	rec := httptest.NewRecorder()
	Write(rec, req, 404, CodeRouteNotFound, "x")

	if rec.Code != 404 {
		t.Errorf("status = %d, want 404", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q", got)
	}
	if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q", got)
	}

	var out struct {
		Error struct {
			Status    int    `json:"status"`
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if out.Error.Status != 404 || out.Error.Code != CodeRouteNotFound || out.Error.Message != "x" || out.Error.RequestID != "abc" {
		t.Errorf("body = %+v", out.Error)
	}

	rec = httptest.NewRecorder()
	Write(rec, req, StatusClientClosed, CodeClientClosed, "x")
	if rec.Body.Len() != 0 {
		t.Errorf("body written for 499: %q", rec.Body.String())
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want unwritten 200", rec.Code)
	}
}

func TestWriteUnavailable(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	Write(rec, req, http.StatusServiceUnavailable, CodeUnavailable, Message(CodeUnavailable))

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"code":"service_unavailable"`) {
		t.Errorf("body = %q, want code service_unavailable", rec.Body.String())
	}
}

func TestMessages(t *testing.T) {
	codes := []string{
		CodeRouteNotFound,
		CodeUnauthorized,
		CodeForbidden,
		CodePayloadTooLarge,
		CodeRateLimited,
		CodeBadGateway,
		CodeCircuitOpen,
		CodeNoHealthyUpstream,
		CodeGatewayTimeout,
		CodeInternal,
		CodeClientClosed,
		CodeNotFound,
		CodeMethodNotAllowed,
		CodeUnavailable,
	}
	for _, code := range codes {
		if msg := Message(code); msg == "" || msg == "error" {
			t.Errorf("Message(%q) = %q, want a specific message", code, msg)
		}
	}
}

func TestFromError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{"canceled", context.Canceled, 499, CodeClientClosed},
		{"wrapped deadline", fmt.Errorf("w: %w", context.DeadlineExceeded), http.StatusGatewayTimeout, CodeGatewayTimeout},
		{"net op error", &net.OpError{Op: "dial", Err: errors.New("refused")}, http.StatusBadGateway, CodeBadGateway},
		{"dial timeout", &net.OpError{Op: "dial", Err: timeoutErr{}}, http.StatusBadGateway, CodeBadGateway},
		{"wrapped dial timeout", fmt.Errorf("w: %w", &net.OpError{Op: "dial", Err: timeoutErr{}}), http.StatusBadGateway, CodeBadGateway},
		{"read timeout", &net.OpError{Op: "read", Err: timeoutErr{}}, http.StatusGatewayTimeout, CodeGatewayTimeout},
		{"net timeout", timeoutErr{}, http.StatusGatewayTimeout, CodeGatewayTimeout},
		{"wrapped no healthy", fmt.Errorf("w: %w", ErrNoHealthyUpstream), http.StatusServiceUnavailable, CodeNoHealthyUpstream},
		{"circuit open", ErrCircuitOpen, http.StatusServiceUnavailable, CodeCircuitOpen},
		{"plain", errors.New("x"), http.StatusBadGateway, CodeBadGateway},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, code := FromError(tt.err)
			if status != tt.wantStatus || code != tt.wantCode {
				t.Errorf("FromError = (%d, %q), want (%d, %q)", status, code, tt.wantStatus, tt.wantCode)
			}
		})
	}
}
