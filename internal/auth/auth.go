// Package auth authenticates requests with JWTs and API keys, authorizes them against route requirements and manages identity headers. Key sets are kept per gateway instance in memory.
package auth

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/ok0/lychgate/internal/reqctx"
)

type Reason string

const (
	ReasonMissing         Reason = "missing"
	ReasonMalformed       Reason = "malformed"
	ReasonTooLarge        Reason = "too_large"
	ReasonExpired         Reason = "expired"
	ReasonSignature       Reason = "signature"
	ReasonAlgorithm       Reason = "algorithm"
	ReasonClaims          Reason = "claims"
	ReasonUnknownKID      Reason = "unknown_kid"
	ReasonKeysUnavailable Reason = "keys_unavailable"
	ReasonInvalidKey      Reason = "invalid_key"
	ReasonForbidden       Reason = "forbidden"
)

var (
	ErrNoCredentials   = errors.New("auth: no credentials presented")
	ErrInvalid         = errors.New("auth: invalid credentials")
	ErrUnknownKID      = errors.New("auth: unknown key id")
	ErrKeysUnavailable = errors.New("auth: verification keys unavailable")
)

type Error struct {
	Reason Reason
	Err    error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return "auth: " + string(e.Reason)
	}
	return "auth: " + string(e.Reason) + ": " + e.Err.Error()
}

func (e *Error) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrInvalid}
	}
	return []error{ErrInvalid, e.Err}
}

func Fail(reason Reason, err error) error {
	return &Error{Reason: reason, Err: err}
}

func ReasonOf(err error) Reason {
	if err == nil {
		return ""
	}
	if errors.Is(err, ErrNoCredentials) {
		return ReasonMissing
	}
	var ae *Error
	if errors.As(err, &ae) {
		return ae.Reason
	}
	return ReasonMalformed
}

type Result struct {
	Identity reqctx.Identity
	Scopes   []string
	Roles    []string
	Values   map[string]any
	Forward  map[string]string
}

type Authenticator interface {
	Name() string
	Challenge() string
	Authenticate(r *http.Request) (Result, error)
	Credentials() (header, query string)
	ForwardCredentials() bool
}

func forwardValue(v any) (string, bool) {
	var s string
	switch x := v.(type) {
	case nil:
		return "", false
	case string:
		s = x
	case float64:
		s = strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		s = strconv.FormatBool(x)
	default:
		if n, ok := intText(v); ok {
			s = n
			break
		}
		b, err := json.Marshal(v)
		if err != nil {
			return "", false
		}
		s = string(b)
	}
	if s == "" {
		return "", false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return "", false
		}
	}
	return s, true
}

func intText(v any) (string, bool) {
	switch x := v.(type) {
	case int:
		return strconv.FormatInt(int64(x), 10), true
	case int8:
		return strconv.FormatInt(int64(x), 10), true
	case int16:
		return strconv.FormatInt(int64(x), 10), true
	case int32:
		return strconv.FormatInt(int64(x), 10), true
	case int64:
		return strconv.FormatInt(x, 10), true
	case uint:
		return strconv.FormatUint(uint64(x), 10), true
	case uint8:
		return strconv.FormatUint(uint64(x), 10), true
	case uint16:
		return strconv.FormatUint(uint64(x), 10), true
	case uint32:
		return strconv.FormatUint(uint64(x), 10), true
	case uint64:
		return strconv.FormatUint(x, 10), true
	}
	return "", false
}

func stringList(v any) []string {
	switch x := v.(type) {
	case string:
		return strings.Fields(x)
	case []any:
		out := make([]string, 0, len(x))
		for _, e := range x {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return slices.Clone(x)
	}
	return nil
}
