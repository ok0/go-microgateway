package admin

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/ok0/go-microgateway/internal/auth"
	"github.com/ok0/go-microgateway/internal/cache"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/store"
)

const purgePath = "/cache/purge"

const maxPurgeBody = 1 << 20

// purgeBody is the success response of the purge endpoint. Affected is omitted when the store did not report it.
type purgeBody struct {
	Type     string `json:"type"`
	Soft     bool   `json:"soft"`
	Accepted int    `json:"accepted"`
	Affected *int   `json:"affected,omitempty"`
	Scope    string `json:"scope"`
}

// servePurge answers POST /cache/purge. The checks run in order: no keys, method, credentials, cache, body.
func servePurge(src Source, logger *slog.Logger, w http.ResponseWriter, r *http.Request) {
	acc := src.PurgeAccess()
	if len(acc.Hashes) == 0 {
		httperr.Write(w, r, http.StatusNotFound, httperr.CodeNotFound, httperr.Message(httperr.CodeNotFound))
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		httperr.Write(w, r, http.StatusMethodNotAllowed, httperr.CodeMethodNotAllowed, httperr.Message(httperr.CodeMethodNotAllowed))
		return
	}
	token, ok := bearer(r)
	if !ok {
		w.Header().Set("WWW-Authenticate", "Bearer")
		httperr.Write(w, r, http.StatusUnauthorized, httperr.CodeUnauthorized, httperr.Message(httperr.CodeUnauthorized))
		logger.Warn("admin request rejected", "path", purgePath, "status", http.StatusUnauthorized, "reason", "missing or malformed bearer token")
		return
	}
	idx, ok := auth.MatchKey(acc.Hashes, token)
	if !ok {
		httperr.Write(w, r, http.StatusForbidden, httperr.CodeForbidden, httperr.Message(httperr.CodeForbidden))
		logger.Warn("admin request rejected", "path", purgePath, "status", http.StatusForbidden, "reason", "unknown key")
		return
	}
	if !acc.Cache {
		httperr.Write(w, r, http.StatusNotFound, httperr.CodeCacheNotConfigured, httperr.Message(httperr.CodeCacheNotConfigured))
		return
	}
	b, err := io.ReadAll(io.LimitReader(r.Body, maxPurgeBody+1))
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		httperr.Write(w, r, http.StatusBadRequest, httperr.CodeBadRequest, "invalid purge request: body could not be read")
		return
	}
	if len(b) > maxPurgeBody {
		httperr.Write(w, r, http.StatusBadRequest, httperr.CodeBadRequest, "invalid purge request: body exceeds 1 MiB")
		return
	}
	req, reason := decodePurge(b)
	if reason != "" {
		httperr.Write(w, r, http.StatusBadRequest, httperr.CodeBadRequest, "invalid purge request: "+reason)
		return
	}
	res, err := src.Purge(r.Context(), req, acc.KeyIDs[idx])
	var re *cache.RequestError
	switch {
	case r.Context().Err() != nil:
		return
	case errors.As(err, &re):
		httperr.Write(w, r, http.StatusBadRequest, httperr.CodeBadRequest, "invalid purge request: "+re.Reason)
		return
	case errors.Is(err, cache.ErrCacheDisabled):
		httperr.Write(w, r, http.StatusNotFound, httperr.CodeCacheNotConfigured, httperr.Message(httperr.CodeCacheNotConfigured))
		return
	case errors.Is(err, store.ErrUnavailable):
		httperr.Write(w, r, http.StatusServiceUnavailable, httperr.CodeStoreUnavailable, httperr.Message(httperr.CodeStoreUnavailable))
		return
	case err != nil:
		httperr.Write(w, r, http.StatusServiceUnavailable, httperr.CodeUnavailable, httperr.Message(httperr.CodeUnavailable))
		return
	}
	out := purgeBody{Type: res.Type, Soft: res.Soft, Accepted: res.Accepted, Scope: res.Scope}
	if res.AffectedKnown {
		n := res.Affected
		out.Affected = &n
	}
	writeValue(w, r, http.StatusOK, out)
}

// bearer returns the token of a Bearer Authorization header, comparing the scheme without case.
func bearer(r *http.Request) (string, bool) {
	vals := r.Header.Values("Authorization")
	if len(vals) == 0 {
		return "", false
	}
	scheme, rest, _ := strings.Cut(vals[0], " ")
	if !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token := strings.TrimLeft(rest, " ")
	if token == "" {
		return "", false
	}
	return token, true
}

// decodePurge parses a purge body into a request. It returns a non-empty reason naming the field when the body is invalid.
// Item-level rules are left to cache.Purger.
func decodePurge(b []byte) (cache.PurgeRequest, string) {
	if len(bytes.TrimSpace(b)) == 0 {
		return cache.PurgeRequest{}, "empty body"
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	var fields map[string]json.RawMessage
	if err := dec.Decode(&fields); err != nil || fields == nil {
		return cache.PurgeRequest{}, "body must be one JSON object"
	}
	if _, err := dec.Token(); err != io.EOF {
		return cache.PurgeRequest{}, "trailing data after the JSON object"
	}
	for name := range fields {
		switch name {
		case "urls", "tags", "all", "soft":
		default:
			return cache.PurgeRequest{}, "unknown field"
		}
	}
	var selectors []string
	for _, name := range []string{"urls", "tags", "all"} {
		if _, ok := fields[name]; ok {
			selectors = append(selectors, name)
		}
	}
	switch {
	case len(selectors) == 0:
		return cache.PurgeRequest{}, "no selector: set urls, tags or all"
	case len(selectors) > 1:
		return cache.PurgeRequest{}, "more than one selector"
	}

	var req cache.PurgeRequest
	sel := selectors[0]
	raw := fields[sel]
	if isNull(raw) {
		return cache.PurgeRequest{}, sel + " has the wrong type"
	}
	switch sel {
	case "urls", "tags":
		var list []string
		if err := json.Unmarshal(raw, &list); err != nil {
			return cache.PurgeRequest{}, sel + " must be a list of strings"
		}
		if len(list) == 0 {
			return cache.PurgeRequest{}, sel + " is empty"
		}
		if sel == "urls" {
			req.URLs = list
		} else {
			req.Tags = list
		}
	case "all":
		var all bool
		if err := json.Unmarshal(raw, &all); err != nil {
			return cache.PurgeRequest{}, "all must be a boolean"
		}
		if !all {
			return cache.PurgeRequest{}, "all must be true"
		}
		req.All = true
	}

	if raw, ok := fields["soft"]; ok {
		if isNull(raw) {
			return cache.PurgeRequest{}, "soft has the wrong type"
		}
		var soft bool
		if err := json.Unmarshal(raw, &soft); err != nil {
			return cache.PurgeRequest{}, "soft must be a boolean"
		}
		req.Soft = soft
	}
	return req, ""
}

func isNull(raw json.RawMessage) bool {
	return string(bytes.TrimSpace(raw)) == "null"
}
