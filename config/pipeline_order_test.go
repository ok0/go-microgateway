package config

import (
	"slices"
	"strings"
	"testing"
)

func TestCheckPipelineOrder(t *testing.T) {
	t.Parallel()
	const base = "routes[0].pipeline"
	e := func(path, msg string) FieldError {
		return FieldError{Path: path, Msg: msg}
	}
	tests := []struct {
		name   string
		stages []string
		prior  []FieldError
		want   []FieldError
	}{
		{
			name:   "cache first",
			stages: []string{"cache", "auth"},
			want:   []FieldError{e(base+"[0]", msgCacheBeforeAuth)},
		},
		{
			name:   "cache in the middle",
			stages: []string{"timeout", "cache", "auth"},
			want:   []FieldError{e(base+"[1]", msgCacheBeforeAuth)},
		},
		{
			name:   "other stages between",
			stages: []string{"cache", "rate_limit", "circuit_breaker", "timeout", "auth"},
			want:   []FieldError{e(base+"[0]", msgCacheBeforeAuth)},
		},
		{
			name:   "cache after rate_limit before auth",
			stages: []string{"rate_limit", "cache", "auth"},
			want:   []FieldError{e(base+"[1]", msgCacheBeforeAuth)},
		},
		{
			name:   "duplicate cache reported once",
			stages: []string{"cache", "cache", "auth"},
			want:   []FieldError{e(base+"[0]", msgCacheBeforeAuth)},
		},
		{
			name:   "auth after a later duplicate",
			stages: []string{"auth", "cache", "auth"},
			want:   []FieldError{e(base+"[1]", msgCacheBeforeAuth)},
		},
		{
			name:   "auth before cache",
			stages: []string{"auth", "cache"},
		},
		{
			name:   "cache before rate_limit",
			stages: []string{"cache", "rate_limit"},
		},
		{
			name:   "cache before rate_limit after auth",
			stages: []string{"auth", "cache", "rate_limit"},
		},
		{
			name:   "no auth",
			stages: []string{"cache", "timeout", "retry"},
		},
		{
			name:   "no cache",
			stages: []string{"timeout", "auth"},
		},
		{
			name: "empty",
		},
		{
			name:   "user middleware",
			stages: []string{"mw1", "auth", "cache", "timeout"},
		},
		{
			name:   "entry already reported",
			stages: []string{"cache", "auth"},
			prior:  []FieldError{e(base+"[0]", "x")},
			want:   []FieldError{e(base+"[0]", "x")},
		},
		{
			name:   "nested path already reported",
			stages: []string{"cache", "auth"},
			prior:  []FieldError{e(base+"[0].x", "x")},
			want:   []FieldError{e(base+"[0].x", "x")},
		},
		{
			name:   "pipeline itself reported",
			stages: []string{"cache", "auth"},
			prior:  []FieldError{e(base, "x")},
			want:   []FieldError{e(base, "x"), e(base+"[0]", msgCacheBeforeAuth)},
		},
		{
			name:   "other entry reported",
			stages: []string{"cache", "auth"},
			prior:  []FieldError{e(base+"[1]", "x")},
			want:   []FieldError{e(base+"[1]", "x"), e(base+"[0]", msgCacheBeforeAuth)},
		},
		{
			name:   "similar index not nested",
			stages: []string{"cache", "auth"},
			prior:  []FieldError{e(base+"[10]", "x")},
			want:   []FieldError{e(base+"[10]", "x"), e(base+"[0]", msgCacheBeforeAuth)},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ck := &checker{errs: slices.Clone(tc.prior)}
			checkPipelineOrder(ck, base, tc.stages)
			if !slices.Equal(ck.errs, tc.want) {
				t.Errorf("errs = %+v\n want %+v", ck.errs, tc.want)
			}
		})
	}
}

func TestPipelineOrderMessage(t *testing.T) {
	t.Parallel()
	for _, sub := range []string{`"cache"`, `"auth"`, "after"} {
		if !strings.Contains(msgCacheBeforeAuth, sub) {
			t.Errorf("msgCacheBeforeAuth = %q, lacks %s", msgCacheBeforeAuth, sub)
		}
	}
}

func TestPipelineOrderWithDecodeError(t *testing.T) {
	t.Parallel()
	doc := cacheDoc("global:\n  drain_timeout: soon\n", "pipeline:\n  - cache\n  - auth\n")
	_, err := Parse([]byte(doc))
	got := cacheErrSet(t, err)
	want := cacheWantSet(t, doc, []cacheWant{
		{"global.drain_timeout", `invalid duration "soon" (use a value such as "250ms" or "5s")`, "drain_timeout: soon"},
		{"routes[0].pipeline[0]", msgCacheBeforeAuth, "- cache"},
	})
	if !slices.Equal(got, want) {
		t.Errorf("errors = %q\n want %q", got, want)
	}
}
