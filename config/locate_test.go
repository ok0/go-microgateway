package config

import (
	"errors"
	"strings"
	"testing"
)

const decodeHead = `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
`

const decodeTail = `routes:
  - name: all
    upstream: users
`

func decodeDoc(stores string) string {
	return decodeHead + "stores:\n" + stores + decodeTail
}

type wantDecodeErr struct {
	path string
	line int
	msg  string
}

type decodeCase struct {
	name         string
	doc          string
	want         []wantDecodeErr
	forbidden    []string
	forbiddenMsg []string
}

func decodeCases(t *testing.T) []decodeCase {
	t.Helper()
	redisStore := func(extra string) string {
		return "  - name: cache\n    type: redis\n    redis:\n      addresses: [\"127.0.0.1:6379\"]\n" + extra
	}

	flowMap := decodeDoc(redisStore("      password: {a: 1}\n"))

	nested := decodeDoc("  - name: cache\n    type: redis\n    redis: {addresses: [\"h:1\"], password: {a: {b: 1}}}\n")

	block := decodeDoc(redisStore("      password:\n        a: 1\n"))

	flowSeq := decodeDoc(redisStore("      password: [x, y]\n"))

	flowSeqItem := decodeDoc("  - name: cache\n    type: redis\n    redis:\n      addresses: [\"h:1\", {x: 1}]\n")

	repeated := decodeDoc("  - name: cache\n    type: custom\n    guard: {consecutive_failures: 1.5, max_inflight: 1.5}\n")

	disambiguated := decodeDoc("  - name: cache\n    type: redis\n    redis: {addresses: [\"h:1\"], username: abc, db: abc}\n")

	unknownNested := decodeDoc("  - name: cache\n    type: redis\n    redis: {addresses: [\"h:1\"], bogus: 1, tls: {bogus: 2}}\n")

	scalarParams := decodeDoc("  - name: cache\n    type: custom\n    params: \"p-scalar-SECRET\"\n")

	durationFlow := decodeDoc("  - name: cache\n    type: custom\n    guard: {open_timeout: {a: 1}}\n")

	alias := decodeDoc("  - name: first\n    type: custom\n    params: &p {k: v}\n" +
		"  - name: second\n    type: redis\n    redis:\n      addresses: [\"127.0.0.1:6379\"]\n      password: *p\n")

	headerMap := decodeHead + "routes:\n  - name: all\n    upstream: users\n    request_headers:\n      set:\n        X-Key: {a: 1}\n"
	mwScalarParams := decodeHead + "middlewares:\n  - name: mw\n    type: custom\n    params: \"mp-scalar-SECRET\"\n" + decodeTail

	listenerPort := strings.Replace(decodeDoc(redisStore("")), "    address: \":8080\"\n", "    address: \":8080\"\n    port: abc\n", 1)

	return []decodeCase{
		{
			name:      "flow map into Secret",
			doc:       flowMap,
			want:      []wantDecodeErr{{"stores[0].redis.password", lineOf(flowMap, "password: {a: 1}"), "must be a string"}},
			forbidden: []string{"stores[0].redis.password."},
		},
		{
			name:      "nested flow on one line",
			doc:       nested,
			want:      []wantDecodeErr{{"stores[0].redis.password", lineOf(nested, "password: {a: {b: 1}}"), "must be a string"}},
			forbidden: []string{"stores[0].redis", "stores[0].redis.password.a"},
		},
		{
			name:      "block map into Secret",
			doc:       block,
			want:      []wantDecodeErr{{"stores[0].redis.password", lineOf(block, "a: 1"), "must be a string"}},
			forbidden: []string{"stores[0].redis.password."},
		},
		{
			name:      "flow sequence into Secret",
			doc:       flowSeq,
			want:      []wantDecodeErr{{"stores[0].redis.password", lineOf(flowSeq, "password: [x, y]"), "must be a string"}},
			forbidden: []string{"stores[0].redis.password."},
		},
		{
			name: "flow sequence item",
			doc:  flowSeqItem,
			want: []wantDecodeErr{{"stores[0].redis.addresses[1]", lineOf(flowSeqItem, "addresses: [\"h:1\", {x: 1}]"), "cannot unmarshal !!map into string"}},
		},
		{
			name: "repeated identical errors on one line",
			doc:  repeated,
			want: []wantDecodeErr{
				{"stores[0].guard.consecutive_failures", lineOf(repeated, "consecutive_failures: 1.5"), "into int"},
				{"stores[0].guard.max_inflight", lineOf(repeated, "consecutive_failures: 1.5"), "into int"},
			},
		},
		{
			name:      "type disambiguates same-line scalars",
			doc:       disambiguated,
			want:      []wantDecodeErr{{"stores[0].redis.db", lineOf(disambiguated, "username: abc"), "into int"}},
			forbidden: []string{"stores[0].redis.username"},
		},
		{
			name: "unknown keys at two depths on one line",
			doc:  unknownNested,
			want: []wantDecodeErr{
				{"stores[0].redis.bogus", lineOf(unknownNested, "bogus: 1"), `unknown field "bogus"`},
				{"stores[0].redis.tls.bogus", lineOf(unknownNested, "bogus: 1"), `unknown field "bogus"`},
			},
		},
		{
			name: "scalar into SecretParams",
			doc:  scalarParams,
			want: []wantDecodeErr{{"stores[0].params", lineOf(scalarParams, "params: \"p-scalar-SECRET\""), "must be a mapping"}},
		},
		{
			name:         "duration flow mapping",
			doc:          durationFlow,
			want:         []wantDecodeErr{{"stores[0].guard.open_timeout", lineOf(durationFlow, "open_timeout: {a: 1}"), "must be a duration string"}},
			forbidden:    []string{"stores[0].guard.open_timeout."},
			forbiddenMsg: []string{"invalid duration"},
		},
		{
			name:      "alias into Secret",
			doc:       alias,
			want:      []wantDecodeErr{{"stores[1].redis.password", lineOf(alias, "password: *p"), "must be a string"}},
			forbidden: []string{"stores[0].params"},
		},
		{
			name:      "route header value mapping",
			doc:       headerMap,
			want:      []wantDecodeErr{{"routes[0].request_headers.set.X-Key", lineOf(headerMap, "X-Key: {a: 1}"), "must be a string"}},
			forbidden: []string{"routes[0].request_headers.set.X-Key."},
		},
		{
			name: "scalar into middleware params",
			doc:  mwScalarParams,
			want: []wantDecodeErr{{"middlewares[0].params", lineOf(mwScalarParams, "params: \"mp-scalar-SECRET\""), "must be a mapping"}},
		},
		{
			name: "block regression on a listener",
			doc:  listenerPort,
			want: []wantDecodeErr{{"listeners[0].port", lineOf(listenerPort, "port: abc"), `unknown field "port"`}},
		},
	}
}

func hasDecodeError(errs []FieldError, w wantDecodeErr) bool {
	for _, fe := range errs {
		if fe.Path == w.path && fe.Line == w.line && strings.Contains(fe.Msg, w.msg) {
			return true
		}
	}
	return false
}

func TestDecodeErrorPaths(t *testing.T) {
	for _, tc := range decodeCases(t) {
		for _, f := range tc.forbidden {
			if !strings.HasPrefix(f, "stores[") && !strings.HasPrefix(f, "routes[") && !strings.HasPrefix(f, "middlewares[") && !strings.HasPrefix(f, "listeners[") {
				t.Fatalf("%s: forbidden entry %q is not a path; use forbiddenMsg", tc.name, f)
			}
		}
	}
	for _, tc := range decodeCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.doc))
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("Parse() = %v, want *ValidationError", err)
			}
			for _, w := range tc.want {
				if !hasDecodeError(ve.Errors, w) {
					t.Errorf("no error at %s line %d with message containing %q: %v", w.path, w.line, w.msg, ve)
				}
			}
			for _, fe := range ve.Errors {
				for _, f := range tc.forbidden {
					if fe.Path == f || (strings.HasSuffix(f, ".") && strings.HasPrefix(fe.Path, f)) {
						t.Errorf("error at forbidden path %q: %s", fe.Path, fe.Msg)
					}
				}
			}
			for _, f := range tc.forbiddenMsg {
				for _, fe := range ve.Errors {
					if strings.Contains(fe.Msg, f) {
						t.Errorf("error %q at %s carries forbidden message fragment %q", fe.Msg, fe.Path, f)
					}
				}
				if strings.Contains(err.Error(), f) {
					t.Errorf("error text carries forbidden fragment %q: %v", f, err)
				}
			}
		})
	}
}

func TestDecodeErrorMessagesHidePosition(t *testing.T) {
	for _, tc := range decodeCases(t) {
		_, err := Parse([]byte(tc.doc))
		var ve *ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("%s: Parse() = %v, want *ValidationError", tc.name, err)
		}
		for _, fe := range ve.Errors {
			if strings.Contains(fe.Msg, "column ") || strings.Contains(fe.Msg, "type config.") {
				t.Errorf("%s: message %q shows decoder position", tc.name, fe.Msg)
			}
		}
		if strings.Contains(err.Error(), "SECRET") {
			t.Errorf("%s: error text %q carries a document value", tc.name, err)
		}
	}
}
