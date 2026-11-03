package config

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

const collisionDocHead = `version: 1
listeners:
  - name: public
    address: ":8080"
upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
  - name: admin
    hosts:
      - url: "http://127.0.0.1:9002"
routes:
`

type collisionWant struct {
	path, route, msg string
}

type collisionRow struct {
	name   string
	routes string
	want   []collisionWant
}

func collisionMsg(named, other string, idx int, field string) string {
	return fmt.Sprintf("route %q can serve the same cached URLs as route %q (routes[%d].cache) but differs in %s; such routes must be identical except name, match.headers and priority", named, other, idx, field)
}

func collisionRouteLine(t *testing.T, doc, route string) int {
	t.Helper()
	lines := strings.Split(doc, "\n")
	start := -1
	for i, l := range lines {
		if strings.TrimSpace(l) == "- name: "+route {
			start = i
			break
		}
	}
	if start < 0 {
		t.Fatalf("route %q not found in document", route)
	}
	for i := start; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) == "cache:" {
			return i + 1
		}
	}
	t.Fatalf("route %q has no cache key", route)
	return 0
}

func collisionRows() []collisionRow {
	return []collisionRow{
		{
			name: "header split differing upstream",
			routes: `  - name: a-api
    match:
      path: /api
      headers:
        X-Version: "1"
    upstream: users
    cache:
      enabled: true
  - name: b-api
    match:
      path: /api
      headers:
        X-Version: "2"
    upstream: admin
    cache:
      enabled: true
`,
			want: []collisionWant{
				{"routes[1].cache", "b-api", collisionMsg("b-api", "a-api", 0, "upstream")},
				{"routes[0].cache", "a-api", collisionMsg("a-api", "b-api", 1, "upstream")},
			},
		},
		{
			name: "identical apart from name headers and priority",
			routes: `  - name: a-api
    match:
      path: /api
      headers:
        X-Version: "1"
    upstream: users
    cache:
      enabled: true
  - name: b-api
    priority: 5
    match:
      path: /api
      headers:
        X-Version: "2"
    upstream: users
    cache:
      enabled: true
`,
		},
		{
			name: "methods GET and HEAD",
			routes: `  - name: a-api
    match:
      path: /api
      methods: [GET]
      headers:
        X-Version: "1"
    upstream: users
    cache:
      enabled: true
  - name: b-api
    match:
      path: /api
      methods: [HEAD]
    upstream: admin
    cache:
      enabled: true
`,
		},
		{
			name: "methods POST and GET",
			routes: `  - name: a-api
    match:
      path: /api
      methods: [POST]
      headers:
        X-Version: "1"
    upstream: users
    cache:
      enabled: true
  - name: b-api
    match:
      path: /api
      methods: [GET]
    upstream: admin
    cache:
      enabled: true
`,
		},
		{
			name: "disjoint exact paths",
			routes: `  - name: a-api
    match:
      path: /a
      path_type: exact
      headers:
        X-Version: "1"
    upstream: users
    cache:
      enabled: true
  - name: b-api
    match:
      path: /b
      path_type: exact
    upstream: admin
    cache:
      enabled: true
`,
		},
		{
			name: "prefix and pattern overlapping",
			routes: `  - name: a-api
    match:
      path: /a
      headers:
        X-Version: "1"
    upstream: users
    cache:
      enabled: true
  - name: b-api
    match:
      path: /a/:id
      path_type: pattern
    upstream: admin
    cache:
      enabled: true
`,
			want: []collisionWant{
				{"routes[1].cache", "b-api", collisionMsg("b-api", "a-api", 0, "match.path")},
				{"routes[0].cache", "a-api", collisionMsg("a-api", "b-api", 1, "match.path")},
			},
		},
		{
			name: "exact host and wildcard overlapping",
			routes: `  - name: a-api
    match:
      hosts: [a.example]
      path: /api
      headers:
        X-Version: "1"
    upstream: users
    cache:
      enabled: true
  - name: b-api
    match:
      hosts: ["*.example"]
      path: /api
    upstream: admin
    cache:
      enabled: true
`,
			want: []collisionWant{
				{"routes[1].cache", "b-api", collisionMsg("b-api", "a-api", 0, "match.hosts")},
				{"routes[0].cache", "a-api", collisionMsg("a-api", "b-api", 1, "match.hosts")},
			},
		},
		{
			name: "distinct exact hosts",
			routes: `  - name: a-api
    match:
      hosts: [a.example]
      path: /api
      headers:
        X-Version: "1"
    upstream: users
    cache:
      enabled: true
  - name: b-api
    match:
      hosts: [b.example]
      path: /api
    upstream: admin
    cache:
      enabled: true
`,
		},
		{
			name: "only one route cache enabled",
			routes: `  - name: a-api
    match:
      path: /api
      headers:
        X-Version: "1"
    upstream: users
    cache:
      enabled: true
  - name: b-api
    match:
      path: /api
    upstream: admin
`,
		},
		{
			name: "neither route has headers",
			routes: `  - name: a-api
    match:
      path: /api
    upstream: users
    cache:
      enabled: true
  - name: b-api
    match:
      path: /api/:id
      path_type: pattern
    upstream: admin
    cache:
      enabled: true
`,
		},
	}
}

func TestCacheCollisions(t *testing.T) {
	for _, tc := range collisionRows() {
		t.Run(tc.name, func(t *testing.T) {
			doc := collisionDocHead + tc.routes
			_, err := Parse([]byte(doc))
			got := cacheErrSet(t, err)
			want := make([]string, 0, len(tc.want))
			for _, w := range tc.want {
				want = append(want, fmt.Sprintf("%s|%d|%s", w.path, collisionRouteLine(t, doc, w.route), w.msg))
			}
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Fatalf("errors mismatch\n got: %q\nwant: %q", got, want)
			}
		})
	}
}
