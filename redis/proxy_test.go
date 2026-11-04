package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	goredis "github.com/redis/go-redis/v9"
)

// ptFill writes n fields f0000..f<n-1> with values v0..v<n-1> and returns them.
func ptFill(t *testing.T, c goredis.UniversalClient, key string, n int) map[string]string {
	t.Helper()
	want := make(map[string]string, n)
	fields := make([]any, 0, 2*n)
	for i := 0; i < n; i++ {
		f := fmt.Sprintf("f%04d", i)
		v := "v" + strconv.Itoa(i)
		want[f] = v
		fields = append(fields, f, v)
	}
	if err := c.HSet(context.Background(), key, fields...).Err(); err != nil {
		t.Fatal(err)
	}
	return want
}

// ptHscanAll runs HSCAN with the given COUNT until the cursor returns to 0 and returns every page's flat reply.
func ptHscanAll(t *testing.T, c goredis.UniversalClient, key string, count int64) [][]string {
	t.Helper()
	ctx := context.Background()
	var pages [][]string
	var cursor uint64
	for i := 0; ; i++ {
		if i > 10000 {
			t.Fatal("HSCAN cursor did not return to 0 within 10000 calls")
		}
		page, next, err := c.HScan(ctx, key, cursor, "", count).Result()
		if err != nil {
			t.Fatal(err)
		}
		pages = append(pages, page)
		cursor = next
		if cursor == 0 {
			return pages
		}
	}
}

// ptPairs splits a flat HSCAN reply into fields and values.
func ptPairs(t *testing.T, page []string) ([]string, []string) {
	t.Helper()
	if len(page)%2 != 0 {
		t.Fatalf("HSCAN page has %d elements, want field/value pairs", len(page))
	}
	var fields, values []string
	for i := 0; i < len(page); i += 2 {
		fields = append(fields, page[i])
		values = append(values, page[i+1])
	}
	return fields, values
}

func newPagedClient(t *testing.T, target string, cfg *pagerConfig) (*respProxy, goredis.UniversalClient) {
	t.Helper()
	p := newRespProxy(t, target)
	p.SetHscanPager(cfg)
	return p, newTestClient(t, p.Addr(), 0)
}

func TestRespProxyHscanPager(t *testing.T) {
	ctx := context.Background()

	t.Run("Pages", func(t *testing.T) {
		m := newMini(t)
		_, c := newPagedClient(t, m.Addr(), &pagerConfig{})
		key := "pt:pages"
		want := ptFill(t, c, key, 1050)
		pages := ptHscanAll(t, c, key, 100)
		if len(pages) != 11 {
			t.Fatalf("got %d pages, want 11", len(pages))
		}
		seen := make(map[string]string, len(want))
		var prev string
		for i, page := range pages {
			fields, values := ptPairs(t, page)
			if len(fields) > 100 {
				t.Fatalf("page %d has %d pairs, want at most 100", i, len(fields))
			}
			for j, f := range fields {
				if f <= prev {
					t.Fatalf("page %d field %q does not follow %q in bytewise order", i, f, prev)
				}
				prev = f
				seen[f] = values[j]
			}
		}
		if len(seen) != len(want) {
			t.Fatalf("covered %d fields, want %d", len(seen), len(want))
		}
		for f, v := range want {
			if seen[f] != v {
				t.Fatalf("field %q = %q, want %q", f, seen[f], v)
			}
		}
	})

	t.Run("Page7", func(t *testing.T) {
		m := newMini(t)
		_, c := newPagedClient(t, m.Addr(), &pagerConfig{Page: 7})
		key := "pt:page7"
		want := ptFill(t, c, key, 1050)
		pages := ptHscanAll(t, c, key, 100)
		if len(pages) != (1050+7-1)/7 {
			t.Fatalf("got %d pages, want %d", len(pages), (1050+7-1)/7)
		}
		seen := make(map[string]bool, len(want))
		for i, page := range pages {
			fields, _ := ptPairs(t, page)
			if len(fields) > 7 {
				t.Fatalf("page %d has %d pairs, want at most 7", i, len(fields))
			}
			for _, f := range fields {
				if seen[f] {
					t.Fatalf("field %q returned twice without Duplicate", f)
				}
				seen[f] = true
			}
		}
		if len(seen) != len(want) {
			t.Fatalf("covered %d fields, want %d", len(seen), len(want))
		}
	})

	t.Run("Duplicate", func(t *testing.T) {
		m := newMini(t)
		_, c := newPagedClient(t, m.Addr(), &pagerConfig{Duplicate: true})
		key := "pt:duplicate"
		want := ptFill(t, c, key, 1050)
		pages := ptHscanAll(t, c, key, 100)
		if len(pages) < 2 {
			t.Fatalf("got %d page(s), want more than one", len(pages))
		}
		counts := make(map[string]int, len(want))
		var prevLast string
		for i, page := range pages {
			fields, _ := ptPairs(t, page)
			if i > 0 {
				if len(fields) == 0 || fields[0] != prevLast {
					t.Fatalf("page %d starts with %v, want the previous last field %q", i, fields, prevLast)
				}
			}
			for _, f := range fields {
				counts[f]++
			}
			if len(fields) > 0 {
				prevLast = fields[len(fields)-1]
			}
		}
		if len(counts) != len(want) {
			t.Fatalf("covered %d fields, want %d", len(counts), len(want))
		}
		duplicated := false
		for _, n := range counts {
			if n > 1 {
				duplicated = true
			}
		}
		if !duplicated {
			t.Fatal("no field was returned twice")
		}
	})

	t.Run("MutationBetweenPages", func(t *testing.T) {
		m := newMini(t)
		_, c := newPagedClient(t, m.Addr(), &pagerConfig{})
		key := "pt:mutation"
		want := ptFill(t, c, key, 1050)
		page, cursor, err := c.HScan(ctx, key, 0, "", 100).Result()
		if err != nil {
			t.Fatal(err)
		}
		if cursor == 0 {
			t.Fatal("first page ended the scan")
		}
		counts := make(map[string]int, len(want))
		fields, _ := ptPairs(t, page)
		for _, f := range fields {
			counts[f]++
		}
		var deleted []string
		for i := 500; i < 510; i++ {
			deleted = append(deleted, fmt.Sprintf("f%04d", i))
		}
		if err := c.HDel(ctx, key, deleted...).Err(); err != nil {
			t.Fatal(err)
		}
		if err := c.HSet(ctx, key, "f0050a", "below", "f0800a", "above").Err(); err != nil {
			t.Fatal(err)
		}
		for i := 0; ; i++ {
			if i > 10000 {
				t.Fatal("HSCAN cursor did not return to 0 within 10000 calls")
			}
			page, next, err := c.HScan(ctx, key, cursor, "", 100).Result()
			if err != nil {
				t.Fatal(err)
			}
			fields, _ := ptPairs(t, page)
			for _, f := range fields {
				counts[f]++
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		for _, f := range deleted {
			if counts[f] != 0 {
				t.Fatalf("deleted field %q was returned %d time(s)", f, counts[f])
			}
		}
		if counts["f0050a"] != 0 {
			t.Fatalf("field added below the resume point was returned %d time(s)", counts["f0050a"])
		}
		if counts["f0800a"] != 1 {
			t.Fatalf("field added above the resume point was returned %d time(s), want 1", counts["f0800a"])
		}
		gone := make(map[string]bool, len(deleted))
		for _, f := range deleted {
			gone[f] = true
		}
		for f := range want {
			if gone[f] {
				continue
			}
			if counts[f] != 1 {
				t.Fatalf("field %q present throughout the scan was returned %d times, want 1", f, counts[f])
			}
		}
	})

	t.Run("MissingKey", func(t *testing.T) {
		m := newMini(t)
		_, c := newPagedClient(t, m.Addr(), &pagerConfig{})
		page, cursor, err := c.HScan(ctx, "pt:absent", 0, "", 100).Result()
		if err != nil {
			t.Fatal(err)
		}
		if len(page) != 0 || cursor != 0 {
			t.Fatalf("missing key: page %v cursor %d, want empty page and cursor 0", page, cursor)
		}
	})

	t.Run("WrongtypeRelayed", func(t *testing.T) {
		m := newMini(t)
		_, c := newPagedClient(t, m.Addr(), &pagerConfig{})
		if err := c.Set(ctx, "pt:string", "v", 0).Err(); err != nil {
			t.Fatal(err)
		}
		_, _, err := c.HScan(ctx, "pt:string", 0, "", 100).Result()
		if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
			t.Fatalf("HSCAN on a string: got %v, want WRONGTYPE", err)
		}
	})

	t.Run("MatchUnsupported", func(t *testing.T) {
		m := newMini(t)
		_, c := newPagedClient(t, m.Addr(), &pagerConfig{})
		key := "pt:match"
		ptFill(t, c, key, 10)
		_, _, err := c.HScan(ctx, key, 0, "f*", 100).Result()
		if err == nil || !strings.Contains(err.Error(), "unsupported MATCH") {
			t.Fatalf("HSCAN MATCH f*: got %v, want unsupported MATCH", err)
		}
		if _, _, err := c.HScan(ctx, key, 0, "*", 100).Result(); err != nil {
			t.Fatalf("HSCAN MATCH *: %v", err)
		}
	})

	t.Run("UnknownCursor", func(t *testing.T) {
		m := newMini(t)
		_, c := newPagedClient(t, m.Addr(), &pagerConfig{})
		key := "pt:cursor"
		ptFill(t, c, key, 10)
		_, _, err := c.HScan(ctx, key, 424242, "", 100).Result()
		if err == nil || !strings.Contains(err.Error(), "unknown cursor") {
			t.Fatalf("HSCAN with an unknown cursor: got %v, want unknown cursor", err)
		}
	})
}

func TestRespProxySlotAudit(t *testing.T) {
	ctx := context.Background()
	m := newMini(t)
	p := newRespProxy(t, m.Addr())
	c := newTestClient(t, p.Addr(), 0)

	if keySlot("{a}x") != keySlot("{a}y") {
		t.Fatal("precondition: {a}x and {a}y must share a slot")
	}
	if keySlot("{a}x") == keySlot("{b}y") {
		t.Fatal("precondition: {a}x and {b}y must not share a slot")
	}

	if _, err := c.Eval(ctx, "return 1", []string{"{a}x", "{a}y"}).Result(); err != nil {
		t.Fatal(err)
	}
	if got := p.Violations(); len(got) != 0 {
		t.Fatalf("EVAL with keys in one slot: violations %v, want none", got)
	}

	if _, err := c.Eval(ctx, "return 1", []string{"{a}x", "{b}y"}).Result(); err != nil {
		t.Fatal(err)
	}
	if got := p.Violations(); len(got) != 1 {
		t.Fatalf("EVAL with keys in two slots: violations %v, want exactly one", got)
	}

	if err := c.Del(ctx, "{a}d1", "{b}d2").Err(); err != nil {
		t.Fatal(err)
	}
	if got := p.Violations(); len(got) != 2 {
		t.Fatalf("DEL with keys in two slots: violations %v, want two in total", got)
	}

	if err := c.Del(ctx, "{a}d1", "{a}d2").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, "{a}s1", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, "{b}s2", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, "{b}s2").Result(); err != nil {
		t.Fatal(err)
	}
	if got := p.Violations(); len(got) != 2 {
		t.Fatalf("same-slot DEL and single-key commands: violations %v, want still two", got)
	}
}
