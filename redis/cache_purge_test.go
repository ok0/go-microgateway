package redis

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/ok0/go-microgateway/store"
)

func ptRun(t *testing.T, s store.Cache, p store.Purge) store.PurgeResult {
	t.Helper()
	r, err := s.Purge(context.Background(), p)
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	return r
}

func ptPurgeFor(kind string, soft bool, key, tag string) store.Purge {
	p := store.Purge{Soft: soft, Retain: time.Hour}
	switch kind {
	case "keys":
		p.Kind, p.Keys = store.PurgeKeys, []string{key}
	case "tags":
		p.Kind, p.Tags = store.PurgeTags, []string{tag}
	default:
		p.Kind = store.PurgeAll
	}
	return p
}

func ptState(t *testing.T, s store.Cache, primary, sec string) string {
	t.Helper()
	l := ckGet(t, s, primary, sec)
	switch {
	case l.Entry == nil:
		return "miss"
	case l.Entry.Stale:
		return "stale"
	default:
		return "fresh"
	}
}

func ptHit(t *testing.T, s store.Cache, primary, sec string) {
	t.Helper()
	if st := ptState(t, s, primary, sec); st != "fresh" {
		t.Fatalf("%s/%s: state %s, want fresh", primary, sec, st)
	}
}

func ptNotFresh(t *testing.T, s store.Cache, primary, sec string) {
	t.Helper()
	if st := ptState(t, s, primary, sec); st == "fresh" {
		t.Fatalf("%s/%s: state fresh after purge", primary, sec)
	}
}

func ptDel(t *testing.T, s *Store, key string) {
	t.Helper()
	if err := s.c.Del(context.Background(), key).Err(); err != nil {
		t.Fatalf("DEL %s: %v", key, err)
	}
}

func TestPurgeCounts(t *testing.T) {
	t.Parallel()
	m := newMini(t)
	mc := &miniClock{m: m, now: miniStart}
	s := cacheStore(t, m.Addr(), 0, "mgtest")
	const retain = time.Hour
	const ttl = time.Minute
	fill := func(primary, sec string, tags ...string) {
		t.Helper()
		if !ckFill(t, s, primary, sec, nil, ckEntry(primary+"/"+sec, tags...), ttl) {
			t.Fatalf("fill %s/%s not stored", primary, sec)
		}
	}
	fill("k1", "a")
	fill("k1", "b")
	fill("k2", "a")
	fill("t1", "a", "x", "y")
	fill("t2", "a", "x")
	fill("t2", "b", "z")
	fill("t3", "a", "X")
	fill("t1", "b", "q")
	if !ckFill(t, s, "e1", "a", nil, ckEntry("e1", "x"), time.Second) {
		t.Fatal("e1 not stored")
	}
	mc.Advance(2 * time.Second)

	check := func(name string, p store.Purge, accepted, affected int) {
		t.Helper()
		r := ptRun(t, s, p)
		if r.Accepted != accepted || r.Affected != affected || !r.AffectedKnown {
			t.Fatalf("%s: got %+v, want accepted %d, affected %d, known", name, r, accepted, affected)
		}
	}
	keysP := func(soft bool, k ...string) store.Purge {
		return store.Purge{Kind: store.PurgeKeys, Keys: k, Soft: soft, Retain: retain}
	}
	tagsP := func(soft bool, tg ...string) store.Purge {
		return store.Purge{Kind: store.PurgeTags, Tags: tg, Soft: soft, Retain: retain}
	}

	check("soft keys", keysP(true, "k1", "k2", "k3"), 3, 2)
	check("soft keys again", keysP(true, "k1", "k2", "k3"), 3, 2)
	check("hard keys k2", keysP(false, "k2"), 1, 1)
	check("hard keys k2 again", keysP(false, "k2"), 1, 0)
	check("hard keys k1", keysP(false, "k1"), 1, 2)
	check("hard keys k1 again", keysP(false, "k1"), 1, 0)
	check("absent key", keysP(false, "nope"), 1, 0)
	if !ckFill(t, s, "k1", "a", nil, ckEntry("k1 fresh"), ttl) {
		t.Fatal("fill at the instant of a hard keys purge not stored")
	}
	ptHit(t, s, "k1", "a")

	check("hard tags x y", tagsP(false, "x", "y"), 2, 2)
	check("hard tags x y again", tagsP(false, "x", "y"), 2, 0)
	ptHit(t, s, "t2", "b")
	ptHit(t, s, "t3", "a")
	ptHit(t, s, "t1", "b")
	check("soft tags z", tagsP(true, "z"), 1, 1)
	check("soft tags z again", tagsP(true, "z"), 1, 1)
	check("hard tags z", tagsP(false, "z"), 1, 1)
	check("soft tags z after hard", tagsP(true, "z"), 1, 0)
	check("hard tags z again", tagsP(false, "z"), 1, 0)

	mc.Advance(time.Second)
	if !ckFill(t, s, "t2", "b", nil, ckEntry("t2 b fresh", "z"), ttl) {
		t.Fatal("fill one second after a tag purge not stored")
	}
	ptHit(t, s, "t2", "b")

	r := ptRun(t, s, store.Purge{Kind: store.PurgeAll, Retain: retain})
	if r.Accepted != 1 || r.Affected != 0 || r.AffectedKnown {
		t.Fatalf("all: got %+v, want accepted 1, affected 0, unknown", r)
	}
	ptNotFresh(t, s, "t2", "b")
	ptNotFresh(t, s, "t3", "a")
	ptNotFresh(t, s, "t1", "b")
	ptNotFresh(t, s, "k1", "a")
}

const ptBigTag = "big"

func ptBigPrimary(i int) string {
	return fmt.Sprintf("big-%04d", i)
}

func ptBigFill(t *testing.T, s *Store, n, shortTTL int) (int, []string) {
	t.Helper()
	total := 0
	var short []string
	for i := 0; i < n; i++ {
		primary := ptBigPrimary(i)
		tags := []string{ptBigTag}
		if i == 0 {
			tags = append(tags, "other")
		}
		ttl := time.Minute
		if i < shortTTL {
			ttl = time.Second
		}
		h := s.primaryKeys(primary).h
		if !ckFill(t, s, primary, "a", nil, ckEntry(primary, tags...), ttl) {
			t.Fatalf("%s/a not stored", primary)
		}
		total++
		if i < shortTTL {
			short = append(short, h+"a")
		}
		if i < 50 {
			if !ckFill(t, s, primary, "b", nil, ckEntry(primary+"/b", ptBigTag), ttl) {
				t.Fatalf("%s/b not stored", primary)
			}
			total++
			if i < shortTTL {
				short = append(short, h+"b")
			}
		}
	}
	return total, short
}

func ptBigFirstPage(s *Store, n, size int) map[string]bool {
	all := []string{"!x"}
	for i := 0; i < n; i++ {
		h := s.primaryKeys(ptBigPrimary(i)).h
		all = append(all, h+"a")
		if i < 50 {
			all = append(all, h+"b")
		}
	}
	slices.Sort(all)
	first := make(map[string]bool)
	for _, f := range all[:min(size, len(all))] {
		first[f] = true
	}
	return first
}

func ptHscanCalls(t *testing.T, p *respProxy, key string) int {
	t.Helper()
	n := 0
	for _, args := range p.Requests() {
		if len(args) < 2 || !strings.EqualFold(args[0], "hscan") || args[1] != key {
			continue
		}
		n++
		if len(args) != 7 || !strings.EqualFold(args[3], "match") || args[4] != "*" ||
			!strings.EqualFold(args[5], "count") || args[6] != "100" {
			t.Fatalf("HSCAN %s: args %q, want MATCH * COUNT 100", key, args)
		}
	}
	return n
}

func ptNoBulkReads(t *testing.T, p *respProxy, keys ...string) {
	t.Helper()
	for _, args := range p.Requests() {
		if len(args) < 2 {
			continue
		}
		switch strings.ToUpper(args[0]) {
		case "HGETALL", "SMEMBERS":
			if slices.Contains(keys, args[1]) {
				t.Fatalf("client sent %s on tag key %s", args[0], args[1])
			}
		}
	}
}

// miniKey is one key of a miniredis database: its type, its value and its remaining lifetime.
type miniKey struct {
	typ  string
	str  string
	hash map[string]string
	ttl  time.Duration
}

// miniState is the data of database 0 of a miniredis server and the store scripts loaded on it.
type miniState struct {
	keys    map[string]miniKey
	scripts []*goredis.Script
}

// storeScripts returns every script of ss, and fails the test if ss has a field the list does not name.
func storeScripts(t *testing.T, ss scriptSet) []*goredis.Script {
	t.Helper()
	all := []*goredis.Script{ss.tb, ss.sw, ss.cGet, ss.cPend, ss.cJoin, ss.cCommit, ss.cAbandon, ss.cPurgeKey,
		ss.cPurgeAll, ss.cTagCounter, ss.cTagBucket, ss.cTagMark, ss.cTagPrimary, ss.cTagPrune}
	if n := reflect.TypeOf(ss).NumField(); n != len(all) {
		t.Fatalf("scriptSet has %d fields, storeScripts names %d", n, len(all))
	}
	return all
}

// miniSnapshot reads every key of database 0 of m through the in-process API, and which of scripts are loaded
// (through c, a client connected to m directly).
func miniSnapshot(t *testing.T, m *miniredis.Miniredis, c goredis.UniversalClient, scripts []*goredis.Script) miniState {
	t.Helper()
	st := miniState{keys: map[string]miniKey{}}
	for _, k := range m.Keys() {
		mk := miniKey{typ: m.Type(k), ttl: m.TTL(k)}
		switch mk.typ {
		case "string":
			v, err := m.Get(k)
			if err != nil {
				t.Fatalf("GET %s: %v", k, err)
			}
			mk.str = v
		case "hash":
			fields, err := m.HKeys(k)
			if err != nil {
				t.Fatalf("HKEYS %s: %v", k, err)
			}
			mk.hash = make(map[string]string, len(fields))
			for _, f := range fields {
				mk.hash[f] = m.HGet(k, f)
			}
		default:
			t.Fatalf("%s has type %s; the snapshot reads hash and string keys", k, mk.typ)
		}
		st.keys[k] = mk
	}
	for _, sc := range scripts {
		ok, err := sc.Exists(context.Background(), c).Result()
		if err != nil {
			t.Fatalf("SCRIPT EXISTS: %v", err)
		}
		if len(ok) != 1 {
			t.Fatalf("SCRIPT EXISTS returned %d results, want 1", len(ok))
		}
		if ok[0] {
			st.scripts = append(st.scripts, sc)
		}
	}
	return st
}

// miniRestore writes st into m, which must be empty and on the same server time as the server st was read from,
// and loads st's scripts through a client connected to m directly, so no proxy in front of m sees the restore.
func miniRestore(t *testing.T, m *miniredis.Miniredis, st miniState) {
	t.Helper()
	if n := len(m.Keys()); n != 0 {
		t.Fatalf("restore into a server with %d keys, want 0", n)
	}
	for k, mk := range st.keys {
		switch mk.typ {
		case "string":
			if err := m.Set(k, mk.str); err != nil {
				t.Fatalf("SET %s: %v", k, err)
			}
		case "hash":
			var fv []string
			for f, v := range mk.hash {
				fv = append(fv, f, v)
			}
			m.HSet(k, fv...)
		}
		if mk.ttl > 0 {
			m.SetTTL(k, mk.ttl)
		}
	}
	if len(st.scripts) > 0 {
		c := newTestClient(t, m.Addr(), 0)
		for _, sc := range st.scripts {
			if err := sc.Load(context.Background(), c).Err(); err != nil {
				t.Fatalf("SCRIPT LOAD: %v", err)
			}
		}
	}
}

// ptFixture is the store state a large purge case starts from. It is filled once through the real fill path and
// restored into each case's own server.
type ptFixture struct {
	state miniState
	total int
	short []string
}

// ptBuild fills a fresh server through a proxied store with fill and checks the fill's traffic: no HSCAN and no
// bulk read on the tag keys, and no slot violation (at cleanup). A copy restored into another fresh server must read
// back equal to the filled server.
func ptBuild(t *testing.T, fill func(s *Store) (int, []string)) ptFixture {
	t.Helper()
	m := newMini(t)
	s, p := ckProxied(t, m, "mgtest")
	set, repl, _, _ := s.tagKeys(ptBigTag)
	total, short := fill(s)
	ptNoBulkReads(t, p, set, repl)
	for _, key := range []string{set, repl} {
		if n := ptHscanCalls(t, p, key); n != 0 {
			t.Fatalf("fill sent %d HSCAN calls on %s, want 0", n, key)
		}
	}
	scripts := storeScripts(t, s.sc)
	fx := ptFixture{state: miniSnapshot(t, m, newTestClient(t, m.Addr(), 0), scripts), total: total, short: short}
	m2 := newMini(t)
	miniRestore(t, m2, fx.state)
	if got := miniSnapshot(t, m2, newTestClient(t, m2.Addr(), 0), scripts); !reflect.DeepEqual(got, fx.state) {
		t.Fatal("a restored copy of the fixture differs from the filled server")
	}
	return fx
}

type ptBigCase struct {
	pager *pagerConfig
	size  int
	n     int
	soft  bool
	mut   string
	fx    *ptFixture
}

func ptBigRun(t *testing.T, c ptBigCase) {
	t.Helper()
	ctx := context.Background()
	m := newMini(t)
	mc := &miniClock{m: m, now: miniStart}
	miniRestore(t, m, c.fx.state)
	s, p := ckProxied(t, m, "mgtest")
	p.SetHscanPager(c.pager)
	set, repl, _, _ := s.tagKeys(ptBigTag)
	total, short := c.fx.total, c.fx.short
	wantShort := 0
	if c.mut == "expire" {
		wantShort = 20
	}
	if len(short) != wantShort {
		t.Fatalf("fixture has %d short-lived entries, want %d", len(short), wantShort)
	}
	first := ptBigFirstPage(s, c.n, c.size)
	want := total
	for _, sh := range short {
		if !first[sh] {
			want--
		}
	}

	var tokOld store.FillToken
	if c.mut == "refusedFill" {
		tokOld = ckGet(t, s, "late", "a").Token
	}
	var lens [2]int
	if c.mut == "" {
		for i, key := range []string{set, repl} {
			n, err := s.c.HLen(ctx, key).Result()
			if err != nil {
				t.Fatalf("HLEN %s: %v", key, err)
			}
			lens[i] = int(n)
		}
	}

	steps, acted := 0, false
	s.hook = func(hp hookPoint) {
		if hp != hookPurgeStep {
			return
		}
		steps++
		if c.mut == "down" {
			if steps == 2 {
				p.Down()
			}
			return
		}
		if c.mut == "" || acted || steps != 4 {
			return
		}
		acted = true
		switch c.mut {
		case "replicaGone":
			ptDel(t, s, repl)
		case "setGone":
			ptDel(t, s, set)
		case "refusedFill":
			stored, err := s.Put(ctx, tokOld, "late", "a", nil, ckEntry("late", ptBigTag), time.Minute)
			if err != nil || stored {
				t.Errorf("fill with a token from before the purge: stored %t, err %v", stored, err)
			}
		case "expire":
			mc.Advance(2 * time.Second)
		}
	}

	tagP := store.Purge{Kind: store.PurgeTags, Tags: []string{ptBigTag}, Soft: c.soft, Retain: time.Hour}
	res, err := s.Purge(ctx, tagP)
	s.hook = nil
	if c.mut == "down" {
		if !errors.Is(err, store.ErrUnavailable) {
			t.Fatalf("purge with the proxy down: %v, want ErrUnavailable", err)
		}
		if err := p.Up(); err != nil {
			t.Fatal(err)
		}
		res, err = s.Purge(ctx, tagP)
	}
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if res.Accepted != 1 || !res.AffectedKnown || res.Affected != want {
		t.Fatalf("Purge: got %+v, want accepted 1, affected %d, known", res, want)
	}

	if c.mut == "" {
		for i, key := range []string{set, repl} {
			calls := ptHscanCalls(t, p, key)
			if calls < 2 || calls != (lens[i]+c.size-1)/c.size {
				t.Fatalf("%s: %d HSCAN calls over %d fields with page %d", key, calls, lens[i], c.size)
			}
		}
		ptNoBulkReads(t, p, set, repl)
	}
	if c.mut == "" || c.mut == "expire" {
		wantLen := int64(total + 1 - len(short))
		for _, key := range []string{set, repl} {
			got, err := s.c.HLen(ctx, key).Result()
			if err != nil || got != wantLen {
				t.Fatalf("%s: HLEN %d (%v), want %d", key, got, err, wantLen)
			}
		}
	}

	for i := 0; i < c.n; i++ {
		ptNotFresh(t, s, ptBigPrimary(i), "a")
		if i < 50 {
			ptNotFresh(t, s, ptBigPrimary(i), "b")
		}
	}
	if c.mut == "refusedFill" {
		ptNotFresh(t, s, "late", "a")
	}
}

func ptDownFill(t *testing.T, s *Store) int {
	t.Helper()
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("kd-%04d", i)
		if !ckFill(t, s, k, "a", nil, ckEntry(k), time.Minute) {
			t.Fatalf("%s not stored", k)
		}
	}
	return 1000
}

func ptDownRun(t *testing.T, fx *ptFixture, all, soft bool) {
	t.Helper()
	ctx := context.Background()
	m := newMini(t)
	miniRestore(t, m, fx.state)
	s, p := ckProxied(t, m, "mgtest")
	keys := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		keys = append(keys, fmt.Sprintf("kd-%04d", i))
	}
	pur := store.Purge{Kind: store.PurgeKeys, Keys: keys, Soft: soft, Retain: time.Hour}
	failAt := 2
	if all {
		pur = store.Purge{Kind: store.PurgeAll, Soft: soft, Retain: time.Hour}
		failAt = 1
	}
	steps := 0
	s.hook = func(hp hookPoint) {
		if hp != hookPurgeStep {
			return
		}
		steps++
		if steps == failAt {
			p.Down()
		}
	}
	_, err := s.Purge(ctx, pur)
	s.hook = nil
	if !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("purge with the proxy down: %v, want ErrUnavailable", err)
	}
	if err := p.Up(); err != nil {
		t.Fatal(err)
	}
	res, err := s.Purge(ctx, pur)
	if err != nil {
		t.Fatalf("repeat purge: %v", err)
	}
	if all {
		if res.Accepted != 1 || res.AffectedKnown {
			t.Fatalf("repeat all purge: got %+v", res)
		}
	} else {
		want := 1000
		if !soft {
			want = 800
		}
		if res.Accepted != 1000 || !res.AffectedKnown || res.Affected != want {
			t.Fatalf("repeat keys purge: got %+v, want affected %d", res, want)
		}
	}
	for _, k := range keys {
		ptNotFresh(t, s, k, "a")
	}
}

func TestPurgeLargeTag(t *testing.T) {
	t.Parallel()
	big := ptBuild(t, func(s *Store) (int, []string) { return ptBigFill(t, s, 1100, 0) })
	bigShort := ptBuild(t, func(s *Store) (int, []string) { return ptBigFill(t, s, 1100, 10) })
	small := ptBuild(t, func(s *Store) (int, []string) { return ptBigFill(t, s, 100, 0) })
	down := ptBuild(t, func(s *Store) (int, []string) { return ptDownFill(t, s), nil })
	pagers := []struct {
		name string
		cfg  *pagerConfig
		size int
		n    int
	}{
		{"count", &pagerConfig{}, 100, 1100},
		{"page7", &pagerConfig{Page: 7}, 7, 1100},
		{"page1", &pagerConfig{Page: 1}, 1, 100},
		{"duplicate", &pagerConfig{Duplicate: true}, 100, 1100},
	}
	for _, pg := range pagers {
		fx := &big
		if pg.n == 100 {
			fx = &small
		}
		for _, soft := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/soft=%t", pg.name, soft), func(t *testing.T) {
				t.Parallel()
				ptBigRun(t, ptBigCase{pager: pg.cfg, size: pg.size, n: pg.n, soft: soft, fx: fx})
			})
		}
	}
	for _, mut := range []string{"replicaGone", "setGone", "refusedFill", "expire", "down"} {
		fx := &big
		if mut == "expire" {
			fx = &bigShort
		}
		for _, soft := range []bool{false, true} {
			t.Run(fmt.Sprintf("midscan/%s/soft=%t", mut, soft), func(t *testing.T) {
				t.Parallel()
				ptBigRun(t, ptBigCase{pager: &pagerConfig{}, size: 100, n: 1100, soft: soft, mut: mut, fx: fx})
			})
		}
	}
	for _, all := range []bool{false, true} {
		for _, soft := range []bool{false, true} {
			name := "keys1000"
			if all {
				name = "all1000"
			}
			t.Run(fmt.Sprintf("%s/soft=%t", name, soft), func(t *testing.T) {
				t.Parallel()
				ptDownRun(t, &down, all, soft)
			})
		}
	}
}

type ptLoss struct {
	name   string
	key    func(s *Store) string
	before bool
	joined bool
}

var ptLossKinds = []ptLoss{
	{name: "index", key: func(s *Store) string { return s.primaryKeys("lm-cov").index }, before: true},
	{name: "marker", key: func(s *Store) string { return s.primaryKeys("lm-cov").marker }},
	{name: "tagSet", key: func(s *Store) string { set, _, _, _ := s.tagKeys("t1"); return set }, before: true, joined: true},
	{name: "tagSetReplica", key: func(s *Store) string { _, repl, _, _ := s.tagKeys("t1"); return repl }, before: true, joined: true},
	{name: "tagMarker", key: func(s *Store) string { _, _, m, _ := s.tagKeys("t1"); return m }},
	{name: "tagMarkerReplica", key: func(s *Store) string { _, _, _, r := s.tagKeys("t1"); return r }},
	{name: "hardEpoch", key: func(s *Store) string { return s.primaryKeys("lm-cov").eh }, before: true},
	{name: "softEpoch", key: func(s *Store) string { return s.primaryKeys("lm-cov").es }, before: true},
	{name: "counter", key: func(s *Store) string { tq, _ := s.counterKeys(); return tq }},
	{name: "counterReplica", key: func(s *Store) string { _, tr := s.counterKeys(); return tr }},
}

func ptLossRun(t *testing.T, kind string, soft bool, lk ptLoss, when string) {
	ctx := context.Background()
	m := newMini(t)
	mc := &miniClock{m: m, now: miniStart}
	a := cacheStore(t, m.Addr(), 0, "mgtest")
	b := cacheStore(t, m.Addr(), 0, "mgtest")
	const ttl = time.Minute
	key := lk.key(a)
	if when == "joined" {
		joins := 0
		a.hook = func(hp hookPoint) {
			if hp == hookJoined && joins == 0 {
				joins++
				if err := a.c.Del(ctx, key).Err(); err != nil {
					t.Errorf("DEL %s: %v", key, err)
				}
			}
		}
		if !ckFill(t, a, "lm-cov", "a", nil, ckEntry("a", "t1", "t2"), ttl) {
			t.Fatal("lm-cov/a not stored")
		}
		a.hook = nil
		if joins != 1 {
			t.Fatalf("hookJoined fired %d times, want 1", joins)
		}
	} else if !ckFill(t, a, "lm-cov", "a", nil, ckEntry("a", "t1", "t2"), ttl) {
		t.Fatal("lm-cov/a not stored")
	}
	if !ckFill(t, a, "lm-cov", "b", nil, ckEntry("b", "t1"), ttl) {
		t.Fatal("lm-cov/b not stored")
	}
	if !ckFill(t, a, "lm-keep", "a", nil, ckEntry("keep", "keep"), ttl) {
		t.Fatal("lm-keep/a not stored")
	}
	tokOld := ckGet(t, a, "lm-cov", "c").Token
	if when == "before" {
		ptDel(t, a, key)
	}
	res := ptRun(t, a, ptPurgeFor(kind, soft, "lm-cov", "t1"))
	if when == "after" {
		ptDel(t, a, key)
	}

	wantAffected := 2
	if kind == "keys" && soft {
		wantAffected = 1
	}
	switch {
	case kind == "all":
		if res.Accepted != 1 || res.AffectedKnown || res.Affected != 0 {
			t.Fatalf("all purge: got %+v", res)
		}
	case lk.name == "index" && when != "after":
	default:
		if res.Accepted != 1 || !res.AffectedKnown || res.Affected != wantAffected {
			t.Fatalf("%s purge: got %+v, want affected %d", kind, res, wantAffected)
		}
	}

	ptNotFresh(t, a, "lm-cov", "a")
	ptNotFresh(t, b, "lm-cov", "a")
	ptNotFresh(t, a, "lm-cov", "b")
	ptNotFresh(t, b, "lm-cov", "b")

	stored, err := b.Put(ctx, tokOld, "lm-cov", "c", nil, ckEntry("c", "t1"), ttl)
	if err != nil || stored {
		t.Fatalf("fill with a token from before the purge: stored %t, err %v", stored, err)
	}

	if kind != "all" {
		ptHit(t, a, "lm-keep", "a")
		ptHit(t, b, "lm-keep", "a")
	}

	if kind == "all" {
		if !ckFill(t, b, "lm-late", "a", nil, ckEntry("late"), 2*time.Hour) {
			t.Fatal("entry stored after the purge-all not stored")
		}
		mc.Advance(time.Hour + time.Microsecond)
		ptHit(t, a, "lm-late", "a")
	}
}

func TestPurgeLossMatrix(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"keys", "tags", "all"} {
		for _, soft := range []bool{false, true} {
			for _, lk := range ptLossKinds {
				for _, when := range []string{"before", "after", "joined"} {
					if (when == "before" && !lk.before) || (when == "joined" && !lk.joined) {
						continue
					}
					t.Run(fmt.Sprintf("%s/soft=%t/%s/%s", kind, soft, lk.name, when), func(t *testing.T) {
						ptLossRun(t, kind, soft, lk, when)
					})
				}
			}
		}
	}
}

func ptInterleave(t *testing.T, kind string, soft, third bool, point hookPoint, unrelated bool) {
	ctx := context.Background()
	m := newMini(t)
	a := cacheStore(t, m.Addr(), 0, "mgtest")
	b := cacheStore(t, m.Addr(), 0, "mgtest")
	c := cacheStore(t, m.Addr(), 0, "mgtest")
	filler := b
	if third {
		filler = c
	}
	const primary, sec = "ip", "s"
	const ttl = time.Minute
	p := ptPurgeFor(kind, soft, primary, "ft")
	if unrelated {
		p = ptPurgeFor("keys", soft, "zz", "zz")
	}
	tok := ckGet(t, filler, primary, sec).Token
	seen := make(map[hookPoint]int)
	actions := 0
	act := func() {
		actions++
		if _, err := a.Purge(ctx, p); err != nil {
			t.Fatalf("purge: %v", err)
		}
	}
	filler.hook = func(hp hookPoint) {
		seen[hp]++
		if point != 0 && hp == point && actions == 0 {
			act()
		}
	}
	if point == 0 {
		act()
	}
	stored, err := filler.Put(ctx, tok, primary, sec, nil, ckEntry(primary, "ft"), ttl)
	filler.hook = nil
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if actions != 1 {
		t.Fatalf("purge ran %d times, want 1", actions)
	}
	if want := unrelated || point == hookCommitted; stored != want {
		t.Fatalf("stored %t, want %t", stored, want)
	}
	markerRefused := kind == "tags" && !unrelated && (point == 0 || point == hookPending || point == hookJoined)
	if markerRefused && seen[hookChecked] != 0 {
		t.Fatal("fill refused by the marker check reached hookChecked")
	}
	if unrelated && seen[hookChecked] != 1 {
		t.Fatalf("unrelated purge: hookChecked fired %d times, want 1", seen[hookChecked])
	}
	for _, s := range []*Store{a, b, c} {
		if unrelated {
			ptHit(t, s, primary, sec)
		} else {
			ptNotFresh(t, s, primary, sec)
		}
	}
}

func TestFillInterleavings(t *testing.T) {
	t.Parallel()
	points := []struct {
		name  string
		point hookPoint
	}{
		{"before", 0},
		{"pending", hookPending},
		{"joined", hookJoined},
		{"checked", hookChecked},
		{"committed", hookCommitted},
	}
	for _, kind := range []string{"keys", "tags", "all"} {
		for _, soft := range []bool{false, true} {
			for _, third := range []bool{false, true} {
				for _, pt := range points {
					for _, unrelated := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/soft=%t/third=%t/%s/unrelated=%t", kind, soft, third, pt.name, unrelated), func(t *testing.T) {
							ptInterleave(t, kind, soft, third, pt.point, unrelated)
						})
					}
				}
			}
		}
	}
}

func TestFillTolerance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := newMini(t)
	mc := &miniClock{m: m, now: miniStart}
	a := cacheStore(t, m.Addr(), 0, "mgtest")
	b := cacheStore(t, m.Addr(), 0, "mgtest")
	const ttl = time.Minute

	before := ckGet(t, b, "tp", "pre").Token
	ptRun(t, a, ptPurgeFor("tags", false, "", "tol"))
	stored, err := b.Put(ctx, before, "tp", "pre", nil, ckEntry("pre", "tol"), ttl)
	if err != nil || stored {
		t.Fatalf("token issued before the tag purge: stored %t, err %v", stored, err)
	}
	mc.Advance(time.Second - time.Microsecond)
	if ckFill(t, b, "tp", "s1", nil, ckEntry("s1", "tol"), ttl) {
		t.Fatal("token 1s-1µs after the tag purge stored")
	}
	mc.Advance(time.Microsecond)
	if !ckFill(t, b, "tp", "s2", nil, ckEntry("s2", "tol"), ttl) {
		t.Fatal("token 1s after the tag purge refused")
	}
	ptHit(t, b, "tp", "s2")

	ptRun(t, a, ptPurgeFor("keys", false, "kp", ""))
	if !ckFill(t, b, "kp", "s", nil, ckEntry("kp"), ttl) {
		t.Fatal("token after a key purge at the same instant refused")
	}
	ptHit(t, b, "kp", "s")
	tok := ckGet(t, b, "kp2", "s").Token
	ptRun(t, a, ptPurgeFor("keys", false, "kp2", ""))
	stored, err = b.Put(ctx, tok, "kp2", "s", nil, ckEntry("kp2"), ttl)
	if err != nil || stored {
		t.Fatalf("token before a key purge at the same instant: stored %t, err %v", stored, err)
	}

	tok = ckGet(t, b, "ka", "s").Token
	ptRun(t, a, store.Purge{Kind: store.PurgeAll, Retain: time.Hour})
	stored, err = b.Put(ctx, tok, "ka", "s", nil, ckEntry("ka"), ttl)
	if err != nil || stored {
		t.Fatalf("token before an all purge at the same instant: stored %t, err %v", stored, err)
	}
	if !ckFill(t, b, "ka", "s2", nil, ckEntry("ka2"), ttl) {
		t.Fatal("token after an all purge at the same instant refused")
	}
	ptHit(t, b, "ka", "s2")
}

func TestFillAbandoned(t *testing.T) {
	t.Parallel()
	points := []struct {
		name  string
		point hookPoint
	}{
		{"pending", hookPending},
		{"joined", hookJoined},
		{"checked", hookChecked},
	}
	for _, pt := range points {
		t.Run(pt.name, func(t *testing.T) {
			ptAbandon(t, pt.point)
		})
	}
}

func ptAbandon(t *testing.T, point hookPoint) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := newMini(t)
	mc := &miniClock{m: m, now: miniStart}
	a := cacheStore(t, m.Addr(), 0, "mgtest")
	b := cacheStore(t, m.Addr(), 0, "mgtest")
	const ttl = time.Minute
	fired := 0
	a.hook = func(hp hookPoint) {
		if hp == point && fired == 0 {
			fired++
			cancel()
		}
	}
	tok := ckGet(t, a, "ab", "s").Token
	stored, err := a.Put(ctx, tok, "ab", "s", nil, ckEntry("ab", "abt"), ttl)
	a.hook = nil
	if !errors.Is(err, context.Canceled) || stored {
		t.Fatalf("canceled Put: stored %t, err %v, want canceled", stored, err)
	}
	if fired != 1 {
		t.Fatalf("hook fired %d times, want 1", fired)
	}
	if st := ptState(t, b, "ab", "s"); st != "miss" {
		t.Fatalf("canceled fill: state %s, want miss", st)
	}
	mc.Advance(ttl + time.Millisecond)
	for _, k := range ckKeys(t, b.c, "mgtest") {
		if strings.Contains(k, ":p:") {
			t.Fatalf("pending key %s outlives its ttl", k)
		}
	}
	fields, err := b.c.HKeys(context.Background(), b.primaryKeys("ab").index).Result()
	if err != nil {
		t.Fatalf("HKEYS: %v", err)
	}
	for _, f := range fields {
		if strings.HasPrefix(f, "f:") {
			t.Fatalf("registry field %s outlives the fill ttl", f)
		}
	}
}

func TestPurgeLateAndDisconnected(t *testing.T) {
	t.Parallel()
	sel := func([]string) (string, bool) { return "s", true }
	for _, kind := range []string{"keys", "tags", "all"} {
		for _, soft := range []bool{false, true} {
			t.Run(fmt.Sprintf("late/%s/soft=%t", kind, soft), func(t *testing.T) {
				m := newMini(t)
				a := cacheStore(t, m.Addr(), 0, "mgtest")
				if !ckFill(t, a, "lp", "s", nil, ckEntry("lp", "lt"), time.Minute) {
					t.Fatal("lp not stored")
				}
				ptRun(t, a, ptPurgeFor(kind, soft, "lp", "lt"))
				late := cacheStore(t, m.Addr(), 0, "mgtest")
				ptNotFresh(t, late, "lp", "s")
			})
		}
	}
	for _, kind := range []string{"keys", "tags", "all"} {
		for _, soft := range []bool{false, true} {
			t.Run(fmt.Sprintf("disconnected/%s/soft=%t", kind, soft), func(t *testing.T) {
				ctx := context.Background()
				m := newMini(t)
				a := cacheStore(t, m.Addr(), 0, "mgtest")
				d, p := ckProxied(t, m, "mgtest")
				if !ckFill(t, a, "lp", "s", nil, ckEntry("lp", "lt"), time.Minute) {
					t.Fatal("lp not stored")
				}
				ptHit(t, d, "lp", "s")
				p.Down()
				ptRun(t, a, ptPurgeFor(kind, soft, "lp", "lt"))
				if _, err := d.Get(ctx, "lp", sel); !errors.Is(err, store.ErrUnavailable) {
					t.Fatalf("Get with the proxy down: %v, want ErrUnavailable", err)
				}
				if err := p.Up(); err != nil {
					t.Fatal(err)
				}
				ptNotFresh(t, d, "lp", "s")
			})
		}
	}
}
