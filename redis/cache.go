package redis

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/ok0/go-microgateway/store"
)

// hookPoint names a point of a cache operation at which a test hook runs.
type hookPoint uint8

const (
	// hookPending runs after the pending fill is written.
	hookPending hookPoint = iota + 1
	// hookJoined runs after the tag registrations of a fill.
	hookJoined
	// hookChecked runs after the tag markers of a fill are checked and the fill is not refused.
	hookChecked
	// hookCommitted runs after the commit of a fill.
	hookCommitted
	// hookSelected runs after the selector of Get returns.
	hookSelected
	// hookPurgeStep runs after each batch or stage of a purge.
	hookPurgeStep
)

// cacheBucketNames holds the names of the cache buckets.
var cacheBucketNames = bucketNames()

// at runs the test hook for p when one is set.
func (s *Store) at(p hookPoint) {
	if s.hook != nil {
		s.hook(p)
	}
}

// scriptCall is one script call of a pipeline.
type scriptCall struct {
	script *goredis.Script
	keys   []string
	args   []any
}

// clock returns args followed by the clock arguments of the store, when it has any.
func (s *Store) clock(args ...any) []any {
	if s.clockArgs != nil {
		args = append(args, s.clockArgs()...)
	}
	return args
}

// pipe runs calls in one pipeline and returns their replies in order. A call that
// fails with NOSCRIPT was never executed; pipe loads its script and sends it once
// more in a second pipeline. Any other error fails the whole call.
func (s *Store) pipe(ctx context.Context, calls []scriptCall) ([]any, error) {
	if len(calls) == 0 {
		return nil, nil
	}
	out := make([]any, len(calls))
	idx := make([]int, len(calls))
	for i := range idx {
		idx[i] = i
	}
	err := s.await(ctx, func(ctx context.Context) error {
		missing, err := s.evalPipe(ctx, calls, idx, out)
		if err != nil || len(missing) == 0 {
			return err
		}
		for _, i := range missing {
			if err := calls[i].script.Load(ctx, s.c).Err(); err != nil {
				return err
			}
		}
		again, err := s.evalPipe(ctx, calls, missing, out)
		if err != nil {
			return err
		}
		if len(again) > 0 {
			return fmt.Errorf("script not loaded")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// evalPipe sends EvalSha for the calls idx in one pipeline, stores their replies in out, and returns the
// indexes of the calls that failed with NOSCRIPT.
func (s *Store) evalPipe(ctx context.Context, calls []scriptCall, idx []int, out []any) ([]int, error) {
	p := s.c.Pipeline()
	cmds := make([]*goredis.Cmd, len(idx))
	for j, i := range idx {
		cmds[j] = calls[i].script.EvalSha(ctx, p, calls[i].keys, calls[i].args...)
	}
	_, _ = p.Exec(ctx)
	var missing []int
	for j, i := range idx {
		v, err := cmds[j].Result()
		if err != nil {
			if goredis.HasErrorPrefix(err, "NOSCRIPT") {
				missing = append(missing, i)
				continue
			}
			return nil, err
		}
		out[i] = v
	}
	return missing, nil
}

// unexpectedReply returns the error for a script reply of operation op that does not have the expected shape.
func unexpectedReply(op string) error {
	return fmt.Errorf("redis: %s: unexpected script reply: %w", op, store.ErrUnavailable)
}

// cacheHeader is the first part of every reply of the get script: the clock reading in microseconds, the
// generation of the primary, its hard and soft epochs and its bucket marker.
type cacheHeader struct {
	us           int64
	g, eh, es, q string
}

// parseCacheHeader splits the reply of a get script into its header and its body.
func parseCacheHeader(v any) (cacheHeader, []any, bool) {
	vals, ok := v.([]any)
	if !ok || len(vals) != 6 {
		return cacheHeader{}, nil, false
	}
	var str [5]string
	for i := range str {
		x, ok := vals[i].(string)
		if !ok {
			return cacheHeader{}, nil, false
		}
		str[i] = x
	}
	us, err := strconv.ParseInt(str[0], 10, 64)
	if err != nil || us < 0 {
		return cacheHeader{}, nil, false
	}
	body, ok := vals[5].([]any)
	if !ok {
		return cacheHeader{}, nil, false
	}
	return cacheHeader{us: us, g: str[1], eh: str[2], es: str[3], q: str[4]}, body, true
}

// getBody is the body of a get script reply.
type getBody struct {
	record bool
	n      string
	live   []string
	found  bool
	sec    string
	stale  string
	st     string
	meta   string
	value  string
}

// parseGetBody parses the body of a get script reply. A body starting with "0" reports no live record. A body
// starting with "1" carries the canonical names, the live secondary keys, and optionally one variant.
func parseGetBody(v any) (getBody, bool) {
	list, ok := v.([]any)
	if !ok || len(list) == 0 {
		return getBody{}, false
	}
	flag, ok := list[0].(string)
	if !ok {
		return getBody{}, false
	}
	if flag == "0" {
		return getBody{}, len(list) == 1
	}
	if flag != "1" || (len(list) != 4 && len(list) != 9) {
		return getBody{}, false
	}
	n, ok := list[1].(string)
	if !ok {
		return getBody{}, false
	}
	rawLive, ok := list[2].([]any)
	if !ok {
		return getBody{}, false
	}
	live := make([]string, 0, len(rawLive))
	for _, x := range rawLive {
		sec, ok := x.(string)
		if !ok {
			return getBody{}, false
		}
		live = append(live, sec)
	}
	b := getBody{record: true, n: n, live: live}
	found, ok := list[3].(string)
	if !ok {
		return getBody{}, false
	}
	switch {
	case found == "0" && len(list) == 4:
		return b, true
	case found == "1" && len(list) == 9:
		var f [5]string
		for i := range f {
			x, ok := list[4+i].(string)
			if !ok {
				return getBody{}, false
			}
			f[i] = x
		}
		b.found = true
		b.sec, b.stale, b.st, b.meta, b.value = f[0], f[1], f[2], f[3], f[4]
		return b, true
	}
	return getBody{}, false
}

// entryOf builds the entry of a found variant. It reports false when the stored time or the metadata is malformed.
func entryOf(b getBody) (*store.Entry, bool) {
	st, err := strconv.ParseInt(b.st, 10, 64)
	if err != nil || st < 0 {
		return nil, false
	}
	status, hdr, tags, age, ok := decodeMeta([]byte(b.meta))
	if !ok {
		return nil, false
	}
	return &store.Entry{
		Status:     status,
		Header:     hdr,
		Body:       []byte(b.value),
		Tags:       tags,
		InitialAge: age,
		StoredAt:   time.UnixMicro(st).UTC(),
		Stale:      b.stale == "1",
	}, true
}

// Get implements store.Cache. It runs one get script, calls sel with the canonical
// names, and runs a second get script for the selected variant only when the first
// one did not return it.
func (s *Store) Get(ctx context.Context, primary string, sel func(varyNames []string) (string, bool)) (store.Lookup, error) {
	if err := ctx.Err(); err != nil {
		return store.Lookup{}, err
	}
	if sel == nil {
		return store.Lookup{}, fmt.Errorf("%w: nil selector", store.ErrInvalidEntry)
	}
	if s.closed.Load() {
		return store.Lookup{}, fmt.Errorf("redis: store is closed: %w", store.ErrUnavailable)
	}
	if primary == "" || len(primary) > store.MaxKeyBytes {
		return store.Lookup{Token: noneToken}, nil
	}
	pk := s.primaryKeys(primary)
	out, err := s.pipe(ctx, []scriptCall{{s.sc.cGet, pk.all(), s.clock("1", "", "")}})
	if err != nil {
		return store.Lookup{}, s.fail(ctx, "get", err)
	}
	h, body, ok := parseCacheHeader(out[0])
	if !ok {
		return store.Lookup{}, unexpectedReply("get")
	}
	now := time.UnixMicro(h.us).UTC()
	tok := encodeToken(fillToken{h: pk.h, t: h.us, g: h.g, eh: h.eh, es: h.es, q: h.q})
	gb, ok := parseGetBody(body)
	if !ok {
		return store.Lookup{}, unexpectedReply("get")
	}
	if !gb.record {
		return store.Lookup{Now: now, Token: tok}, nil
	}
	names := splitNames(gb.n)
	sec, ok := sel(slices.Clone(names))
	s.at(hookSelected)
	if !ok {
		return store.Lookup{VaryNames: names, Now: now, Token: tok}, nil
	}
	if !(gb.found && gb.sec == sec) {
		if !slices.Contains(gb.live, sec) {
			return store.Lookup{VaryNames: names, Now: now, Token: tok}, nil
		}
		out, err := s.pipe(ctx, []scriptCall{{s.sc.cGet, pk.all(), s.clock("2", sec, gb.n)}})
		if err != nil {
			return store.Lookup{}, s.fail(ctx, "get", err)
		}
		h, body, ok := parseCacheHeader(out[0])
		if !ok {
			return store.Lookup{}, unexpectedReply("get")
		}
		now = time.UnixMicro(h.us).UTC()
		tok = encodeToken(fillToken{h: pk.h, t: h.us, g: h.g, eh: h.eh, es: h.es, q: h.q})
		gb, ok = parseGetBody(body)
		if !ok {
			return store.Lookup{}, unexpectedReply("get")
		}
		names = splitNames(gb.n)
		if !gb.found {
			return store.Lookup{VaryNames: names, Now: now, Token: tok}, nil
		}
	}
	e, ok := entryOf(gb)
	if !ok {
		return store.Lookup{VaryNames: names, Now: now, Token: tok}, nil
	}
	return store.Lookup{Entry: e, VaryNames: names, Now: now, Token: tok}, nil
}

// Put implements store.Cache. The steps run in this order: the pending fill is
// written; the fill is registered under each tag; the tag markers are checked; the
// fill is committed into a slot. A refusal at any step leaves no entry behind.
func (s *Store) Put(ctx context.Context, tok store.FillToken, primary, secondary string, varyNames []string, e *store.Entry, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	canon, err := store.ValidateFill(tok, primary, secondary, varyNames, e, ttl)
	if err != nil {
		return false, err
	}
	pk := s.primaryKeys(primary)
	ft, ok := decodeToken(tok)
	if !ok || ft.h != pk.h {
		return false, fmt.Errorf("%w: foreign fill token", store.ErrInvalidEntry)
	}
	if s.closed.Load() {
		return false, fmt.Errorf("redis: store is closed: %w", store.ErrUnavailable)
	}
	if ttl <= 0 {
		return false, nil
	}
	fid := newNonce()
	pend := s.pendingKey(pk, fid)
	ttlUs, ttlRem := splitDur(ttl)
	ttlUsN := int64(ttl / time.Microsecond)
	out, err := s.pipe(ctx, []scriptCall{{s.sc.cPend, []string{pk.index, pk.marker, pk.eh, pk.es, pend},
		s.clock(fid, secondary, joinTags(e.Tags), encodeMeta(e), e.Body, ttlUs, ttlRem, ft.g, ft.eh, ft.es)}})
	if err != nil {
		return false, s.fail(ctx, "put", err)
	}
	word, rep, ok := replyWord(out[0])
	if !ok || (word != "ok" && word != "refused") || (word == "ok" && len(rep) != 1) {
		return false, unexpectedReply("put")
	}
	if word == "refused" {
		return false, nil
	}
	s.at(hookPending)
	at, err := strconv.ParseInt(rep[0], 10, 64)
	if err != nil {
		return false, unexpectedReply("put")
	}
	tags := sortedUnique(e.Tags)
	member := pk.h + secondary
	dlu := strconv.FormatInt(at+ttlUsN, 10)
	allow := strconv.FormatInt(s.allowUs, 10)
	joins := make([]scriptCall, 0, 2*len(tags))
	for _, tag := range tags {
		set, repl, _, _ := s.tagKeys(tag)
		joins = append(joins, scriptCall{s.sc.cJoin, []string{set, repl}, s.clock(member, dlu, ttlRem, allow)})
	}
	if err := s.runJoins(ctx, joins); err != nil {
		return false, s.fail(ctx, "put", err)
	}
	s.at(hookJoined)
	refused, err := s.tagRefused(ctx, tags, ft)
	if err != nil {
		return false, s.fail(ctx, "put", err)
	}
	if refused {
		_, err := s.pipe(ctx, []scriptCall{{s.sc.cAbandon, []string{pk.index, pk.eh, pk.es, pend}, s.clock(fid)}})
		if err != nil {
			return false, s.fail(ctx, "put", err)
		}
		return false, nil
	}
	s.at(hookChecked)
	tokQ := "-"
	if ft.q != "" {
		tokQ = strings.Replace(ft.q, " ", ":", 1)
	}
	keys := []string{pk.index, pk.marker, pk.eh, pk.es, pend}
	keys = append(keys, pk.slots[:]...)
	out, err = s.pipe(ctx, []scriptCall{{s.sc.cCommit, keys,
		s.clock(fid, secondary, strings.Join(canon, ","), ft.g, ft.eh, ft.es, newNonce(), tokQ)}})
	if err != nil {
		return false, s.fail(ctx, "put", err)
	}
	s.at(hookCommitted)
	word, _, ok = replyWord(out[0])
	if !ok || (word != "stored" && word != "refused") {
		return false, unexpectedReply("put")
	}
	return word == "stored", nil
}

// runJoins runs the tag registration calls and checks that each one returns "ok".
func (s *Store) runJoins(ctx context.Context, calls []scriptCall) error {
	out, err := s.pipe(ctx, calls)
	if err != nil {
		return err
	}
	for _, v := range out {
		if x, ok := v.(string); !ok || x != "ok" {
			return unexpectedReply("put")
		}
	}
	return nil
}

// tagRefused reports whether a tag marker of tags refuses a fill with token ft. A marker
// refuses the fill when it was set within PurgeTolerance before the token, when the
// token carries no counter, or when the marker's counter is newer than the token's.
func (s *Store) tagRefused(ctx context.Context, tags []string, ft fillToken) (bool, error) {
	if len(tags) == 0 {
		return false, nil
	}
	tokEra, tokV, _ := splitCounter(ft.q)
	var refused bool
	err := s.await(ctx, func(ctx context.Context) error {
		p := s.c.Pipeline()
		cmds := make([]*goredis.SliceCmd, 0, 2*len(tags))
		for _, tag := range tags {
			_, _, m, r := s.tagKeys(tag)
			cmds = append(cmds, p.HMGet(ctx, m, "t", "q"), p.HMGet(ctx, r, "t", "q"))
		}
		if _, err := p.Exec(ctx); err != nil {
			return err
		}
		for _, c := range cmds {
			vals, err := c.Result()
			if err != nil {
				return err
			}
			mt, mEra, mV, ok := parseMarker(vals)
			if !ok {
				continue
			}
			if ft.q == "" || mt > ft.t-toleranceUs || mEra != tokEra || mV > tokV {
				refused = true
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return refused, nil
}

// parseMarker reads the time and counter of a tag marker from the values of HMGET t q. It reports false when
// either field is absent or malformed.
func parseMarker(vals []any) (us int64, era string, v int64, ok bool) {
	if len(vals) != 2 {
		return 0, "", 0, false
	}
	ts, ok1 := vals[0].(string)
	qs, ok2 := vals[1].(string)
	if !ok1 || !ok2 {
		return 0, "", 0, false
	}
	us, err := strconv.ParseInt(ts, 10, 64)
	if err != nil || us < 0 {
		return 0, "", 0, false
	}
	era, v, ok = splitCounter(qs)
	return us, era, v, ok
}

// splitCounter splits a counter "<era> <v>" into its era and value. It reports false when the text is malformed,
// and then returns an empty era and zero.
func splitCounter(q string) (era string, v int64, ok bool) {
	era, vs, found := strings.Cut(q, " ")
	if !found || !isHex32(era) || !isDigits(vs) {
		return "", 0, false
	}
	v, err := strconv.ParseInt(vs, 10, 64)
	if err != nil {
		return "", 0, false
	}
	return era, v, true
}

// replyWord returns the first element of a script reply and the elements after it. It reports false when the
// reply is not a list whose first element is a string.
func replyWord(v any) (string, []string, bool) {
	list, ok := v.([]any)
	if !ok || len(list) == 0 {
		if s, isStr := v.(string); isStr {
			return s, nil, true
		}
		return "", nil, false
	}
	word, ok := list[0].(string)
	if !ok {
		return "", nil, false
	}
	rest := make([]string, 0, len(list)-1)
	for _, x := range list[1:] {
		s, ok := x.(string)
		if !ok {
			return "", nil, false
		}
		rest = append(rest, s)
	}
	return word, rest, true
}

// sortedUnique returns the distinct values of list in sorted order, in a new slice.
func sortedUnique(list []string) []string {
	out := slices.Clone(list)
	slices.Sort(out)
	return slices.Compact(out)
}

// Purge implements store.Cache.
func (s *Store) Purge(ctx context.Context, p store.Purge) (store.PurgeResult, error) {
	if err := ctx.Err(); err != nil {
		return store.PurgeResult{}, err
	}
	if err := p.Validate(); err != nil {
		return store.PurgeResult{}, err
	}
	if s.closed.Load() {
		return store.PurgeResult{}, fmt.Errorf("redis: store is closed: %w", store.ErrUnavailable)
	}
	retUs, retRem := splitDur(p.Retain)
	nonce := newNonce()
	soft := "0"
	if p.Soft {
		soft = "1"
	}
	switch p.Kind {
	case store.PurgeKeys:
		return s.purgeKeys(ctx, p.Keys, soft, nonce, retUs, retRem)
	case store.PurgeTags:
		return s.purgeTags(ctx, p.Tags, soft, nonce, retUs, retRem)
	default:
		return s.purgeAll(ctx, soft, nonce, retUs, retRem)
	}
}

// purgeKeys marks the primary keys in batches of purgeBatch, one pipeline per batch.
func (s *Store) purgeKeys(ctx context.Context, keys []string, soft, nonce, retUs, retRem string) (store.PurgeResult, error) {
	distinct := sortedUnique(keys)
	affected := 0
	for start := 0; start < len(distinct); start += purgeBatch {
		end := min(start+purgeBatch, len(distinct))
		calls := make([]scriptCall, 0, end-start)
		for _, k := range distinct[start:end] {
			pk := s.primaryKeys(k)
			calls = append(calls, scriptCall{s.sc.cPurgeKey, pk.all(), s.clock(soft, "p"+nonce, retUs, retRem)})
		}
		out, err := s.pipe(ctx, calls)
		if err != nil {
			return store.PurgeResult{}, s.fail(ctx, "purge", err)
		}
		for _, v := range out {
			x, ok := v.(string)
			if !ok {
				return store.PurgeResult{}, unexpectedReply("purge")
			}
			n, err := strconv.Atoi(x)
			if err != nil {
				return store.PurgeResult{}, unexpectedReply("purge")
			}
			affected += n
		}
		s.at(hookPurgeStep)
	}
	return store.PurgeResult{Accepted: len(distinct), Affected: affected, AffectedKnown: true}, nil
}

// purgeAll runs one purge script per bucket, in batches of 128.
func (s *Store) purgeAll(ctx context.Context, soft, nonce, retUs, retRem string) (store.PurgeResult, error) {
	for start := 0; start < cacheBuckets; start += 128 {
		calls := make([]scriptCall, 0, 128)
		for _, bb := range cacheBucketNames[start : start+128] {
			eh, es := s.bucketEpochKeys(bb)
			calls = append(calls, scriptCall{s.sc.cPurgeAll, []string{eh, es}, s.clock(soft, nonce, retUs, retRem)})
		}
		out, err := s.pipe(ctx, calls)
		if err != nil {
			return store.PurgeResult{}, s.fail(ctx, "purge", err)
		}
		for _, v := range out {
			if x, ok := v.(string); !ok || x != "ok" {
				return store.PurgeResult{}, unexpectedReply("purge")
			}
		}
		s.at(hookPurgeStep)
	}
	return store.PurgeResult{Accepted: 1, AffectedKnown: false}, nil
}

// purgeTags marks the tags in five stages: the tag purge counter, every bucket, the tag markers, then the tag
// sets. The sets are scanned per tag, first the set and then its replica, so each covered variant is counted once.
func (s *Store) purgeTags(ctx context.Context, tags []string, soft, nonce, retUs, retRem string) (store.PurgeResult, error) {
	distinct := sortedUnique(tags)
	allow := strconv.FormatInt(s.allowUs, 10)
	tq, tr := s.counterKeys()
	out, err := s.pipe(ctx, []scriptCall{{s.sc.cTagCounter, []string{tq, tr}, s.clock(nonce, retUs, retRem, allow)}})
	if err != nil {
		return store.PurgeResult{}, s.fail(ctx, "purge", err)
	}
	era, v, ok := replyWord(out[0])
	if !ok || len(v) != 1 {
		return store.PurgeResult{}, unexpectedReply("purge")
	}
	s.at(hookPurgeStep)

	calls := make([]scriptCall, 0, cacheBuckets)
	for _, bb := range cacheBucketNames {
		eh, es := s.bucketEpochKeys(bb)
		calls = append(calls, scriptCall{s.sc.cTagBucket, []string{eh, es}, s.clock(era, v[0], retUs, retRem, allow)})
	}
	if _, err := s.pipe(ctx, calls); err != nil {
		return store.PurgeResult{}, s.fail(ctx, "purge", err)
	}
	s.at(hookPurgeStep)

	for start := 0; start < len(distinct); start += purgeBatch {
		end := min(start+purgeBatch, len(distinct))
		calls := make([]scriptCall, 0, end-start)
		for _, tag := range distinct[start:end] {
			_, _, m, r := s.tagKeys(tag)
			calls = append(calls, scriptCall{s.sc.cTagMark, []string{m, r}, s.clock(era, v[0], retUs, retRem)})
		}
		if _, err := s.pipe(ctx, calls); err != nil {
			return store.PurgeResult{}, s.fail(ctx, "purge", err)
		}
		s.at(hookPurgeStep)
	}

	affected := 0
	for _, tag := range distinct {
		set, repl, _, _ := s.tagKeys(tag)
		for _, key := range []string{set, repl} {
			var cursor uint64
			for {
				fields, next, err := s.hscan(ctx, key, cursor)
				if err != nil {
					return store.PurgeResult{}, s.fail(ctx, "purge", err)
				}
				n, err := s.purgeMembers(ctx, tag, set, repl, fields, soft == "1", nonce, era, v[0])
				if err != nil {
					return store.PurgeResult{}, s.fail(ctx, "purge", err)
				}
				affected += n
				s.at(hookPurgeStep)
				cursor = next
				if cursor == 0 {
					break
				}
			}
		}
	}
	return store.PurgeResult{Accepted: len(distinct), Affected: affected, AffectedKnown: true}, nil
}

// hscan returns one HSCAN batch of key from cursor, with COUNT purgeBatch, as flattened field and value pairs.
func (s *Store) hscan(ctx context.Context, key string, cursor uint64) ([]string, uint64, error) {
	var fields []string
	var next uint64
	err := s.await(ctx, func(ctx context.Context) error {
		var err error
		fields, next, err = s.c.HScan(ctx, key, cursor, "*", purgeBatch).Result()
		return err
	})
	if err != nil {
		return nil, 0, err
	}
	return fields, next, nil
}

// purgeMembers marks the variants listed by one HSCAN batch of the tag set key or its replica. The members are
// grouped by primary, one script call per primary, and the members of variants that no longer exist are then
// pruned from both keys. It returns the number of variants changed.
func (s *Store) purgeMembers(ctx context.Context, tag, set, repl string, fields []string, soft bool, nonce, era, v string) (int, error) {
	type group struct {
		h    string
		secs []string
	}
	var groups []*group
	byHash := make(map[string]*group)
	for i := 0; i+1 < len(fields); i += 2 {
		f := fields[i]
		if strings.HasPrefix(f, "!") || len(f) < 32 || !isHex32(f[:32]) {
			continue
		}
		g := byHash[f[:32]]
		if g == nil {
			g = &group{h: f[:32]}
			byHash[f[:32]] = g
			groups = append(groups, g)
		}
		g.secs = append(g.secs, f[32:])
	}
	if len(groups) == 0 {
		return 0, nil
	}
	mode := "0"
	if soft {
		mode = "1"
	}
	calls := make([]scriptCall, 0, len(groups))
	for _, g := range groups {
		pk := s.keysOf(g.h)
		keys := []string{pk.index, pk.eh, pk.es}
		keys = append(keys, pk.slots[:]...)
		args := []any{mode, tag, nonce, era, v, strconv.Itoa(len(g.secs))}
		for _, sec := range sortedUnique(g.secs) {
			args = append(args, sec)
		}
		calls = append(calls, scriptCall{s.sc.cTagPrimary, keys, s.clock(args...)})
	}
	out, err := s.pipe(ctx, calls)
	if err != nil {
		return 0, err
	}
	changed := 0
	var dead []string
	for i, rep := range out {
		count, rest, ok := replyDeadList(rep)
		if !ok {
			return 0, unexpectedReply("purge")
		}
		n, err := strconv.Atoi(count)
		if err != nil {
			return 0, unexpectedReply("purge")
		}
		changed += n
		for _, sec := range rest {
			dead = append(dead, groups[i].h+sec)
		}
	}
	if len(dead) == 0 {
		return changed, nil
	}
	prune := []any{strconv.FormatInt(s.allowUs, 10), strconv.Itoa(len(dead))}
	for _, m := range dead {
		prune = append(prune, m)
	}
	if _, err := s.pipe(ctx, []scriptCall{{s.sc.cTagPrune, []string{set, repl}, s.clock(prune...)}}); err != nil {
		return 0, err
	}
	return changed, nil
}

// replyDeadList splits a tag primary reply into its count and the secondary keys reported dead.
func replyDeadList(v any) (string, []string, bool) {
	list, ok := v.([]any)
	if !ok || len(list) != 2 {
		return "", nil, false
	}
	count, ok := list[0].(string)
	if !ok {
		return "", nil, false
	}
	rawDead, ok := list[1].([]any)
	if !ok {
		return "", nil, false
	}
	dead := make([]string, 0, len(rawDead))
	for _, x := range rawDead {
		s, ok := x.(string)
		if !ok {
			return "", nil, false
		}
		dead = append(dead, s)
	}
	return count, dead, true
}
