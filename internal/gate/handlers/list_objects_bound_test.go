package handlers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fsmMeta serves the handlers from a real meta FSM on badger, so a scan costs
// what it costs on a replica.
type fsmMeta struct {
	fsm   *meta.FSM
	mu    sync.Mutex
	index uint64
}

func newFSMMeta(tb testing.TB) *fsmMeta {
	tb.Helper()
	db, err := badger.Open(badger.DefaultOptions(tb.TempDir()).WithLoggingLevel(badger.WARNING))
	require.NoError(tb, err)
	tb.Cleanup(func() { db.Close() })
	fsm, err := meta.OpenFSM(db)
	require.NoError(tb, err)
	return &fsmMeta{fsm: fsm}
}

// apply commits cmds as consecutive raft entries and returns their answers.
func (m *fsmMeta) apply(cmds ...meta.Command) []any {
	m.mu.Lock()
	logs := make([]*raft.Log, len(cmds))
	for i, cmd := range cmds {
		data, err := json.Marshal(cmd)
		if err != nil {
			panic(err)
		}
		m.index++
		logs[i] = &raft.Log{Index: m.index, Term: 1, Type: raft.LogCommand, Data: data}
	}
	m.mu.Unlock()
	return m.fsm.ApplyBatch(logs)
}

func (m *fsmMeta) Get(_ context.Context, key string) ([]byte, error) {
	v, err := m.fsm.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, meta.ErrNotFound
	}
	return v, err
}

func (m *fsmMeta) LeaderGet(ctx context.Context, key string) ([]byte, error) { return m.Get(ctx, key) }

func (m *fsmMeta) Put(_ context.Context, key string, value []byte) error {
	_ = m.apply(meta.Command{Type: meta.CommandPut, Key: []byte(key), Value: value})
	return nil
}

func (m *fsmMeta) Swap(_ context.Context, key string, value []byte) ([]byte, error) {
	prev, _ := m.apply(meta.Command{Type: meta.CommandSwap, Key: []byte(key), Value: value})[0].([]byte)
	return prev, nil
}

func (m *fsmMeta) Delete(_ context.Context, key string) error {
	_ = m.apply(meta.Command{Type: meta.CommandDelete, Key: []byte(key)})
	return nil
}

func (m *fsmMeta) Scan(ctx context.Context, prefix string, limit int) ([]meta.Item, error) {
	return m.ScanFrom(ctx, prefix, "", limit)
}

// ScanFrom stops at limit as the replica's scan handler does.
func (m *fsmMeta) ScanFrom(_ context.Context, prefix, after string, limit int) ([]meta.Item, error) {
	errLimit := errors.New("limit")
	var items []meta.Item
	err := m.fsm.ScanFrom(prefix, after, func(key string, value []byte) error {
		if limit > 0 && len(items) >= limit {
			return errLimit
		}
		items = append(items, meta.Item{Key: key, Value: value})
		return nil
	})
	if err != nil && !errors.Is(err, errLimit) {
		return nil, err
	}
	return items, nil
}

var _ MetaClient = (*fsmMeta)(nil)

// storeListing writes the listing row of each key in bucket, and with
// placements also the placement row it names, in batches of 1000 entries.
func (m *fsmMeta) storeListing(tb testing.TB, bucket string, keys []string, placements bool) {
	tb.Helper()
	cmds := make([]meta.Command, 0, 1000)
	flush := func() {
		if len(cmds) > 0 {
			m.apply(cmds...)
			cmds = cmds[:0]
		}
	}
	for _, key := range keys {
		hash := model.ObjectHash(bucket, key)
		cmds = append(cmds, meta.Command{Type: meta.CommandPut,
			Key: []byte(TableKey(model.TableObjects, objectARN(bucket, key))), Value: hash[:]})
		if placements {
			raw, err := EncodePlacement(ObjectToShardNodes{DataShardNodes: []config.NodeID{1}, Size: int64(len(key)), WriteEpoch: 1})
			require.NoError(tb, err)
			cmds = append(cmds, meta.Command{Type: meta.CommandPut,
				Key: []byte(TableKey(model.TableObjects, string(hash[:]))), Value: raw})
		}
		if len(cmds) >= 999 {
			flush()
		}
	}
	flush()
}

// countingMeta records the limit of every scan and the rows each returned.
type countingMeta struct {
	*fsmMeta

	mu     sync.Mutex
	rows   int
	limits []int
}

func (c *countingMeta) record(items []meta.Item, limit int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows += len(items)
	c.limits = append(c.limits, limit)
}

func (c *countingMeta) Scan(ctx context.Context, prefix string, limit int) ([]meta.Item, error) {
	items, err := c.fsmMeta.Scan(ctx, prefix, limit)
	c.record(items, limit)
	return items, err
}

func (c *countingMeta) ScanFrom(ctx context.Context, prefix, after string, limit int) ([]meta.Item, error) {
	items, err := c.fsmMeta.ScanFrom(ctx, prefix, after, limit)
	c.record(items, limit)
	return items, err
}

func (c *countingMeta) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows, c.limits = 0, nil
}

// listPage sends one ListObjects request for bucket "bkt".
func listPage(h http.Handler, query url.Values) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/bkt?"+query.Encode(), nil).
		WithContext(WithBucket(context.Background(), model.Bucket{Name: "bkt"}))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// listedPage is the part of a listing page that paging depends on.
type listedPage struct {
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken"`
	NextMarker            string   `xml:"NextMarker"`
	Keys                  []string `xml:"Contents>Key"`
	Prefixes              []string `xml:"CommonPrefixes>Prefix"`
}

func parsePage(t testing.TB, body []byte) listedPage {
	t.Helper()
	var p listedPage
	require.NoError(t, xml.Unmarshal(body, &p), string(body))
	return p
}

func bucketCache() *BucketCache { return NewBucketCache([]BucketConfig{{Name: "bkt"}}) }

func numberedKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("obj/%08d", i)
	}
	return keys
}

// One page without a delimiter reads at most one page of rows, wherever it
// starts, however many keys the bucket holds.
func TestListObjectsPageReadsAtMostOnePageOfRows(t *testing.T) {
	mc := &countingMeta{fsmMeta: newFSMMeta(t)}
	keys := numberedKeys(10_000)
	mc.storeListing(t, "bkt", keys, false)
	h := ListObjects(mc, bucketCache())

	for name, query := range map[string]url.Values{
		"from the start": {"list-type": {"2"}, "max-keys": {"1"}},
		"from a token":   {"list-type": {"2"}, "max-keys": {"1"}, "continuation-token": {base64.StdEncoding.EncodeToString([]byte(keys[5000]))}},
	} {
		t.Run(name, func(t *testing.T) {
			mc.reset()
			rr := listPage(h, query)
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			page := parsePage(t, rr.Body.Bytes())
			require.Len(t, page.Keys, 1)
			assert.True(t, page.IsTruncated)

			assert.LessOrEqual(t, mc.rows, 2, "a page of one key reads at most two rows")
			for _, limit := range mc.limits {
				assert.Equal(t, 2, limit, "every scan asks for max-keys plus one")
			}
		})
	}
}

// edgeListKeys is a keyset with the edges a bounded scan can get wrong.
var edgeListKeys = []string{
	"a", "b", "b/", "b/1", "b/2", "b/2/x", "b/2/y", "b/3", "b/é", "b/日本", "b0", "bé",
	"c/d/e", "k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "zz",
}

// walkKeys follows a listing to its last page, as a client does, and returns
// the keys and the truncation flag of each page.
func walkKeys(t *testing.T, h http.Handler, base url.Values) ([][]string, []bool) {
	t.Helper()
	var pages [][]string
	var truncated []bool
	query := cloneQuery(base)
	for range 50 {
		rr := listPage(h, query)
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		page := parsePage(t, rr.Body.Bytes())
		pages = append(pages, page.Keys)
		truncated = append(truncated, page.IsTruncated)
		if !page.IsTruncated {
			return pages, truncated
		}
		query = cloneQuery(base)
		if base.Get("list-type") == "2" {
			query.Set("continuation-token", page.NextContinuationToken)
		} else {
			query.Set("marker", page.Keys[len(page.Keys)-1])
		}
	}
	t.Fatalf("listing %v did not end in 50 pages", base)
	return nil, nil
}

func cloneQuery(v url.Values) url.Values {
	out := url.Values{}
	for k, vs := range v {
		out[k] = append([]string(nil), vs...)
	}
	return out
}

// Listings without a delimiter, walked to the end over a real FSM, return
// these keys in byte order, with each cursor exclusive.
func TestListObjectsPagesWithoutADelimiter(t *testing.T) {
	mc := newFSMMeta(t)
	mc.storeListing(t, "bkt", edgeListKeys, true)
	h := ListObjects(mc, bucketCache())

	cases := []struct {
		name      string
		query     url.Values
		pages     [][]string
		truncated []bool
	}{
		{"v2 prefix in pages of two", url.Values{"list-type": {"2"}, "prefix": {"b/"}, "max-keys": {"2"}},
			[][]string{{"b/", "b/1"}, {"b/2", "b/2/x"}, {"b/2/y", "b/3"}, {"b/é", "b/日本"}}, []bool{true, true, true, false}},
		{"v2 start-after inside the prefix", url.Values{"list-type": {"2"}, "prefix": {"b"}, "max-keys": {"7"}, "start-after": {"b/2/y"}},
			[][]string{{"b/3", "b/é", "b/日本", "b0", "bé"}}, []bool{false}},
		{"v1 marker inside the prefix", url.Values{"prefix": {"k"}, "max-keys": {"3"}, "marker": {"k07"}},
			[][]string{{"k08", "k09"}}, []bool{false}},
		{"v1 marker below the prefix", url.Values{"prefix": {"b/"}, "max-keys": {"3"}, "marker": {"a"}},
			[][]string{{"b/", "b/1", "b/2"}, {"b/2/x", "b/2/y", "b/3"}, {"b/é", "b/日本"}}, []bool{true, true, false}},
		{"v2 start-after past every key", url.Values{"list-type": {"2"}, "max-keys": {"1000"}, "start-after": {"zz"}},
			[][]string{nil}, []bool{false}},
		{"v2 prefix that is one whole key", url.Values{"list-type": {"2"}, "prefix": {"b0"}},
			[][]string{{"b0"}}, []bool{false}},
		{"v2 whole bucket in pages of seven", url.Values{"list-type": {"2"}, "max-keys": {"7"}},
			[][]string{
				{"a", "b", "b/", "b/1", "b/2", "b/2/x", "b/2/y"},
				{"b/3", "b/é", "b/日本", "b0", "bé", "c/d/e", "k00"},
				{"k01", "k02", "k03", "k04", "k05", "k06", "k07"},
				{"k08", "k09", "zz"},
			}, []bool{true, true, true, false}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pages, truncated := walkKeys(t, h, tc.query)
			assert.Equal(t, tc.pages, pages)
			assert.Equal(t, tc.truncated, truncated)
		})
	}
}

// failingMeta is a meta store whose scans fail.
type failingMeta struct{ *fsmMeta }

var errScanFailed = errors.New("scan failed")

func (failingMeta) Scan(context.Context, string, int) ([]meta.Item, error) { return nil, errScanFailed }

func (failingMeta) ScanFrom(context.Context, string, string, int) ([]meta.Item, error) {
	return nil, errScanFailed
}

// A listing whose scan fails is an internal error, max-keys=0 included: the
// empty page is only an answer when the store could be read.
func TestListObjectsAFailedScanIsAnInternalErrorAtEveryPageSize(t *testing.T) {
	h := ListObjects(failingMeta{newFSMMeta(t)}, bucketCache())
	for _, maxKeys := range []string{"0", "1", "1000"} {
		rr := listPage(h, url.Values{"list-type": {"2"}, "max-keys": {maxKeys}})
		assert.Equal(t, http.StatusInternalServerError, rr.Code, "max-keys=%s", maxKeys)
	}
}

// A start-after below the prefix lists the whole prefix. A scan that seeks to
// a cursor outside its prefix finds nothing, so the cursor must be dropped.
func TestListObjectsStartAfterBeforeThePrefixListsThePrefix(t *testing.T) {
	mc := newFSMMeta(t)
	mc.storeListing(t, "bkt", []string{"a", "b/1", "b/2", "c"}, false)
	h := ListObjects(mc, bucketCache())

	rr := listPage(h, url.Values{"list-type": {"2"}, "prefix": {"b/"}, "start-after": {"a"}})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Equal(t, []string{"b/1", "b/2"}, parsePage(t, rr.Body.Bytes()).Keys)
}

// An object named as the prefix is listed once. The cursor that names it is
// exclusive, so the second page starts after it.
func TestListObjectsCursorEqualToThePrefixIsExclusive(t *testing.T) {
	mc := newFSMMeta(t)
	mc.storeListing(t, "bkt", []string{"b/", "b/1", "b/2"}, false)
	h := ListObjects(mc, bucketCache())

	first := parsePage(t, listPage(h, url.Values{"list-type": {"2"}, "prefix": {"b/"}, "max-keys": {"1"}}).Body.Bytes())
	require.Equal(t, []string{"b/"}, first.Keys)
	require.True(t, first.IsTruncated)
	second := parsePage(t, listPage(h, url.Values{
		"list-type": {"2"}, "prefix": {"b/"}, "max-keys": {"1"}, "continuation-token": {first.NextContinuationToken},
	}).Body.Bytes())
	assert.Equal(t, []string{"b/1"}, second.Keys)

	v1 := parsePage(t, listPage(h, url.Values{"prefix": {"b/"}, "max-keys": {"1"}, "marker": {"b/"}}).Body.Bytes())
	assert.Equal(t, []string{"b/1"}, v1.Keys, "a v1 marker equal to the prefix is exclusive too")
}
