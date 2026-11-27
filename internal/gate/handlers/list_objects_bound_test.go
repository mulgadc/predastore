package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
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

var updateListGolden = flag.Bool("update-list-golden", false, "rewrite testdata/list_objects_golden.json from the handler")

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

// goldenCase is one listing walked to its last page.
type goldenCase struct {
	Name  string   `json:"name"`
	Pages []string `json:"pages"`
}

// walkListing follows a listing to its last page and returns the sha256 of
// each page body.
func walkListing(t *testing.T, h http.Handler, base url.Values, v2 bool) []string {
	t.Helper()
	var pages []string
	query := cloneQuery(base)
	for range 100 {
		rr := listPage(h, query)
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		sum := sha256.Sum256(rr.Body.Bytes())
		pages = append(pages, hex.EncodeToString(sum[:]))
		page := parsePage(t, rr.Body.Bytes())
		if !page.IsTruncated {
			return pages
		}
		query = cloneQuery(base)
		switch {
		case v2:
			query.Set("continuation-token", page.NextContinuationToken)
		case page.NextMarker != "":
			query.Set("marker", page.NextMarker)
		default:
			query.Set("marker", page.Keys[len(page.Keys)-1])
		}
	}
	t.Fatalf("listing %v did not end in 100 pages", base)
	return nil
}

func cloneQuery(v url.Values) url.Values {
	out := url.Values{}
	for k, vs := range v {
		out[k] = append([]string(nil), vs...)
	}
	return out
}

// The paged handler answers every listing, page by page, with the bytes the
// handler that scanned the whole prefix answered. The golden file is that
// handler's output, so any change in S3 behaviour shows as a changed page.
func TestListObjectsPagedMatchesTheFullScan(t *testing.T) {
	mc := newFSMMeta(t)
	mc.storeListing(t, "bkt", edgeListKeys, false)
	h := ListObjects(mc, bucketCache())

	var got []goldenCase
	for _, v2 := range []bool{false, true} {
		for _, prefix := range []string{"", "b/", "b", "k"} {
			for _, maxKeys := range []string{"", "0", "1", "7"} {
				for _, start := range []string{"", "a", "b/", "b/2", "zzz"} {
					for _, delimiter := range []string{"", "/"} {
						q := url.Values{}
						name := fmt.Sprintf("v2=%v prefix=%q max-keys=%q start=%q delimiter=%q", v2, prefix, maxKeys, start, delimiter)
						if v2 {
							q.Set("list-type", "2")
						}
						for k, v := range map[string]string{"prefix": prefix, "max-keys": maxKeys, "delimiter": delimiter} {
							if v != "" {
								q.Set(k, v)
							}
						}
						if start != "" && v2 {
							q.Set("start-after", start)
						} else if start != "" {
							q.Set("marker", start)
						}
						got = append(got, goldenCase{Name: name, Pages: walkListing(t, h, q, v2)})
					}
				}
			}
		}
	}

	path := filepath.Join("testdata", "list_objects_golden.json")
	if *updateListGolden {
		data, err := json.MarshalIndent(got, "", " ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, append(data, '\n'), 0o600))
		return
	}
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var want []goldenCase
	require.NoError(t, json.Unmarshal(data, &want))
	require.Len(t, got, len(want))
	for i := range want {
		assert.Equal(t, want[i], got[i], want[i].Name)
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
