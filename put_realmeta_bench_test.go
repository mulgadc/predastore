package predastore_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/predastore"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
)

const realMetaBucket = "bench-realmeta"

var realMetaSizes = []struct {
	name string
	size int
}{
	{"1KiB", 1 << 10},
	{"1MiB", 1 << 20},
	{"64MiB", 64 << 20},
}

var realMetaModes = []string{"unconditional", "if-none-match", "if-match"}

// BenchmarkPutObjectRealMeta is the PutObject handler against a fresh running
// host per case: three raft meta replicas and three blob nodes on its pipe, so
// every placement swap, intent and transaction is a replicated raft write.
func BenchmarkPutObjectRealMeta(b *testing.B) {
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	b.Cleanup(func() { slog.SetDefault(prev) })

	for _, sz := range realMetaSizes {
		body := bytes.Repeat([]byte("x"), sz.size)
		for _, mode := range realMetaModes {
			b.Run(sz.name+"/"+mode+"/single", func(b *testing.B) {
				h := startRealMetaHost(b)
				w := h.writer(b, mode, body, sz.name+"-"+mode+"-s")
				b.SetBytes(int64(sz.size))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if err := w.write(); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(sz.name+"/"+mode+"/concurrent", func(b *testing.B) {
				h := startRealMetaHost(b)
				writers := make(chan *realMetaWriter, runtime.GOMAXPROCS(0))
				for i := range cap(writers) {
					writers <- h.writer(b, mode, body, fmt.Sprintf("%s-%s-c%d", sz.name, mode, i))
				}
				b.SetBytes(int64(sz.size))
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					w := <-writers
					for pb.Next() {
						if err := w.write(); err != nil {
							b.Error(err)
							return
						}
					}
				})
			})
		}
	}
}

type realMetaHost struct {
	put http.Handler
}

// startRealMetaHost runs one host until the benchmark ends, and builds a
// PutObject handler on meta and blob clients that dial its nodes.
func startRealMetaHost(b *testing.B) *realMetaHost {
	b.Helper()
	// Every listener stays open until all ports are taken, so no two match.
	var held []net.Listener
	port := func() int {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			b.Fatal(err)
		}
		held = append(held, ln)
		return ln.Addr().(*net.TCPAddr).Port
	}
	off := false
	cfg := &config.Config{
		Version: config.Version,
		Region:  "ap-southeast-2",
		RS:      config.RS{Data: 2, Parity: 1},
		Reaper:  config.Reaper{Enabled: &off},
		Hosts: []config.Host{{
			ID: 1, Addr: "127.0.0.1", DataDir: b.TempDir(), AdminPort: port(),
			Nodes: []config.Node{
				{ID: 2, Role: config.RoleMeta, Port: port()},
				{ID: 3, Role: config.RoleMeta, Port: port()},
				{ID: 4, Role: config.RoleMeta, Port: port()},
				{ID: 5, Role: config.RoleBlob, Port: port()},
				{ID: 6, Role: config.RoleBlob, Port: port()},
				{ID: 7, Role: config.RoleBlob, Port: port()},
			},
		}},
		Buckets: []config.Bucket{{Name: realMetaBucket, Region: "ap-southeast-2", AccountID: "123456789012"}},
	}
	for _, ln := range held {
		_ = ln.Close()
	}
	if err := cfg.Validate(); err != nil {
		b.Fatal(err)
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		b.Fatal(err)
	}
	key, err := masterkey.New(secret)
	if err != nil {
		b.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- predastore.Run(ctx, predastore.Options{Config: cfg, HostID: 1, MasterKey: key}) }()
	b.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			b.Error("host did not stop")
		}
	})

	pipe := transport.NewPipeTransport(cfg.Hosts[0].Addr, 0)
	b.Cleanup(func() { _ = pipe.Close() })
	res, err := rpc.NewResolver(cfg, 5, pipe)
	if err != nil {
		b.Fatal(err)
	}
	pool := rpc.NewConnPool(5, res)
	b.Cleanup(func() { _ = pool.Close() })
	cli := rpc.NewClient(pool)
	mc, err := meta.NewClient(meta.ClientConfig{Client: cli, Replicas: []config.NodeID{2, 3, 4}})
	if err != nil {
		b.Fatal(err)
	}
	bc, err := blob.NewClient(blob.ClientConfig{Client: cli})
	if err != nil {
		b.Fatal(err)
	}
	awaitRealMetaReady(b, cfg.Hosts[0].AdminPort)

	epochs, err := handlers.NewEpochMinter(1)
	if err != nil {
		b.Fatal(err)
	}
	buckets := []handlers.BucketConfig{{Name: realMetaBucket, Region: cfg.Region, AccountID: "123456789012"}}
	put := handlers.PutObject(mc, bc, placement.NewRing([]config.NodeID{5, 6, 7}), handlers.NewBucketCache(buckets),
		handlers.Config{
			Region: cfg.Region, DataShards: cfg.RS.Data, ParityShards: cfg.RS.Parity,
			DegradedWrites: cfg.RS.DegradedWritesEnabled(), Buckets: buckets, Epochs: epochs,
		})
	return &realMetaHost{put: put}
}

// awaitRealMetaReady returns once the host's readiness probe answers 200,
// which needs an elected meta leader and every blob node serving.
func awaitRealMetaReady(b *testing.B, adminPort int) {
	b.Helper()
	url := "http://127.0.0.1:" + strconv.Itoa(adminPort) + "/readyz"
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(30 * time.Second)
	var last string
	for {
		resp, err := http.Get(url) //nolint:noctx // a loopback probe under a deadline.
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
			last = resp.Status
		} else {
			last = err.Error()
		}
		select {
		case <-tick.C:
		case <-deadline:
			b.Fatalf("the host never became ready: %s", last)
		}
	}
}

// realMetaWriter writes one mode repeatedly. A create uses a fresh key each
// time; an If-Match writer overwrites its own key at the ETag it last got.
type realMetaWriter struct {
	h      *realMetaHost
	mode   string
	prefix string
	body   []byte
	etag   string
	n      int
}

func (h *realMetaHost) writer(b *testing.B, mode string, body []byte, prefix string) *realMetaWriter {
	w := &realMetaWriter{h: h, mode: mode, prefix: prefix, body: body}
	if mode == "if-match" {
		rr := h.serve(prefix, body, nil)
		if rr.Code != http.StatusOK {
			b.Fatalf("seed: %d %s", rr.Code, rr.Body.String())
		}
		w.etag = rr.Header().Get("ETag")
	}
	return w
}

func (w *realMetaWriter) write() error {
	w.n++
	key, header := fmt.Sprintf("%s-%09d", w.prefix, w.n), http.Header(nil)
	switch w.mode {
	case "if-none-match":
		header = http.Header{"If-None-Match": {"*"}}
	case "if-match":
		key, header = w.prefix, http.Header{"If-Match": {w.etag}}
	}
	rr := w.h.serve(key, w.body, header)
	if rr.Code != http.StatusOK {
		return fmt.Errorf("%s put: %d %s", w.mode, rr.Code, rr.Body.String())
	}
	w.etag = rr.Header().Get("ETag")
	return nil
}

func (h *realMetaHost) serve(key string, body []byte, header http.Header) *httptest.ResponseRecorder {
	bucket := model.Bucket{Name: realMetaBucket}
	ctx := handlers.WithObject(handlers.WithBucket(context.Background(), bucket), model.Object{Bucket: bucket, Key: key})
	req := httptest.NewRequest(http.MethodPut, "/"+realMetaBucket+"/"+key, bytes.NewReader(body)).WithContext(ctx)
	req.ContentLength = int64(len(body))
	maps.Copy(req.Header, header)
	rr := httptest.NewRecorder()
	h.put.ServeHTTP(rr, req)
	return rr
}
