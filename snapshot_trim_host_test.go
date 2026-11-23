package predastore_test

import (
	"crypto/tls"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/stretchr/testify/require"
)

// TestS3PutDoesNotStallBehindASnapshotLogTrim writes through the gate until
// the meta replica snapshots and trims its log, and requires that no PUT waits
// on the trim. It takes minutes, so it runs only when PREDASTORE_S3_TRIM_PUTS is set.
func TestS3PutDoesNotStallBehindASnapshotLogTrim(t *testing.T) {
	puts, _ := strconv.ParseInt(os.Getenv("PREDASTORE_S3_TRIM_PUTS"), 10, 64)
	if puts == 0 {
		t.Skip("set PREDASTORE_S3_TRIM_PUTS, such as 200000, to run")
	}
	certPath, keyPath, _ := testcerts.Generate(t)
	adminPort, gatePort := freePort(t), freePort(t)
	dataDir := t.TempDir()
	cfg := &config.Config{
		Version: config.Version,
		Region:  "ap-southeast-2",
		RS:      config.RS{Data: 1, Parity: 0},
		Meta:    config.Meta{SnapshotIntervalSeconds: 1, SnapshotThreshold: uint64(puts * 9 / 10), TrailingLogs: 1},
		Hosts: []config.Host{{
			ID: 1, Addr: "127.0.0.1", DataDir: dataDir,
			TLSCert: certPath, TLSKey: keyPath, AdminPort: adminPort,
			Nodes: []config.Node{
				{ID: 1, Role: config.RoleGate, Port: gatePort, BindAddr: "127.0.0.1"},
				{ID: 2, Role: config.RoleMeta, Port: freePort(t)},
				{ID: 3, Role: config.RoleBlob, Port: freePort(t)},
			},
		}},
		Auth: []config.AuthEntry{{AccessKeyID: keyA, SecretAccessKey: secretA, AccountID: accountA}},
	}
	runHost(t, cfg, newMasterKey(t))
	awaitProbe(t, "http://127.0.0.1:"+strconv.Itoa(adminPort)+"/readyz", http.StatusOK)
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	base := "https://127.0.0.1:" + strconv.Itoa(gatePort)
	awaitGate(t, hc, base+"/")
	c := &s3Client{t: t, base: base, region: cfg.Region, accessKey: keyA, secretKey: secretA, http: hc}

	status, _ := c.send(http.MethodPut, "/trim", "", nil)
	require.Equal(t, http.StatusOK, status)

	snapshots := ""
	require.NoError(t, filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() && d.Name() == "snapshots" {
			snapshots = path
			return filepath.SkipAll
		}
		return err
	}))
	require.NotEmpty(t, snapshots, "no meta snapshot directory under %s", dataDir)

	// Raft renames a snapshot out of its .tmp directory and then trims the
	// log, so the rename marks the trim's start. PUTs run until well after it.
	var trimStart atomic.Pointer[time.Time]
	go func() {
		for trimStart.Load() == nil {
			if dirs, _ := filepath.Glob(filepath.Join(snapshots, "*", "state.bin")); slices.ContainsFunc(dirs, func(d string) bool {
				return !strings.HasSuffix(filepath.Dir(d), ".tmp")
			}) {
				now := time.Now()
				trimStart.Store(&now)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	const afterTrim = 5 * time.Second
	running := func() bool {
		start := trimStart.Load()
		return start == nil || time.Since(*start) < afterTrim
	}

	// A key near S3's 1,024-byte limit makes each placement entry about 2 KB.
	pad := strings.Repeat("k", 960)
	var next atomic.Int64
	var mu sync.Mutex
	var worst time.Duration
	overlapping := 0
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for running() {
				i := next.Add(1)
				if i > 2*puts {
					t.Errorf("no snapshot after %d PUTs", 2*puts)
					return
				}
				start := time.Now()
				status, body := c.send(http.MethodPut, fmt.Sprintf("/trim/%s%08d", pad, i), "", []byte("v"))
				end := time.Now()
				if status != http.StatusOK {
					t.Errorf("PUT %d: %d %s", i, status, body)
					return
				}
				if trim := trimStart.Load(); trim != nil && end.After(*trim) {
					mu.Lock()
					overlapping++
					worst = max(worst, end.Sub(start))
					mu.Unlock()
				}
			}
		})
	}
	wg.Wait()

	t.Logf("worst of %d PUTs ending in the %v after the trim began: %v", overlapping, afterTrim, worst)
	require.Positive(t, overlapping, "no PUT overlapped the trim")
	require.Less(t, worst, 250*time.Millisecond, "a PUT waited %v", worst)
}
