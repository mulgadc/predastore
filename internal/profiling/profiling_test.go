package profiling

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// files lists the profile files in a directory, sorted, so a test can assert
// on what a run left behind.
func files(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	sort.Strings(out)

	return out
}

func countKind(names []string, kind Kind) int {
	n := 0
	for _, name := range names {
		if strings.Contains(name, "-"+string(kind)+"-") {
			n++
		}
	}

	return n
}

func TestAnEmptyEnvironmentAsksForNothing(t *testing.T) {
	t.Setenv("GO_PROF", "")
	t.Setenv("PPROF_ENABLED", "")

	cfg, err := FromEnv()
	require.NoError(t, err)
	assert.False(t, cfg.Enabled())
}

// The cost of profiling being off has to be nothing: no goroutine, no timer,
// no sampling rate changed, and a Stop that is safe to defer unconditionally.
func TestDisabledProfilingStartsNothing(t *testing.T) {
	t.Setenv("GO_PROF", "")
	t.Setenv("PPROF_ENABLED", "")

	cfg, err := FromEnv()
	require.NoError(t, err)

	before := runtime.NumGoroutine()
	p, err := Start(cfg, 1)
	require.NoError(t, err)
	require.Nil(t, p, "a disabled profiler is nil, so nothing holds a timer")

	assert.LessOrEqual(t, runtime.NumGoroutine(), before,
		"profiling is off, so it must not have started a goroutine")
	p.Stop()
}

// GO_PROF set and unusable is a failed run, not a quiet one: the alternative
// is a run reported as profiled that produced nothing.
func TestAnUnusableRequestIsRefusedLoudly(t *testing.T) {
	tests := []struct {
		name  string
		env   map[string]string
		match string
	}{
		{"unknown profile", map[string]string{"GO_PROF": "cpu,cache"}, "unknown profile"},
		{"no directory", map[string]string{"GO_PROF": "cpu"}, "GO_PROF_DIR is empty"},
		{"relative directory", map[string]string{"GO_PROF": "cpu", "GO_PROF_DIR": "profiles"}, "must be absolute"},
		{"bad interval", map[string]string{
			"GO_PROF": "heap", "GO_PROF_DIR": "/tmp/p", "GO_PROF_INTERVAL": "soon",
		}, "GO_PROF_INTERVAL"},
		{"negative window", map[string]string{
			"GO_PROF": "cpu", "GO_PROF_DIR": "/tmp/p", "GO_PROF_CPU_WINDOW": "-5s",
		}, "must be positive"},
		{"names nothing", map[string]string{"GO_PROF": ",,"}, "names no profiles"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range []string{"GO_PROF", "GO_PROF_DIR", "GO_PROF_INTERVAL", "GO_PROF_CPU_WINDOW"} {
				t.Setenv(k, tt.env[k])
			}

			_, err := FromEnv()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.match)
		})
	}
}

func TestTheDefaultsAreTheDocumentedOnes(t *testing.T) {
	t.Setenv("GO_PROF", "cpu, heap ,MUTEX")
	t.Setenv("GO_PROF_DIR", "/tmp/predastore-profiles")
	t.Setenv("GO_PROF_INTERVAL", "")
	t.Setenv("GO_PROF_CPU_WINDOW", "")

	cfg, err := FromEnv()
	require.NoError(t, err)
	assert.Equal(t, map[Kind]bool{KindCPU: true, KindHeap: true, KindMutex: true}, cfg.Kinds)
	assert.Equal(t, defaultInterval, cfg.Interval)
	assert.Equal(t, defaultCPUWindow, cfg.CPUWindow)
}

// The runbooks that predate the flags say PPROF_ENABLED=1. It profiles the CPU
// rather than nothing, which is what it used to do.
func TestPprofEnabledStillMeansACPUProfile(t *testing.T) {
	t.Setenv("GO_PROF", "")
	t.Setenv("PPROF_ENABLED", "1")
	t.Setenv("GO_PROF_DIR", "/tmp/predastore-profiles")

	cfg, err := FromEnv()
	require.NoError(t, err)
	assert.Equal(t, map[Kind]bool{KindCPU: true}, cfg.Kinds)
}

// Four s3d on one machine inherit one GO_PROF, so the only thing keeping their
// profiles apart is the name.
func TestFilenamesSeparateHostsProcessesKindsAndWindows(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Kinds:     map[Kind]bool{KindCPU: true, KindHeap: true},
		Dir:       dir,
		Interval:  10 * time.Millisecond,
		CPUWindow: 20 * time.Millisecond,
	}

	p, err := Start(cfg, 3)
	require.NoError(t, err)
	time.Sleep(80 * time.Millisecond)
	p.Stop()

	names := files(t, dir)
	require.NotEmpty(t, names)

	pattern := regexp.MustCompile(`^\d{8}T\d{6}Z-host3-pid\d+-(cpu|heap)-\d{3}\.pprof$`)
	for _, name := range names {
		assert.Regexp(t, pattern, name, "a profile filename must carry host, pid, kind and sequence")
	}
	assert.Len(t, names, len(unique(names)), "no two profiles may share a name")

	assert.Greater(t, countKind(names, KindCPU), 1, "the CPU profile must have rotated at its window")
	assert.Greater(t, countKind(names, KindHeap), 1, "snapshots must repeat at the interval")
}

func unique(in []string) map[string]bool {
	out := make(map[string]bool, len(in))
	for _, s := range in {
		out[s] = true
	}

	return out
}

// A CPU window that is still open at shutdown holds every sample in it, so a
// profiler that did not flush would lose the end of the run — which is where a
// scenario's interesting work usually is.
func TestShutdownFlushesTheOpenCPUWindow(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Kinds: map[Kind]bool{KindCPU: true}, Dir: dir,
		Interval: time.Hour, CPUWindow: time.Hour,
	}

	p, err := Start(cfg, 1)
	require.NoError(t, err)
	burn(50 * time.Millisecond)
	p.Stop()

	names := files(t, dir)
	require.Len(t, names, 1, "one window was opened and none rotated")

	prof := parse(t, filepath.Join(dir, names[0]))
	assert.NotEmpty(t, prof.Sample, "the flushed window carries the samples taken in it")
}

// The periodic snapshots are the evidence a host leaves when it never gets to
// shut down: e2e-stress SIGKILLs hosts and wipes their directories.
func TestSnapshotsLandBeforeShutdown(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Kinds:     map[Kind]bool{KindHeap: true, KindGoroutine: true},
		Dir:       dir,
		Interval:  10 * time.Millisecond,
		CPUWindow: time.Hour,
	}

	p, err := Start(cfg, 2)
	require.NoError(t, err)
	defer p.Stop()

	// Two of each, so the first of each is known to be finished rather than
	// still being written as the test reads it.
	require.Eventually(t, func() bool {
		names := files(t, dir)
		return countKind(names, KindHeap) > 1 && countKind(names, KindGoroutine) > 1
	}, 2*time.Second, 10*time.Millisecond,
		"a process killed now would have left nothing")

	// Readable while the process is still running, not merely present.
	for _, kind := range []Kind{KindHeap, KindGoroutine} {
		name := firstOfKind(t, files(t, dir), kind)
		prof := parse(t, filepath.Join(dir, name))
		assert.NotEmpty(t, prof.SampleType, "%s is not a parseable profile", name)
	}
}

// firstOfKind is the earliest snapshot of a kind, which the sequence number in
// the filename orders.
func firstOfKind(t *testing.T, names []string, kind Kind) string {
	t.Helper()
	for _, name := range names {
		if strings.Contains(name, "-"+string(kind)+"-") {
			return name
		}
	}
	t.Fatalf("no %s profile among %v", kind, names)

	return ""
}

// Block and mutex sampling cost something on every event they sample, so a run
// that did not ask for them must not pay for them, and one that did must not
// leave them armed behind it.
func TestSamplingIsArmedOnlyWhenAskedForAndRestoredAfter(t *testing.T) {
	restore := runtime.SetMutexProfileFraction(0)
	t.Cleanup(func() { runtime.SetMutexProfileFraction(restore) })

	dir := t.TempDir()
	unasked := Config{
		Kinds: map[Kind]bool{KindHeap: true}, Dir: dir,
		Interval: time.Hour, CPUWindow: time.Hour,
	}
	p, err := Start(unasked, 1)
	require.NoError(t, err)
	assert.Equal(t, 0, runtime.SetMutexProfileFraction(-1),
		"mutex sampling was not requested, so it must not be on")
	p.Stop()

	asked := Config{
		Kinds: map[Kind]bool{KindMutex: true, KindBlock: true}, Dir: t.TempDir(),
		Interval: time.Hour, CPUWindow: time.Hour,
	}
	p, err = Start(asked, 1)
	require.NoError(t, err)
	assert.Equal(t, mutexProfileFraction, runtime.SetMutexProfileFraction(-1),
		"mutex sampling was requested and must be on")

	p.Stop()
	assert.Equal(t, 0, runtime.SetMutexProfileFraction(-1),
		"shutdown must put the sampling rate back")
}

func TestStopIsIdempotentAndSafeOnANilProfiler(t *testing.T) {
	var nilProf *Profiler
	assert.NotPanics(t, nilProf.Stop)

	dir := t.TempDir()
	p, err := Start(Config{
		Kinds: map[Kind]bool{KindHeap: true}, Dir: dir,
		Interval: time.Hour, CPUWindow: time.Hour,
	}, 1)
	require.NoError(t, err)

	p.Stop()
	assert.NotPanics(t, p.Stop, "a deferred Stop after an explicit one must not close a closed channel")
}

// A directory that cannot be created is a failed run rather than a silent one.
func TestAnUnusableDirectoryFailsTheStart(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))

	_, err := Start(Config{
		Kinds: map[Kind]bool{KindHeap: true}, Dir: filepath.Join(file, "profiles"),
		Interval: time.Hour, CPUWindow: time.Hour,
	}, 1)
	assert.Error(t, err)
}

func parse(t *testing.T, path string) *profile.Profile {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	prof, err := profile.Parse(f)
	require.NoError(t, err)

	return prof
}

// burn keeps a CPU busy long enough for the sampler to take something, so the
// flush assertion is about the profiler and not about an idle process.
func burn(d time.Duration) {
	deadline := time.Now().Add(d)
	x := 0
	for time.Now().Before(deadline) {
		for i := range 100_000 {
			x += i
		}
	}
	_ = x
}
