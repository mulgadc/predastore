package profiling

import (
	"os"
	"os/exec"
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
	require.NoError(t, p.Stop())
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
	require.NoError(t, p.Stop())

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
	require.NoError(t, p.Stop())

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
	defer func() { _ = p.Stop() }()

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
	require.NoError(t, p.Stop())

	asked := Config{
		Kinds: map[Kind]bool{KindMutex: true, KindBlock: true}, Dir: t.TempDir(),
		Interval: time.Hour, CPUWindow: time.Hour,
	}
	p, err = Start(asked, 1)
	require.NoError(t, err)
	assert.Equal(t, mutexProfileFraction, runtime.SetMutexProfileFraction(-1),
		"mutex sampling was requested and must be on")

	require.NoError(t, p.Stop())
	assert.Equal(t, 0, runtime.SetMutexProfileFraction(-1),
		"shutdown must put the sampling rate back")
}

func TestStopIsIdempotentAndSafeOnANilProfiler(t *testing.T) {
	var nilProf *Profiler
	assert.NotPanics(t, func() { assert.NoError(t, nilProf.Stop()) })

	dir := t.TempDir()
	p, err := Start(Config{
		Kinds: map[Kind]bool{KindHeap: true}, Dir: dir,
		Interval: time.Hour, CPUWindow: time.Hour,
	}, 1)
	require.NoError(t, err)

	require.NoError(t, p.Stop())
	assert.NotPanics(t, func() { assert.NoError(t, p.Stop()) },
		"a deferred Stop after an explicit one must not close a closed channel")
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

// The old runbooks name a file, and the new controller names a directory with
// one file per host, process, kind and window. The mapping takes the directory
// and drops the basename, because four s3d on one machine inherit one
// environment and honouring the basename would have three overwrite the fourth.
func TestTheLegacyOutputVariableNamesTheDirectory(t *testing.T) {
	t.Setenv("GO_PROF", "")
	t.Setenv("GO_PROF_DIR", "")
	t.Setenv("PPROF_ENABLED", "1")
	t.Setenv("PPROF_OUTPUT", "/var/tmp/old-runbook/predastore-cpu.prof")

	cfg, err := FromEnv()
	require.NoError(t, err)
	assert.Equal(t, "/var/tmp/old-runbook", cfg.Dir)
	assert.Equal(t, map[Kind]bool{KindCPU: true}, cfg.Kinds)
}

// GO_PROF_DIR still wins, so a runbook part-way through the migration profiles
// where the new variable says rather than in two places.
func TestTheNewDirectoryWinsOverTheLegacyOutput(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("GO_PROF", "")
	t.Setenv("GO_PROF_DIR", dir)
	t.Setenv("PPROF_ENABLED", "1")
	t.Setenv("PPROF_OUTPUT", "/var/tmp/old-runbook/predastore-cpu.prof")

	cfg, err := FromEnv()
	require.NoError(t, err)
	assert.Equal(t, dir, cfg.Dir)
}

// Neither variable is a request that cannot be served, and profiling that was
// asked for and cannot run fails the process rather than running unprofiled.
func TestTheLegacyAliasWithNoOutputAtAllIsRefused(t *testing.T) {
	t.Setenv("GO_PROF", "")
	t.Setenv("GO_PROF_DIR", "")
	t.Setenv("PPROF_ENABLED", "1")
	t.Setenv("PPROF_OUTPUT", "")

	_, err := FromEnv()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PPROF_OUTPUT")
}

// A snapshot that cannot be written is a failed run. Logging it and carrying on
// produces an incomplete profile set behind a zero exit, which reads as a
// profiled run and is not one.
func TestASnapshotFailureEndsTheRun(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "profiles")
	p, err := Start(Config{
		Kinds: map[Kind]bool{KindHeap: true}, Dir: dir,
		Interval: 10 * time.Millisecond, CPUWindow: time.Hour,
	}, 1)
	require.NoError(t, err)

	watched := p.Watch(t.Context())
	require.NoError(t, os.RemoveAll(dir))

	select {
	case <-watched.Done():
	case <-time.After(10 * time.Second):
		require.Fail(t, "profiling stopped writing and the run carried on")
	}

	err = p.Stop()
	require.Error(t, err, "Stop must report the failure so the process can exit non-zero")
	assert.ErrorIs(t, err, os.ErrNotExist)
}

// A profile only gets its final name once it has been written and closed, so a
// process killed mid-write leaves nothing a report script can mistake for the
// last complete snapshot.
func TestAnUnfinishedProfileNeverTakesTheFinalName(t *testing.T) {
	dir := t.TempDir()
	p, err := Start(Config{
		Kinds: map[Kind]bool{KindHeap: true}, Dir: dir,
		Interval: time.Hour, CPUWindow: time.Hour,
	}, 1)
	require.NoError(t, err)
	defer func() { _ = p.Stop() }()

	f, final, err := p.create(KindHeap)
	require.NoError(t, err)
	_, err = f.WriteString("half a profile")
	require.NoError(t, err)

	assert.NotEqual(t, final, f.Name(), "a profile is written under a temporary name")
	assert.FileExists(t, f.Name())
	assert.NoFileExists(t, final)

	// The extension is the whole mechanism: a report script globs .pprof, so a
	// half-written file must not carry it.
	assert.Equal(t, ".pprof", filepath.Ext(final))
	assert.NotEqual(t, ".pprof", filepath.Ext(f.Name()))

	require.NoError(t, commit(f, final))
	assert.FileExists(t, final)
	assert.NoFileExists(t, f.Name())
}

// The same property against a real SIGKILL: this test re-runs itself as a
// child that snapshots continuously, kills it without warning, and requires
// every file under a final name to parse.
func TestAKilledProcessLeavesNoProfileThatLooksComplete(t *testing.T) {
	if dir := os.Getenv("PROFILING_CHILD_DIR"); dir != "" {
		snapshotUntilKilled(dir)

		return
	}

	dir := t.TempDir()
	child := exec.Command(os.Args[0], "-test.run", t.Name(), "-test.timeout=60s")
	child.Env = append(os.Environ(), "PROFILING_CHILD_DIR="+dir)
	require.NoError(t, child.Start())

	// Killed once it is demonstrably in the middle of writing: several
	// snapshots have landed, so the next one is in flight.
	deadline := time.Now().Add(30 * time.Second)
	for len(glob(t, dir, "*.pprof")) < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.NoError(t, child.Process.Kill())
	_ = child.Wait()

	complete := glob(t, dir, "*.pprof")
	require.NotEmpty(t, complete, "the child wrote nothing, so this proves nothing")
	for _, name := range complete {
		parse(t, name)
	}
}

// snapshotUntilKilled is the child half of the test above. The goroutines make
// the profile large enough that writing it takes real time, so a kill at an
// arbitrary moment is likely to land inside a write.
func snapshotUntilKilled(dir string) {
	park := make(chan struct{})
	defer close(park)
	for range 20000 {
		go func() { <-park }()
	}

	p, err := Start(Config{
		Kinds: map[Kind]bool{KindGoroutine: true}, Dir: dir,
		Interval: time.Millisecond, CPUWindow: time.Hour,
	}, 1)
	if err != nil {
		panic(err)
	}
	defer func() { _ = p.Stop() }()

	time.Sleep(time.Minute)
}

func glob(t *testing.T, dir, pattern string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	require.NoError(t, err)

	return matches
}
