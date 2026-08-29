// Package profiling turns Go's runtime profiles on for one s3d process from
// the environment, and off again when it stops.
//
// It is driven by environment rather than flags because the workload that most
// needs profiling — scripts/bench/e2e-stress.sh — is an acceptance gate that
// may not be edited to pass one. Environment is process-global, which is
// exactly why every filename carries the host id and the pid: four s3d on one
// machine all inherit GO_PROF and must not overwrite one another.
//
// It is process-level rather than gate-level for the same reason the process
// is: one s3d holds the gate, the blob nodes, the metadata replicas and the
// repair service for its host, and profiling only the HTTP server would miss
// the storage and consensus work the request caused.
package profiling

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"
)

// Kind is one runtime profile.
type Kind string

const (
	KindCPU       Kind = "cpu"
	KindHeap      Kind = "heap"
	KindAllocs    Kind = "allocs"
	KindBlock     Kind = "block"
	KindMutex     Kind = "mutex"
	KindGoroutine Kind = "goroutine"
)

// snapshotKinds are the profiles written whole at every interval. CPU is not
// among them: it samples over a window rather than describing an instant.
var snapshotKinds = []Kind{KindHeap, KindAllocs, KindGoroutine, KindBlock, KindMutex}

const (
	defaultInterval  = 5 * time.Second
	defaultCPUWindow = 30 * time.Second

	// blockProfileRate samples one blocking event per microsecond of delay,
	// and mutexProfileFraction one contention event in a hundred. Both cost
	// something on every event they sample, which is why neither is armed
	// unless its profile was asked for.
	blockProfileRate     = 1000
	mutexProfileFraction = 100
)

// Config is the resolved profiling environment.
type Config struct {
	Kinds     map[Kind]bool
	Dir       string
	Interval  time.Duration
	CPUWindow time.Duration
}

// Enabled reports whether anything was asked for. A process with nothing to
// profile starts no goroutine, arms no timer and changes no sampling rate.
func (c Config) Enabled() bool { return len(c.Kinds) > 0 }

// FromEnv resolves the profiling environment.
//
// An empty GO_PROF is off and never an error. Anything else is a request that
// must either work or fail the process: a run that reports success while
// producing no profiles is worse than one that does not start, because the
// numbers it did produce look like a profiled run.
func FromEnv() (Config, error) {
	spec := strings.TrimSpace(os.Getenv("GO_PROF"))

	// PPROF_ENABLED is what the pre-c932d4a builds took. It survives as an
	// alias so an old runbook still profiles something rather than silently
	// profiling nothing.
	if spec == "" && os.Getenv("PPROF_ENABLED") == "1" {
		spec = string(KindCPU)
	}
	if spec == "" {
		return Config{}, nil
	}

	cfg := Config{Kinds: make(map[Kind]bool), Interval: defaultInterval, CPUWindow: defaultCPUWindow}
	for name := range strings.SplitSeq(spec, ",") {
		kind := Kind(strings.ToLower(strings.TrimSpace(name)))
		switch kind {
		case "":
			continue
		case KindCPU, KindHeap, KindAllocs, KindBlock, KindMutex, KindGoroutine:
			cfg.Kinds[kind] = true
		default:
			return Config{}, fmt.Errorf("GO_PROF: unknown profile %q", name)
		}
	}
	if len(cfg.Kinds) == 0 {
		return Config{}, errors.New("GO_PROF names no profiles")
	}

	cfg.Dir = strings.TrimSpace(os.Getenv("GO_PROF_DIR"))
	if cfg.Dir == "" {
		return Config{}, errors.New("GO_PROF is set but GO_PROF_DIR is empty")
	}
	if !filepath.IsAbs(cfg.Dir) {
		return Config{}, fmt.Errorf("GO_PROF_DIR must be absolute, got %q", cfg.Dir)
	}

	var err error
	if cfg.Interval, err = duration("GO_PROF_INTERVAL", defaultInterval); err != nil {
		return Config{}, err
	}
	if cfg.CPUWindow, err = duration("GO_PROF_CPU_WINDOW", defaultCPUWindow); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func duration(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %s", name, raw)
	}

	return d, nil
}

// Profiler writes the requested profiles for the lifetime of one process.
type Profiler struct {
	cfg     Config
	prefix  string
	stop    chan struct{}
	done    chan struct{}
	stopped sync.Once

	// prevMutexFraction is what the process had before this armed mutex
	// sampling, so shutdown can put it back.
	prevMutexFraction int

	mu      sync.Mutex
	cpuFile *os.File
	seq     map[Kind]int
}

// Start begins profiling for this host, or returns a nil *Profiler when the
// environment asked for none. Stop is safe on a nil receiver, so a caller can
// defer it without checking.
func Start(cfg Config, hostID int) (*Profiler, error) {
	if !cfg.Enabled() {
		return nil, nil
	}
	if err := os.MkdirAll(cfg.Dir, 0o750); err != nil {
		return nil, fmt.Errorf("profiling: %w", err)
	}

	p := &Profiler{
		cfg:  cfg,
		stop: make(chan struct{}),
		done: make(chan struct{}),
		seq:  make(map[Kind]int),
		// Started once per process: a host that restarts mid-run gets a new
		// prefix rather than overwriting the evidence from before the fault.
		prefix: fmt.Sprintf("%s-host%d-pid%d", time.Now().UTC().Format("20060102T150405Z"), hostID, os.Getpid()),
	}

	// Armed only when asked for, because both sample on every event they
	// count. The runtime exposes no getter for the block rate, so shutdown
	// restores it to off, which is the default and what this process had
	// unless something else in it set one.
	if cfg.Kinds[KindBlock] {
		runtime.SetBlockProfileRate(blockProfileRate)
	}
	if cfg.Kinds[KindMutex] {
		p.prevMutexFraction = runtime.SetMutexProfileFraction(mutexProfileFraction)
	}

	if cfg.Kinds[KindCPU] {
		if err := p.rotateCPU(); err != nil {
			p.disarm()
			return nil, err
		}
	}

	go p.loop()

	slog.Info("profiling enabled",
		"profiles", strings.Join(names(cfg.Kinds), ","), "dir", cfg.Dir,
		"interval_ms", cfg.Interval.Milliseconds(), "cpu_window_ms", cfg.CPUWindow.Milliseconds(),
		"prefix", p.prefix)

	return p, nil
}

// Stop flushes the open CPU window, takes one last snapshot of every other
// requested profile and restores the sampling rates it changed.
//
// The final snapshot is the point of taking them periodically as well: a host
// this process cannot shut down gracefully — SIGKILLed, or wiped by a scenario
// — still leaves the intervals it wrote before the fault.
func (p *Profiler) Stop() {
	if p == nil {
		return
	}
	p.stopped.Do(func() {
		close(p.stop)
		<-p.done
	})
}

func (p *Profiler) loop() {
	defer close(p.done)

	var cpuRotate <-chan time.Time
	if p.cfg.Kinds[KindCPU] {
		t := time.NewTicker(p.cfg.CPUWindow)
		defer t.Stop()
		cpuRotate = t.C
	}

	var snapshot <-chan time.Time
	if p.wantsSnapshots() {
		t := time.NewTicker(p.cfg.Interval)
		defer t.Stop()
		snapshot = t.C
	}

	for {
		select {
		case <-p.stop:
			p.writeSnapshots()
			p.closeCPU()
			p.disarm()
			return
		case <-cpuRotate:
			if err := p.rotateCPU(); err != nil {
				slog.Error("profiling: CPU profile rotation failed", "error", err)
			}
		case <-snapshot:
			p.writeSnapshots()
		}
	}
}

func (p *Profiler) wantsSnapshots() bool {
	for _, kind := range snapshotKinds {
		if p.cfg.Kinds[kind] {
			return true
		}
	}

	return false
}

// rotateCPU closes the window in progress and opens the next. Each window is
// its own file, so a process killed mid-run leaves every completed window
// readable rather than one truncated profile.
func (p *Profiler) rotateCPU() error {
	p.closeCPU()

	f, err := p.create(KindCPU)
	if err != nil {
		return err
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		f.Close()
		return fmt.Errorf("profiling: start CPU profile: %w", err)
	}

	p.mu.Lock()
	p.cpuFile = f
	p.mu.Unlock()

	return nil
}

func (p *Profiler) closeCPU() {
	p.mu.Lock()
	f := p.cpuFile
	p.cpuFile = nil
	p.mu.Unlock()

	if f == nil {
		return
	}
	pprof.StopCPUProfile()
	if err := f.Close(); err != nil {
		slog.Error("profiling: closing CPU profile", "file", f.Name(), "error", err)
	}
}

func (p *Profiler) writeSnapshots() {
	for _, kind := range snapshotKinds {
		if !p.cfg.Kinds[kind] {
			continue
		}
		if err := p.writeSnapshot(kind); err != nil {
			slog.Error("profiling: snapshot failed", "profile", string(kind), "error", err)
		}
	}
}

func (p *Profiler) writeSnapshot(kind Kind) error {
	prof := pprof.Lookup(string(kind))
	if prof == nil {
		return fmt.Errorf("no runtime profile named %q", kind)
	}

	f, err := p.create(kind)
	if err != nil {
		return err
	}
	defer f.Close()

	// Debug 0 is the binary pprof format, which is what go tool pprof reads
	// and what carries the sample values rather than a rendered listing.
	if err := prof.WriteTo(f, 0); err != nil {
		return fmt.Errorf("write %s profile: %w", kind, err)
	}

	return nil
}

// create opens the next file for a profile kind. The sequence number is what
// orders a run's snapshots, so it is per kind and never reused.
func (p *Profiler) create(kind Kind) (*os.File, error) {
	p.mu.Lock()
	seq := p.seq[kind]
	p.seq[kind]++
	p.mu.Unlock()

	name := filepath.Join(p.cfg.Dir, fmt.Sprintf("%s-%s-%03d.pprof", p.prefix, kind, seq))
	f, err := os.Create(name)
	if err != nil {
		return nil, fmt.Errorf("profiling: %w", err)
	}

	return f, nil
}

// disarm puts back the sampling rates this profiler changed.
func (p *Profiler) disarm() {
	if p.cfg.Kinds[KindBlock] {
		runtime.SetBlockProfileRate(0)
	}
	if p.cfg.Kinds[KindMutex] {
		runtime.SetMutexProfileFraction(p.prevMutexFraction)
	}
}

func names(kinds map[Kind]bool) []string {
	out := make([]string, 0, len(kinds))
	for _, kind := range append([]Kind{KindCPU}, snapshotKinds...) {
		if kinds[kind] {
			out = append(out, string(kind))
		}
	}

	return out
}
