# Predastore Benchmark Harness

A minimal, manually-invoked harness for tracking predastore performance over time. The goal is regression tracking across commits, not marketing numbers or micro-optimisation.

## Contents

- `bench-disk.sh` — raw-disk fio ceiling (run independently of predastore).
- `bench-cluster.sh` — predastore cluster on loopback, driven by `warp mixed`.
- `fio-jobs/` — four fio jobs covering predastore's predicted access patterns.
- `e2e-stress.sh` — fault injection and end-to-end correctness, one scenario per
  `STRESS_SCENARIO`.
- `partialput/` — a PUT client that declares a body length and then stops sending,
  used by the `partial-put` scenario. No S3 client can express this: they all
  either finish the body or abort, and both already work. `-rate` paces the send
  so a caller can kill it mid-body.

### `e2e-stress` scenarios

`freeze` (default) puts a four-host cluster under Warp GET load, stops one host
with SIGSTOP, and asserts the survivors keep serving and the host rejoins raft.
The fault is in the cluster.

`partial-put` injects the fault in the *client* instead — the class that stranded
PUTs for tens of minutes on a production cluster. It stores an object, opens an
overwrite of the same key that does not complete, and asserts three things per
case:

- the object still reads back intact **while** the incomplete overwrite is outstanding;
- the gate abandons the request within `STRESS_PARTIAL_ABANDON` seconds, measured
  from the gate's own `S3 request still running` tracker rather than from the
  client, whose elapsed time also counts however long it chose to stall;
- the object still reads back intact afterwards.

Six cases, varying the three things that could differ from production:

| Case | Fault |
|------|-------|
| `http1-stall`, `http2-stall` | client stops sending mid-body and holds the connection open |
| `http1-large` | the same, with a 512MiB declared body |
| `http1-kill`, `http2-kill` | client SIGKILLed while genuinely still transmitting |
| `concurrent` | eight stalled uploads at once, each declaring 512MiB |

Both protocols appear because a stalled h2 stream is flow-control state on a
shared connection while an HTTP/1.1 stall is a socket the server owns outright,
and they need not fail the same way. The kill cases pace their send so the signal
lands mid-body rather than after the upload has already finished.

The concurrent case also logs the gate's peak RSS. `writeObject` allocates its
shard buffers from the `Content-Length` the client declared, before reading any
of the body, so N stalled uploads hold N × declared bytes. The figure is recorded,
not asserted: no ceiling has been agreed, and a threshold would only be a guess.

    make e2e-stress                                    # freeze
    make e2e-stress STRESS_SCENARIO=partial-put        # incomplete client
    make e2e-stress STRESS_SCENARIO=last-modified      # object dates

`last-modified` injects no fault. It asserts that HEAD, GET, ListObjectsV2 and
ListParts all report when the object was written and all report the same thing,
because they used to give three different answers: HEAD and GET dated everything
`0001-01-01` and ListObjectsV2 answered the time of the listing, so a client that
listed a bucket and then headed a key saw two dates decades apart and an
incremental sync saw every object change on every pass.

The date is the write epoch in the placement record, so it is asserted end to end
rather than in a handler test: it has to survive being encoded into the record,
committed through raft and decoded by a gate other than the one that wrote it,
which is why every read in the scenario is taken from a second gate. Ten
assertions, about 30 seconds, no fault injection and no tunables.

Tunables: `STRESS_PARTIAL_HOLD` (how long the client stalls, default 180),
`STRESS_PARTIAL_ABANDON` (the bound the gate must meet, default 90),
`STRESS_PARTIAL_DECLARE` and `STRESS_PARTIAL_SEND` (declared and actual body
bytes), `STRESS_PARTIAL_LARGE_DECLARE` and `STRESS_PARTIAL_LARGE_SEND` (the same
for the large and kill cases), `STRESS_PARTIAL_KILL_RATE` and
`STRESS_PARTIAL_KILL_AFTER` (send pacing and when to kill),
`STRESS_PARTIAL_CONCURRENCY` (default 8).

The abandon bound must stay above the gate's 50s request deadline or it asserts
nothing. Concurrency times the large declared size is roughly the peak memory the
run needs, so raising either on a small machine is how it gets OOM-killed.

All benchmarks can be run via the top-level dispatcher:

    ./scripts/bench.sh disk          # raw-disk fio
    ./scripts/bench.sh 3host         # cluster warp benchmark

## Prerequisites

- `fio` (`apt install fio`).
- `warp` (`go install github.com/minio/warp@latest`).
- `curl`, `ip` (usually present on Linux).
- `make build` in the predastore repo (produces `bin/s3d`).
- `sudo` — required by multi-host profiles only, for `ip addr add` on `lo` and for installing the cluster certificate as a trust anchor. The aliases are removed on exit. The `1host` profile needs neither.

TLS certificates and the master key are generated by `start.sh` under `$PREDA_DIR`; `make certs` is for the test suite, not for these scripts.

## Usage

Raw disk ceiling:

    ./scripts/bench/bench-disk.sh

fio writes to `$PREDA_DIR/disk`, alongside the per-cluster data directories rather than inside one; each job runs twice (buffered and `--direct=1`) and produces a JSON file per run under `scripts/bench/results/disk-<timestamp>/`.

Predastore cluster benchmark:

    ./scripts/bench.sh 3host
    # or directly:
    ./scripts/bench/bench-cluster.sh 3host

Warp is pointed at every gate the profile declares, so the load spreads over all of them. Results land under `predastore/scripts/bench/results/<clustername>-<timestamp>/` and contain:

- `warp-mixed.json.zst` — warp's raw samples.
- `cluster.toml` — the config used for the run.
- `run-info.txt` — commit SHA, warp version, date, hostname, gate endpoints.

Warp's cleanup phase calls `DeleteObjects` (`POST /{bucket}?delete`), which the gate does not implement. It logs `405 Method Not Allowed` at the end of a run; the benchmark itself is unaffected.

### `PREDA_DIR`

All scripts share a single root directory controlled by `PREDA_DIR` (default `/tmp/predastore`). Cluster data, warp temp files (`.warp-tmp/`), the TLS keypair, the master key and fio targets all live under this path. Override it to move everything off tmpfs:

    PREDA_DIR=/var/lib/predastore ./scripts/bench.sh 3host

With RS(2,1) the on-disk footprint is ~1.5× the logical object volume, spread across three blob nodes; warp's defaults (2500 × 10 MiB) do not fit a typical dev-host tmpfs.

### Tuning warp mixed

Four env vars forward through to `warp mixed`; leaving any of them unset keeps warp's own default:

| Variable          | warp flag      | warp default |
|-------------------|----------------|--------------|
| `WARP_OBJECTS`    | `--objects`    | 2500         |
| `WARP_OBJ_SIZE`   | `--obj.size`   | 10MiB        |
| `WARP_DURATION`   | `--duration`   | 5m           |
| `WARP_CONCURRENT` | `--concurrent` | 20           |

For a tmpfs-safe local run (~750 MB on disk):

    WARP_OBJECTS=512 WARP_OBJ_SIZE=1MiB WARP_DURATION=30s WARP_CONCURRENT=10 \
        ./scripts/bench/bench-cluster.sh 3host

Dedicated-hardware CI runs leave them unset.

## fio Jobs

Each job maps to an access pattern predastore is predicted to exhibit in production. Every job runs twice — buffered and `--direct=1` — so cache effects are visible.

| Job               | Pattern               | Reflects                                      |
|-------------------|-----------------------|-----------------------------------------------|
| `seq-write-1m`    | `write`, 1M, fsync-on-close | Bulk ingest ceiling (PutObject, AMI writes)   |
| `rand-write-8k`   | `randwrite`, 8k, `fsync=1`, iodepth 32 | WAL `WriteAt` pattern |
| `seq-read-1m`     | `read`, 1M            | Bulk GET ceiling                              |
| `rand-read-8k`    | `randread`, 8k, iodepth 32 | RS reconstruction read fan-out           |

## Predastore Config

Uses the static profiles under `config/` directly, no templating. `bench-cluster.sh` reads the cluster's region, credentials and gate endpoints straight out of the profile it is given:

| Profile | Hosts | RS | Inter-node transport |
|---------|-------|----|----------------------|
| `1host` | 1 | (1, 0) | in-process pipe only |
| `3host` | 3 | (2, 1) | QUIC between hosts, pipe within |
| `5host` | 5 | (3, 2) | QUIC between hosts, pipe within |

Common to all three:

- **A gate, a meta replica and a blob node per host** — every host answers S3 on `:8443`, meta replicas form the Raft quorum on `:6660`, blob nodes hold erasure-coded shards on `:9991`.
- **No buckets configured** — warp creates its own via `--bucket=predastore-bench`.
- **Test credentials** — `AKIAIOSFODNN7EXAMPLE` / standard test secret key. Self-contained; no AWS profile or credential files needed.
- **Host-local paths passed as flags** — `start.sh` supplies `-data-dir`, `-tls-cert`, `-tls-key` and `-encryption-key` per process, so the profile carries no absolute path and stays identical on every machine.

## Deferred

Out of scope for this pass, kept as follow-on work:

- MinIO comparison numbers.
- Tuned warp workloads (duration, object size, concurrency).
- CI-gated regression detection and `benchstat`-level statistical rigour.
- Separated client/server hosts.
- Automated trend visualisation.

The absolute throughput numbers from a single-host run will be lower than a separated-client or real-multi-host setup; the same setup run on a later commit gives a comparable delta, which is what this harness is for.
