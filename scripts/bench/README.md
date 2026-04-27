# Predastore Benchmark Harness

A minimal, manually-invoked harness for tracking predastore performance over
time. The goal is regression tracking across commits, not marketing numbers or
micro-optimisation.

## Contents

- `bench-disk.sh` — raw-disk fio ceiling (run independently of predastore).
- `bench-predastore.sh` — three-node predastore cluster on loopback, driven by
  `warp mixed`.
- `fio-jobs/` — four fio jobs covering predastore's predicted access patterns.

## Prerequisites

- `fio` (`apt install fio`).
- `warp` (`go install github.com/minio/warp@latest`).
- `curl`, `ip` (usually present on Linux).
- `make build` in the predastore repo (produces `bin/s3d`).
- `make certs` to generate TLS certificates (or `make build`, which does both).
- `sudo` — required only by `bench-predastore.sh` for `ip addr add` on `lo`.
  The script aliases `10.11.12.{1,2,3}/24` and removes them on exit.

## Usage

Raw disk ceiling:

    ./scripts/bench/bench-disk.sh

fio writes to `$BENCH_DIR/disk` (parallel to predastore's `distributed/`
tree); each job runs twice (buffered and `--direct=1`) and produces a JSON
file per run under `scripts/bench/results/disk-<timestamp>/`. Override
`BENCH_DIR` to point at a different filesystem.

Predastore cluster benchmark:

    ./scripts/bench/bench-predastore.sh

Results land under `predastore/scripts/bench/results/predastore-<timestamp>/`
and contain:

- `warp-mixed.csv.zst` — warp's raw samples.
- `cluster.toml` — the config used for the run.
- `logs/node-{1,2,3}.log` — per-node s3d stderr/stdout.
- `run-info.txt` — commit SHA, warp version, date, hostname.

The benchmark data root (default `/tmp/predastore-bench`) is wiped on exit by
the trap. Override with `BENCH_DIR=/some/other/path` if the default filesystem
is not representative — `bench-disk.sh` honours the same variable, so both
scripts target the same storage when `BENCH_DIR` is set. Note that with RS(2,1)
the on-disk footprint is ~1.5× the logical object volume, spread across three
nodes; warp's defaults (2500 × 10 MiB) do not fit a typical dev-host tmpfs.

### Tuning warp mixed

Four env vars forward through to `warp mixed`; leaving any of them unset keeps
warp's own default:

| Variable          | warp flag      | warp default |
|-------------------|----------------|--------------|
| `WARP_OBJECTS`    | `--objects`    | 2500         |
| `WARP_OBJ_SIZE`   | `--obj.size`   | 10MiB        |
| `WARP_DURATION`   | `--duration`   | 5m           |
| `WARP_CONCURRENT` | `--concurrent` | 20           |

For a tmpfs-safe local run (~750 MB on disk):

    WARP_OBJECTS=512 WARP_OBJ_SIZE=1MiB WARP_DURATION=30s WARP_CONCURRENT=10 \
        ./scripts/bench/bench-predastore.sh

Dedicated-hardware CI runs leave them unset.

## fio Jobs

Each job maps to an access pattern predastore is predicted to exhibit in production.
Every job runs twice — buffered and `--direct=1` — so cache effects are visible.

| Job               | Pattern               | Reflects                                      |
|-------------------|-----------------------|-----------------------------------------------|
| `seq-write-1m`    | `write`, 1M, fsync-on-close | Bulk ingest ceiling (PutObject, AMI writes)   |
| `rand-write-8k`   | `randwrite`, 8k, `fsync=1`, iodepth 32 | WAL `WriteAt` pattern |
| `seq-read-1m`     | `read`, 1M            | Bulk GET ceiling                              |
| `rand-read-8k`    | `randread`, 8k, iodepth 32 | RS reconstruction read fan-out           |

## Predastore Config

Uses `clusters/3node/cluster.toml` directly (static config, no templating):

- **RS(2, 1)** — 2 data shards + 1 parity.
- **3 db nodes** on `10.11.12.{1,2,3}:6660` — Raft quorum for metadata,
  node 1 is the bootstrap leader.
- **3 QUIC storage nodes** on `10.11.12.{1,2,3}:9991` — shard distribution
  across the three processes.
- **No buckets configured** — warp creates its own via `--bucket=predastore`.
- **Test credentials** — `AKIAIOSFODNN7EXAMPLE` / standard test secret key.
  Self-contained; no AWS profile or credential files needed.
- **`-base-path $BENCH_DIR` passed on the CLI** — the distributed backend
  resolves relative data paths from the cluster config against this root.

## Deferred

Out of scope for this pass, kept as follow-on work:

- MinIO comparison numbers.
- Tuned warp workloads (duration, object size, concurrency).
- CI-gated regression detection and `benchstat`-level statistical rigour.
- Separated client/server hosts.
- Automated trend visualisation.

The absolute throughput numbers from a single-host run will be lower than a
separated-client or real-multi-host setup; the same setup run on a later
commit gives a comparable delta, which is what this harness is for.
