# Predastore Benchmark Harness

A minimal, manually-invoked harness for tracking predastore performance over
time. The goal is regression tracking across commits, not marketing numbers or
micro-optimisation.

## Contents

- `bench-disk.sh` — raw-disk fio ceiling (run independently of predastore).
- `bench-predastore.sh` — three-node predastore cluster on loopback, driven by
  `warp mixed`.
- `fio-jobs/` — four fio jobs covering predastore's access patterns.
- `predastore.toml.tmpl` — config template rendered by `envsubst` at run time.

## Prerequisites

- `fio` (`apt install fio`).
- `warp` (`go install github.com/minio/warp@latest`).
- `envsubst`, `curl`, `ip` (usually present on Linux).
- `make build` in the predastore repo (produces `bin/s3d`).
- `sudo` — required only by `bench-predastore.sh` for `ip addr add` on `lo`.
  The script aliases `10.11.12.{1,2,3}/24` and removes them on exit.

Before running `bench-predastore.sh`:

    export AWS_ACCESS_KEY_ID=...
    export AWS_SECRET_ACCESS_KEY=...

These credentials are baked into the rendered config's `[[db]]` and `[[auth]]`
sections, and are passed to warp unchanged.

## Usage

Raw disk ceiling:

    ./scripts/bench/bench-disk.sh --target /path/on/fs-under-test \
                                  --out   /path/for/results

Each fio job runs twice — buffered and `--direct=1` — and produces a JSON file
per run under `<out>/disk-<timestamp>/`.

Predastore cluster benchmark:

    ./scripts/bench/bench-predastore.sh

Results land under `predastore/scripts/bench/results/predastore-<timestamp>/`
and contain:

- `warp-mixed.csv.zst` — warp's raw samples.
- `predastore.toml` — the rendered config used for the run.
- `logs/node-{1,2,3}.log` — per-node s3d stderr/stdout.
- `run-info.txt` — commit SHA, warp version, date, hostname.

The benchmark data root (default `/tmp/predastore-bench`) is wiped on exit by
the trap. Override with `BENCH_DIR=/some/other/path` if the default filesystem
is not representative.

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
