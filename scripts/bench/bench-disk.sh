#!/usr/bin/env bash
# bench-disk.sh — raw-disk fio harness for predastore benchmark ceiling.
#
# Runs four fio jobs (seq/rand × read/write) both buffered and with --direct=1
# against $BENCH_DIR/disk, emitting one JSON file per run under the results
# dir. No orchestration beyond invoking fio — this is the disk ceiling, the
# bench-predastore.sh script measures what predastore extracts from it.
#
# Layout mirrors bench-predastore.sh: fio target lives next to predastore's
# distributed/ tree under BENCH_DIR, results land under
# scripts/bench/results/disk-<timestamp>/.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
JOBS_DIR="$SCRIPT_DIR/fio-jobs"

BENCH_DIR="${BENCH_DIR:-/tmp/predastore-bench}"
TARGET="$BENCH_DIR/disk"

STAMP="$(date -u +%Y-%m-%dT%H%M%SZ)"
RESULTS_PARENT="${RESULTS_PARENT:-$REPO_ROOT/scripts/bench/results}"
RESULTS_DIR="$RESULTS_PARENT/disk-$STAMP"

command -v fio >/dev/null || { echo "fio not on PATH (apt install fio)" >&2; exit 1; }

mkdir -p "$TARGET" "$RESULTS_DIR"

# Wipe only the disk target on exit — leave BENCH_DIR alone so a concurrent
# predastore run's data is not touched.
cleanup() {
    rm -rf "$TARGET"
}
trap cleanup EXIT INT TERM

JOBS=(seq-write-1m rand-write-8k seq-read-1m rand-read-8k)
MODES=(buffered direct)

echo "bench-disk: target=$TARGET results=$RESULTS_DIR"

for job in "${JOBS[@]}"; do
    job_file="$JOBS_DIR/${job}.fio"
    [ -f "$job_file" ] || { echo "missing job file: $job_file" >&2; exit 1; }

    for mode in "${MODES[@]}"; do
        out_json="$RESULTS_DIR/${job}.${mode}.json"
        direct_flag=()
        if [ "$mode" = "direct" ]; then
            direct_flag=(--direct=1)
        fi

        echo "  running $job ($mode)"
        fio --directory="$TARGET" \
            --output-format=json \
            --output="$out_json" \
            "${direct_flag[@]}" \
            "$job_file"

        # Remove the job's file between runs so each invocation starts from
        # a clean state and disk usage does not balloon.
        rm -f "$TARGET"/"$job"*
    done
done

echo "bench-disk: done. results under $RESULTS_DIR"
