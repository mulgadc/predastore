#!/usr/bin/env bash
# bench-disk.sh — raw-disk fio harness for predastore benchmark ceiling.
#
# Runs four fio jobs (seq/rand × read/write) both buffered and with --direct=1
# against a target directory, emitting one JSON file per run under the results
# dir. No orchestration beyond invoking fio — this is the disk ceiling, the
# bench-predastore.sh script measures what predastore extracts from it.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOBS_DIR="$SCRIPT_DIR/fio-jobs"

TARGET=""
OUT=""

usage() {
    cat >&2 <<EOF
Usage: $0 --target <dir> --out <results-dir>

  --target  Directory on the filesystem under test (must exist, writable).
            Use the same filesystem that predastore will write to.
  --out     Directory where JSON results will be written. Created if missing.
EOF
    exit 2
}

while [ $# -gt 0 ]; do
    case "$1" in
        --target) TARGET="$2"; shift 2 ;;
        --out)    OUT="$2"; shift 2 ;;
        -h|--help) usage ;;
        *) echo "Unknown argument: $1" >&2; usage ;;
    esac
done

[ -n "$TARGET" ] || usage
[ -n "$OUT" ] || usage
[ -d "$TARGET" ] || { echo "target does not exist: $TARGET" >&2; exit 1; }
command -v fio >/dev/null || { echo "fio not on PATH (apt install fio)" >&2; exit 1; }

STAMP="$(date -u +%Y-%m-%dT%H%M%SZ)"
RESULTS="$OUT/disk-$STAMP"
mkdir -p "$RESULTS"

JOBS=(seq-write-1m rand-write-8k seq-read-1m rand-read-8k)
MODES=(buffered direct)

echo "bench-disk: target=$TARGET results=$RESULTS"

for job in "${JOBS[@]}"; do
    job_file="$JOBS_DIR/${job}.fio"
    [ -f "$job_file" ] || { echo "missing job file: $job_file" >&2; exit 1; }

    for mode in "${MODES[@]}"; do
        out_json="$RESULTS/${job}.${mode}.json"
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

        # fio leaves test files in --directory; remove them between runs so
        # each job starts from a clean state and disk space does not balloon.
        rm -f "$TARGET"/"$job"*
    done
done

echo "bench-disk: done. results under $RESULTS"
