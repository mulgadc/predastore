#!/usr/bin/env bash
#
# Drives the unchanged e2e-stress scenarios under the profiling environment,
# one output directory per scenario, and records a manifest of what produced
# them.
#
#   prof-matrix.sh <output-root> [scenario ...]
#
# It sets environment around the workload and never edits it: e2e-stress.sh is
# an acceptance gate, and a gate that had to be modified to be measured is not
# measuring the thing that ships.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"

if [ "$#" -lt 1 ]; then
    echo "usage: $0 <output-root> [scenario ...]" >&2
    exit 2
fi

ROOT="$1"
shift

SCENARIOS=("$@")
if [ "${#SCENARIOS[@]}" -eq 0 ]; then
    SCENARIOS=(large-object multipart-upload repair handoff
        node-rejoin node-resync node-rebuild freeze)
fi

mkdir -p "$ROOT"

export GO_PROF="${GO_PROF:-cpu,heap,allocs,block,mutex,goroutine}"
export GO_PROF_INTERVAL="${GO_PROF_INTERVAL:-5s}"
export GO_PROF_CPU_WINDOW="${GO_PROF_CPU_WINDOW:-30s}"

"$HERE/prof-manifest.sh" "$ROOT" > "$ROOT/manifest.txt"

{
    echo "=== matrix starting $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    for s in "${SCENARIOS[@]}"; do
        GO_PROF_DIR="$ROOT/$s"
        export GO_PROF_DIR
        mkdir -p "$GO_PROF_DIR"
        echo "=== $s starting $(date -u +%H:%M:%S)"
        STRESS_SCENARIO="$s" "$REPO/scripts/bench/e2e-stress.sh" \
            > "$ROOT/$s.log" 2>&1
        status=$?
        files=$(find "$GO_PROF_DIR" -maxdepth 1 -name '*.pprof' | wc -l)
        parts=$(find "$GO_PROF_DIR" -maxdepth 1 -name '*.part' | wc -l)
        echo "=== $s exit=$status files=$files incomplete=$parts $(date -u +%H:%M:%S)"
    done
    echo "=== matrix done $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} | tee "$ROOT/matrix.log"

# The checksums are taken after the run, so the manifest names the profiles the
# reports were built from rather than an empty directory.
find "$ROOT" -maxdepth 2 -name '*.pprof' -print0 |
    sort -z | xargs -0 --no-run-if-empty sha256sum > "$ROOT/profiles.sha256"

echo "profiles in $ROOT; manifest in $ROOT/manifest.txt"
