#!/usr/bin/env bash
#
# compare-performance.sh - Diff two e2e-performance runs, workload by workload.
#
# Warp compares its own artifacts, so this pairs them by relative path and
# leaves the statistics to `warp cmp`.
#
# Usage:
#   ./scripts/bench/compare-performance.sh BEFORE_DIR AFTER_DIR
#   make e2e-performance-compare PERF_BEFORE=... PERF_AFTER=...
#

set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "usage: $0 BEFORE_DIR AFTER_DIR" >&2
    exit 2
fi

BEFORE="$1"
AFTER="$2"
WARP="${WARP:-warp}"

[ -d "$BEFORE" ] || { echo "missing before directory: $BEFORE" >&2; exit 1; }
[ -d "$AFTER" ] || { echo "missing after directory: $AFTER" >&2; exit 1; }

# A missing counterpart has to fail the comparison rather than be skipped: a
# run that quietly compared fewer workloads would read as a clean result.
missing=0
while IFS= read -r before_file; do
    relative="${before_file#"$BEFORE"/}"
    after_file="$AFTER/$relative"
    if [ ! -f "$after_file" ]; then
        echo "missing matching result: $after_file" >&2
        missing=1
        continue
    fi
    echo "Comparing $relative"
    "$WARP" cmp --no-color "$before_file" "$after_file"
done < <(find "$BEFORE" -mindepth 2 -maxdepth 2 -type f \
    \( -name '*.json.zst' -o -name '*.csv.zst' \) -print | sort)

exit "$missing"
