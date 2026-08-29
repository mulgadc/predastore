#!/usr/bin/env bash
#
# Records what a profiling run was taken from, so a number in a report can be
# traced back to a tree.
#
#   prof-manifest.sh [output-root]
#
# The five-repo refs are here because a Predastore build is only half the
# answer: the same gate code against a different bluebottle is a different
# system, and `spx version` reports neither.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
ROOT="${1:-}"

describe() {
    local d="$1"
    [ -d "$d/.git" ] || [ -f "$d/.git" ] || return 0
    printf '%-12s %-26s %-42s %s\n' "$(basename "$d")" \
        "$(git -C "$d" describe --tags --always --dirty 2>/dev/null || echo -)" \
        "$(git -C "$d" rev-parse HEAD 2>/dev/null || echo -)" \
        "$([ -z "$(git -C "$d" status --porcelain 2>/dev/null)" ] && echo clean || echo DIRTY)"
}

echo "generated      $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "host           $(uname -srmo)"
echo "go             $(go version)"
[ -n "$ROOT" ] && echo "output         $ROOT"
echo

echo "branch         $(git -C "$REPO" rev-parse --abbrev-ref HEAD 2>/dev/null || echo -)"
echo
echo "repository   describe                   head                                       state"
describe "$REPO"
for d in "$REPO"/../{spinifex,viperblock,northstar,bluebottle}; do
    [ -d "$d" ] && describe "$(cd "$d" && pwd)"
done
echo

# The workload's blob hash is the acceptance gate's identity. A run whose hash
# differs from the recorded one measured a different workload, whatever the
# scenario names say.
echo "workload       scripts/bench/e2e-stress.sh"
echo "workload_hash  $(git -C "$REPO" hash-object scripts/bench/e2e-stress.sh)"
echo
echo "GO_PROF            ${GO_PROF:-}"
echo "GO_PROF_INTERVAL   ${GO_PROF_INTERVAL:-}"
echo "GO_PROF_CPU_WINDOW ${GO_PROF_CPU_WINDOW:-}"
