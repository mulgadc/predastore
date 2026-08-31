#!/usr/bin/env bash
#
# One row per scenario from the reports prof-report.sh produced.
#
#   prof-summarize.sh <reports-root> [scenario ...]
#
# The reports root holds one directory per scenario, which is what
# prof-report.sh writes when it is run once per scenario directory.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -lt 1 ]; then
    echo "usage: $0 <reports-root> [scenario ...]" >&2
    exit 2
fi

ROOT="$1"
shift

SCENARIOS=("$@")
if [ "${#SCENARIOS[@]}" -eq 0 ]; then
    SCENARIOS=(large-object multipart-upload repair handoff
        node-rejoin node-resync node-rebuild freeze)
fi

total() {
    [ -f "$1" ] || return 0
    go tool pprof -top -nodecount=1 "$1" 2>/dev/null | awk '/total$/{print $(NF-1)}'
}

printf '%-17s %9s %9s %9s %9s %9s %9s %8s %7s\n' \
    scenario span_s cpu_s pred_cpu alloc_MB pred_MB block_s mutex_s max_gor

for s in "${SCENARIOS[@]}"; do
    d="$ROOT/$s"
    [ -f "$d/cpu-top.txt" ] || continue
    span=$(awk -F'[ ,]+' '/^Duration:/{print $2}' "$d/cpu-top.txt" | tr -d 's')
    cpu=$(awk '/Total samples =/{for(i=1;i<=NF;i++) if($i=="=") print $(i+1)}' "$d/cpu-top.txt" | tr -d 's')
    predcpu=$("$HERE/prof-pkgshare.sh" "$d/cpu-merged.pprof" | awk '$1=="predastore"{print $3}')
    predmb=$("$HERE/prof-pkgshare.sh" "$d/allocs-merged.pprof" | awk '$1=="predastore"{printf "%.0f", $2}')
    gor=$(awk '{if($4>m)m=$4} END{print m+0}' "$d/goroutines.txt" 2>/dev/null)
    printf '%-17s %9s %9s %9s %9s %9s %9s %8s %7s\n' \
        "$s" "${span:-?}" "${cpu:-?}" "${predcpu:-0.0%}" \
        "$(total "$d/allocs-merged.pprof")" "${predmb:-0}" \
        "$(total "$d/block-merged.pprof")" "$(total "$d/mutex-merged.pprof")" "${gor:-0}"
done
