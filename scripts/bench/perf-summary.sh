#!/usr/bin/env bash
#
# perf-summary.sh - Render an e2e-performance run as a markdown summary.
#
# Reads the run directory e2e-performance.sh writes and emits GitHub-flavoured
# markdown on stdout, so CI can append it to $GITHUB_STEP_SUMMARY and a person
# can read the same thing locally without unpacking an artefact.
#
# Usage:
#   ./scripts/bench/perf-summary.sh <run-dir> [hostlogs-dir]
#
# <run-dir> holds run-info.txt and one directory per config, each holding
# <workload>-latency.txt. [hostlogs-dir] is optional and holds <host>/disk.txt,
# the `df -PB1` of the deployment root captured on each host.
#

set -euo pipefail

RUN_DIR="${1:-}"
HOSTLOGS_DIR="${2:-}"

[ -n "$RUN_DIR" ] || { echo "usage: $0 <run-dir> [hostlogs-dir]" >&2; exit 2; }
[ -d "$RUN_DIR" ] || { echo "no such run directory: $RUN_DIR" >&2; exit 1; }

INFO="$RUN_DIR/run-info.txt"

# info_field reads a key=value line from run-info.txt. Absent is normal --
# an external run cannot report the cluster's CPU, for one -- so a missing
# key yields the empty string and the caller decides whether to print it.
info_field() {
    [ -f "$INFO" ] || return 0
    sed -n "s/^$1=//p" "$INFO" | head -1
}

# report_field pulls one value out of a warp latency report by sed script.
report_field() {
    sed -n "$1" "$2" | head -1
}

# latency_files lists a config's reports in the order the workloads ran, so
# the table reads the way the run happened rather than alphabetically. Any
# report not named here still appears, after the known ones.
latency_files() {
    local dir="$1" w f
    for w in put multipart-put get; do
        [ -f "$dir/$w-latency.txt" ] && printf '%s\n' "$dir/$w-latency.txt"
    done
    for f in "$dir"/*-latency.txt; do
        case "$(basename "$f")" in
            put-latency.txt|multipart-put-latency.txt|get-latency.txt) ;;
            *) [ -f "$f" ] && printf '%s\n' "$f" ;;
        esac
    done
}

human_bytes() {
    awk -v b="${1:-0}" 'BEGIN {
        split("B KiB MiB GiB TiB", u, " ")
        i = 1
        while (b >= 1024 && i < 5) { b /= 1024; i++ }
        printf (i == 1 ? "%d %s\n" : "%.1f %s\n"), b, u[i]
    }'
}

emit_workload_rows() {
    local config_dir="$1" f name reqs size thr objs avg p50 p90 p99 slowest

    for f in $(latency_files "$config_dir"); do
        name="$(report_field 's/^Report: \([A-Za-z-]*\) (.*/\1/p' "$f")"
        [ -n "$name" ] || continue

        reqs="$(report_field 's/^Report: [A-Za-z-]* (\([0-9]*\) reqs).*/\1/p' "$f")"
        size="$(report_field 's/.*Size: \([0-9]*\) bytes.*/\1/p' "$f")"
        thr="$(report_field 's/^ \* Average: \([^,]*\),.*/\1/p' "$f")"
        objs="$(report_field 's/^ \* Average: [^,]*, \([0-9.]*\) obj\/s.*/\1/p' "$f")"
        avg="$(report_field 's/^ \* Reqs: Avg: \([^,]*\),.*/\1/p' "$f")"
        p50="$(report_field 's/^ \* Reqs:.*[^0-9]50%: \([^,]*\),.*/\1/p' "$f")"
        p90="$(report_field 's/^ \* Reqs:.*[^0-9]90%: \([^,]*\),.*/\1/p' "$f")"
        p99="$(report_field 's/^ \* Reqs:.*[^0-9]99%: \([^,]*\),.*/\1/p' "$f")"
        slowest="$(report_field 's/^ \* Reqs:.*Slowest: \([^,]*\),.*/\1/p' "$f")"

        printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
            "$name" "${reqs:--}" "$(human_bytes "$size")" "${thr:--}" \
            "${objs:--}" "${avg:--}" "${p50:--}" "${p90:--}" "${p99:--}" "${slowest:--}"
    done
}

# The TTFB line only appears on read workloads, and it is the number a
# throughput gate should key on rather than mean bandwidth.
emit_ttfb() {
    local config_dir="$1" f name line printed=0

    for f in $(latency_files "$config_dir"); do
        line="$(grep -m1 '^ \* TTFB:' "$f" 2>/dev/null || true)"
        [ -n "$line" ] || continue
        name="$(report_field 's/^Report: \([A-Za-z-]*\) (.*/\1/p' "$f")"

        if [ "$printed" -eq 0 ]; then
            printf '\n### Time to first byte\n\n'
            printf '| Workload | Avg | Median | 90th | 99th | Worst |\n'
            printf '|---|---:|---:|---:|---:|---:|\n'
            printed=1
        fi
        printf '| %s | %s | %s | %s | %s | %s |\n' "$name" \
            "$(sed -n 's/.*TTFB: Avg: \([^,]*\),.*/\1/p' <<< "$line")" \
            "$(sed -n 's/.*Median: \([^,]*\),.*/\1/p' <<< "$line")" \
            "$(sed -n 's/.*90th: \([^,]*\),.*/\1/p' <<< "$line")" \
            "$(sed -n 's/.*99th: \([^,]*\),.*/\1/p' <<< "$line")" \
            "$(sed -n 's/.*Worst: \([^ ,]*\).*/\1/p' <<< "$line")"
    done
}

# Warp reports each gate separately. An uneven split is the interesting case:
# it says the load did not spread, which an aggregate figure hides.
emit_per_host() {
    local config_dir="$1" f name hosts host val
    local -a names=()

    hosts="$(sed -n 's/^ \* \(https:\/\/[^:]*:[0-9]*\): Avg:.*/\1/p' "$config_dir"/*-latency.txt \
        2>/dev/null | sort -u)"
    [ -n "$hosts" ] || return 0

    for f in $(latency_files "$config_dir"); do
        name="$(report_field 's/^Report: \([A-Za-z-]*\) (.*/\1/p' "$f")"
        [ -n "$name" ] && names+=("$name|$f")
    done
    [ "${#names[@]}" -gt 0 ] || return 0

    printf '\n### Throughput by host\n\n| Host |'
    for n in "${names[@]}"; do printf ' %s |' "${n%%|*}"; done
    printf '\n|---|'
    for _ in "${names[@]}"; do printf '%s' '---:|'; done
    printf '\n'

    while IFS= read -r host; do
        printf '| %s |' "${host#https://}"
        for n in "${names[@]}"; do
            val="$(sed -n "s|^ \* $host: Avg: \([^,]*\),.*|\1|p" "${n#*|}" | head -1)"
            printf ' %s |' "${val:--}"
        done
        printf '\n'
    done <<< "$hosts"
}

# Free space on the deployment root, per host. This is the number that explains
# a capacity failure, and nothing else in the artefact carries it.
emit_disk() {
    local d f host size used avail pct

    [ -n "$HOSTLOGS_DIR" ] && [ -d "$HOSTLOGS_DIR" ] || return 0
    ls "$HOSTLOGS_DIR"/*/disk.txt >/dev/null 2>&1 || return 0

    printf '\n### Free space on the deployment root\n\n'
    printf '| Host | Size | Used | Avail | Use%% |\n|---|---:|---:|---:|---:|\n'
    for d in "$HOSTLOGS_DIR"/*/; do
        f="$d/disk.txt"
        [ -f "$f" ] || continue
        host="$(basename "$d")"
        size="$(awk 'NR==2 {print $2}' "$f")"
        used="$(awk 'NR==2 {print $3}' "$f")"
        avail="$(awk 'NR==2 {print $4}' "$f")"
        pct="$(awk 'NR==2 {print $5}' "$f")"
        printf '| %s | %s | %s | %s | %s |\n' "$host" \
            "$(human_bytes "$size")" "$(human_bytes "$used")" \
            "$(human_bytes "$avail")" "${pct:--}"
    done
}

printf '## Performance (Warp)\n\n'

sha="$(info_field predastore_sha)"
external="$(info_field external_hosts)"
{
    printf '| | |\n|---|---|\n'
    [ -n "$sha" ] && printf '| Commit | `%s` |\n' "${sha:0:12}"
    printf '| Preset | `%s`, %s per workload, %s concurrent |\n' \
        "$(info_field preset)" "$(info_field duration)" "$(info_field concurrent)"
    if [ -n "$external" ]; then
        printf '| Cluster | bare metal, `%s` |\n' "$external"
    else
        printf '| Cluster | loopback profiles on one machine |\n'
    fi
    printf '| Measured from | %s, %s logical CPUs, %s RAM |\n' \
        "$(info_field host)" "$(info_field logical_cpus)" \
        "$(human_bytes "$(info_field memory_bytes)")"
    printf '| Go | %s |\n' "$(info_field go_version)"
} | sed 's/ | *|$/ | |/'

for config_dir in "$RUN_DIR"/*/; do
    config="$(basename "$config_dir")"
    case "$config" in logs|correctness) continue ;; esac
    ls "$config_dir"/*-latency.txt >/dev/null 2>&1 || continue

    printf '\n### Workloads — `%s`\n\n' "$config"
    printf '| Workload | Reqs | Object | Throughput | Objects/s | Avg | p50 | p90 | p99 | Slowest |\n'
    printf '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n'
    emit_workload_rows "$config_dir"
    emit_ttfb "$config_dir"
    emit_per_host "$config_dir"
done

emit_disk

printf '\nCorrectness round trips (PUT, empty object, multipart, GET, diff, SHA256) passed for every config; the run fails before this summary otherwise.\n'
