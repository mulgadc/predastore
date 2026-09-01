#!/usr/bin/env bash
#
# perf-summary.sh - Render an e2e-performance run as a markdown summary.
#
# Reads the run directory e2e-performance.sh writes and emits GitHub-flavoured
# markdown on stdout, so CI can append it to $GITHUB_STEP_SUMMARY and a person
# can read the same thing locally without unpacking an artefact.
#
# Everything warp measured is reported. A figure the harness recorded but the
# summary omits is a figure nobody reads, and the ones that were omitted --
# per-second throughput spread, standard deviations, and warp's own "too few
# samples" notices -- are the ones that say whether the rest can be trusted.
#
# One CI run measures four times -- loopback at 1 MiB, loopback at 64 KiB, then
# bare metal at both the smoke and compare presets -- so this takes every run
# directory rather than one. Rendering the last of them was rendering a quarter
# of what the job measured.
#
# Usage:
#   ./scripts/bench/perf-summary.sh [--hostlogs <dir>] <run-dir>...
#
# Each <run-dir> holds run-info.txt and one directory per config, each holding
# <workload>-latency.txt. --hostlogs is optional and names a directory of
# <host>/disk.txt, the `df -PB1` of the deployment root captured on each host.
#

set -euo pipefail

HOSTLOGS_DIR=""
RUN_DIRS=()

while [ $# -gt 0 ]; do
    case "$1" in
        --hostlogs) HOSTLOGS_DIR="${2:-}"; shift 2 ;;
        -*) echo "unknown option: $1" >&2; exit 2 ;;
        *) RUN_DIRS+=("$1"); shift ;;
    esac
done

[ "${#RUN_DIRS[@]}" -gt 0 ] || { echo "usage: $0 [--hostlogs <dir>] <run-dir>..." >&2; exit 2; }

RUN_DIR=""
INFO=""

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

workload_name() {
    report_field 's/^Report: \([A-Za-z-]*\) (.*/\1/p' "$1"
}

human_bytes() {
    awk -v b="${1:-0}" 'BEGIN {
        split("B KiB MiB GiB TiB", u, " ")
        i = 1
        while (b >= 1024 && i < 5) { b /= 1024; i++ }
        printf (i == 1 ? "%d %s\n" : "%.1f %s\n"), b, u[i]
    }'
}

# seg_rate lifts the byte rate out of a segment line. Warp writes the rate and
# the object rate on the same line and omits the former entirely at zero, so
# matching the unit is the only way to tell an absent rate from a slow one.
seg_rate() {
    grep -o -m1 '[0-9.]\+ \?\([KMGT]iB\|B\)/s' <<< "${1:-}" || true
}

# to_mib turns a warp rate such as 761.0MiB/s or 1.2GiB/s into a bare number of
# MiB/s, so the segment figures can be compared to each other.
to_mib() {
    awk -v s="${1:-}" 'BEGIN {
        if (match(s, /^[0-9.]+/) == 0) { print ""; exit }
        n = substr(s, 1, RLENGTH) + 0
        unit = substr(s, RLENGTH + 1)
        if (unit ~ /^KiB/) n /= 1024
        else if (unit ~ /^GiB/) n *= 1024
        else if (unit ~ /^TiB/) n *= 1024 * 1024
        printf "%.2f\n", n
    }'
}

emit_workload_rows() {
    local config_dir="$1" f name reqs size thr objs conc hosts ran

    for f in $(latency_files "$config_dir"); do
        name="$(workload_name "$f")"
        [ -n "$name" ] || continue

        reqs="$(report_field 's/^Report: [A-Za-z-]* (\([0-9]*\) reqs).*/\1/p' "$f")"
        ran="$(report_field 's/^Report: .*Ran Duration: \([^,]*\),.*/\1/p' "$f")"
        size="$(report_field 's/.*Size: \([0-9]*\) bytes.*/\1/p' "$f")"
        conc="$(report_field 's/.*Concurrency: \([0-9]*\)\..*/\1/p' "$f")"
        hosts="$(report_field 's/.*Hosts: \([0-9]*\)\..*/\1/p' "$f")"
        thr="$(report_field 's/^ \* Average: \([^,]*\),.*/\1/p' "$f")"
        objs="$(report_field 's/^ \* Average: [^,]*, \([0-9.]*\) obj\/s.*/\1/p' "$f")"

        printf '| %s | %s | %s | %s | %s | %s | %s | %s |\n' \
            "$name" "${reqs:--}" "$(human_bytes "$size")" "${conc:--}" \
            "${hosts:--}" "${ran:--}" "${thr:--}" "${objs:--}"
    done
}

# Standard deviation and the fastest request are here rather than in the
# throughput table because they say how tight the distribution was, which a
# mean and four percentiles do not.
emit_latency_rows() {
    local config_dir="$1" f name fastest avg p50 p90 p99 slowest stddev

    for f in $(latency_files "$config_dir"); do
        name="$(workload_name "$f")"
        [ -n "$name" ] || continue

        avg="$(report_field 's/^ \* Reqs: Avg: \([^,]*\),.*/\1/p' "$f")"
        p50="$(report_field 's/^ \* Reqs:.*[^0-9]50%: \([^,]*\),.*/\1/p' "$f")"
        p90="$(report_field 's/^ \* Reqs:.*[^0-9]90%: \([^,]*\),.*/\1/p' "$f")"
        p99="$(report_field 's/^ \* Reqs:.*[^0-9]99%: \([^,]*\),.*/\1/p' "$f")"
        fastest="$(report_field 's/^ \* Reqs:.*Fastest: \([^,]*\),.*/\1/p' "$f")"
        slowest="$(report_field 's/^ \* Reqs:.*Slowest: \([^,]*\),.*/\1/p' "$f")"
        stddev="$(report_field 's/^ \* Reqs:.*StdDev: \([^,]*\).*/\1/p' "$f")"

        printf '| %s | %s | %s | %s | %s | %s | %s | %s |\n' "$name" \
            "${fastest:--}" "${avg:--}" "${p50:--}" "${p90:--}" \
            "${p99:--}" "${slowest:--}" "${stddev:--}"
    done
}

# Warp splits the run into one-second segments and reports the best, median and
# worst. The spread between them is what says whether a headline average
# describes a steady run or one that stalled and caught up, and it is the
# figure a throughput gate has to be tolerant of.
emit_segments() {
    local config_dir="$1" f name split fast med slow spread printed=0 stalled=0

    for f in $(latency_files "$config_dir"); do
        grep -q '^ \* Fastest: ' "$f" || continue
        name="$(workload_name "$f")"
        fast="$(seg_rate "$(grep -m1 '^ \* Fastest: ' "$f")")"
        med="$(seg_rate "$(grep -m1 '^ \* 50% Median: ' "$f")")"
        slow="$(seg_rate "$(grep -m1 '^ \* Slowest: ' "$f")")"
        split="$(report_field 's/^Throughput, split into \(.*\):/\1/p' "$f")"

        # Warp prints no rate at all for a segment that moved no bytes, so an
        # absent figure is a whole second in which the workload did nothing.
        if [ -z "$slow" ]; then
            slow='0 B/s'
            stalled=1
        fi

        spread="$(awk -v f="$(to_mib "$fast")" -v s="$(to_mib "$slow")" \
            -v m="$(to_mib "$med")" 'BEGIN {
                if (m == "" || m + 0 == 0 || f == "") { print "-"; exit }
                printf "%.1f%%\n", (f - (s == "" ? 0 : s)) / m * 100
            }')"

        if [ "$printed" -eq 0 ]; then
            printf '\n### Throughput stability\n\n'
            printf 'Throughput per segment, and the spread between the best and worst as a share of the median.\n\n'
            printf '| Workload | Segments | Fastest | Median | Slowest | Spread |\n'
            printf '|---|---|---:|---:|---:|---:|\n'
            printed=1
        fi
        printf '| %s | %s | %s | %s | %s | %s |\n' \
            "$name" "${split:--}" "${fast:--}" "${med:--}" "$slow" "$spread"
    done

    [ "$stalled" -eq 0 ] || printf '\nA slowest segment of 0 B/s is a full second in which the workload completed nothing.\n'
}

# The TTFB line only appears on read workloads, and it is the number a
# throughput gate should key on rather than mean bandwidth.
emit_ttfb() {
    local config_dir="$1" f name line printed=0

    for f in $(latency_files "$config_dir"); do
        line="$(grep -m1 '^ \* TTFB:' "$f" 2>/dev/null || true)"
        [ -n "$line" ] || continue
        name="$(workload_name "$f")"

        if [ "$printed" -eq 0 ]; then
            printf '\n### Time to first byte\n\n'
            printf '| Workload | Best | Avg | 25th | Median | 75th | 90th | 99th | Worst | StdDev |\n'
            printf '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n'
            printed=1
        fi
        printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' "$name" \
            "$(sed -n 's/.*Best: \([^,]*\),.*/\1/p' <<< "$line")" \
            "$(sed -n 's/.*TTFB: Avg: \([^,]*\),.*/\1/p' <<< "$line")" \
            "$(sed -n 's/.*25th: \([^,]*\),.*/\1/p' <<< "$line")" \
            "$(sed -n 's/.*Median: \([^,]*\),.*/\1/p' <<< "$line")" \
            "$(sed -n 's/.*75th: \([^,]*\),.*/\1/p' <<< "$line")" \
            "$(sed -n 's/.*90th: \([^,]*\),.*/\1/p' <<< "$line")" \
            "$(sed -n 's/.*99th: \([^,]*\),.*/\1/p' <<< "$line")" \
            "$(sed -n 's/.*Worst: \([^ ,]*\).*/\1/p' <<< "$line")" \
            "$(sed -n 's/.*Worst: [^ ,]* StdDev: \([^ ,]*\).*/\1/p' <<< "$line")"
    done
}

# Warp reports each gate separately. An uneven split is the interesting case:
# it says the load did not spread, which an aggregate figure hides.
emit_per_host() {
    local config_dir="$1" f name hosts host val n
    local -a names=()

    hosts="$(sed -n 's/^ \* \(https:\/\/[^:]*:[0-9]*\): Avg:.*/\1/p' "$config_dir"/*-latency.txt \
        2>/dev/null | sort -u)"
    [ -n "$hosts" ] || return 0

    for f in $(latency_files "$config_dir"); do
        name="$(workload_name "$f")"
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

# Warp says so when a workload gathered too few samples to be reliable, and it
# says it inside a report for a different workload. Unread, the run looks like
# a measurement when warp has already said it is not one.
emit_notices() {
    local config_dir="$1" f line printed=0

    for f in $(latency_files "$config_dir"); do
        while IFS= read -r line; do
            [ -n "$line" ] || continue
            if [ "$printed" -eq 0 ]; then
                printf '\n### Notices from warp\n\n'
                printed=1
            fi
            printf -- '- %s\n' "$line"
        done < <(grep -hE '^(Skipping|Warning|WARNING)' "$f" 2>/dev/null | sort -u || true)
    done
}

# The round trips either matched or the run stopped, so this is a table of what
# was actually compared rather than a claim that something was.
emit_correctness() {
    local config="$1" f name src dl printed=0

    f="$RUN_DIR/correctness/$config/sha256.txt"
    [ -f "$f" ] || return 0

    while IFS= read -r name; do
        src="$(sed -n "s/^${name}_source=//p" "$f" | head -1)"
        dl="$(sed -n "s/^${name}_download=//p" "$f" | head -1)"
        [ -n "$src" ] || continue
        if [ "$printed" -eq 0 ]; then
            printf '\n### Correctness round trips\n\n'
            printf '| Object | SHA256 | Match |\n|---|---|---|\n'
            printed=1
        fi
        # Backticks are markdown for the summary, not a substitution.
        # shellcheck disable=SC2016
        printf '| %s | `%s` | %s |\n' "$name" "${src:0:16}" \
            "$([ "$src" = "$dl" ] && echo yes || echo NO)"
    done < <(sed -n 's/_source=.*//p' "$f" | sort -u)
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

# The results root says where the pass ran and the preset says how hard it
# pushed. Two passes share each, so the heading needs both to be unambiguous.
pass_label() {
    local place preset
    case "$(basename "$(dirname "$RUN_DIR")")" in
        *-loopback-small) place="loopback, small objects" ;;
        *-loopback) place="loopback" ;;
        *) place="$([ -n "$(info_field external_hosts)" ] && echo "bare metal" || echo "loopback")" ;;
    esac
    preset="$(info_field preset)"
    printf '%s%s' "$place" "${preset:+ — $preset}"
}

render_run() {
    RUN_DIR="${1%/}"
    INFO="$RUN_DIR/run-info.txt"

    local sha dirty external mp_parts config config_dir

    printf '## Performance — %s\n\n' "$(pass_label)"

    sha="$(info_field predastore_sha)"
    dirty="$(info_field predastore_dirty)"
    external="$(info_field external_hosts)"
    mp_parts="$(info_field multipart_parts)"
    {
        printf '| | |\n|---|---|\n'
        if [ -n "$sha" ]; then
            # Backticks are markdown for the summary, not a substitution.
            # shellcheck disable=SC2016
            printf '| Commit | `%s`%s |\n' "${sha:0:12}" \
                "$([ "$dirty" = true ] && echo ' (working tree dirty)' || echo '')"
        fi
        printf '| Preset | `%s`, %s per workload, %s concurrent |\n' \
            "$(info_field preset)" "$(info_field duration)" "$(info_field concurrent)"
        printf '| Object sizes | PUT %s, multipart %s x %s, GET %s |\n' \
            "$(info_field put_size)" "$(info_field multipart_part_size)" \
            "${mp_parts:--}" "$(info_field get_object_size)"
        if [ -n "$external" ]; then
            printf '| Cluster | bare metal, `%s` |\n' "$external"
        else
            printf '| Cluster | loopback profiles on one machine |\n'
        fi
        printf '| Measured from | %s, %s logical CPUs, %s RAM |\n' \
            "$(info_field host)" "$(info_field logical_cpus)" \
            "$(human_bytes "$(info_field memory_bytes)")"
        printf '| Go | %s |\n' "$(info_field go_version)"
        printf '| Warp | %s |\n' "$(info_field warp_version)"
    } | sed 's/ | *|$/ | |/'

    for config_dir in "$RUN_DIR"/*/; do
        config="$(basename "$config_dir")"
        case "$config" in logs|correctness) continue ;; esac
        ls "$config_dir"/*-latency.txt >/dev/null 2>&1 || continue

        printf '\n### Workloads — `%s`\n\n' "$config"
        printf '| Workload | Reqs | Object | Concurrency | Hosts | Ran | Throughput | Objects/s |\n'
        printf '|---|---:|---:|---:|---:|---:|---:|---:|\n'
        emit_workload_rows "$config_dir"

        printf '\n### Request latency\n\n'
        printf '| Workload | Fastest | Avg | p50 | p90 | p99 | Slowest | StdDev |\n'
        printf '|---|---:|---:|---:|---:|---:|---:|---:|\n'
        emit_latency_rows "$config_dir"

        emit_segments "$config_dir"
        emit_ttfb "$config_dir"
        emit_per_host "$config_dir"
        emit_correctness "$config"
        emit_notices "$config_dir"
    done
}

for run in "${RUN_DIRS[@]}"; do
    if [ ! -d "$run" ]; then
        echo "no such run directory: $run" >&2
        continue
    fi
    render_run "$run"
    printf '\n'
done

# Once, at the end. The hosts are the same for every pass, and the figure is
# what they had left when the run finished rather than per pass.
emit_disk
