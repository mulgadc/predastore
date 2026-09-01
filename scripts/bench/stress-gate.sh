#!/usr/bin/env bash
#
# stress-gate.sh - Run e2e-stress scenarios and gate on a committed baseline.
#
# Some of the stress scenarios fail against predastore today, so a bare exit
# status makes the suite unusable as a gate: it would be red from its first run
# and read by nobody. Against a baseline the question becomes the useful one —
# did something that used to survive stop surviving.
#
# Scenario granularity rather than assertion granularity, because e2e-stress.sh
# aborts on its first failed assertion and writes no result file when it does.
# An assertion-level baseline would have nothing to compare after a hard
# failure, which is exactly the case a gate exists for.
#
# Usage:
#   ./scripts/bench/stress-gate.sh                    # every baselined scenario
#   ./scripts/bench/stress-gate.sh freeze             # named scenarios only
#
# Environment:
#   STRESS_BASELINE        Baseline file (default: scripts/stress-baseline.txt)
#   STRESS_WRITE_BASELINE  Record outcomes here instead of gating
#   STRESS_STRICT          1 to fail on any failing scenario, not just a
#                          regression. The baseline is still read, to separate
#                          a known gap from a new one in the report.
#   Everything e2e-stress.sh reads is passed through untouched.
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd -P)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd -P)"

BASELINE="${STRESS_BASELINE:-$REPO_DIR/scripts/stress-baseline.txt}"
WRITE_BASELINE="${STRESS_WRITE_BASELINE:-}"

# The scenarios e2e-stress.sh accepts for STRESS_SCENARIO. "all" is excluded on
# purpose: it chains them and stops at the first failure, so it cannot report
# the ones after a known-red one. Keep this in step with the case statement in
# e2e-stress.sh — a scenario missing here is rejected rather than run.
ALL_SCENARIOS=(
    repair handoff
    node-rejoin node-resync node-rebuild
    multipart-upload last-modified large-object concurrent-put
    partial-put torn-overwrite stale-shard freeze
)

if [ $# -gt 0 ]; then
    SCENARIOS=("$@")
else
    SCENARIOS=("${ALL_SCENARIOS[@]}")
fi

for s in "${SCENARIOS[@]}"; do
    found=false
    for known in "${ALL_SCENARIOS[@]}"; do
        [ "$s" = "$known" ] && found=true
    done
    if [ "$found" = false ]; then
        echo "stress-gate: unknown scenario '$s' (want: ${ALL_SCENARIOS[*]})" >&2
        exit 2
    fi
done

if [ -z "$WRITE_BASELINE" ] && [ ! -f "$BASELINE" ]; then
    echo "stress-gate: baseline $BASELINE does not exist" >&2
    exit 2
fi

# The pattern matches only a cluster e2e-stress.sh started, because its work
# directory is an mktemp under that name. A dev cluster, or the one an operator
# runs from /tmp/predastore, does not match and is never signalled.
STRAY_PATTERN='s3d -config [^ ]*predastore-e2e-stress\.'

# pgrep -f matches on the whole command line, which any shell holding this
# pattern in its own arguments satisfies. Every candidate is confirmed to be
# an s3d before it is signalled, so the reaper cannot turn on its caller.
stray_pids() {
    local pid
    for pid in $(pgrep -f "$STRAY_PATTERN" 2>/dev/null); do
        [ "$(cat "/proc/$pid/comm" 2>/dev/null)" = s3d ] && printf '%s\n' "$pid"
    done
}

# A scenario that fails can exit with its cluster still running. The next one
# then fails on the ports and loopback addresses it still holds, so one broken
# scenario reads as several, and the freed-but-open data keeps its disk space
# until the process dies. Reaping between scenarios keeps a result meaning what
# it says.
reap_strays() {
    local after="$1" pid waited=0
    [ -n "$(stray_pids)" ] || return 0

    printf '  reaping the cluster left running after %s\n' "$after" >&2
    [ -z "${GITHUB_ACTIONS:-}" ] \
        || printf '::warning::%s left its cluster running; the harness reaped it, but any scenario after it may have failed on the ports it held\n' "$after"

    for pid in $(stray_pids); do
        kill -TERM "$pid" 2>/dev/null || true
    done
    while [ "$waited" -lt 15 ] && [ -n "$(stray_pids)" ]; do
        sleep 1
        waited=$(( waited + 1 ))
    done
    for pid in $(stray_pids); do
        kill -KILL "$pid" 2>/dev/null || true
    done

    # Reported rather than removed: deleting an address needs root, and one
    # left behind is a symptom worth seeing rather than a thing to paper over.
    local aliases
    aliases="$(ip -4 -o addr show lo 2>/dev/null | awk '$4 ~ /^10\./ { print $4 }' | paste -sd' ')"
    [ -z "$aliases" ] || printf '  loopback aliases still assigned after %s: %s\n' "$after" "$aliases" >&2
}

RESULTS=()

# The runner is persistent, so a job that was cancelled part way through can
# leave a cluster behind for this one to trip over.
reap_strays "an earlier run"

for scenario in "${SCENARIOS[@]}"; do
    printf '\n=== scenario: %s ===\n' "$scenario"
    if STRESS_SCENARIO="$scenario" "$SCRIPT_DIR/e2e-stress.sh"; then
        RESULTS+=("PASS|$scenario")
        printf '  %s: PASS\n' "$scenario"
    else
        RESULTS+=("FAIL|$scenario")
        printf '  %s: FAIL\n' "$scenario"
    fi
    reap_strays "$scenario"
done

if [ -n "$WRITE_BASELINE" ]; then
    {
        echo "# Generated by scripts/bench/stress-gate.sh."
        echo "# Regenerate with STRESS_WRITE_BASELINE=scripts/stress-baseline.txt when a fix lands."
        printf '%s\n' "${RESULTS[@]}"
    } > "$WRITE_BASELINE"
    printf '\nwrote baseline to %s\n' "$WRITE_BASELINE"
    # Recording what predastore does today succeeded even though some of what
    # it does today is fail, so this exits clean.
    exit 0
fi

note() {
    [ -n "${GITHUB_ACTIONS:-}" ] || return 0
    printf '::%s::%s\n' "${2:-warning}" "$1"
}

regressions=0
fixed=0
failed=0
printf '\n=== gate ===\n'
for r in "${RESULTS[@]}"; do
    IFS='|' read -r status scenario <<< "$r"
    [ "$status" = FAIL ] && failed=$((failed + 1))
    want=$(grep -F "|$scenario" "$BASELINE" | cut -d'|' -f1 | head -1)
    if [ -z "$want" ]; then
        printf '  ? %s is not in the baseline\n' "$scenario"
        note "$scenario is not in the baseline"
        continue
    fi
    if [ "$want" = PASS ] && [ "$status" = FAIL ]; then
        printf '  REGRESSION %s survived in the baseline and fails now\n' "$scenario"
        note "REGRESSION: $scenario survived in the baseline and fails now" error
        regressions=$((regressions + 1))
    elif [ "$want" = FAIL ] && [ "$status" = PASS ]; then
        fixed=$((fixed + 1))
    elif [ "$status" = FAIL ]; then
        note "known gap: $scenario"
    fi
done

if [ "$fixed" -gt 0 ]; then
    printf '\n  %d scenario(s) now pass that the baseline expects to fail. Update %s.\n' \
        "$fixed" "$BASELINE"
fi

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
        printf '## Stress\n\n| Scenario | Result | Baseline |\n|---|---|---|\n'
        for r in "${RESULTS[@]}"; do
            IFS='|' read -r status scenario <<< "$r"
            want=$(grep -F "|$scenario" "$BASELINE" | cut -d'|' -f1 | head -1)
            # Backticks are markdown for the summary, not a substitution.
            # shellcheck disable=SC2016
            printf '| `%s` | %s | %s |\n' "$scenario" "$status" "${want:-not baselined}"
        done
        printf '\n%d regression(s), %d newly passing.\n\n' "$regressions" "$fixed"
    } >> "$GITHUB_STEP_SUMMARY"
fi

printf '\n'

# Strict asks whether the cluster survives the fault; the default asks whether
# this change made it survive less well than it used to.
if [ "${STRESS_STRICT:-}" = 1 ]; then
    [ "$failed" -eq 0 ]
    exit $?
fi
[ "$regressions" -eq 0 ]
