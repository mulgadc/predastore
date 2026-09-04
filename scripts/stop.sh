#!/bin/bash
#
# stop.sh - Stop all running Predastore clusters
#
# Scans $PREDA_DIR/*/pids/ and stops any running processes, confirming each one
# has actually exited before reporting success.
#
# Usage:
#   ./scripts/stop.sh
#
# Environment:
#   PREDA_DIR      cluster root to scan (default /tmp/predastore)
#   STOP_TIMEOUT   seconds to wait after SIGTERM before SIGKILL (default 20)
#   KILL_TIMEOUT   seconds to wait after SIGKILL before failing (default 5)
#

set -euo pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
REPO_DIR="$SCRIPT_DIR/.."
CONFIG_DIR="$REPO_DIR/config"

# shellcheck source=scripts/lib.sh
source "$SCRIPT_DIR/lib.sh"

BASE_DIR="${PREDA_DIR:-/tmp/predastore}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

# A cluster launched against a different PREDA_DIR has no pidfile here, so it
# survives this script silently and keeps holding its data directory. Report
# those rather than kill them: a cluster outside this root may not be ours.
report_stray() {
    local stray
    stray=$(pgrep -af '/bin/s3d -config' 2>/dev/null | grep -Fv "$BASE_DIR" || true)
    [ -n "$stray" ] || return 0
    log_warn "s3d processes outside $BASE_DIR are still running:"
    while IFS= read -r line; do
        log_warn "  $line"
    done <<< "$stray"
    log_warn "Set PREDA_DIR to their root and re-run, or stop them by hand."
}

if [ ! -d "$BASE_DIR" ]; then
    log_info "Nothing to stop — $BASE_DIR does not exist"
    report_stray
    exit 0
fi

# How long a signalled process gets before the next signal. A clean shutdown
# flushes Badger and closes the raft log, which is seconds rather than
# milliseconds on a cluster that has just been benchmarked.
STOP_TIMEOUT="${STOP_TIMEOUT:-20}"
KILL_TIMEOUT="${KILL_TIMEOUT:-5}"

# alive answers whether a pid exists, which is not the same question `kill -0`
# answers. A process owned by another user fails `kill -0` with EPERM, so a
# cluster started under sudo or by CI would read as already stopped while it
# kept its ports. /proc does not confuse the two.
alive() {
    if [ -d /proc ]; then
        [ -d "/proc/$1" ]
    else
        kill -0 "$1" 2>/dev/null
    fi
}

# wait_for_exit polls the given pids and echoes those still alive when it gives
# up. Polling is what this needs: the processes are not children of this shell,
# so there is nothing to `wait` on.
wait_for_exit() {
    local deadline=$(( SECONDS + $1 )); shift
    local pid remaining
    while :; do
        remaining=()
        for pid in "$@"; do
            alive "$pid" && remaining+=("$pid")
        done
        [ ${#remaining[@]} -eq 0 ] && return 0
        [ "$SECONDS" -ge "$deadline" ] && break
        sleep 0.2
    done
    printf '%s\n' "${remaining[@]}"
}

stopped=0
pids=()
declare -A pid_label=() pid_file=()

for pid_dir in "$BASE_DIR"/*/pids; do
    [ -d "$pid_dir" ] || continue
    cluster=$(basename "$(dirname "$pid_dir")")

    for pidfile in "$pid_dir"/*.pid; do
        [ -f "$pidfile" ] || continue
        pid=$(cat "$pidfile")
        node=$(basename "$pidfile" .pid)

        if alive "$pid"; then
            log_info "Stopping $cluster/$node (PID: $pid)"
            kill "$pid" 2>/dev/null || true
            pids+=("$pid")
            pid_label[$pid]="$cluster/$node"
            pid_file[$pid]="$pidfile"
            stopped=$((stopped + 1))
        else
            rm -f "$pidfile"
        fi
    done
done

# Signalling is not stopping. An s3d that is slow to close its raft log, or that
# ignores SIGTERM outright, keeps its listening ports and its data directory, and
# the next start.sh then fails at readiness looking like a fault in whatever was
# just built. Deleting the pidfile here is what made that invisible, so it is
# deleted once the process is gone rather than once it has been signalled.
if [ ${#pids[@]} -gt 0 ]; then
    mapfile -t survivors < <(wait_for_exit "$STOP_TIMEOUT" "${pids[@]}")

    if [ ${#survivors[@]} -gt 0 ]; then
        for pid in "${survivors[@]}"; do
            log_warn "${pid_label[$pid]} (PID: $pid) ignored SIGTERM after ${STOP_TIMEOUT}s — sending SIGKILL"
            kill -9 "$pid" 2>/dev/null || true
        done
        mapfile -t survivors < <(wait_for_exit "$KILL_TIMEOUT" "${survivors[@]}")
    fi

    for pid in "${pids[@]}"; do
        alive "$pid" || rm -f "${pid_file[$pid]}"
    done

    if [ ${#survivors[@]} -gt 0 ]; then
        for pid in "${survivors[@]}"; do
            log_error "${pid_label[$pid]} (PID: $pid) survived SIGKILL and still holds its ports"
        done
        log_error "Refusing to report a stop that did not happen. Investigate before starting another cluster."
        exit 1
    fi
fi

# Teardown loopback IPs for each cluster that has a matching config. Only
# addresses start.sh could have aliased are removed: loopback is the machine's
# own and a single-host profile never aliased anything.
for cluster_dir in "$BASE_DIR"/*/; do
    [ -d "$cluster_dir" ] || continue
    cluster=$(basename "$cluster_dir")
    config="$CONFIG_DIR/${cluster}.toml"
    [ -f "$config" ] || continue

    # A profile with no routable host is normal, not an error: the pipeline
    # must not abort the script under `set -e`.
    for ip in $(routable_addrs "$config" || true); do
        sudo ip addr del "${ip}/24" dev lo 2>/dev/null || true
    done
done

if [ "$stopped" -eq 0 ]; then
    log_info "No running processes found"
else
    log_info "Stopped $stopped process(es)"
fi

report_stray
