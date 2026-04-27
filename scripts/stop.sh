#!/bin/bash
#
# stop.sh - Stop all running Predastore clusters
#
# Scans /tmp/predastore/*/pids/ and kills any running processes.
#
# Usage:
#   ./scripts/stop.sh
#

set -euo pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
REPO_DIR="$SCRIPT_DIR/.."
CONFIG_DIR="$REPO_DIR/config"

BASE_DIR="/tmp/predastore"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }

if [ ! -d "$BASE_DIR" ]; then
    log_info "Nothing to stop — $BASE_DIR does not exist"
    exit 0
fi

stopped=0

for pid_dir in "$BASE_DIR"/*/pids; do
    [ -d "$pid_dir" ] || continue
    cluster=$(basename "$(dirname "$pid_dir")")

    for pidfile in "$pid_dir"/*.pid; do
        [ -f "$pidfile" ] || continue
        pid=$(cat "$pidfile")
        node=$(basename "$pidfile" .pid)

        if kill -0 "$pid" 2>/dev/null; then
            log_info "Stopping $cluster/$node (PID: $pid)"
            kill "$pid" 2>/dev/null || true
            stopped=$((stopped + 1))
        fi
        rm -f "$pidfile"
    done
done

# Teardown loopback IPs for each cluster that has a matching config
for cluster_dir in "$BASE_DIR"/*/; do
    [ -d "$cluster_dir" ] || continue
    cluster=$(basename "$cluster_dir")
    config="$CONFIG_DIR/${cluster}.toml"
    [ -f "$config" ] || continue

    ips=$(grep -E '^\s*host\s*=' "$config" | \
        sed 's/.*=\s*"\(.*\)".*/\1/' | \
        grep -v '0\.0\.0\.0' | \
        sort -u)

    for ip in $ips; do
        sudo ip addr del "${ip}/24" dev lo 2>/dev/null || true
    done
done

if [ "$stopped" -eq 0 ]; then
    log_info "No running processes found"
else
    log_info "Stopped $stopped process(es)"
fi
