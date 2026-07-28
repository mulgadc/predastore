#!/bin/bash
#
# clean.sh - Stop all clusters and remove cluster data (preserves $PREDA_DIR root and certs)
#
# Usage:
#   ./scripts/clean.sh
#

set -euo pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"

# Colors
GREEN='\033[0;32m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }

# Stop any running processes first. A failed teardown must not prevent the
# cleanup below: leaving gigabytes behind is worse than a stale loopback alias.
if ! "$SCRIPT_DIR/stop.sh"; then
    echo "[WARN] stop.sh failed; cleaning up anyway" >&2
fi

PREDA_DIR="${PREDA_DIR:-/tmp/predastore}"

# Logs are retained outside the per-cluster directories, which this script
# removes wholesale. A cluster that failed to start is diagnosed from its
# logs, and deleting them along with the data leaves nothing to look at.
# The dot prefix keeps the retention directory out of the glob below.
RETAIN="$PREDA_DIR/.last-logs"

cleaned=0
for dir in "$PREDA_DIR"/*/; do
    [ -d "$dir" ] || continue
    cluster="$(basename "$dir")"

    if [ -d "$dir/logs" ]; then
        mkdir -p "$RETAIN"
        rm -rf "${RETAIN:?}/$cluster"
        cp -r "$dir/logs" "$RETAIN/$cluster"
    fi

    rm -rf "$dir"
    log_info "Removed $dir"
    cleaned=$((cleaned + 1))
done

if [ "$cleaned" -eq 0 ]; then
    log_info "Nothing to clean"
elif [ -d "$RETAIN" ]; then
    log_info "Logs retained under $RETAIN"
fi
