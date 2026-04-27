#!/bin/bash
#
# clean.sh - Stop all clusters and remove cluster data (preserves /tmp/predastore and certs)
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

# Stop any running processes first
"$SCRIPT_DIR/stop.sh"

cleaned=0
for dir in /tmp/predastore/*/; do
    [ -d "$dir" ] || continue
    rm -rf "$dir"
    log_info "Removed $dir"
    cleaned=$((cleaned + 1))
done

if [ "$cleaned" -eq 0 ]; then
    log_info "Nothing to clean"
fi
