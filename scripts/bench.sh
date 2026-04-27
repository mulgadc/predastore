#!/bin/bash
#
# bench.sh - Predastore benchmark dispatcher
#
# Usage:
#   ./scripts/bench.sh disk
#   ./scripts/bench.sh <clustername>
#

set -euo pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
BENCH_DIR="$SCRIPT_DIR/bench"

if [ $# -lt 1 ]; then
    echo "Usage: $0 disk|<clustername>"
    echo ""
    echo "  disk            Run raw-disk fio benchmark"
    echo "  <clustername>   Run warp cluster benchmark (e.g. 3node, 5node)"
    exit 1
fi

if [ "$1" = "disk" ]; then
    exec "$BENCH_DIR/bench-disk.sh"
else
    exec "$BENCH_DIR/bench-cluster.sh" "$1"
fi
