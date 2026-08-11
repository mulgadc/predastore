#!/bin/bash
#
# bench-cluster.sh - Start a Predastore cluster, run warp benchmark, then stop
#
# Warp is pointed at every gate the profile declares, so how the cluster is
# spread across hosts and which transport its nodes use follow from the
# profile rather than from a flag here.
#
# Usage:
#   ./scripts/bench/bench-cluster.sh <clustername>
#
# Environment variables for warp tuning (unset = warp defaults):
#   WARP_OBJECTS      Number of objects
#   WARP_OBJ_SIZE     Object size (e.g. 10MiB)
#   WARP_DURATION     Test duration (e.g. 5m)
#   WARP_CONCURRENT   Concurrent operations
#
# Examples:
#   ./scripts/bench/bench-cluster.sh 1host
#   WARP_DURATION=1m WARP_CONCURRENT=8 ./scripts/bench/bench-cluster.sh 5host
#

set -euo pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
REPO_DIR="$SCRIPT_DIR/../.."
CONFIG_DIR="$REPO_DIR/config"
SCRIPTS_DIR="$SCRIPT_DIR/.."
PREDA_DIR="${PREDA_DIR:-/tmp/predastore}"
export PREDA_DIR

# shellcheck source=scripts/lib.sh
source "$SCRIPTS_DIR/lib.sh"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# --- Validate argument ---

if [ $# -lt 1 ]; then
    echo "Usage: $0 <clustername>"
    exit 1
fi

CLUSTER_NAME="$1"
CONFIG_FILE="$CONFIG_DIR/${CLUSTER_NAME}.toml"

if [ ! -f "$CONFIG_FILE" ]; then
    log_error "Config not found: $CONFIG_FILE"
    exit 1
fi

command -v warp >/dev/null || { log_error "warp not on PATH"; exit 1; }

# --- Always stop cluster on exit ---

cleanup() {
    "$SCRIPTS_DIR/clean.sh"
}
trap cleanup EXIT INT TERM

# --- Parse config ---

REGION=$(grep -E '^\s*region\s*=' "$CONFIG_FILE" | head -1 | sed 's/.*=\s*"\(.*\)".*/\1/')
ACCESS_KEY=$(awk '/^\[\[auth\]\]/{a=1} a && /access_key_id/{gsub(/.*= *"/,""); gsub(/".*/,""); print; exit}' "$CONFIG_FILE")
SECRET_KEY=$(awk '/^\[\[auth\]\]/{a=1} a && /secret_access_key/{gsub(/.*= *"/,""); gsub(/".*/,""); print; exit}' "$CONFIG_FILE")

# Warp spreads its load over every gate the profile declares.
HOST_LIST="$(gate_endpoints "$CONFIG_FILE" | paste -sd,)"
if [ -z "$HOST_LIST" ]; then
    log_error "no host in $CONFIG_FILE runs a gate — nothing serves S3"
    exit 1
fi
HOST_COUNT="$(parse_hosts "$CONFIG_FILE" | wc -l)"

# --- Start cluster ---

log_info "Starting cluster '$CLUSTER_NAME' across $HOST_COUNT host(s)..."
"$SCRIPTS_DIR/start.sh" -w "$CLUSTER_NAME"

# --- Results directory ---

STAMP="$(date -u +%Y-%m-%dT%H%M%SZ)"
RESULTS_DIR="$SCRIPT_DIR/results/${CLUSTER_NAME}-$STAMP"
mkdir -p "$RESULTS_DIR"
cp "$CONFIG_FILE" "$RESULTS_DIR/cluster.toml"

# --- Redirect warp temp dir off tmpfs ---
#
# Warp writes multi-MB upload payloads to TMPDIR (distributed-put-* files).
# On hosts where /tmp is a small tmpfs this competes with the cluster data.
# Place warp's temp files under PREDA_DIR so they share the same filesystem
# as the cluster and are cleaned up together.

WARP_TMPDIR="$PREDA_DIR/.warp-tmp"
mkdir -p "$WARP_TMPDIR"
export TMPDIR="$WARP_TMPDIR"

# --- Run warp ---

warp_args=()
[ -n "${WARP_OBJECTS:-}" ]    && warp_args+=(--objects="$WARP_OBJECTS")
[ -n "${WARP_OBJ_SIZE:-}" ]   && warp_args+=(--obj.size="$WARP_OBJ_SIZE")
[ -n "${WARP_DURATION:-}" ]   && warp_args+=(--duration="$WARP_DURATION")
[ -n "${WARP_CONCURRENT:-}" ] && warp_args+=(--concurrent="$WARP_CONCURRENT")

log_info "Running warp mixed against $HOST_LIST"
warp mixed \
    --host="$HOST_LIST" \
    --tls --insecure \
    --region="$REGION" \
    --access-key="$ACCESS_KEY" \
    --secret-key="$SECRET_KEY" \
    --bucket=predastore-bench \
    --benchdata="$RESULTS_DIR/warp-mixed" \
    "${warp_args[@]}"

# --- Preserve run metadata ---

{
    echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "hostname=$(hostname)"
    echo "cluster=$CLUSTER_NAME"
    echo "hosts=$HOST_LIST"
    echo "predastore_sha=$(git -C "$REPO_DIR" rev-parse HEAD 2>/dev/null || echo unknown)"
    echo "warp_version=$(warp --version 2>/dev/null | head -n1 || echo unknown)"
} > "$RESULTS_DIR/run-info.txt"

log_info "Done. Results under $RESULTS_DIR"
