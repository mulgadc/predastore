#!/bin/bash
#
# start.sh - Start a Predastore cluster from a config profile
#
# Usage:
#   ./scripts/start.sh [-w] <clustername>
#
# Options:
#   -w    Wait for all nodes to become ready (60s timeout)
#
# Examples:
#   ./scripts/start.sh 3node
#   ./scripts/start.sh -w 5node
#

set -euo pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
REPO_DIR="$SCRIPT_DIR/.."
CONFIG_DIR="$REPO_DIR/config"
S3D_BINARY="$REPO_DIR/bin/s3d"
S3_PORT=8443

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# --- Parse options ---

WAIT_READY=false

while getopts "w" opt; do
    case $opt in
        w) WAIT_READY=true ;;
        *) echo "Usage: $0 [-w] <clustername>"; exit 1 ;;
    esac
done
shift $((OPTIND - 1))

# --- Validate argument ---

if [ $# -ne 1 ]; then
    echo "Usage: $0 [-w] <clustername>"
    echo ""
    echo "Available clusters:"
    for f in "$CONFIG_DIR"/*.toml; do
        [ -f "$f" ] && echo "  $(basename "$f" .toml)"
    done
    exit 1
fi

CLUSTER_NAME="$1"
CONFIG_FILE="$CONFIG_DIR/${CLUSTER_NAME}.toml"

if [ ! -f "$CONFIG_FILE" ]; then
    log_error "Config not found: $CONFIG_FILE"
    echo "Available clusters:"
    for f in "$CONFIG_DIR"/*.toml; do
        [ -f "$f" ] && echo "  $(basename "$f" .toml)"
    done
    exit 1
fi

# --- Paths ---

ROOT="${PREDA_DIR:-/tmp/predastore}"
BASE="$ROOT/${CLUSTER_NAME}"
LOGS="$BASE/logs"
PIDS="$BASE/pids"

# --- Collision check ---

if [ -d "$PIDS" ]; then
    for pidfile in "$PIDS"/*.pid; do
        [ -f "$pidfile" ] || continue
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            log_error "Cluster '$CLUSTER_NAME' is already running (PID $pid from $(basename "$pidfile"))"
            log_error "Run ./scripts/stop.sh first"
            exit 1
        else
            # Stale PID file — clean it up
            rm -f "$pidfile"
        fi
    done
fi

# --- Create directories ---

mkdir -p "$ROOT" "$LOGS" "$PIDS"

# --- Parse host IPs from config ---

parse_host_ips() {
    grep -E '^\s*host\s*=' "$CONFIG_FILE" | \
        sed 's/.*=\s*"\(.*\)".*/\1/' | \
        grep -v '0\.0\.0\.0' | \
        sort -u
}

# --- Generate certs ---

TLS_KEY="$ROOT/server.key"
TLS_CERT="$ROOT/server.pem"

if [ ! -f "$TLS_CERT" ]; then
    SAN="DNS:localhost,IP:127.0.0.1"
    for ip in $(parse_host_ips); do
        SAN="${SAN},IP:${ip}"
    done

    log_info "Generating TLS certificates..."
    openssl req -x509 -newkey rsa:2048 -nodes \
        -keyout "$TLS_KEY" -out "$TLS_CERT" \
        -days 3650 -subj '/CN=localhost' \
        -addext "subjectAltName=${SAN}" \
        2>/dev/null
    log_info "Certificates written to $ROOT/"
fi

# --- Generate master encryption key ---
#
# s3d's keyfile loader is fail-closed on group/other-readable modes, so the
# key file must be 0600. Create it under a tightened umask to avoid a
# briefly world-readable window between open and chmod.

MASTER_KEY="$ROOT/master.key"

if [ ! -f "$MASTER_KEY" ]; then
    log_info "Generating AES-256 master key..."
    ( umask 0177 && openssl rand -out "$MASTER_KEY" 32 )
    log_info "Master key written to $MASTER_KEY"
fi

# --- Build s3d if needed ---

if [ ! -f "$S3D_BINARY" ]; then
    log_warn "s3d binary not found, building..."
    make -C "$REPO_DIR" build
fi

log_info "Setting up loopback IP aliases..."
for ip in $(parse_host_ips); do
    if ! ip addr show lo | grep -qw "$ip"; then
        sudo ip addr add "${ip}/24" dev lo
        log_info "  Added $ip to lo"
    fi
done

# --- Parse db nodes (id + host) from [[db]] sections ---

# Emits "id host" pairs, e.g. "1 10.11.12.1"
parse_db_nodes() {
    awk '
    /^\[\[db\]\]/                              { in_db=1; id=""; host="" }
    /^\[/ && !/^\[\[db\]\]/                    { in_db=0 }
    in_db && /^[[:space:]]*id[[:space:]]*=/     { gsub(/[[:space:]]/, ""); split($0,a,"="); id=a[2] }
    in_db && /^[[:space:]]*host[[:space:]]*=/   { gsub(/.*= *"/,""); gsub(/".*/,""); host=$0 }
    in_db && id != "" && host != ""            { print id, host; in_db=0 }
    ' "$CONFIG_FILE"
}

DB_NODES=$(parse_db_nodes)

if [ -z "$DB_NODES" ]; then
    log_error "No database nodes found in $CONFIG_FILE"
    exit 1
fi

log_info "Launching cluster '$CLUSTER_NAME' (nodes: $(echo "$DB_NODES" | awk '{printf "%s ", $1}'))"

# --- Launch nodes ---

echo "$DB_NODES" | while read -r node_id node_host; do
    log_file="$LOGS/node-${node_id}.log"
    pid_file="$PIDS/node-${node_id}.pid"

    nohup "$S3D_BINARY" \
        -config "$CONFIG_FILE" \
        -node "$node_id" \
        -host "$node_host" \
        -port "$S3_PORT" \
        -base-path "$BASE" \
        -tls-key "$TLS_KEY" \
        -tls-cert "$TLS_CERT" \
        -encryption-key-file "$MASTER_KEY" \
        > "$log_file" 2>&1 &

    pid=$!
    echo "$pid" > "$pid_file"
    log_info "  Node $node_id started (PID: $pid, https://${node_host}:${S3_PORT})"
done

# --- Wait for readiness ---

if [ "$WAIT_READY" = true ]; then
    log_info "Waiting for cluster readiness (60s timeout)..."
    deadline=$(( $(date +%s) + 60 ))
    for ip in $(parse_host_ips); do
        while :; do
            if curl -k -s "https://${ip}:${S3_PORT}/" >/dev/null 2>&1; then
                log_info "  $ip ready"
                break
            fi
            if [ "$(date +%s)" -ge "$deadline" ]; then
                log_error "Node $ip did not become ready within 60s"
                log_error "Check logs in $LOGS/"
                exit 1
            fi
            sleep 1
        done
    done
else
    sleep 1
fi

log_info ""
log_info "Cluster '$CLUSTER_NAME' launched!"
log_info "  Base:  $BASE"
log_info "  Logs:  $LOGS/"
log_info "  PIDs:  $PIDS/"
log_info ""
log_info "Stop with: ./scripts/stop.sh"
