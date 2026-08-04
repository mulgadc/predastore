#!/bin/bash
#
# start.sh - Start a Predastore cluster from a config profile
#
# Usage:
#   ./scripts/start.sh [-w] [-s] <clustername>
#
# Options:
#   -w    Wait for all nodes to become ready (60s timeout)
#   -s    Split the cluster across one process per host. Without it the whole
#         topology runs in a single process over the in-process pipe, which
#         needs no loopback aliases, no certificates and no network socket.
#
# Examples:
#   ./scripts/start.sh 3node          # one process, every node
#   ./scripts/start.sh -s 3node       # one process per host, over quic
#   ./scripts/start.sh -w -s 5node
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
SPLIT=false

while getopts "ws" opt; do
    case $opt in
        w) WAIT_READY=true ;;
        s) SPLIT=true ;;
        *) echo "Usage: $0 [-w] [-s] <clustername>"; exit 1 ;;
    esac
done
shift $((OPTIND - 1))

# --- Validate argument ---

if [ $# -ne 1 ]; then
    echo "Usage: $0 [-w] [-s] <clustername>"
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

if ! grep -qE '^\s*\[\[host\]\]' "$CONFIG_FILE"; then
    log_error "$CONFIG_FILE has no [[host]] topology"
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

# Emits each host's public IP. An empty result is normal for a config with no
# routable hosts, so the pipeline must not abort under `set -e`.
parse_host_ips() {
    grep -E '^\s*public_addr\s*=' "$CONFIG_FILE" | \
        sed 's/.*=\s*"\(.*\)".*/\1/' | \
        cut -d: -f1 | \
        grep -v '0\.0\.0\.0' | \
        sort -u || true
}

# --- Generate certs ---

TLS_KEY="$ROOT/server.key"
TLS_CERT="$ROOT/server.pem"

SAN="DNS:localhost,IP:127.0.0.1"
for ip in $(parse_host_ips); do
    SAN="${SAN},IP:${ip}"
done

# Regenerate whenever the existing certificate does not cover every host in
# this config. $PREDA_DIR is shared across clusters, so a cert left by a
# smaller topology otherwise survives and peers fail verification at dial
# time with an error that points at TLS rather than at the stale cert.
cert_covers_hosts() {
    [ -f "$TLS_CERT" ] || return 1
    local have
    have=$(openssl x509 -in "$TLS_CERT" -noout -ext subjectAltName 2>/dev/null) || return 1
    for ip in $(parse_host_ips); do
        echo "$have" | grep -qw "$ip" || return 1
    done
    return 0
}

if ! cert_covers_hosts; then
    [ -f "$TLS_CERT" ] && log_warn "Existing certificate does not cover every host, regenerating"

    log_info "Generating TLS certificates..."
    openssl req -x509 -newkey rsa:2048 -nodes \
        -keyout "$TLS_KEY" -out "$TLS_CERT" \
        -days 3650 -subj '/CN=localhost' \
        -addext "subjectAltName=${SAN}" \
        2>/dev/null
    log_info "Certificates written to $ROOT/"
fi

# --- Install cert into the OS trust store ---
#
# s3d verifies inter-node Raft and QUIC peer certs strictly against the OS
# trust store (quicclient.tlsConfigForDial / raft_streamlayer.Dial), with no
# CA-bundle flag. A self-signed cert that is not a trust anchor fails with
# "x509: certificate signed by unknown authority", so the cluster never elects
# a leader. Install it so the dialers trust it.

TRUST_ANCHOR="/usr/local/share/ca-certificates/predastore-${CLUSTER_NAME}.crt"

if [ "$SPLIT" = true ] && ! cmp -s "$TLS_CERT" "$TRUST_ANCHOR" 2>/dev/null; then
    log_info "Installing TLS cert into OS trust store..."
    sudo cp "$TLS_CERT" "$TRUST_ANCHOR"
    sudo update-ca-certificates >/dev/null
    log_info "Trust anchor installed at $TRUST_ANCHOR"
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

# Rebuild when the binary is missing or older than any source file: a stale
# binary silently benchmarks the wrong revision.
if [ ! -f "$S3D_BINARY" ] || [ -n "$(find "$REPO_DIR" -name '*.go' -newer "$S3D_BINARY" -print -quit 2>/dev/null)" ]; then
    log_warn "s3d binary missing or stale, building..."
    make -C "$REPO_DIR" build
fi

if [ "$SPLIT" = true ]; then
    log_info "Setting up loopback IP aliases..."
    for ip in $(parse_host_ips); do
        if ! ip addr show lo | grep -qw "$ip"; then
            sudo ip addr add "${ip}/24" dev lo
            log_info "  Added $ip to lo"
        fi
    done
fi

# --- Parse topology ---

# Emits "host_id ip" pairs from [[host]] blocks, e.g. "1 10.11.12.1".
parse_hosts() {
    awk '
    /^\[\[host\]\]/                            { in_h=1; id=""; ip="" }
    /^\[/ && !/^\[\[host\]\]/                  { in_h=0 }
    in_h && /^[[:space:]]*id[[:space:]]*=/       { gsub(/[[:space:]]/, ""); split($0,a,"="); id=a[2] }
    in_h && /^[[:space:]]*public_addr[[:space:]]*=/ { gsub(/.*= *"/,""); gsub(/".*/,""); split($0,b,":"); ip=b[1] }
    in_h && id != "" && ip != ""                 { print id, ip; in_h=0 }
    ' "$CONFIG_FILE"
}

# Emits the comma-separated node ids pinned to the given host id.
parse_host_nodes() {
    awk -v want="$1" '
    /^\[\[node\]\]/                            { in_n=1; id=""; host="" }
    /^\[/ && !/^\[\[node\]\]/                  { in_n=0 }
    in_n && /^[[:space:]]*id[[:space:]]*=/       { gsub(/[[:space:]]/, ""); split($0,a,"="); id=a[2] }
    in_n && /^[[:space:]]*host_id[[:space:]]*=/  { gsub(/[[:space:]]/, ""); split($0,b,"="); host=b[2] }
    in_n && id != "" && host != ""               { if (host == want) ids = ids (ids == "" ? "" : ",") id; in_n=0 }
    END                                          { print ids }
    ' "$CONFIG_FILE"
}

# launch starts one s3d process and records its pid.
# Args: label, s3 gateway ip, node selection ("" = every node)
launch() {
    local label="$1" ip="$2" node_sel="$3"
    local log_file="$LOGS/${label}.log"
    local pid_file="$PIDS/${label}.pid"
    local args=(
        -config "$CONFIG_FILE"
        -host "$ip"
        -port "$S3_PORT"
        -base-path "$BASE"
        -tls-key "$TLS_KEY"
        -tls-cert "$TLS_CERT"
        -encryption-key-file "$MASTER_KEY"
    )
    if [ -n "$node_sel" ]; then
        args+=(-nodes "$node_sel")
    fi

    nohup "$S3D_BINARY" "${args[@]}" > "$log_file" 2>&1 &
    local pid=$!
    echo "$pid" > "$pid_file"
    LAUNCHED_PIDS+=("$pid")
    LAUNCHED_IPS+=("$ip")
    log_info "  $label started (PID: $pid, https://${ip}:${S3_PORT})"
}

# --- Launch ---

LAUNCHED_PIDS=()
LAUNCHED_IPS=()

if [ "$SPLIT" = true ]; then
    log_info "Launching cluster '$CLUSTER_NAME' split across one process per host (quic transport)"
    while read -r host_id host_ip; do
        [ -n "$host_id" ] || continue
        node_sel="$(parse_host_nodes "$host_id")"
        if [ -z "$node_sel" ]; then
            log_warn "  host $host_id has no nodes, skipping"
            continue
        fi
        launch "host-${host_id}" "$host_ip" "$node_sel"
    done <<< "$(parse_hosts)"
else
    # No -nodes selection: every node in the topology runs in this process
    # over the pipe, so no socket, certificates or aliases are involved.
    log_info "Launching cluster '$CLUSTER_NAME' colocated in one process (pipe transport)"
    launch "colo" "127.0.0.1" ""
fi

if [ ${#LAUNCHED_PIDS[@]} -eq 0 ]; then
    log_error "No processes launched — check $CONFIG_FILE"
    exit 1
fi

# --- Wait for readiness ---

if [ "$WAIT_READY" = true ]; then
    log_info "Waiting for cluster readiness (60s timeout)..."
    deadline=$(( $(date +%s) + 60 ))
    for i in "${!LAUNCHED_PIDS[@]}"; do
        pid="${LAUNCHED_PIDS[$i]}"
        ip="${LAUNCHED_IPS[$i]}"
        while :; do
            if curl -k -s "https://${ip}:${S3_PORT}/" >/dev/null 2>&1; then
                log_info "  $ip ready"
                break
            fi
            if ! kill -0 "$pid" 2>/dev/null; then
                log_error "Process $pid exited during startup — see $LOGS/"
                exit 1
            fi
            if [ "$(date +%s)" -ge "$deadline" ]; then
                log_error "Cluster did not become ready within 60s — see $LOGS/"
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
