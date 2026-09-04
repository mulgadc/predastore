#!/bin/bash
#
# start.sh - Start a Predastore cluster from a config profile
#
# Launches one s3d process per [[host]] in the profile. Whether nodes reach
# each other over the in-process pipe or over QUIC follows from the config —
# same host is a pipe, different hosts is QUIC — so there is no launch mode
# to choose here.
#
# Usage:
#   ./scripts/start.sh [-w] <clustername>
#
# Options:
#   -w    Wait for every gate to answer (60s timeout)
#
# Environment:
#   PREDA_DIR          Root for cluster data, certs and the master key
#   PREDA_CONFIG_DIR   Where profiles are read from (default: repo config/)
#   LOG_LEVEL          Passed through as -log-level (debug|info|warn|error)
#
# Examples:
#   ./scripts/start.sh 1host          # one process, pipe only
#   ./scripts/start.sh -w 3host       # three processes over quic
#

set -euo pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
REPO_DIR="$SCRIPT_DIR/.."
# PREDA_CONFIG_DIR lets a harness run generated profiles — on shifted ports, so
# a benchmark does not collide with a cluster already using the defaults.
CONFIG_DIR="${PREDA_CONFIG_DIR:-$REPO_DIR/config}"
S3D_BINARY="$REPO_DIR/bin/s3d"

# shellcheck source=scripts/lib.sh
source "$SCRIPT_DIR/lib.sh"

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

list_clusters() {
    echo "Available clusters:"
    for f in "$CONFIG_DIR"/*.toml; do
        [ -f "$f" ] && echo "  $(basename "$f" .toml)"
    done
}

if [ $# -ne 1 ]; then
    echo "Usage: $0 [-w] <clustername>"
    echo ""
    list_clusters
    exit 1
fi

CLUSTER_NAME="$1"
CONFIG_FILE="$CONFIG_DIR/${CLUSTER_NAME}.toml"

if [ ! -f "$CONFIG_FILE" ]; then
    log_error "Config not found: $CONFIG_FILE"
    list_clusters
    exit 1
fi

# --- Parse topology ---

HOSTS="$(parse_hosts "$CONFIG_FILE")"
if [ -z "$HOSTS" ]; then
    log_error "$CONFIG_FILE declares no [[host]] entries"
    exit 1
fi

HOST_COUNT="$(echo "$HOSTS" | wc -l)"

# --- Paths ---
#
# s3d rejects a relative data_dir: nothing anchors one, since the process is
# started from wherever the operator happens to be.

ROOT="$(realpath -m "${PREDA_DIR:-/tmp/predastore}")"
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
            log_error "Run ./scripts/stop.sh $CLUSTER_NAME first"
            exit 1
        else
            # Stale PID file — clean it up
            rm -f "$pidfile"
        fi
    done
fi

mkdir -p "$ROOT" "$LOGS" "$PIDS"

# --- Generate certs ---
#
# One keypair covers the whole cluster: the gate serves it to S3 clients and
# every host presents it to its peers. TLS is host-scoped by design, so a SAN
# per host address is all that is needed.

TLS_KEY="$ROOT/server.key"
TLS_CERT="$ROOT/server.pem"

SAN="DNS:localhost,IP:127.0.0.1"
for ip in $(routable_addrs "$CONFIG_FILE"); do
    SAN="${SAN},IP:${ip}"
done

# Regenerate whenever the existing certificate does not cover every host in
# this config. $PREDA_DIR is shared across clusters, so a cert left by a
# smaller topology otherwise survives and peers fail verification at dial
# time with an error that points at TLS rather than at the stale cert.
cert_covers_hosts() {
    [ -f "$TLS_CERT" ] && [ -f "$TLS_KEY" ] || return 1
    local have
    have=$(openssl x509 -in "$TLS_CERT" -noout -ext subjectAltName 2>/dev/null) || return 1
    for ip in $(routable_addrs "$CONFIG_FILE"); do
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
# s3d verifies QUIC peer certificates strictly against the OS trust store
# (transport.NewQUICTransport is built with no RootCAs override), so a
# self-signed cert that is not a trust anchor fails with "certificate signed
# by unknown authority" and the cluster never elects a leader. A single-host
# profile opens no QUIC socket at all, so it needs none of this.

TRUST_ANCHOR="/usr/local/share/ca-certificates/predastore-${CLUSTER_NAME}.crt"

if [ "$HOST_COUNT" -gt 1 ] && ! cmp -s "$TLS_CERT" "$TRUST_ANCHOR" 2>/dev/null; then
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

# --- Loopback aliases ---

if [ "$HOST_COUNT" -gt 1 ]; then
    log_info "Setting up loopback IP aliases..."
    for ip in $(routable_addrs "$CONFIG_FILE"); do
        if ! ip addr show lo | grep -qw "$ip"; then
            sudo ip addr add "${ip}/24" dev lo
            log_info "  Added $ip to lo"
        fi
    done
fi

# --- Launch ---
#
# Everything host-local is passed as a flag rather than written into the
# profile, so the profile stays identical on every machine.

LAUNCHED_PIDS=()
LAUNCHED_ENDPOINTS=()

launch() {
    local host_id="$1" addr="$2" gate_port="$3"
    local label="host-${host_id}"
    local data_dir="$BASE/${label}"
    local args=(
        -config "$CONFIG_FILE"
        -host "$host_id"
        -data-dir "$data_dir"
        -tls-cert "$TLS_CERT"
        -tls-key "$TLS_KEY"
        -encryption-key "$MASTER_KEY"
    )
    [ -n "${LOG_LEVEL:-}" ] && args+=(-log-level "$LOG_LEVEL")

    mkdir -p "$data_dir"
    nohup "$S3D_BINARY" "${args[@]}" > "$LOGS/${label}.log" 2>&1 &
    local pid=$!
    echo "$pid" > "$PIDS/${label}.pid"
    LAUNCHED_PIDS+=("$pid")

    if [ -n "$gate_port" ]; then
        LAUNCHED_ENDPOINTS+=("${addr}:${gate_port}")
        log_info "  $label started (PID: $pid, https://${addr}:${gate_port})"
    else
        LAUNCHED_ENDPOINTS+=("")
        log_info "  $label started (PID: $pid, no gate)"
    fi
}

log_info "Launching cluster '$CLUSTER_NAME' across $HOST_COUNT host(s)"
while read -r host_id addr gate_port; do
    [ -n "$host_id" ] || continue
    launch "$host_id" "$addr" "${gate_port:-}"
done <<< "$HOSTS"

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
        endpoint="${LAUNCHED_ENDPOINTS[$i]}"
        # A host with no gate serves no S3, so there is nothing to poll.
        [ -n "$endpoint" ] || continue
        while :; do
            if curl -k -s "https://${endpoint}/" >/dev/null 2>&1; then
                log_info "  $endpoint ready"
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
log_info "Stop with: ./scripts/stop.sh $CLUSTER_NAME"
