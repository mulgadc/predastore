#!/usr/bin/env bash
# bench-predastore.sh — pseudo-multinode predastore benchmark harness.
#
# Brings up three s3d processes on loopback aliases (10.11.12.{1,2,3}) using
# clusters/3node/cluster.toml, waits for readiness, runs `warp mixed`
# distributed across all three nodes, and tears everything down on exit.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

BENCH_DIR="${BENCH_DIR:-/tmp/predastore-bench}"
S3D_BIN="${S3D_BIN:-$REPO_ROOT/bin/s3d}"
TLS_CERT="${TLS_CERT:-$REPO_ROOT/certs/server.pem}"
TLS_KEY="${TLS_KEY:-$REPO_ROOT/certs/server.key}"
CONFIG="$REPO_ROOT/clusters/3node/cluster.toml"

NODE_IPS=(10.11.12.1 10.11.12.2 10.11.12.3)
S3_PORT=8443

# Test credentials — match the cluster config's [[auth]] section.
AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

# warp mixed tuning — unset values fall back to warp's built-in defaults
# (2500 objects × 10MiB, 5m, 20 concurrent). Override for tmpfs-safe local
# runs or scaled CI/CD runs on dedicated hardware.
WARP_OBJECTS="${WARP_OBJECTS:-}"
WARP_OBJ_SIZE="${WARP_OBJ_SIZE:-}"
WARP_DURATION="${WARP_DURATION:-}"
WARP_CONCURRENT="${WARP_CONCURRENT:-}"

declare -a PIDS=()
ADDED_IPS=0

# ---------------------------------------------------------------------------
# Cleanup. Installed before any mutation so Ctrl-C, SIGTERM, or an early
# failure still leaves a clean machine — leaked IP aliases or s3d processes
# will break the next run.
# ---------------------------------------------------------------------------
cleanup() {
    local rc=$?
    set +e
    echo "bench-predastore: cleanup"

    for pid in "${PIDS[@]:-}"; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null
        fi
    done
    # Give processes a moment to exit before SIGKILL fallback.
    sleep 1
    for pid in "${PIDS[@]:-}"; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null
        fi
    done

    if [ "$ADDED_IPS" = "1" ]; then
        for ip in "${NODE_IPS[@]}"; do
            sudo ip addr del "${ip}/24" dev lo 2>/dev/null || true
        done
    fi

    # Wipe the benchmark data root. The resolved config, logs, and warp results
    # live under RESULTS_DIR (separate from BENCH_DIR) so they survive cleanup.
    if [ -n "${BENCH_DIR:-}" ] && [ -d "$BENCH_DIR" ]; then
        rm -rf "$BENCH_DIR"
    fi

    exit "$rc"
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Pre-flight.
# ---------------------------------------------------------------------------
for bin in curl ip warp; do
    command -v "$bin" >/dev/null || {
        echo "required binary not on PATH: $bin" >&2; exit 1;
    }
done
[ -x "$S3D_BIN" ] || { echo "s3d not found or not executable: $S3D_BIN (run make build)" >&2; exit 1; }
[ -f "$TLS_CERT" ] || { echo "missing TLS cert: $TLS_CERT (run make certs)" >&2; exit 1; }
[ -f "$TLS_KEY" ]  || { echo "missing TLS key: $TLS_KEY (run make certs)" >&2; exit 1; }
[ -f "$CONFIG" ]   || { echo "missing config: $CONFIG" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Layout. Logs and the config copy live under RESULTS_DIR (outside BENCH_DIR)
# so they survive trap cleanup even if warp or an s3d process dies mid-run.
# ---------------------------------------------------------------------------
STAMP="$(date -u +%Y-%m-%dT%H%M%SZ)"
RESULTS_PARENT="${RESULTS_PARENT:-$REPO_ROOT/scripts/bench/results}"
RESULTS_DIR="$RESULTS_PARENT/predastore-$STAMP"
mkdir -p "$RESULTS_DIR/logs" "$BENCH_DIR"
export BENCH_DIR

# Archive the config used for this run.
cp "$CONFIG" "$RESULTS_DIR/cluster.toml"

# ---------------------------------------------------------------------------
# Simulated IPs on loopback.
# ---------------------------------------------------------------------------
echo "bench-predastore: adding simulated IPs on lo"
for ip in "${NODE_IPS[@]}"; do
    if ! ip addr show lo | grep -qw "$ip"; then
        sudo ip addr add "${ip}/24" dev lo
        ADDED_IPS=1
    fi
done

# ---------------------------------------------------------------------------
# Launch three s3d processes.
# ---------------------------------------------------------------------------
for n in 1 2 3; do
    node_ip="${NODE_IPS[$((n-1))]}"
    log="$RESULTS_DIR/logs/node-$n.log"
    echo "bench-predastore: launching node $n on $node_ip:$S3_PORT"
    nohup "$S3D_BIN" \
        -config    "$CONFIG" \
        -tls-cert  "$TLS_CERT" \
        -tls-key   "$TLS_KEY" \
        -base-path "$BENCH_DIR" \
        -node      "$n" \
        -host      "$node_ip" \
        -port      "$S3_PORT" \
        > "$log" 2>&1 &
    PIDS+=("$!")
    # Remove from bash's job table so cleanup's SIGKILL doesn't emit
    # "line N: <pid> Killed …" status lines to stderr.
    disown "$!"
done

# ---------------------------------------------------------------------------
# Readiness probe. Raft quorum must be formed before warp hits the cluster,
# otherwise early requests see 503s.
# ---------------------------------------------------------------------------
echo "bench-predastore: waiting for cluster readiness"
deadline=$(( $(date +%s) + 60 ))
for n in 1 2 3; do
    node_ip="${NODE_IPS[$((n-1))]}"
    while :; do
        if curl -k -s "https://${node_ip}:${S3_PORT}/" >/dev/null 2>&1; then
            echo "  node $n ready"
            break
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            echo "  node $n did not become ready within 60s" >&2
            echo "  tail of node-$n.log:" >&2
            tail -n 40 "$RESULTS_DIR/logs/node-$n.log" >&2 || true
            exit 1
        fi
        sleep 1
    done
done

# ---------------------------------------------------------------------------
# Run warp. warp creates its own bucket via --bucket.
# ---------------------------------------------------------------------------
warp_args=()
[ -n "$WARP_OBJECTS" ]    && warp_args+=(--objects="$WARP_OBJECTS")
[ -n "$WARP_OBJ_SIZE" ]   && warp_args+=(--obj.size="$WARP_OBJ_SIZE")
[ -n "$WARP_DURATION" ]   && warp_args+=(--duration="$WARP_DURATION")
[ -n "$WARP_CONCURRENT" ] && warp_args+=(--concurrent="$WARP_CONCURRENT")

echo "bench-predastore: running warp mixed"
warp mixed \
    --host="${NODE_IPS[0]}:${S3_PORT},${NODE_IPS[1]}:${S3_PORT},${NODE_IPS[2]}:${S3_PORT}" \
    --tls --insecure \
    --region=ap-southeast-2 \
    --access-key="$AWS_ACCESS_KEY_ID" \
    --secret-key="$AWS_SECRET_ACCESS_KEY" \
    --bucket=predastore \
    --benchdata="$RESULTS_DIR/warp-mixed" \
    "${warp_args[@]}"

# ---------------------------------------------------------------------------
# Preserve run metadata.
# ---------------------------------------------------------------------------
{
    echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "hostname=$(hostname)"
    echo "predastore_sha=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
    echo "warp_version=$(warp --version 2>/dev/null | head -n1 || echo unknown)"
} > "$RESULTS_DIR/run-info.txt"

echo "bench-predastore: done. results under $RESULTS_DIR"
