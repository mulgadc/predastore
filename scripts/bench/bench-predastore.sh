#!/usr/bin/env bash
# bench-predastore.sh — pseudo-multinode predastore benchmark harness.
#
# Brings up three s3d processes on loopback aliases (10.11.12.{1,2,3}) using a
# single rendered predastore.toml, waits for readiness, runs `warp mixed`
# distributed across all three nodes, and tears everything down on exit.
#
# See docs/development/improvements/simple-predastore-benchmark.md for intent.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

BENCH_DIR="${BENCH_DIR:-/tmp/predastore-bench}"
S3D_BIN="${S3D_BIN:-$REPO_ROOT/bin/s3d}"
TLS_CERT="${TLS_CERT:-$REPO_ROOT/config/server.pem}"
TLS_KEY="${TLS_KEY:-$REPO_ROOT/config/server.key}"
TEMPLATE="$SCRIPT_DIR/predastore.toml.tmpl"

export NODE1_IP=10.11.12.1
export NODE2_IP=10.11.12.2
export NODE3_IP=10.11.12.3
S3_PORT=8443

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
        for ip in "$NODE1_IP" "$NODE2_IP" "$NODE3_IP"; do
            sudo ip addr del "${ip}/24" dev lo 2>/dev/null || true
        done
    fi

    # Wipe the benchmark data root. The resolved config, logs, and warp results
    # live here too — but we move the results dir out before cleanup runs.
    if [ -n "${BENCH_DIR:-}" ] && [ -d "$BENCH_DIR" ]; then
        rm -rf "$BENCH_DIR"
    fi

    exit "$rc"
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Pre-flight.
# ---------------------------------------------------------------------------
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID must be set}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY must be set}"
export ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID"
export SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"

for bin in envsubst curl ip warp; do
    command -v "$bin" >/dev/null || {
        echo "required binary not on PATH: $bin" >&2; exit 1;
    }
done
[ -x "$S3D_BIN" ] || { echo "s3d not found or not executable: $S3D_BIN (run make build)" >&2; exit 1; }
[ -f "$TLS_CERT" ] || { echo "missing TLS cert: $TLS_CERT" >&2; exit 1; }
[ -f "$TLS_KEY" ]  || { echo "missing TLS key: $TLS_KEY" >&2; exit 1; }
[ -f "$TEMPLATE" ] || { echo "missing config template: $TEMPLATE" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Layout. Logs and the resolved config live under RESULTS_DIR (outside
# BENCH_DIR) so they survive trap cleanup even if warp or an s3d process
# dies mid-run.
# ---------------------------------------------------------------------------
STAMP="$(date -u +%Y-%m-%dT%H%M%SZ)"
RESULTS_PARENT="${RESULTS_PARENT:-$REPO_ROOT/scripts/bench/results}"
RESULTS_DIR="$RESULTS_PARENT/predastore-$STAMP"
mkdir -p "$RESULTS_DIR/logs" "$BENCH_DIR"
export BENCH_DIR
RESOLVED_CONFIG="$RESULTS_DIR/predastore.toml"

# ---------------------------------------------------------------------------
# Simulated IPs. Pattern borrowed (inlined) from
# spinifex/tests/e2e/lib/multinode-helpers.sh:25 — no Spinifex dependency.
# ---------------------------------------------------------------------------
echo "bench-predastore: adding simulated IPs on lo"
for ip in "$NODE1_IP" "$NODE2_IP" "$NODE3_IP"; do
    if ! ip addr show lo | grep -qw "$ip"; then
        sudo ip addr add "${ip}/24" dev lo
        ADDED_IPS=1
    fi
done

# ---------------------------------------------------------------------------
# Render the config.
# ---------------------------------------------------------------------------
envsubst < "$TEMPLATE" > "$RESOLVED_CONFIG"

# ---------------------------------------------------------------------------
# Launch three s3d processes.
# ---------------------------------------------------------------------------
ips=("$NODE1_IP" "$NODE2_IP" "$NODE3_IP")
for n in 1 2 3; do
    node_ip="${ips[$((n-1))]}"
    log="$RESULTS_DIR/logs/node-$n.log"
    echo "bench-predastore: launching node $n on $node_ip:$S3_PORT"
    nohup "$S3D_BIN" \
        -config   "$RESOLVED_CONFIG" \
        -tls-cert "$TLS_CERT" \
        -tls-key  "$TLS_KEY" \
        -backend  distributed \
        -node     "$n" \
        -host     "$node_ip" \
        -port     "$S3_PORT" \
        > "$log" 2>&1 &
    PIDS+=("$!")
done

# ---------------------------------------------------------------------------
# Readiness probe. Pattern from verify_predastore_cluster (helpers.sh:838).
# Raft quorum must be formed before warp hits the cluster, otherwise early
# requests see 503s.
# ---------------------------------------------------------------------------
echo "bench-predastore: waiting for cluster readiness"
deadline=$(( $(date +%s) + 60 ))
for n in 1 2 3; do
    node_ip="${ips[$((n-1))]}"
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
# Run warp. Defaults for duration/object size/concurrency accepted — tuning
# is a deliberate follow-on (see plan §Scope).
# ---------------------------------------------------------------------------
echo "bench-predastore: running warp mixed"
warp mixed \
    --host="${NODE1_IP}:${S3_PORT},${NODE2_IP}:${S3_PORT},${NODE3_IP}:${S3_PORT}" \
    --tls --insecure \
    --access-key="$AWS_ACCESS_KEY_ID" \
    --secret-key="$AWS_SECRET_ACCESS_KEY" \
    --bucket=predastore \
    --benchdata="$RESULTS_DIR/warp-mixed"

# ---------------------------------------------------------------------------
# Preserve run metadata. The resolved config and per-node logs already live
# under RESULTS_DIR; warp's output is under RESULTS_DIR/warp-mixed. Only the
# run-info file is left to write.
# ---------------------------------------------------------------------------
{
    echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "hostname=$(hostname)"
    echo "predastore_sha=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
    echo "warp_version=$(warp --version 2>/dev/null | head -n1 || echo unknown)"
} > "$RESULTS_DIR/run-info.txt"

echo "bench-predastore: done. results under $RESULTS_DIR"
