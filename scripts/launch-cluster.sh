#!/bin/bash
#
# launch-cluster.sh - Launch a distributed Predastore cluster
#
# Parses a cluster TOML configuration, sets up loopback IP aliases,
# and launches individual s3d processes for each node.
#
# Usage:
#   ./scripts/launch-cluster.sh [options]
#
# Options:
#   -c, --config     Path to cluster.toml (default: clusters/3node/cluster.toml)
#   -b, --binary     Path to s3d binary (default: ./bin/s3d)
#   -k, --kill       Kill all running s3d processes
#   -s, --status     Show status of running s3d processes
#   -h, --help       Show this help message
#
# Examples:
#   ./scripts/launch-cluster.sh
#   ./scripts/launch-cluster.sh -c clusters/7node/cluster.toml
#   ./scripts/launch-cluster.sh --kill
#

set -e

# Default values
CONFIG_FILE="clusters/3node/cluster.toml"
S3D_BINARY="./bin/s3d"
TLS_KEY="certs/server.key"
TLS_CERT="certs/server.pem"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -c, --config FILE    Path to cluster.toml (default: $CONFIG_FILE)"
    echo "  -b, --binary FILE    Path to s3d binary (default: $S3D_BINARY)"
    echo "  -k, --kill           Kill all running s3d processes"
    echo "  -s, --status         Show status of running s3d processes"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                          # Launch 3-node cluster"
    echo "  $0 -c clusters/7node/cluster.toml           # Launch 7-node cluster"
    echo "  $0 --kill                                   # Stop all nodes"
    echo "  $0 --status                                 # Check node status"
}

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        -b|--binary)
            S3D_BINARY="$2"
            shift 2
            ;;
        -k|--kill)
            KILL_MODE=true
            shift
            ;;
        -s|--status)
            STATUS_MODE=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Derive cluster directory from config path (e.g., clusters/3node from clusters/3node/cluster.toml)
CLUSTER_DIR="$(dirname "$CONFIG_FILE")"
LOG_DIR="$CLUSTER_DIR/logs"
PID_DIR="$CLUSTER_DIR/pids"

# Create directories
mkdir -p "$LOG_DIR" "$PID_DIR"

# Parse unique host IPs from the config's [[db]] sections
parse_host_ips() {
    grep -E '^\s*host\s*=' "$CONFIG_FILE" | \
        sed 's/.*=\s*"\(.*\)".*/\1/' | \
        grep -v '0\.0\.0\.0' | \
        sort -u
}

# Parse node IDs from the config's [[db]] sections
parse_node_ids() {
    grep -E '^\s*id\s*=' "$CONFIG_FILE" | \
        awk -F'=' '{gsub(/[[:space:]]/, "", $2); print $2}' | \
        head -n "$(grep -c '^\[\[db\]\]' "$CONFIG_FILE")"
}

# Setup loopback IP aliases
setup_ips() {
    local ips
    ips=$(parse_host_ips)
    if [ -z "$ips" ]; then
        return
    fi

    log_info "Adding loopback IP aliases..."
    for ip in $ips; do
        if ! ip addr show lo | grep -qw "$ip"; then
            sudo ip addr add "${ip}/24" dev lo
            log_info "  Added $ip to lo"
        fi
    done
}

# Remove loopback IP aliases
teardown_ips() {
    local ips
    ips=$(parse_host_ips)
    for ip in $ips; do
        sudo ip addr del "${ip}/24" dev lo 2>/dev/null || true
    done
}

# Kill mode
if [ "$KILL_MODE" = true ]; then
    log_info "Stopping all s3d processes..."

    if [ -d "$PID_DIR" ]; then
        for pidfile in "$PID_DIR"/*.pid; do
            if [ -f "$pidfile" ]; then
                pid=$(cat "$pidfile")
                node=$(basename "$pidfile" .pid)
                if kill -0 "$pid" 2>/dev/null; then
                    log_info "Stopping node $node (PID: $pid)"
                    kill "$pid" 2>/dev/null || true
                fi
                rm -f "$pidfile"
            fi
        done
    fi

    # Also kill any remaining s3d processes
    pkill -f "s3d" 2>/dev/null || true

    # Teardown loopback aliases
    if [ -f "$CONFIG_FILE" ]; then
        teardown_ips
    fi

    log_info "All s3d processes stopped"
    exit 0
fi

# Status mode
if [ "$STATUS_MODE" = true ]; then
    log_info "Checking s3d process status..."

    found=false
    if [ -d "$PID_DIR" ]; then
        for pidfile in "$PID_DIR"/*.pid; do
            if [ -f "$pidfile" ]; then
                pid=$(cat "$pidfile")
                node=$(basename "$pidfile" .pid)
                if kill -0 "$pid" 2>/dev/null; then
                    echo -e "  ${GREEN}[RUNNING]${NC} $node (PID: $pid)"
                    found=true
                else
                    echo -e "  ${RED}[STOPPED]${NC} $node (stale PID file)"
                    rm -f "$pidfile"
                fi
            fi
        done
    fi

    if [ "$found" = false ]; then
        log_warn "No running s3d processes found"
    fi
    exit 0
fi

# Check prerequisites
if [ ! -f "$CONFIG_FILE" ]; then
    log_error "Config file not found: $CONFIG_FILE"
    exit 1
fi

if [ ! -f "$S3D_BINARY" ]; then
    log_warn "s3d binary not found at $S3D_BINARY, attempting to build..."
    make build || {
        log_error "Failed to build s3d"
        exit 1
    }
fi

if [ ! -f "$TLS_KEY" ] || [ ! -f "$TLS_CERT" ]; then
    log_warn "TLS certificates not found, generating..."
    make certs || {
        log_error "Failed to generate TLS certificates"
        exit 1
    }
fi

# Setup loopback IP aliases
setup_ips

# Get unique DB node IDs (one s3d process per DB node)
NODE_IDS=$(parse_node_ids | sort -un)

if [ -z "$NODE_IDS" ]; then
    log_error "No database nodes found in $CONFIG_FILE"
    exit 1
fi

log_info "Found nodes in config: $(echo $NODE_IDS | tr '\n' ' ')"
log_info "Launching distributed cluster from $CONFIG_FILE..."

# Launch each node
for node_id in $NODE_IDS; do
    log_file="$LOG_DIR/node-${node_id}.log"
    pid_file="$PID_DIR/node-${node_id}.pid"

    # Check if already running
    if [ -f "$pid_file" ]; then
        old_pid=$(cat "$pid_file")
        if kill -0 "$old_pid" 2>/dev/null; then
            log_warn "Node $node_id already running (PID: $old_pid), skipping"
            continue
        fi
    fi

    log_info "Starting node $node_id..."

    nohup "$S3D_BINARY" \
        -config "$CONFIG_FILE" \
        -node "$node_id" \
        -base-path "$CLUSTER_DIR" \
        -tls-key "$TLS_KEY" \
        -tls-cert "$TLS_CERT" \
        > "$log_file" 2>&1 &

    pid=$!
    echo "$pid" > "$pid_file"

    log_info "  Node $node_id started (PID: $pid, Log: $log_file)"
done

# Wait a moment for processes to initialize
sleep 1

# Show status
log_info ""
log_info "Cluster launched! Use '$0 --status' to check status"
log_info "Use '$0 --kill' to stop all nodes"
log_info ""
log_info "Node logs available in $LOG_DIR/"
