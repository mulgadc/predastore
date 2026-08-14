#!/usr/bin/env bash
#
# e2e-stress.sh - Freeze a host under live S3 load and prove it rejoins.
#
# A four-host cluster is put under Warp GET load, one host is stopped with
# SIGSTOP, and the run asserts that the survivors keep serving throughout and
# that the frozen host rejoins raft and takes writes again once it is
# continued. SIGSTOP is the fault worth injecting because it is the one a
# healthy transport cannot distinguish from a slow peer: the process stays
# dialable and its sockets stay open while it answers nothing, which is
# exactly the state a connection pool can sit on indefinitely.
#
# Usage:
#   ./scripts/bench/e2e-stress.sh          # or: make e2e-stress
#
# Environment:
#   STRESS_CONFIG      Profile to run (default: 4host)
#   STRESS_HOST        "follower" (default), "leader", or an explicit host id.
#                      The role is resolved against the running cluster, since
#                      which host raft elects varies per run.
#   STRESS_FREEZE      How long the host stays frozen (default: 90)
#   STRESS_REJOIN      Seconds allowed for the thawed replica to catch up
#                      (default: 120)
#   STRESS_OBJECTS     Objects in the GET corpus (default: 32)
#   STRESS_OBJ_SIZE    Size of each (default: 8MiB)
#   STRESS_CONCURRENT  Warp concurrency (default: 8)
#   STRESS_RESULTS_ROOT Where runs are written
#   STRESS_KEEP_WORK   1 to keep the cluster work directory after the run
#   STRESS_PORT_OFFSET Added to every node port, so a run does not collide
#                      with a cluster already on the defaults (default: 10000)
#   WARP               Path to the warp binary (default: bin/tools/warp)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd -P)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
SCRIPTS_DIR="$REPO_DIR/scripts"
CONFIG_DIR="$REPO_DIR/config"
RESULTS_ROOT="${STRESS_RESULTS_ROOT:-$SCRIPT_DIR/results/e2e-stress}"
WARP="${WARP:-$REPO_DIR/bin/tools/warp}"

# shellcheck source=scripts/lib.sh
source "$SCRIPTS_DIR/lib.sh"

CONFIG_NAME="${STRESS_CONFIG:-4host}"
FREEZE_SECONDS="${STRESS_FREEZE:-90}"
REJOIN_SECONDS="${STRESS_REJOIN:-120}"
OBJECTS="${STRESS_OBJECTS:-32}"
OBJ_SIZE="${STRESS_OBJ_SIZE:-8MiB}"
CONCURRENT="${STRESS_CONCURRENT:-8}"
PORT_OFFSET="${STRESS_PORT_OFFSET:-10000}"

ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
REGION="ap-southeast-2"

for command in aws awk diff git go openssl; do
    command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 1; }
done
[ -x "$WARP" ] || { echo "Warp not executable: $WARP (run make warp-install)" >&2; exit 1; }
[ -f "$CONFIG_DIR/$CONFIG_NAME.toml" ] || { echo "missing config: $CONFIG_NAME" >&2; exit 1; }

mkdir -p "$RESULTS_ROOT"
STAMP="$(date -u +%Y-%m-%dT%H%M%SZ)"
RUN_ID="$(printf '%s' "$STAMP" | tr '[:upper:]' '[:lower:]')"
SHA="$(git -C "$REPO_DIR" rev-parse --short HEAD)"
RUN_DIR="$RESULTS_ROOT/${STAMP}-${SHA}"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/predastore-e2e-stress.XXXXXX")"

case "$WORK_DIR" in
    ""|/|"${TMPDIR:-/tmp}") echo "refusing unsafe work directory: $WORK_DIR" >&2; exit 1 ;;
esac

mkdir -p "$RUN_DIR/logs"
export PREDA_DIR="$WORK_DIR/predastore"
export PREDA_CONFIG_DIR="$WORK_DIR/config"
export TMPDIR="$WORK_DIR/tmp"
mkdir -p "$PREDA_DIR" "$PREDA_CONFIG_DIR" "$TMPDIR"

CONFIG_FILE="$PREDA_CONFIG_DIR/$CONFIG_NAME.toml"
render_profile "$CONFIG_DIR/$CONFIG_NAME.toml" "$CONFIG_FILE" "$PORT_OFFSET"
cp "$CONFIG_FILE" "$RUN_DIR/$CONFIG_NAME.toml"

CA_FILE="$PREDA_DIR/server.pem"
PID_DIR="$PREDA_DIR/$CONFIG_NAME/pids"
EVENTS="$RUN_DIR/events.txt"
WARP_LOG="$RUN_DIR/get.log"

# Which host to freeze is resolved once the cluster is up, because neither
# "leader" nor "follower" is knowable before then.
FROZEN_HOST=""
FROZEN_PID=""
WARP_PID=""

log() {
    local line
    line="$(date -u +%H:%M:%S) $*"
    echo "$line"
    echo "$line" >> "$EVENTS"
}

fail() { log "FAIL: $*"; exit 1; }

# A stopped process never handles SIGTERM, so stop.sh would leave it behind.
# Continuing it first is what makes the teardown reliable rather than a race
# against a signal that cannot be delivered. A node is then confirmed gone
# rather than assumed: one that outlives the run holds its ports and fails
# the next one somewhere unrelated.
cleanup() {
    if [ -n "$WARP_PID" ] && kill -0 "$WARP_PID" 2>/dev/null; then
        kill "$WARP_PID" 2>/dev/null || true
        wait "$WARP_PID" 2>/dev/null || true
    fi
    if [ -n "$FROZEN_PID" ] && kill -0 "$FROZEN_PID" 2>/dev/null; then
        kill -CONT "$FROZEN_PID" 2>/dev/null || true
    fi
    # stop.sh removes each pidfile as it signals, so the handles are taken
    # before it runs rather than looked for afterwards.
    local pidfile pid waited
    local pids=()
    for pidfile in "$PID_DIR"/*.pid; do
        [ -e "$pidfile" ] || continue
        pid="$(cat "$pidfile" 2>/dev/null)" || continue
        [ -n "$pid" ] && pids+=("$pid")
    done

    "$SCRIPTS_DIR/stop.sh" >/dev/null 2>&1 || true

    for pid in ${pids[@]+"${pids[@]}"}; do
        waited=0
        while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 15 ]; do
            sleep 1
            waited=$(( waited + 1 ))
        done
        if kill -0 "$pid" 2>/dev/null; then
            echo "node $pid ignored SIGTERM for ${waited}s; killing" >&2
            kill -KILL "$pid" 2>/dev/null || true
        fi
    done
    if [ -d "$PREDA_DIR/$CONFIG_NAME/logs" ]; then
        cp -R "$PREDA_DIR/$CONFIG_NAME/logs/." "$RUN_DIR/logs/" 2>/dev/null || true
    fi
    if [ "${STRESS_KEEP_WORK:-0}" = "1" ]; then
        echo "Work directory retained: $WORK_DIR"
    else
        rm -rf "$WORK_DIR"
    fi
}
trap cleanup EXIT INT TERM

aws_s3() {
    AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
    AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
    AWS_DEFAULT_REGION="$REGION" \
    AWS_REQUEST_CHECKSUM_CALCULATION=when_required \
    AWS_RESPONSE_CHECKSUM_VALIDATION=when_required \
    PYTHONWARNINGS=ignore \
        aws --no-cli-pager --no-verify-ssl --endpoint-url "$1" "${@:2}"
}

# meta_status reports every meta replica's own view of raft. It is the only
# probe that distinguishes a replica that rejoined from a gate that is merely
# following a redirect to a leader elsewhere. A node that does not answer is
# reported, not fatal: that is the condition the rejoin loop polls for.
META_PROBE="$WORK_DIR/metastatus"
go build -o "$META_PROBE" "$REPO_DIR/scripts/bench/metastatus"

meta_status() {
    "$META_PROBE" -config "$CONFIG_FILE" -ca "$CA_FILE" "$@" || true
}

# applied_index reads one replica's applied log position, empty when it did
# not answer.
applied_index() {
    meta_status "$1" | awk -v n="$1" '$1 == "node=" n { for (i = 1; i <= NF; i++) if ($i ~ /^applied=/) { sub(/^applied=/, "", $i); print $i } }'
}

# round_trip proves the whole data path through one gate: a PUT, a GET and a
# byte comparison. Writes need every shard node, so it is only asked of a
# healthy cluster.
round_trip() {
    local endpoint="$1" label="$2"
    local bucket="stress-${label}-${RUN_ID}"
    local src="$WORK_DIR/$label.bin" dst="$WORK_DIR/$label.out"

    openssl rand -out "$src" 1048576
    aws_s3 "$endpoint" s3 mb "s3://$bucket" >/dev/null
    aws_s3 "$endpoint" s3api put-object --bucket "$bucket" --key rt.bin --body "$src" >/dev/null
    aws_s3 "$endpoint" s3 cp "s3://$bucket/rt.bin" "$dst" --only-show-errors
    diff -q "$src" "$dst" >/dev/null
    aws_s3 "$endpoint" s3 rb "s3://$bucket" --force >/dev/null
}

# --- Start ---

log "starting $CONFIG_NAME"
"$SCRIPTS_DIR/start.sh" -w "$CONFIG_NAME"

mapfile -t META_ALL < <(meta_nodes "$CONFIG_FILE" | awk '{print $2}')
[ "${#META_ALL[@]}" -gt 0 ] || fail "no meta node in $CONFIG_NAME"

log "baseline raft state"
meta_status "${META_ALL[@]}" | tee -a "$EVENTS"

# --- Topology ---
#
# Which host raft elects varies per run, so "leader" and "follower" are
# resolved from the cluster rather than fixed in the profile. Naming the role
# is the only way to test either case on purpose instead of one run in four by
# chance, and a fixed host id would make this a coin toss.

case "${STRESS_HOST:-follower}" in
    leader|follower)
        leader_node="$(meta_status "${META_ALL[@]}" | awk '/is_leader=true/ {sub(/^node=/, "", $1); print $1; exit}')"
        [ -n "$leader_node" ] || fail "no replica reports itself leader"
        leader_host="$(meta_nodes "$CONFIG_FILE" | awk -v n="$leader_node" '$2 == n {print $1; exit}')"
        if [ "${STRESS_HOST:-follower}" = leader ]; then
            FROZEN_HOST="$leader_host"
        else
            # Any host with a meta replica that is not the leader's, so the
            # frozen replica is one that has to catch up rather than one the
            # cluster must replace.
            FROZEN_HOST="$(meta_nodes "$CONFIG_FILE" | awk -v h="$leader_host" '$1 != h {print $1; exit}')"
            [ -n "$FROZEN_HOST" ] || fail "every meta replica is on the leader's host"
        fi
        log "leader is meta node $leader_node on host $leader_host; freezing host $FROZEN_HOST"
        ;;
    *)
        FROZEN_HOST="$STRESS_HOST"
        ;;
esac

parse_hosts "$CONFIG_FILE" | awk -v h="$FROZEN_HOST" '$1 == h {found = 1} END {exit !found}' \
    || fail "host $FROZEN_HOST is not in $CONFIG_NAME"

mapfile -t SURVIVOR_META < <(meta_nodes "$CONFIG_FILE" | awk -v h="$FROZEN_HOST" '$1 != h {print $2}')
FROZEN_META="$(meta_nodes "$CONFIG_FILE" | awk -v h="$FROZEN_HOST" '$1 == h {print $2; exit}')"
[ -n "$FROZEN_META" ] || fail "host $FROZEN_HOST runs no meta node, so freezing it proves nothing"

# A frozen host must leave a majority behind, or the stall under test is
# ordinary loss of quorum rather than a replica failing to rejoin one.
[ "${#SURVIVOR_META[@]}" -gt $(( ${#META_ALL[@]} / 2 )) ] \
    || fail "freezing host $FROZEN_HOST leaves ${#SURVIVOR_META[@]} of ${#META_ALL[@]} replicas, which is not a quorum"

FROZEN_GATE="$(parse_hosts "$CONFIG_FILE" | awk -v h="$FROZEN_HOST" '$1 == h && $3 != "" {print $2 ":" $3}')"
[ -n "$FROZEN_GATE" ] || fail "host $FROZEN_HOST runs no gate"
SURVIVOR_GATES="$(parse_hosts "$CONFIG_FILE" | awk -v h="$FROZEN_HOST" '$1 != h && $3 != "" {print $2 ":" $3}')"
[ -n "$SURVIVOR_GATES" ] || fail "no gate survives freezing host $FROZEN_HOST"
SURVIVOR_LIST="$(echo "$SURVIVOR_GATES" | paste -sd,)"
SURVIVOR_ENDPOINT="https://$(echo "$SURVIVOR_GATES" | head -1)"

FROZEN_PID="$(cat "$PID_DIR/host-${FROZEN_HOST}.pid")"
kill -0 "$FROZEN_PID" 2>/dev/null || fail "host $FROZEN_HOST is not running"
log "freezing host $FROZEN_HOST (meta node $FROZEN_META), survivors $SURVIVOR_LIST"

log "baseline round trip through the host about to be frozen"
round_trip "https://$FROZEN_GATE" baseline || fail "baseline round trip failed"

# --- Load ---
#
# Warp seeds its corpus with writes, which need every shard node, so the
# prepare phase has to finish before anything is frozen. Object count is
# polled rather than the log scraped, so the wait turns on the state of the
# cluster rather than on Warp's output format.

GET_BUCKET="warp-stress-get-${RUN_ID}"
log "starting Warp GET load over $SURVIVOR_LIST"
"$WARP" get \
    --host="$SURVIVOR_LIST" --tls --insecure --region="$REGION" \
    --access-key="$ACCESS_KEY" --secret-key="$SECRET_KEY" \
    --objects="$OBJECTS" --obj.size="$OBJ_SIZE" \
    --concurrent="$CONCURRENT" \
    --duration="$(( FREEZE_SECONDS + 60 ))s" \
    --bucket="$GET_BUCKET" --benchdata="$RUN_DIR/get" \
    --no-color --noclear --full > "$WARP_LOG" 2>&1 &
WARP_PID=$!

log "waiting for the Warp corpus to be written"
deadline=$(( $(date +%s) + 300 ))
while :; do
    count="$(aws_s3 "$SURVIVOR_ENDPOINT" s3 ls "s3://$GET_BUCKET/" --recursive 2>/dev/null | wc -l)"
    [ "$count" -ge "$OBJECTS" ] && break
    kill -0 "$WARP_PID" 2>/dev/null || fail "Warp exited during prepare; see $WARP_LOG"
    [ "$(date +%s)" -lt "$deadline" ] || fail "Warp corpus not ready after 300s"
    sleep 2
done
log "corpus ready ($count objects)"

# --- Freeze ---

# Where the frozen replica's log stood going in. The rejoin assertion is only
# worth making if the cluster commits past this while it is away.
FROZEN_BEFORE="$(applied_index "$FROZEN_META")"

log "SIGSTOP host $FROZEN_HOST (pid $FROZEN_PID) for ${FREEZE_SECONDS}s, applied=${FROZEN_BEFORE:-unknown}"
kill -STOP "$FROZEN_PID"
FREEZE_START="$(date +%s)"

# Reads survive because RS(2,1) reconstructs from parity, so losing one of the
# four shard nodes still leaves every object readable. Writes do not: the
# write path fails on any shard error and placement spreads over all four
# nodes, so PUTs are neither issued nor asserted while a host is frozen.
sleep 5
log "raft state during freeze"
meta_status "${META_ALL[@]}" | tee -a "$EVENTS"

leader_seen="$(meta_status "${SURVIVOR_META[@]}" | grep -c 'is_leader=true' || true)"
[ "$leader_seen" -ge 1 ] || fail "survivors elected no leader while host $FROZEN_HOST was frozen"
log "survivors hold a leader with the frozen host out"

# Bucket creates are meta writes with no shard of their own, so they commit
# with one voter absent where an object write would not. They are what gives
# the frozen replica something to catch up on, and a run without them proves
# only that a replica which never fell behind can answer.
frozen_writes=0
while [ $(( $(date +%s) - FREEZE_START )) -lt "$FREEZE_SECONDS" ]; do
    kill -0 "$WARP_PID" 2>/dev/null || fail "Warp exited while a host was frozen; see $WARP_LOG"
    frozen_writes=$(( frozen_writes + 1 ))
    aws_s3 "$SURVIVOR_ENDPOINT" --cli-connect-timeout 10 --cli-read-timeout 30 \
        s3 mb "s3://stress-frozen-${frozen_writes}-${RUN_ID}" >/dev/null \
        || fail "a survivor gate refused a meta write while host $FROZEN_HOST was frozen"
    sleep 5
done
log "$frozen_writes meta writes committed with host $FROZEN_HOST frozen"

# Bounded on purpose: a gate that answers eventually is still a gate that
# stopped serving, so the assertion is a deadline rather than an error code.
aws_s3 "$SURVIVOR_ENDPOINT" --cli-connect-timeout 10 --cli-read-timeout 30 \
    s3 ls "s3://$GET_BUCKET/" --recursive >/dev/null \
    || fail "a survivor gate stopped serving while host $FROZEN_HOST was frozen"
log "survivors still serving after ${FREEZE_SECONDS}s frozen"

# --- Thaw ---
#
# The catch-up target is a survivor's applied index at the moment of thaw, so
# the assertion is that the replica reached what the cluster had committed
# without it, rather than merely that it started answering again.

TARGET="$(applied_index "${SURVIVOR_META[0]}")"
[ -n "$TARGET" ] || fail "no survivor reported an applied index to catch up to"
[ -n "$FROZEN_BEFORE" ] || fail "the frozen replica reported no applied index before the freeze"

# A replica that was never behind rejoins trivially, so a run where the log
# did not move is reported as inconclusive rather than passed.
[ "$TARGET" -gt "$FROZEN_BEFORE" ] \
    || fail "the cluster committed nothing while host $FROZEN_HOST was frozen (applied stayed at $TARGET), so catching up proves nothing"

log "SIGCONT host $FROZEN_HOST, catch-up target applied=$TARGET (was $FROZEN_BEFORE)"
kill -CONT "$FROZEN_PID"
THAW_START="$(date +%s)"

rejoined=false
deadline=$(( THAW_START + REJOIN_SECONDS ))
while [ "$(date +%s)" -lt "$deadline" ]; do
    line="$(meta_status "$FROZEN_META" | grep "^node=$FROZEN_META " || true)"
    applied="$(echo "$line" | awk '{for (i = 1; i <= NF; i++) if ($i ~ /^applied=/) { sub(/^applied=/, "", $i); print $i }}')"
    if [ -n "$applied" ] && [ -n "$TARGET" ] && [ "$applied" -ge "$TARGET" ] \
        && ! echo "$line" | grep -q 'leader=""'; then
        rejoined=true
        REJOIN_SECS=$(( $(date +%s) - THAW_START ))
        log "rejoined after ${REJOIN_SECS}s: $line"
        break
    fi
    sleep 2
done
[ "$rejoined" = true ] || fail "host $FROZEN_HOST did not rejoin within ${REJOIN_SECONDS}s: $(meta_status "$FROZEN_META")"

# Writes are the proof the whole cluster is back: they need every shard node,
# so one still-absent host fails this where a read would not.
log "round trip through the thawed gate"
round_trip "https://$FROZEN_GATE" rejoined || fail "thawed host does not take writes"

log "final raft state"
meta_status "${META_ALL[@]}" | tee -a "$EVENTS"

# --- Load result ---

set +e
wait "$WARP_PID"
warp_status=$?
set -e
WARP_PID=""
if [ "$warp_status" -ne 0 ] || grep -q 'warp: <ERROR>' "$WARP_LOG"; then
    fail "Warp reported errors under load; see $WARP_LOG"
fi
log "Warp completed with no errors"

"$WARP" analyze --no-color --analyze.v \
    "$(find "$RUN_DIR" -maxdepth 1 -type f \( -name 'get.json.zst' -o -name 'get.csv.zst' \) -print -quit)" \
    > "$RUN_DIR/get-latency.txt" 2>/dev/null || true

{
    echo "Predastore end-to-end stress run"
    echo "==============================="
    echo
    echo "date_utc=$STAMP"
    echo "predastore_sha=$(git -C "$REPO_DIR" rev-parse HEAD)"
    echo "go_version=$(go version)"
    echo "warp_version=$($WARP --version 2>&1 | head -n 1)"
    echo "host=$(hostname)"
    echo
    echo "config=$CONFIG_NAME"
    echo "frozen_host=$FROZEN_HOST"
    echo "frozen_meta_node=$FROZEN_META"
    echo "survivor_gates=$SURVIVOR_LIST"
    echo "freeze_seconds=$FREEZE_SECONDS"
    echo "rejoin_seconds=${REJOIN_SECS:-unknown}"
    echo "rejoin_deadline=$REJOIN_SECONDS"
    echo "applied_before_freeze=$FROZEN_BEFORE"
    echo "applied_catch_up_target=$TARGET"
    echo "objects=$OBJECTS"
    echo "object_size=$OBJ_SIZE"
    echo "concurrent=$CONCURRENT"
    echo "port_offset=$PORT_OFFSET"
    echo
    echo "Assertions"
    echo "----------"
    echo "survivors_elected_leader=pass"
    echo "survivors_served_while_frozen=pass"
    echo "meta_writes_while_frozen=$frozen_writes"
    echo "frozen_replica_caught_up=pass"
    echo "thawed_host_took_writes=pass"
    echo "warp_error_free=pass"
    echo
    echo "Timeline"
    echo "--------"
    cat "$EVENTS"
} > "$RUN_DIR/run-info.txt"

echo "Stress results: $RUN_DIR"
