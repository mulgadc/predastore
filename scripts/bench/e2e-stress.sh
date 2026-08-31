#!/usr/bin/env bash
#
# e2e-stress.sh - Fault-inject a four-host cluster and assert what survives.
#
# A default run is two tests, in order. The first overwrites an object with the
# one host holding a named shard of it stopped, and asserts the failed write
# left the object exactly as it was. The second puts the cluster under Warp GET
# load, stops a host with SIGSTOP, and asserts the survivors keep serving
# throughout and that the frozen host rejoins raft and takes writes again once
# it is continued.
#
# SIGSTOP is the fault worth injecting because it is the one a healthy
# transport cannot distinguish from a slow peer: the process stays dialable and
# its sockets stay open while it answers nothing, which is exactly the state a
# connection pool can sit on indefinitely.
#
# Usage:
#   ./scripts/bench/e2e-stress.sh          # or: make e2e-stress
#
# Environment:
#   STRESS_SCENARIO    Narrows the run to one test: "repair", "handoff",
#                      "large-object", "last-modified", "torn-overwrite",
#                      "stale-shard", "freeze", or "partial-put" — a client
#                      that stops sending mid-body, which is not in a default
#                      run. Unset runs repair, handoff, large-object,
#                      last-modified, torn-overwrite, stale-shard and freeze.
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
#   STRESS_TORN_LINES  Records in the torn-overwrite state document, which sets
#                      its size (default: 16384, about 1MiB)
#   STRESS_STALE_KEYS  Objects the stale-shard scenario overwrites with a host
#                      frozen (default: 12)
#   STRESS_REPAIR_KEYS Objects the repair scenario writes with a host down
#                      (default: 12)
#   STRESS_REPAIR_DEADLINE
#                      Seconds allowed for the sweep to restore them
#                      (default: 180)
#   STRESS_HANDOFF_KEYS Objects the handoff scenario writes at full width with
#                      a host down (default: 12)
#   STRESS_HANDOFF_DEADLINE
#                      Seconds allowed for the sweep to bring the handed-off
#                      shards home (default: 180)
#   STRESS_LARGE_SIZES Object sizes the large-object scenario writes and reads
#                      (default: "2GiB 4GiB 8GiB"). A size with no room on
#                      disk is skipped loudly, never silently.
#   STRESS_WORK_ROOT   Where the cluster work directory is created. Defaults to
#                      TMPDIR, except when large-object is in the run: TMPDIR is
#                      often tmpfs, which is RAM-backed, so it would both cap
#                      the sizes and make the memory measurement meaningless.
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
STALE_KEYS="${STRESS_STALE_KEYS:-12}"

# Validated rather than defaulted, so a typo runs nothing instead of quietly
# running everything and reporting a pass for a test that was never named.
SCENARIO="${STRESS_SCENARIO:-all}"
case "$SCENARIO" in
    all|freeze|partial-put|torn-overwrite|stale-shard|repair|handoff|large-object|multipart-upload) ;;
    last-modified|concurrent-put) ;;
    node-rejoin|node-resync|node-rebuild) ;;
    *) echo "unknown STRESS_SCENARIO: $SCENARIO" >&2; exit 1 ;;
esac

# Shards land under the work directory, and TMPDIR on a developer workstation
# is commonly tmpfs. For the large-object scenario that is doubly wrong: it
# caps the sizes at the size of RAM, and it charges the object's own bytes to
# the memory figure the scenario exists to measure.
WORK_ROOT="${TMPDIR:-/tmp}"
case "$SCENARIO" in
    all|large-object|multipart-upload) WORK_ROOT="$HOME/.cache/predastore-e2e" ;;
esac
WORK_ROOT="${STRESS_WORK_ROOT:-$WORK_ROOT}"
mkdir -p "$WORK_ROOT"

ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
REGION="ap-southeast-2"

for command in aws awk curl diff git go openssl; do
    command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 1; }
done
[ -x "$WARP" ] || { echo "Warp not executable: $WARP (run make warp-install)" >&2; exit 1; }
[ -f "$CONFIG_DIR/$CONFIG_NAME.toml" ] || { echo "missing config: $CONFIG_NAME" >&2; exit 1; }

mkdir -p "$RESULTS_ROOT"
STAMP="$(date -u +%Y-%m-%dT%H%M%SZ)"
RUN_ID="$(printf '%s' "$STAMP" | tr '[:upper:]' '[:lower:]')"
SHA="$(git -C "$REPO_DIR" rev-parse --short HEAD)"
RUN_DIR="$RESULTS_ROOT/${STAMP}-${SHA}"
WORK_DIR="$(mktemp -d "$WORK_ROOT/predastore-e2e-stress.XXXXXX")"

case "$WORK_DIR" in
    ""|/|"$WORK_ROOT"|"${TMPDIR:-/tmp}") echo "refusing unsafe work directory: $WORK_DIR" >&2; exit 1 ;;
esac

mkdir -p "$RUN_DIR/logs"
export PREDA_DIR="$WORK_DIR/predastore"
export PREDA_CONFIG_DIR="$WORK_DIR/config"
export TMPDIR="$WORK_DIR/tmp"
mkdir -p "$PREDA_DIR" "$PREDA_CONFIG_DIR" "$TMPDIR"

# pin_availability states the two [rs] availability settings a profile runs
# under instead of inheriting them, so a scenario measures the behaviour it
# names even when the build's defaults change underneath it.
pin_availability() {
    local file="$1" degraded="$2" handoff="$3"
    awk -v d="$degraded" -v h="$handoff" \
        '/^\[rs\]$/ { print; print "degraded_writes = " d; print "hinted_handoff = " h; next } { print }' \
        "$file" > "$file.tmp"
    mv "$file.tmp" "$file"
}

# pin_repair_off is the same for the sweep: a profile testing what a damaged
# cluster does must not have the damage repaired underneath it.
pin_repair_off() {
    printf '\n[repair]\nenabled = false\n' >> "$1"
}

CONFIG_FILE="$PREDA_CONFIG_DIR/$CONFIG_NAME.toml"
render_profile "$CONFIG_DIR/$CONFIG_NAME.toml" "$CONFIG_FILE" "$PORT_OFFSET"
pin_availability "$CONFIG_FILE" false false
pin_repair_off "$CONFIG_FILE"
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

# stop_clusters takes down every cluster under PREDA_DIR and confirms each node
# is gone rather than assuming the signal landed: one that outlives the run
# holds its ports and fails the next one somewhere unrelated. stop.sh removes
# each pidfile as it signals, so the handles are taken before it runs rather
# than looked for afterwards.
stop_clusters() {
    local pidfile pid waited
    local pids=()
    for pidfile in "$PREDA_DIR"/*/pids/*.pid; do
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
}

# A stopped process never handles SIGTERM, so stop.sh would leave it behind.
# Continuing it first is what makes the teardown reliable rather than a race
# against a signal that cannot be delivered. A node is then confirmed gone
# rather than assumed: one that outlives the run holds its ports and fails
# the next one somewhere unrelated.
cleanup() {
    if [ -n "${LARGE_RSS_SAMPLER:-}" ] && kill -0 "$LARGE_RSS_SAMPLER" 2>/dev/null; then
        kill "$LARGE_RSS_SAMPLER" 2>/dev/null || true
    fi
    if [ -n "$WARP_PID" ] && kill -0 "$WARP_PID" 2>/dev/null; then
        kill "$WARP_PID" 2>/dev/null || true
        wait "$WARP_PID" 2>/dev/null || true
    fi
    if [ -n "$FROZEN_PID" ] && kill -0 "$FROZEN_PID" 2>/dev/null; then
        kill -CONT "$FROZEN_PID" 2>/dev/null || true
    fi
    stop_clusters
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

# Placement follows the object hash, which is derived from bucket and key
# alone, so the exact host holding a named shard of a named key is resolvable
# before the object is written. Both fault scenarios below aim their SIGSTOP
# with it rather than guessing.
SHARD_PROBE="$WORK_DIR/shardplace"
go build -o "$SHARD_PROBE" "$REPO_DIR/scripts/bench/shardplace"

# shard_host names the host holding a given shard role of a given key, and
# survivor_gate a gate that is not on it. A PUT has to be issued through a gate
# that is still running, or what stalls is the frontend rather than the shard
# write.
# Both take the profile as their first argument: the repair scenario below runs
# its own cluster from its own file, and a helper that closed over the shared
# one would silently answer for the wrong cluster.
shard_host() {
    "$SHARD_PROBE" -config "$1" -bucket "$2" -key "$3" \
        | awk -v r="role=$4" '$2 == r { sub(/^host=/, "", $4); print $4; exit }'
}
survivor_gate() {
    parse_hosts "$1" | awk -v h="$2" '$1 != h && $3 != "" { print "https://" $2 ":" $3; exit }'
}
gate_of() {
    parse_hosts "$1" | awk -v h="$2" '$1 == h && $3 != "" { print "https://" $2 ":" $3; exit }'
}

# A host that was SIGSTOPped keeps the connections that died under it, and the
# pool only evicts one after three stalls of five seconds each — long enough
# that a client's whole retry budget can land inside the window. Reading
# through such a gate measures that recovery rather than the shard generations
# either scenario is asking about, so the gate is driven with the cluster whole
# until it serves, and how long that took is recorded rather than hidden.
warm_gate() {
    local gate="$1" bucket="$2" key="$3" label="$4"
    local scratch="$WORK_DIR/warm.bin" started attempt
    started="$(date +%s)"
    for attempt in $(seq 1 12); do
        if aws_s3 "$gate" --cli-connect-timeout 10 --cli-read-timeout 30 \
            s3 cp "s3://$bucket/$key" "$scratch" --only-show-errors >/dev/null 2>&1; then
            if [ "$attempt" -gt 1 ]; then
                log "$label: thawed gate served on attempt $attempt after $(( $(date +%s) - started ))s"
            fi
            return 0
        fi
        sleep 3
    done
    log "$label: thawed gate never served in $(( $(date +%s) - started ))s"

    return 1
}

# --- Scenario: repair ---
#
# The user-visible claim: a host is down, writes land anyway, it comes back,
# and what it missed is restored — after which reading an object whose data
# shard it holds costs no reconstruction, and the parity it holds rebuilds a
# lost data shard correctly.
#
# It runs its own cluster because it is the one scenario needing different
# settings. Degraded writes accept a PUT at k shards, which is what lets a write
# land with a host down; torn-overwrite asserts the exact opposite, that such a
# write is refused. Both are right under their own configuration and neither can
# be run against the other's, so this one is started and stopped before the
# shared cluster that carries the rest of the run.

REPAIR_CLUSTER="${CONFIG_NAME}-repair"
REPAIR_CONFIG="$PREDA_CONFIG_DIR/$REPAIR_CLUSTER.toml"
REPAIR_PID_DIR="$PREDA_DIR/$REPAIR_CLUSTER/pids"
# Fixed rather than run-scoped: the bucket is declared in the profile, which is
# what makes it public, and the cluster is built fresh for each run anyway.
REPAIR_BUCKET="stress-repair"
REPAIR_KEYS="${STRESS_REPAIR_KEYS:-12}"
REPAIR_DEADLINE="${STRESS_REPAIR_DEADLINE:-180}"
REPAIR_FAILURES=0
REPAIR_CASES=0

# render_repair_profile is the shared profile with three additions: degraded
# writes, the sweep on a short interval, and a public bucket so an unsigned GET
# can read the header that says what a read cost. Ports are shifted further so
# nothing here can collide with the cluster the rest of the run uses.
render_repair_profile() {
    render_profile "$CONFIG_DIR/$CONFIG_NAME.toml" "$REPAIR_CONFIG" "$(( PORT_OFFSET + 100 ))"
    pin_availability "$REPAIR_CONFIG" true false
    cat >> "$REPAIR_CONFIG" <<EOF

[repair]
enabled = true
interval_seconds = 5
page_size = 64

[[bucket]]
name = "$REPAIR_BUCKET"
region = "$REGION"
public = true
account_id = "123456789012"
EOF
    grep -q '^degraded_writes = true$' "$REPAIR_CONFIG" \
        || fail "repair: degraded_writes was not written into $REPAIR_CONFIG"
    cp "$REPAIR_CONFIG" "$RUN_DIR/$REPAIR_CLUSTER.toml"
}

# degraded_read fetches an object unsigned — the bucket is public — and prints
# how many shards the gate had to reconstruct to answer. The header is absent
# from a read that cost nothing, which is the state this scenario waits for.
degraded_read() {
    local gate="$1" bucket="$2" key="$3" out="$4"
    local headers="$WORK_DIR/degraded-headers.txt" status
    curl -sk --max-time 120 -o "$out" -D "$headers" "$gate/$bucket/$key" || return 1
    status="$(awk 'NR == 1 { print $2; exit }' "$headers")"
    if [ "$status" != 200 ]; then
        echo "http-$status"
        return 1
    fi
    awk 'tolower($1) == "x-spx-degraded:" { gsub(/\r/, "", $2); print $2; found = 1 }
         END { if (!found) print 0 }' "$headers"
}

# repair_check records one assertion, so the verdict counts what ran rather
# than what was expected to run.
repair_check() {
    local ok="$1" message="$2"
    REPAIR_CASES=$(( REPAIR_CASES + 1 ))
    if [ "$ok" = true ]; then
        log "repair: pass, $message"
    else
        log "repair: FAIL $message"
        REPAIR_FAILURES=$(( REPAIR_FAILURES + 1 ))
    fi
}

run_repair() {
    local src="$WORK_DIR/repair-src.bin" got="$WORK_DIR/repair-got.bin"
    local keys=() data_keys=() parity_keys=()
    local key i role frozen gate fpid frozen_gate

    render_repair_profile
    log "repair: starting $REPAIR_CLUSTER with degraded writes and the sweep on a 5s interval"
    "$SCRIPTS_DIR/start.sh" -w "$REPAIR_CLUSTER"

    openssl rand -out "$src" 2097152
    for i in $(seq 1 "$REPAIR_KEYS"); do
        keys+=("$(printf 'repair-%03d.bin' "$i")")
    done

    frozen="$(parse_hosts "$REPAIR_CONFIG" | awk 'NR == 1 { print $1 }')"
    gate="$(survivor_gate "$REPAIR_CONFIG" "$frozen")"
    [ -n "$gate" ] || fail "repair: no gate survives stopping host $frozen"
    frozen_gate="$(parse_hosts "$REPAIR_CONFIG" \
        | awk -v h="$frozen" '$1 == h && $3 != "" { print "https://" $2 ":" $3; exit }')"
    [ -n "$frozen_gate" ] || fail "repair: host $frozen runs no gate, so nothing repairs its blob node"

    # Which role each key places on the host about to go down. Both are needed
    # and neither is guaranteed by a hash, so they are counted rather than
    # assumed: a corpus that put no data shard there would prove nothing and
    # still pass.
    for key in "${keys[@]}"; do
        role="$("$SHARD_PROBE" -config "$REPAIR_CONFIG" -bucket "$REPAIR_BUCKET" -key "$key" \
            | awk -v h="host=$frozen" '$4 == h { sub(/^role=/, "", $2); print $2; exit }')"
        case "$role" in
            data) data_keys+=("$key") ;;
            parity) parity_keys+=("$key") ;;
        esac
    done
    [ "${#data_keys[@]}" -gt 0 ] \
        || fail "repair: no key places a data shard on host $frozen, so the no-reconstruction check proves nothing"
    [ "${#parity_keys[@]}" -gt 0 ] \
        || fail "repair: no key places a parity shard on host $frozen, so the parity check proves nothing"
    log "repair: host $frozen holds a data shard of ${#data_keys[@]} keys and a parity shard of ${#parity_keys[@]}"

    fpid="$(cat "$REPAIR_PID_DIR/host-${frozen}.pid")"
    kill -0 "$fpid" 2>/dev/null || fail "repair: host $frozen is not running"

    local accepted=0
    kill -STOP "$fpid"
    log "repair: SIGSTOP host $frozen, writing $REPAIR_KEYS objects through $gate"
    for key in "${keys[@]}"; do
        if aws_s3 "$gate" --cli-connect-timeout 10 --cli-read-timeout 120 \
            s3api put-object --bucket "$REPAIR_BUCKET" --key "$key" --body "$src" \
            >/dev/null 2>>"$RUN_DIR/repair-puts.txt"; then
            accepted=$(( accepted + 1 ))
        fi
    done
    kill -CONT "$fpid"
    log "repair: SIGCONT host $frozen; $accepted of $REPAIR_KEYS writes were accepted with it down"

    # The premise of everything below. Without it there is no redundancy gap,
    # and a sweep that found nothing to do would pass every check that follows.
    repair_check "$([ "$accepted" -eq "$REPAIR_KEYS" ] && echo true || echo false)" \
        "all $REPAIR_KEYS writes landed at k shards with host $frozen down"
    [ "$accepted" -eq "$REPAIR_KEYS" ] \
        || fail "repair: degraded writes are not in force, so the rest of the scenario is not attributable"

    # The gate that repairs the thawed host's blob node is the one in its own
    # process, so it has to be serving before the wait below means anything.
    warm_gate "$frozen_gate" "$REPAIR_BUCKET" "${keys[0]}" repair \
        || fail "repair: the thawed host's gate never served, so its sweep cannot be observed"

    # Only the keys whose data shard is on the thawed host can answer this: an
    # ordinary GET reads the data shards and never touches parity, so a parity
    # key reports no reconstruction whether or not it was ever repaired.
    local started deadline settled=false pending=0 seen first=""
    started="$(date +%s)"
    deadline=$(( started + REPAIR_DEADLINE ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        pending=0
        for key in "${data_keys[@]}"; do
            seen="$(degraded_read "$gate" "$REPAIR_BUCKET" "$key" "$got" 2>/dev/null || true)"
            [ "$seen" = 0 ] || pending=$(( pending + 1 ))
        done
        # Recorded, not asserted: the sweep runs every five seconds and may
        # already have finished while the thawed gate was being warmed. What a
        # stopped host could not have taken is settled by the write count above.
        if [ -z "$first" ]; then
            first="$pending"
            log "repair: at the first read after the thaw, $pending of ${#data_keys[@]} objects still reconstructed"
        fi
        if [ "$pending" -eq 0 ]; then
            settled=true
            break
        fi
        sleep 5
    done
    if [ "$settled" = true ]; then
        repair_check true \
            "all ${#data_keys[@]} data shards were restored in $(( $(date +%s) - started ))s, and reading them costs no reconstruction"
    else
        repair_check false \
            "$pending of ${#data_keys[@]} objects still reconstruct after ${REPAIR_DEADLINE}s, so the sweep did not restore them"
    fi

    # Restored is not the same as restored correctly, and a rebuilt shard that
    # reads back as the wrong bytes would satisfy every count above.
    local bad=0
    for key in "${keys[@]}"; do
        if ! aws_s3 "$gate" s3 cp "s3://$REPAIR_BUCKET/$key" "$got" --only-show-errors 2>>"$EVENTS"; then
            log "repair: GET errored for $key"
            bad=$(( bad + 1 ))
            continue
        fi
        cmp -s "$src" "$got" || { log "repair: $key does not match what was written"; bad=$(( bad + 1 )); }
    done
    repair_check "$([ "$bad" -eq 0 ] && echo true || echo false)" \
        "all $REPAIR_KEYS objects read back byte for byte"

    # What the restored parity is worth, which no healthy read asks. One data
    # shard's host is stopped, so the read has to rebuild from the parity the
    # sweep wrote — bytes that were never on that node until it repaired them.
    local pkey phost ppid pgate
    pkey="${parity_keys[0]}"
    phost="$(shard_host "$REPAIR_CONFIG" "$REPAIR_BUCKET" "$pkey" data)"
    if [ -z "$phost" ] || [ "$phost" = "$frozen" ]; then
        fail "repair: no data shard of $pkey sits off host $frozen, so nothing forces its parity to be read"
    fi
    pgate="$(survivor_gate "$REPAIR_CONFIG" "$phost")"
    ppid="$(cat "$REPAIR_PID_DIR/host-${phost}.pid")"

    warm_gate "$pgate" "$REPAIR_BUCKET" "$pkey" repair-parity \
        || fail "repair: the read gate did not serve before host $phost was stopped"
    log "repair: stopping host $phost so $pkey rebuilds from the parity restored on host $frozen"
    kill -STOP "$ppid"
    seen="$(degraded_read "$pgate" "$REPAIR_BUCKET" "$pkey" "$got" 2>/dev/null || true)"
    kill -CONT "$ppid"

    if [ "$seen" = 0 ] || [ -z "$seen" ]; then
        repair_check false \
            "the read of $pkey reported '$seen' shards reconstructed, so it did not go through parity and proves nothing"
    else
        repair_check "$(cmp -s "$src" "$got" && echo true || echo false)" \
            "$pkey rebuilt correctly from $seen reconstructed shard(s), so the restored parity is sound"
    fi

    log "repair: stopping $REPAIR_CLUSTER"
    stop_clusters
    if [ "$REPAIR_FAILURES" -eq 0 ]; then
        log "repair: passed $REPAIR_CASES assertions"
    else
        log "repair: FAILED $REPAIR_FAILURES of $REPAIR_CASES assertions"
    fi
}

if [ "$SCENARIO" = all ] || [ "$SCENARIO" = repair ]; then
    run_repair

    if [ "$SCENARIO" = repair ]; then
        echo "Stress results: $RUN_DIR"
        [ "$REPAIR_FAILURES" -eq 0 ] \
            || fail "repair failed $REPAIR_FAILURES of $REPAIR_CASES assertions"
        exit 0
    fi
fi

# --- Scenario: handoff ---
#
# The user-visible claim: with a host down, writes still land at full width —
# every shard on a node, none given up on — and the object stays exactly as
# redundant as one written with the cluster whole. When the host returns, the
# shards it never received come home.
#
# Degraded writes are deliberately left off here. A write acknowledged with a
# host down is then attributable to handoff and to nothing else: without it the
# floor is the whole stripe and the write is refused, which is what the
# torn-overwrite scenario asserts on the shared cluster.
#
# Like repair it runs its own cluster, for the same reason and on ports of its
# own.

HANDOFF_CLUSTER="${CONFIG_NAME}-handoff"
HANDOFF_CONFIG="$PREDA_CONFIG_DIR/$HANDOFF_CLUSTER.toml"
HANDOFF_PID_DIR="$PREDA_DIR/$HANDOFF_CLUSTER/pids"
HANDOFF_BUCKET="stress-handoff"
HANDOFF_KEYS="${STRESS_HANDOFF_KEYS:-12}"
HANDOFF_DEADLINE="${STRESS_HANDOFF_DEADLINE:-180}"
HANDOFF_FAILURES=0
HANDOFF_CASES=0

# render_handoff_profile turns handoff on and degraded writes explicitly off,
# so the write floor stays the full stripe. Repair is on so the shards can be
# watched coming home, and the bucket is public so an unsigned GET can read the
# header saying what a read cost.
render_handoff_profile() {
    render_profile "$CONFIG_DIR/$CONFIG_NAME.toml" "$HANDOFF_CONFIG" "$(( PORT_OFFSET + 200 ))"
    pin_availability "$HANDOFF_CONFIG" false true
    cat >> "$HANDOFF_CONFIG" <<EOF

[repair]
enabled = true
interval_seconds = 5
page_size = 64

[[bucket]]
name = "$HANDOFF_BUCKET"
region = "$REGION"
public = true
account_id = "123456789012"
EOF
    grep -q '^hinted_handoff = true$' "$HANDOFF_CONFIG" \
        || fail "handoff: hinted_handoff was not written into $HANDOFF_CONFIG"
    if grep -q '^degraded_writes = true$' "$HANDOFF_CONFIG"; then
        fail "handoff: degraded writes are on, so an accepted write proves nothing about handoff"
    fi
    cp "$HANDOFF_CONFIG" "$RUN_DIR/$HANDOFF_CLUSTER.toml"
}

handoff_check() {
    local ok="$1" message="$2"
    HANDOFF_CASES=$(( HANDOFF_CASES + 1 ))
    if [ "$ok" = true ]; then
        log "handoff: pass, $message"
    else
        log "handoff: FAIL $message"
        HANDOFF_FAILURES=$(( HANDOFF_FAILURES + 1 ))
    fi
}

run_handoff() {
    local src="$WORK_DIR/handoff-src.bin" got="$WORK_DIR/handoff-got.bin"
    local keys=() data_keys=()
    local key i role frozen gate fpid frozen_gate

    render_handoff_profile
    log "handoff: starting $HANDOFF_CLUSTER with hinted handoff on and the full stripe as the write floor"
    "$SCRIPTS_DIR/start.sh" -w "$HANDOFF_CLUSTER"

    openssl rand -out "$src" 2097152
    for i in $(seq 1 "$HANDOFF_KEYS"); do
        keys+=("$(printf 'handoff-%03d.bin' "$i")")
    done

    frozen="$(parse_hosts "$HANDOFF_CONFIG" | awk 'NR == 1 { print $1 }')"
    gate="$(survivor_gate "$HANDOFF_CONFIG" "$frozen")"
    [ -n "$gate" ] || fail "handoff: no gate survives stopping host $frozen"
    frozen_gate="$(parse_hosts "$HANDOFF_CONFIG" \
        | awk -v h="$frozen" '$1 == h && $3 != "" { print "https://" $2 ":" $3; exit }')"
    [ -n "$frozen_gate" ] || fail "handoff: host $frozen runs no gate, so nothing repairs its blob node"

    # Only a key whose data shard sits on the stopped host can say anything
    # about the read path: an ordinary GET reads the data shards and never
    # touches parity.
    for key in "${keys[@]}"; do
        role="$("$SHARD_PROBE" -config "$HANDOFF_CONFIG" -bucket "$HANDOFF_BUCKET" -key "$key" \
            | awk -v h="host=$frozen" '$4 == h { sub(/^role=/, "", $2); print $2; exit }')"
        if [ "$role" = data ]; then
            data_keys+=("$key")
        fi
    done
    [ "${#data_keys[@]}" -gt 0 ] \
        || fail "handoff: no key places a data shard on host $frozen, so the read check proves nothing"
    log "handoff: host $frozen holds a data shard of ${#data_keys[@]} of the $HANDOFF_KEYS keys"

    fpid="$(cat "$HANDOFF_PID_DIR/host-${frozen}.pid")"
    kill -0 "$fpid" 2>/dev/null || fail "handoff: host $frozen is not running"

    local accepted=0
    kill -STOP "$fpid"
    log "handoff: SIGSTOP host $frozen, writing $HANDOFF_KEYS objects through $gate"
    for key in "${keys[@]}"; do
        if aws_s3 "$gate" --cli-connect-timeout 10 --cli-read-timeout 120 \
            s3api put-object --bucket "$HANDOFF_BUCKET" --key "$key" --body "$src" \
            >/dev/null 2>>"$RUN_DIR/handoff-puts.txt"; then
            accepted=$(( accepted + 1 ))
        fi
    done
    kill -CONT "$fpid"
    log "handoff: SIGCONT host $frozen; $accepted of $HANDOFF_KEYS writes were accepted with it down"

    # The whole claim in one number. The floor is the full stripe, so a write
    # acknowledged with a host down placed every shard somewhere, and the only
    # somewhere available was the node off the end of the ring.
    handoff_check "$([ "$accepted" -eq "$HANDOFF_KEYS" ] && echo true || echo false)" \
        "all $HANDOFF_KEYS writes landed at full width with host $frozen down"
    [ "$accepted" -eq "$HANDOFF_KEYS" ] \
        || fail "handoff: writes were refused with a host down, so nothing below is attributable"

    warm_gate "$frozen_gate" "$HANDOFF_BUCKET" "${keys[0]}" handoff \
        || fail "handoff: the thawed host's gate never served, so its sweep cannot be observed"

    # What the shards are worth. A handed-off object has to read back with no
    # reconstruction: the bytes are on a node the record does not name, and a
    # gate that could not find them there would be serving a stripe that is
    # complete only on paper.
    local bad=0 seen
    for key in "${data_keys[@]}"; do
        seen="$(degraded_read "$gate" "$HANDOFF_BUCKET" "$key" "$got" 2>/dev/null || true)"
        [ "$seen" = 0 ] || bad=$(( bad + 1 ))
    done
    handoff_check "$([ "$bad" -eq 0 ] && echo true || echo false)" \
        "reading the ${#data_keys[@]} objects whose data shard was handed off costs no reconstruction ($bad did)"

    # Home is where the record says. Stopping the holder is what tells the two
    # apart: until repair returns the shard, the only copy is on the node about
    # to be stopped and the read has to rebuild it from parity.
    local hkey holder hgate hpid started deadline settled=false
    hkey="${data_keys[0]}"
    holder="$(shard_host "$HANDOFF_CONFIG" "$HANDOFF_BUCKET" "$hkey" handoff)"
    [ -n "$holder" ] || fail "handoff: $hkey has no handoff holder, so the cluster has no node to spare"
    if [ "$holder" = "$frozen" ]; then
        fail "handoff: the holder of $hkey is the host that was down, which cannot have taken its shard"
    fi
    hgate="$(survivor_gate "$HANDOFF_CONFIG" "$holder")"
    hpid="$(cat "$HANDOFF_PID_DIR/host-${holder}.pid")"
    warm_gate "$hgate" "$HANDOFF_BUCKET" "$hkey" handoff-home \
        || fail "handoff: the read gate did not serve before host $holder was stopped"

    log "handoff: waiting for the sweep to return $hkey's shard from host $holder to host $frozen"
    started="$(date +%s)"
    deadline=$(( started + HANDOFF_DEADLINE ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        kill -STOP "$hpid"
        seen="$(degraded_read "$hgate" "$HANDOFF_BUCKET" "$hkey" "$got" 2>/dev/null || true)"
        kill -CONT "$hpid"
        if [ "$seen" = 0 ] && cmp -s "$src" "$got"; then
            settled=true
            break
        fi
        sleep 5
    done
    handoff_check "$settled" \
        "$hkey reads without reconstruction while its holder is stopped, so the shard is back on host $frozen"

    # Restored is not the same as restored correctly, and every count above
    # would be satisfied by a shard that reads back as the wrong bytes.
    bad=0
    for key in "${keys[@]}"; do
        if ! aws_s3 "$gate" s3 cp "s3://$HANDOFF_BUCKET/$key" "$got" --only-show-errors 2>>"$EVENTS"; then
            log "handoff: GET errored for $key"
            bad=$(( bad + 1 ))
            continue
        fi
        cmp -s "$src" "$got" || { log "handoff: $key does not match what was written"; bad=$(( bad + 1 )); }
    done
    handoff_check "$([ "$bad" -eq 0 ] && echo true || echo false)" \
        "all $HANDOFF_KEYS objects read back byte for byte"

    log "handoff: stopping $HANDOFF_CLUSTER"
    stop_clusters
    if [ "$HANDOFF_FAILURES" -eq 0 ]; then
        log "handoff: passed $HANDOFF_CASES assertions"
    else
        log "handoff: FAILED $HANDOFF_FAILURES of $HANDOFF_CASES assertions"
    fi
}

if [ "$SCENARIO" = all ] || [ "$SCENARIO" = handoff ]; then
    run_handoff

    if [ "$SCENARIO" = handoff ]; then
        echo "Stress results: $RUN_DIR"
        [ "$HANDOFF_FAILURES" -eq 0 ] \
            || fail "handoff failed $HANDOFF_FAILURES of $HANDOFF_CASES assertions"
        exit 0
    fi
fi

# --- Scenarios: node-rejoin, node-resync, node-rebuild ---
#
# Every other fault in this file is a SIGSTOP: the host is stalled, and what
# it holds is still on its disk when it thaws. These three take a host away
# properly and bring it back, which is the outage an operator actually has,
# and they differ only in how much state it comes back with.
#
#   node-rejoin   the disk survives, and the node is within the retained log,
#                 so metadata arrives by log replay
#   node-resync   the disk survives, but the profile pins the retention
#                 boundary so low that a dozen writes put the node past it,
#                 so metadata can only arrive by snapshot install
#   node-rebuild  the data directory is deleted, so there is no log, no
#                 metadata and no shard to come back to
#
# The middle one is the reason the raft knobs are configurable. TrailingLogs
# defaults to 10240, which is about 5,100 object writes, so reaching the
# snapshot path by writing to it costs minutes of upload to exercise one
# branch. Pinned at 8 it happens immediately, over the identical code path.

NODE_KEYS="${STRESS_NODE_KEYS:-12}"
NODE_PREKEYS="${STRESS_NODE_PREKEYS:-4}"
NODE_DEADLINE="${STRESS_NODE_DEADLINE:-240}"
NODE_FAILURES=0
NODE_CASES=0

node_check() {
    local ok="$1" message="$2"
    NODE_CASES=$(( NODE_CASES + 1 ))
    if [ "$ok" = true ]; then
        log "$NODE_MODE: pass, $message"
    else
        log "$NODE_MODE: FAIL $message"
        NODE_FAILURES=$(( NODE_FAILURES + 1 ))
    fi
}

# stop_host takes one host down the way a machine goes down, and waits for the
# process to be gone rather than for the signal to be sent. A pid that is still
# alive still holds the ports, and the restart below would fail somewhere
# unrelated to what is being tested.
stop_host() {
    local cluster="$1" host="$2" pidfile pid waited=0
    pidfile="$PREDA_DIR/$cluster/pids/host-${host}.pid"
    [ -f "$pidfile" ] || fail "$NODE_MODE: no pidfile for host $host"
    pid="$(cat "$pidfile")"

    kill -TERM "$pid" 2>/dev/null || true
    while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 30 ]; do
        sleep 1
        waited=$(( waited + 1 ))
    done
    if kill -0 "$pid" 2>/dev/null; then
        kill -KILL "$pid" 2>/dev/null || true
        sleep 1
        log "$NODE_MODE: host $host ignored SIGTERM for ${waited}s and was killed"
    else
        log "$NODE_MODE: host $host stopped after ${waited}s"
    fi
    rm -f "$pidfile"
}

# start_host relaunches one host with the arguments start.sh gave it. The certs
# and the master key are cluster-wide and already on disk, so this is the same
# process the cluster started with rather than a new kind of node.
start_host() {
    local cluster="$1" host="$2"
    local base="$PREDA_DIR/$cluster"
    local config="$PREDA_CONFIG_DIR/$cluster.toml"

    mkdir -p "$base/host-${host}" "$base/logs" "$base/pids"
    nohup "$REPO_DIR/bin/s3d" \
        -config "$config" \
        -host "$host" \
        -data-dir "$base/host-${host}" \
        -tls-cert "$PREDA_DIR/server.pem" \
        -tls-key "$PREDA_DIR/server.key" \
        -encryption-key "$PREDA_DIR/master.key" \
        >> "$base/logs/host-${host}.log" 2>&1 &
    echo $! > "$base/pids/host-${host}.pid"
    log "$NODE_MODE: host $host restarted as pid $(cat "$base/pids/host-${host}.pid")"
}

# render_node_profile is the shared profile with the availability settings the
# scenario needs, plus — for the two snapshot cases — a retention boundary
# small enough to cross. snapshot_threshold and snapshot_interval_seconds are
# pinned with it: a boundary the leader never compacts past is not a boundary.
render_node_profile() {
    local target="$1" offset="$2" pin="$3"

    render_profile "$CONFIG_DIR/$CONFIG_NAME.toml" "$target" "$offset"
    pin_availability "$target" true true
    cat >> "$target" <<EOF

[repair]
enabled = true
interval_seconds = 5
page_size = 64

[[bucket]]
name = "$NODE_BUCKET"
region = "$REGION"
public = true
account_id = "123456789012"
EOF
    if [ "$pin" = true ]; then
        cat >> "$target" <<'EOF'

[meta]
snapshot_interval_seconds = 1
snapshot_threshold = 4
trailing_logs = 8
EOF
    fi
    grep -q '^degraded_writes = true$' "$target" \
        || fail "$NODE_MODE: degraded_writes was not written into $target"
    cp "$target" "$RUN_DIR/$(basename "$target")"
}

# installs_seen counts snapshot installs in one host's journal. It reads the
# warning the FSM emits when a restore arrives after the replica is already
# serving, which is the only thing that distinguishes a leader sending a
# snapshot from raft replaying a local one at boot.
installs_seen() {
    local cluster="$1" host="$2" logfile
    logfile="$PREDA_DIR/$cluster/logs/host-${host}.log"
    [ -f "$logfile" ] || { echo 0; return; }
    # grep -c prints its zero and then exits 1, so the status is swallowed
    # rather than answered: an || branch here would print a second count.
    grep -c 'catching up by snapshot install' "$logfile" 2>/dev/null || true
}

# node_meta_status probes this scenario's own cluster. meta_status closes over
# the shared profile, so asking it about a replica of a scenario cluster gets an
# answer about a different cluster's node of that number, or no answer at all —
# the same trap shard_host takes its profile as an argument to avoid.
node_meta_status() {
    "$META_PROBE" -config "$NODE_CONFIG" -ca "$CA_FILE" "$@" 2>/dev/null || true
}

# rejoined waits for one replica's applied index to reach the leader's. That is
# the property, and it is the reason nothing here asserts on a duration: a node
# that caught up in 40s on a loaded machine is not a failure, and a node that
# never caught up is not a slow pass.
rejoined() {
    local victim_meta="$1" deadline applied leader
    deadline=$(( $(date +%s) + NODE_DEADLINE ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        leader="$(node_meta_status "${NODE_META_ALL[@]}" \
            | awk '/is_leader=true/ { for (i = 1; i <= NF; i++) if ($i ~ /^applied=/) { sub(/^applied=/, "", $i); print $i; exit } }')"
        applied="$(node_meta_status "$victim_meta" \
            | awk -v n="$victim_meta" '$1 == "node=" n { for (i = 1; i <= NF; i++) if ($i ~ /^applied=/) { sub(/^applied=/, "", $i); print $i } }')"
        if [ -n "$leader" ] && [ -n "$applied" ] && [ "$applied" -ge "$leader" ]; then
            echo "$applied"
            return 0
        fi
        sleep 3
    done
    echo "${applied:-none}"

    return 1
}

run_node_recovery() {
    NODE_MODE="$1"
    local pin="$2" offset="$3" wipe="$4"
    local cluster="${CONFIG_NAME}-$NODE_MODE"
    local config="$PREDA_CONFIG_DIR/$cluster.toml"
    local src="$WORK_DIR/$NODE_MODE-src.bin" got="$WORK_DIR/$NODE_MODE-got.bin"
    local keys=() data_keys=() key i victim gate victim_gate victim_meta
    local before after settled applied

    NODE_BUCKET="stress-$NODE_MODE"
    NODE_CONFIG="$config"
    render_node_profile "$config" "$offset" "$pin"
    log "$NODE_MODE: starting $cluster"
    "$SCRIPTS_DIR/start.sh" -w "$cluster"

    mapfile -t NODE_META_ALL < <(meta_nodes "$config" | awk '{print $2}')

    # The last host in the profile, so the scenario names one host rather than
    # whichever the hash happened to favour. It has to be one the cluster can
    # lose: a majority of meta replicas must survive it, or what is measured is
    # loss of quorum rather than a replica catching up.
    victim="$(parse_hosts "$config" | awk 'END { print $1 }')"
    victim_meta="$(meta_nodes "$config" | awk -v h="$victim" '$1 == h { print $2; exit }')"
    [ -n "$victim_meta" ] || fail "$NODE_MODE: host $victim runs no meta replica"
    [ "$(( ${#NODE_META_ALL[@]} - 1 ))" -gt $(( ${#NODE_META_ALL[@]} / 2 )) ] \
        || fail "$NODE_MODE: losing host $victim leaves no quorum"

    gate="$(survivor_gate "$config" "$victim")"
    [ -n "$gate" ] || fail "$NODE_MODE: no gate survives losing host $victim"
    victim_gate="$(parse_hosts "$config" \
        | awk -v h="$victim" '$1 == h && $3 != "" { print "https://" $2 ":" $3; exit }')"
    [ -n "$victim_gate" ] || fail "$NODE_MODE: host $victim runs no gate"

    openssl rand -out "$src" 1048576
    for i in $(seq 1 "$NODE_KEYS"); do
        keys+=("$(printf '%s-%03d.bin' "$NODE_MODE" "$i")")
    done

    # Only a key whose data shard the victim owns can answer the
    # no-reconstruction check: an ordinary GET reads the data shards and never
    # touches parity, so a parity key reports nothing either way.
    for key in "${keys[@]}"; do
        if [ "$("$SHARD_PROBE" -config "$config" -bucket "$NODE_BUCKET" -key "$key" \
            | awk -v h="host=$victim" '$4 == h { sub(/^role=/, "", $2); print $2; exit }')" = data ]; then
            data_keys+=("$key")
        fi
    done
    [ "${#data_keys[@]}" -gt 0 ] \
        || fail "$NODE_MODE: no key places a data shard on host $victim"

    # Objects written while the victim is still up, so its metadata store holds
    # rows the snapshot will also hold. Without them every row is new on the way
    # back and the restore cannot show it kept anything.
    local pre_keys=() pre_accepted=0
    for i in $(seq 1 "$NODE_PREKEYS"); do
        pre_keys+=("$(printf '%s-pre-%03d.bin' "$NODE_MODE" "$i")")
    done
    for key in "${pre_keys[@]}"; do
        if aws_s3 "$gate" --cli-connect-timeout 10 --cli-read-timeout 120 \
            s3api put-object --bucket "$NODE_BUCKET" --key "$key" --body "$src" \
            >/dev/null 2>>"$RUN_DIR/$NODE_MODE-puts.txt"; then
            pre_accepted=$(( pre_accepted + 1 ))
        fi
    done
    node_check "$([ "$pre_accepted" -eq "$NODE_PREKEYS" ] && echo true || echo false)" \
        "all $NODE_PREKEYS objects predating the outage landed while host $victim was up"

    before="$(installs_seen "$cluster" "$victim")"
    stop_host "$cluster" "$victim"

    local accepted=0
    log "$NODE_MODE: writing $NODE_KEYS objects through $gate with host $victim gone"
    for key in "${keys[@]}"; do
        if aws_s3 "$gate" --cli-connect-timeout 10 --cli-read-timeout 120 \
            s3api put-object --bucket "$NODE_BUCKET" --key "$key" --body "$src" \
            >/dev/null 2>>"$RUN_DIR/$NODE_MODE-puts.txt"; then
            accepted=$(( accepted + 1 ))
        fi
    done
    node_check "$([ "$accepted" -eq "$NODE_KEYS" ] && echo true || echo false)" \
        "all $NODE_KEYS writes landed with host $victim gone"
    [ "$accepted" -eq "$NODE_KEYS" ] \
        || fail "$NODE_MODE: writes did not land, so nothing below is attributable"

    # The two snapshot cases need the leader to have compacted past the
    # returning node's position. The threshold is 4 and the interval 1s, so a
    # dozen writes and a few seconds is enough; waiting for the leader to say it
    # persisted one is better than assuming it.
    if [ "$pin" = true ]; then
        local waited=0
        while [ "$waited" -lt 30 ]; do
            grep -qh 'meta: snapshot persisted' "$PREDA_DIR/$cluster"/logs/host-*.log 2>/dev/null && break
            sleep 2
            waited=$(( waited + 2 ))
        done
        log "$NODE_MODE: leader persisted a snapshot after ${waited}s"
    fi

    if [ "$wipe" = true ]; then
        rm -rf "${PREDA_DIR:?}/$cluster/host-${victim:?}"
        log "$NODE_MODE: deleted host $victim data directory — no raft log, no metadata, no shards"
    fi

    start_host "$cluster" "$victim"

    if applied="$(rejoined "$victim_meta")"; then
        node_check true "host $victim rejoined and applied index reached the leader at $applied"
    else
        node_check false "host $victim never caught up; applied index stuck at $applied"
    fi

    after="$(installs_seen "$cluster" "$victim")"
    if [ "$pin" = true ]; then
        node_check "$([ "$after" -gt "$before" ] && echo true || echo false)" \
            "host $victim caught up by snapshot install, which is the path the pinned retention forces"
    else
        node_check "$([ "$after" -eq "$before" ] && echo true || echo false)" \
            "host $victim caught up by log replay, with no snapshot install"
    fi

    # The gate in the returned host's own process has to be serving before its
    # sweep can be observed, and a gate that never serves would make every
    # check below pass by reading somewhere else.
    warm_gate "$victim_gate" "$NODE_BUCKET" "${keys[0]}" "$NODE_MODE" \
        || fail "$NODE_MODE: the returned host's gate never served"

    local intact=0
    for key in "${keys[@]}"; do
        if aws_s3 "$victim_gate" --cli-connect-timeout 10 --cli-read-timeout 120 \
            s3 cp "s3://$NODE_BUCKET/$key" "$got" --only-show-errors >/dev/null 2>&1 \
            && diff -q "$src" "$got" >/dev/null 2>&1; then
            intact=$(( intact + 1 ))
        fi
    done
    node_check "$([ "$intact" -eq "$NODE_KEYS" ] && echo true || echo false)" \
        "all $NODE_KEYS objects read back byte for byte through the returned host"

    local pre_intact=0
    for key in "${pre_keys[@]}"; do
        if aws_s3 "$victim_gate" --cli-connect-timeout 10 --cli-read-timeout 120 \
            s3 cp "s3://$NODE_BUCKET/$key" "$got" --only-show-errors >/dev/null 2>&1 \
            && diff -q "$src" "$got" >/dev/null 2>&1; then
            pre_intact=$(( pre_intact + 1 ))
        fi
    done
    node_check "$([ "$pre_intact" -eq "$NODE_PREKEYS" ] && echo true || echo false)" \
        "all $NODE_PREKEYS objects predating the outage survived the catch-up"

    # A restore onto a store that kept its disk must merge rather than rewrite:
    # the rows it already held are reported unchanged. Reading zero here would
    # mean the store was cleared and rebuilt, which is the window this replaced.
    if [ "$pin" = true ] && [ "$wipe" = false ]; then
        local unchanged
        unchanged="$(grep -h 'meta: snapshot restored' \
            "$PREDA_DIR/$cluster/logs/host-${victim}.log" 2>/dev/null \
            | tail -1 | grep -oP '"unchanged":\K[0-9]+' || true)"
        node_check "$([ "${unchanged:-0}" -gt 0 ] && echo true || echo false)" \
            "the restore kept ${unchanged:-0} rows it already held instead of rewriting the store"
    fi

    # Shards are the other half of catching up, and they come back by repair
    # rather than by raft. A read that costs no reconstruction is the evidence:
    # the gate would have hedged to parity and said so if the shard were missing
    # or at the wrong generation.
    local deadline pending=0 seen first=""
    settled=false
    deadline=$(( $(date +%s) + NODE_DEADLINE ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        pending=0
        for key in "${data_keys[@]}"; do
            seen="$(degraded_read "$gate" "$NODE_BUCKET" "$key" "$got" 2>/dev/null || true)"
            [ "$seen" = 0 ] || pending=$(( pending + 1 ))
        done
        if [ -z "$first" ]; then
            first="$pending"
            log "$NODE_MODE: at the first read after the restart, $pending of ${#data_keys[@]} objects still reconstructed"
        fi
        if [ "$pending" -eq 0 ]; then
            settled=true
            break
        fi
        sleep 5
    done
    node_check "$settled" \
        "every object whose data shard host $victim owns reads with no reconstruction, so repair restored them"

    # For the node that lost everything, one more: take away the parity as
    # well. A read then has no way to answer except from the rebuilt shard
    # itself, so it distinguishes a shard that is really back from a header
    # that merely says no reconstruction was needed.
    if [ "$wipe" = true ] && [ "$settled" = true ]; then
        local proof="${data_keys[0]}" parity_host parity_gate
        parity_host="$(shard_host "$config" "$NODE_BUCKET" "$proof" parity)"
        if [ -z "$parity_host" ] || [ "$parity_host" = "$victim" ]; then
            log "$NODE_MODE: $proof keeps its parity on host $victim, so the parity-down check is not available"
        else
            parity_gate="$(parse_hosts "$config" \
                | awk -v a="$victim" -v b="$parity_host" '$1 != a && $1 != b && $3 != "" { print "https://" $2 ":" $3; exit }')"
            stop_host "$cluster" "$parity_host"
            seen="$(degraded_read "$parity_gate" "$NODE_BUCKET" "$proof" "$got" 2>/dev/null || echo failed)"
            node_check "$([ "$seen" = 0 ] && diff -q "$src" "$got" >/dev/null 2>&1 && echo true || echo false)" \
                "$proof reads correctly with its parity host $parity_host also down, so host $victim rebuilt the shard itself"
            start_host "$cluster" "$parity_host"
        fi
    fi

    log "$NODE_MODE: stopping $cluster"
    stop_clusters

    # The snapshot lifecycle is only reported in the returning host's log, and
    # the work directory does not survive the run, so a failure investigated
    # afterwards has nothing to read unless the logs are copied out here.
    mkdir -p "$RUN_DIR/logs-$NODE_MODE"
    cp -R "$PREDA_DIR/$cluster/logs/." "$RUN_DIR/logs-$NODE_MODE/" 2>/dev/null || true
    if [ "$NODE_FAILURES" -eq 0 ]; then
        log "$NODE_MODE: passed $NODE_CASES assertions"
    else
        log "$NODE_MODE: FAILED $NODE_FAILURES of $NODE_CASES assertions"
    fi
}

for node_scenario in node-rejoin node-resync node-rebuild; do
    if [ "$SCENARIO" = all ] || [ "$SCENARIO" = "$node_scenario" ]; then
        case "$node_scenario" in
            node-rejoin)  run_node_recovery node-rejoin  false "$(( PORT_OFFSET + 400 ))" false ;;
            node-resync)  run_node_recovery node-resync  true  "$(( PORT_OFFSET + 500 ))" false ;;
            node-rebuild) run_node_recovery node-rebuild true  "$(( PORT_OFFSET + 600 ))" true  ;;
        esac

        if [ "$SCENARIO" = "$node_scenario" ]; then
            echo "Stress results: $RUN_DIR"
            [ "$NODE_FAILURES" -eq 0 ] \
                || fail "$node_scenario failed $NODE_FAILURES of $NODE_CASES assertions"
            exit 0
        fi
    fi
done

# --- Scenario: large-object ---
#
# Every other scenario in this file writes 8 MiB or less. At RS(2,1) that is a
# 4 MiB shard: exactly one reedsolomon stream block, and inside every timeout
# the gate applies. So the whole suite can be green while the gate cannot serve
# an object larger than about a gigabyte, which is what happened.
#
# This scenario writes objects big enough to leave that regime, and records
# what they cost. The assertion that matters is not a throughput number but a
# flat memory curve: peak RSS at the largest size must not be materially above
# peak RSS at the smallest. A gate that streams has a working set set by its
# block size; a gate that buffers has one set by the object.

LARGE_CLUSTER="${CONFIG_NAME}-large"
LARGE_CONFIG="$PREDA_CONFIG_DIR/$LARGE_CLUSTER.toml"
LARGE_PID_DIR="$PREDA_DIR/$LARGE_CLUSTER/pids"
LARGE_BUCKET="stress-large"
LARGE_SIZES="${STRESS_LARGE_SIZES:-2GiB 4GiB}"
LARGE_FAILURES=0
LARGE_CASES=0
LARGE_SKIPPED=0
LARGE_RSS_SAMPLER=""

# Deterministic pseudorandom bytes at line rate, from a fixed key over zeros.
# The object is generated twice — once to digest, once to upload — rather than
# staged, because staging a copy of a 16 GiB object doubles the disk footprint
# to prove nothing the digest does not already prove.
LARGE_KEY="00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
LARGE_IV="000102030405060708090a0b0c0d0e0f"

gen_stream() {
    head -c "$1" /dev/zero | openssl enc -aes-256-ctr -K "$LARGE_KEY" -iv "$LARGE_IV" -nosalt 2>/dev/null
}

# parse_size turns 2GiB, 512MiB or a bare byte count into bytes. An
# unrecognised unit is fatal rather than defaulted: a scenario that silently
# ran at 2 bytes would pass every assertion in it.
parse_size() {
    local spec="$1" n unit
    n="${spec%%[!0-9]*}"
    unit="${spec#"$n"}"
    [ -n "$n" ] || fail "large-object: cannot parse size '$spec'"
    case "$unit" in
        ""|B) echo "$n" ;;
        KiB|K) echo $(( n * 1024 )) ;;
        MiB|M) echo $(( n * 1024 * 1024 )) ;;
        GiB|G) echo $(( n * 1024 * 1024 * 1024 )) ;;
        *) fail "large-object: unknown unit in '$spec'" ;;
    esac
}

# Peak RSS per node, sampled while the transfer runs. Sampling is the only way
# to see this: the peak is transient and gone by the time a transfer returns.
start_rss_sampler() {
    local out="$1"
    : > "$out"
    (
        while :; do
            local pidfile pid rss
            for pidfile in "$LARGE_PID_DIR"/*.pid; do
                [ -e "$pidfile" ] || continue
                pid="$(cat "$pidfile" 2>/dev/null || true)"
                [ -n "$pid" ] || continue
                rss="$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ' || true)"
                if [ -n "$rss" ]; then
                    printf '%s %s\n' "$(basename "$pidfile" .pid)" "$rss" >> "$out"
                fi
            done
            sleep 1
        done
    ) &
    LARGE_RSS_SAMPLER=$!
}

stop_rss_sampler() {
    if [ -n "$LARGE_RSS_SAMPLER" ] && kill -0 "$LARGE_RSS_SAMPLER" 2>/dev/null; then
        kill "$LARGE_RSS_SAMPLER" 2>/dev/null || true
        wait "$LARGE_RSS_SAMPLER" 2>/dev/null || true
    fi
    LARGE_RSS_SAMPLER=""
}

# The largest single node's peak, in MiB. The gate handling the request is one
# process, so the maximum across nodes is what says whether it held the object;
# a sum across nodes would blur that with the blob nodes' own buffers.
peak_rss_mib() {
    awk '{ if ($2 > peak[$1]) peak[$1] = $2 }
         END { m = 0; for (h in peak) if (peak[h] > m) m = peak[h]; printf "%d", m / 1024 }' "$1"
}

render_large_profile() {
    render_profile "$CONFIG_DIR/$CONFIG_NAME.toml" "$LARGE_CONFIG" "$(( PORT_OFFSET + 300 ))"
    pin_availability "$LARGE_CONFIG" false false
    pin_repair_off "$LARGE_CONFIG"
    cat >> "$LARGE_CONFIG" <<EOF

[[bucket]]
name = "$LARGE_BUCKET"
region = "$REGION"
public = true
account_id = "123456789012"
EOF
    cp "$LARGE_CONFIG" "$RUN_DIR/$LARGE_CLUSTER.toml"
}

large_check() {
    local ok="$1" message="$2"
    LARGE_CASES=$(( LARGE_CASES + 1 ))
    if [ "$ok" = true ]; then
        log "large-object: pass, $message"
    else
        log "large-object: FAIL $message"
        LARGE_FAILURES=$(( LARGE_FAILURES + 1 ))
    fi
}

# Anything at or below this goes through s3api put-object, which is a single
# PUT and exercises writeObject directly. Above it the object is streamed from
# stdin and the CLI makes it multipart, which reaches the same code through
# CompleteMultipartUpload with the assembled size. Both paths matter and they
# are not the same path.
LARGE_SINGLE_SHOT_MAX=$(( 4 * 1024 * 1024 * 1024 ))

run_large_object() {
    local results="$RUN_DIR/large-object.tsv"
    local gate spec bytes need avail key src digest got_digest
    local put_peak get_peak put_start put_secs get_secs reconstructed hedges status hdr

    render_large_profile

    gate="$(parse_hosts "$LARGE_CONFIG" | awk '$3 != "" { print "https://" $2 ":" $3; exit }')"
    [ -n "$gate" ] || fail "large-object: no gate in $LARGE_CLUSTER"

    printf 'size\tbytes\tpath\tput_s\tput_MiBps\tget_s\tget_MiBps\tput_peak_MiB\tget_peak_MiB\treconstructed\tverdict\n' > "$results"

    for spec in $LARGE_SIZES; do
        bytes="$(parse_size "$spec")"
        key="large-${spec}.bin"

        # Each size gets the cluster to itself on an empty data directory. The
        # shards of an earlier size are not returned by DeleteObject promptly
        # enough to fund the next one, and a cold store also makes the sizes
        # comparable to each other rather than to whatever ran before them.
        stop_clusters
        if [ -d "$PREDA_DIR/$LARGE_CLUSTER/logs" ]; then
            mkdir -p "$RUN_DIR/logs-large"
            cp -R "$PREDA_DIR/$LARGE_CLUSTER/logs/." "$RUN_DIR/logs-large/" 2>/dev/null || true
        fi
        rm -rf "${PREDA_DIR:?}/$LARGE_CLUSTER"
        log "large-object: starting $LARGE_CLUSTER for $spec"
        "$SCRIPTS_DIR/start.sh" -w "$LARGE_CLUSTER"

        # Shards are 1.5x the object at RS(2,1). The single-shot path needs a
        # source file on top of that, so 2.5x.
        #
        # The multipart path needs 3x, not 1.5x: CompleteMultipartUpload
        # assembles the object while the parts are still stored, and only
        # deletes them afterwards, so both exist at once. Assuming 1.5x here is
        # what filled the disk mid-run rather than skipping the size cleanly.
        if [ "$bytes" -le "$LARGE_SINGLE_SHOT_MAX" ]; then
            need=$(( bytes * 5 / 2 + 2 * 1024 * 1024 * 1024 ))
        else
            need=$(( bytes * 3 + 2 * 1024 * 1024 * 1024 ))
        fi
        avail="$(df -PB1 "$PREDA_DIR" | awk 'NR == 2 { print $4 }')"
        if [ "$avail" -lt "$need" ]; then
            log "large-object: SKIP $spec — needs $(( need / 1024 / 1024 / 1024 ))GiB free under $PREDA_DIR, has $(( avail / 1024 / 1024 / 1024 ))GiB"
            printf '%s\t%s\t-\t-\t-\t-\t-\t-\t-\t-\tskipped-no-disk\n' "$spec" "$bytes" >> "$results"
            LARGE_SKIPPED=$(( LARGE_SKIPPED + 1 ))
            continue
        fi

        digest="$(gen_stream "$bytes" | sha256sum | awk '{ print $1 }')"
        start_rss_sampler "$WORK_DIR/large-rss-put-$spec.txt"

        put_start="$(date +%s)"
        if [ "$bytes" -le "$LARGE_SINGLE_SHOT_MAX" ]; then
            src="$WORK_DIR/large-$spec.bin"
            gen_stream "$bytes" > "$src"
            status=ok
            aws_s3 "$gate" --cli-connect-timeout 10 --cli-read-timeout 0 \
                s3api put-object --bucket "$LARGE_BUCKET" --key "$key" --body "$src" \
                >/dev/null 2>>"$RUN_DIR/large-puts.txt" || status=put-failed
            rm -f "$src"
        else
            status=ok
            gen_stream "$bytes" | aws_s3 "$gate" --cli-connect-timeout 10 --cli-read-timeout 0 \
                s3 cp - "s3://$LARGE_BUCKET/$key" --expected-size "$bytes" --only-show-errors \
                2>>"$RUN_DIR/large-puts.txt" || status=put-failed
        fi
        put_secs=$(( $(date +%s) - put_start ))
        stop_rss_sampler
        put_peak="$(peak_rss_mib "$WORK_DIR/large-rss-put-$spec.txt")"

        if [ "$status" != ok ]; then
            large_check false "$spec PUT failed after ${put_secs}s (peak RSS ${put_peak}MiB) — see large-puts.txt"
            printf '%s\t%s\t%s\t%s\t-\t-\t-\t%s\t-\t-\tput-failed\n' \
                "$spec" "$bytes" "$([ "$bytes" -le "$LARGE_SINGLE_SHOT_MAX" ] && echo single || echo multipart)" \
                "$put_secs" "$put_peak" >> "$results"
            continue
        fi

        # Read back through the digest rather than to a file: a second copy on
        # disk doubles the footprint and proves nothing more.
        hdr="$WORK_DIR/large-headers-$spec.txt"
        local get_start http_status verdict
        start_rss_sampler "$WORK_DIR/large-rss-get-$spec.txt"
        get_start="$(date +%s)"
        got_digest="$(curl -sk --max-time 3600 -D "$hdr" "$gate/$LARGE_BUCKET/$key" | sha256sum | awk '{ print $1 }')" \
            || got_digest="get-failed"
        get_secs=$(( $(date +%s) - get_start ))

        stop_rss_sampler
        get_peak="$(peak_rss_mib "$WORK_DIR/large-rss-get-$spec.txt")"

        # The status is checked before anything else is read from the response.
        # An error body has no X-Spx-Degraded header, so a 500 would otherwise
        # report zero reconstructions and pass the check that exists to prove
        # the read was clean.
        http_status="$(awk 'NR == 1 { print $2; exit }' "$hdr")"
        if [ "$http_status" != 200 ]; then
            curl -sk -o "$RUN_DIR/large-get-error-$spec.xml" "$gate/$LARGE_BUCKET/$key" 2>/dev/null || true
            large_check false "$spec GET returned HTTP $http_status after ${get_secs}s (peak RSS ${get_peak}MiB) — body in large-get-error-$spec.xml"
            printf '%s\t%s\t%s\t%s\t%s\t%s\t-\t%s\t%s\t-\tget-http-%s\n' \
                "$spec" "$bytes" \
                "$([ "$bytes" -le "$LARGE_SINGLE_SHOT_MAX" ] && echo single || echo multipart)" \
                "$put_secs" "$(( bytes / 1024 / 1024 / (put_secs > 0 ? put_secs : 1) ))" \
                "$get_secs" "$put_peak" "$get_peak" "$http_status" >> "$results"
            continue
        fi

        reconstructed="$(awk 'tolower($1) == "x-spx-degraded:" { gsub(/\r/, "", $2); print $2; found = 1 }
                              END { if (!found) print 0 }' "$hdr")"

        # The header is written before the second stripe is read, so it reports
        # the first stripe alone and is a floor rather than a total. The gate's
        # own log is the authoritative count for everything after that.
        hedges="$(cat "$PREDA_DIR/$LARGE_CLUSTER/logs"/*.log 2>/dev/null \
            | grep -c 'Served degraded read\|Shard delivered below the throughput floor')" || hedges=0

        verdict=mismatch
        if [ "$got_digest" = "$digest" ]; then
            verdict=ok
            large_check true "$spec round-tripped byte for byte in ${put_secs}s up, ${get_secs}s down, peak RSS ${put_peak}MiB writing and ${get_peak}MiB reading"
        else
            large_check false "$spec read back as $got_digest, wrote $digest"
        fi
        large_check "$([ "$reconstructed" = 0 ] && echo true || echo false)" \
            "$spec read cost $reconstructed reconstructions on a healthy cluster"
        large_check "$([ "$hedges" = 0 ] && echo true || echo false)" \
            "$spec read fetched k shards and never fell back to parity ($hedges gate log entries say otherwise)"

        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$spec" "$bytes" \
            "$([ "$bytes" -le "$LARGE_SINGLE_SHOT_MAX" ] && echo single || echo multipart)" \
            "$put_secs" "$(( bytes / 1024 / 1024 / (put_secs > 0 ? put_secs : 1) ))" \
            "$get_secs" "$(( bytes / 1024 / 1024 / (get_secs > 0 ? get_secs : 1) ))" \
            "$put_peak" "$get_peak" "$reconstructed" "$verdict" >> "$results"

        aws_s3 "$gate" s3 rm "s3://$LARGE_BUCKET/$key" --only-show-errors >/dev/null 2>&1 || true
    done

    # The property under test, stated as what streaming means rather than as a
    # growth rate: a streaming path holds a fixed working set, so its peak is
    # below the object at every size, while a buffered one is a multiple of it.
    #
    # This replaced a largest-against-smallest ratio with a 200% budget, which
    # a linear path passes whenever the two sizes are 2x apart -- the read path
    # scored 197% and passed while holding 5x the object. The growth figure is
    # still logged, because it is informative, but it is not the assertion.
    local col label peak size_mib completed
    completed="$(awk -F'\t' 'NR > 1 && $11 == "ok"' "$results" | wc -l)"
    if [ "$completed" -eq 0 ]; then
        log "large-object: NOTE no size round-tripped, so memory was not judged"
    else
        for col in 8 9; do
            if [ "$col" = 8 ]; then label="write"; else label="read"; fi
            while IFS="$(printf '\t')" read -r spec size_mib peak; do
                large_check "$([ "$peak" -lt "$size_mib" ] && echo true || echo false)" \
                    "$spec $label peak RSS ${peak}MiB is below the ${size_mib}MiB object, so the $label path streams"
            done < <(awk -F'\t' -v c="$col" 'NR > 1 && $11 == "ok" {
                         print $1 "\t" int($2 / 1048576) "\t" $c }' "$results")
        done
    fi

    # Appended without the header, prefixed with the run and the commit, so a
    # regression on this path is visible across runs rather than only inside one.
    awk -F'\t' -v run="$STAMP" -v sha="$SHA" 'NR > 1 { print run "\t" sha "\t" $0 }' "$results" \
        >> "$RESULTS_ROOT/large-object-history.tsv"
    log "large-object: results in $results"

    log "large-object: stopping $LARGE_CLUSTER"
    stop_clusters
    # Taken here rather than by the exit trap, which only knows about the
    # shared cluster: without this the evidence for a failure goes with the
    # work directory.
    if [ -d "$PREDA_DIR/$LARGE_CLUSTER/logs" ]; then
        mkdir -p "$RUN_DIR/logs-large"
        cp -R "$PREDA_DIR/$LARGE_CLUSTER/logs/." "$RUN_DIR/logs-large/" 2>/dev/null || true
    fi
    if [ "$LARGE_SKIPPED" -gt 0 ]; then
        log "large-object: $LARGE_SKIPPED size(s) skipped for want of disk — the run does not cover them"
    fi
    if [ "$LARGE_FAILURES" -eq 0 ]; then
        log "large-object: passed $LARGE_CASES assertions"
    else
        log "large-object: FAILED $LARGE_FAILURES of $LARGE_CASES assertions"
    fi
}

# --- Scenario: multipart-upload ---
#
# Multipart is the path large writes actually take: above 4 GiB there is no
# single-shot option, and the AWS CLI switches to it at 8 MiB by default. The
# fault it is here to measure is a working set bounded by how *many* parts are
# held rather than by how much, so the part size is the variable and the part
# count is held fixed.
#
# The parts are driven explicitly rather than through `aws s3 cp`, because the
# window that matters is completion alone. Folded into the upload it would be
# hidden by whichever of the two is larger, which is exactly how a 28x
# improvement in the write path first showed up as 17%.

MP_BUCKET="stress-multipart"
MP_PARTS="${STRESS_MULTIPART_PARTS:-64MiB 256MiB 1GiB}"
MP_PART_COUNT="${STRESS_MULTIPART_COUNT:-8}"
MP_FAILURES=0
MP_CASES=0
MP_SKIPPED=0

mp_check() {
    local ok="$1" message="$2"
    MP_CASES=$(( MP_CASES + 1 ))
    if [ "$ok" = true ]; then
        log "multipart-upload: pass, $message"
    else
        log "multipart-upload: FAIL $message"
        MP_FAILURES=$(( MP_FAILURES + 1 ))
    fi
}

# Each part gets its own IV, so the parts differ from each other. Identical
# parts would let an assembly that dropped or reordered one still digest
# correctly, which is the bug most worth catching here.
gen_part() {
    head -c "$2" /dev/zero \
        | openssl enc -aes-256-ctr -K "$LARGE_KEY" -iv "$(printf '%032x' "$1")" -nosalt 2>/dev/null
}

run_multipart_upload() {
    local results="$RUN_DIR/multipart-upload.tsv"
    local gate spec part_bytes total need avail key digest got_digest
    local upload_id up_peak done_peak get_peak up_secs done_secs get_secs
    local reconstructed verdict two_parts i etag partfile hdr
    local up_start done_start get_start

    render_large_profile
    printf '\n[[bucket]]\nname = "%s"\nregion = "%s"\npublic = true\naccount_id = "123456789012"\n' \
        "$MP_BUCKET" "$REGION" >> "$LARGE_CONFIG"
    cp "$LARGE_CONFIG" "$RUN_DIR/$LARGE_CLUSTER.toml"

    gate="$(parse_hosts "$LARGE_CONFIG" | awk '$3 != "" { print "https://" $2 ":" $3; exit }')"
    [ -n "$gate" ] || fail "multipart-upload: no gate in $LARGE_CLUSTER"

    printf 'part\tparts\tbytes\tupload_s\tupload_MiBps\tcomplete_s\tget_s\tget_MiBps\tupload_peak_MiB\tcomplete_peak_MiB\tget_peak_MiB\treconstructed\tverdict\n' > "$results"

    for spec in $MP_PARTS; do
        part_bytes="$(parse_size "$spec")"
        total=$(( part_bytes * MP_PART_COUNT ))
        key="multipart-${spec}x${MP_PART_COUNT}.bin"

        # Parts and the assembled object coexist until cleanupMultipartUpload
        # runs, so completion transiently needs 3x the object in shards. One
        # staged part on top of that, plus headroom.
        need=$(( total * 3 + part_bytes + 2 * 1024 * 1024 * 1024 ))
        avail="$(df -PB1 "$PREDA_DIR" | awk 'NR == 2 { print $4 }')"
        if [ "$avail" -lt "$need" ]; then
            log "multipart-upload: SKIP ${spec}x${MP_PART_COUNT} — needs $(( need / 1024 / 1024 / 1024 ))GiB free under $PREDA_DIR, has $(( avail / 1024 / 1024 / 1024 ))GiB"
            printf '%s\t%s\t%s\t-\t-\t-\t-\t-\t-\t-\t-\t-\tskipped-no-disk\n' \
                "$spec" "$MP_PART_COUNT" "$total" >> "$results"
            MP_SKIPPED=$(( MP_SKIPPED + 1 ))
            continue
        fi

        # A cold store per part size, so the sizes are comparable to each other
        # rather than to whatever ran before them.
        "$SCRIPTS_DIR/stop.sh" -w "$LARGE_CLUSTER" >/dev/null 2>&1 || true
        rm -rf "${PREDA_DIR:?}/$LARGE_CLUSTER/data"
        log "multipart-upload: starting $LARGE_CLUSTER for ${spec}x${MP_PART_COUNT}"
        "$SCRIPTS_DIR/start.sh" -w "$LARGE_CLUSTER"

        digest="$(for i in $(seq 1 "$MP_PART_COUNT"); do gen_part "$i" "$part_bytes"; done | sha256sum | awk '{ print $1 }')"

        upload_id="$(aws_s3 "$gate" s3api create-multipart-upload \
            --bucket "$MP_BUCKET" --key "$key" --query UploadId --output text)" \
            || { mp_check false "${spec}x${MP_PART_COUNT} create-multipart-upload failed"; continue; }

        : > "$WORK_DIR/mp-parts-$spec.json.parts"
        partfile="$WORK_DIR/mp-part.bin"
        start_rss_sampler "$WORK_DIR/mp-rss-upload-$spec.txt"
        up_start=$(date +%s)
        for i in $(seq 1 "$MP_PART_COUNT"); do
            gen_part "$i" "$part_bytes" > "$partfile"
            etag="$(aws_s3 "$gate" s3api upload-part --bucket "$MP_BUCKET" --key "$key" \
                --upload-id "$upload_id" --part-number "$i" --body "$partfile" \
                --query ETag --output text)" || etag=""
            [ -n "$etag" ] || { mp_check false "${spec}x${MP_PART_COUNT} part $i failed to upload"; break; }
            printf '{"ETag":%s,"PartNumber":%d}\n' "$etag" "$i" >> "$WORK_DIR/mp-parts-$spec.json.parts"
        done
        up_secs=$(( $(date +%s) - up_start ))
        stop_rss_sampler
        rm -f "$partfile"
        up_peak="$(peak_rss_mib "$WORK_DIR/mp-rss-upload-$spec.txt")"

        if [ "$(wc -l < "$WORK_DIR/mp-parts-$spec.json.parts")" -ne "$MP_PART_COUNT" ]; then
            aws_s3 "$gate" s3api abort-multipart-upload --bucket "$MP_BUCKET" --key "$key" \
                --upload-id "$upload_id" >/dev/null 2>&1 || true
            continue
        fi
        printf '{"Parts":[%s]}' "$(paste -sd, "$WORK_DIR/mp-parts-$spec.json.parts")" \
            > "$WORK_DIR/mp-parts-$spec.json"

        # The window this scenario exists for.
        start_rss_sampler "$WORK_DIR/mp-rss-complete-$spec.txt"
        done_start=$(date +%s)
        aws_s3 "$gate" s3api complete-multipart-upload --bucket "$MP_BUCKET" --key "$key" \
            --upload-id "$upload_id" \
            --multipart-upload "file://$WORK_DIR/mp-parts-$spec.json" >/dev/null \
            || mp_check false "${spec}x${MP_PART_COUNT} complete-multipart-upload failed"
        done_secs=$(( $(date +%s) - done_start ))
        stop_rss_sampler
        done_peak="$(peak_rss_mib "$WORK_DIR/mp-rss-complete-$spec.txt")"

        hdr="$WORK_DIR/mp-get-$spec.hdr"
        start_rss_sampler "$WORK_DIR/mp-rss-get-$spec.txt"
        get_start=$(date +%s)
        got_digest="$(curl -sk --max-time 3600 -D "$hdr" "$gate/$MP_BUCKET/$key" | sha256sum | awk '{ print $1 }')" \
            || got_digest="get-failed"
        get_secs=$(( $(date +%s) - get_start ))
        stop_rss_sampler
        get_peak="$(peak_rss_mib "$WORK_DIR/mp-rss-get-$spec.txt")"

        reconstructed="$(awk 'tolower($1) == "x-spx-degraded:" { gsub(/\r/, "", $2); print $2; found = 1 }
                              END { if (!found) print 0 }' "$hdr")"

        verdict=mismatch
        if [ "$got_digest" = "$digest" ]; then
            verdict=ok
            mp_check true "${spec}x${MP_PART_COUNT} ($(( total / 1024 / 1024 ))MiB) round-tripped byte for byte: ${up_secs}s up, ${done_secs}s to complete, ${get_secs}s down"
        else
            mp_check false "${spec}x${MP_PART_COUNT} read back as $got_digest, wrote $digest"
        fi

        # The property, stated in bytes rather than in parts: assembling an
        # object must cost a working set set by the block size, not by the part
        # size the client happened to choose. Measured as growth over the upload
        # phase, because total RSS carries a process floor that no part size
        # explains -- and because Go hands the heap back to the OS lazily, so a
        # buffering completion stays resident and an absolute bound would read
        # the floor rather than the object. Two parts' worth is a generous
        # ceiling that buffering a whole object still fails at every size.
        two_parts=$(( part_bytes * 2 / 1024 / 1024 ))
        mp_check "$([ $(( done_peak - up_peak )) -lt "$two_parts" ] && echo true || echo false)" \
            "${spec}x${MP_PART_COUNT} completion grew RSS by $(( done_peak - up_peak ))MiB (${up_peak}->${done_peak}), under two parts (${two_parts}MiB), so completion streams"
        mp_check "$([ "$reconstructed" = 0 ] && echo true || echo false)" \
            "${spec}x${MP_PART_COUNT} read cost $reconstructed reconstructions on a healthy cluster"

        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$spec" "$MP_PART_COUNT" "$total" \
            "$up_secs" "$(( total / 1024 / 1024 / (up_secs > 0 ? up_secs : 1) ))" \
            "$done_secs" \
            "$get_secs" "$(( total / 1024 / 1024 / (get_secs > 0 ? get_secs : 1) ))" \
            "$up_peak" "$done_peak" "$get_peak" "$reconstructed" "$verdict" >> "$results"

        aws_s3 "$gate" s3 rm "s3://$MP_BUCKET/$key" --only-show-errors >/dev/null 2>&1 || true
    done

    log "multipart-upload: results in $results"
    log "multipart-upload: stopping $LARGE_CLUSTER"
    "$SCRIPTS_DIR/stop.sh" -w "$LARGE_CLUSTER" >/dev/null 2>&1 || true
    if [ -d "$PREDA_DIR/$LARGE_CLUSTER/logs" ]; then
        mkdir -p "$RUN_DIR/logs-multipart"
        cp -R "$PREDA_DIR/$LARGE_CLUSTER/logs/." "$RUN_DIR/logs-multipart/" 2>/dev/null || true
    fi
    if [ "$MP_SKIPPED" -gt 0 ]; then
        log "multipart-upload: $MP_SKIPPED part size(s) skipped for want of disk — the run does not cover them"
    fi
    if [ "$MP_FAILURES" -eq 0 ]; then
        log "multipart-upload: passed $MP_CASES assertions"
    else
        log "multipart-upload: FAILED $MP_FAILURES of $MP_CASES assertions"
    fi
}

if [ "$SCENARIO" = all ] || [ "$SCENARIO" = multipart-upload ]; then
    run_multipart_upload

    if [ "$SCENARIO" = multipart-upload ]; then
        echo "Stress results: $RUN_DIR"
        [ "$MP_FAILURES" -eq 0 ] \
            || fail "multipart-upload failed $MP_FAILURES of $MP_CASES assertions"
        exit 0
    fi
fi

# --- Scenario: last-modified ---
#
# The user-visible claim: every surface that dates an object reports when that
# object was written, and they all report the same thing. HEAD and GET dated
# every object 0001-01-01 and ListObjectsV2 answered the time of the listing,
# so a client that listed a bucket and then headed a key got two answers
# decades apart and an incremental sync saw every object change on every pass.
#
# The date is the write epoch in the placement record, so this is asserted end
# to end rather than in a handler test: it has to survive being encoded into
# the record, committed through raft, and decoded again by a different gate
# from the one that wrote it. Reads are therefore taken from a second gate.

LM_CLUSTER="${CONFIG_NAME}-lastmod"
LM_CONFIG="$PREDA_CONFIG_DIR/$LM_CLUSTER.toml"
LM_BUCKET="stress-lastmod"
LM_FAILURES=0
LM_CASES=0

lm_check() {
    local ok="$1" message="$2"
    LM_CASES=$(( LM_CASES + 1 ))
    if [ "$ok" = true ]; then
        log "last-modified: pass, $message"
    else
        log "last-modified: FAIL $message"
        LM_FAILURES=$(( LM_FAILURES + 1 ))
    fi
}

render_lastmod_profile() {
    render_profile "$CONFIG_DIR/$CONFIG_NAME.toml" "$LM_CONFIG" "$(( PORT_OFFSET + 700 ))"
    pin_availability "$LM_CONFIG" false false
    pin_repair_off "$LM_CONFIG"
    cat >> "$LM_CONFIG" <<EOF

[[bucket]]
name = "$LM_BUCKET"
region = "$REGION"
public = true
account_id = "123456789012"
EOF
    cp "$LM_CONFIG" "$RUN_DIR/$LM_CLUSTER.toml"
}

# epoch_of parses either date form S3 serves — RFC 1123 in a header, ISO 8601
# in a listing — into seconds. An unparseable or absent date prints nothing,
# which every caller below treats as a failure rather than as zero.
epoch_of() {
    [ -n "$1" ] || return 0
    date -u -d "$1" +%s 2>/dev/null || true
}

# The header is read off the wire rather than from the CLI's parse of it: what
# was wrong before was a Last-Modified that was present and false, and a client
# reads exactly these bytes.
lm_header() {
    local method="$1" gate="$2" key="$3" args=(-sk --max-time 120)
    case "$method" in
        HEAD) args+=(-I) ;;
        *) args+=(-o /dev/null -D -) ;;
    esac
    curl "${args[@]}" "$gate/$LM_BUCKET/$key" \
        | awk 'tolower($1) == "last-modified:" { sub(/^[^:]*: */, ""); gsub(/\r/, ""); print; exit }'
}

# within reports whether an epoch falls in the window the write was issued in,
# with a second of slack at each end for the clocks the two sides read.
within() {
    local got="$1" from="$2" to="$3"
    [ -n "$got" ] && [ "$got" -ge $(( from - 1 )) ] && [ "$got" -le $(( to + 1 )) ]
}

run_last_modified() {
    local src="$WORK_DIR/lastmod-src.bin" partfile="$WORK_DIR/lastmod-part.bin"
    local gate read_gate key before after
    local head_raw get_raw list_raw head_epoch get_epoch list_epoch
    local first_epoch second_epoch upload_id etag i
    local part_raw part_epoch object_epoch complete_before

    render_lastmod_profile
    log "last-modified: starting $LM_CLUSTER"
    "$SCRIPTS_DIR/start.sh" -w "$LM_CLUSTER"

    gate="$(parse_hosts "$LM_CONFIG" | awk '$3 != "" { print "https://" $2 ":" $3; exit }')"
    [ -n "$gate" ] || fail "last-modified: no gate in $LM_CLUSTER"
    read_gate="$(survivor_gate "$LM_CONFIG" "$(parse_hosts "$LM_CONFIG" | awk 'NR == 1 { print $1 }')")"
    [ -n "$read_gate" ] || fail "last-modified: $LM_CLUSTER has only one gate, so a read cannot come from another"
    log "last-modified: writing through $gate, reading through $read_gate"

    # --- A whole-object write ---

    key="lastmod-object.bin"
    openssl rand -out "$src" 1048576
    before="$(date -u +%s)"
    aws_s3 "$gate" s3api put-object --bucket "$LM_BUCKET" --key "$key" --body "$src" >/dev/null \
        || fail "last-modified: the object could not be written"
    after="$(date -u +%s)"

    head_raw="$(lm_header HEAD "$read_gate" "$key")"
    head_epoch="$(epoch_of "$head_raw")"
    lm_check "$(within "$head_epoch" "$before" "$after" && echo true || echo false)" \
        "HEAD reports the write time: ${head_raw:-<no Last-Modified header>}"

    get_raw="$(lm_header GET "$read_gate" "$key")"
    get_epoch="$(epoch_of "$get_raw")"
    lm_check "$([ -n "$get_epoch" ] && [ "$get_epoch" = "$head_epoch" ] && echo true || echo false)" \
        "GET and HEAD report the same time: ${get_raw:-<no Last-Modified header>}"

    list_raw="$(aws_s3 "$read_gate" s3api list-objects-v2 --bucket "$LM_BUCKET" \
        --query "Contents[?Key=='$key'].LastModified" --output text)"
    list_epoch="$(epoch_of "$list_raw")"
    lm_check "$([ -n "$list_epoch" ] && [ "$list_epoch" = "$head_epoch" ] && echo true || echo false)" \
        "ListObjectsV2 agrees with HEAD rather than answering the time of the listing: ${list_raw:-<none>}"

    # The two dates the broken surfaces served. Asserted by name because both
    # parse as a time and neither is one an object can have been written at.
    lm_check "$([ "${head_epoch:-0}" -gt 1600000000 ] && echo true || echo false)" \
        "the reported time is a real write time, not the zero date or the Unix epoch"

    # --- An overwrite ---
    #
    # A listing that answered time.Now() passed the window check above and
    # still told a sync every object had changed. What separates the two is
    # that the object's time moves when the object does and not otherwise.

    first_epoch="$head_epoch"
    sleep 2
    before="$(date -u +%s)"
    aws_s3 "$gate" s3api put-object --bucket "$LM_BUCKET" --key "$key" --body "$src" >/dev/null \
        || fail "last-modified: the object could not be overwritten"
    after="$(date -u +%s)"

    second_epoch="$(epoch_of "$(lm_header HEAD "$read_gate" "$key")")"
    lm_check "$([ -n "$second_epoch" ] && [ -n "$first_epoch" ] && [ "$second_epoch" -gt "$first_epoch" ] \
        && echo true || echo false)" \
        "an overwrite advances the reported time ($first_epoch to ${second_epoch:-unknown})"
    lm_check "$(within "$second_epoch" "$before" "$after" && echo true || echo false)" \
        "the overwrite reports its own write time"

    sleep 2
    lm_check "$([ "$(epoch_of "$(lm_header HEAD "$read_gate" "$key")")" = "$second_epoch" ] \
        && echo true || echo false)" \
        "reading again does not move the time"

    # --- A multipart upload ---
    #
    # ListParts is the one surface that always served a real time, so it is
    # asserted as a regression: parts are dated from their own epoch now, and
    # the assembled object is dated from its completion rather than from the
    # first part, which is the S3 semantic.

    key="lastmod-multipart.bin"
    upload_id="$(aws_s3 "$gate" s3api create-multipart-upload \
        --bucket "$LM_BUCKET" --key "$key" --query UploadId --output text)" \
        || fail "last-modified: create-multipart-upload failed"

    head -c $(( 5 * 1024 * 1024 )) /dev/urandom > "$partfile"
    : > "$WORK_DIR/lastmod-parts.txt"
    before="$(date -u +%s)"
    for i in 1 2; do
        etag="$(aws_s3 "$gate" s3api upload-part --bucket "$LM_BUCKET" --key "$key" \
            --upload-id "$upload_id" --part-number "$i" --body "$partfile" \
            --query ETag --output text)" || etag=""
        [ -n "$etag" ] || fail "last-modified: part $i failed to upload"
        printf '{"ETag":%s,"PartNumber":%d}\n' "$etag" "$i" >> "$WORK_DIR/lastmod-parts.txt"
    done
    after="$(date -u +%s)"

    part_raw="$(aws_s3 "$read_gate" s3api list-parts --bucket "$LM_BUCKET" --key "$key" \
        --upload-id "$upload_id" --query 'Parts[0].LastModified' --output text)"
    part_epoch="$(epoch_of "$part_raw")"
    lm_check "$(within "$part_epoch" "$before" "$after" && echo true || echo false)" \
        "ListParts reports when the part was uploaded: ${part_raw:-<none>}"

    # Held open on purpose: a completion dated from its first part rather than
    # from itself lands before this sleep ends and fails the check below.
    sleep 3
    printf '{"Parts":[%s]}' "$(paste -sd, "$WORK_DIR/lastmod-parts.txt")" \
        > "$WORK_DIR/lastmod-parts.json"
    complete_before="$(date -u +%s)"
    aws_s3 "$gate" s3api complete-multipart-upload --bucket "$LM_BUCKET" --key "$key" \
        --upload-id "$upload_id" \
        --multipart-upload "file://$WORK_DIR/lastmod-parts.json" >/dev/null \
        || fail "last-modified: complete-multipart-upload failed"

    object_epoch="$(epoch_of "$(lm_header HEAD "$read_gate" "$key")")"
    lm_check "$(within "$object_epoch" "$complete_before" "$(date -u +%s)" && echo true || echo false)" \
        "a completed multipart object is dated from its completion, not from its first part"

    list_epoch="$(epoch_of "$(aws_s3 "$read_gate" s3api list-objects-v2 --bucket "$LM_BUCKET" \
        --query "Contents[?Key=='$key'].LastModified" --output text)")"
    lm_check "$([ -n "$list_epoch" ] && [ "$list_epoch" = "$object_epoch" ] && echo true || echo false)" \
        "ListObjectsV2 and HEAD agree on the multipart object too"

    rm -f "$src" "$partfile"
    log "last-modified: stopping $LM_CLUSTER"
    "$SCRIPTS_DIR/stop.sh" -w "$LM_CLUSTER" >/dev/null 2>&1 || true
    mkdir -p "$RUN_DIR/logs-lastmod"
    cp -R "$PREDA_DIR/$LM_CLUSTER/logs/." "$RUN_DIR/logs-lastmod/" 2>/dev/null || true
    if [ "$LM_FAILURES" -eq 0 ]; then
        log "last-modified: passed $LM_CASES assertions"
    else
        log "last-modified: FAILED $LM_FAILURES of $LM_CASES assertions"
    fi
}

if [ "$SCENARIO" = all ] || [ "$SCENARIO" = last-modified ]; then
    run_last_modified

    if [ "$SCENARIO" = last-modified ]; then
        echo "Stress results: $RUN_DIR"
        [ "$LM_FAILURES" -eq 0 ] \
            || fail "last-modified failed $LM_FAILURES of $LM_CASES assertions"
        exit 0
    fi
fi

if [ "$SCENARIO" = all ] || [ "$SCENARIO" = large-object ]; then
    run_large_object

    if [ "$SCENARIO" = large-object ]; then
        echo "Stress results: $RUN_DIR"
        [ "$LARGE_FAILURES" -eq 0 ] \
            || fail "large-object failed $LARGE_FAILURES of $LARGE_CASES assertions"
        exit 0
    fi
fi

# --- Start ---

log "starting $CONFIG_NAME"
"$SCRIPTS_DIR/start.sh" -w "$CONFIG_NAME"

mapfile -t META_ALL < <(meta_nodes "$CONFIG_FILE" | awk '{print $2}')
[ "${#META_ALL[@]}" -gt 0 ] || fail "no meta node in $CONFIG_NAME"

log "baseline raft state"
meta_status "${META_ALL[@]}" | tee -a "$EVENTS"

# --- Scenario: partial-put ---
#
# The freeze scenario below injects a fault in the cluster. This one injects a
# fault in the client, which is the class that stranded PUTs for 51 minutes on
# the test-prod cluster and corrupted eleven volume documents doing it.
#
# A client that stops sending mid-body is not a client that aborts: the request
# stays open and the gate keeps waiting on bytes that never arrive. What makes
# it a data-loss bug rather than a leak is that the shards of the object being
# overwritten have already been replaced with the truncated ones, while the
# metadata record still describes the object that was there before.
if [ "$SCENARIO" = partial-put ]; then
    PARTIAL_PROBE="$WORK_DIR/partialput"
    go build -o "$PARTIAL_PROBE" "$REPO_DIR/scripts/bench/partialput"

    GATE="$(parse_hosts "$CONFIG_FILE" | awk '$3 != "" {print $2 ":" $3; exit}')"
    [ -n "$GATE" ] || fail "no gate in $CONFIG_NAME"
    ENDPOINT="https://$GATE"
    BUCKET="stress-partial-${RUN_ID}"
    KEY="victim.bin"
    V1="$WORK_DIR/v1.bin"
    GOT="$WORK_DIR/got.bin"

    HOLD="${STRESS_PARTIAL_HOLD:-180}"
    ABANDON="${STRESS_PARTIAL_ABANDON:-90}"
    DECLARE="${STRESS_PARTIAL_DECLARE:-4194304}"
    SEND="${STRESS_PARTIAL_SEND:-1048576}"

    # The large case exists because the gate buffers every data shard in memory
    # before it sends any of them, sized from the Content-Length the client
    # declared. 512MiB declared is 512MiB of gate heap for a body that never
    # arrives, which the 4MiB default cannot show.
    LARGE_DECLARE="${STRESS_PARTIAL_LARGE_DECLARE:-536870912}"
    LARGE_SEND="${STRESS_PARTIAL_LARGE_SEND:-134217728}"

    # Paced so the client is provably still transmitting when it is killed,
    # rather than already stalled: on loopback an unthrottled body of any size
    # is finished before the kill lands.
    KILL_RATE="${STRESS_PARTIAL_KILL_RATE:-4194304}"
    KILL_AFTER="${STRESS_PARTIAL_KILL_AFTER:-5}"

    log "partial-put scenario against $ENDPOINT (declare $DECLARE, send $SEND, hold ${HOLD}s)"

    openssl rand -out "$V1" 1048576
    aws_s3 "$ENDPOINT" s3 mb "s3://$BUCKET" >/dev/null
    aws_s3 "$ENDPOINT" s3api put-object --bucket "$BUCKET" --key "$KEY" --body "$V1" >/dev/null
    aws_s3 "$ENDPOINT" s3 cp "s3://$BUCKET/$KEY" "$GOT" --only-show-errors
    diff -q "$V1" "$GOT" >/dev/null || fail "baseline object did not round trip"
    log "baseline object stored and verified"

    GATE_LOGS="$PREDA_DIR/$CONFIG_NAME/logs"

    # gate_max_elapsed reports the longest the gate itself admits to still
    # running a request for this key. The gate's own tracker is the measure that
    # matters: a client's elapsed time also counts however long the client chose
    # to stall, which is not evidence about the server.
    #
    # No matching line is the best possible answer, not an error: the tracker
    # only starts logging once a request is already slow, so silence means the
    # gate finished with it promptly. Reported as 0.
    gate_max_elapsed() {
        local found
        found="$(grep -h "S3 request still running" "$GATE_LOGS"/*.log 2>/dev/null \
            | grep -F "$1" \
            | grep -oE '"elapsed_ms":[0-9]+' | cut -d: -f2 \
            | sort -n | tail -1)" || true
        echo "${found:-0}"
    }

    FAILURES=0

    # run_case: <name> <http2> <declare> <send> <rate> <kill-after-seconds, 0 to stall>
    #
    # Each case stores a fresh object under its own key and then overwrites that
    # key part-way. The overwrite has to target a key that already holds a
    # readable object, or there is nothing for the partial write to damage and
    # the assertions mean nothing.
    run_case() {
        local name="$1" http2_flag="$2" declare_len="$3" send_len="$4" rate="$5" kill_after="$6"
        local case_key="case-${name}.bin"
        local out="$RUN_DIR/partial-put-${name}.txt"

        aws_s3 "$ENDPOINT" s3api put-object --bucket "$BUCKET" --key "$case_key" --body "$V1" >/dev/null
        aws_s3 "$ENDPOINT" s3 cp "s3://$BUCKET/$case_key" "$GOT" --only-show-errors
        diff -q "$V1" "$GOT" >/dev/null || fail "$name: baseline object did not round trip"

        "$PARTIAL_PROBE" -endpoint "$ENDPOINT" -bucket "$BUCKET" -key "$case_key" \
            -declare "$declare_len" -send "$send_len" -hold "${HOLD}s" -region "$REGION" \
            -http2="$http2_flag" -rate "$rate" \
            -access-key "$ACCESS_KEY" -secret-key "$SECRET_KEY" > "$out" 2>&1 &
        local probe_pid="$!"

        if [ "$kill_after" -gt 0 ]; then
            # SIGKILL, because the production clients were killed rather than
            # asked to stop: qemu and nbdkit went away mid-upload and never got
            # to close anything. The kernel still closes the socket on process
            # death, so this is the closest reachable approximation without
            # dropping packets, and the gap between it and a truly vanished peer
            # is itself worth knowing.
            sleep "$kill_after"
            kill -KILL "$probe_pid" 2>/dev/null || true
            log "$name: killed the uploading client after ${kill_after}s"
        else
            # Long enough for the declared bytes to be sent and the gate to be
            # waiting on the rest, short enough to still be inside the deadline.
            sleep 10
        fi

        local during=pass
        if ! aws_s3 "$ENDPOINT" s3 cp "s3://$BUCKET/$case_key" "$GOT" --only-show-errors 2>>"$EVENTS"; then
            during="fail: GET errored"
        elif ! diff -q "$V1" "$GOT" >/dev/null 2>&1; then
            during="fail: GET returned different bytes"
        fi

        wait "$probe_pid" 2>/dev/null || true
        if [ -s "$out" ]; then
            log "$name probe: $(cat "$out")"
        else
            log "$name probe: killed before it reported"
        fi

        # The gate is given a moment past the bound to still be logging, so a
        # request abandoned exactly on it is not read as one that ran.
        sleep 5
        local gate_ms
        gate_ms="$(gate_max_elapsed "$case_key")"
        gate_ms="${gate_ms:-0}"

        local abandoned=pass
        if [ "$gate_ms" -ge $(( ABANDON * 1000 )) ]; then
            abandoned="fail: gate still ran the request at ${gate_ms}ms, past the ${ABANDON}s bound"
        fi

        local after=pass
        if ! aws_s3 "$ENDPOINT" s3 cp "s3://$BUCKET/$case_key" "$GOT" --only-show-errors 2>>"$EVENTS"; then
            after="fail: GET errored"
        elif ! diff -q "$V1" "$GOT" >/dev/null 2>&1; then
            after="fail: GET returned different bytes"
        fi

        log "$name: during=$during gate_max=${gate_ms}ms abandoned=$abandoned after=$after"
        local result
        for result in "$during" "$abandoned" "$after"; do
            [ "$result" = pass ] || FAILURES=$(( FAILURES + 1 ))
        done
        CASES_RUN=$(( CASES_RUN + 3 ))
    }

    CASES_RUN=0

    # Both protocols, because they need not fail the same way: a stalled h2
    # stream is flow-control state on a shared connection, an HTTP/1.1 stall is
    # a socket the server owns outright.
    run_case http1-stall false "$DECLARE" "$SEND" 0 0
    run_case http2-stall true  "$DECLARE" "$SEND" 0 0

    # A body the size of the production writes that stranded, which were volume
    # chunks rather than the 4MiB this defaults to. The gate buffers every data
    # shard in memory before sending any, sized from the declared length, so
    # size is the axis the small cases cannot vary.
    run_case http1-large false "$LARGE_DECLARE" "$LARGE_SEND" 0 0

    # The production shape: a client killed mid-upload rather than one that
    # politely stops sending. Both protocols, because what the peer's death does
    # to an h2 connection carrying other streams is not what it does to a
    # dedicated HTTP/1.1 socket.
    run_case http1-kill false "$LARGE_DECLARE" "$LARGE_SEND" "$KILL_RATE" "$KILL_AFTER"
    run_case http2-kill true  "$LARGE_DECLARE" "$LARGE_SEND" "$KILL_RATE" "$KILL_AFTER"

    # --- Many stalled uploads at once ---
    #
    # The last difference from production, which had 74 instances' worth of
    # traffic in flight rather than one probe against an idle cluster.
    #
    # It also puts a number on the gate's shard buffers. writeObject reserves
    # declared/DataShards bytes per data shard before it reads a byte, so N
    # stalled uploads reserve N x declared bytes. Reserved is not resident, which
    # is why both are recorded. Neither is asserted: what the right ceiling is
    # has not been decided, and a threshold now would only be a guess.
    CONCURRENCY="${STRESS_PARTIAL_CONCURRENCY:-8}"
    GATE_PID="$(cat "$PREDA_DIR/$CONFIG_NAME/pids/host-1.pid")"

    gate_rss_mb() {
        awk '/^VmRSS:/ {print int($2/1024)}' "/proc/$GATE_PID/status" 2>/dev/null || echo 0
    }

    # Reserved separately from resident, because the shard buffers are allocated
    # by capacity and untouched pages never become resident. A body that is
    # declared but not sent moves this and not RSS, and the difference is the
    # whole question.
    gate_vsz_mb() {
        awk '/^VmSize:/ {print int($2/1024)}' "/proc/$GATE_PID/status" 2>/dev/null || echo 0
    }

    CONC_KEYS=()
    for i in $(seq 1 "$CONCURRENCY"); do
        conc_key="case-concurrent-${i}.bin"
        CONC_KEYS+=("$conc_key")
        aws_s3 "$ENDPOINT" s3api put-object --bucket "$BUCKET" --key "$conc_key" --body "$V1" >/dev/null
    done

    RSS_BEFORE="$(gate_rss_mb)"
    VSZ_BEFORE="$(gate_vsz_mb)"
    CONC_PIDS=()
    for conc_key in "${CONC_KEYS[@]}"; do
        "$PARTIAL_PROBE" -endpoint "$ENDPOINT" -bucket "$BUCKET" -key "$conc_key" \
            -declare "$LARGE_DECLARE" -send 1048576 -hold "${HOLD}s" -region "$REGION" \
            -access-key "$ACCESS_KEY" -secret-key "$SECRET_KEY" \
            > "$RUN_DIR/partial-put-concurrent-${#CONC_PIDS[@]}.txt" 2>&1 &
        CONC_PIDS+=("$!")
    done
    log "concurrent: launched $CONCURRENCY stalled uploads declaring $LARGE_DECLARE each"

    # Sampled across the deadline rather than read once, because the buffers are
    # released when the handler returns and a single late read would miss them.
    RSS_PEAK="$RSS_BEFORE"
    VSZ_PEAK="$VSZ_BEFORE"
    for _ in $(seq 1 30); do
        sleep 2
        rss="$(gate_rss_mb)"
        if [ "$rss" -gt "$RSS_PEAK" ]; then
            RSS_PEAK="$rss"
        fi
        vsz="$(gate_vsz_mb)"
        if [ "$vsz" -gt "$VSZ_PEAK" ]; then
            VSZ_PEAK="$vsz"
        fi
    done

    CONC_DURING=pass
    if ! aws_s3 "$ENDPOINT" s3 cp "s3://$BUCKET/${CONC_KEYS[0]}" "$GOT" --only-show-errors 2>>"$EVENTS"; then
        CONC_DURING="fail: GET errored"
    elif ! diff -q "$V1" "$GOT" >/dev/null 2>&1; then
        CONC_DURING="fail: GET returned different bytes"
    fi

    for pid in "${CONC_PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    sleep 5

    CONC_GATE_MS="$(gate_max_elapsed "case-concurrent-")"
    CONC_ABANDONED=pass
    if [ "$CONC_GATE_MS" -ge $(( ABANDON * 1000 )) ]; then
        CONC_ABANDONED="fail: gate still ran a request at ${CONC_GATE_MS}ms, past the ${ABANDON}s bound"
    fi

    # Every key, not a sample: a partial write that damaged one object out of
    # eight is exactly the production shape, and checking one would miss it.
    CONC_AFTER=pass
    for conc_key in "${CONC_KEYS[@]}"; do
        if ! aws_s3 "$ENDPOINT" s3 cp "s3://$BUCKET/$conc_key" "$GOT" --only-show-errors 2>>"$EVENTS"; then
            CONC_AFTER="fail: GET errored for $conc_key"
            break
        elif ! diff -q "$V1" "$GOT" >/dev/null 2>&1; then
            CONC_AFTER="fail: $conc_key returned different bytes"
            break
        fi
    done

    log "concurrent: during=$CONC_DURING gate_max=${CONC_GATE_MS}ms abandoned=$CONC_ABANDONED after=$CONC_AFTER"
    log "concurrent: gate rss ${RSS_BEFORE}->${RSS_PEAK}MB, vsz ${VSZ_BEFORE}->${VSZ_PEAK}MB across $CONCURRENCY stalled uploads"
    for result in "$CONC_DURING" "$CONC_ABANDONED" "$CONC_AFTER"; do
        [ "$result" = pass ] || FAILURES=$(( FAILURES + 1 ))
    done
    CASES_RUN=$(( CASES_RUN + 3 ))

    aws_s3 "$ENDPOINT" s3 rb "s3://$BUCKET" --force >/dev/null 2>&1 || true

    echo "Stress results: $RUN_DIR"
    [ "$FAILURES" -eq 0 ] || fail "partial-put scenario failed $FAILURES of $CASES_RUN assertions"
    log "partial-put scenario passed"
    exit 0
fi

# A state document rather than random bytes, because the objects this destroyed
# in production were volume state: every record carries the generation that
# wrote it, so a mixture is legible in the file itself rather than only as a
# checksum that no longer matches. Both generations are byte-for-byte the same
# length, which is what an in-place rewrite of a state document looks like and
# what keeps the stored size honest.
make_state() {
    awk -v gen="$1" -v n="$3" 'BEGIN {
        printf "{\n  \"v\": 1,\n  \"generation\": \"%s\",\n", gen
        for (i = 1; i <= n; i++)
            printf "  \"extent_%06d\": \"%s-%06d-0123456789abcdef0123456789abcdef\",\n", i, gen, i
        printf "  \"trailer\": \"%s\"\n}\n", gen
    }' > "$2"
}


# --- Scenario: concurrent-put ---
#
# Publishing an object is two independent writes, and nothing orders them
# against a second writer of the same key. The placement record naming the
# write epoch goes to the meta plane as a plain last-write-wins Put, and the
# shards are published separately by commitShards. So whoever writes the record
# last owns the record, whoever prepares last owns the shards, and those are
# two different races.
#
# When they disagree the object is unreadable and stays that way. A shard node
# keeps one prepared slot per position, so a second writer's prepare overwrites
# the first's; the first writer's commit is then refused as not-prepared and
# that refusal is logged and discarded. If that writer's record landed last,
# the object's record names an epoch no shard holds and every read fails on the
# epoch. Repair cannot mend it either, because repair rebuilds a shard at the
# epoch the record names and no node has one.
#
# Both writers are told 200.
#
# Nothing here injects a fault. The cluster is whole, every node is healthy,
# and the only ingredient is two clients writing the same key at the same time
# — which is why it is asserted on the ordinary path rather than behind a
# freeze. Writers are spread across all four gates, because two requests
# arriving at one gate could serialise somewhere and prove less than they look
# like proving.
#
# The assertion is deliberately weak about *which* value wins: concurrent
# writers to one key have no defined winner and picking one would be inventing
# a guarantee. It holds only that the object reads back, and reads back as
# exactly one of the bodies whose PUT was acknowledged. A writer told its write
# was superseded is not owed the object.
CPUT_FAILURES=0
CPUT_CASES=0

# cput_generation_of names which body came back, or reports a mixture. Separate
# from generation_of because that one is built for the two-generation torn
# tests and this scenario has one body per writer.
cput_generation_of() {
    local got="$1" writers="$2" i
    for i in $(seq 1 "$writers"); do
        if cmp -s "$WORK_DIR/cput-bodies/g${i}.json" "$got"; then
            echo "g${i}"
            return
        fi
    done
    echo "spliced($(grep -oE '"g[0-9]+-' "$got" | sort -u | tr -d '"-' | paste -sd,))"
}

run_concurrent_put() {
    BUCKET="stress-cput-${RUN_ID}"
    local writers="${STRESS_CPUT_WRITERS:-6}"
    local keys_n="${STRESS_CPUT_KEYS:-8}"
    local rounds="${STRESS_CPUT_ROUNDS:-3}"
    local lines="${STRESS_CPUT_LINES:-2048}"

    CPUT_FAILURES=0
    CPUT_CASES=0

    local first_gate gates=() endpoint_list
    mapfile -t gates < <(gate_endpoints "$CONFIG_FILE" | sed 's#^#https://#')
    [ "${#gates[@]}" -gt 0 ] || fail "concurrent-put: no gate in $CONFIG_NAME"
    first_gate="${gates[0]}"
    endpoint_list="$(printf '%s\n' "${gates[@]}" | paste -sd,)"

    # The AWS CLI cannot race anything: each invocation spends the better part
    # of a second starting Python, which staggers writers by far more than the
    # window under test. racedput signs every request up front and releases them
    # on a barrier, so they are inside the gate together.
    local RACE_PROBE="$WORK_DIR/racedput"
    go build -o "$RACE_PROBE" "$REPO_DIR/scripts/bench/racedput"

    # Named for the writer, because racedput takes each writer's name from its
    # body file and the readback matches what came back against that name.
    mkdir -p "$WORK_DIR/cput-bodies"

    local i body_list=()
    for i in $(seq 1 "$writers"); do
        make_state "g${i}" "$WORK_DIR/cput-bodies/g${i}.json" "$lines"
        body_list+=("$WORK_DIR/cput-bodies/g${i}.json")
    done
    local bodies
    bodies="$(printf '%s\n' "${body_list[@]}" | paste -sd,)"
    log "concurrent-put: $writers bodies of $(wc -c < "$WORK_DIR/cput-bodies/g1.json") bytes, one per writer"

    aws_s3 "$first_gate" s3 mb "s3://$BUCKET" >/dev/null

    local outdir="$WORK_DIR/cput-out"
    rm -rf "$outdir"
    mkdir -p "$outdir"

    local round key k pids=() p
    for round in $(seq 1 "$rounds"); do
        pids=()
        for k in $(seq 1 "$keys_n"); do
            key="$(printf 'cput-r%02d-k%02d.json' "$round" "$k")"
            (
                "$RACE_PROBE" -endpoints "$endpoint_list" -bucket "$BUCKET" -key "$key" \
                    -bodies "$bodies" -region "$REGION" \
                    -access-key "$ACCESS_KEY" -secret-key "$SECRET_KEY" \
                    > "$outdir/${key}.race" 2>>"$outdir/put-errors.txt"
            ) &
            pids+=("$!")
        done
        for p in "${pids[@]}"; do wait "$p" || true; done
        log "concurrent-put: round $round wrote $keys_n keys with $writers simultaneous writers each"
    done

    # What each writer was told, tallied. A key can read back correctly while
    # most of its writers were refused, so the verdicts are a separate finding
    # from the readback and are worth stating even on a pass.
    local verdicts
    verdicts="$(awk '/^writer=/ {
        for (i = 1; i <= NF; i++) if ($i ~ /^status=/) { sub(/^status=/, "", $i); n[$i]++ }
        if ($0 ~ /outcome=client_error/) n["client_error"]++
    } END { for (s in n) printf "%s=%d ", s, n[s]; printf "\n" }' "$outdir"/*.race)"
    log "concurrent-put: writer verdicts $verdicts"

    # Read back through a gate that wrote nothing in the last round, so a
    # cached anything on the writing gate cannot answer for the cluster.
    local read_gate="${gates[${#gates[@]} - 1]}"
    local got="$WORK_DIR/cput-got.json" acked seen bad_keys=()
    for round in $(seq 1 "$rounds"); do
        for k in $(seq 1 "$keys_n"); do
            key="$(printf 'cput-r%02d-k%02d.json' "$round" "$k")"
            CPUT_CASES=$(( CPUT_CASES + 1 ))

            # Only a 2xx is a promise. A writer told its epoch was superseded is
            # not owed the object, so its body is not an acceptable answer.
            acked=" $(awk '/^writer=/ && /status=2[0-9][0-9]/ {
                sub(/^writer=/, "", $1); printf "%s ", $1 }' "$outdir/${key}.race")"
            acked="${acked% }"
            if [ -z "${acked// /}" ]; then
                log "concurrent-put: FAIL $key had no acknowledged writer, so nothing published it"
                CPUT_FAILURES=$(( CPUT_FAILURES + 1 ))
                bad_keys+=("$key")
                continue
            fi

            if ! aws_s3 "$read_gate" --cli-connect-timeout 10 --cli-read-timeout 120 \
                s3api get-object --bucket "$BUCKET" --key "$key" "$got" \
                >/dev/null 2>>"$outdir/get-errors.txt"; then
                log "concurrent-put: FAIL $key is unreadable after$acked were acknowledged"
                CPUT_FAILURES=$(( CPUT_FAILURES + 1 ))
                bad_keys+=("$key")
                continue
            fi

            seen="$(cput_generation_of "$got" "$writers")"
            case " $acked " in
                *" $seen "*) ;;
                *)
                    log "concurrent-put: FAIL $key reads as $seen, which no acknowledged writer sent ($acked)"
                    cp "$got" "$RUN_DIR/concurrent-put-${key}"
                    CPUT_FAILURES=$(( CPUT_FAILURES + 1 ))
                    bad_keys+=("$key")
                    ;;
            esac
        done
    done

    # Whether it is permanent is the difference between a defect and a delay,
    # and repair runs by default, so a key still broken after a sweep interval
    # is one nothing in the system will mend.
    if [ "${#bad_keys[@]}" -gt 0 ]; then
        log "concurrent-put: ${#bad_keys[@]} keys broken; waiting one repair interval to see whether anything mends them"
        sleep "${STRESS_CPUT_RECHECK_S:-45}"
        local recovered=0
        for key in "${bad_keys[@]}"; do
            if aws_s3 "$read_gate" --cli-connect-timeout 10 --cli-read-timeout 120 \
                s3api get-object --bucket "$BUCKET" --key "$key" "$got" >/dev/null 2>&1; then
                recovered=$(( recovered + 1 ))
            fi
        done
        log "concurrent-put: $recovered of ${#bad_keys[@]} broken keys became readable again"
        grep -oE 'An error occurred \([A-Za-z]+\)' "$outdir/get-errors.txt" 2>/dev/null \
            | sort | uniq -c | while read -r n code; do
                log "concurrent-put: read error $code x$n"
            done
    fi

    # The work dir goes with the run, and these are the only record of what each
    # writer was told, which is where a failing key's explanation lives.
    mkdir -p "$RUN_DIR/concurrent-put"
    cp "$outdir"/*.race "$outdir"/*errors.txt "$RUN_DIR/concurrent-put/" 2>/dev/null || true

    aws_s3 "$first_gate" s3 rb "s3://$BUCKET" --force >/dev/null 2>&1 || true

    if [ "$CPUT_FAILURES" -eq 0 ]; then
        log "concurrent-put: passed $CPUT_CASES assertions"
    else
        log "concurrent-put: FAILED $CPUT_FAILURES of $CPUT_CASES assertions"
    fi
}

if [ "$SCENARIO" = all ] || [ "$SCENARIO" = concurrent-put ]; then
    run_concurrent_put

    if [ "$SCENARIO" = concurrent-put ]; then
        echo "Stress results: $RUN_DIR"
        [ "$CPUT_FAILURES" -eq 0 ] \
            || fail "concurrent-put failed $CPUT_FAILURES of $CPUT_CASES assertions"
        exit 0
    fi

    log "round trip after concurrent-put, before the torn-overwrite scenario"
    round_trip "https://$(gate_endpoints "$CONFIG_FILE" | head -1)" post-cput \
        || fail "the cluster did not take writes after the concurrent-put scenario"
fi

# --- Scenario: torn-overwrite ---
#
# An overwrite is not atomic across an object's shards. writeObject sends every
# shard concurrently and each blob node commits its own the moment that shard
# lands, so a fault that reaches one shard node and not the others leaves the
# object holding some shards from the new write and the rest from the old one.
# The PUT fails and the metadata record is never updated, so nothing anywhere
# records that the object is now a mixture — the next GET reads it back as if
# it were whole.
#
# SIGSTOP on one host is the fault, for the same reason the freeze scenario
# uses it: the node stays dialable and answers nothing, which is what a stalled
# peer looks like to the write path. One host is enough and two would prove
# less, because stopping two of four takes raft's quorum with it and the
# failure would then be ordinary loss of quorum rather than this.
#
# Which host to stop is not a guess. Placement follows the object hash, which
# is derived from bucket and key alone, so shardplace resolves the exact host
# holding a named shard of a named key before the object is written.
#
# This runs on every invocation rather than only when asked for. It is silent
# data loss on the ordinary overwrite path, so a run that skipped it would be
# reporting on a narrower cluster than the one being shipped. It costs about
# three minutes and needs no load, and its failures are recorded rather than
# fatal so the freeze test below still runs while this one is red.
TORN_FAILURES=0
TORN_CASES=0

# generation_of classifies what came back against the $V1 and $V2 the calling
# scenario built. Neither generation intact is the finding: a spliced object is
# one the reader cannot detect and the writer never knew it made.
generation_of() {
    local got="$1"
    if cmp -s "$V1" "$got"; then
        echo v1
    elif cmp -s "$V2" "$got"; then
        echo v2
    else
        echo "spliced(v1_records=$(grep -c '"v1-' "$got" || true) v2_records=$(grep -c '"v2-' "$got" || true))"
    fi
}

run_torn_overwrite() {
    BUCKET="stress-torn-${RUN_ID}"
    LINES="${STRESS_TORN_LINES:-16384}"
    V1="$WORK_DIR/state-v1.json"
    V2="$WORK_DIR/state-v2.json"

    make_state v1 "$V1" "$LINES"
    make_state v2 "$V2" "$LINES"
    [ "$(wc -c < "$V1")" -eq "$(wc -c < "$V2")" ] \
        || fail "the two generations differ in length, which is not the overwrite under test"
    log "torn-overwrite scenario: state document is $(wc -c < "$V1") bytes over $LINES records"

    FIRST_GATE="https://$(gate_endpoints "$CONFIG_FILE" | head -1)"
    aws_s3 "$FIRST_GATE" s3 mb "s3://$BUCKET" >/dev/null

    # No case overwrites this one, so a gate that cannot serve it is cold rather
    # than holding a torn object. Warming against a key under test would confuse
    # the two, which is the whole thing the assertions here distinguish.
    TORN_SENTINEL="sentinel.json"
    aws_s3 "$FIRST_GATE" s3api put-object \
        --bucket "$BUCKET" --key "$TORN_SENTINEL" --body "$V1" >/dev/null

    TORN_FAILURES=0
    TORN_CASES=0

    # freeze_and_overwrite stores v1, stops the host holding one named shard of
    # that key, overwrites with v2, and thaws. The PUT is expected to fail: one
    # shard node is unreachable and the write path fails on any shard error.
    # What the run is here to establish is the state it leaves behind.
    freeze_and_overwrite() {
        local name="$1" role="$2"
        local key="state-${name}.json"
        local got="$WORK_DIR/got-${name}.json"
        local host gate pid rc

        host="$(shard_host "$CONFIG_FILE" "$BUCKET" "$key" "$role")"
        [ -n "$host" ] || fail "$name: shardplace named no $role shard host for $key"
        gate="$(survivor_gate "$CONFIG_FILE" "$host")"
        [ -n "$gate" ] || fail "$name: no gate survives stopping host $host"
        pid="$(cat "$PID_DIR/host-${host}.pid")"
        kill -0 "$pid" 2>/dev/null || fail "$name: host $host is not running"

        log "$name: $role shard of $key is on host $host (pid $pid); writing through $gate"
        "$SHARD_PROBE" -config "$CONFIG_FILE" -bucket "$BUCKET" -key "$key" | tee -a "$EVENTS"

        aws_s3 "$gate" s3api put-object --bucket "$BUCKET" --key "$key" --body "$V1" >/dev/null
        aws_s3 "$gate" s3 cp "s3://$BUCKET/$key" "$got" --only-show-errors
        cmp -s "$V1" "$got" || fail "$name: baseline object did not round trip"
        log "$name: v1 stored and verified"

        kill -STOP "$pid"
        log "$name: SIGSTOP host $host, overwriting with v2"
        rc=0
        aws_s3 "$gate" --cli-connect-timeout 10 --cli-read-timeout 180 \
            s3api put-object --bucket "$BUCKET" --key "$key" --body "$V2" >/dev/null 2>>"$EVENTS" || rc=$?
        kill -CONT "$pid"
        log "$name: SIGCONT host $host, overwriting PUT exited $rc"

        if [ "$rc" -eq 0 ]; then
            log "$name: FAIL the overwrite reported success with a shard node stopped"
            TORN_FAILURES=$(( TORN_FAILURES + 1 ))
        fi
        TORN_CASES=$(( TORN_CASES + 1 ))

        # The thawed host's own gate is warmed, not merely waited on. It holds
        # connections that died under the SIGSTOP, and the next case can pick it
        # as the gate it writes through, where the eviction delay reads as a
        # cluster fault rather than the recovery it is.
        local thawed_gate
        thawed_gate="$(gate_of "$CONFIG_FILE" "$host")"
        if [ -n "$thawed_gate" ]; then
            warm_gate "$thawed_gate" "$BUCKET" "$TORN_SENTINEL" "$name" \
                || fail "$name: the thawed host's gate never served again"
        else
            sleep 10
        fi

        local seen
        aws_s3 "$gate" s3 cp "s3://$BUCKET/$key" "$got" --only-show-errors 2>>"$EVENTS" || true
        seen="$(generation_of "$got")"
        cp "$got" "$RUN_DIR/torn-overwrite-${name}.json"

        # The assertion, and it does not depend on knowing the mechanism: the
        # overwrite failed, so the object still has to be exactly what it was.
        if [ "$seen" = v1 ]; then
            log "$name: pass, the failed overwrite left v1 intact"
        else
            log "$name: FAIL a failed overwrite left the object as $seen"
            TORN_FAILURES=$(( TORN_FAILURES + 1 ))
        fi
        TORN_CASES=$(( TORN_CASES + 1 ))
        TORN_KEY="$key"
    }

    # A data shard, where the mixture is served straight back: the reader takes
    # the data shards in order and joins them, so an object whose shards
    # disagree reads as one generation's bytes followed by another's.
    freeze_and_overwrite data-shard data

    # The parity shard, which is the quieter half of the same defect. Both data
    # shards take the new write and only parity is left behind, so an ordinary
    # GET returns the object the failed PUT was not supposed to store, and the
    # parity that is meant to rebuild it belongs to the generation before.
    freeze_and_overwrite parity-shard parity
    PARITY_KEY="$TORN_KEY"

    # What that stale parity is worth, which a healthy read never asks. One
    # data shard's host is stopped so the read has to reconstruct, and the
    # parity it reconstructs from is the older generation's.
    RECON_HOST="$(shard_host "$CONFIG_FILE" "$BUCKET" "$PARITY_KEY" data)"
    RECON_GATE="$(survivor_gate "$CONFIG_FILE" "$RECON_HOST")"
    RECON_PID="$(cat "$PID_DIR/host-${RECON_HOST}.pid")"
    RECON_GOT="$WORK_DIR/got-reconstructed.json"

    warm_gate "$RECON_GATE" "$BUCKET" "$PARITY_KEY" reconstruction \
        || fail "the read gate never recovered from its own freeze, so reconstruction is untestable"

    log "reconstruction: stopping host $RECON_HOST to force $PARITY_KEY to rebuild from parity"
    kill -STOP "$RECON_PID"
    RECON_RC=0
    aws_s3 "$RECON_GATE" --cli-connect-timeout 10 --cli-read-timeout 120 \
        s3 cp "s3://$BUCKET/$PARITY_KEY" "$RECON_GOT" --only-show-errors 2>>"$EVENTS" || RECON_RC=$?
    kill -CONT "$RECON_PID"

    if [ "$RECON_RC" -ne 0 ]; then
        log "reconstruction: GET failed with $RECON_RC, so the object is unreadable one node down"
        TORN_FAILURES=$(( TORN_FAILURES + 1 ))
    else
        cp "$RECON_GOT" "$RUN_DIR/torn-overwrite-reconstructed.json"
        RECON_SEEN="$(generation_of "$RECON_GOT")"
        if [ "$RECON_SEEN" = v1 ]; then
            log "reconstruction: pass, rebuilt v1 from parity"
        else
            log "reconstruction: FAIL rebuilt $RECON_SEEN from parity"
            TORN_FAILURES=$(( TORN_FAILURES + 1 ))
        fi
    fi
    TORN_CASES=$(( TORN_CASES + 1 ))

    # Same reason as each case above: the freeze scenario runs next and may
    # write through this host.
    RECON_THAWED="$(gate_of "$CONFIG_FILE" "$RECON_HOST")"
    if [ -n "$RECON_THAWED" ]; then
        warm_gate "$RECON_THAWED" "$BUCKET" "$TORN_SENTINEL" reconstruction \
            || fail "reconstruction: the thawed host's gate never served again"
    else
        sleep 10
    fi

    log "raft state after the scenario"
    meta_status "${META_ALL[@]}" | tee -a "$EVENTS"

    aws_s3 "$FIRST_GATE" s3 rb "s3://$BUCKET" --force >/dev/null 2>&1 || true

    if [ "$TORN_FAILURES" -eq 0 ]; then
        log "torn-overwrite: passed $TORN_CASES assertions"
    else
        log "torn-overwrite: FAILED $TORN_FAILURES of $TORN_CASES assertions"
    fi
}

if [ "$SCENARIO" = all ] || [ "$SCENARIO" = torn-overwrite ]; then
    run_torn_overwrite

    # Asked for on its own, the scenario is the whole run and its result is the
    # exit status. Otherwise it is one part of a longer run and the verdict
    # waits until the end, so a red torn-overwrite does not cost the freeze
    # coverage.
    if [ "$SCENARIO" = torn-overwrite ]; then
        echo "Stress results: $RUN_DIR"
        [ "$TORN_FAILURES" -eq 0 ] \
            || fail "torn-overwrite failed $TORN_FAILURES of $TORN_CASES assertions"
        exit 0
    fi

    # The freeze test needs every shard node, and the scenario above stopped
    # two hosts and continued them. Proving the cluster is whole again here
    # keeps a leftover from it out of the freeze test's own assertions.
    log "round trip after torn-overwrite, before the freeze test"
    round_trip "https://$(gate_endpoints "$CONFIG_FILE" | head -1)" post-torn \
        || fail "the cluster did not take writes after the torn-overwrite scenario"
fi

# --- Scenario: stale-shard ---
#
# torn-overwrite asks the question on two objects and one overwrite each. This
# asks it at width and after the fault has cleared: one host is frozen for a
# whole batch of concurrent overwrites, thawed, and then every object is read
# back. A generation that survived on two objects and not on the other ten is
# the shape a two-object test misses.
#
# The assertion is not "everything is v1". It is that every object reads back
# as exactly the generation its own PUT reported, which is what makes this
# scenario outlive the phase it was written in: today a write with a shard node
# down fails and the answer is v1 for all of them, and once degraded writes
# land the answer becomes v2 for the ones that were accepted. Either way a
# spliced object, or a v2 the client was told had failed, is a failure.
#
# It is read four times over. Once with the cluster whole, which takes the data
# shards straight, and then once with each of the three other hosts stopped in
# turn. With three shards spread over three of four hosts, stopping any single
# peer of the thawed host forces its own shard to be read and the missing one
# rebuilt from parity — so the sweep is what reaches the shards a healthy read
# never touches, and a stale one hiding in parity is only ever found there.
STALE_FAILURES=0
STALE_CASES=0

run_stale_shard() {
    BUCKET="stress-stale-${RUN_ID}"
    LINES="${STRESS_STALE_LINES:-2048}"
    V1="$WORK_DIR/stale-v1.json"
    V2="$WORK_DIR/stale-v2.json"
    GOT="$WORK_DIR/stale-got.json"

    make_state v1 "$V1" "$LINES"
    make_state v2 "$V2" "$LINES"
    [ "$(wc -c < "$V1")" -eq "$(wc -c < "$V2")" ] \
        || fail "the two generations differ in length, which is not the overwrite under test"

    STALE_FAILURES=0
    STALE_CASES=0

    local keys=() key i
    for i in $(seq 1 "$STALE_KEYS"); do
        keys+=("$(printf 'state-stale-%03d.json' "$i")")
    done

    # expect_dir records what each PUT reported, one file per key, because the
    # overwrites run concurrently and a subshell cannot write back into an array.
    local expect_dir="$WORK_DIR/stale-expect"
    mkdir -p "$expect_dir"

    local first_gate
    first_gate="https://$(gate_endpoints "$CONFIG_FILE" | head -1)"
    aws_s3 "$first_gate" s3 mb "s3://$BUCKET" >/dev/null

    # The host to freeze is any host, but how much of the corpus it actually
    # holds is counted rather than assumed: freezing a host that carries no
    # shard of any key would inject no fault and still report a pass.
    local frozen on_frozen=0
    frozen="$(parse_hosts "$CONFIG_FILE" | awk 'NR == 1 { print $1 }')"
    for key in "${keys[@]}"; do
        if "$SHARD_PROBE" -config "$CONFIG_FILE" -bucket "$BUCKET" -key "$key" \
            | awk -v h="host=$frozen" '$4 == h { found = 1 } END { exit !found }'; then
            on_frozen=$(( on_frozen + 1 ))
        fi
    done
    [ "$on_frozen" -gt 0 ] \
        || fail "no key in the corpus places a shard on host $frozen, so freezing it proves nothing"
    log "stale-shard: $on_frozen of $STALE_KEYS keys hold a shard on host $frozen"

    for key in "${keys[@]}"; do
        aws_s3 "$first_gate" s3api put-object --bucket "$BUCKET" --key "$key" --body "$V1" >/dev/null
        echo v1 > "$expect_dir/$key"
    done
    log "stale-shard: $STALE_KEYS objects of $(wc -c < "$V1") bytes stored as v1"

    local gate pid p pids=()
    gate="$(survivor_gate "$CONFIG_FILE" "$frozen")"
    [ -n "$gate" ] || fail "no gate survives stopping host $frozen"
    pid="$(cat "$PID_DIR/host-${frozen}.pid")"
    kill -0 "$pid" 2>/dev/null || fail "host $frozen is not running"

    # Concurrently, so the freeze window is one write's worth of timeout rather
    # than one per key. It is also the more honest shape: a host does not go
    # away between requests, it goes away during all of them.
    kill -STOP "$pid"
    log "stale-shard: SIGSTOP host $frozen, overwriting all $STALE_KEYS objects with v2 through $gate"
    for key in "${keys[@]}"; do
        (
            if aws_s3 "$gate" --cli-connect-timeout 10 --cli-read-timeout 180 \
                s3api put-object --bucket "$BUCKET" --key "$key" --body "$V2" \
                >/dev/null 2>>"$RUN_DIR/stale-shard-puts.txt"; then
                echo v2 > "$expect_dir/$key"
            fi
        ) &
        pids+=("$!")
    done
    for p in "${pids[@]}"; do wait "$p" || true; done
    kill -CONT "$pid"

    local accepted=0
    for key in "${keys[@]}"; do
        if [ "$(cat "$expect_dir/$key")" = v2 ]; then accepted=$(( accepted + 1 )); fi
    done
    log "stale-shard: SIGCONT host $frozen; $accepted of $STALE_KEYS overwrites were accepted"

    # Long enough for the thawed host to answer again, so the reads below are
    # of the cluster's settled state rather than racing the thaw.
    sleep 15

    # check_all reads every key through one gate and holds each to the
    # generation its own PUT reported.
    check_all() {
        local read_gate="$1" label="$2" seen bad=0
        for key in "${keys[@]}"; do
            if ! aws_s3 "$read_gate" --cli-connect-timeout 10 --cli-read-timeout 120 \
                s3 cp "s3://$BUCKET/$key" "$GOT" --only-show-errors 2>>"$EVENTS"; then
                log "stale-shard: $label FAIL GET errored for $key"
                bad=$(( bad + 1 ))
                continue
            fi
            seen="$(generation_of "$GOT")"
            if [ "$seen" != "$(cat "$expect_dir/$key")" ]; then
                log "stale-shard: $label FAIL $key is $seen, but its PUT reported $(cat "$expect_dir/$key")"
                cp "$GOT" "$RUN_DIR/stale-shard-${label}-${key}"
                bad=$(( bad + 1 ))
            fi
        done
        STALE_CASES=$(( STALE_CASES + ${#keys[@]} ))
        if [ "$bad" -eq 0 ]; then
            log "stale-shard: $label pass, all ${#keys[@]} objects match what their PUT reported"
        else
            STALE_FAILURES=$(( STALE_FAILURES + bad ))
        fi
    }

    warm_gate "$first_gate" "$BUCKET" "${keys[0]}" stale-shard \
        || fail "no gate served after the thaw, so nothing below can be attributed"
    check_all "$first_gate" whole

    # Each of the thawed host's peers in turn, which is what forces its own
    # shards to be read and the missing one rebuilt from parity.
    local other opid ogate
    for other in $(parse_hosts "$CONFIG_FILE" | awk -v h="$frozen" '$1 != h { print $1 }'); do
        opid="$(cat "$PID_DIR/host-${other}.pid")"
        ogate="$(survivor_gate "$CONFIG_FILE" "$other")"
        warm_gate "$ogate" "$BUCKET" "${keys[0]}" "host-${other}-down" \
            || fail "the read gate did not serve before host $other was stopped"
        log "stale-shard: stopping host $other so every object rebuilds, reading through $ogate"
        kill -STOP "$opid"
        check_all "$ogate" "host-${other}-down"
        kill -CONT "$opid"
        sleep 10
    done

    aws_s3 "$first_gate" s3 rb "s3://$BUCKET" --force >/dev/null 2>&1 || true

    if [ "$STALE_FAILURES" -eq 0 ]; then
        log "stale-shard: passed $STALE_CASES assertions"
    else
        log "stale-shard: FAILED $STALE_FAILURES of $STALE_CASES assertions"
    fi
}

if [ "$SCENARIO" = all ] || [ "$SCENARIO" = stale-shard ]; then
    run_stale_shard

    if [ "$SCENARIO" = stale-shard ]; then
        echo "Stress results: $RUN_DIR"
        [ "$STALE_FAILURES" -eq 0 ] \
            || fail "stale-shard failed $STALE_FAILURES of $STALE_CASES assertions"
        exit 0
    fi

    log "round trip after stale-shard, before the freeze test"
    round_trip "https://$(gate_endpoints "$CONFIG_FILE" | head -1)" post-stale \
        || fail "the cluster did not take writes after the stale-shard scenario"
fi

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
    if [ "$REPAIR_CASES" -eq 0 ]; then
        echo "repair=skipped"
    elif [ "$REPAIR_FAILURES" -eq 0 ]; then
        echo "repair=pass"
    else
        echo "repair=fail ($REPAIR_FAILURES of $REPAIR_CASES)"
    fi
    if [ "$HANDOFF_CASES" -eq 0 ]; then
        echo "handoff=skipped"
    elif [ "$HANDOFF_FAILURES" -eq 0 ]; then
        echo "handoff=pass"
    else
        echo "handoff=fail ($HANDOFF_FAILURES of $HANDOFF_CASES)"
    fi
    if [ "$TORN_CASES" -eq 0 ]; then
        echo "torn_overwrite=skipped"
    elif [ "$TORN_FAILURES" -eq 0 ]; then
        echo "torn_overwrite=pass"
    else
        echo "torn_overwrite=fail ($TORN_FAILURES of $TORN_CASES)"
    fi
    if [ "$CPUT_CASES" -eq 0 ]; then
        echo "concurrent_put=skipped"
    elif [ "$CPUT_FAILURES" -eq 0 ]; then
        echo "concurrent_put=pass"
    else
        echo "concurrent_put=fail ($CPUT_FAILURES of $CPUT_CASES)"
    fi
    if [ "$STALE_CASES" -eq 0 ]; then
        echo "stale_shard=skipped"
    elif [ "$STALE_FAILURES" -eq 0 ]; then
        echo "stale_shard=pass"
    else
        echo "stale_shard=fail ($STALE_FAILURES of $STALE_CASES)"
    fi
    if [ "$LM_CASES" -eq 0 ]; then
        echo "last_modified=skipped"
    elif [ "$LM_FAILURES" -eq 0 ]; then
        echo "last_modified=pass"
    else
        echo "last_modified=fail ($LM_FAILURES of $LM_CASES)"
    fi
    if [ "$LARGE_CASES" -eq 0 ]; then
        echo "large_object=skipped"
    elif [ "$LARGE_FAILURES" -eq 0 ]; then
        echo "large_object=pass${LARGE_SKIPPED:+ ($LARGE_SKIPPED size(s) skipped for disk)}"
    else
        echo "large_object=fail ($LARGE_FAILURES of $LARGE_CASES)"
    fi
    echo
    echo "Timeline"
    echo "--------"
    cat "$EVENTS"
} > "$RUN_DIR/run-info.txt"

echo "Stress results: $RUN_DIR"

# Held to the end so the freeze test ran and reported. The run is red either
# way: an overwrite that failed and did not leave the object alone is data loss
# on the ordinary write path, which is not a lesser result than a host that did
# not rejoin.
[ "$TORN_FAILURES" -eq 0 ] \
    || fail "torn-overwrite failed $TORN_FAILURES of $TORN_CASES assertions"
[ "$STALE_FAILURES" -eq 0 ] \
    || fail "stale-shard failed $STALE_FAILURES of $STALE_CASES assertions"
[ "$REPAIR_FAILURES" -eq 0 ] \
    || fail "repair failed $REPAIR_FAILURES of $REPAIR_CASES assertions"
[ "$HANDOFF_FAILURES" -eq 0 ] \
    || fail "handoff failed $HANDOFF_FAILURES of $HANDOFF_CASES assertions"
[ "$LARGE_FAILURES" -eq 0 ] \
    || fail "large-object failed $LARGE_FAILURES of $LARGE_CASES assertions"
[ "$LM_FAILURES" -eq 0 ] \
    || fail "last-modified failed $LM_FAILURES of $LM_CASES assertions"
[ "$CPUT_FAILURES" -eq 0 ] \
    || fail "concurrent-put failed $CPUT_FAILURES of $CPUT_CASES assertions"
