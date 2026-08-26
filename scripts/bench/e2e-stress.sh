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
#   STRESS_SCENARIO    "freeze" (default), "partial-put" or "torn-overwrite".
#                      freeze is the rejoin test described above; the other two
#                      are write-path faults and return before the load phase.
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
if [ "${STRESS_SCENARIO:-freeze}" = partial-put ]; then
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
if [ "${STRESS_SCENARIO:-freeze}" = torn-overwrite ]; then
    SHARD_PROBE="$WORK_DIR/shardplace"
    go build -o "$SHARD_PROBE" "$REPO_DIR/scripts/bench/shardplace"

    BUCKET="stress-torn-${RUN_ID}"
    LINES="${STRESS_TORN_LINES:-16384}"
    V1="$WORK_DIR/state-v1.json"
    V2="$WORK_DIR/state-v2.json"

    # A state document rather than random bytes, because the objects this
    # destroyed in production were volume state: every record carries the
    # generation that wrote it, so a mixture is legible in the file itself
    # rather than only as a checksum that no longer matches. Both generations
    # are byte-for-byte the same length, which is what an in-place rewrite of
    # a state document looks like and what keeps the stored size honest.
    make_state() {
        awk -v gen="$1" -v n="$LINES" 'BEGIN {
            printf "{\n  \"v\": 1,\n  \"generation\": \"%s\",\n", gen
            for (i = 1; i <= n; i++)
                printf "  \"extent_%06d\": \"%s-%06d-0123456789abcdef0123456789abcdef\",\n", i, gen, i
            printf "  \"trailer\": \"%s\"\n}\n", gen
        }' > "$2"
    }

    make_state v1 "$V1"
    make_state v2 "$V2"
    [ "$(wc -c < "$V1")" -eq "$(wc -c < "$V2")" ] \
        || fail "the two generations differ in length, which is not the overwrite under test"
    log "torn-overwrite scenario: state document is $(wc -c < "$V1") bytes over $LINES records"

    # shard_host names the host holding a given shard role of a given key, and
    # survivor_gate a gate that is not on it. The PUT has to be issued through
    # a gate that is still running, or what stalls is the frontend rather than
    # the shard write.
    shard_host() {
        "$SHARD_PROBE" -config "$CONFIG_FILE" -bucket "$BUCKET" -key "$1" \
            | awk -v r="role=$2" '$2 == r { sub(/^host=/, "", $4); print $4; exit }'
    }
    survivor_gate() {
        parse_hosts "$CONFIG_FILE" | awk -v h="$1" '$1 != h && $3 != "" { print "https://" $2 ":" $3; exit }'
    }

    FIRST_GATE="https://$(gate_endpoints "$CONFIG_FILE" | head -1)"
    aws_s3 "$FIRST_GATE" s3 mb "s3://$BUCKET" >/dev/null

    FAILURES=0
    CASES_RUN=0

    # generation_of classifies what came back. Neither generation intact is
    # the finding: a spliced object is one the reader cannot detect and the
    # writer never knew it made.
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

    # freeze_and_overwrite stores v1, stops the host holding one named shard of
    # that key, overwrites with v2, and thaws. The PUT is expected to fail: one
    # shard node is unreachable and the write path fails on any shard error.
    # What the run is here to establish is the state it leaves behind.
    freeze_and_overwrite() {
        local name="$1" role="$2"
        local key="state-${name}.json"
        local got="$WORK_DIR/got-${name}.json"
        local host gate pid rc

        host="$(shard_host "$key" "$role")"
        [ -n "$host" ] || fail "$name: shardplace named no $role shard host for $key"
        gate="$(survivor_gate "$host")"
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
            FAILURES=$(( FAILURES + 1 ))
        fi
        CASES_RUN=$(( CASES_RUN + 1 ))

        # Long enough for the thawed host to answer again, so the GET below is
        # reading the cluster's settled state rather than racing the thaw.
        sleep 10

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
            FAILURES=$(( FAILURES + 1 ))
        fi
        CASES_RUN=$(( CASES_RUN + 1 ))
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
    RECON_HOST="$(shard_host "$PARITY_KEY" data)"
    RECON_GATE="$(survivor_gate "$RECON_HOST")"
    RECON_PID="$(cat "$PID_DIR/host-${RECON_HOST}.pid")"
    RECON_GOT="$WORK_DIR/got-reconstructed.json"

    log "reconstruction: stopping host $RECON_HOST to force $PARITY_KEY to rebuild from parity"
    kill -STOP "$RECON_PID"
    RECON_RC=0
    aws_s3 "$RECON_GATE" --cli-connect-timeout 10 --cli-read-timeout 120 \
        s3 cp "s3://$BUCKET/$PARITY_KEY" "$RECON_GOT" --only-show-errors 2>>"$EVENTS" || RECON_RC=$?
    kill -CONT "$RECON_PID"

    if [ "$RECON_RC" -ne 0 ]; then
        log "reconstruction: GET failed with $RECON_RC, so the object is unreadable one node down"
        FAILURES=$(( FAILURES + 1 ))
    else
        cp "$RECON_GOT" "$RUN_DIR/torn-overwrite-reconstructed.json"
        RECON_SEEN="$(generation_of "$RECON_GOT")"
        if [ "$RECON_SEEN" = v1 ]; then
            log "reconstruction: pass, rebuilt v1 from parity"
        else
            log "reconstruction: FAIL rebuilt $RECON_SEEN from parity"
            FAILURES=$(( FAILURES + 1 ))
        fi
    fi
    CASES_RUN=$(( CASES_RUN + 1 ))
    sleep 10

    log "raft state after the scenario"
    meta_status "${META_ALL[@]}" | tee -a "$EVENTS"

    aws_s3 "$FIRST_GATE" s3 rb "s3://$BUCKET" --force >/dev/null 2>&1 || true

    echo "Stress results: $RUN_DIR"
    [ "$FAILURES" -eq 0 ] || fail "torn-overwrite scenario failed $FAILURES of $CASES_RUN assertions"
    log "torn-overwrite scenario passed"
    exit 0
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
    echo
    echo "Timeline"
    echo "--------"
    cat "$EVENTS"
} > "$RUN_DIR/run-info.txt"

echo "Stress results: $RUN_DIR"
