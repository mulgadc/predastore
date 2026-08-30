#!/usr/bin/env bash
#
# hwbench.sh - Run a three-host Predastore cluster on bare metal and measure it.
#
# The hosts are real machines on a 25GbE link, not loopback aliases, so every
# step that scripts/start.sh does locally is done over ssh here. Nothing runs
# as root and nothing is installed on the hosts: the binary is static, the
# trust anchor is passed through SSL_CERT_FILE rather than the OS store, and
# everything lands under one directory that can be deleted.
#
# Usage:
#   hwbench.sh build   [ref...]     Build a static s3d per ref into the work dir
#   hwbench.sh deploy  <ref>        Push that ref's binary and its inputs
#   hwbench.sh start   <ref>        Start all three hosts and wait for a leader
#   hwbench.sh verify  <ref>        Round trip an object through every gate
#   hwbench.sh stop                 Stop every host and confirm no s3d survives
#   hwbench.sh status               What is running where
#   hwbench.sh perf    <ref> [tag]  Run e2e-performance.sh against the cluster
#   hwbench.sh clean                Remove the deployment from every host
#
# Environment:
#   HW_HOSTS       Space separated ssh targets, in host-id order
#   HW_ADDRS       Their br-lan addresses, in the same order
#   HW_ROOT        Deployment root on each host
#   HW_REFS        Refs to build, as name:committish pairs
#   PERF_PRESET    Passed to Warp sizing: smoke or compare
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd -P)"
REPO_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd -P)"

HW_HOSTS="${HW_HOSTS:-tf-user@bottlebrush tf-user@ironbark tf-user@casuarina}"
HW_ADDRS="${HW_ADDRS:-10.10.8.4 10.10.8.5 10.10.8.6}"
HW_ROOT="${HW_ROOT:-/mnt/disk3/tf-user/predastore}"
HW_REFS="${HW_REFS:-dev:origin/dev base:f3a795d tip:80bda06}"
WORK="${HW_WORK:-/tmp/hwbench}"
CONFIG_NAME="3host-hw.toml"

read -ra HOSTS <<< "$HW_HOSTS"
read -ra ADDRS <<< "$HW_ADDRS"

ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
REGION="ap-southeast-2"
GATE_PORT=8443
TOOLS_HOST="${HW_TOOLS_HOST:-${HOSTS[-1]}}"

log()  { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
fail() { printf '%s FAIL %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; exit 1; }

on() { local h="$1"; shift; timeout 120 ssh -n -o BatchMode=yes -o ConnectTimeout=10 "$h" "$@"; }

# each runs one command on every host, in parallel, and fails if any host does.
each() {
    local pids=() h rc=0
    for h in "${HOSTS[@]}"; do on "$h" "$@" & pids+=("$!"); done
    for p in "${pids[@]}"; do wait "$p" || rc=1; done
    return "$rc"
}

# --- build ---------------------------------------------------------------
#
# GOWORK=off so each ref resolves its own declared bluebottle rather than
# whatever the workspace has checked out, and GOFIPS140 because fipsboot
# panics at init without it — a failure that surfaces at startup on the far
# host rather than at build time here.

cmd_build() {
    mkdir -p "$WORK/build"
    local pair name ref
    for pair in $HW_REFS; do
        name="${pair%%:*}"; ref="${pair##*:}"
        if [ ! -d "$WORK/src-$name" ]; then
            git -C "$REPO_DIR" worktree add -q --detach "$WORK/src-$name" "$ref"
        fi
        log "building $name from $(git -C "$WORK/src-$name" rev-parse --short HEAD)"
        ( cd "$WORK/src-$name" \
            && GOWORK=off CGO_ENABLED=0 GOFIPS140=v1.0.0 \
               go build -ldflags "-s -w" -o "$WORK/build/s3d-$name" ./cmd/s3d )
        git -C "$WORK/src-$name" rev-parse HEAD > "$WORK/build/s3d-$name.sha"
    done
}

# --- shared inputs -------------------------------------------------------
#
# One keypair and one master key for the whole cluster, generated once and
# reused across refs so a comparison never has a new key as a variable. The
# SANs cover every host address because each host presents this cert to its
# peers as well as to S3 clients.

cmd_inputs() {
    mkdir -p "$WORK/inputs"
    if [ ! -f "$WORK/inputs/server.pem" ]; then
        local san="DNS:localhost,IP:127.0.0.1" a
        for a in "${ADDRS[@]}"; do san="${san},IP:${a}"; done
        log "generating TLS keypair for ${san}"
        openssl req -x509 -newkey rsa:2048 -nodes \
            -keyout "$WORK/inputs/server.key" -out "$WORK/inputs/server.pem" \
            -days 3650 -subj '/CN=localhost' -addext "subjectAltName=${san}" 2>/dev/null
    fi
    if [ ! -f "$WORK/inputs/master.key" ]; then
        log "generating AES-256 master key"
        ( umask 0177 && openssl rand -out "$WORK/inputs/master.key" 32 )
    fi
    cp "$REPO_DIR/config/$CONFIG_NAME" "$WORK/inputs/$CONFIG_NAME"
}

cmd_deploy() {
    local ref="${1:?usage: deploy <ref>}"
    [ -f "$WORK/build/s3d-$ref" ] || fail "no binary for $ref — run build first"
    cmd_inputs

    local i h
    for i in "${!HOSTS[@]}"; do
        h="${HOSTS[$i]}"
        on "$h" "mkdir -p $HW_ROOT/$ref/{data,logs,pids} $HW_ROOT/inputs"
        scp -q "$WORK/build/s3d-$ref" "$h:$HW_ROOT/$ref/s3d"
        scp -q "$WORK/inputs/server.pem" "$WORK/inputs/server.key" \
               "$WORK/inputs/master.key" "$WORK/inputs/$CONFIG_NAME" \
               "$h:$HW_ROOT/inputs/"
        on "$h" "chmod 700 $HW_ROOT/$ref/s3d; chmod 600 $HW_ROOT/inputs/master.key $HW_ROOT/inputs/server.key"
        log "deployed $ref to $h"
    done
}

# --- tools ---------------------------------------------------------------
#
# Warp goes to every host because the load is driven from all three at once,
# in its client/server mode, so no single machine is both the whole client and
# a third of the cluster. The AWS CLI goes to one host only: it addresses any
# gate over the network, so correctness does not need it everywhere, and it is
# a 245 MB self-contained tree rather than one static binary.

cmd_tools() {
    local i h
    [ -x "$REPO_DIR/bin/tools/warp" ] || fail "no warp at bin/tools/warp — run make warp-install"
    for i in "${!HOSTS[@]}"; do
        h="${HOSTS[$i]}"
        on "$h" "mkdir -p $HW_ROOT/tools"
        scp -q "$REPO_DIR/bin/tools/warp" "$h:$HW_ROOT/tools/warp"
        on "$h" "chmod 700 $HW_ROOT/tools/warp"
    done
    log "warp deployed to all ${#HOSTS[@]} hosts"

    if ! on "$TOOLS_HOST" "test -x $HW_ROOT/tools/aws-cli/v2/current/bin/aws"; then
        log "shipping the AWS CLI to $TOOLS_HOST"
        tar -C /usr/local -cf - aws-cli | on "$TOOLS_HOST" "tar -C $HW_ROOT/tools -xf -"
        # The installer's "current" symlink is absolute into /usr/local, which
        # does not exist on the host. Relink it beside the version it names.
        on "$TOOLS_HOST" "cd $HW_ROOT/tools/aws-cli/v2 && ln -sfn \"\$(ls -d [0-9]* | tail -1)\" current"
    fi
    on "$TOOLS_HOST" "$HW_ROOT/tools/aws-cli/v2/current/bin/aws --version"
}

# --- lifecycle -----------------------------------------------------------
#
# SSL_CERT_FILE is what keeps this sudo-free. s3d verifies QUIC peers against
# the system pool with no RootCAs override, and Go's x509 reads that variable
# when it builds the pool, so the cluster trusts its own cert without anything
# being written to /usr/local/share/ca-certificates.

cmd_start() {
    local ref="${1:?usage: start <ref>}"
    local i h id

    # Every run starts from an empty store. Warp buckets are unique per run so
    # stale objects would not break a result, but they would let one ref be
    # measured against a fuller disk than another.
    each "rm -rf $HW_ROOT/$ref/data && mkdir -p $HW_ROOT/$ref/data" >/dev/null

    # setsid --fork around a shell that owns the redirection. Backgrounding with
    # the redirect on the s3d command itself leaves ssh waiting on the channel
    # even though s3d's own descriptors are clean, and the launch hangs until
    # the timeout fires.
    local inner
    for i in "${!HOSTS[@]}"; do
        h="${HOSTS[$i]}"; id=$((i + 1))
        inner="cd $HW_ROOT/$ref && \
            export SSL_CERT_FILE=$HW_ROOT/inputs/server.pem \
                   GO_PROF='${GO_PROF:-}' GO_PROF_DIR='${GO_PROF_DIR:-}' \
                   GO_PROF_INTERVAL='${GO_PROF_INTERVAL:-}' \
                   GO_PROF_CPU_WINDOW='${GO_PROF_CPU_WINDOW:-}' && \
            exec ./s3d -config $HW_ROOT/inputs/$CONFIG_NAME -host $id \
                -data-dir $HW_ROOT/$ref/data \
                -tls-cert $HW_ROOT/inputs/server.pem \
                -tls-key $HW_ROOT/inputs/server.key \
                -encryption-key $HW_ROOT/inputs/master.key"
        on "$h" "setsid --fork bash -c \"$inner\" < /dev/null > $HW_ROOT/$ref/logs/host-$id.log 2>&1"
        log "started host $id on $h"
    done
    wait_ready "$ref"
}

# wait_ready holds until every gate answers TLS. A gate that never answers is
# a failed formation, and returning early would hand the benchmark a cluster
# that is still electing.
wait_ready() {
    local ref="$1" i a deadline ok
    deadline=$(( $(date +%s) + 90 ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        ok=1
        for i in "${!ADDRS[@]}"; do
            a="${ADDRS[$i]}"
            on "${HOSTS[0]}" "timeout 3 bash -c '</dev/tcp/$a/$GATE_PORT'" 2>/dev/null || ok=0
        done
        [ "$ok" -eq 1 ] && { log "all ${#ADDRS[@]} gates listening"; return 0; }
        sleep 3
    done
    for i in "${!HOSTS[@]}"; do
        echo "--- ${HOSTS[$i]} host $((i+1)) ---"
        on "${HOSTS[$i]}" "tail -20 $HW_ROOT/$ref/logs/host-$((i+1)).log" || true
    done
    fail "gates did not come up within 90s"
}

# cmd_stop confirms no s3d survives rather than trusting the signal. A stop
# that returns while a process still holds the ports serves the next ref's
# measurement from the previous ref's binary.
cmd_stop() {
    # shellcheck disable=SC2016  # expands on the remote host, not here
    each 'pkill -x s3d 2>/dev/null; for i in $(seq 1 45); do pgrep -x s3d >/dev/null || break; sleep 1; done; \
          if pgrep -x s3d >/dev/null; then pkill -9 -x s3d; sleep 2; fi; \
          echo "$(hostname) s3d remaining: $(pgrep -cx s3d || true)"'
}

cmd_status() {
    # shellcheck disable=SC2016  # expands on the remote host, not here
    each 'echo "$(hostname): s3d=$(pgrep -cx s3d || echo 0) load=$(cut -d" " -f1 /proc/loadavg)"'
}

cmd_clean() {
    cmd_stop || true
    each "rm -rf $HW_ROOT"
    log "removed $HW_ROOT from every host"
}

# --- verification --------------------------------------------------------
#
# A cluster that is merely listening is not a cluster that works. Every gate
# has to serve the object every other gate stored, or the shards are not
# crossing the wire and the benchmark measures nothing.

cmd_verify() {
    local i a bucket="hwbench-verify"
    on "$TOOLS_HOST" "head -c $((8 * 1024 * 1024)) /dev/urandom > $HW_ROOT/verify.bin"
    local want; want="$(on "$TOOLS_HOST" "sha256sum $HW_ROOT/verify.bin | cut -d' ' -f1")"

    awscli "${ADDRS[0]}" s3 mb "s3://$bucket" >/dev/null 2>&1 || true
    awscli "${ADDRS[0]}" s3 cp "$HW_ROOT/verify.bin" "s3://$bucket/probe" >/dev/null \
        || fail "PUT through ${ADDRS[0]} failed"

    # Read back through every gate, including the two that did not take the
    # write: at RS(2,1) over three hosts each of those has to fetch a shard
    # from a peer, so this is also the proof that QUIC is carrying data.
    for i in "${!ADDRS[@]}"; do
        a="${ADDRS[$i]}"
        on "$TOOLS_HOST" "rm -f $HW_ROOT/got.bin"
        awscli "$a" s3 cp "s3://$bucket/probe" "$HW_ROOT/got.bin" >/dev/null \
            || fail "GET through $a failed"
        local got; got="$(on "$TOOLS_HOST" "sha256sum $HW_ROOT/got.bin | cut -d' ' -f1")"
        [ "$got" = "$want" ] || fail "gate $a returned different bytes than were stored"
        log "verify: gate $a round tripped 8 MiB byte for byte"
    done
    awscli "${ADDRS[0]}" s3 rb "s3://$bucket" --force >/dev/null 2>&1 || true
}

# --- benchmark -----------------------------------------------------------
#
# The measurement is e2e-performance.sh in its external-hosts mode, not a
# second harness: same workloads, same preset sizing, same analysis, so a
# bare-metal number is comparable with the loopback one it replaces. It runs on
# a host because this workstation has no route to the cluster network.

cmd_perf() {
    local ref="${1:?usage: perf <ref>}" tag="${2:-run1}"
    local hosts i sha
    hosts=""
    for i in "${!ADDRS[@]}"; do hosts="${hosts:+$hosts,}${ADDRS[$i]}:$GATE_PORT"; done
    sha="$(cat "$WORK/build/s3d-$ref.sha" 2>/dev/null || echo unknown)"

    on "$TOOLS_HOST" "mkdir -p $HW_ROOT/harness/scripts/bench $HW_ROOT/results"
    scp -q "$REPO_DIR/scripts/lib.sh" "$TOOLS_HOST:$HW_ROOT/harness/scripts/lib.sh"
    scp -q "$REPO_DIR/scripts/bench/e2e-performance.sh" \
        "$TOOLS_HOST:$HW_ROOT/harness/scripts/bench/e2e-performance.sh"

    on "$TOOLS_HOST" "chmod +x $HW_ROOT/harness/scripts/bench/e2e-performance.sh && \
        PATH=$HW_ROOT/tools/aws-cli/v2/current/bin:\$PATH \
        WARP=$HW_ROOT/tools/warp \
        PERF_PRESET=${PERF_PRESET:-compare} \
        PERF_CONFIGS=$ref-$tag \
        PERF_EXTERNAL_HOSTS=$hosts \
        PERF_EXTERNAL_SHA=$sha \
        PERF_EXTERNAL_GO='$(go version)' \
        PERF_RESULTS_ROOT=$HW_ROOT/results \
        $HW_ROOT/harness/scripts/bench/e2e-performance.sh 2>&1 | tail -30"
}

awscli() {
    local addr="$1"; shift
    on "$TOOLS_HOST" "AWS_ACCESS_KEY_ID=$ACCESS_KEY AWS_SECRET_ACCESS_KEY=$SECRET_KEY \
        AWS_DEFAULT_REGION=$REGION AWS_EC2_METADATA_DISABLED=true \
        $HW_ROOT/tools/aws-cli/v2/current/bin/aws --no-verify-ssl \
        --endpoint-url https://$addr:$GATE_PORT \
        --cli-connect-timeout 10 --cli-read-timeout 300 $*" 2>/dev/null
}

case "${1:-}" in
    build)  shift; cmd_build "$@" ;;
    tools)  shift; cmd_tools "$@" ;;
    inputs) shift; cmd_inputs "$@" ;;
    deploy) shift; cmd_deploy "$@" ;;
    start)  shift; cmd_start "$@" ;;
    verify) shift; cmd_verify "$@" ;;
    perf)   shift; cmd_perf "$@" ;;
    stop)   shift; cmd_stop "$@" ;;
    status) shift; cmd_status "$@" ;;
    clean)  shift; cmd_clean "$@" ;;
    *) sed -n '2,30p' "$0"; exit 2 ;;
esac
