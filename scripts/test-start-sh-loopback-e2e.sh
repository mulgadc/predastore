#!/usr/bin/env bash
#
# test-start-sh-loopback-e2e.sh — end-to-end reproduction for the loopback
# certificate plan (docs/development/bugs/predastore-start-sh-loopback-cert.md):
# a generated all-loopback 3-host profile must start to readyz on every host
# and accept an authenticated S3 write with the certificate start.sh made.
#
# Red on dev: start.sh's SAN assembly excludes every 127.* address, so the
# generated certificate covers only 127.0.0.1. QUIC dials between loopback
# aliases fail verification ("certificate is valid for 127.0.0.1, not
# 127.0.0.3"), meta never reaches quorum, and the profile starts
# half-connected.
#
# Needs passwordless sudo: start.sh installs the trust anchor and adds the
# loopback aliases with sudo. Without it the test skips with a named reason;
# run it where sudo is available (operator workstation, root CI container).
# Side effects beyond the temp PREDA_DIR: loopback aliases (removed by
# stop.sh) and a trust anchor for the run's own certificate (inert once the
# profile is gone; remove from /usr/local/share/ca-certificates if unwanted).
#
# Usage: scripts/test-start-sh-loopback-e2e.sh   (0 pass, 1 fail, 77 skip)
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

if ! command -v aws >/dev/null 2>&1; then
    echo "SKIP: aws CLI not found; the S3-write leg cannot run"
    exit 77
fi
if ! sudo -n true 2>/dev/null; then
    echo "SKIP: passwordless sudo unavailable; start.sh cannot install the trust anchor or add loopback aliases. Run where sudo works (root CI container)."
    exit 77
fi

WORK="$(mktemp -d /tmp/preda-e2e.XXXXXX)"
PROFILE_DIR="$WORK/config"
PREDA_DIR="$WORK/preda"
mkdir -p "$PROFILE_DIR"
PROFILE_NAME="loopback-e2e"

# The shipped 3-host profile, addressed over loopback aliases — the shape the
# 2026-11-27 lab used and the case the SAN filter drops.
sed -e 's/10\.11\.12\.1/127.0.0.1/' \
    -e 's/10\.11\.12\.2/127.0.0.2/' \
    -e 's/10\.11\.12\.3/127.0.0.3/' \
    "$REPO_DIR/config/3host.toml" > "$PROFILE_DIR/$PROFILE_NAME.toml"

cleanup() {
    "$REPO_DIR/scripts/stop.sh" "$PROFILE_NAME" >/dev/null 2>&1
    pkill -x s3d 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

echo "starting all-loopback 3-host profile (PREDA_DIR=$PREDA_DIR)"
PREDA_DIR="$PREDA_DIR" PREDA_CONFIG_DIR="$PROFILE_DIR" LOG_LEVEL=warn \
    "$REPO_DIR/scripts/start.sh" -w "$PROFILE_NAME" >"$WORK/start.log" 2>&1
START_RC=$?
if [ $START_RC -ne 0 ]; then
    echo "FAIL: start.sh exited $START_RC"
    tail -20 "$WORK/start.log"
    exit 1
fi

# Every host must report ready. A half-connected start shows up here: the
# hosts whose peers they cannot verify never report meta_leader ok.
READY=0
for _ in $(seq 1 45); do
    OK=3
    for ip in 127.0.0.1 127.0.0.2 127.0.0.3; do
        R=$(curl -s --max-time 2 "http://$ip:9099/readyz" | grep -o '"meta_leader":"[a-z]*"')
        [ "$R" = '"meta_leader":"ok"' ] || OK=$((OK - 1))
    done
    if [ "$OK" -eq 3 ]; then
        READY=1
        break
    fi
    sleep 2
done
if [ $READY -ne 1 ]; then
    echo "FAIL: cluster did not reach readyz on all three hosts within 90s"
    for ip in 127.0.0.1 127.0.0.2 127.0.0.3; do
        echo "  readyz $ip: $(curl -s --max-time 2 "http://$ip:9099/readyz" || echo unreachable)"
    done
    tail -10 "$WORK/start.log"
    exit 1
fi
echo "readyz: all three hosts ok"

# End-to-end proof: an authenticated S3 write through the gate, served by the
# generated certificate.
export AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export AWS_DEFAULT_REGION="ap-southeast-2"
ENDPOINT="https://127.0.0.1:8443"
CA="$PREDA_DIR/server.pem"
BUCKET="loopback-e2e-$$"

echo "hello from the loopback e2e" > "$WORK/object.txt"
if ! aws s3api create-bucket --bucket "$BUCKET" --endpoint-url "$ENDPOINT" \
        --create-bucket-configuration LocationConstraint="$AWS_DEFAULT_REGION" \
        --ca-bundle "$CA" >"$WORK/aws.log" 2>&1; then
    echo "FAIL: authenticated create-bucket failed"
    cat "$WORK/aws.log"
    exit 1
fi
if ! aws s3api put-object --bucket "$BUCKET" --key e2e/object.txt \
        --body "$WORK/object.txt" --endpoint-url "$ENDPOINT" \
        --ca-bundle "$CA" >>"$WORK/aws.log" 2>&1; then
    echo "FAIL: authenticated put-object failed"
    tail -5 "$WORK/aws.log"
    exit 1
fi
if ! aws s3api get-object --bucket "$BUCKET" --key e2e/object.txt \
        "$WORK/readback.txt" --endpoint-url "$ENDPOINT" \
        --ca-bundle "$CA" >>"$WORK/aws.log" 2>&1; then
    echo "FAIL: authenticated get-object failed"
    tail -5 "$WORK/aws.log"
    exit 1
fi
if ! cmp -s "$WORK/object.txt" "$WORK/readback.txt"; then
    echo "FAIL: object round-tripped with different content"
    exit 1
fi
echo "s3: authenticated put/get round-trip ok"

echo "PASS: all-loopback profile starts to readyz and serves authenticated S3"
exit 0
