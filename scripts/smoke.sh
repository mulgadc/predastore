#!/usr/bin/env bash
#
# smoke.sh - Drive a running predastore with real S3 clients and workloads.
#
# The point is not that the AWS CLI can put an object. It is to find which real
# applications work unmodified against predastore today, and exactly which S3
# operation stops the ones that do not. Every check runs even after an earlier
# one fails, and the summary at the end is the deliverable.
#
# Checks assert response content wherever content is the thing at stake. An
# exit-code-only suite scores a CopyObject that answers 200 and writes zero
# bytes as a pass, which is how that defect survived as long as it did.
#
# Usage:
#   scripts/smoke.sh                 # everything
#   scripts/smoke.sh aws rclone      # named suites only
#
# Suites: aws rclone restic registry
#
# Environment:
#   PREDA_ENDPOINT   S3 endpoint (default https://127.0.0.1:8443)
#   PREDA_CA         Trust anchor. Defaults to deploy/docker/certs/server.pem
#                    if present, else it is taken from the endpoint itself.
#   PREDA_BASELINE   Compare against this baseline and fail only on
#                    regressions. See scripts/smoke-baseline.txt.
#   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

# Client images are pinned. An unpinned client that changes behaviour presents
# as a predastore regression, and the whole value of this suite is that a
# failure names the operation that broke.
AWS_IMAGE="${AWS_IMAGE:-amazon/aws-cli:2.36.34}"
RCLONE_IMAGE="${RCLONE_IMAGE:-rclone/rclone:1.72.0}"
RESTIC_IMAGE="${RESTIC_IMAGE:-restic/restic:0.19.0}"
REGISTRY_IMAGE="${REGISTRY_IMAGE:-registry:3.0.0}"

PREDA_ENDPOINT="${PREDA_ENDPOINT:-https://127.0.0.1:8443}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-AKIAIOSFODNN7EXAMPLE}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-southeast-2}"

RUN_ID="smoke-$(date +%s)"
WORK="$(mktemp -d)"
chmod 0755 "$WORK"
trap 'rm -rf "$WORK"' EXIT

# The trust anchor. rclone and restic verify properly and have no insecure
# switch worth using, so they need the certificate rather than a flag. Taking
# it from the endpoint costs nothing and means the suite works against a
# single container, a compose cluster or a remote host without being told
# which. It trusts on first use, which is the right trade for a smoke test
# against a self-signed development identity and nowhere else.
CERT="${PREDA_CA:-}"
if [ -z "$CERT" ] && [ -f "$REPO_DIR/deploy/docker/certs/server.pem" ]; then
    CERT="$REPO_DIR/deploy/docker/certs/server.pem"
fi
if [ -z "$CERT" ]; then
    CERT="$WORK/endpoint-ca.pem"
    host_port="${PREDA_ENDPOINT#*://}"
    case "$host_port" in *:*) : ;; *) host_port="$host_port:443" ;; esac
    if ! openssl s_client -showcerts -connect "$host_port" </dev/null 2>/dev/null \
        | openssl x509 -outform PEM > "$CERT" 2>/dev/null || [ ! -s "$CERT" ]; then
        echo "smoke: could not obtain a certificate from $PREDA_ENDPOINT; set PREDA_CA" >&2
        exit 2
    fi
fi

RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[1;33m'; BOLD=$'\033[1m'; NC=$'\033[0m'

RESULTS=()
PASSED=0
FAILED=0

# check <label> <command...> — runs the command, records the outcome, never
# aborts the script. Output is kept and only printed when the check fails,
# because a passing suite of twenty is not worth twenty screens.
check() {
    local label="$1"; shift
    local out status
    out="$("$@" 2>&1)"; status=$?
    if [ $status -eq 0 ]; then
        printf '  %s✓%s %s\n' "$GREEN" "$NC" "$label"
        RESULTS+=("PASS|$label|")
        PASSED=$((PASSED + 1))
    else
        printf '  %s✗%s %s\n' "$RED" "$NC" "$label"
        printf '%s\n' "$out" | sed 's/^/      /' | tail -8
        RESULTS+=("FAIL|$label|$(printf '%s' "$out" | grep -oE '(NotImplemented|MethodNotAllowed|AccessDenied|NoSuchKey|InvalidRequest|501|405|403|404)' | head -1)")
        FAILED=$((FAILED + 1))
    fi
    return 0
}

section() { printf '\n%s%s%s\n' "$BOLD" "$1" "$NC"; }

# Clients run on the host network so they reach the published gate the same way
# an operator would, rather than through a compose-internal address.
aws_cli() {
    docker run --rm --network host \
        -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
        -v "$WORK:/work" \
        "$AWS_IMAGE" --no-verify-ssl --endpoint-url "$PREDA_ENDPOINT" "$@"
}

rclone_cli() {
    docker run --rm --network host \
        -v "$WORK:/work" -v "$CERT:/ca.pem:ro" \
        -e RCLONE_CONFIG_PREDA_TYPE=s3 \
        -e RCLONE_CONFIG_PREDA_PROVIDER=Other \
        -e RCLONE_CONFIG_PREDA_ENDPOINT="$PREDA_ENDPOINT" \
        -e RCLONE_CONFIG_PREDA_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
        -e RCLONE_CONFIG_PREDA_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
        -e RCLONE_CONFIG_PREDA_REGION="$AWS_DEFAULT_REGION" \
        -e RCLONE_CONFIG_PREDA_FORCE_PATH_STYLE=true \
        "$RCLONE_IMAGE" --ca-cert /ca.pem "$@"
}

# restic has no insecure-TLS switch, so it gets the dev cert as its root store.
restic_cli() {
    docker run --rm --network host \
        -v "$WORK:/work" -v "$CERT:/ca.pem:ro" \
        -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
        -e SSL_CERT_FILE=/ca.pem \
        -e RESTIC_PASSWORD=smoke \
        -e RESTIC_REPOSITORY="s3:${PREDA_ENDPOINT}/${RUN_ID}-restic" \
        "$RESTIC_IMAGE" "$@"
}

SUITES=("$@")
run_suite() {
    [ ${#SUITES[@]} -eq 0 ] && return 0
    local s
    for s in "${SUITES[@]}"; do [ "$s" = "$1" ] && return 0; done
    return 1
}

printf '%sPredastore smoke — %s%s\n' "$BOLD" "$PREDA_ENDPOINT" "$NC"

# --- AWS CLI: the baseline S3 surface ---------------------------------------

if run_suite aws; then
    section "aws-cli — core S3 surface"

    B="${RUN_ID}-aws"
    head -c 1048576 /dev/urandom > "$WORK/blob.bin"
    echo "hello predastore" > "$WORK/hello.txt"

    check "CreateBucket"            aws_cli s3 mb "s3://$B"
    check "ListBuckets"             aws_cli s3 ls
    check "PutObject (small)"       aws_cli s3 cp /work/hello.txt "s3://$B/hello.txt"
    check "PutObject (1 MiB)"       aws_cli s3 cp /work/blob.bin "s3://$B/blob.bin"
    check "ListObjectsV2"           aws_cli s3 ls "s3://$B/"
    check "HeadObject"              aws_cli s3api head-object --bucket "$B" --key hello.txt
    check "GetObject"               aws_cli s3 cp "s3://$B/hello.txt" /work/hello.out
    check "GetObject (range)"       aws_cli s3api get-object --bucket "$B" --key blob.bin --range "bytes=0-1023" /work/range.out

    # Multipart: 16 MiB forces the CLI past its 8 MiB default threshold.
    head -c 16777216 /dev/urandom > "$WORK/multi.bin"
    check "Multipart upload (16 MiB)" aws_cli s3 cp /work/multi.bin "s3://$B/multi.bin"

    # Content assertions. Each of these currently fails, and each is asserted
    # on what came back rather than on a status code, because that is the
    # difference between naming the defect and recording a pass.
    #
    # The ETag of a single-part PUT is the hex MD5 of the body. Predastore
    # derives it from the object name instead, so it is also stable across an
    # overwrite: any client that syncs on ETag concludes nothing ever changes.
    etag_is_content_md5() {
        local want got
        want=$(md5sum "$WORK/hello.txt" | cut -d' ' -f1)
        got=$(aws_cli s3api head-object --bucket "$B" --key hello.txt \
            --query ETag --output text 2>/dev/null | tr -d '"\r')
        if [ "$got" != "$want" ]; then
            echo "ETag is $got, body MD5 is $want"
            return 1
        fi
        return 0
    }
    check "ETag is the body MD5"     etag_is_content_md5

    # CopyObject answers 200 and writes a zero-byte object, so an exit-code
    # check calls the silent truncation a pass.
    copy_object_roundtrip() {
        aws_cli s3 cp "s3://$B/hello.txt" "s3://$B/hello-copy.txt" >/dev/null || return 1
        local size
        size=$(aws_cli s3api head-object --bucket "$B" --key hello-copy.txt \
            --query ContentLength --output text 2>/dev/null | tr -d '\r')
        if [ "$size" != "17" ]; then
            echo "CopyObject answered 200 but the destination is ${size:-missing} bytes, source is 17"
            return 1
        fi
        return 0
    }
    check "CopyObject (server-side)"  copy_object_roundtrip

    check "DeleteObjects (bulk)"      aws_cli s3api delete-objects --bucket "$B" \
        --delete 'Objects=[{Key=hello.txt}],Quiet=true'
    check "DeleteObject (single)"   aws_cli s3 rm "s3://$B/blob.bin"
    check "Presigned GET"           aws_cli s3 presign "s3://$B/multi.bin"
fi

# --- rclone: sync semantics -------------------------------------------------

if run_suite rclone; then
    section "rclone — sync and verify"

    B="${RUN_ID}-rclone"
    mkdir -p "$WORK/tree/sub"
    head -c 65536  /dev/urandom > "$WORK/tree/a.bin"
    head -c 131072 /dev/urandom > "$WORK/tree/sub/b.bin"
    echo "plain" > "$WORK/tree/c.txt"

    check "rclone mkdir"        rclone_cli mkdir "preda:$B"
    check "rclone copy (up)"    rclone_cli copy /work/tree "preda:$B/tree"
    check "rclone ls"           rclone_cli ls "preda:$B/tree"
    check "rclone check"        rclone_cli check /work/tree "preda:$B/tree"
    check "rclone copy (down)"  rclone_cli copy "preda:$B/tree" /work/tree-down
    # server-side copy exercises CopyObject; the fallback is a download+upload
    check "rclone copyto (server-side)" rclone_cli copyto "preda:$B/tree/c.txt" "preda:$B/tree/c-copy.txt"
    check "rclone purge (bulk delete)"  rclone_cli purge "preda:$B/tree"
fi

# --- restic: the correctness oracle ----------------------------------------

if run_suite restic; then
    section "restic — backup, verify, prune"

    check "restic bucket"       aws_cli s3 mb "s3://${RUN_ID}-restic"

    mkdir -p "$WORK/data"
    head -c 4194304 /dev/urandom > "$WORK/data/big.bin"
    cp "$SCRIPT_DIR/smoke.sh" "$WORK/data/" 2>/dev/null || true

    check "restic init"         restic_cli init
    check "restic backup"       restic_cli backup /work/data
    check "restic snapshots"    restic_cli snapshots
    # read-data re-downloads and verifies every pack: the strongest cheap
    # correctness signal available, since it fails on a single wrong byte.
    check "restic check --read-data" restic_cli check --read-data
    check "restic backup (2nd, dedup)" restic_cli backup /work/data
    check "restic forget --prune"      restic_cli forget --keep-last 1 --prune
fi

# --- docker registry: a real application on top of S3 -----------------------

if run_suite registry; then
    section "docker registry — S3 storage driver"

    B="${RUN_ID}-registry"
    check "registry bucket"     aws_cli s3 mb "s3://$B"

    docker rm -f preda-registry >/dev/null 2>&1

    registry_up() {
        docker run -d --rm --name preda-registry --network host \
            -e REGISTRY_HTTP_ADDR=127.0.0.1:5000 \
            -e REGISTRY_STORAGE=s3 \
            -e REGISTRY_STORAGE_S3_REGION="$AWS_DEFAULT_REGION" \
            -e REGISTRY_STORAGE_S3_REGIONENDPOINT="$PREDA_ENDPOINT" \
            -e REGISTRY_STORAGE_S3_BUCKET="$B" \
            -e REGISTRY_STORAGE_S3_ACCESSKEY="$AWS_ACCESS_KEY_ID" \
            -e REGISTRY_STORAGE_S3_SECRETKEY="$AWS_SECRET_ACCESS_KEY" \
            -e REGISTRY_STORAGE_S3_FORCEPATHSTYLE=true \
            -e REGISTRY_STORAGE_S3_SKIPVERIFY=true \
            "$REGISTRY_IMAGE" >/dev/null || return 1

        # The driver only touches S3 on first write, so a container that is up
        # is not yet evidence the backend works. Wait for the API instead.
        for _ in $(seq 1 30); do
            curl -sf -o /dev/null http://127.0.0.1:5000/v2/ && return 0
            docker ps --format '{{.Names}}' | grep -qx preda-registry || return 1
            sleep 1
        done
        return 1
    }

    check "registry starts on predastore" registry_up

    if docker ps --format '{{.Names}}' | grep -qx preda-registry; then
        docker pull -q hello-world >/dev/null 2>&1
        docker tag hello-world 127.0.0.1:5000/hello:smoke >/dev/null 2>&1

        # push is the interesting one: blob upload is a multipart PUT and the
        # manifest write is a small PUT, both through the S3 driver.
        check "docker push"  docker push -q 127.0.0.1:5000/hello:smoke
        docker rmi 127.0.0.1:5000/hello:smoke >/dev/null 2>&1
        check "docker pull"  docker pull -q 127.0.0.1:5000/hello:smoke
        check "registry catalog" curl -sf http://127.0.0.1:5000/v2/_catalog

        docker rmi 127.0.0.1:5000/hello:smoke >/dev/null 2>&1
        docker rm -f preda-registry >/dev/null 2>&1
    else
        printf '  %s—%s registry did not start, skipping push/pull\n' "$YELLOW" "$NC"
        docker logs preda-registry 2>&1 | tail -5 | sed 's/^/      /'
        docker rm -f preda-registry >/dev/null 2>&1
    fi
fi

# --- Summary ----------------------------------------------------------------

section "Summary"

printf '  %-34s %s\n' "CHECK" "RESULT"
for r in "${RESULTS[@]}"; do
    IFS='|' read -r status label code <<< "$r"
    if [ "$status" = "PASS" ]; then
        printf '  %-34s %s%s%s\n' "$label" "$GREEN" "$status" "$NC"
    else
        printf '  %-34s %s%s%s %s\n' "$label" "$RED" "$status" "$NC" "$code"
    fi
done

printf '\n  %s%d passed%s, %s%d failed%s\n' "$GREEN" "$PASSED" "$NC" "$RED" "$FAILED" "$NC"

# --- Baseline ---------------------------------------------------------------
#
# Predastore fails several of these today, so a bare exit status makes the
# suite unmergeable as a gate. Against a baseline the question becomes the
# useful one: did anything that used to work stop working. A check that starts
# passing is reported and does not fail the run, because the fix and the
# baseline update belong in the same change but not in the same second.

if [ -n "${PREDA_WRITE_BASELINE:-}" ]; then
    {
        echo "# Generated by scripts/smoke.sh against $PREDA_ENDPOINT."
        echo "# Regenerate with PREDA_WRITE_BASELINE=scripts/smoke-baseline.txt when a fix lands."
        for r in "${RESULTS[@]}"; do
            IFS='|' read -r status label _ <<< "$r"
            printf '%s|%s\n' "$status" "$label"
        done
    } > "$PREDA_WRITE_BASELINE"
    printf '  wrote baseline to %s\n' "$PREDA_WRITE_BASELINE"
fi

if [ -n "${PREDA_BASELINE:-}" ]; then
    if [ ! -f "$PREDA_BASELINE" ]; then
        echo "smoke: baseline $PREDA_BASELINE does not exist" >&2
        exit 2
    fi

    regressions=0
    fixed=0
    for r in "${RESULTS[@]}"; do
        IFS='|' read -r status label _ <<< "$r"
        want=$(grep -F "|$label" "$PREDA_BASELINE" | cut -d'|' -f1 | head -1)
        if [ -z "$want" ]; then
            printf '  %s?%s %s is not in the baseline\n' "$YELLOW" "$NC" "$label"
            continue
        fi
        if [ "$want" = "PASS" ] && [ "$status" = "FAIL" ]; then
            printf '  %sREGRESSION%s %s passed in the baseline and fails now\n' "$RED" "$NC" "$label"
            regressions=$((regressions + 1))
        elif [ "$want" = "FAIL" ] && [ "$status" = "PASS" ]; then
            fixed=$((fixed + 1))
        fi
    done

    if [ "$fixed" -gt 0 ]; then
        printf '\n  %s%d check(s) now pass that the baseline expects to fail.%s Update %s.\n' \
            "$GREEN" "$fixed" "$NC" "$PREDA_BASELINE"
    fi
    printf '\n'
    [ "$regressions" -eq 0 ]
    exit $?
fi

printf '\n'
[ "$FAILED" -eq 0 ]
