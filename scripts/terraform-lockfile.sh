#!/usr/bin/env bash
#
# terraform-lockfile.sh - Drive the Terraform S3 backend with use_lockfile = true
# against a local single-host predastore.
#
# Terraform takes its state lock by writing <key>.tflock with If-None-Match: *,
# so this is the client that depends on a conditional PutObject answering 412
# while another process holds the lock. The script starts its own cluster on
# shifted ports, creates a bucket, runs init and plan, then holds the lock with
# a paused apply and shows a second plan is refused until the apply ends.
#
# Usage:
#   scripts/terraform-lockfile.sh
#
# Environment:
#   TERRAFORM     terraform binary (default: terraform on PATH)
#   PREDA_PORT    gate port of the generated profile (default 18443)
#
# Only terraform's built-in provider is used, so init needs no registry. The
# credentials are the AWS documentation example keys the dev profiles accept.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM="${TERRAFORM:-terraform}"
PORT="${PREDA_PORT:-18443}"
ADMIN_PORT=$((PORT + 1))
REGION="ap-southeast-2"
BUCKET="tflock"
STATE_KEY="demo/terraform.tfstate"
ENDPOINT="https://127.0.0.1:${PORT}"
CLUSTER="tflock"

ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

WORK="$(mktemp -d)"
export PREDA_DIR="$WORK/preda"
export PREDA_CONFIG_DIR="$WORK/config"
mkdir -p "$PREDA_CONFIG_DIR" "$WORK/tf"

FAILED=0
APPLY_PID=""

step() { echo; echo "==== $*"; }
pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; FAILED=1; }

cleanup() {
    [ -n "$APPLY_PID" ] && kill "$APPLY_PID" 2>/dev/null || true
    "$SCRIPT_DIR/stop.sh" "$CLUSTER" >/dev/null 2>&1 || true
    find "$WORK" -delete 2>/dev/null || true
}
trap cleanup EXIT

# Keep the run off any real AWS account: no shared config, no profile, no IMDS.
unset AWS_PROFILE AWS_SESSION_TOKEN AWS_DEFAULT_PROFILE
export AWS_CONFIG_FILE=/dev/null AWS_SHARED_CREDENTIALS_FILE=/dev/null
export AWS_EC2_METADATA_DISABLED=true
export AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY"
export AWS_REGION="$REGION"
export CHECKPOINT_DISABLE=1 TF_IN_AUTOMATION=1 TF_INPUT=0

# s3 issues one SigV4-signed request with curl and prints the HTTP status.
s3() {
    local method="$1" path="$2" verb
    # -X HEAD would wait for a body that never comes; --head does not.
    if [ "$method" = HEAD ]; then verb=(--head); else verb=(-X "$method"); fi
    curl -s -o /dev/null -w '%{http_code}' --cacert "$PREDA_DIR/server.pem" \
        --aws-sigv4 "aws:amz:${REGION}:s3" --user "${ACCESS_KEY}:${SECRET_KEY}" \
        "${verb[@]}" "${ENDPOINT}${path}"
}

# wait_for runs "$@" until it succeeds, for at most $WAIT_TRIES tries.
wait_for() {
    local i
    for ((i = 0; i < ${WAIT_TRIES:-300}; i++)); do
        "$@" && return 0
        sleep 0.2
    done
    return 1
}

step "Start a single-host predastore on ${ENDPOINT}"
cat > "$PREDA_CONFIG_DIR/${CLUSTER}.toml" <<EOF
version = 1
region = "${REGION}"

[rs]
data = 1
parity = 0

[[host]]
id = 1
addr = "127.0.0.1"
admin_port = ${ADMIN_PORT}

  [[host.node]]
  id = 1
  role = "gate"
  port = ${PORT}

  [[host.node]]
  id = 2
  role = "meta"
  port = $((PORT + 2))

  [[host.node]]
  id = 3
  role = "blob"
  port = $((PORT + 3))

[[auth]]
access_key_id = "${ACCESS_KEY}"
secret_access_key = "${SECRET_KEY}"
account_id = "123456789012"
EOF
"$SCRIPT_DIR/start.sh" -w "$CLUSTER"
wait_for curl -sf "http://127.0.0.1:${ADMIN_PORT}/readyz" -o /dev/null \
    || { echo "predastore did not become ready"; exit 1; }

step "Create bucket ${BUCKET}"
code="$(s3 PUT "/${BUCKET}")"
[ "$code" = 200 ] && pass "CreateBucket answered 200" || { fail "CreateBucket answered $code"; exit 1; }

step "Terraform configuration"
cat > "$WORK/tf/main.tf" <<EOF
terraform {
  backend "s3" {
    bucket                      = "${BUCKET}"
    key                         = "${STATE_KEY}"
    region                      = "${REGION}"
    endpoints                   = { s3 = "${ENDPOINT}" }
    use_path_style              = true
    use_lockfile                = true
    skip_credentials_validation = true
    skip_requesting_account_id  = true
    skip_metadata_api_check     = true
    skip_region_validation      = true
  }
}

resource "terraform_data" "hold" {
  input = "lock-demo"

  # The lock is held for as long as this provisioner runs.
  provisioner "local-exec" {
    command = "touch '${WORK}/holding'; while [ ! -f '${WORK}/release' ]; do sleep 0.2; done"
  }
}
EOF
cat "$WORK/tf/main.tf"
export AWS_CA_BUNDLE="$PREDA_DIR/server.pem"
"$TERRAFORM" version
cd "$WORK/tf"

step "terraform init"
if "$TERRAFORM" init -no-color; then pass "init"; else fail "init"; exit 1; fi

step "terraform plan"
if "$TERRAFORM" plan -no-color -lock-timeout=0s; then pass "plan"; else fail "plan"; fi
code="$(s3 HEAD "/${BUCKET}/${STATE_KEY}.tflock")"
[ "$code" = 404 ] && pass "no lock object after plan (HEAD $code)" || fail "lock object after plan (HEAD $code)"

step "terraform apply (holds the lock until released)"
"$TERRAFORM" apply -no-color -auto-approve -lock-timeout=0s > "$WORK/apply.log" 2>&1 &
APPLY_PID=$!
if wait_for test -f "$WORK/holding"; then pass "apply is inside its provisioner"; else fail "apply never reached its provisioner"; cat "$WORK/apply.log"; exit 1; fi
code="$(s3 HEAD "/${BUCKET}/${STATE_KEY}.tflock")"
[ "$code" = 200 ] && pass "lock object present while apply runs (HEAD $code)" || fail "no lock object while apply runs (HEAD $code)"

step "Concurrent terraform plan while the lock is held (expect a lock error)"
if TF_LOG=DEBUG TF_LOG_PATH="$WORK/conflict.debug" "$TERRAFORM" plan -no-color -lock-timeout=0s > "$WORK/conflict.log" 2>&1; then
    cat "$WORK/conflict.log"
    fail "second plan succeeded while the lock was held"
else
    cat "$WORK/conflict.log"
    grep -q "Error acquiring the state lock" "$WORK/conflict.log" \
        && pass "second plan refused with a state lock error" \
        || fail "second plan failed without a state lock error"
fi
echo "-- lock PUT exchange from the refused plan's debug log:"
grep -E "PutObject|If-None-Match|412|PreconditionFailed" "$WORK/conflict.debug" \
    | grep -v -i -E "authorization|x-amz-security-token|credential" | head -20 || true

step "Release the lock"
touch "$WORK/release"
if wait "$APPLY_PID"; then pass "apply finished"; else fail "apply failed"; fi
APPLY_PID=""
cat "$WORK/apply.log"
code="$(s3 HEAD "/${BUCKET}/${STATE_KEY}.tflock")"
[ "$code" = 404 ] && pass "lock object removed after apply (HEAD $code)" || fail "lock object left after apply (HEAD $code)"

step "terraform plan after release (expect success)"
if "$TERRAFORM" plan -no-color -lock-timeout=0s -detailed-exitcode; then
    pass "plan after release succeeded with no changes"
else
    fail "plan after release failed or found changes"
fi
"$TERRAFORM" state list -no-color

echo
if [ "$FAILED" = 0 ]; then echo "RESULT: PASS"; else echo "RESULT: FAIL"; fi
exit "$FAILED"
