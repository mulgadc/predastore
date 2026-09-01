#!/usr/bin/env bash
#
# s3-tests.sh - Run the ceph/s3-tests conformance suite against predastore.
#
# scripts/smoke.sh answers "does restic work". This answers "does PutObject
# match the specification", which is the wider net underneath it. s3-tests is
# the suite Ceph RGW, MinIO and Garage are all validated against, so running it
# is how S3 compatibility becomes a measured property rather than a claim.
#
# The deliverable is the delta, not the pass rate. A fresh run against any
# non-AWS implementation fails a large number of cases, most of them features
# that were never in scope, so the committed baseline is what makes a change
# visible.
#
# The cluster is not started here. Bring one up first:
#
#   ./scripts/start.sh -w s3tests
#   ./scripts/s3-tests.sh
#
# The s3tests profile carries three service accounts and serves us-east-1, both
# of which the suite requires. Any other profile will error in fixture setup.
#
# Usage:
#   scripts/s3-tests.sh                        # everything
#   scripts/s3-tests.sh -k bucket_list         # pytest args pass through
#
# Environment:
#   PREDA_S3TESTS_HOST   Gate host                   (default 127.0.0.1)
#   PREDA_S3TESTS_PORT   Gate port                   (default 8443)
#   PREDA_S3TESTS_DIR    Checkout and venv cache     (default /tmp/predastore-s3-tests)
#   PREDA_S3TESTS_REF    ceph/s3-tests commit to pin
#   PREDA_BASELINE       Compare against this manifest and fail only on a
#                        regression. See scripts/s3-tests-baseline.txt.
#   PREDA_STRICT         1 to fail on any failing case, not just a regression.
#   PREDA_WRITE_BASELINE Write the manifest here instead of comparing.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
PLUGIN_DIR="$SCRIPT_DIR/s3tests"

# The suite is pinned for the same reason smoke.sh pins its client images: an
# unpinned suite that adds cases presents as a predastore regression, and the
# whole value here is that a diff names the operation that broke. Move it
# deliberately, and re-record the baseline in the same change.
S3TESTS_REPO="${PREDA_S3TESTS_REPO:-https://github.com/ceph/s3-tests.git}"
S3TESTS_REF="${PREDA_S3TESTS_REF:-5522d1c351f75bc00ae0f64f742f3f095f5939d9}"

HOST="${PREDA_S3TESTS_HOST:-127.0.0.1}"
PORT="${PREDA_S3TESTS_PORT:-8443}"
CACHE="${PREDA_S3TESTS_DIR:-/tmp/predastore-s3-tests}"
CHECKOUT="$CACHE/s3-tests"
VENV="$CACHE/venv"
BASELINE_FILE="${PREDA_BASELINE:-}"
WRITE_BASELINE="${PREDA_WRITE_BASELINE:-}"
STRICT="${PREDA_STRICT:-0}"
SKIPS="$SCRIPT_DIR/s3-tests-skips.txt"

# Only the S3 surface. test_iam, test_sts, test_sns, test_s3select and
# test_s3control cover RGW extensions and other AWS services, none of which
# predastore claims, so running them would fill the manifest with cases that
# are not a compatibility question.
SUITES=(s3tests/functional/test_s3.py s3tests/functional/test_headers.py)

RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[1;33m'; BOLD=$'\033[1m'; NC=$'\033[0m'

log()  { printf '%s%s%s\n' "$BOLD" "$1" "$NC"; }
warn() { printf '%s%s%s\n' "$YELLOW" "$1" "$NC" >&2; }
fail() { printf '%s%s%s\n' "$RED" "$1" "$NC" >&2; exit 2; }

# --- Suite checkout ---

if [ ! -d "$CHECKOUT/.git" ]; then
    log "Cloning ceph/s3-tests"
    mkdir -p "$CACHE"
    git clone -q "$S3TESTS_REPO" "$CHECKOUT" || fail "s3-tests: clone failed"
fi

git -C "$CHECKOUT" fetch -q origin || warn "s3-tests: fetch failed, using the checkout as it stands"
git -C "$CHECKOUT" checkout -q "$S3TESTS_REF" 2>/dev/null \
    || fail "s3-tests: no such commit $S3TESTS_REF"

# --- Virtualenv ---

if [ ! -x "$VENV/bin/pytest" ]; then
    log "Building the s3-tests virtualenv"
    python3 -m venv "$VENV" || fail "s3-tests: could not create a virtualenv"
    "$VENV/bin/pip" install -q --upgrade pip \
        || fail "s3-tests: could not upgrade pip"
    "$VENV/bin/pip" install -q -r "$CHECKOUT/requirements.txt" \
        || fail "s3-tests: could not install the suite's requirements"
fi

# --- Configuration ---
#
# Credentials are the ones in config/s3tests.toml. They are written out here
# rather than parsed from the profile so that a run against a remote gate needs
# only the two host variables, but the two files have to agree.

CONF="$CACHE/s3tests.conf"
cat > "$CONF" <<EOF
[DEFAULT]
host = $HOST
port = $PORT
is_secure = True
ssl_verify = False

[fixtures]
bucket prefix = s3tests-{random}-
iam name prefix = s3-tests-
iam path prefix = /s3-tests/

[s3 main]
display_name = main
user_id = 123456789012
email = main@example.com
api_name = us-east-1
access_key = AKIAIOSFODNN7EXAMPLE
secret_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

[s3 alt]
display_name = alt
user_id = 210987654321
email = alt@example.com
access_key = AKIAI44QH8DHBEXAMPLE
secret_key = je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY

[s3 tenant]
display_name = tenant
user_id = 345678901234
email = tenant@example.com
access_key = AKIAIOSFODNN7TENANT0
secret_key = kPxRfiCYEXAMPLEKEY/K7MDENG/bTenantKey0
tenant = tenantx

[iam]
display_name = iam
user_id = 123456789012
email = iam@example.com
access_key = AKIAIOSFODNN7EXAMPLE
secret_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

[iam root]
access_key = AKIAIOSFODNN7EXAMPLE
secret_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
account_id = 123456789012
user_id = 123456789012
email = root@example.com

[iam alt root]
access_key = AKIAI44QH8DHBEXAMPLE
secret_key = je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY
account_id = 210987654321
user_id = 210987654321
email = altroot@example.com
EOF

# --- Reachability ---
#
# Checked before the run because the suite's own failure for an unreachable
# gate is 880 identical connection errors, which takes fifteen minutes to
# produce and says nothing. Any HTTP answer will do, including the 403 an
# unsigned request to the root earns: the question here is only whether
# something is listening and speaking TLS.

if ! curl -sSk -o /dev/null --max-time 10 "https://$HOST:$PORT/" >/dev/null 2>&1; then
    fail "s3-tests: no gate answering at https://$HOST:$PORT — start one with ./scripts/start.sh -w s3tests"
fi

# --- Run ---

RUN="$CACHE/run.txt"
MANIFEST="$CACHE/manifest.txt"
rm -f "$RUN" "$MANIFEST"

# predastore_cleanup.py reads SKIPS itself and does the deselecting, in
# pytest_collection_modifyitems, so that it can drop any node id or marker
# match the committed baseline records as PASS before it ever reaches
# --deselect or -m. See the skip guard note at the top of that file.
BASELINE_COMMITTED="$SCRIPT_DIR/s3-tests-baseline.txt"

log "Running ceph/s3-tests at ${S3TESTS_REF:0:12} against https://$HOST:$PORT"

# CI is cleared deliberately. pytest disables its own assertion truncation when
# it detects a CI environment, and this suite compares whole object bodies, so
# one failed multi-megabyte comparison emits that body four times. The product
# of this run is the manifest and the regression list, not the diffs.
#
# --tb=line for the same reason: several hundred known failures each rendering
# a full traceback through botocore's internals says nothing the manifest does
# not, and buries what does. -rf keeps the one-line failure roll-up.
(
    cd "$CHECKOUT" || exit 2
    PYTHONPATH="$PLUGIN_DIR:$CHECKOUT" \
    PYTHONWARNINGS=ignore \
    CI= \
    S3TEST_CONF="$CONF" \
    PREDA_S3TESTS_MANIFEST="$RUN" \
    PREDA_S3TESTS_SKIPS="$SKIPS" \
    PREDA_S3TESTS_BASELINE_FILE="$BASELINE_COMMITTED" \
        "$VENV/bin/pytest" "${SUITES[@]}" \
            -q --no-header --tb=line -rf \
            -p no:cacheprovider -p predastore_cleanup "$@"
)

if [ ! -s "$RUN" ]; then
    fail "s3-tests: the run produced no manifest — the suite did not start"
fi

"$VENV/bin/python" "$PLUGIN_DIR/manifest.py" merge "$RUN" "$SKIPS" "$MANIFEST" \
    || fail "s3-tests: could not build the manifest"

# --- Baseline ---

if [ -n "$WRITE_BASELINE" ]; then
    cp "$MANIFEST" "$WRITE_BASELINE" || fail "s3-tests: could not write $WRITE_BASELINE"
    log "Baseline written to $WRITE_BASELINE"
    exit 0
fi

if [ -z "$BASELINE_FILE" ]; then
    "$VENV/bin/python" - "$MANIFEST" <<'PY'
import sys
from collections import Counter
counts = Counter(line.split('|', 1)[0] for line in open(sys.argv[1])
                 if line.strip() and not line.startswith('#'))
print('%d cases: %d pass, %d fail, %d error, %d skip' % (
    sum(counts.values()), counts['PASS'], counts['FAIL'],
    counts['ERROR'], counts['SKIP']))
PY
    exit 0
fi

STRICT_ARG=()
[ "$STRICT" = "1" ] && STRICT_ARG+=(--strict)

"$VENV/bin/python" "$PLUGIN_DIR/manifest.py" compare "$BASELINE_FILE" "$MANIFEST" "${STRICT_ARG[@]}"
status=$?

if [ $status -eq 0 ]; then
    printf '%s✓%s no regression against %s\n' "$GREEN" "$NC" "$BASELINE_FILE"
else
    printf '%s✗%s conformance moved against %s\n' "$RED" "$NC" "$BASELINE_FILE"
fi
exit $status
