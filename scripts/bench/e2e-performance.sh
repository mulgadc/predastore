#!/usr/bin/env bash
#
# e2e-performance.sh - Correctness round trips followed by isolated Warp workloads.
#
# Each profile is started, proved correct with the AWS CLI, measured with Warp,
# and stopped before the next one starts, so the profiles never contend.
#
# Usage:
#   ./scripts/bench/e2e-performance.sh          # or: make e2e-performance
#
# Environment:
#   PERF_PRESET      smoke (30s per workload, default) or compare (2m)
#   PERF_CONFIGS     Profiles to run, space separated (default: "1host 3host")
#   PERF_RESULTS_ROOT Where runs are written
#   PERF_KEEP_WORK   1 to keep the cluster work directory after the run
#   PERF_PORT_OFFSET Added to every node port, so a run does not collide with a
#                    cluster already on the defaults (default: 10000)
#   WARP             Path to the warp binary (default: bin/tools/warp)
#   PERF_EXTERNAL_HOSTS Measure a cluster this script did not start, as a comma
#                    separated gate list of host:port. Skips the profile, the
#                    port offset and start/stop; the config name is a label.
#   PERF_EXTERNAL_SHA, PERF_EXTERNAL_GO  Provenance for that cluster's build,
#                    which this machine did not produce and cannot read.
#
# Preset overrides: PERF_DURATION, PERF_CONCURRENT, PERF_PUT_SIZE,
# PERF_PART_SIZE, PERF_PARTS, PERF_GET_SIZE, PERF_MIN_FREE_BYTES.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd -P)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
SCRIPTS_DIR="$REPO_DIR/scripts"
CONFIG_DIR="$REPO_DIR/config"
RESULTS_ROOT="${PERF_RESULTS_ROOT:-$SCRIPT_DIR/results/e2e-performance}"
PERF_PRESET="${PERF_PRESET:-smoke}"
PERF_CONFIGS="${PERF_CONFIGS:-1host 3host}"
WARP="${WARP:-$REPO_DIR/bin/tools/warp}"

# shellcheck source=scripts/lib.sh
source "$SCRIPTS_DIR/lib.sh"

ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
REGION="ap-southeast-2"

case "$PERF_PRESET" in
    smoke)
        DURATION="${PERF_DURATION:-30s}"
        CONCURRENT="${PERF_CONCURRENT:-4}"
        PUT_SIZE="${PERF_PUT_SIZE:-1MiB}"
        MULTIPART_SIZE="${PERF_PART_SIZE:-5MiB}"
        MULTIPART_PARTS="${PERF_PARTS:-2}"
        GET_SIZE="${PERF_GET_SIZE:-10MiB}"
        # `put` runs for a fixed duration, not a fixed object count, so this
        # is a floor with headroom over a measured peak, not a prediction: a
        # faster disk writes more in 30s, not less, than what was measured.
        # Measured peak (sum of the three workload buckets, before any
        # cleanup) was ~40 GiB. +30% headroom covers hardware variance and
        # erasure-coded bytes on disk exceeding the client-side throughput
        # the measurement was taken from: 40 * 1.3 = 52 GiB. Dividing by
        # 0.95 keeps the blob engine's 5%-free watermark intact even if a
        # run lands exactly on budget, rather than finishing at 4% free:
        # 52 / 0.95 = 54.74 GiB, rounded up to 56 GiB.
        MIN_FREE_BYTES="${PERF_MIN_FREE_BYTES:-60129542144}" # 56 GiB
        ;;
    compare)
        DURATION="${PERF_DURATION:-2m}"
        CONCURRENT="${PERF_CONCURRENT:-8}"
        PUT_SIZE="${PERF_PUT_SIZE:-64MiB}"
        MULTIPART_SIZE="${PERF_PART_SIZE:-8MiB}"
        MULTIPART_PARTS="${PERF_PARTS:-16}"
        GET_SIZE="${PERF_GET_SIZE:-128MiB}"
        # Same reasoning as smoke, against a measured peak of ~50 GB:
        # 50 * 1.3 = 65 GB, / 0.95 = 68.42 GB, rounded up to 70 GB.
        MIN_FREE_BYTES="${PERF_MIN_FREE_BYTES:-70000000000}" # 70 GB
        ;;
    *)
        echo "unknown PERF_PRESET: $PERF_PRESET (want smoke or compare)" >&2
        exit 2
        ;;
esac

# PERF_EXTERNAL_HOSTS measures a cluster this script did not start: a comma
# separated gate list, as host:port. The workloads, their sizing and the
# analysis are then identical to a local run, which is the point — a bare-metal
# number is only comparable with a loopback one if the same code produced both.
EXTERNAL="${PERF_EXTERNAL_HOSTS:-}"

# git and go describe the tree that built the binary. On an external cluster
# this machine did not build it, so the provenance is supplied rather than read.
REQUIRED="aws diff openssl"
[ -n "$EXTERNAL" ] || REQUIRED="$REQUIRED git go"
for command in $REQUIRED; do
    command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 1; }
done
[ -x "$WARP" ] || { echo "Warp not executable: $WARP (run make warp-install)" >&2; exit 1; }

mkdir -p "$RESULTS_ROOT"
STAMP="$(date -u +%Y-%m-%dT%H%M%SZ)"
RUN_ID="$(printf '%s' "$STAMP" | tr '[:upper:]' '[:lower:]')"
if [ -n "$EXTERNAL" ]; then
    SHA="${PERF_EXTERNAL_SHA:-external}"
else
    SHA="$(git -C "$REPO_DIR" rev-parse --short HEAD)"
fi
RUN_DIR="$RESULTS_ROOT/${STAMP}-${SHA}"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/predastore-e2e-performance.XXXXXX")"

case "$WORK_DIR" in
    ""|/|"${TMPDIR:-/tmp}") echo "refusing unsafe work directory: $WORK_DIR" >&2; exit 1 ;;
esac

mkdir -p "$RUN_DIR/logs" "$RUN_DIR/correctness"
export PREDA_DIR="$WORK_DIR/predastore"

# Profiles are rendered rather than used in place, so a benchmark can run
# beside a cluster already holding the default ports.
PERF_PORT_OFFSET="${PERF_PORT_OFFSET:-10000}"
export PREDA_CONFIG_DIR="$WORK_DIR/config"
mkdir -p "$PREDA_CONFIG_DIR"

# Warp stages multi-MB upload payloads in TMPDIR. Keeping it under the work
# directory stops those competing with the cluster for a small /tmp tmpfs.
export TMPDIR="$WORK_DIR/tmp"
mkdir -p "$PREDA_DIR" "$TMPDIR"

# work_fs_stat reads byte-denominated `df` fields for the filesystem holding
# WORK_DIR. -PB1 gives a single POSIX-format line in 1-byte blocks, so this
# avoids parsing df's human-readable (G/M-suffixed) output. WORK_DIR must
# already exist (mktemp -d above), or df has nothing to report on.
work_fs_stat() {
    df -PB1 "$WORK_DIR" | awk -v f="$1" 'NR==2 {print $f}'
}

WORK_FS_TOTAL_BYTES="$(work_fs_stat 2)"

# check_free_space refuses to start a config without headroom for its
# workload, so a capacity failure is a preflight message before any cluster
# or data directory exists, rather than a Warp error mid-workload.
check_free_space() {
    local avail requirement shortfall mount
    avail="$(work_fs_stat 4)"
    [ -n "$avail" ] || { echo "could not read free space on $WORK_DIR" >&2; exit 1; }
    requirement="$MIN_FREE_BYTES"
    if [ "$avail" -lt "$requirement" ]; then
        shortfall=$((requirement - avail))
        mount="$(df -P "$WORK_DIR" | awk 'NR==2 {print $NF}')"
        echo "insufficient free space for preset '$PERF_PRESET' on $mount ($WORK_DIR): needs $requirement bytes, has $avail bytes, short by $shortfall bytes" >&2
        exit 1
    fi
}

CURRENT_CONFIG=""
cleanup() {
    "$SCRIPTS_DIR/stop.sh" >/dev/null 2>&1 || true
    if [ -n "$CURRENT_CONFIG" ] && [ -d "$PREDA_DIR/$CURRENT_CONFIG/logs" ]; then
        mkdir -p "$RUN_DIR/logs/$CURRENT_CONFIG"
        cp -R "$PREDA_DIR/$CURRENT_CONFIG/logs/." "$RUN_DIR/logs/$CURRENT_CONFIG/" || true
    fi
    if [ "${PERF_KEEP_WORK:-0}" = "1" ]; then
        echo "Work directory retained: $WORK_DIR"
    else
        rm -rf "$WORK_DIR"
    fi
}
trap cleanup EXIT INT TERM

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

aws_s3() {
    AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
    AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
    AWS_DEFAULT_REGION="$REGION" \
    AWS_REQUEST_CHECKSUM_CALCULATION=when_required \
    AWS_RESPONSE_CHECKSUM_VALIDATION=when_required \
    PYTHONWARNINGS=ignore \
        aws --no-cli-pager --no-verify-ssl --endpoint-url "$ENDPOINT" "$@"
}

write_aws_config() {
    printf '%s\n' \
        '[default]' \
        "region = $REGION" \
        's3 =' \
        '    addressing_style = path' \
        '    multipart_threshold = 8MB' \
        '    multipart_chunksize = 5MB' > "$1"
}

# run_correctness proves the data path before anything is timed: a run that is
# fast and wrong is worse than no number at all.
run_correctness() {
    local config_name="$1" output_dir="$2"
    local bucket="predastore-correctness-${config_name}-${RUN_ID}"
    local single_src="$WORK_DIR/single.bin" single_dst="$WORK_DIR/single.out"
    local multipart_src="$WORK_DIR/multipart.bin" multipart_dst="$WORK_DIR/multipart.out"
    local empty_src="$WORK_DIR/empty.bin" empty_dst="$WORK_DIR/empty.out"
    local aws_config="$WORK_DIR/aws-config"

    openssl rand -out "$single_src" "${PERF_CORRECTNESS_SINGLE_BYTES:-1048576}"
    openssl rand -out "$multipart_src" "${PERF_CORRECTNESS_MULTIPART_BYTES:-20971520}"
    : > "$empty_src"
    write_aws_config "$aws_config"
    export AWS_CONFIG_FILE="$aws_config"

    aws_s3 s3 mb "s3://$bucket"

    aws_s3 s3api put-object --bucket "$bucket" --key single.bin --body "$single_src" >/dev/null
    aws_s3 s3 cp "s3://$bucket/single.bin" "$single_dst" --only-show-errors
    diff -q "$single_src" "$single_dst"

    # A zero-length object has no shard to store, so it exercises a path no
    # other case reaches.
    aws_s3 s3api put-object --bucket "$bucket" --key empty.bin --body "$empty_src" >/dev/null
    aws_s3 s3 cp "s3://$bucket/empty.bin" "$empty_dst" --only-show-errors
    diff -q "$empty_src" "$empty_dst"

    # The configured 8 MiB threshold forces this through explicit UploadPart
    # and CompleteMultipartUpload calls rather than a single PUT.
    aws_s3 s3 cp "$multipart_src" "s3://$bucket/multipart.bin" --only-show-errors
    aws_s3 s3 cp "s3://$bucket/multipart.bin" "$multipart_dst" --only-show-errors
    diff -q "$multipart_src" "$multipart_dst"

    mkdir -p "$output_dir"
    {
        echo "single_source=$(sha256_file "$single_src")"
        echo "single_download=$(sha256_file "$single_dst")"
        echo "empty_source=$(sha256_file "$empty_src")"
        echo "empty_download=$(sha256_file "$empty_dst")"
        echo "multipart_source=$(sha256_file "$multipart_src")"
        echo "multipart_download=$(sha256_file "$multipart_dst")"
    } > "$output_dir/sha256.txt"

    aws_s3 s3 rb "s3://$bucket" --force >/dev/null
}

# drop_bucket removes a bucket once its workload has been measured, so the
# next workload's writes don't have to coexist with it on disk. The
# measurement is already recorded by the time this runs, so a failure here
# is logged and swallowed rather than failing the run.
drop_bucket() {
    local bucket="$1"
    aws_s3 s3 rb "s3://$bucket" --force >/dev/null 2>&1 ||
        echo "warning: failed to drop bucket $bucket; continuing" >&2
}

# run_warp measures each workload on its own. A mixed workload cannot say which
# path regressed, which is the whole question a before/after run is asked.
run_warp() {
    local config_name="$1" output_dir="$2"
    local part_concurrent="$CONCURRENT"
    if [ "$part_concurrent" -gt "$MULTIPART_PARTS" ]; then
        part_concurrent="$MULTIPART_PARTS"
    fi
    local common=(
        --host="$HOST_LIST"
        --tls
        --insecure
        --region="$REGION"
        --access-key="$ACCESS_KEY"
        --secret-key="$SECRET_KEY"
        --duration="$DURATION"
        --concurrent="$CONCURRENT"
        --no-color
        --noclear
        --full
    )

    local put_bucket="warp-${config_name}-put-${RUN_ID}"
    local multipart_bucket="warp-${config_name}-multipart-put-${RUN_ID}"
    local get_bucket="warp-${config_name}-get-${RUN_ID}"

    mkdir -p "$output_dir"
    run_warp_checked "$output_dir/put.log" "$WARP" put "${common[@]}" \
        --disable-multipart --obj.size="$PUT_SIZE" \
        --bucket="$put_bucket" --benchdata="$output_dir/put"
    # Each workload owns its bucket, so dropping it here cannot affect the
    # multipart-put or get workloads that follow. This is what keeps the
    # config's peak disk usage to the largest single workload rather than
    # the sum of all three.
    drop_bucket "$put_bucket"

    run_warp_checked "$output_dir/multipart-put.log" "$WARP" multipart-put "${common[@]}" \
        --part.size="$MULTIPART_SIZE" --parts="$MULTIPART_PARTS" \
        --part.concurrent="$part_concurrent" --bucket="$multipart_bucket" \
        --benchdata="$output_dir/multipart-put"
    drop_bucket "$multipart_bucket"

    # The GET set is seeded by multipart upload and then read whole. Warp's
    # `multipart` command reads with GET ?partNumber=N, a separate S3 feature
    # predastore does not implement.
    run_warp_checked "$output_dir/get.log" "$WARP" get "${common[@]}" \
        --objects=16 --obj.size="$GET_SIZE" --part.size="$MULTIPART_SIZE" \
        --bucket="$get_bucket" --benchdata="$output_dir/get"
    drop_bucket "$get_bucket"

    write_warp_analysis "$output_dir"
}

write_warp_analysis() {
    local output_dir="$1" workload artifact
    for workload in put multipart-put get; do
        artifact="$(find "$output_dir" -maxdepth 1 -type f \
            \( -name "${workload}.json.zst" -o -name "${workload}.csv.zst" \) -print -quit)"
        [ -n "$artifact" ] || { echo "missing Warp artifact for $workload" >&2; return 1; }
        "$WARP" analyze --no-color --analyze.v "$artifact" > "$output_dir/${workload}-latency.txt"
    done
}

# rejected_completions counts the multipart completions the gates in $1 have
# refused, or only those that had no part stored when $2 is "empty". The gates
# append for the life of the profile, so one workload's count is a difference.
rejected_completions() {
    local dir="$1" pattern='Multipart completion rejected'
    if [ "${2:-}" = "empty" ]; then
        pattern="$pattern"'.*"stored":0[,}]'
    fi
    { grep -hE "$pattern" "$dir"/*.log 2>/dev/null || true; } | wc -l
}

run_warp_checked() {
    local log_file="$1"
    shift
    # An external cluster's gate logs are on other machines, so the counts stay
    # zero and the partless tolerance below cannot fire. That fails such a run
    # rather than excusing it, which is the safe direction.
    local logs="$PREDA_DIR/$CURRENT_CONFIG/logs"
    local status errors partless refused unstored

    refused="$(rejected_completions "$logs")"
    unstored="$(rejected_completions "$logs" empty)"

    set +e
    "$@" 2>&1 | tee "$log_file"
    status="${PIPESTATUS[0]}"
    set -e

    refused=$(($(rejected_completions "$logs") - refused))
    unstored=$(($(rejected_completions "$logs" empty) - unstored))

    # Warp reports some failures in its output while still exiting zero, so the
    # log is checked as well as the status.
    errors="$(grep -c 'warp: <ERROR>' "$log_file" || true)"
    partless="$(grep -c 'warp: <ERROR> complete multipart upload' "$log_file" || true)"

    # Warp completes an upload it never put a part into when its duration
    # expires in between, and a gate is right to refuse that. Tolerated only
    # when every error is that one and the gates agree no part was stored.
    if [ "$status" -eq 0 ] && [ "$errors" -gt 0 ] && [ "$errors" -eq "$partless" ] &&
        [ "$refused" -eq "$errors" ] && [ "$unstored" -eq "$refused" ]; then
        echo "Tolerated $errors completion(s) of an upload with no part; see $log_file" >&2
        return 0
    fi

    if [ "$status" -ne 0 ] || [ "$errors" -gt 0 ]; then
        echo "Warp workload failed; see $log_file" >&2
        return 1
    fi
}

DIRTY="false"
if [ -n "$EXTERNAL" ]; then
    PREDASTORE_SHA="${PERF_EXTERNAL_SHA:-unknown}"
    GO_VERSION="${PERF_EXTERNAL_GO:-unknown}"
else
    [ -z "$(git -C "$REPO_DIR" status --porcelain --untracked-files=no)" ] || DIRTY="true"
    PREDASTORE_SHA="$(git -C "$REPO_DIR" rev-parse HEAD)"
    GO_VERSION="$(go version)"
fi
{
    echo "Predastore end-to-end performance run"
    echo "===================================="
    echo
    echo "Run identity"
    echo "------------"
    echo "date_utc=$STAMP"
    echo "predastore_sha=$PREDASTORE_SHA"
    echo "predastore_dirty=$DIRTY"
    echo "go_version=$GO_VERSION"
    [ -z "$EXTERNAL" ] || echo "external_hosts=$EXTERNAL"
    echo "warp_version=$($WARP --version 2>&1 | head -n 1)"
    echo "warp_module_version=${WARP_VERSION:-unknown}"
    echo "host=$(hostname)"
    echo "os=$(uname -s)"
    echo "arch=$(uname -m)"
    echo "cpu=$(uname -p)"
    echo "logical_cpus=$(getconf _NPROCESSORS_ONLN)"
    echo "memory_bytes=$(awk '/MemTotal/ {print $2 * 1024}' /proc/meminfo 2>/dev/null || echo unknown)"
    echo
    echo "Work filesystem"
    echo "---------------"
    echo "work_dir=$WORK_DIR"
    echo "work_fs_avail_bytes=$(work_fs_stat 4)"
    echo "work_fs_total_bytes=$WORK_FS_TOTAL_BYTES"
    echo "min_free_bytes_required=$MIN_FREE_BYTES"
    echo
    echo "Workload controls"
    echo "-----------------"
    echo "preset=$PERF_PRESET"
    echo "duration=$DURATION"
    echo "concurrent=$CONCURRENT"
    echo "put_size=$PUT_SIZE"
    echo "multipart_part_size=$MULTIPART_SIZE"
    echo "multipart_parts=$MULTIPART_PARTS"
    echo "get_object_size=$GET_SIZE"
    echo "configs=$PERF_CONFIGS"
    echo "port_offset=$PERF_PORT_OFFSET"
    echo "full_request_samples=true"
    echo "correctness=AWS_CLI_PUT,empty_object,multipart_upload,GET,diff,SHA256"
    echo
    echo "Outputs"
    echo "-------"
    echo "raw_samples=<config>/<workload>.json.zst"
    echo "latency_reports=<config>/<workload>-latency.txt"
    echo "client_logs=<config>/<workload>.log"
    echo "server_logs=logs/<config>/"
    echo "correctness_hashes=correctness/<config>/sha256.txt"
} > "$RUN_DIR/run-info.txt"

for config_name in $PERF_CONFIGS; do
    if [ -n "$EXTERNAL" ]; then
        # The cluster is already running somewhere else, so the profile, the
        # port offset and the start/stop below have nothing to act on. The
        # config name is only a label for the results directory here.
        HOST_LIST="$EXTERNAL"
        ENDPOINT="https://${EXTERNAL%%,*}"
        echo "Measuring external $config_name ($HOST_LIST)"
    else
        [ -f "$CONFIG_DIR/$config_name.toml" ] || { echo "missing config: $config_name" >&2; exit 1; }
        CONFIG_FILE="$PREDA_CONFIG_DIR/$config_name.toml"
        render_profile "$CONFIG_DIR/$config_name.toml" "$CONFIG_FILE" "$PERF_PORT_OFFSET"

        # The profile decides where S3 answers, so the endpoints are read from it
        # rather than assumed to be one gate on the default port.
        HOST_LIST="$(gate_endpoints "$CONFIG_FILE" | paste -sd,)"
        [ -n "$HOST_LIST" ] || { echo "no host in $config_name runs a gate" >&2; exit 1; }
        ENDPOINT="https://$(gate_endpoints "$CONFIG_FILE" | head -1)"

        # Only the cluster this script starts writes to this box's disk. A
        # cluster named by PERF_EXTERNAL_HOSTS stores its data elsewhere, so
        # the free space here says nothing about whether that run can proceed.
        check_free_space

        CURRENT_CONFIG="$config_name"
        echo "Starting $config_name ($HOST_LIST)"
        "$SCRIPTS_DIR/start.sh" -w "$config_name"
        cp "$CONFIG_FILE" "$RUN_DIR/$config_name.toml"
    fi

    run_correctness "$config_name" "$RUN_DIR/correctness/$config_name"
    run_warp "$config_name" "$RUN_DIR/$config_name"

    {
        echo
        echo "Latency reports: $config_name"
        echo "---------------------------"
        for report in "$RUN_DIR/$config_name"/*-latency.txt; do
            echo
            echo "### $(basename "$report")"
            sed 's/^/  /' "$report"
        done
    } >> "$RUN_DIR/run-info.txt"

    if [ -z "$EXTERNAL" ]; then
        "$SCRIPTS_DIR/stop.sh"
        if [ -d "$PREDA_DIR/$config_name/logs" ]; then
            mkdir -p "$RUN_DIR/logs/$config_name"
            cp -R "$PREDA_DIR/$config_name/logs/." "$RUN_DIR/logs/$config_name/"
        fi
        rm -rf "${PREDA_DIR:?}/$config_name"
        CURRENT_CONFIG=""
    fi
done

echo "Performance results: $RUN_DIR"
