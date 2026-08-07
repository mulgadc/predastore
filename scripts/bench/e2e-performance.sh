#!/usr/bin/env bash
# Run deterministic S3 round trips followed by isolated Warp workloads.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
SCRIPTS_DIR="$REPO_DIR/scripts"
CONFIG_DIR="$REPO_DIR/config"
RESULTS_ROOT="${PERF_RESULTS_ROOT:-$SCRIPT_DIR/results/e2e-performance}"
PERF_PRESET="${PERF_PRESET:-smoke}"
PERF_CONFIGS="${PERF_CONFIGS:-1node 4node}"
WARP="${WARP:-$REPO_DIR/bin/tools/warp}"
ENDPOINT="https://127.0.0.1:8443"

ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
REGION="ap-southeast-2"

case "$PERF_PRESET" in
    smoke)
        DURATION="${PERF_DURATION:-10s}"
        CONCURRENT="${PERF_CONCURRENT:-2}"
        PUT_SIZE="${PERF_PUT_SIZE:-1MiB}"
        MULTIPART_SIZE="${PERF_PART_SIZE:-5MiB}"
        MULTIPART_PARTS="${PERF_PARTS:-2}"
        ;;
    compare)
        DURATION="${PERF_DURATION:-60s}"
        CONCURRENT="${PERF_CONCURRENT:-8}"
        PUT_SIZE="${PERF_PUT_SIZE:-64MiB}"
        MULTIPART_SIZE="${PERF_PART_SIZE:-8MiB}"
        MULTIPART_PARTS="${PERF_PARTS:-16}"
        ;;
    *)
        echo "unknown PERF_PRESET: $PERF_PRESET (want smoke or compare)" >&2
        exit 2
        ;;
esac

for command in aws curl diff git go openssl; do
    command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 1; }
done
[ -x "$WARP" ] || { echo "Warp not executable: $WARP (run make warp-install)" >&2; exit 1; }

mkdir -p "$RESULTS_ROOT"
STAMP="$(date -u +%Y-%m-%dT%H%M%SZ)"
RUN_ID="$(printf '%s' "$STAMP" | tr '[:upper:]' '[:lower:]')"
SHA="$(git -C "$REPO_DIR" rev-parse --short HEAD)"
RUN_DIR="$RESULTS_ROOT/${STAMP}-${SHA}"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/predastore-e2e-performance.XXXXXX")"

case "$WORK_DIR" in
    ""|/|"${TMPDIR:-/tmp}") echo "refusing unsafe work directory: $WORK_DIR" >&2; exit 1 ;;
esac

mkdir -p "$RUN_DIR/logs" "$RUN_DIR/correctness"
export PREDA_DIR="$WORK_DIR/predastore"
export TMPDIR="$WORK_DIR/tmp"
mkdir -p "$PREDA_DIR" "$TMPDIR"

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
    local path="$1"
    printf '%s\n' \
        '[default]' \
        "region = $REGION" \
        's3 =' \
        '    addressing_style = path' \
        '    multipart_threshold = 8MB' \
        '    multipart_chunksize = 5MB' > "$path"
}

run_correctness() {
    local config_name="$1" output_dir="$2"
    local bucket="predastore-correctness-${config_name}-${RUN_ID}"
    local single_src="$WORK_DIR/single.bin" single_dst="$WORK_DIR/single.out"
    local multipart_src="$WORK_DIR/multipart.bin" multipart_dst="$WORK_DIR/multipart.out"
    local aws_config="$WORK_DIR/aws-config"

    openssl rand -out "$single_src" "${PERF_CORRECTNESS_SINGLE_BYTES:-1048576}"
    openssl rand -out "$multipart_src" "${PERF_CORRECTNESS_MULTIPART_BYTES:-20971520}"
    write_aws_config "$aws_config"
    export AWS_CONFIG_FILE="$aws_config"

    aws_s3 s3 mb "s3://$bucket"
    aws_s3 s3api put-object --bucket "$bucket" --key single.bin --body "$single_src" >/dev/null
    aws_s3 s3 cp "s3://$bucket/single.bin" "$single_dst" --only-show-errors
    diff -q "$single_src" "$single_dst"

    # AWS CLI's configured 8 MiB threshold forces this path through explicit
    # multipart UploadPart and CompleteMultipartUpload operations.
    aws_s3 s3 cp "$multipart_src" "s3://$bucket/multipart.bin" --only-show-errors
    aws_s3 s3 cp "s3://$bucket/multipart.bin" "$multipart_dst" --only-show-errors
    diff -q "$multipart_src" "$multipart_dst"

    mkdir -p "$output_dir"
    {
        echo "single_source=$(sha256_file "$single_src")"
        echo "single_download=$(sha256_file "$single_dst")"
        echo "multipart_source=$(sha256_file "$multipart_src")"
        echo "multipart_download=$(sha256_file "$multipart_dst")"
    } > "$output_dir/sha256.txt"
    aws_s3 s3 rb "s3://$bucket" --force >/dev/null
}

run_warp() {
    local config_name="$1" output_dir="$2"
    local common=(
        --host=127.0.0.1:8443
        --tls
        --insecure
        --region="$REGION"
        --lookup=path
        --access-key="$ACCESS_KEY"
        --secret-key="$SECRET_KEY"
        --duration="$DURATION"
        --concurrent="$CONCURRENT"
        --no-color
        --noclear
    )

    mkdir -p "$output_dir"
    run_warp_checked "$output_dir/put.log" "$WARP" put "${common[@]}" \
        --disable-multipart --obj.size="$PUT_SIZE" \
        --bucket="warp-${config_name}-put-${RUN_ID}" --benchdata="$output_dir/put"
    run_warp_checked "$output_dir/multipart-put.log" "$WARP" multipart-put "${common[@]}" \
        --part.size="$MULTIPART_SIZE" --parts="$MULTIPART_PARTS" \
        --part.concurrent="$CONCURRENT" --bucket="warp-${config_name}-multipart-put-${RUN_ID}" \
        --benchdata="$output_dir/multipart-put"
    run_warp_checked "$output_dir/multipart.log" "$WARP" multipart "${common[@]}" \
        --part.size="$MULTIPART_SIZE" --parts="$MULTIPART_PARTS" \
        --bucket="warp-${config_name}-multipart-${RUN_ID}" --benchdata="$output_dir/multipart"
    run_warp_checked "$output_dir/get.log" "$WARP" get "${common[@]}" \
        --objects=16 --obj.size="$PUT_SIZE" \
        --bucket="warp-${config_name}-get-${RUN_ID}" --benchdata="$output_dir/get"
}

run_warp_checked() {
    local log_file="$1"
    shift
    local status

    set +e
    "$@" 2>&1 | tee "$log_file"
    status="${PIPESTATUS[0]}"
    set -e

    if [ "$status" -ne 0 ] || grep -q 'warp: <ERROR>' "$log_file"; then
        echo "Warp workload failed; see $log_file" >&2
        return 1
    fi
}

DIRTY="false"
[ -z "$(git -C "$REPO_DIR" status --porcelain --untracked-files=no)" ] || DIRTY="true"
{
    echo "date=$STAMP"
    echo "predastore_sha=$(git -C "$REPO_DIR" rev-parse HEAD)"
    echo "predastore_dirty=$DIRTY"
    echo "go_version=$(go version)"
    echo "warp_version=$($WARP --version 2>&1 | head -n 1)"
    echo "host=$(hostname)"
    echo "os=$(uname -s)"
    echo "arch=$(uname -m)"
    echo "preset=$PERF_PRESET"
    echo "duration=$DURATION"
    echo "concurrent=$CONCURRENT"
    echo "put_size=$PUT_SIZE"
    echo "multipart_part_size=$MULTIPART_SIZE"
    echo "multipart_parts=$MULTIPART_PARTS"
    echo "configs=$PERF_CONFIGS"
} > "$RUN_DIR/run-info.txt"

for config_name in $PERF_CONFIGS; do
    [ -f "$CONFIG_DIR/$config_name.toml" ] || { echo "missing config: $config_name" >&2; exit 1; }
    CURRENT_CONFIG="$config_name"
    echo "Starting $config_name"
    "$SCRIPTS_DIR/start.sh" -w "$config_name"
    cp "$CONFIG_DIR/$config_name.toml" "$RUN_DIR/$config_name.toml"

    run_correctness "$config_name" "$RUN_DIR/correctness/$config_name"
    run_warp "$config_name" "$RUN_DIR/$config_name"

    "$SCRIPTS_DIR/stop.sh"
    if [ -d "$PREDA_DIR/$config_name/logs" ]; then
        mkdir -p "$RUN_DIR/logs/$config_name"
        cp -R "$PREDA_DIR/$config_name/logs/." "$RUN_DIR/logs/$config_name/"
    fi
    rm -rf "$PREDA_DIR/$config_name"
    CURRENT_CONFIG=""
done

echo "Performance results: $RUN_DIR"
