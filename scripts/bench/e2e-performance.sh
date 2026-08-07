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
        DURATION="${PERF_DURATION:-30s}"
        CONCURRENT="${PERF_CONCURRENT:-4}"
        PUT_SIZE="${PERF_PUT_SIZE:-1MiB}"
        MULTIPART_SIZE="${PERF_PART_SIZE:-5MiB}"
        MULTIPART_PARTS="${PERF_PARTS:-2}"
        GET_SIZE="${PERF_GET_SIZE:-10MiB}"
        ;;
    compare)
        DURATION="${PERF_DURATION:-2m}"
        CONCURRENT="${PERF_CONCURRENT:-8}"
        PUT_SIZE="${PERF_PUT_SIZE:-64MiB}"
        MULTIPART_SIZE="${PERF_PART_SIZE:-8MiB}"
        MULTIPART_PARTS="${PERF_PARTS:-16}"
        GET_SIZE="${PERF_GET_SIZE:-128MiB}"
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
    local part_concurrent="$CONCURRENT"
    if [ "$part_concurrent" -gt "$MULTIPART_PARTS" ]; then
        part_concurrent="$MULTIPART_PARTS"
    fi
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
        --full
    )

    mkdir -p "$output_dir"
    run_warp_checked "$output_dir/put.log" "$WARP" put "${common[@]}" \
        --disable-multipart --obj.size="$PUT_SIZE" \
        --bucket="warp-${config_name}-put-${RUN_ID}" --benchdata="$output_dir/put"
    run_warp_checked "$output_dir/multipart-put.log" "$WARP" multipart-put "${common[@]}" \
        --part.size="$MULTIPART_SIZE" --parts="$MULTIPART_PARTS" \
        --part.concurrent="$part_concurrent" --bucket="warp-${config_name}-multipart-put-${RUN_ID}" \
        --benchdata="$output_dir/multipart-put"
    # Seed the GET workload through multipart upload, then measure complete
    # object GETs. Warp's separate `multipart` command uses GET ?partNumber=N,
    # an independent S3 API feature Predastore does not currently implement.
    run_warp_checked "$output_dir/get.log" "$WARP" get "${common[@]}" \
        --objects=16 --obj.size="$GET_SIZE" --part.size="$MULTIPART_SIZE" \
        --bucket="warp-${config_name}-get-${RUN_ID}" --benchdata="$output_dir/get"

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

write_access_analysis() {
    local log_file="$1" output_file="$2"
    local samples_file="$output_file.samples" values_file="$output_file.values"
    local operation count p50_pos p90_pos p99_pos

    sed -n 's/.*"operation":"\([^"]*\)".*"duration_us":\([0-9][0-9]*\).*/\1 \2/p' \
        "$log_file" > "$samples_file"
    {
        echo "Server request latency (microseconds)"
        echo "operation count avg_us p50_us p90_us p99_us min_us max_us"
        awk '{print $1}' "$samples_file" | sort -u | while IFS= read -r operation; do
            awk -v op="$operation" '$1 == op {print $2}' "$samples_file" | sort -n > "$values_file"
            count="$(wc -l < "$values_file" | tr -d ' ')"
            [ "$count" -gt 0 ] || continue
            p50_pos=$(( (count * 50 + 99) / 100 ))
            p90_pos=$(( (count * 90 + 99) / 100 ))
            p99_pos=$(( (count * 99 + 99) / 100 ))
            printf '%s %s %s %s %s %s %s %s\n' \
                "$operation" "$count" \
                "$(awk '{sum += $1} END {printf "%.0f", sum / NR}' "$values_file")" \
                "$(sed -n "${p50_pos}p" "$values_file")" \
                "$(sed -n "${p90_pos}p" "$values_file")" \
                "$(sed -n "${p99_pos}p" "$values_file")" \
                "$(sed -n '1p' "$values_file")" \
                "$(sed -n '$p' "$values_file")"
        done
    } > "$output_file"
    rm -f "$samples_file" "$values_file"
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
    echo "Predastore end-to-end performance run"
    echo "===================================="
    echo
    echo "Run identity"
    echo "------------"
    echo "date_utc=$STAMP"
    echo "predastore_sha=$(git -C "$REPO_DIR" rev-parse HEAD)"
    echo "predastore_dirty=$DIRTY"
    echo "go_version=$(go version)"
    echo "warp_version=$($WARP --version 2>&1 | head -n 1)"
    echo "warp_module_version=${WARP_VERSION:-unknown}"
    echo "host=$(hostname)"
    echo "os=$(uname -s)"
    echo "arch=$(uname -m)"
    echo "cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -p)"
    echo "logical_cpus=$(sysctl -n hw.logicalcpu 2>/dev/null || getconf _NPROCESSORS_ONLN)"
    echo "memory_bytes=$(sysctl -n hw.memsize 2>/dev/null || echo unknown)"
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
    echo "full_request_samples=true"
    echo "latency_resolution=nanoseconds_in_artifact; milliseconds_and_microseconds_in_reports"
    echo "correctness=AWS_CLI_PUT,multipart_upload,GET,diff,SHA256"
    echo
    echo "Outputs"
    echo "-------"
    echo "raw_samples=<config>/<workload>.json.zst"
    echo "latency_reports=<config>/<workload>-latency.txt"
    echo "server_latency_report=<config>/server-latency-us.txt"
    echo "client_logs=<config>/<workload>.log"
    echo "server_access_logs=logs/<config>/colo.log"
    echo "correctness_hashes=correctness/<config>/sha256.txt"
} > "$RUN_DIR/run-info.txt"

for config_name in $PERF_CONFIGS; do
    [ -f "$CONFIG_DIR/$config_name.toml" ] || { echo "missing config: $config_name" >&2; exit 1; }
    CURRENT_CONFIG="$config_name"
    echo "Starting $config_name"
    "$SCRIPTS_DIR/start.sh" -w "$config_name"
    cp "$CONFIG_DIR/$config_name.toml" "$RUN_DIR/$config_name.toml"

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

    "$SCRIPTS_DIR/stop.sh"
    if [ -d "$PREDA_DIR/$config_name/logs" ]; then
        mkdir -p "$RUN_DIR/logs/$config_name"
        cp -R "$PREDA_DIR/$config_name/logs/." "$RUN_DIR/logs/$config_name/"
    fi
    write_access_analysis "$RUN_DIR/logs/$config_name/colo.log" \
        "$RUN_DIR/$config_name/server-latency-us.txt"
    {
        echo
        echo "Server latency: $config_name"
        echo "-------------------------"
        sed 's/^/  /' "$RUN_DIR/$config_name/server-latency-us.txt"
    } >> "$RUN_DIR/run-info.txt"
    rm -rf "$PREDA_DIR/$config_name"
    CURRENT_CONFIG=""
done

echo "Performance results: $RUN_DIR"
