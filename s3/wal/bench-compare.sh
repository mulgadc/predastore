#!/usr/bin/env bash
set -euo pipefail

# Compare WAL Go benchmarks vs native dd write/read throughput.
#
# This script is intentionally "dd-like": it measures sequential write/read rates
# for a sweep of record sizes.
#
# WAL side:
#   - uses `go test -bench BenchmarkWAL_...` (WAL opens files with O_SYNC)
#   - uses the same sizes + record-count logic as `wal_bench_test.go`
#
# dd side:
#   - writes a single file per size with O_SYNC semantics (best-effort via oflag=sync)
#   - reads the same file back into /dev/null
#
# Usage:
#   ./bench-compare.sh
#   BENCHTIME=5s ./bench-compare.sh
#   SHORT=1 ./bench-compare.sh
#
# Notes:
# - OS page cache will affect read numbers. Dropping cache is OS-specific and not done here.
# - On macOS, dd feature flags vary; the script probes for supported oflag.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WAL_DIR="$ROOT_DIR/s3/wal"

BENCHTIME="${BENCHTIME:-2s}"
SHORT="${SHORT:-0}"

# Where to write outputs. (Default: s3/wal/bench-compare-<timestamp>.*)
TS="$(date +%Y%m%d-%H%M%S)"
OUT_PREFIX="${OUT_PREFIX:-$WAL_DIR/bench-compare-$TS}"
GO_LOG="${OUT_PREFIX}.go.txt"
DD_LOG="${OUT_PREFIX}.dd.txt"
REPORT="${OUT_PREFIX}.report.txt"

# Temporary workspace (avoid `set -u` trap + local-variable issues)
WORKDIR=""
cleanup() {
  if [[ -n "${WORKDIR:-}" && -d "${WORKDIR:-}" ]]; then
    rm -rf "$WORKDIR"
  fi
}
trap cleanup EXIT

# Sizes match s3/wal/wal_bench_test.go full sweep.
SIZES=(
  1024
  2048
  4096
  8192
  16384
  32768
  65536
  131072
  262144
  524288
  1048576
  2097152
  4194304
  8388608
)

if [[ "$SHORT" == "1" ]]; then
  # Mirrors the -short selection in benchSizes()
  SIZES=(4096 65536 1048576 8388608)
fi

human_bytes() {
  local n="$1"
  if (( n % (1024*1024) == 0 )); then
    printf "%dMiB" $(( n / (1024*1024) ))
  elif (( n % 1024 == 0 )); then
    printf "%dKiB" $(( n / 1024 ))
  else
    printf "%dB" "$n"
  fi
}

# Match benchRecordsForSize(recordSize) from wal_bench_test.go:
# records := (16<<20)/recordSize; min 2; max 256
records_for_size() {
  local sz="$1"
  local records=$(( (16*1024*1024) / sz ))
  if (( records < 2 )); then records=2; fi
  if (( records > 256 )); then records=256; fi
  printf "%d" "$records"
}

require_cmd() {
  local c="$1"
  command -v "$c" >/dev/null 2>&1 || { echo "error: missing required command: $c" >&2; exit 1; }
}

size_label_to_bytes() {
  # Inputs like: 1KiB 2KiB 512KiB 1MiB 8MiB
  local s="$1"
  if [[ "$s" =~ ^([0-9]+)KiB$ ]]; then
    echo $(( BASH_REMATCH[1] * 1024 ))
    return 0
  fi
  if [[ "$s" =~ ^([0-9]+)MiB$ ]]; then
    echo $(( BASH_REMATCH[1] * 1024 * 1024 ))
    return 0
  fi
  if [[ "$s" =~ ^([0-9]+)B$ ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  echo "0"
}

pick_dd_sync_flag() {
  # Probe dd support for oflag=sync / oflag=dsync.
  local tmp
  tmp="$(mktemp -t dd-sync-probe.XXXXXX)"
  rm -f "$tmp"

  if dd if=/dev/zero of="$tmp" bs=1 count=1 oflag=sync >/dev/null 2>&1; then
    rm -f "$tmp"
    echo "oflag=sync"
    return 0
  fi

  if dd if=/dev/zero of="$tmp" bs=1 count=1 oflag=dsync >/dev/null 2>&1; then
    rm -f "$tmp"
    echo "oflag=dsync"
    return 0
  fi

  rm -f "$tmp"
  echo ""
}

parse_go_to_tsv() {
  # stdin: go benchmark output
  # stdout: TSV rows: op<TAB>size_bytes<TAB>mbps_decimal
  # Example line:
  # BenchmarkWAL_Write/write/8KiB-8  19533  111031 ns/op  73.78 MB/s ...
  awk '
    function size_to_bytes(lbl, n) {
      if (lbl ~ /KiB$/) { n = substr(lbl, 1, length(lbl)-3) + 0; return n * 1024 }
      if (lbl ~ /MiB$/) { n = substr(lbl, 1, length(lbl)-3) + 0; return n * 1024 * 1024 }
      if (lbl ~ /B$/)   { n = substr(lbl, 1, length(lbl)-1) + 0; return n }
      return 0
    }
    {
      if (index($1, "BenchmarkWAL_Write/write/") == 1) op = "write"
      else if (index($1, "BenchmarkWAL_Read/read/") == 1) op = "read"
      else next

      # Extract size label: BenchmarkWAL_Write/write/8KiB-8 -> 8KiB
      n = split($1, parts, "/")
      sizepart = parts[n]
      sub(/-[0-9]+$/, "", sizepart)
      bytes = size_to_bytes(sizepart)

      mbps = ""
      for (i = 1; i <= NF; i++) {
        if ($i == "MB/s" && i > 1) {
          mbps = $(i-1)
        }
      }

      if (op != "" && bytes > 0 && mbps != "") {
        printf "%s\t%d\t%s\n", op, bytes, mbps
      }
    }
  '
}

parse_dd_line_to_mbps() {
  # Parse dd output line and print MB/s (decimal, 1e6 bytes) as a float.
  # macOS dd example:
  #   16777216 bytes transferred in 0.235123 secs (71351786 bytes/sec)
  # GNU dd example:
  #   16777216 bytes (17 MB, 16 MiB) copied, 0.235123 s, 71.4 MB/s
  local line="$1"
  # Prefer bytes/sec in parentheses.
  if [[ "$line" =~ \(([0-9,]+)[[:space:]]bytes/sec\) ]]; then
    local bps="${BASH_REMATCH[1]//,/}"
    awk -v bps="$bps" 'BEGIN { printf "%.2f", (bps/1000000.0) }'
    return 0
  fi
  # Fallback: capture trailing "<num> MB/s" (GNU dd style)
  if [[ "$line" =~ ([0-9.]+)[[:space:]]MB/s ]]; then
    printf "%s" "${BASH_REMATCH[1]}"
    return 0
  fi
  echo ""
}

run_wal_bench() {
  echo "== WAL (Go benchmark, O_SYNC) ==" | tee -a "$GO_LOG" >/dev/null
  echo "benchtime=$BENCHTIME" | tee -a "$GO_LOG" >/dev/null
  echo | tee -a "$GO_LOG" >/dev/null

  local short_flag=()
  if [[ "$SHORT" == "1" ]]; then
    short_flag=(-short)
  fi

  (cd "$ROOT_DIR" && go test ./s3/wal "${short_flag[@]}" -run '^$' \
    -bench '^(BenchmarkWAL_Write|BenchmarkWAL_Read)$' \
    -benchtime "$BENCHTIME" -benchmem) | tee -a "$GO_LOG"
}

run_dd_bench() {
  echo "== dd (filesystem) ==" | tee -a "$DD_LOG" >/dev/null

  local dd_sync
  dd_sync="$(pick_dd_sync_flag)"
  if [[ -z "$dd_sync" ]]; then
    echo "warning: your dd does not support oflag=sync/dsync; proceeding without O_SYNC flag" | tee -a "$DD_LOG" >&2
  else
    echo "dd sync flag: $dd_sync" | tee -a "$DD_LOG" >/dev/null
  fi

  WORKDIR="$(mktemp -d -t wal-bench-compare.XXXXXX)"
  local dd_work="$WORKDIR/dd"
  mkdir -p "$dd_work"

  echo "workdir: $dd_work" | tee -a "$DD_LOG" >/dev/null
  echo | tee -a "$DD_LOG" >/dev/null

  for sz in "${SIZES[@]}"; do
    local rec total out
    rec="$(records_for_size "$sz")"
    total=$(( sz * rec ))
    out="$dd_work/dd-$(human_bytes "$sz").bin"

    echo "-- size=$(human_bytes "$sz") records=$rec total=$total bytes --" | tee -a "$DD_LOG" >/dev/null

    # Write
    # Use /dev/zero to avoid CPU-bound generation; focus on IO.
    # Capture dd output (it includes bytes/sec).
    local wline rline
    if [[ -n "$dd_sync" ]]; then
      wline="$(dd if=/dev/zero of="$out" bs="$sz" count="$rec" $dd_sync 2>&1 | tail -n 1)"
    else
      wline="$(dd if=/dev/zero of="$out" bs="$sz" count="$rec" 2>&1 | tail -n 1)"
    fi
    echo "$wline" | tee -a "$DD_LOG" >/dev/null

    # Read
    rline="$(dd if="$out" of=/dev/null bs="$sz" count="$rec" 2>&1 | tail -n 1)"
    echo "$rline" | tee -a "$DD_LOG" >/dev/null

    echo | tee -a "$DD_LOG" >/dev/null
  done
}

generate_report() {
  : >"$REPORT"

  local go_tsv="$WORKDIR/go.tsv"
  local dd_tsv="$WORKDIR/dd.tsv"

  # Build Go TSV by parsing the raw log.
  parse_go_to_tsv <"$GO_LOG" >"$go_tsv"

  # Build dd TSV by parsing the raw log.
  # We rely on the structure emitted by run_dd_bench():
  #   -- size=<label> records=<n> ...
  #   <write dd line>
  #   <read dd line>
  awk -v OFS="\t" '
    function size_to_bytes(lbl, n) {
      if (lbl ~ /KiB$/) { n = substr(lbl, 1, length(lbl)-3) + 0; return n * 1024 }
      if (lbl ~ /MiB$/) { n = substr(lbl, 1, length(lbl)-3) + 0; return n * 1024 * 1024 }
      if (lbl ~ /B$/)   { n = substr(lbl, 1, length(lbl)-1) + 0; return n }
      return 0
    }
    function dd_line_to_mbps(line, bps) {
      # macOS: (71,351,786 bytes/sec)
      if (match(line, /\([0-9,]+[[:space:]]bytes\/sec\)/)) {
        bps = substr(line, RSTART+1, RLENGTH-2)
        sub(/[[:space:]]bytes\/sec/, "", bps)
        gsub(/,/, "", bps)
        return (bps + 0.0) / 1000000.0
      }
      # GNU: "... 71.4 MB/s"
      if (match(line, /[0-9.]+[[:space:]]MB\/s/)) {
        bps = substr(line, RSTART, RLENGTH)
        sub(/[[:space:]]MB\/s/, "", bps)
        return bps + 0.0
      }
      return -1
    }
    /^-- size=/ {
      # extract size label between "size=" and space
      sizeLabel = $2
      sub(/^size=/, "", sizeLabel)
      bytes = size_to_bytes(sizeLabel)
      op = "write"
      next
    }
    bytes > 0 && (op == "write" || op == "read") && ($0 ~ /bytes transferred/ || $0 ~ /MB\/s/) {
      mbps = dd_line_to_mbps($0)
      if (mbps >= 0) {
        printf "%s\t%d\t%.2f\n", op, bytes, mbps
      }
      op = (op == "write") ? "read" : "done"
    }
  ' "$DD_LOG" >"$dd_tsv"

  {
    echo "WAL vs dd comparison"
    echo "Go log: $GO_LOG"
    echo "dd log: $DD_LOG"
    echo "benchtime: $BENCHTIME"
    echo
    printf "%-6s %-8s %12s %12s %10s %s\n" "op" "size" "go(MB/s)" "dd(MB/s)" "delta" "summary"
    printf "%-6s %-8s %12s %12s %10s %s\n" "--" "----" "--------" "--------" "-----" "-------"
  } >>"$REPORT"

  # Append rows in fixed size order using shell + awk lookup.
  for op in write read; do
    for sz in "${SIZES[@]}"; do
      awk -v report="$REPORT" -v op="$op" -v bytes="$sz" -v FS="\t" '
        function hb(b) {
          if (b % (1024*1024) == 0) return (b/(1024*1024)) "MiB"
          if (b % 1024 == 0) return (b/1024) "KiB"
          return b "B"
        }
        function fmt(x){ return sprintf("%.2f", x) }
        function abs(x){ return x < 0 ? -x : x }
        BEGIN { go=0; dd=0 }
        FNR==NR {
          if ($1==op && ($2+0)==bytes) go=$3+0
          next
        }
        {
          if ($1==op && ($2+0)==bytes) dd=$3+0
        }
        END {
          delta="-"
          summary=""
          if (go > 0 && dd > 0) {
            d=((dd-go)/go)*100.0
            delta=sprintf("%+.0f%%", d)
            if (dd > go) summary=sprintf("dd is %.0f%% faster (%s vs %s)", d, fmt(dd), fmt(go))
            else if (dd < go) summary=sprintf("go is %.0f%% faster (%s vs %s)", abs(d), fmt(go), fmt(dd))
            else summary=sprintf("tie (%s)", fmt(go))
          }
          printf "%-6s %-8s %12s %12s %10s %s\n", op, hb(bytes), (go>0?fmt(go):"-"), (dd>0?fmt(dd):"-"), delta, summary >> report
        }
      ' "$go_tsv" "$dd_tsv"
    done
  done
}

main() {
  require_cmd go
  require_cmd dd
  require_cmd awk

  # Create temp workspace early (so set -u traps are safe)
  WORKDIR="$(mktemp -d -t wal-bench-compare.XXXXXX)"

  # Truncate output files for this run
  : >"$GO_LOG"
  : >"$DD_LOG"

  {
    echo "ROOT_DIR: $ROOT_DIR"
    echo "WAL_DIR:  $WAL_DIR"
    echo "OUT_PREFIX: $OUT_PREFIX"
    echo "BENCHTIME: $BENCHTIME"
    echo "SHORT: $SHORT"
    echo
  } | tee -a "$GO_LOG" >/dev/null

  run_wal_bench
  run_dd_bench
  generate_report

  echo
  echo "Wrote:"
  echo "  $GO_LOG"
  echo "  $DD_LOG"
  echo "  $REPORT"
  echo
  cat "$REPORT"
}

main "$@"




