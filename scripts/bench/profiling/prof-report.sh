#!/usr/bin/env bash
#
# Resolves one scenario's profiles into text reports.
#
#   prof-report.sh <scenario-profile-dir> <out-dir>
#
# CPU is merged across every window of every process, because each window is a
# separate sample of the same run. The cumulative profiles -- allocs, block,
# mutex -- are merged one snapshot per process lifetime, not one per host: a
# node-loss scenario runs more than one pid for a host, and each pid's counters
# start at zero and end at its own last snapshot. Taking the last file per host
# would silently drop everything a killed process did.
set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "usage: $0 <scenario-profile-dir> <out-dir>" >&2
    exit 2
fi

DIR="$1"
OUT="$2"
mkdir -p "$OUT"

# series lists the distinct <timestamp>-host<n>-pid<n> prefixes in the
# directory. One series is one process lifetime.
series() {
    find "$DIR" -maxdepth 1 -name '*.pprof' -printf '%f\n' 2>/dev/null |
        sed -n 's/^\(.*-host[0-9]*-pid[0-9]*\)-[a-z]*-[0-9]*\.pprof$/\1/p' |
        sort -u
}

# last_of prints the final snapshot of one kind within one series, or nothing.
last_of() {
    find "$DIR" -maxdepth 1 -name "$1-$2-*.pprof" -printf '%f\n' 2>/dev/null |
        sort | tail -1
}

mapfile -t all_series < <(series)
if [ "${#all_series[@]}" -eq 0 ]; then
    echo "no profiles in $DIR" >&2
    exit 1
fi

mapfile -t cpu < <(find "$DIR" -maxdepth 1 -name '*-cpu-*.pprof' | sort)
if [ "${#cpu[@]}" -gt 0 ]; then
    go tool pprof -proto "${cpu[@]}" > "$OUT/cpu-merged.pprof" 2>/dev/null
    go tool pprof -top -nodecount=40 "$OUT/cpu-merged.pprof" > "$OUT/cpu-top.txt" 2>&1
    go tool pprof -top -cum -nodecount=40 "$OUT/cpu-merged.pprof" > "$OUT/cpu-top-cum.txt" 2>&1
fi

for kind in allocs block mutex; do
    files=()
    for s in "${all_series[@]}"; do
        last=$(last_of "$s" "$kind")
        [ -n "$last" ] && files+=("$DIR/$last")
    done
    [ "${#files[@]}" -eq 0 ] && continue
    go tool pprof -proto "${files[@]}" > "$OUT/$kind-merged.pprof" 2>/dev/null
    go tool pprof -top -nodecount=30 "$OUT/$kind-merged.pprof" > "$OUT/$kind-top.txt" 2>&1
    go tool pprof -top -cum -nodecount=30 "$OUT/$kind-merged.pprof" > "$OUT/$kind-top-cum.txt" 2>&1
    printf '%s\n' "${files[@]}" > "$OUT/$kind-sources.txt"
done

# Heap is a live set rather than a total, so it is reported per process instead
# of merged: adding the live heaps of processes that did not overlap in time
# would describe a moment that never happened.
for s in "${all_series[@]}"; do
    last=$(last_of "$s" heap)
    [ -n "$last" ] && go tool pprof -top -nodecount=20 -sample_index=inuse_space \
        "$DIR/$last" > "$OUT/heap-$s-final.txt" 2>&1
done

# Goroutine count over time, from each snapshot's total sample count.
: > "$OUT/goroutines.txt"
for s in "${all_series[@]}"; do
    host=$(sed -n 's/.*-host\([0-9]*\)-pid.*/\1/p' <<< "$s")
    while IFS= read -r f; do
        [ -n "$f" ] || continue
        n=$(go tool pprof -top -nodecount=1 "$DIR/$f" 2>/dev/null |
            awk '/of .* total/ {gsub(/,/,"",$(NF-1)); print $(NF-1)}')
        seq=$(sed 's/.*goroutine-//;s/\.pprof//' <<< "$f")
        echo "host$host $s $seq $n" >> "$OUT/goroutines.txt"
    done < <(find "$DIR" -maxdepth 1 -name "$s-goroutine-*.pprof" -printf '%f\n' | sort)
done

echo "reports in $OUT (${#all_series[@]} process lifetimes)"
