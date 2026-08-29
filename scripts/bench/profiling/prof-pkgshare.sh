#!/usr/bin/env bash
#
# Attributes a merged profile's samples to the package that spent them.
#
#   prof-pkgshare.sh <merged.pprof> [sample_index]
#
# Flat by package answers "who ran", which is rarely the question. Read it
# beside the cumulative tree, which answers "on whose behalf".
set -uo pipefail

if [ "$#" -lt 1 ]; then
    echo "usage: $0 <merged.pprof> [sample_index]" >&2
    exit 2
fi

go tool pprof -top -nodecount=3000 ${2:+-sample_index=$2} "$1" 2>/dev/null |
    awk 'NF>=6 && $1 ~ /(s|ms|B|MB|kB|GB)$/ {
        fn=($NF=="(inline)")?$(NF-1):$NF; v=$1
        if (v ~ /ms$/) {sub(/ms$/,"",v); v=v/1000}
        else if (v ~ /kB$/) {sub(/kB$/,"",v); v=v/1024}
        else if (v ~ /GB$/) {sub(/GB$/,"",v); v=v*1024}
        else if (v ~ /MB$/) {sub(/MB$/,"",v)}
        else if (v ~ /s$/) {sub(/s$/,"",v)}
        else next
        pkg="other"
        if (fn ~ /^runtime|^internal\/runtime|^internal\/poll|^syscall|^internal\/sync|^sync\.|^bufio|^io\./) pkg="go runtime+syscall+io"
        else if (fn ~ /crypto|fips140/) pkg="crypto"
        else if (fn ~ /quic-go/) pkg="quic"
        else if (fn ~ /reedsolomon/) pkg="reedsolomon"
        else if (fn ~ /badger|ristretto/) pkg="badger"
        else if (fn ~ /hashicorp\/raft/) pkg="raft"
        else if (fn ~ /mulgadc\/predastore/) pkg="predastore"
        else if (fn ~ /^net\.|^net\//) pkg="net"
        else if (fn ~ /pprof/) pkg="profiler itself"
        s[pkg]+=v; t+=v
    } END { for (p in s) printf "%-24s %10.2f %5.1f%%\n", p, s[p], 100*s[p]/t }' |
    sort -k2 -gr
