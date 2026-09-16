#!/bin/bash
#
# lib.sh - Topology parsing shared by the dev scripts. Sourced, not executed.
#

# parse_hosts emits "host_id addr gate_port" per [[host]] in the config named
# by $1, with an empty gate port for a host running none. Nodes are nested
# under [[host.node]], so this tracks which table it is inside.
parse_hosts() {
    awk '
    function endnode() { if (sect == "node" && role == "gate") gate = port; role = ""; port = "" }
    function endhost() { if (id != "" && addr != "") print id, addr, gate; id = ""; addr = ""; gate = "" }
    function num(s)    { sub(/^[^=]*=[[:space:]]*/, "", s); gsub(/[[:space:]]/, "", s); return s }
    function str(s)    { sub(/^[^=]*=[[:space:]]*"/, "", s); sub(/".*$/, "", s); return s }

    /^[[:space:]]*#/                  { next }
    /^[[:space:]]*\[\[host\]\]/       { endnode(); endhost(); sect = "host"; next }
    /^[[:space:]]*\[\[host\.node\]\]/ { endnode(); sect = "node"; next }
    /^[[:space:]]*\[/                 { endnode(); endhost(); sect = "other"; next }

    sect == "host" && /^[[:space:]]*id[[:space:]]*=/   { id = num($0) }
    sect == "host" && /^[[:space:]]*addr[[:space:]]*=/ { addr = str($0) }
    sect == "node" && /^[[:space:]]*role[[:space:]]*=/ { role = str($0) }
    sect == "node" && /^[[:space:]]*port[[:space:]]*=/ { port = num($0) }

    END { endnode(); endhost() }
    ' "$1"
}

# gate_endpoints emits "addr:port" for every host in $1 that runs a gate —
# the S3 endpoints the cluster answers on.
gate_endpoints() {
    parse_hosts "$1" | awk '$3 != "" { print $2 ":" $3 }'
}

# routable_addrs emits the host addresses that need a loopback alias and a
# trust anchor. Loopback is the machine's own, so it is excluded here rather
# than at each use.
routable_addrs() {
    parse_hosts "$1" | awk '$2 !~ /^127\./ { print $2 }' | sort -u
}

# meta_nodes emits "host_id node_id" for every meta node in the config named
# by $1, in file order. A host running no meta node is simply absent, so a
# caller must not assume one line per host.
meta_nodes() {
    awk '
    function endnode() { if (sect == "node" && role == "meta" && nid != "") print hid, nid; role = ""; nid = "" }
    function num(s)    { sub(/^[^=]*=[[:space:]]*/, "", s); gsub(/[[:space:]]/, "", s); return s }
    function str(s)    { sub(/^[^=]*=[[:space:]]*"/, "", s); sub(/".*$/, "", s); return s }

    /^[[:space:]]*#/                  { next }
    /^[[:space:]]*\[\[host\]\]/       { endnode(); sect = "host"; next }
    /^[[:space:]]*\[\[host\.node\]\]/ { endnode(); sect = "node"; next }
    /^[[:space:]]*\[/                 { endnode(); sect = "other"; next }

    sect == "host" && /^[[:space:]]*id[[:space:]]*=/   { hid = num($0) }
    sect == "node" && /^[[:space:]]*id[[:space:]]*=/   { nid = num($0) }
    sect == "node" && /^[[:space:]]*role[[:space:]]*=/ { role = str($0) }

    END { endnode() }
    ' "$1"
}

# take_host_lock blocks until this shell owns the host-wide benchmark lock,
# waiting at most $1 seconds, and reports the path it took in HOST_LOCK_PATH.
# The benchmark harnesses contend for things that are properties of the machine
# rather than of a run — the loopback aliases and the shifted port range — and
# the GitHub concurrency groups that serialise CI are scoped to one repository
# each, so they cannot see a run started by another repository or by hand.
#
# flock rather than a sentinel file: the kernel releases the lock when the
# holding descriptor closes, which covers a clean exit, a crash, a SIGKILL and a
# runner reset alike. There is no stale lock to expire, so the only timeout is
# on acquiring it.
#
# The path comes back in a variable because the lock lives on fd 9 of whichever
# shell opened it. Called as `$(take_host_lock)` the function would run in a
# subshell that exits immediately, dropping the lock before the caller has done
# anything with it.
#
# fd 9 is reserved for this across the dev scripts. Children inherit it, so
# anything launched to outlive the harness — s3d, above all — must close it with
# `9>&-`. An orphan holding the lock open is the one way this can wedge, since
# there would then be no process left that releasing it is waiting on.
HOST_LOCK_PATH=""
take_host_lock() {
    local timeout="${1:-3600}"
    HOST_LOCK_PATH="${PREDA_BENCH_LOCK:-/var/lock/predastore-bench.lock}"

    if ! command -v flock >/dev/null 2>&1; then
        echo "flock is required to serialise benchmark runs on this host" >&2
        return 1
    fi

    # /run/lock is world-writable and sticky, so the usual case is a lock file
    # this user already owns. A file left by another user is the exception worth
    # handling: fall back rather than fail, since an unlocked run is worse than a
    # lock in a second place, and every caller resolves the fallback identically.
    if [ -e "$HOST_LOCK_PATH" ]; then
        [ -w "$HOST_LOCK_PATH" ] || HOST_LOCK_PATH="${TMPDIR:-/tmp}/predastore-bench.lock"
    else
        [ -w "$(dirname "$HOST_LOCK_PATH")" ] || HOST_LOCK_PATH="${TMPDIR:-/tmp}/predastore-bench.lock"
    fi

    # Append rather than truncate: the file is a lock and never has contents,
    # and truncation is the one open mode that can fail on a file we may share.
    exec 9>>"$HOST_LOCK_PATH"

    if ! flock -w "$timeout" 9; then
        echo "another predastore benchmark has held $HOST_LOCK_PATH for ${timeout}s" >&2
        return 1
    fi
}

# render_profile copies the profile named by $1 to $2 with every node port and
# the host admin_port shifted by $3, so a harness can run beside a cluster
# already holding the defaults. A zero is left alone: on admin_port that is the
# off switch, and shifting it would start a listener nobody asked for.
render_profile() {
    awk -v offset="$3" '
        function shift(line,   val) {
            match(line, /[0-9]+/)
            val = substr(line, RSTART, RLENGTH) + 0
            if (val == 0) return line
            return sprintf("%s%d%s", substr(line, 1, RSTART - 1), \
                val + offset, substr(line, RSTART + RLENGTH))
        }
        /^[[:space:]]*(admin_)?port[[:space:]]*=/ { print shift($0); next }
        { print }
    ' "$1" > "$2"
}
