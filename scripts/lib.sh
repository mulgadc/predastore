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
