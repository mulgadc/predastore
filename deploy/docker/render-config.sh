#!/bin/sh
#
# render-config.sh — write a predastore TOML profile to stdout from the
# environment.
#
# s3d reads no environment variables and requires -config, so a container that
# wants to be configured the way containers are configured needs this. The
# alternative is mounting a file, which PREDA_CONFIG still allows.
#
# Every host renders the whole topology, and each one must agree with the
# others about which node id belongs where. Node ids are therefore allocated
# from the host index rather than counted up as hosts are emitted: host i owns
# the block of (2 + blob nodes) ids starting at (i-1) * (2 + blob nodes) + 1.
#
# Environment, all optional:
#   PREDA_PEERS         Comma-separated host addresses, in host-id order.
#                       One entry (or unset) is a single-host cluster.
#   PREDA_BIND_ADDR     Cluster-plane bind address        (default 0.0.0.0)
#   PREDA_RS_DATA       Data shards            (default 1 alone, else 2)
#   PREDA_RS_PARITY     Parity shards          (default 0 alone, else 1)
#   PREDA_BLOB_NODES    Blob nodes per host              (default 1)
#   PREDA_REGION        Region                (default ap-southeast-2)
#   PREDA_S3_PORT       Gate port                        (default 8443)
#   PREDA_META_PORT     Meta port                        (default 6660)
#   PREDA_BLOB_PORT     First blob port                  (default 9991)
#   PREDA_ADMIN_PORT    Admin port, 0 disables           (default 9099)
#   PREDA_ACCESS_KEY_ID, PREDA_SECRET_ACCESS_KEY, PREDA_ACCOUNT_ID

set -eu

PREDA_PEERS="${PREDA_PEERS:-127.0.0.1}"
PREDA_BIND_ADDR="${PREDA_BIND_ADDR:-0.0.0.0}"
PREDA_BLOB_NODES="${PREDA_BLOB_NODES:-1}"
PREDA_REGION="${PREDA_REGION:-ap-southeast-2}"
PREDA_S3_PORT="${PREDA_S3_PORT:-8443}"
PREDA_META_PORT="${PREDA_META_PORT:-6660}"
PREDA_BLOB_PORT="${PREDA_BLOB_PORT:-9991}"
PREDA_ADMIN_PORT="${PREDA_ADMIN_PORT:-9099}"

host_count=0
for _peer in $(echo "$PREDA_PEERS" | tr ',' ' '); do
    host_count=$((host_count + 1))
done
[ "$host_count" -ge 1 ] || { echo "render-config: PREDA_PEERS is empty" >&2; exit 2; }

# A lone host has nowhere to put a parity shard, so RS(1,0) is the only width
# that is legal there. Any real cluster defaults to one parity unit.
if [ "$host_count" -eq 1 ]; then
    PREDA_RS_DATA="${PREDA_RS_DATA:-1}"
    PREDA_RS_PARITY="${PREDA_RS_PARITY:-0}"
else
    PREDA_RS_DATA="${PREDA_RS_DATA:-2}"
    PREDA_RS_PARITY="${PREDA_RS_PARITY:-1}"
fi

blobs=$((host_count * PREDA_BLOB_NODES))
stripe=$((PREDA_RS_DATA + PREDA_RS_PARITY))
if [ "$stripe" -gt "$blobs" ]; then
    echo "render-config: RS($PREDA_RS_DATA,$PREDA_RS_PARITY) is $stripe shards wide but the cluster has $blobs blob nodes" >&2
    exit 2
fi

# Credentials. A single-host cluster is an evaluation, and the documented AWS
# example pair is what the shipped profiles already use, so the quick start
# stays copy-pasteable. Anything with peers is refused without explicit ones:
# a multi-host deployment is not a demo.
if [ "$host_count" -gt 1 ] && [ -z "${PREDA_ACCESS_KEY_ID:-}" ]; then
    echo "render-config: PREDA_ACCESS_KEY_ID and PREDA_SECRET_ACCESS_KEY are required for a multi-host cluster" >&2
    exit 2
fi
access_key="${PREDA_ACCESS_KEY_ID:-AKIAIOSFODNN7EXAMPLE}"
secret_key="${PREDA_SECRET_ACCESS_KEY:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY}"
account_id="${PREDA_ACCOUNT_ID:-123456789012}"

printf '# Rendered by render-config.sh. Edits are lost on restart; mount a file\n'
printf '# and set PREDA_CONFIG to take ownership of the topology instead.\n\n'
printf 'version = 1\n'
printf 'region = "%s"\n\n' "$PREDA_REGION"
printf '[rs]\ndata = %d\nparity = %d\n\n' "$PREDA_RS_DATA" "$PREDA_RS_PARITY"

per_host=$((2 + PREDA_BLOB_NODES))
host_id=0
for peer in $(echo "$PREDA_PEERS" | tr ',' ' '); do
    host_id=$((host_id + 1))
    base=$(((host_id - 1) * per_host))

    printf '[[host]]\n'
    printf 'id = %d\n' "$host_id"
    printf 'addr = "%s"\n' "$peer"
    printf 'bind_addr = "%s"\n' "$PREDA_BIND_ADDR"
    if [ "$PREDA_ADMIN_PORT" -ne 0 ]; then
        printf 'admin_port = %d\n' "$PREDA_ADMIN_PORT"
    fi
    printf '\n'

    printf '  [[host.node]]\n  id = %d\n  role = "gate"\n  port = %d\n\n' \
        "$((base + 1))" "$PREDA_S3_PORT"
    printf '  [[host.node]]\n  id = %d\n  role = "meta"\n  port = %d\n\n' \
        "$((base + 2))" "$PREDA_META_PORT"

    n=1
    while [ "$n" -le "$PREDA_BLOB_NODES" ]; do
        printf '  [[host.node]]\n  id = %d\n  role = "blob"\n  port = %d\n\n' \
            "$((base + 2 + n))" "$((PREDA_BLOB_PORT + n - 1))"
        n=$((n + 1))
    done
done

printf '[[auth]]\n'
printf 'access_key_id = "%s"\n' "$access_key"
printf 'secret_access_key = "%s"\n' "$secret_key"
printf 'account_id = "%s"\n' "$account_id"
