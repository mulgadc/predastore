#!/usr/bin/env bash
#
# gen-certs.sh — one TLS keypair for a multi-host predastore cluster.
#
# Every host presents the same identity: the gate serves it to S3 clients and
# each host presents it to its QUIC peers. Peers verify against the trust store
# with no RootCAs override, so hosts holding different self-signed identities
# do not fail loudly — they simply never elect a leader. Generating once, here,
# is what avoids that.
#
# The single-container profile needs none of this: the entrypoint generates its
# own identity on first start, because it has no peer to agree with.
#
# Usage:
#   deploy/docker/gen-certs.sh [output-dir]
#
# Environment:
#   PREDA_SUBNET   Cluster subnet prefix, without the final octet
#                  (default 10.11.12)
#   PREDA_NODES    How many host addresses to cover        (default 4)

set -euo pipefail

out="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/certs}"
subnet="${PREDA_SUBNET:-10.11.12}"
nodes="${PREDA_NODES:-4}"

mkdir -p "$out"

if [ -f "$out/server.pem" ] && [ -f "$out/server.key" ]; then
    echo "gen-certs: $out already holds a keypair, leaving it alone"
    exit 0
fi

san="DNS:localhost,IP:127.0.0.1"
for i in $(seq 1 "$nodes"); do
    san="${san},IP:${subnet}.${i},DNS:preda-${i}"
done

# 825 days is the maximum a modern client will accept for a server certificate.
( umask 0077 && openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$out/server.key" -out "$out/server.pem" \
    -days 825 -subj '/CN=predastore-cluster' \
    -addext "subjectAltName=${san}" 2>/dev/null )

# Readable by anyone, deliberately: the containers run as uid 10001 and mount
# this read-only, and picking a mode that only they can read means guessing at
# the host's uid mapping. This is a local development identity — it is
# gitignored, it is regenerated per machine, and a cluster that matters gets
# its keypair from a real CA rather than from this script.
chmod 0644 "$out/server.pem" "$out/server.key"

echo "gen-certs: wrote $out/server.pem covering ${san}"
