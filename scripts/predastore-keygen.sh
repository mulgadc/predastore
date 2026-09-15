#!/usr/bin/env bash
#
# predastore-keygen.sh — the two secrets s3d refuses to start without.
#
# Run this once per host before the first `systemctl start predastore`. It is
# idempotent: anything already present is left exactly as it is, because
# regenerating either of these destroys access to stored data.
#
# It is deliberately not wired into ExecStartPre. Minting a self-signed
# certificate on service start is wrong for production, and for a multi-host
# cluster it is worse than wrong — see the refusal below.
#
# Usage:
#   scripts/predastore-keygen.sh [config-dir]
#
# Environment:
#   PREDA_CONFIG   Cluster TOML, read only to count hosts and collect SANs
#                  (default <config-dir>/predastore.toml)
#   PREDA_OWNER    chown the results to this user:group, when run as root
#                  (default predastore:predastore)
#   PREDA_SAN      Override the certificate's subjectAltName entirely

set -euo pipefail

config_dir="${1:-/etc/predastore}"
config="${PREDA_CONFIG:-${config_dir}/predastore.toml}"
owner="${PREDA_OWNER:-predastore:predastore}"

key="${config_dir}/master.key"
cert="${config_dir}/server.pem"
certkey="${config_dir}/server.key"

log() { echo "keygen: $*"; }
die() { echo "keygen: $*" >&2; exit 1; }

command -v openssl >/dev/null 2>&1 || die "openssl is required"

mkdir -p "$config_dir"

# The at-rest key. Exactly 32 raw bytes, no base64 and no header, and the
# loader is fail-closed on any group- or other-readable bit. The umask is
# tightened around the write rather than chmod'ed after, which would leave a
# briefly world-readable window.
if [ -e "$key" ]; then
    log "$key exists, leaving it alone"
else
    log "generating the AES-256 key at rest: $key"
    ( umask 0177 && openssl rand -out "$key" 32 )
fi

# One keypair serves both planes: the gate presents it to S3 clients and each
# host presents it to its QUIC peers.
if [ -e "$cert" ] && [ -e "$certkey" ]; then
    log "$cert exists, leaving it alone"
else
    # Peers verify each other against the system trust store with no RootCAs
    # override, so a cluster whose hosts each generated their own identity does
    # not fail loudly — it simply never elects a leader. Refusing beats hanging.
    hosts=0
    if [ -r "$config" ]; then
        hosts="$(grep -c '^[[:space:]]*\[\[host\]\][[:space:]]*$' "$config" || true)"
    fi
    if [ "$hosts" -gt 1 ]; then
        echo "keygen: $config declares $hosts hosts and no keypair is present at $cert" >&2
        echo "keygen: a multi-host cluster needs one keypair shared by every host, plus its CA in the" >&2
        echo "keygen: system trust store. Generate it once, distribute it, and re-run this on each host." >&2
        exit 1
    fi

    san="${PREDA_SAN:-}"
    if [ -z "$san" ]; then
        san="DNS:localhost,IP:127.0.0.1,DNS:$(hostname)"
        # Cover whatever addresses the config's single host names, so a client
        # dialling the machine by address verifies without an override.
        if [ -r "$config" ]; then
            while read -r addr; do
                [ -n "$addr" ] && san="${san},IP:${addr}"
            done < <(grep -oE '^[[:space:]]*(addr|bind_addr)[[:space:]]*=[[:space:]]*"[0-9.]+"' "$config" |
                grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' |
                grep -v '^0\.0\.0\.0$' | grep -v '^127\.0\.0\.1$' | sort -u || true)
        fi
    fi

    # 825 days is the longest a modern client accepts for a server certificate.
    log "generating a self-signed TLS identity: $cert"
    ( umask 0077 && openssl req -x509 -newkey rsa:2048 -nodes \
        -keyout "$certkey" -out "$cert" \
        -days 825 -subj '/CN=predastore' \
        -addext "subjectAltName=${san}" 2>/dev/null )
    log "certificate covers ${san}"

    # Self-signed means no client trusts it yet. Say so rather than letting the
    # first aws-cli call fail with an opaque verification error.
    log "self-signed: install $cert as a trust anchor on every client, or pass --no-verify-ssl to test"
fi

# Only root can hand these to the service account, and only if it exists yet.
if [ "$(id -u)" = "0" ] && id -u "${owner%%:*}" >/dev/null 2>&1; then
    chown "$owner" "$key" "$cert" "$certkey"
    log "owner set to $owner"
else
    log "not root or $owner missing: chown $owner $key $cert $certkey before starting the service"
fi
