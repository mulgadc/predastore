#!/bin/sh
#
# Container entrypoint for s3d.
#
# Everything host-local reaches s3d as a flag; the topology reaches it as a
# rendered TOML, because s3d reads no environment and requires -config.
#
# Environment:
#   PREDA_CONFIG    Use this profile verbatim and render nothing
#   PREDA_HOST_ID   Which [[host]] this process is        (default 1)
#   PREDA_HOST_ID_FROM_ORDINAL=1
#                   Derive the host id from a StatefulSet pod ordinal
#   PREDA_DATA      Data root              (default /var/lib/predastore)
#   PREDA_TLS_CERT  TLS certificate        (default $PREDA_DATA/tls/server.pem)
#   PREDA_TLS_KEY   TLS key                (default $PREDA_DATA/tls/server.key)
#   PREDA_KEY       AES-256 key at rest    (default $PREDA_DATA/master.key)
#   LOG_LEVEL       debug|info|warn|error                (default info)
#
# render-config.sh documents the topology variables. Extra arguments are
# appended to the s3d command line.

set -eu

PREDA_DATA="${PREDA_DATA:-/var/lib/predastore}"
PREDA_TLS_CERT="${PREDA_TLS_CERT:-${PREDA_DATA}/tls/server.pem}"
PREDA_TLS_KEY="${PREDA_TLS_KEY:-${PREDA_DATA}/tls/server.key}"
PREDA_KEY="${PREDA_KEY:-${PREDA_DATA}/master.key}"
LOG_LEVEL="${LOG_LEVEL:-info}"

mkdir -p "$PREDA_DATA"

# Under a StatefulSet the pod ordinal is the only stable per-pod identity, so
# the host id comes from it rather than from a per-replica environment. An
# explicit PREDA_HOST_ID still wins, which is how compose sets it.
if [ -z "${PREDA_HOST_ID:-}" ] && [ "${PREDA_HOST_ID_FROM_ORDINAL:-0}" = "1" ]; then
    ordinal="$(hostname | sed 's/.*-//')"
    case "$ordinal" in
        ''|*[!0-9]*) echo "[entrypoint] cannot derive an ordinal from $(hostname)" >&2; exit 1 ;;
    esac
    PREDA_HOST_ID=$((ordinal + 1))
fi
PREDA_HOST_ID="${PREDA_HOST_ID:-1}"

# A mounted profile owns the topology outright: rendering over the top of one
# would silently ignore whatever the operator wrote.
if [ -n "${PREDA_CONFIG:-}" ]; then
    [ -r "$PREDA_CONFIG" ] || { echo "[entrypoint] PREDA_CONFIG $PREDA_CONFIG is not readable" >&2; exit 1; }
    config="$PREDA_CONFIG"
    echo "[entrypoint] host ${PREDA_HOST_ID} from mounted $(basename "$config")"
else
    config=/etc/predastore/rendered/predastore.toml
    render-config.sh > "$config"
    echo "[entrypoint] host ${PREDA_HOST_ID} from a rendered profile: ${PREDA_PEERS:-127.0.0.1}"
fi

# The key is generated into the data volume, never baked into the image: an
# image-resident key would encrypt every user's data under a public one.
#
# The loader is fail-closed on group- and other-readable modes, so the umask is
# tightened around the write rather than chmod'ed afterwards, which would leave
# a briefly world-readable window.
if [ ! -f "$PREDA_KEY" ]; then
    echo "[entrypoint] generating AES-256 key at rest: $PREDA_KEY"
    ( umask 0177 && openssl rand -out "$PREDA_KEY" 32 )
fi

# TLS. One keypair serves both planes: the gate presents it to S3 clients and
# every host presents it to its QUIC peers.
#
# Generating one is only correct for a single host. Peers verify each other
# against the trust store with no RootCAs override, so a cluster whose members
# each generated their own identity does not fail loudly — it simply never
# elects a leader. A multi-host deployment therefore has to be given a shared
# keypair, and is refused rather than left to hang.
if [ ! -f "$PREDA_TLS_CERT" ] || [ ! -f "$PREDA_TLS_KEY" ]; then
    case "${PREDA_PEERS:-}" in
        *,*)
            echo "[entrypoint] no TLS keypair at $PREDA_TLS_CERT" >&2
            echo "[entrypoint] a multi-host cluster needs one keypair shared by every host; generate it once and mount it" >&2
            exit 1
            ;;
    esac
    echo "[entrypoint] generating a self-signed TLS identity: $PREDA_TLS_CERT"
    mkdir -p "$(dirname "$PREDA_TLS_CERT")" "$(dirname "$PREDA_TLS_KEY")"
    ( umask 0077 && openssl req -x509 -newkey rsa:2048 -nodes \
        -keyout "$PREDA_TLS_KEY" -out "$PREDA_TLS_CERT" \
        -days 825 -subj '/CN=predastore' \
        -addext "subjectAltName=DNS:localhost,DNS:$(hostname),IP:127.0.0.1" \
        2>/dev/null )
fi

# SSL_CERT_FILE rather than update-ca-certificates: Go reads it for the system
# pool, and it needs no root, which is what lets this image run as a normal
# user. It replaces Go's list of certificate *files* but not its list of
# directories, so /etc/ssl/certs is still read and a real CA-issued
# certificate keeps verifying.
if [ -z "${SSL_CERT_FILE:-}" ]; then
    export SSL_CERT_FILE="$PREDA_TLS_CERT"
fi

exec /usr/local/bin/s3d \
    -config "$config" \
    -host "$PREDA_HOST_ID" \
    -data-dir "$PREDA_DATA" \
    -tls-cert "$PREDA_TLS_CERT" \
    -tls-key "$PREDA_TLS_KEY" \
    -encryption-key "$PREDA_KEY" \
    -log-level "$LOG_LEVEL" \
    "$@"
