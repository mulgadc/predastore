#!/usr/bin/env bash
#
# test-start-sh-loopback-cert.sh — committed failing reproduction for the
# loopback-certificate plan. Assembles the certificate SAN exactly the way
# scripts/start.sh does for an all-loopback multi-host profile, then asserts
# every configured address is covered, verified per-address with
# `openssl x509 -checkip` (the same check cert_covers_hosts will use).
#
# Red on dev: routable_addrs excludes every 127.* address, so the SAN covers
# only DNS:localhost and 127.0.0.1, and QUIC dials between loopback aliases
# fail with "certificate is valid for 127.0.0.1, not 127.0.0.3".
#
# The SAN assembly is deliberately mirrored here rather than refactored out of
# start.sh: the fix should extract it into lib.sh so both call one
# implementation, and this test should then exercise the shared function.
#
# Usage: scripts/test-start-sh-loopback-cert.sh   (exit 0 = pass, 1 = fail)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib.sh
source "$SCRIPT_DIR/lib.sh"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# A generated all-loopback 3-host profile: the shape the 2026-11-27 lab used.
PROFILE="$TMP/loopback3.toml"
cat > "$PROFILE" <<'EOF'
version = 1
region = "ap-southeast-2"
[rs]
data = 2
parity = 1
[[host]]
id = 1
addr = "127.0.0.1"
admin_port = 9099
  [[host.node]]
  id = 1
  role = "gate"
  port = 8443
[[host]]
id = 2
addr = "127.0.0.2"
admin_port = 9099
  [[host.node]]
  id = 4
  role = "gate"
  port = 8443
[[host]]
id = 3
addr = "127.0.0.3"
admin_port = 9099
  [[host.node]]
  id = 7
  role = "gate"
  port = 8443
EOF

# The SAN assembly, mirrored from start.sh.
SAN="DNS:localhost,IP:127.0.0.1"
for ip in $(routable_addrs "$PROFILE"); do
    SAN="${SAN},IP:${ip}"
done
echo "assembled SAN: $SAN"

openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$TMP/server.key" -out "$TMP/server.pem" \
    -days 1 -subj '/CN=localhost' -addext "subjectAltName=${SAN}" 2>/dev/null

FAIL=0
# Every address the profile names must be covered. This is the contract QUIC
# dials actually exercise: transport.NewQUICTransport verifies the dialed
# address against the certificate's IP SANs. Note: openssl -checkip always
# exits 0; the verdict is in its output text.
for ip in $(parse_hosts "$PROFILE" | awk '{print $2}' | sort -u); do
    out=$(openssl x509 -in "$TMP/server.pem" -noout -checkip "$ip" 2>/dev/null || true)
    if echo "$out" | grep -q "does NOT match"; then
        echo "FAIL: SAN does not cover $ip (profile names it; QUIC dials it)"
        FAIL=1
    else
        echo "ok: SAN covers $ip"
    fi
done

exit $FAIL
