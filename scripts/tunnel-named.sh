#!/bin/sh
# A named tunnel, so the address survives a restart.
#
# A quick tunnel gets a random `*.trycloudflare.com` hostname every time
# it starts, and this machine has already seen those hostnames fail to
# resolve for several minutes after creation — the tunnel connects, the
# name does not answer. A named tunnel has a hostname you own, created
# once.
#
# Everything else is unchanged: the tunnel forwards the Worker's port
# and nothing else, and ClickHouse stays bound to the loopback
# interface.
#
# Usage:
#
#   ./scripts/tunnel-named.sh sbom.example.com
#
# One-time, interactive, and yours to run — it opens a browser and picks
# the zone:
#
#   cloudflared tunnel login
#
set -e

HOSTNAME_ARG="$1"
if [ -z "$HOSTNAME_ARG" ]; then
    echo "usage: $0 <hostname>    e.g. $0 sbom.example.com" >&2
    exit 2
fi

NAME="chatsbom"
CONFIG_DIR="${HOME}/.cloudflared"
CONFIG="${CONFIG_DIR}/${NAME}.yml"

if [ ! -f "${CONFIG_DIR}/cert.pem" ]; then
    echo "Not logged in. Run this once, in a session where you can open" >&2
    echo "a browser:" >&2
    echo >&2
    echo "    cloudflared tunnel login" >&2
    exit 1
fi

# Idempotent: a second run reuses the tunnel rather than creating a
# duplicate, because `tunnel create` fails on an existing name and the
# credentials file is what actually matters.
if ! cloudflared tunnel list --name "$NAME" --output json 2>/dev/null \
        | grep -q '"id"'; then
    echo "creating tunnel ${NAME}..."
    cloudflared tunnel create "$NAME"
fi

TUNNEL_ID=$(cloudflared tunnel list --name "$NAME" --output json \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["id"])')
echo "tunnel ${NAME} = ${TUNNEL_ID}"

# The DNS record points the hostname at this tunnel. Re-running is
# harmless; an existing record for the same tunnel is left alone.
cloudflared tunnel route dns "$NAME" "$HOSTNAME_ARG" 2>&1 \
    | grep -v 'already exists' || true

cat > "$CONFIG" <<YAML
# Written by scripts/tunnel-named.sh. Edit the script, not this file.
tunnel: ${TUNNEL_ID}
credentials-file: ${CONFIG_DIR}/${TUNNEL_ID}.json

ingress:
  # The Worker's port, and nothing else. ClickHouse is not on the
  # tunnel: it binds 127.0.0.1 and reaching it needs shell access to
  # this machine.
  - hostname: ${HOSTNAME_ARG}
    service: http://127.0.0.1:8787
  # Required terminator. Anything not matched above is refused rather
  # than forwarded somewhere by accident.
  - service: http_status:404
YAML

echo "config written to ${CONFIG}"
echo
echo "start it with:"
echo "    cloudflared tunnel --config ${CONFIG} run ${NAME}"
echo
echo "or run it in the background:"
echo "    setsid nohup cloudflared tunnel --config ${CONFIG} run ${NAME} \\"
echo "        > /tmp/chatsbom-tunnel.log 2>&1 < /dev/null &"
echo
echo "then: https://${HOSTNAME_ARG}"
