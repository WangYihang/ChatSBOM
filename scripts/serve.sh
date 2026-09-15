#!/bin/sh
# Serve the dashboard locally and expose it through a quick tunnel.
#
# Everything stays on this machine: the Worker, the static assets and
# ClickHouse. `cloudflared` forwards one port; the database is not on
# the tunnel and is not reachable off-host (docker-compose binds it to
# 127.0.0.1).
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
W="$ROOT/web"
S="${TMPDIR:-/tmp}"

# `docker compose up` is the way to run this now; see README. This
# script remains for a host-only run, and refuses when the container
# already holds the port — two workers on 8787 means whichever bound
# first serves, and the other fails in a way that reads as a build
# problem.
if docker compose -f "$ROOT/docker-compose.yaml" ps --status running \
        --services 2>/dev/null | grep -qx web; then
    echo "The 'web' container is already serving on 8787." >&2
    echo >&2
    echo "Use that, or stop it first:" >&2
    echo "    docker compose stop web" >&2
    exit 1
fi

cd "$W"
echo "building..."
npm run build >/dev/null 2>&1

# `wrangler dev` previews the build, not the sources, so the build above
# is not optional.
echo "starting the worker..."
setsid nohup npx wrangler dev --port 8787 --local > "$S/serve-worker.log" 2>&1 < /dev/null &
for i in $(seq 1 40); do
  curl -s -m 2 http://127.0.0.1:8787/ >/dev/null 2>&1 && break
  sleep 1
done
curl -s -m 5 http://127.0.0.1:8787/ >/dev/null || { echo "worker did not start"; tail -5 "$S/serve-worker.log"; exit 1; }
echo "  worker up, serving $(curl -s http://127.0.0.1:8787/ | grep -o 'index-[A-Za-z0-9_-]*\.js')"

echo "opening the tunnel..."
setsid nohup cloudflared tunnel --url http://127.0.0.1:8787 --no-autoupdate \
  > "$S/serve-tunnel.log" 2>&1 < /dev/null &
for i in $(seq 1 40); do
  grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com' "$S/serve-tunnel.log" 2>/dev/null | head -1 && break
  sleep 2
done
