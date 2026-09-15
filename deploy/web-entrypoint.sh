#!/bin/sh
# Start the dashboard, passing the container's configuration to the
# Worker.
#
# `wrangler dev` does not read the process environment: a Worker sees
# only `vars` from wrangler.jsonc, a `.dev.vars` file, or `--var`. So
# `CLICKHOUSE_URL: http://clickhouse:8123` in compose reached the shell
# and not the Worker, `selectDataset` found no ClickHouse binding, and
# the container fell through to the local D1 that `web/.wrangler` had
# carried into the image — serving February's numbers over a healthy
# container. Hence `--var`, and hence the healthcheck asserting which
# backend answered rather than just that something did.
set -e

if [ -z "$CLICKHOUSE_URL" ]; then
    echo "CLICKHOUSE_URL is not set — refusing to start." >&2
    echo "Without it the Worker has no live backend and would serve" >&2
    echo "whatever stale snapshot it can find instead." >&2
    exit 1
fi

set -- npx wrangler dev --local --ip 0.0.0.0 --port 8787 \
    --var "CLICKHOUSE_URL:$CLICKHOUSE_URL" \
    --var "CLICKHOUSE_DB:${CLICKHOUSE_DB:-chatsbom}" \
    --var "CLICKHOUSE_USER:${CLICKHOUSE_USER:-guest}" \
    --var "CLICKHOUSE_PASSWORD:${CLICKHOUSE_PASSWORD:-guest}" \
    --var "GENERATOR:${GENERATOR:-chatsbom clickhouse}"

# Only when set: an empty value would still count as configured, and
# /api/chat would fail oddly rather than saying it is not set up.
if [ -n "$ANTHROPIC_API_KEY" ]; then
    set -- "$@" --var "ANTHROPIC_API_KEY:$ANTHROPIC_API_KEY"
fi
if [ -n "$DAILY_SPEND_CAP_USD" ]; then
    set -- "$@" --var "DAILY_SPEND_CAP_USD:$DAILY_SPEND_CAP_USD"
fi

exec "$@"
