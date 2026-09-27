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
#
# `--var` only for what may be read, though. A command line is readable
# by every user on the host, container or not: uid 65534 read the
# ClickHouse password and the Anthropic key off wrangler's
# /proc/<pid>/cmdline, where the environment holding the same values was
# refused to it. So secrets go in `.dev.vars` instead: written here at
# each start, readable by this uid alone, and never part of the image
# (.dockerignore excludes it, and nothing at build time writes one).
#
# Not CLOUDFLARE_INCLUDE_PROCESS_ENV, which wrangler also supports: that
# binds the entire environment — PATH, HOSTNAME and whatever compose adds
# next — as Worker secrets, and only while no `.dev.vars` exists, so a
# stray one would silently win. Writing the file names exactly what the
# Worker gets, and overwrites whatever was there.
set -e

# The Worker project: wrangler finds wrangler.jsonc from where it runs,
# and reads `.dev.vars` and keeps its state beside it. Overridable so the
# script can be run against a scratch directory.
WEB_DIR="${WEB_DIR:-/app/web}"

if [ -z "$CLICKHOUSE_URL" ]; then
    echo "CLICKHOUSE_URL is not set — refusing to start." >&2
    echo "Without it the Worker has no live backend and would serve" >&2
    echo "whatever stale snapshot it can find instead." >&2
    exit 1
fi

# One `.dev.vars` line that dotenv reads back as exactly the value given.
# Unquoted, dotenv ends a value at the first `#`, and inside double quotes
# it turns `\n` into a newline. Single quotes and backticks it takes
# literally, but neither can be escaped — so whichever the value does not
# contain, and a refusal if it holds both, rather than a Worker with a
# password that is subtly not the password.
dev_var() {
    case "$2" in
        *"'"*) quote='`' ;;
        *) quote="'" ;;
    esac
    case "$2" in
        *"$quote"*)
            echo "$1 contains both ' and \`, which .dev.vars cannot carry." >&2
            exit 1
            ;;
    esac
    # A builtin, so the value is on no command line even while written.
    printf '%s=%s%s%s\n' "$1" "$quote" "$2" "$quote"
}

cd "$WEB_DIR"

# 0600 from the moment it exists rather than tightened afterwards, and
# removed first: `>` keeps the mode of a file already there, which after
# a restart it is.
rm -f .dev.vars
(
    umask 077
    {
        dev_var CLICKHOUSE_PASSWORD "${CLICKHOUSE_PASSWORD:-guest}"
        # Only when set: an empty value would still count as configured,
        # and /api/chat would fail oddly rather than saying it is not set
        # up. Turnstile likewise: empty means off.
        if [ -n "$ANTHROPIC_API_KEY" ]; then
            dev_var ANTHROPIC_API_KEY "$ANTHROPIC_API_KEY"
        fi
        if [ -n "$TURNSTILE_SECRET" ]; then
            dev_var TURNSTILE_SECRET "$TURNSTILE_SECRET"
        fi
    } > .dev.vars
)

set -- npx wrangler dev --local --ip 0.0.0.0 --port 8787 \
    --var "CLICKHOUSE_URL:$CLICKHOUSE_URL" \
    --var "CLICKHOUSE_DB:${CLICKHOUSE_DB:-chatsbom}" \
    --var "CLICKHOUSE_USER:${CLICKHOUSE_USER:-guest}" \
    --var "GENERATOR:${GENERATOR:-chatsbom clickhouse}"

if [ -n "$DAILY_SPEND_CAP_USD" ]; then
    set -- "$@" --var "DAILY_SPEND_CAP_USD:$DAILY_SPEND_CAP_USD"
fi

exec "$@"
