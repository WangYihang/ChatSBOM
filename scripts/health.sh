#!/bin/sh
# Is the dashboard actually serving?
#
# Liveness is a request. Neither of the two ways this stack goes down
# shows up in `ps` or in a port check:
#
#   - a quick tunnel stops ("no more connections active and exiting")
#     and leaves its process running, `etime` still climbing;
#   - `wrangler dev` is killed by a concurrent `npm run build` — the
#     content-hashed chunk it resolved is gone — and the port keeps
#     listening for a moment after it exits.
#
# So this asks for an answer, and asks for one that could only come
# from the database.
#
# Usage:
#
#   ./scripts/health.sh                          # local worker only
#   ./scripts/health.sh https://host.example     # and the public side
#
# Exit status is the number of failed checks, so it composes with a
# monitor or a cron line.
set -u

FAILED=0

check() {
    label="$1"
    base="$2"

    code=$(curl -s -o /dev/null -w '%{http_code}' -m 20 "$base/" 2>/dev/null)
    if [ "$code" != "200" ]; then
        # 000 is curl's "no response", which is what a dead worker
        # behind a live port and a dead tunnel both look like.
        echo "${label}: page HTTP ${code}"
        FAILED=$((FAILED + 1))
        return
    fi

    # The page can render its shell with the database unreachable — the
    # panels just say "Failed to fetch" — so the page alone is not
    # evidence. This asks the API for a number only ClickHouse has.
    body=$(curl -s -m 25 -X POST "$base/api/q" \
        -H 'content-type: application/json' \
        -d '{"method":"totals","args":[]}' 2>/dev/null)
    case "$body" in
        *'"repositories":'*)
            echo "${label}: ok — $(printf '%s' "$body" | cut -c1-72)"
            ;;
        *)
            echo "${label}: api did not answer — $(printf '%s' "$body" | cut -c1-72)"
            FAILED=$((FAILED + 1))
            ;;
    esac
}

check "local " "http://127.0.0.1:8787"
if [ "$#" -ge 1 ]; then
    check "public" "${1%/}"
fi

exit "$FAILED"
