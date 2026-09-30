#!/bin/sh
# Is the site actually serving?
#
# Liveness is a request. A tunnel that stops ("no more connections
# active and exiting") leaves its process running, `etime` still
# climbing, and a service with no dataset to read still serves its page
# and its port: neither shows up in `ps` or in a port check.
#
# So this asks for an answer, and asks for one that could only come
# from the dataset: which snapshot `/api/meta` says is current, and that
# snapshot's totals.
#
# Public checks resolve over DoH rather than through this machine's
# resolver. That is not belt-and-braces: `systemd-resolved` here does
# not resolve `*.trycloudflare.com` at all, so the first version of
# this script reported three consecutive tunnels as dead while all
# three were serving fine to everyone else. A monitor that cries
# outage is worse than none.
#
# Usage:
#
#   ./scripts/health.sh https://sbom.example.com    # the site, from outside
#   ./scripts/health.sh http://127.0.0.1:8080       # `chatsbom web serve` here
#
# Each address given is checked. Under compose the web service publishes
# no port on this machine, so the public side is the whole answer from
# here, and `docker compose ps` has the containers' own checks.
#
# Exit status is the number of failed checks, so it composes with a
# monitor or a cron line.
set -u

# With nothing to check it would exit 0, which a monitor reads as
# healthy.
if [ "$#" -lt 1 ]; then
    echo "usage: $0 <https://public.host> [<address>...]" >&2
    exit 2
fi

FAILED=0

# Cloudflare's own resolver, used for public hostnames only. A local
# address must not go through it.
DOH="https://1.1.1.1/dns-query"

check() {
    base="$1"
    # Loopback resolves locally by definition; everything else is
    # checked the way a visitor would reach it.
    case "$base" in
        *127.0.0.1*|*localhost*) resolver="" ;;
        *) resolver="--doh-url $DOH" ;;
    esac

    # shellcheck disable=SC2086  # resolver is one flag pair or empty
    code=$(curl -s $resolver -o /dev/null -w '%{http_code}' -m 20 "$base/" 2>/dev/null)
    if [ "$code" != "200" ]; then
        # 000 is curl's "no response", which is what a dead service
        # behind a live port and a dead tunnel both look like.
        echo "${base}: page HTTP ${code}"
        FAILED=$((FAILED + 1))
        return
    fi

    # The page draws its shell with no dataset to read, the panels
    # saying they could not answer, so the page alone is not evidence.
    # shellcheck disable=SC2086
    meta=$(curl -s $resolver -m 20 "$base/api/meta" 2>/dev/null)
    snapshot=$(printf '%s' "$meta" \
        | sed -n 's/.*"snapshot": *"\([0-9a-f]\{1,\}\)".*/\1/p')
    if [ -z "$snapshot" ]; then
        echo "${base}: no snapshot — $(printf '%s' "$meta" | cut -c1-72)"
        FAILED=$((FAILED + 1))
        return
    fi

    # shellcheck disable=SC2086
    body=$(curl -s $resolver -m 25 "$base/api/v/${snapshot}/totals" 2>/dev/null)
    case "$body" in
        *'"repositories":'*)
            echo "${base}: ok, snapshot ${snapshot} — $(printf '%s' "$body" | cut -c1-72)"
            ;;
        *)
            echo "${base}: api did not answer — $(printf '%s' "$body" | cut -c1-72)"
            FAILED=$((FAILED + 1))
            ;;
    esac
}

for base in "$@"; do
    check "${base%/}"
done

exit "$FAILED"
