#!/bin/sh
# One revalidation slice, then wait. That is the whole scheduler.
#
# A loop rather than cron-in-a-container: the interval is the only
# schedule there is, `docker compose logs -f collector` is the whole
# observability story, and Docker's restart policy already covers the
# crash case that a supervisor would.
set -eu

# Here rather than in docker-compose.yaml, which interpolates the whole
# file for every command: a `${GITHUB_TOKEN:?}` there stopped `up`, `ps`
# and `down` for every service whenever the token was not set.
: "${GITHUB_TOKEN:?is empty or unset, and every slice needs it. Set it in the .env beside docker-compose.yaml.}"

INTERVAL="${SYNC_INTERVAL_SECONDS:-900}"
SLICE="${SYNC_SLICE:-500}"
QUOTA="${SYNC_QUOTA:-250}"
PRUNE_EVERY="${PRUNE_EVERY_SLICES:-96}"   # 96 x 15min ~= daily
KEEP="${PRUNE_KEEP:-2}"

# The step in flight, if any.
child=''

# Stopping: compose sends TERM, which docker-init (`init: true`) passes
# to this shell, and a terminal sends INT. Either way the step in flight
# is sent TERM and waited for, so it ends on the signal rather than on
# SIGKILL when the grace period runs out; a slice cut short loses at most
# the repository it was on. TERM whichever arrived: a background command
# of a non-interactive shell starts with INT ignored, so INT would reach
# nothing.
stop() {
    echo "collector: stopping"
    if [ -n "${child}" ]; then
        kill -TERM "${child}" 2>/dev/null || true
        wait "${child}" 2>/dev/null || true
    fi
    exit 0
}
trap stop TERM INT

# Runs one step and returns its status. In the background, because a
# shell runs a trap only once the foreground command has returned: a stop
# would have waited out the whole slice, or the whole interval. `wait`
# returns as soon as a trapped signal arrives.
step() {
    "$@" &
    child=$!
    status=0
    wait "${child}" || status=$?
    child=''
    return "${status}"
}

echo "collector: slice=${SLICE} quota=${QUOTA} interval=${INTERVAL}s"

# Register whatever the collection stages have produced. Idempotent, so
# it is safe on every start and picks up newly discovered repositories.
step chatsbom queue track || echo "collector: track failed, continuing"

slices=0
while true; do
    slices=$((slices + 1))

    # A failing slice must not kill the loop: the ledger records the
    # failure and backs that repository off, and the next slice proceeds.
    step chatsbom queue sync --slice "${SLICE}" --quota "${QUOTA}" \
        || echo "collector: slice ${slices} failed"

    if [ "$((slices % PRUNE_EVERY))" -eq 0 ]; then
        echo "collector: retention pass after ${slices} slices"
        step chatsbom data prune --keep "${KEEP}" --apply \
            || echo "collector: prune failed"
    fi

    step sleep "${INTERVAL}"
done
