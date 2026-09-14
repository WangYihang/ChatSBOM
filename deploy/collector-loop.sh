#!/bin/sh
# One revalidation slice, then wait. That is the whole scheduler.
#
# A loop rather than cron-in-a-container: the interval is the only
# schedule there is, `docker compose logs -f collector` is the whole
# observability story, and Docker's restart policy already covers the
# crash case that a supervisor would.
set -eu

INTERVAL="${SYNC_INTERVAL_SECONDS:-900}"
SLICE="${SYNC_SLICE:-500}"
QUOTA="${SYNC_QUOTA:-250}"
PRUNE_EVERY="${PRUNE_EVERY_SLICES:-96}"   # 96 x 15min ~= daily
KEEP="${PRUNE_KEEP:-2}"

echo "collector: slice=${SLICE} quota=${QUOTA} interval=${INTERVAL}s"

# Register whatever the collection stages have produced. Idempotent, so
# it is safe on every start and picks up newly discovered repositories.
chatsbom queue track || echo "collector: track failed, continuing"

slices=0
while true; do
    slices=$((slices + 1))

    # A failing slice must not kill the loop: the ledger records the
    # failure and backs that repository off, and the next slice proceeds.
    chatsbom queue sync --slice "${SLICE}" --quota "${QUOTA}" \
        || echo "collector: slice ${slices} failed"

    if [ "$((slices % PRUNE_EVERY))" -eq 0 ]; then
        echo "collector: retention pass after ${slices} slices"
        chatsbom data prune --keep "${KEEP}" --apply \
            || echo "collector: prune failed"
    fi

    sleep "${INTERVAL}"
done
