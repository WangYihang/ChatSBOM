#!/bin/sh
# Notice what changed, collect what that made due, then wait. That is
# the whole scheduler.
#
# A loop rather than cron-in-a-container: the interval is the only
# schedule there is, `docker compose logs -f collector` is the whole
# observability story, and Docker's restart policy already covers the
# crash case that a supervisor would.
#
# The two collection steps are separate because they cost differently.
# `queue sync` revalidates conditionally and a 304 is free, so a slice
# can check 500 repositories for almost nothing; `run` collects, and
# each repository costs several rate-limited requests. One budget sized
# for both would be sized for the expensive one.
#
# The arithmetic, per hour, at the 15-minute default: four passes of
# (250 + 400) is 2,600 requests against one token's 5,000 -- so the
# loop runs at roughly half allowance and leaves room for a manual
# stage beside it.
set -eu

INTERVAL="${SYNC_INTERVAL_SECONDS:-900}"
SLICE="${SYNC_SLICE:-500}"
QUOTA="${SYNC_QUOTA:-250}"
RUN_LIMIT="${RUN_LIMIT:-50}"
RUN_QUOTA="${RUN_QUOTA:-400}"
PRUNE_EVERY="${PRUNE_EVERY_SLICES:-96}"   # 96 x 15min ~= daily
KEEP="${PRUNE_KEEP:-2}"
INDEX_EVERY="${INDEX_EVERY_SLICES:-96}"   # likewise

echo "collector: slice=${SLICE} quota=${QUOTA}" \
     "run=${RUN_LIMIT}/${RUN_QUOTA} interval=${INTERVAL}s"

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

    # What that made due. Before this the loop noticed pushes and then
    # did nothing with them: a repository could sit due for six stages
    # waiting for someone to run six commands by hand.
    chatsbom run --limit "${RUN_LIMIT}" --quota "${RUN_QUOTA}" \
        || echo "collector: run ${slices} failed"

    if [ "$((slices % INDEX_EVERY))" -eq 0 ]; then
        # The tail of the ETL, on a daily cadence rather than every
        # slice: landing the documents is I/O over the whole corpus and
        # a full index pass is minutes, neither of which is worth doing
        # four times an hour to pick up 200 repositories.
        echo "collector: index pass after ${slices} slices"
        chatsbom db raw --apply || echo "collector: db raw failed"
        chatsbom db index || echo "collector: db index failed"
    fi

    if [ "$((slices % PRUNE_EVERY))" -eq 0 ]; then
        echo "collector: retention pass after ${slices} slices"
        chatsbom data prune --keep "${KEEP}" --apply \
            || echo "collector: prune failed"
    fi

    sleep "${INTERVAL}"
done
