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

# Before anything else: the bind mounts, which compose puts in the
# working directory (/app), must be directories this uid can write. None
# is in a fresh clone, and Docker creates a missing bind-mount source
# owned by root, which the loop, run as the invoking user, cannot write:
# the first sign was a read-only ledger, deep in the first slice.
uid=$(id -u)
gid=$(id -g)
unusable=''
for dir in data .cache .requests-cache; do
    if [ ! -d "${dir}" ]; then
        echo "collector: cannot write ${dir}/ as uid ${uid} (gid ${gid}): it does not exist." >&2
        unusable=1
    elif [ ! -w "${dir}" ] || [ ! -x "${dir}" ]; then
        echo "collector: cannot write ${dir}/ as uid ${uid} (gid ${gid}): permission denied." >&2
        unusable=1
    fi
done
if [ -n "${unusable}" ]; then
    cat >&2 <<EOF
    Docker creates a bind-mount source that does not exist, owned by
    root. On the host, in the checkout, create them before the first
    docker compose up:
        mkdir -p data .cache .requests-cache
    or, where Docker already has, give them to uid ${uid}:
        sudo chown -R ${uid}:${gid} data .cache .requests-cache
    The loop runs as UID and GID from the .env beside docker-compose.yaml,
    1000 if they are unset; if that is not you, set them there instead.
EOF
    exit 1
fi

# Here rather than in docker-compose.yaml, which interpolates the whole
# file for every command: a `${GITHUB_TOKEN:?}` there stopped `up`, `ps`
# and `down` for every service whenever the token was not set.
: "${GITHUB_TOKEN:?is empty or unset, and every slice needs it. Set it in the .env beside docker-compose.yaml.}"

INTERVAL="${SYNC_INTERVAL_SECONDS:-900}"
SLICE="${SYNC_SLICE:-500}"
QUOTA="${SYNC_QUOTA:-250}"
RUN_LIMIT="${RUN_LIMIT:-50}"
RUN_QUOTA="${RUN_QUOTA:-400}"
PRUNE_EVERY="${PRUNE_EVERY_SLICES:-96}"   # 96 x 15min ~= daily
KEEP="${PRUNE_KEEP:-2}"
INDEX_EVERY="${INDEX_EVERY_SLICES:-96}"   # likewise

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

echo "collector: slice=${SLICE} quota=${QUOTA}" \
     "run=${RUN_LIMIT}/${RUN_QUOTA} interval=${INTERVAL}s"

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

    # What that made due. Before this the loop noticed pushes and then
    # did nothing with them: a repository could sit due for six stages
    # waiting for someone to run six commands by hand.
    step chatsbom run --limit "${RUN_LIMIT}" --quota "${RUN_QUOTA}" \
        || echo "collector: run ${slices} failed"

    if [ "$((slices % INDEX_EVERY))" -eq 0 ]; then
        # The tail of the ETL, on a daily cadence rather than every
        # slice: landing the documents is I/O over the whole corpus and
        # a full index pass is minutes, neither of which is worth doing
        # four times an hour to pick up 200 repositories.
        echo "collector: index pass after ${slices} slices"
        step chatsbom db raw --apply || echo "collector: db raw failed"
        step chatsbom db index || echo "collector: db index failed"
    fi

    if [ "$((slices % PRUNE_EVERY))" -eq 0 ]; then
        echo "collector: retention pass after ${slices} slices"
        step chatsbom data prune --keep "${KEEP}" --apply \
            || echo "collector: prune failed"
    fi

    step sleep "${INTERVAL}"
done
