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

# `depgraph` as the first argument runs the dependency-graph worker
# instead: the same checks and stop handling, and a loop of its own,
# because that stage is paced by a bucket of its own (100 to 200
# requests an hour per token) and would otherwise pace all collection.
# Compose runs it as the `depgraph` service beside this one.
MODE="${1:-collect}"

INTERVAL="${SYNC_INTERVAL_SECONDS:-900}"
SLICE="${SYNC_SLICE:-500}"
QUOTA="${SYNC_QUOTA:-250}"
RUN_LIMIT="${RUN_LIMIT:-50}"
RUN_QUOTA="${RUN_QUOTA:-400}"
PRUNE_EVERY="${PRUNE_EVERY_SLICES:-96}"   # 96 x 15min ~= daily
KEEP="${PRUNE_KEEP:-2}"
INDEX_EVERY="${INDEX_EVERY_SLICES:-96}"   # likewise
GENERATE_LIMIT="${GENERATE_LIMIT:-all}"   # roots an index pass rescans
WAREHOUSE="${WAREHOUSE:-on}"              # the warehouse, snapshot, export
EXPORT_INTERVAL="${EXPORT_INTERVAL_SECONDS:-604800}"   # a week

# Refused rather than guessed at: taken for on, a mistyped `of` would
# fill the disk it was set to spare; for off, a mistyped `onn` would
# stop the site's data from moving, with nothing said.
case "${WAREHOUSE}" in
    on|off) ;;
    *)
        echo "collector: WAREHOUSE is '${WAREHOUSE}': on or off." >&2
        exit 1
        ;;
esac

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

# Whether the last export is EXPORT_INTERVAL seconds old, or there is
# none: by its manifest's age, which a start does not change.
export_due() {
    made=$(stat -c %Y data/export/manifest.json 2>/dev/null) || return 0
    [ "$(( $(date +%s) - made ))" -ge "${EXPORT_INTERVAL}" ]
}

if [ "${MODE}" = "depgraph" ]; then
    DEPGRAPH_LIMIT="${DEPGRAPH_LIMIT:-200}"
    DEPGRAPH_RATE="${DEPGRAPH_RATE:-90}"
    DEPGRAPH_INTERVAL="${DEPGRAPH_INTERVAL_SECONDS:-300}"
    echo "depgraph: limit=${DEPGRAPH_LIMIT} rate=${DEPGRAPH_RATE}/h/token" \
         "interval=${DEPGRAPH_INTERVAL}s"
    passes=0
    while true; do
        passes=$((passes + 1))
        # Paced inside: a pass of 200 takes over an hour on one token.
        # What is due comes from the queue, and a refused token stops
        # only its own share, so a failing pass is logged and retried.
        step chatsbom run --stage depgraph --limit "${DEPGRAPH_LIMIT}" \
            --rate "${DEPGRAPH_RATE}" \
            || echo "depgraph: pass ${passes} failed"
        step sleep "${DEPGRAPH_INTERVAL}"
    done
fi

# Where the export goes, which `web` mounts read-only, and is not made
# without (docker-compose.yaml): made as the collector starts, rather
# than by its first export, a warehouse and a day away, so that `web`
# can start before it.
mkdir -p data/export || echo "collector: cannot make data/export"

echo "collector: slice=${SLICE} quota=${QUOTA}" \
     "run=${RUN_LIMIT}/${RUN_QUOTA} interval=${INTERVAL}s"
if [ "${WAREHOUSE}" = off ]; then
    echo "collector: WAREHOUSE=off: index passes build no warehouse and" \
         "publish no snapshot, and there is no export"
fi

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
    # `--no-depgraph`: the `depgraph` service runs that stage, paced by
    # its own bucket, not by this loop's interval.
    step chatsbom run --limit "${RUN_LIMIT}" --quota "${RUN_QUOTA}" \
        --no-depgraph \
        || echo "collector: run ${slices} failed"

    if [ "$((slices % INDEX_EVERY))" -eq 0 ]; then
        # The tail of the ETL, on a daily cadence rather than every
        # slice: the warehouse is built of the whole store, minutes of
        # I/O, which is not worth doing four times an hour to pick up
        # 200 repositories.
        echo "collector: index pass after ${slices} slices"
        # First the SBOMs no longer current, so that this pass indexes
        # what they are regenerated to. `run` regenerates only those of
        # the repositories it walks, the ones due for other reasons: on
        # its own it would leave most of the corpus on the old Syft for
        # months after an upgrade. Most days this is the pre-scan alone.
        # The first pass after an upgrade rescans every root, about
        # seven hours for 28,000 on the collector's two CPUs, with no
        # slice meanwhile; GENERATE_LIMIT spreads that over days.
        if [ "${GENERATE_LIMIT}" = all ]; then
            step chatsbom sbom generate \
                || echo "collector: sbom generate failed"
        else
            step chatsbom sbom generate --limit "${GENERATE_LIMIT}" \
                || echo "collector: sbom generate failed"
        fi
        # Then the index, and what the site serves (#128 §2.3, §2.4):
        # the warehouse, built from the store alone, the only index since
        # the ClickHouse server went (#153), and a snapshot of it,
        # published in data/snapshots only when what it serves has
        # changed, which on most days it has not. Each is a step of its
        # own, as the rest of the pass: a warehouse build that fails
        # leaves the last one in place, and a snapshot of that is the
        # snapshot already published, so the next pass tries again with
        # nothing lost. DuckDB's limits are compose's, and it spills
        # beside the warehouse, in data/.
        if [ "${WAREHOUSE}" = on ]; then
            step chatsbom warehouse build \
                || echo "collector: warehouse build failed"
            step chatsbom snapshot build \
                || echo "collector: snapshot build failed"
        fi
    fi

    if [ "${WAREHOUSE}" = on ] && [ -f data/warehouse.duckdb ] \
        && export_due; then
        # The public Parquet export (Q11), which `web` serves (#154):
        # when the last is a week old, or there is none, of the
        # warehouse the last index pass built, once one has. By the
        # manifest's age, not by slices counted from the container's
        # start, which a collector started again more often than weekly
        # never reached. One directory, exported into again: its files
        # are named by their content, and once the manifest names the
        # new ones, the last export's go (`chatsbom/export/parquet.py`).
        # An export that fails leaves the manifest as old as it was, so
        # the next slice tries again.
        echo "collector: export pass"
        step chatsbom export parquet --output data/export \
            || echo "collector: export failed"
    fi

    if [ "$((slices % PRUNE_EVERY))" -eq 0 ]; then
        echo "collector: retention pass after ${slices} slices"
        step chatsbom data prune --keep "${KEEP}" --apply \
            || echo "collector: prune failed"
    fi

    step sleep "${INTERVAL}"
done
