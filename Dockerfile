# The collector, so continuous operation leaves nothing on the host.
#
# `docker compose down` removes it entirely: no systemd units, no host
# Python, no host syft. Everything it writes lives in the bind-mounted
# data/ and .cache/ directories, which are the same ones a manual run
# uses — so switching between the two loses no progress.
#
# Two images come out of this file, one per target compose builds:
# `collector`, which the collector and `cli` services run, and `lock`,
# for `sbom lock`. Deliberately absent from the first: the Docker CLI.
# `sbom lock` starts a container per repository, and an image with a
# Docker client and a reachable socket is one mistake away from being a
# container escape. The client is added in the `lock` stage below, which
# the collector's target never reaches.
FROM python:3.12-slim AS collector

# Syft is a single binary. Pinned rather than `latest`, because the
# version is part of the SBOM cache key and an unpinned upgrade would
# silently repartition every cached result.
ARG SYFT_VERSION=1.41.2

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl git \
 && rm -rf /var/lib/apt/lists/* \
 && curl -sSfL https://get.anchore.io/syft \
      | sh -s -- -b /usr/local/bin "v${SYFT_VERSION}" \
 && syft version

COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /usr/local/bin/uv

WORKDIR /app

# Dependencies first, so a source edit does not reinstall them.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project

COPY chatsbom ./chatsbom
RUN uv sync --frozen

# The container runs as the *invoking* user (see docker-compose.yaml), so
# the image cannot own /app to one uid: data/ and .cache/ are bind mounts
# belonging to whoever cloned the repo, and a mismatched uid turns the
# ledger into a read-only database. So /app is world-readable and the
# default user is an unprivileged fallback for a bare `docker run`.
RUN chmod -R a+rX /app \
 && useradd --create-home --uid 10001 collector
USER 10001

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["chatsbom"]
CMD ["queue", "status"]


# The lockfile resolver, which needs a Docker *client* — and nothing else
# does.
#
# Kept apart from the collector on purpose. `sbom lock` asks a daemon to
# start a container per repository, so it needs the CLI; the collector
# must not have it, for the reason above. Splitting the images makes that
# a property of the build rather than a rule someone remembers.
#
# A stage of this file, not a Dockerfile of its own: that one was built
# `FROM` the `cli` image as compose had named it under the project's old
# name, which nothing built any more. A fresh clone could not build it,
# and a machine that still held the old image built on that, silently
# stale. A stage is built from these sources every time.
FROM collector AS lock
COPY --from=docker:27-cli /usr/local/bin/docker /usr/local/bin/docker


# The last stage is what `docker build` makes when no `--target` is
# named, so it is the collector again: forgetting the flag should give
# the image without a Docker client, not the one with it.
FROM collector
