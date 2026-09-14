# The collector, so continuous operation leaves nothing on the host.
#
# `docker compose down` removes it entirely: no systemd units, no host
# Python, no host syft. Everything it writes lives in the bind-mounted
# data/ and .cache/ directories, which are the same ones a manual run
# uses — so switching between the two loses no progress.
#
# Deliberately absent: the Docker CLI. `sbom lock` starts a container per
# repository, and giving this one the host socket would hand a
# container-escape to anything it runs. That stage stays a host command;
# see the note in README.
FROM python:3.12-slim

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
