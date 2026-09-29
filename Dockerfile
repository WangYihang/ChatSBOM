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
#
# Every image is pinned by digest, as the lock recipes' are: a tag moves
# with each rebuild of its image. The digest is the multi-platform
# index's, as the registry serves it for the tag, which stays to say
# what it is; `docker buildx imagetools inspect python:3.14-slim` prints
# the current one, to move a pin on deliberately.
FROM python:3.14-slim@sha256:51dafde81dbdb6ebde285137a295cf18a47ca95234fe388a343719cb97305b3d AS collector

# Syft is a single binary. Pinned rather than `latest`, because the
# version is part of the SBOM cache key and an unpinned upgrade would
# silently repartition every cached result. CI installs the same one
# (workflows_test).
ARG SYFT_VERSION=1.52.0

# Syft's installer, from the release's own tag and checked against this
# digest before it runs: get.anchore.io serves whatever the installer is
# the day of the build, and it was piped straight to `sh`. The installer
# takes the build platform's archive and checks it against the release's
# checksums file. A new SYFT_VERSION needs this moved with it, to what
# `sha256sum` says of that tag's install.sh.
ARG SYFT_INSTALLER_SHA256=ea054f8b6754db17d34129482ecda1ab733cadab57c1c9202bbe98eb5fe18d24

# With pipefail a RUN's pipe fails when any command in it does, not
# only its last (hadolint's DL4006).
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# procps for `ps`, which GitPython runs to stop a git that outlives its
# `kill_after_timeout`: without it the timeout never fires, and a
# stalled `ls-remote` holds the loop (#75).
#
# The packages' versions are not pinned (hadolint's DL3008): Debian
# replaces a version with each security update and drops the old one
# from its mirrors, so a pin would fail the build weeks later. They are
# what the base image's Debian release ships.
# hadolint ignore=DL3008
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl git procps \
 && rm -rf /var/lib/apt/lists/* \
 && curl -sSfL -o /tmp/install-syft.sh \
      "https://raw.githubusercontent.com/anchore/syft/v${SYFT_VERSION}/install.sh" \
 && echo "${SYFT_INSTALLER_SHA256}  /tmp/install-syft.sh" \
      | sha256sum --check --strict \
 && sh /tmp/install-syft.sh -b /usr/local/bin "v${SYFT_VERSION}" \
 && rm /tmp/install-syft.sh \
 && syft version

# uv, which installs what uv.lock pins, below. Dependabot moves the
# images `FROM` lines name and not one a `COPY --from=` names, so this
# pin is moved by hand: to uv's newest release, with the digest `docker
# buildx imagetools inspect ghcr.io/astral-sh/uv:<version>` prints.
COPY --from=ghcr.io/astral-sh/uv:0.12.20@sha256:100047e74f30778ab704942321a09750d6158739573ff58bf3924085cc6cd2d8 /uv /usr/local/bin/uv

WORKDIR /app

# Dependencies first, so a source edit does not reinstall them.
#
# chatsbom without extras, and without the dev group, which brings every
# extra. Nothing the collector loop runs needs one — `queue`, `run`, `db
# raw` and `db index`, `data prune`, the `depgraph` worker — and each
# costs where it is not used: the chat SDK alone is 218 MB, and pandas
# and pyarrow, installed, are imported by clickhouse-connect on every
# command's first connection. `cli` runs this image too; a command that
# needs an extra says so there (README, "Installation").
#
# Byte-compiled here: the container's uid cannot write /app, so what the
# build leaves as source is compiled again at every start, and thrown
# away. The project is installed, not linked back to /app, where uv
# compiles nothing: `chatsbom --help` took 1.55 s, and takes 0.63 s.
#
# LICENSE with README.md, as what building the project's wheel reads:
# pyproject.toml names it in `license-files`, and without it hatchling
# left the licence out of the installed distribution, silently (#28).
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --frozen --no-dev --no-install-project --compile-bytecode

COPY chatsbom ./chatsbom
RUN uv sync --frozen --no-dev --no-editable --compile-bytecode

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
#
# Docker 29, the release compose's dind runs: the client `sbom lock`
# drives the daemon with is the daemon's own (compose_test). Both were
# 27, past its end of life (#45). Dependabot moves dind and not this
# line, so move this one with it.
FROM collector AS lock
COPY --from=docker:29-cli@sha256:018edbc908e08fcc9dbf029c812c34251e9b4719e6f71ca0e5eae2a987d014ca /usr/local/bin/docker /usr/local/bin/docker


# The last stage is what `docker build` makes when no `--target` is
# named, so it is the collector again: forgetting the flag should give
# the image without a Docker client, not the one with it.
FROM collector
