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

# Syft is a single binary. Pinned rather than `latest`, and moved on
# deliberately: the version is part of the SBOM cache key, and a stored
# SBOM another version wrote is not current (`is_current_sbom`). So an
# upgrade regenerates every stored SBOM once, in the collector's next
# index pass, which runs `sbom generate` within a day of deploying:
# about seven hours for 28,000 roots on its two CPUs (DEPLOY.md,
# "Upgrading Syft"). Unpinned, any rebuild could start that. CI installs
# the same one (workflows_test).
ARG SYFT_VERSION=1.52.0

# The release's archive of it for the architecture being built, checked
# against that architecture's digest here before anything is taken out
# of it. Syft's install.sh, which did this before, checks the archive
# against the release's checksums file, but a mismatch it only logs:
# given a wrong checksum it said "did not verify", installed the archive
# all the same and exited 0 (#118). It also looked the tag up on
# github.com's releases page first, which #118's egress refused while it
# served the download itself. A new SYFT_VERSION moves both digests with
# it, to the two lines of the release's `syft_<version>_checksums.txt`
# for its `linux_amd64.tar.gz` and `linux_arm64.tar.gz`, and CI's amd64
# one with them (workflows_test holds the two together): DEPLOY.md,
# "Upgrading Syft".
ARG SYFT_SHA256_AMD64=caeedb81fb0491615f1ebd1761e4145d41ee86dd2cc7bf80669f9f5ad9d6133d
ARG SYFT_SHA256_ARM64=c46d5e4c28e12aa4c5becfaa343ef1c7f89045b6b895f2c21d471c62db09c706

# amd64 or arm64: BuildKit sets it for the platform being built, and a
# stage sees only the ARGs it names. Empty without BuildKit, which the
# step below refuses rather than guess.
ARG TARGETARCH

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
#
# The archive's `syft` belongs to uid 1001, the release runner's, and tar
# run as root keeps an archive's owners: --no-same-owner makes it root's,
# so that the uid the collector runs as, often 1001, cannot replace it.
# hadolint ignore=DL3008
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl git procps \
 && rm -rf /var/lib/apt/lists/* \
 && case "${TARGETARCH}" in \
      amd64) syft_sha256="${SYFT_SHA256_AMD64}" ;; \
      arm64) syft_sha256="${SYFT_SHA256_ARM64}" ;; \
      *) echo "No Syft digest is pinned for TARGETARCH '${TARGETARCH}'." >&2; exit 1 ;; \
    esac \
 && curl -sSfL -o /tmp/syft.tar.gz \
      "https://github.com/anchore/syft/releases/download/v${SYFT_VERSION}/syft_${SYFT_VERSION}_linux_${TARGETARCH}.tar.gz" \
 && echo "${syft_sha256}  /tmp/syft.tar.gz" | sha256sum --check --strict \
 && tar -xzf /tmp/syft.tar.gz --no-same-owner -C /usr/local/bin syft \
 && rm /tmp/syft.tar.gz \
 && syft version

# uv, which installs what uv.lock pins, below. Dependabot moves the
# images `FROM` lines name and not one a `COPY --from=` names, so this
# pin is moved by hand: to uv's newest release, with the digest `docker
# buildx imagetools inspect ghcr.io/astral-sh/uv:<version>` prints.
COPY --from=ghcr.io/astral-sh/uv:0.12.20@sha256:100047e74f30778ab704942321a09750d6158739573ff58bf3924085cc6cd2d8 /uv /usr/local/bin/uv

WORKDIR /app

# Dependencies first, so a source edit does not reinstall them.
#
# chatsbom with the one extra the collector loop needs, and without the
# dev group, which brings every extra. The loop's weekly Parquet export
# (#150) needs `export`, pyarrow: about 150 MB on disk, and nothing at
# any other command's start, since the clickhouse-connect uv.lock pins
# imports it only for a query asked for as Arrow. Nothing else the loop
# runs needs one — `queue`, `run`, `sbom generate`, `db raw` and `db
# index`, `warehouse build`, `snapshot build`, `data prune`, the
# `depgraph` worker — and each costs where it is not used: the chat SDK
# alone is 218 MB. `cli` runs this image too; a command that needs
# another extra says so there (README, "Installation").
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
RUN uv sync --frozen --no-dev --extra export --no-install-project \
    --compile-bytecode

COPY chatsbom ./chatsbom
RUN uv sync --frozen --no-dev --extra export --no-editable --compile-bytecode

# The container runs as the *invoking* user (see docker-compose.yaml), so
# the image cannot own /app to one uid: data/ and .cache/ are bind mounts
# belonging to whoever cloned the repo, and a mismatched uid turns the
# ledger into a read-only database. So /app is world-readable and the
# default user is an unprivileged fallback for a bare `docker run`. That
# user cannot write /app, and the image has no data/ of its own: a bare
# run says what to mount (`handle_errors`), where it stopped on a
# PermissionError's traceback (#118).
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
