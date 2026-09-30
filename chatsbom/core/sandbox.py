"""Container isolation for lockfile generation.

Syft can only report a resolved dependency closure when a lockfile
exists, and Maven, Composer and gemspec-only projects often ship none.
Generating one means running the ecosystem's own resolver — and that is
executing project-controlled code:

* `mvn` runs whatever build plugins the POM declares;
* a `Gemfile` *is* Ruby, evaluated on load;
* `composer` runs `scripts` hooks;
* `pip`/`setup.py` executes arbitrary Python.

Running that against 3,000 unvetted repositories on the host is not
acceptable, so every resolver runs in a container with the project
mounted read-only and no other host path, no privileges, a read-only
root filesystem, and bounded memory, CPU, process count, wall-clock time
and output.

The lockfile comes back as a tar on the container's stdout, read under a
cap, and only regular files the recipe names are taken from it. It used
to be written into a host directory mounted writable at /out, with no
bound on what the project's code put there, or how much.

The deadline is kept by removing the container. Killing the `docker run`
client, which is all `subprocess.run(timeout=...)` did, leaves the
container running: SIGKILL is not passed on, and it had no name to be
found by. So each has a name, is removed by it whenever a run ends
early, and ends itself at the same deadline should nothing on this side
be left to do it.

Network access is the one thing that cannot be removed: resolution is
precisely the act of fetching dependency metadata from a registry. That
is the residual risk. Resolvers get a network of their own, on which
they cannot reach each other (`lock_network`), and nothing else is
granted.
"""
import hashlib
import io
import json
import os
import selectors
import shlex
import shutil
import stat
import subprocess
import tarfile
import threading
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from pathlib import PurePosixPath

import structlog

from chatsbom.core.fs import atomic_write_bytes

logger = structlog.get_logger('sandbox')

#: Where the read-only project lands in the container.
PROJECT_MOUNT = '/project'
#: Where every recipe copies the project and resolves it, and so where
#: its lockfiles are when it is done. On the /tmp tmpfs: resolvers write
#: beside the manifest, /project is read-only, and nothing else is
#: writable.
WORKDIR = '/tmp/p'

#: Each resolver container is named this and a uuid, so that it can be
#: removed by name when a run is cut short.
CONTAINER_PREFIX = 'chatsbom-lock-'

#: The network every resolver runs on (`lock_network`).
LOCK_NETWORK = 'chatsbom-lock'
#: The bridge driver's switch for traffic between its own containers.
ICC_OPTION = 'com.docker.network.bridge.enable_icc'

#: `LockResult.returncode` for a run the sandbox ended itself, as a shell
#: would report it: out of time as `timeout(1)` exits, killed (128 + 9)
#: for output over the cap, interrupted (128 + 2) when cancelled.
TIMED_OUT = 124
KILLED = 137
INTERRUPTED = 130

#: Bytes of a resolver's stderr kept: the end, which is where its error
#: is. Resolvers print their progress there, and a hostile one can print
#: forever.
STDERR_TAIL = 64 * 1024

#: How much of that the log line of a failed resolution keeps: this many
#: characters from each end, and how many were left out between them.
#: Not the end alone: after an exception Composer prints the command's
#: usage synopsis, some 600 characters, and the 400 the log kept were
#: the synopsis and never the error (a `curl error 60` in #118). The
#: head holds an error a resolver prints first, the tail one it prints
#: after its progress.
STDERR_LOGGED = 1000

#: Seconds `docker rm -f` may take.
REMOVE_TIMEOUT = 60
#: How often a run waiting on its container looks at the deadline and at
#: whether it was cancelled.
POLL_SECONDS = 0.2
_CHUNK = 64 * 1024


class SandboxError(RuntimeError):
    """The sandbox cannot be set up as it should be, so nothing runs."""


#: Fallback identity when the caller is root: unprivileged and owns
#: nothing on the host.
NOBODY = '65534:65534'


def _invoking_user() -> str:
    """uid:gid the container should run as.

    The container reads the project from a host directory, which the
    invoking user owns and may have made readable to nobody else, so it
    runs as that user: not root, which in the container is root on the
    bind mount. A root caller falls back to `nobody`, who reads what
    anyone may.
    """
    uid = os.getuid()
    return NOBODY if uid == 0 else f'{uid}:{os.getgid()}'


@dataclass(frozen=True, slots=True)
class SandboxLimits:
    """Resource ceilings for one container run."""

    memory: str = '2g'
    cpus: str = '2'
    pids: int = 256
    #: Wall-clock seconds before the container is removed.
    timeout: int = 300
    #: Numeric uid:gid inside the container. Never 0 — root in the
    #: container is root on a bind mount.
    user: str = ''
    #: Bytes the container may send back: the tar of its lockfiles. Far
    #: more than a lockfile needs; what it bounds is how much a resolver
    #: running project-controlled code can make this process hold, and
    #: write to data/.
    output_bytes: int = 32 * 1024 * 1024

    def resolved_user(self) -> str:
        return self.user or _invoking_user()


def _is_regular_file(path: Path) -> bool:
    """Whether `path` is a regular file itself, not a link to one."""
    try:
        return stat.S_ISREG(path.lstat().st_mode)
    except OSError:
        return False


@dataclass(frozen=True, slots=True)
class LockRecipe:
    """How one ecosystem produces a lockfile.

    `script` runs under `sh -c` with the project read-only at /project
    and nothing else from the host. It copies the project into WORKDIR
    and resolves there, since resolvers write beside the manifest. What
    it prints goes to stderr: stdout carries the lockfiles it leaves in
    WORKDIR, as a tar (`container_script`).
    """

    image: str
    #: The manifest the resolver reads. A directory holding it, and none
    #: of `produces`, is one the recipe runs on (`recipes_for`).
    manifest: str
    #: Lockfile names the recipe is expected to leave in WORKDIR, each one
    #: a file Syft reads. They are also how a project that needs no
    #: resolving is recognised (see `shipped_by`), so they must cover
    #: every lockfile name of the ecosystem that Syft reads. Composer and
    #: Bundler have one each: Syft finds nothing in `gems.locked`,
    #: Bundler's other name, on 1.41.2 or on 1.52.0.
    produces: tuple[str, ...]
    script: str
    #: Environment the container starts with, for variables an image
    #: bakes in and the script cannot undo.
    #:
    #: `maven:3.9.9` sets `MAVEN_CONFIG=/root/.m2` and runs
    #: `mvn-entrypoint.sh` *before* `sh -c`, so an `export` inside the
    #: script is too late: the entrypoint has already tried to create
    #: `/root` on a read-only filesystem. That failure is not fatal on
    #: its own — the entrypoint says "Carrying on" — but it is the only
    #: thing on stderr, so it hid the real error underneath it for two
    #: rounds of debugging.
    env: tuple[tuple[str, str], ...] = ()

    @property
    def fingerprint(self) -> str:
        """What tells this recipe from another version of it: a digest of
        everything it is. The resolver keeps a failure by it
        (`resolver.state`), so that a recipe whose image or script moved
        tries again what the last one could not."""
        spelled = json.dumps([
            self.image, self.manifest, list(self.produces), self.script,
            [list(pair) for pair in self.env],
        ])
        return hashlib.sha256(spelled.encode()).hexdigest()[:16]

    def shipped_by(self, project_dir: Path) -> tuple[str, ...]:
        """The lockfiles `project_dir` already has, by name.

        A project that ships its lockfile has nothing to resolve, and
        nothing may be merged over it. It is what the project pins, and
        resolving again pins whatever the registry offers that day: a
        committed `composer.lock` pinning x/y 1.0.0 was scanned as the
        1.9.3 of a resolved copy merged over it. Anything at the path
        counts, whatever it is.
        """
        return tuple(
            name for name in self.produces
            if os.path.lexists(project_dir / name)
        )

    def generated_in(self, output_dir: Path) -> tuple[Path, ...]:
        """The lockfiles a resolution left in `output_dir`.

        Only names in `produces`, and only regular files. What a
        resolver sends back is the project's choice, since it runs
        project-controlled code, and so was what it left here when it
        had this directory mounted writable: a link to any path on the
        host, which `sbom generate` would read with the collector's
        privileges, or another ecosystem's lockfile, which Syft would
        add to the SBOM. `generate_lockfile` writes no such thing now,
        and earlier runs may have.
        """
        found: list[Path] = []
        for name in self.produces:
            path = output_dir / name
            if _is_regular_file(path):
                found.append(path)
            elif os.path.lexists(path):
                logger.warning(
                    'Ignoring a lockfile that is not a regular file',
                    path=str(path),
                )
        return tuple(found)


# Images are pinned by digest. A tag moves with every rebuild of its
# image, so `composer:2.8` resolved the same project with another
# composer or PHP from one week to the next, and the lockfiles were not
# reproducible. The tag is kept for whoever reads it; Docker pulls by the
# digest. Each is the multi-platform index, as the registry serves it for
# the tag: `docker buildx imagetools inspect composer:2.10` prints the
# current one, to move a pin on deliberately.
#
# Keyed by ecosystem (the canonical names of `core/ecosystems.py`), not
# by the repository's language: a recipe runs wherever its manifest is,
# so a Composer project under `backend/` of a repository labelled
# TypeScript is resolved like any other.
LOCK_RECIPES: dict[str, LockRecipe] = {
    # Composer 2.9 leaves out of `update` every version a security
    # advisory names, and 2.10 every version a malware list names, as a
    # project's own composer.json may also ask it to. The lockfile was
    # then no longer what the constraints resolve to, and a project
    # whose constraints admit only such versions got none: "found
    # guzzlehttp/guzzle[6.3.0, ..., 6.3.3] but these were not loaded,
    # because they are affected by security advisories". COMPOSER_POLICY=0
    # turns every policy off, over whatever composer.json says, and
    # --no-audit skips the report on them, so the lockfile is what 2.8
    # wrote for the same project.
    'composer': LockRecipe(
        image=(
            'composer:2.10@sha256:'
            '9715c7f69044da2a212a5fbde29ee7da24e364d426560ae6367b060236f847d7'
        ),
        manifest='composer.json',
        produces=('composer.lock',),
        script=(
            f'cp -r {PROJECT_MOUNT}/. {WORKDIR}; cd {WORKDIR}; '
            'COMPOSER_HOME=/tmp/composer COMPOSER_POLICY=0 composer update '
            '--no-install --no-scripts --no-plugins --no-interaction '
            '--ignore-platform-reqs --no-audit'
        ),
    ),
    # Bundler resolves for the Ruby it runs on, 4.0 here: a gem whose
    # every version the Gemfile admits excludes 4.0 by its
    # `required_ruby_version` does not resolve, and a precompiled gem
    # that excludes it gives way to the gem built from source. Bundler 4
    # refuses a Gemfile with more than one global `source`, which 2.5
    # took with a warning.
    #
    # HOME on the tmpfs, with the rest of what it writes: the container
    # runs as the invoking user, or as nobody, whose home is
    # /nonexistent, and Bundler said "`/nonexistent` is not a directory"
    # on every resolution before it made a home of its own (#118).
    'gem': LockRecipe(
        image=(
            'ruby:4.0-slim@sha256:'
            'db9ddd17cc6ac603f2497d98ac5c88e4118908d6f9a45f2422ebee141f91e485'
        ),
        manifest='Gemfile',
        produces=('Gemfile.lock',),
        script=(
            f'cp -r {PROJECT_MOUNT}/. {WORKDIR}; cd {WORKDIR}; '
            'export HOME=/tmp GEM_HOME=/tmp/gems BUNDLE_PATH=/tmp/bundle; '
            'bundle lock --update'
        ),
    ),
}

#: Ecosystems whose recipe was withdrawn, and why, so that `sbom lock`
#: can say so. Both wrote a file Syft never reads: Syft 1.41.2 finds no
#: package in `dependency-tree.txt` or `requirements.lock`, and every
#: package in the same text named `requirements.txt`, as 1.52.0 does.
#: So each resolution ran project-controlled code in a container for a
#: scan that came out the same. The recipes as they were are in 72b80c1.
DISABLED_RECIPES: dict[str, str] = {
    # Java cannot work on this corpus in any case, and the reason is
    # upstream of this file. `06-github-content` stores manifests, not
    # source trees — by design, because that is all Syft needs to tell
    # a declared dependency from an inherited one. Measured over 60
    # sampled Java projects: 43 have no `pom.xml` at all, the stored
    # tree has a median size of 1 KB, and 10 of the 17 that do have one
    # declare `<modules>`, which Maven cannot resolve without them:
    #
    #     [ERROR] Child module /tmp/p/mall-common of /tmp/p/pom.xml
    #             does not exist
    #
    # A Java recipe needs `github content` to store the module POMs
    # first, a collection change with its own storage cost, and then an
    # output Syft reads (TODO.md, section E). PHP resolves at 72%
    # because `composer.json` is self-contained.
    'maven': (
        'Syft never reads the dependency-tree.txt that `mvn '
        'dependency:tree` writes (its Java cataloger reads pom.xml, '
        'gradle.lockfile* and archives), and a multi-module POM cannot '
        'be resolved from the manifests 06-github-content stores '
        '(TODO.md, section E)'
    ),
    # The Python recipe most likely never ran at all. It called `pip
    # install` on a read-only root with no writable HOME, and both of
    # its `uv pip compile` attempts named a `pyproject.toml`, which a
    # project with only a `requirements.txt` does not have. There was
    # no Docker daemon to confirm the first. A replacement would write
    # `*requirements*.txt`, keep HOME and every cache under /tmp, and
    # count `poetry.lock`, `uv.lock`, `Pipfile.lock` and `pdm.lock` as
    # shipped lockfiles, which `produces` alone cannot say.
    'pypi': (
        'Syft never reads the requirements.lock the recipe wrote (its '
        'Python cataloger reads *requirements*.txt, poetry.lock, '
        'Pipfile.lock, setup.py, uv.lock and pdm.lock)'
    ),
}


def lock_recipe_for(ecosystem: str) -> LockRecipe:
    """The lockfile recipe for an ecosystem.

    Go, Cargo and npm projects are absent on purpose: their ecosystems
    commit lockfiles as a matter of course, so Syft already reads them
    (Go coverage is 90%, Rust 69%). Maven and PyPI had recipes and have
    none now; the error says why (`DISABLED_RECIPES`).
    """
    try:
        return LOCK_RECIPES[str(ecosystem)]
    except KeyError:
        reason = DISABLED_RECIPES.get(str(ecosystem))
        raise ValueError(
            f'no lockfile recipe for {ecosystem}'
            + (f': {reason}' if reason else '')
            + f"; supported: {', '.join(str(k) for k in LOCK_RECIPES)}",
        ) from None


#: Directories one repository may have resolved. Each is a container
#: run of up to `SandboxLimits.timeout`, over project-controlled code.
MAX_LOCK_DIRECTORIES = 10


@dataclass(frozen=True, slots=True)
class LockTarget:
    """One directory of a repository that one recipe should resolve."""

    #: The directory within the repository, '' for its root.
    directory: str
    ecosystem: str
    recipe: LockRecipe

    def within(self, root: Path) -> Path:
        """This directory under `root` (a content root, or a
        generated-lock directory)."""
        return root.joinpath(*self.directory.split('/')) if self.directory else root


def recipes_for(
    paths: Iterable[str],
    limit: int = MAX_LOCK_DIRECTORIES,
) -> list[LockTarget]:
    """Where each recipe should run, from a repository's manifests.

    `paths` are repository paths: the discovery list, or the files of a
    content root. A directory is resolved by a recipe when it holds the
    recipe's manifest and none of its lockfiles: a lockfile it ships is
    what it pins, and resolving again would pin whatever the registry
    offers that day.

    Shallowest first, then by path, and at most `limit` directories, so
    the same repository always resolves the same ones.
    """
    by_directory: dict[str, set[str]] = {}
    for path in paths:
        pure = PurePosixPath(path)
        directory = '' if str(pure.parent) == '.' else str(pure.parent)
        by_directory.setdefault(directory, set()).add(pure.name)
    targets: list[LockTarget] = []
    for directory in sorted(
        by_directory,
        key=lambda d: (d.count('/') + (1 if d else 0), d),
    ):
        names = by_directory[directory]
        for ecosystem, recipe in LOCK_RECIPES.items():
            if recipe.manifest in names and not names & set(recipe.produces):
                targets.append(LockTarget(directory, ecosystem, recipe))
    return targets[:limit]


def container_script(recipe: LockRecipe) -> str:
    """The shell script a resolver's container runs.

    The recipe, with everything it prints sent to stderr, and then its
    lockfiles as a tar on stdout. That is the only way anything leaves:
    no host path is writable, so what the resolver leaves behind goes
    with its tmpfs, and what it sends back is read under a cap and
    checked (`generate_lockfile`). `set -e` first, so that a step of the
    recipe that fails ends the script before the tar: a failed
    resolution sends nothing.
    """
    names = shlex.join(recipe.produces)
    return (
        f'set -e; {{ {recipe.script}; }} >&2; '
        f'tar -cf - -C {WORKDIR} {names}'
    )


def build_docker_command(
    recipe: LockRecipe,
    project_dir: Path,
    limits: SandboxLimits,
    name: str,
    network: str = LOCK_NETWORK,
    rootless_daemon: bool = False,
) -> list[str]:
    """The hardened `docker run` argv for one resolution.

    Built as a list, never a shell string: the project path comes from a
    repository name and must not be re-parsed by a shell.

    `name` is what the container is removed by when the run is cut
    short (`generate_lockfile`): killing this command does not stop the
    container it started.

    `rootless_daemon` drops `--user`, and only that. Under a rootless
    daemon the user namespace already maps container root to an
    unprivileged host uid, and an explicit uid lands on a subuid that
    owns nothing: it broke the write to the output mount there was, and
    reads only what anyone may. Every other restriction is unchanged.
    """
    identity: list[str] = [] if rootless_daemon else [
        '--user', limits.resolved_user(),
    ]

    return [
        'docker', 'run', '--rm',
        # What it is removed by: the client's death does not reach it.
        '--name', name,
        # An init as PID 1, which reaps what the resolver leaves behind.
        '--init',
        # A network of its own, on which resolvers cannot reach each
        # other (`lock_network`). Registries are reached over it; that
        # is the point.
        '--network', network,
        # The resolver must not be able to change the source tree. It is
        # the only host path there is: the lockfile comes back on stdout.
        '--mount',
        f'type=bind,source={project_dir.absolute()},'
        f'target={PROJECT_MOUNT},readonly',
        # Everything the container writes goes to memory and is lost.
        '--read-only',
        '--tmpfs', '/tmp:exec,size=2g',
        # Privilege surface.
        *identity,
        '--cap-drop', 'ALL',
        '--security-opt', 'no-new-privileges',
        # Resource ceilings, so one pathological project cannot stall a run.
        '--memory', limits.memory,
        '--memory-swap', limits.memory,
        '--cpus', limits.cpus,
        '--pids-limit', str(limits.pids),
        # Only what a recipe asks for. Nothing from this process's own
        # environment reaches the container: a resolver running
        # project-controlled code must not inherit a token.
        *[
            arg for key, value in recipe.env
            for arg in ('--env', f'{key}={value}')
        ],
        '--workdir', '/tmp',
        # The command below is run as it is. composer's entrypoint asks
        # `composer help` about the command's first word, and lets only
        # `sh` by without.
        '--entrypoint', '',
        recipe.image,
        # The container ends itself at the deadline as well, should
        # nothing on this side be left to remove it: `docker compose
        # down`, or a SIGKILL, ends this process before any `finally`.
        'timeout', '-s', 'KILL', str(limits.timeout),
        'sh', '-c', container_script(recipe),
    ]


#: `docker info` reports rootless mode as a security option.
ROOTLESS_MARKER = 'name=rootless'


def is_rootless_daemon_output(security_options: str) -> bool:
    """Whether `docker info`'s SecurityOptions indicate a rootless daemon."""
    return ROOTLESS_MARKER in security_options


@cache
def daemon_is_rootless() -> bool:
    """Whether the daemon we talk to runs rootless.

    This inverts the `--user` decision, which is why it is worth a probe
    rather than a guess. A rootful daemon maps container uid 1000 to host
    uid 1000, so passing the invoking uid is what lets the container read
    the project as its owner. A rootless daemon maps container *root* to
    the unprivileged host user instead, and an explicit `--user 1000`
    lands on a subuid that owns nothing: when the lockfile was written
    to a mounted directory, the resolver ran to completion and then
    failed with `cp: /out/Gemfile.lock: Permission denied`.

    Cached: it cannot change within a run, and the probe costs a
    round-trip per repository otherwise.
    """
    try:
        completed = subprocess.run(
            [
                'docker', 'info', '--format',
                '{{range .SecurityOptions}}{{.}} {{end}}',
            ],
            capture_output=True, text=True, timeout=20, check=True,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return is_rootless_daemon_output(completed.stdout)


def docker_available() -> bool:
    """Whether a usable Docker CLI and daemon are present."""
    if shutil.which('docker') is None:
        return False
    try:
        subprocess.run(
            ['docker', 'info'],
            capture_output=True, timeout=15, check=True,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return True


def _inter_container_traffic(network: str) -> str | None:
    """`network`'s inter-container setting as the daemon reports it, or
    None if there is no such network."""
    completed = subprocess.run(
        [
            'docker', 'network', 'inspect', '--format',
            '{{index .Options "' + ICC_OPTION + '"}}', network,
        ],
        capture_output=True, text=True, timeout=30,
    )
    return completed.stdout.strip() if completed.returncode == 0 else None


@cache
def lock_network() -> str:
    """The network resolvers run on, made if it does not exist yet.

    A bridge of their own rather than the daemon's default one, with
    traffic between its containers turned off: under `sbom lock
    --workers`, several resolvers run at once, each one
    project-controlled, and none should reach another. They reach the
    registries through it, so it is not `--internal`.

    A network of that name that lets its containers talk is refused
    rather than used: made by hand, or by something else, it is not the
    one this relies on. Raises `SandboxError`.

    Cached: it cannot change within a run.
    """
    try:
        setting = _inter_container_traffic(LOCK_NETWORK)
        if setting is None:
            created = subprocess.run(
                [
                    'docker', 'network', 'create', '--driver', 'bridge',
                    '--opt', f'{ICC_OPTION}=false', LOCK_NETWORK,
                ],
                capture_output=True, text=True, timeout=30,
            )
            # Read back, not trusted: another run may have made it in
            # the meantime, in which case this one was told it exists.
            setting = _inter_container_traffic(LOCK_NETWORK)
            if setting is None:
                raise SandboxError(
                    f'could not create the network {LOCK_NETWORK}: '
                    f'{created.stderr.strip()}',
                )
    except (OSError, subprocess.SubprocessError) as e:
        raise SandboxError(
            f'could not set up the network {LOCK_NETWORK}: {e}',
        ) from e
    if setting != 'false':
        raise SandboxError(
            f'the network {LOCK_NETWORK} lets its containers reach each '
            f'other; remove it (docker network rm {LOCK_NETWORK}) and it '
            'is made again as it should be',
        )
    return LOCK_NETWORK


@dataclass(frozen=True, slots=True)
class LockResult:
    """Outcome of one containerised resolution."""

    produced: tuple[Path, ...]
    returncode: int
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and bool(self.produced)


class _Stopped(Exception):
    """A run the sandbox ended itself, before the container did."""

    def __init__(self, returncode: int, reason: str, stderr: str) -> None:
        super().__init__(reason)
        self.returncode = returncode
        self.reason = reason
        self.stderr = stderr


def _tail(stderr: bytearray) -> str:
    return bytes(stderr[-STDERR_TAIL:]).decode('utf-8', errors='replace')


def _excerpt(stderr: str, keep: int = STDERR_LOGGED) -> str:
    """`stderr` whole if it is short, else `keep` characters from each
    end of it, and how many between them were left out."""
    if len(stderr) <= 2 * keep:
        return stderr
    left_out = len(stderr) - 2 * keep
    return (
        f'{stderr[:keep]}\n[... {left_out:,} characters left out ...]\n'
        f'{stderr[-keep:]}'
    )


def _collect(
    process: subprocess.Popen[bytes],
    limits: SandboxLimits,
    cancel: threading.Event | None,
) -> tuple[int, bytes, str]:
    """Read a run to its end: its exit status, its stdout, and the tail
    of its stderr.

    Both pipes are read as they fill, so that neither stalls the run.
    Stdout is kept whole, up to `limits.output_bytes`; stderr only its
    last `STDERR_TAIL` bytes. Raises `_Stopped` once the deadline has
    passed, stdout is over the cap, or `cancel` is set.
    """
    assert process.stdout is not None and process.stderr is not None
    deadline = time.monotonic() + limits.timeout
    stdout = bytearray()
    stderr = bytearray()
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ, stdout)
        selector.register(process.stderr, selectors.EVENT_READ, stderr)
        while selector.get_map():
            if cancel is not None and cancel.is_set():
                raise _Stopped(INTERRUPTED, 'cancelled', _tail(stderr))
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise _Stopped(
                    TIMED_OUT, f'timed out after {limits.timeout}s',
                    _tail(stderr),
                )
            for key, _ in selector.select(min(remaining, POLL_SECONDS)):
                chunk = os.read(key.fd, _CHUNK)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                key.data.extend(chunk)
                if len(stdout) > limits.output_bytes:
                    raise _Stopped(
                        KILLED,
                        f'sent more than {limits.output_bytes:,} bytes',
                        _tail(stderr),
                    )
                if len(stderr) > 2 * STDERR_TAIL:
                    del stderr[:-STDERR_TAIL]
    # Both pipes are closed, so the client is on its way out.
    try:
        returncode = process.wait(
            timeout=max(deadline - time.monotonic(), POLL_SECONDS),
        )
    except subprocess.TimeoutExpired:
        raise _Stopped(
            TIMED_OUT, f'timed out after {limits.timeout}s', _tail(stderr),
        ) from None
    return returncode, bytes(stdout), _tail(stderr)


def remove_container(name: str) -> None:
    """`docker rm -f` a resolver's container, as far as that goes.

    Never raises: it runs on the way out of a failure, and must not
    replace it. A container this cannot remove is logged by name, and
    ends itself at its own deadline (`build_docker_command`).
    """
    try:
        completed = subprocess.run(
            ['docker', 'rm', '-f', name],
            capture_output=True, text=True, timeout=REMOVE_TIMEOUT,
        )
    except (OSError, subprocess.SubprocessError) as e:
        logger.warning(
            'Could not remove a resolver container', name=name, error=str(e),
        )
        return
    gone = 'No such container' in completed.stderr
    if completed.returncode != 0 and not gone:
        logger.warning(
            'Could not remove a resolver container',
            name=name, error=completed.stderr.strip(),
        )


def _run(
    command: list[str],
    name: str,
    limits: SandboxLimits,
    cancel: threading.Event | None,
) -> tuple[int, bytes, str]:
    """Run one `docker run` and read it to its end (`_collect`).

    Killing `docker run` does not stop its container: SIGKILL is not
    passed on, and the container runs on, with its memory and CPUs,
    until the resolver exits by itself. So however the wait ends early
    — the deadline, the output cap, `cancel`, an exception, Ctrl-C —
    the client is killed and collected, and then the container is
    removed by name, before this returns or raises. The client first,
    or one still pulling the image could start the container after it
    was removed. After a clean exit `--rm` has removed it; any other
    exit may have been the client's own, with the container still
    running, and is removed too.
    """
    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    returncode: int | None = None
    try:
        returncode, stdout, stderr = _collect(process, limits, cancel)
        return returncode, stdout, stderr
    finally:
        if process.poll() is None:
            process.kill()
        try:
            process.wait(timeout=REMOVE_TIMEOUT)
        except subprocess.TimeoutExpired:
            logger.warning('A docker client outlived SIGKILL', name=name)
        for pipe in (process.stdout, process.stderr):
            if pipe is not None:
                pipe.close()
        if returncode != 0:
            remove_container(name)


def _lockfiles_in(archive: bytes, recipe: LockRecipe) -> dict[str, bytes]:
    """The lockfiles in the tar a resolver sent back, by name.

    Only regular files, and only names in `recipe.produces`, each once.
    The tar is the resolver's to write: project code runs as the same
    user as the script that makes it. So a link, which `sbom generate`
    would follow to wherever it points on the host, a path out of the
    directory, and another ecosystem's lockfile, which Syft would add to
    the SBOM, are dropped with a warning. Nothing is extracted: members
    are read into memory, which `SandboxLimits.output_bytes` bounds.
    """
    found: dict[str, bytes] = {}
    try:
        with tarfile.open(fileobj=io.BytesIO(archive), mode='r:') as tar:
            for member in tar:
                if member.name not in recipe.produces:
                    logger.warning(
                        'Ignoring what the resolver sent besides its lockfile',
                        name=member.name,
                    )
                elif not member.isreg():
                    logger.warning(
                        'Ignoring a lockfile that is not a regular file',
                        name=member.name,
                    )
                elif member.name in found:
                    logger.warning(
                        'Ignoring a second copy of a lockfile',
                        name=member.name,
                    )
                else:
                    content = tar.extractfile(member)
                    if content is not None:
                        found[member.name] = content.read()
    except tarfile.TarError as e:
        logger.warning('The resolver sent back no readable tar', error=str(e))
        return {}
    return found


def generate_lockfile(
    ecosystem: str,
    project_dir: Path,
    output_dir: Path,
    limits: SandboxLimits | None = None,
    cancel: threading.Event | None = None,
) -> LockResult:
    """Resolve a project's dependencies inside a container.

    `project_dir` is the directory holding the manifest, which may be a
    subdirectory of a content root; only it is mounted, read-only. The
    lockfiles come back as a tar on the container's stdout (`_run`), and
    only those `_lockfiles_in` keeps are written into `output_dir`, each
    whole or not at all. A run that fails, or that the sandbox stopped,
    writes nothing, and `output_dir` is made only to be written to.

    Returns rather than raises on resolver failure: in a batch over
    thousands of repositories, a project that does not resolve is
    expected, not exceptional. `cancel`, once set, ends the run as the
    deadline does. Ctrl-C, or any exception, is raised once the
    container is removed.
    """
    limits = limits or SandboxLimits()
    recipe = lock_recipe_for(ecosystem)
    if cancel is not None and cancel.is_set():
        return LockResult(
            produced=(), returncode=INTERRUPTED, stderr='cancelled',
        )

    name = f'{CONTAINER_PREFIX}{uuid.uuid4().hex}'
    command = build_docker_command(
        recipe, project_dir, limits, name,
        network=lock_network(), rootless_daemon=daemon_is_rootless(),
    )

    try:
        returncode, archive, stderr = _run(command, name, limits, cancel)
    except _Stopped as stopped:
        logger.warning(
            'Lockfile generation stopped, and its container removed',
            project=str(project_dir),
            ecosystem=str(ecosystem),
            reason=stopped.reason,
        )
        return LockResult(
            produced=(),
            returncode=stopped.returncode,
            stderr='\n'.join(filter(None, [stopped.stderr, stopped.reason])),
        )
    except OSError as e:
        return LockResult(produced=(), returncode=127, stderr=str(e))

    produced: tuple[Path, ...] = ()
    if returncode == 0:
        lockfiles = _lockfiles_in(archive, recipe)
        for filename, content in lockfiles.items():
            atomic_write_bytes(output_dir / filename, content)
        produced = tuple(
            path for path in recipe.generated_in(output_dir)
            if path.name in lockfiles
        )

    if not produced:
        logger.info(
            'No lockfile produced',
            project=str(project_dir),
            ecosystem=str(ecosystem),
            returncode=returncode,
            stderr=_excerpt(stderr),
        )

    return LockResult(produced=produced, returncode=returncode, stderr=stderr)
