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
mounted read-only, no host paths beyond one output directory, no
privileges, a read-only root filesystem, and bounded memory, CPU and
process count.

Network access is the one thing that cannot be removed: resolution is
precisely the act of fetching dependency metadata from a registry. That
is the residual risk, and it is why nothing else is granted.
"""
import os
import shutil
import stat
import subprocess
from dataclasses import dataclass
from functools import cache
from pathlib import Path

import structlog

from chatsbom.models.language import Language

logger = structlog.get_logger('sandbox')

#: Where the read-only project and the writable output land in the container.
PROJECT_MOUNT = '/project'
OUTPUT_MOUNT = '/out'


#: Fallback identity when the caller is root: unprivileged and owns
#: nothing on the host.
NOBODY = '65534:65534'


def _invoking_user() -> str:
    """uid:gid the container should run as.

    The container writes the lockfile into a host directory, so it must
    run as an identity that can write there — which is the invoking user,
    not root and not `nobody`. Running as root inside the container means
    root on the bind mount, so a root caller falls back to `nobody` and
    the output directory is widened instead.
    """
    uid = os.getuid()
    return NOBODY if uid == 0 else f'{uid}:{os.getgid()}'


@dataclass(frozen=True, slots=True)
class SandboxLimits:
    """Resource ceilings for one container run."""

    memory: str = '2g'
    cpus: str = '2'
    pids: int = 256
    #: Wall-clock seconds before the container is killed.
    timeout: int = 300
    #: Numeric uid:gid inside the container. Never 0 — root in the
    #: container is root on a bind mount.
    user: str = ''

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

    `script` runs as `sh -c` with the project read-only at /project and a
    writable /out. It must copy the project into /tmp before resolving,
    since resolvers write beside the manifest.
    """

    image: str
    #: Lockfile names the recipe is expected to leave in /out, each one
    #: a file Syft reads. They are also how a project that needs no
    #: resolving is recognised (see `shipped_by`), so they must cover
    #: every lockfile name of the ecosystem that Syft reads. Composer and
    #: Bundler have one each: Syft 1.41.2 finds nothing in
    #: `gems.locked`, Bundler's other name.
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

        Only names in `produces`, and only regular files. The resolver
        runs project-controlled code with /out writable, so the project
        decides what else is there: a link to any path on the host,
        which `sbom generate` would read with the collector's
        privileges, or another ecosystem's lockfile, which Syft would
        add to the SBOM.
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


# Images are pinned to explicit versions: `latest` would make the
# generated lockfiles irreproducible across runs.
LOCK_RECIPES: dict[Language, LockRecipe] = {
    Language.PHP: LockRecipe(
        image='composer:2.8',
        produces=('composer.lock',),
        script=(
            'set -e; '
            f'cp -r {PROJECT_MOUNT}/. /tmp/p; cd /tmp/p; '
            'COMPOSER_HOME=/tmp/composer composer update '
            '--no-install --no-scripts --no-plugins --no-interaction '
            '--ignore-platform-reqs; '
            f'cp composer.lock {OUTPUT_MOUNT}/composer.lock'
        ),
    ),
    Language.RUBY: LockRecipe(
        image='ruby:3.3-slim',
        produces=('Gemfile.lock',),
        script=(
            'set -e; '
            f'cp -r {PROJECT_MOUNT}/. /tmp/p; cd /tmp/p; '
            'export GEM_HOME=/tmp/gems BUNDLE_PATH=/tmp/bundle; '
            'bundle lock --update; '
            f'cp Gemfile.lock {OUTPUT_MOUNT}/Gemfile.lock'
        ),
    ),
}

#: Ecosystems whose recipe was withdrawn, and why, so that `sbom lock`
#: can say so. Both wrote a file Syft never reads: Syft 1.41.2 finds no
#: package in `dependency-tree.txt` or `requirements.lock`, and every
#: package in the same text named `requirements.txt`. So each resolution
#: ran project-controlled code in a container for a scan that came out
#: the same. The recipes as they were are in 72b80c1.
DISABLED_RECIPES: dict[Language, str] = {
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
    Language.JAVA: (
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
    Language.PYTHON: (
        'Syft never reads the requirements.lock the recipe wrote (its '
        'Python cataloger reads *requirements*.txt, poetry.lock, '
        'Pipfile.lock, setup.py, uv.lock and pdm.lock)'
    ),
}


def lock_recipe_for(language: Language) -> LockRecipe:
    """The lockfile recipe for a language.

    Go, Rust, npm and Cargo projects are absent on purpose: their
    ecosystems commit lockfiles as a matter of course, so Syft already
    reads them (Go coverage is 90%, Rust 69%). Java and Python had
    recipes and have none now; the error says why (`DISABLED_RECIPES`).
    """
    try:
        return LOCK_RECIPES[language]
    except KeyError:
        reason = DISABLED_RECIPES.get(language)
        raise ValueError(
            f'no lockfile recipe for {language}'
            + (f': {reason}' if reason else '')
            + f"; supported: {', '.join(str(k) for k in LOCK_RECIPES)}",
        ) from None


def build_docker_command(
    recipe: LockRecipe,
    project_dir: Path,
    output_dir: Path,
    limits: SandboxLimits,
    rootless_daemon: bool = False,
) -> list[str]:
    """The hardened `docker run` argv for one resolution.

    Built as a list, never a shell string: the project path comes from a
    repository name and must not be re-parsed by a shell.

    `rootless_daemon` drops `--user`, and only that. Under a rootless
    daemon the user namespace already maps container root to an
    unprivileged host uid, so pinning a uid inside the container both
    loses that mapping and breaks the output write. Every other
    restriction is unchanged.
    """
    identity: list[str] = [] if rootless_daemon else [
        '--user', limits.resolved_user(),
    ]

    return [
        'docker', 'run', '--rm',
        # No interactive TTY, no stdin from the host.
        '--init',
        # The resolver must not be able to change the source tree.
        '--mount',
        f'type=bind,source={project_dir.absolute()},'
        f'target={PROJECT_MOUNT},readonly',
        # The single writable path, holding only the lockfile.
        '--mount',
        f'type=bind,source={output_dir.absolute()},target={OUTPUT_MOUNT}',
        # Everything else the container writes goes to memory and is lost.
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
            arg for name, value in recipe.env
            for arg in ('--env', f'{name}={value}')
        ],
        # Registries are reached over the network; that is the point.
        '--workdir', '/tmp',
        recipe.image,
        'sh', '-c', recipe.script,
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
    uid 1000, so passing the invoking uid is what lets the container write
    the bind-mounted output directory. A rootless daemon maps container
    *root* to the unprivileged host user instead, and an explicit
    `--user 1000` lands on a subuid that owns nothing: the resolver runs
    to completion and then fails with
    `cp: /out/Gemfile.lock: Permission denied`.

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


@dataclass(frozen=True, slots=True)
class LockResult:
    """Outcome of one containerised resolution."""

    produced: tuple[Path, ...]
    returncode: int
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and bool(self.produced)


def generate_lockfile(
    language: Language,
    project_dir: Path,
    output_dir: Path,
    limits: SandboxLimits | None = None,
) -> LockResult:
    """Resolve a project's dependencies inside a container.

    Returns rather than raises on resolver failure: in a batch over
    thousands of repositories, a project that does not resolve is
    expected, not exceptional.
    """
    limits = limits or SandboxLimits()
    recipe = lock_recipe_for(language)
    rootless = daemon_is_rootless()
    output_dir.mkdir(parents=True, exist_ok=True)
    if not rootless and limits.resolved_user() == NOBODY:
        # The container is unprivileged and does not own this directory.
        output_dir.chmod(0o777)

    command = build_docker_command(
        recipe, project_dir, output_dir, limits, rootless_daemon=rootless,
    )

    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=limits.timeout,
            check=False,
        )
        returncode, stderr = completed.returncode, completed.stderr
    except subprocess.TimeoutExpired:
        logger.warning(
            'Lockfile generation timed out',
            project=str(project_dir), seconds=limits.timeout,
        )
        returncode, stderr = 124, f'timed out after {limits.timeout}s'
    except OSError as e:
        return LockResult(produced=(), returncode=127, stderr=str(e))

    produced = recipe.generated_in(output_dir)

    if not produced:
        logger.info(
            'No lockfile produced',
            project=str(project_dir),
            language=str(language),
            returncode=returncode,
            stderr=stderr[-400:] if stderr else '',
        )

    return LockResult(produced=produced, returncode=returncode, stderr=stderr)
