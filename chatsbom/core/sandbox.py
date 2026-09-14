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


@dataclass(frozen=True, slots=True)
class LockRecipe:
    """How one ecosystem produces a lockfile.

    `script` runs as `sh -c` with the project read-only at /project and a
    writable /out. It must copy the project into /tmp before resolving,
    since resolvers write beside the manifest.
    """

    image: str
    #: Lockfile names the recipe is expected to leave in /out.
    produces: tuple[str, ...]
    script: str


# Images are pinned to explicit versions: `latest` would make the
# generated lockfiles irreproducible across runs.
LOCK_RECIPES: dict[Language, LockRecipe] = {
    Language.JAVA: LockRecipe(
        image='maven:3.9.9-eclipse-temurin-21',
        produces=('dependency-tree.txt',),
        script=(
            'set -e; '
            f'cp -r {PROJECT_MOUNT}/. /tmp/p; cd /tmp/p; '
            'mvn -q -B -o=false --no-transfer-progress '
            '-Dmaven.repo.local=/tmp/m2 '
            'dependency:tree -DoutputType=text '
            f'-DoutputFile={OUTPUT_MOUNT}/dependency-tree.txt'
        ),
    ),
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
    Language.PYTHON: LockRecipe(
        image='python:3.12-slim',
        produces=('requirements.lock',),
        script=(
            'set -e; '
            f'cp -r {PROJECT_MOUNT}/. /tmp/p; cd /tmp/p; '
            'pip install --quiet --no-input --disable-pip-version-check uv; '
            'python -m uv pip compile --quiet --no-header '
            f'-o {OUTPUT_MOUNT}/requirements.lock '
            'pyproject.toml requirements.txt 2>/dev/null || '
            'python -m uv pip compile --quiet --no-header '
            f'-o {OUTPUT_MOUNT}/requirements.lock pyproject.toml'
        ),
    ),
}


def lock_recipe_for(language: Language) -> LockRecipe:
    """The lockfile recipe for a language.

    Go, Rust, npm and Cargo projects are absent on purpose: their
    ecosystems commit lockfiles as a matter of course, so Syft already
    reads them (Go coverage is 90%, Rust 69%).
    """
    try:
        return LOCK_RECIPES[language]
    except KeyError:
        raise ValueError(
            f"no lockfile recipe for {language}; "
            f"supported: {', '.join(str(k) for k in LOCK_RECIPES)}",
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

    produced = tuple(
        output_dir / name
        for name in recipe.produces
        if (output_dir / name).exists()
    )

    if not produced:
        logger.info(
            'No lockfile produced',
            project=str(project_dir),
            language=str(language),
            returncode=returncode,
            stderr=stderr[-400:] if stderr else '',
        )

    return LockResult(produced=produced, returncode=returncode, stderr=stderr)
