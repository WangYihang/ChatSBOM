"""Syft installation and connection utilities."""
import platform
import re
import shutil
import subprocess
from functools import cache

import typer
from rich.console import Console
from rich.panel import Panel

from chatsbom.core.logging import stderr_console

#: The Syft the collector's image installs, and the digest of its Linux
#: archive for each architecture, as the release's checksums file gives
#: them: the Dockerfile's SYFT_VERSION and SYFT_SHA256_*, which
#: syft_hint_test holds these to. The hint below suggests this one so
#: that the host and the image run one Syft: the version keys the SBOM
#: cache, and an SBOM another Syft wrote is regenerated (DEPLOY.md,
#: "Upgrading Syft").
SYFT_VERSION = '1.52.0'
SYFT_SHA256 = {
    'amd64': 'caeedb81fb0491615f1ebd1761e4145d41ee86dd2cc7bf80669f9f5ad9d6133d',
    'arm64': 'c46d5e4c28e12aa4c5becfaa343ef1c7f89045b6b895f2c21d471c62db09c706',
}
SYFT_RELEASE = f'https://github.com/anchore/syft/releases/tag/v{SYFT_VERSION}'
_DOWNLOADS = f'https://github.com/anchore/syft/releases/download/v{SYFT_VERSION}'

#: What `platform.machine()` says, as the release names the architecture.
_ARCHITECTURES = {
    'x86_64': 'amd64', 'amd64': 'amd64', 'aarch64': 'arm64', 'arm64': 'arm64',
}


def install_commands() -> list[str]:
    """The commands that install `SYFT_VERSION` here, its archive checked
    against the pinned digest before anything is taken out of it; none
    on a platform no digest is pinned for.

    `--no-same-owner`, as the image has it: the archive's `syft` is uid
    1001's, the release runner's, and tar run as root keeps an archive's
    owners.
    """
    architecture = _ARCHITECTURES.get(platform.machine().lower())
    if platform.system() != 'Linux' or architecture is None:
        return []
    archive = f'syft_{SYFT_VERSION}_linux_{architecture}.tar.gz'
    return [
        f'curl -sSfLO {_DOWNLOADS}/{archive}',
        f'echo "{SYFT_SHA256[architecture]}  {archive}" '
        '| sha256sum --check --strict',
        f'sudo tar -xzf {archive} --no-same-owner -C /usr/local/bin syft',
    ]


def check_syft_installed(console: Console | None = None) -> bool:
    """
    Check if the 'syft' command is available in the system PATH.
    If not, say how to install the one the collector's image runs, on
    stderr, and exit.

    It suggested `curl -sSfL https://get.anchore.io/syft | sudo sh`: an
    installer run as root, installing whichever release was newest, and
    checking the archive in a way that only logs a mismatch (#118).
    """
    console = console or stderr_console

    if shutil.which('syft'):
        return True

    commands = install_commands()
    if commands:
        how = (
            f'Install Syft {SYFT_VERSION}, the one the collector\'s image '
            'runs, with the commands below: the release\'s archive, '
            'checked against the digest pinned for it before anything is '
            'taken out of it.'
        )
    else:
        how = (
            f'Install Syft {SYFT_VERSION}, the one the collector\'s image '
            'runs: the archive for this platform from '
            f'[link={SYFT_RELEASE}]{SYFT_RELEASE}[/link], checked against '
            f'syft_{SYFT_VERSION}_checksums.txt beside it before anything '
            'is taken out of it.'
        )

    console.print()
    console.print(
        Panel(
            '[bold]Syft Not Found[/]\n\n'
            'This command requires [bold blue]Syft[/] to generate SBOMs.\n'
            'Official Repository: [link=https://github.com/anchore/syft][blue]https://github.com/anchore/syft[/link]\n\n'
            f'{how}\n\n'
            'After installation, ensure [bold]syft[/] is in your [bold]PATH[/].',
            title='[bold red]Dependency Missing[/]',
            title_align='left',
            border_style='red',
            padding=(1, 2),
        ),
    )
    # Below the panel and unwrapped, however narrow the terminal: a
    # command Rich wrapped would be pasted as two. Joined by `&&`, so
    # that pasted together they stop where one fails: at the check,
    # before anything is taken out of an archive that did not pass it.
    for at, command in enumerate(commands, start=1):
        console.print(
            command if at == len(commands) else f'{command} &&',
            soft_wrap=True, markup=False, highlight=False,
        )
    raise typer.Exit(1)


_VERSION_RE = re.compile(r'(\d+\.\d+\.\d+\S*)')


@cache
def get_syft_version() -> str | None:
    """The installed Syft version, or None if it cannot be determined.

    SBOM caches are keyed on this: the same input scanned by two Syft
    versions is two different results, and reusing the older one would
    silently mix versions across the dataset.
    """
    try:
        result = subprocess.run(
            ['syft', 'version', '-o', 'json'],
            capture_output=True, text=True, timeout=30, check=True,
        )
    except (OSError, subprocess.SubprocessError):
        return None

    import json
    try:
        return str(json.loads(result.stdout)['version'])
    except (json.JSONDecodeError, KeyError, TypeError):
        pass

    match = _VERSION_RE.search(result.stdout)
    return match.group(1) if match else None
