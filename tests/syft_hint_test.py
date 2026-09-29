"""What a command that needs Syft says when there is none (#122).

It suggested `curl -sSfL https://get.anchore.io/syft | sudo sh`: an
installer, run as root, installing whichever release was newest, and
checking the archive in a way that only logs a mismatch (#118). It
suggests the Syft the collector's image runs now, fetched from the
release and checked against the digest the image pins for it.
"""
import io
import re
from pathlib import Path

import pytest
import typer
from rich.console import Console

from chatsbom.core import syft

ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = (ROOT / 'Dockerfile').read_text()

#: The Syft the image installs, which the hint is to name.
[VERSION] = re.findall(r'^ARG SYFT_VERSION=(\S+)$', DOCKERFILE, re.M)


def pinned(architecture: str) -> str:
    """The digest the Dockerfile pins for this architecture's archive."""
    [digest] = re.findall(
        rf'^ARG SYFT_SHA256_{architecture.upper()}=([0-9a-f]{{64}})$',
        DOCKERFILE, re.M,
    )
    return digest


def hint(
    monkeypatch: pytest.MonkeyPatch, system: str, machine: str,
    width: int = 80,
) -> str:
    """What is printed with no syft on PATH, on this platform, in a
    terminal this wide."""
    monkeypatch.setattr(syft.shutil, 'which', lambda name: None)
    monkeypatch.setattr('platform.system', lambda: system)
    monkeypatch.setattr('platform.machine', lambda: machine)
    output = io.StringIO()
    with pytest.raises(typer.Exit) as stopped:
        syft.check_syft_installed(Console(file=output, width=width))
    assert stopped.value.exit_code == 1
    return output.getvalue()


@pytest.mark.parametrize(
    ('machine', 'architecture'),
    [('x86_64', 'amd64'), ('aarch64', 'arm64'), ('arm64', 'arm64')],
)
def test_on_linux_it_installs_the_image_s_release_checked(
    monkeypatch, machine, architecture,
):
    """The archive the image installs on this architecture, checked
    against the image's digest for it before anything is taken out.
    Each command is a line of its own, whole in an 80-column terminal:
    one Rich wrapped would be pasted as two. Pasted together, `&&`
    stops them at the check when it fails."""
    lines = hint(monkeypatch, 'Linux', machine).splitlines()
    archive = f'syft_{VERSION}_linux_{architecture}.tar.gz'
    fetch = lines.index(
        'curl -sSfLO https://github.com/anchore/syft/releases/download/'
        f'v{VERSION}/{archive} &&',
    )
    check = lines.index(
        f'echo "{pinned(architecture)}  {archive}" '
        '| sha256sum --check --strict &&',
    )
    # The archive's syft is uid 1001's, which tar run as root keeps.
    extract = lines.index(
        f'sudo tar -xzf {archive} --no-same-owner -C /usr/local/bin syft',
    )
    assert fetch + 1 == check == extract - 1


@pytest.mark.parametrize(
    ('system', 'machine'),
    [
        ('Linux', 'x86_64'), ('Linux', 'riscv64'), ('Darwin', 'arm64'),
        ('Windows', 'AMD64'),
    ],
)
def test_nothing_unpinned_or_unchecked_is_suggested(
    monkeypatch, system, machine,
):
    printed = hint(monkeypatch, system, machine, width=200)

    assert 'get.anchore.io' not in printed
    assert not re.search(r'\|\s*(sudo\s+)?(ba)?sh\b', printed)
    assert '@latest' not in printed
    assert 'brew install' not in printed


@pytest.mark.parametrize(
    ('system', 'machine'),
    [('Linux', 'riscv64'), ('Darwin', 'arm64'), ('Windows', 'AMD64')],
)
def test_elsewhere_it_names_the_release_and_its_checksums(
    monkeypatch, system, machine,
):
    """No digest is pinned for these, so the archive is the release's
    for the platform, checked against the release's checksums file."""
    printed = ' '.join(hint(monkeypatch, system, machine, width=200).split())

    assert f'https://github.com/anchore/syft/releases/tag/v{VERSION}' in (
        printed
    )
    assert f'syft_{VERSION}_checksums.txt' in printed
    assert 'sha256sum' not in printed


def test_it_is_said_on_stderr(monkeypatch, capsys):
    """Why a command stops is not its output (#114)."""
    monkeypatch.setattr(syft.shutil, 'which', lambda name: None)

    with pytest.raises(typer.Exit):
        syft.check_syft_installed()

    captured = capsys.readouterr()
    assert captured.out == ''
    assert 'Syft Not Found' in captured.err
