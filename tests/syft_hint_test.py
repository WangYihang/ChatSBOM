"""How to install Syft, as it is said where there is none (#122).

It suggested `curl -sSfL https://get.anchore.io/syft | sudo sh`: an
installer, run as root, installing whichever release was newest, and
checking the archive in a way that only logs a mismatch (#118). It
suggests the Syft the collector's image runs now, fetched from the
release and checked against the digest the image pins for it.

`sbom generate` said it in a panel, and stopped; it went with the old
pipeline (#171). The collector says it as it starts, and goes on
without a Syft (collector_process_run_test): in a log line, the
commands one line of their own.
"""
import re
from pathlib import Path

import pytest

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


def hint(monkeypatch: pytest.MonkeyPatch, system: str, machine: str) -> str:
    """What is said on this platform."""
    monkeypatch.setattr('platform.system', lambda: system)
    monkeypatch.setattr('platform.machine', lambda: machine)
    return syft.install_hint()


@pytest.mark.parametrize(
    ('machine', 'architecture'),
    [('x86_64', 'amd64'), ('aarch64', 'arm64'), ('arm64', 'arm64')],
)
def test_on_linux_it_installs_the_image_s_release_checked(
    monkeypatch, machine, architecture,
):
    """The archive the image installs on this architecture, checked
    against the image's digest for it before anything is taken out.
    The commands end the line, joined by `&&`: pasted together, they
    stop at the check when it fails."""
    said = hint(monkeypatch, 'Linux', machine)
    archive = f'syft_{VERSION}_linux_{architecture}.tar.gz'
    assert said.startswith(f'Install Syft {VERSION}')
    assert '\n' not in said
    assert said.endswith(
        ': ' + ' && '.join([
            'curl -sSfLO https://github.com/anchore/syft/releases/download/'
            f'v{VERSION}/{archive}',
            f'echo "{pinned(architecture)}  {archive}" '
            '| sha256sum --check --strict',
            # The archive's syft is uid 1001's, which tar run as root keeps.
            f'sudo tar -xzf {archive} --no-same-owner -C /usr/local/bin syft',
        ]),
    )


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
    printed = hint(monkeypatch, system, machine)

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
    printed = hint(monkeypatch, system, machine)

    assert f'https://github.com/anchore/syft/releases/tag/v{VERSION}' in (
        printed
    )
    assert f'syft_{VERSION}_checksums.txt' in printed
    assert 'sha256sum' not in printed
