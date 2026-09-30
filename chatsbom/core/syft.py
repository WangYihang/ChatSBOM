"""The Syft the collector's image runs: how to install it here, and
how to read the version a Syft says it is."""
import json
import platform
import re

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


def install_hint() -> str:
    """How to install `SYFT_VERSION` here, in a line: where the platform
    has a digest pinned, the commands, joined by `&&` so that pasted
    together they stop at the check when it fails, before anything is
    taken out of an archive that did not pass it; elsewhere, the
    release's archive, checked against its checksums file.

    It suggested `curl -sSfL https://get.anchore.io/syft | sudo sh`: an
    installer run as root, installing whichever release was newest, and
    checking the archive in a way that only logs a mismatch (#118).
    """
    commands = install_commands()
    if commands:
        return (
            f"Install Syft {SYFT_VERSION}, the one the collector's image "
            'runs, its archive checked against the digest pinned for it: '
            + ' && '.join(commands)
        )
    return (
        f"Install Syft {SYFT_VERSION}, the one the collector's image runs: "
        f'the archive for this platform from {SYFT_RELEASE}, checked '
        f'against syft_{SYFT_VERSION}_checksums.txt beside it before '
        'anything is taken out of it'
    )


_VERSION_RE = re.compile(r'(\d+\.\d+\.\d+\S*)')


def parse_syft_version(output: str) -> str | None:
    """The version `syft version -o json` printed: its JSON's `version`,
    or else the first thing in it shaped like a version; None if it
    printed neither. The collector's pool asks its own Syft with it."""
    try:
        return str(json.loads(output)['version'])
    except (json.JSONDecodeError, KeyError, TypeError):
        pass

    match = _VERSION_RE.search(output)
    return match.group(1) if match else None
