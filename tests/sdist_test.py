"""The sdist is the package, not the repository (#28).

Told nothing about what to take, hatchling put everything git does not
ignore in the sdist: 40 MB, most of it screen recordings, with the
dashboard, the deployment files and this suite around the package. It
is built here as `uv build` builds it, by the backend itself, in
process and offline, and checked by the script CI runs on the sdist it
builds.
"""
import importlib.util
import io
import os
import tarfile
from pathlib import Path
from types import ModuleType

from hatchling.builders.sdist import SdistBuilder

ROOT = Path(__file__).resolve().parent.parent


def check_sdist() -> ModuleType:
    """The script CI runs, loaded as a module: scripts/ is not a package."""
    path = ROOT / 'scripts' / 'check_sdist.py'
    spec = importlib.util.spec_from_file_location('check_sdist', path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build_sdist(directory: Path) -> Path:
    """The sdist `uv build` makes: hatchling's PEP 517 `build_sdist` is
    this call, made from the project's root."""
    [sdist] = SdistBuilder(str(ROOT)).build(
        directory=str(directory), versions=['standard'],
    )
    return Path(sdist)


def sdist_of(directory: Path, files: dict[str, bytes]) -> Path:
    """An sdist holding `files`, in the directory it unpacks into."""
    path = directory / 'chatsbom-0.0.0.tar.gz'
    with tarfile.open(path, 'w:gz') as archive:
        for name, data in files.items():
            member = tarfile.TarInfo(f'chatsbom-0.0.0/{name}')
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    return path


def test_the_sdist_is_the_package_not_the_repository(tmp_path):
    found = check_sdist().problems(build_sdist(tmp_path))
    assert not found, '\n'.join(found)


def test_the_check_finds_the_repository_in_an_sdist(tmp_path):
    """Or the test above would pass on a check that looked at nothing.
    The recording is random bytes, which do not compress."""
    sdist = sdist_of(
        tmp_path, {
            'pyproject.toml': b'',
            'chatsbom/__init__.py': b'',
            'figures/input.mp4': os.urandom(2_100_000),
            'tests/conftest.py': b'',
            'tests/sdist_test.py': b'',
            'web/node_modules/workerd/bin/workerd': b'',
        },
    )
    assert check_sdist().problems(sdist) == [
        f'{sdist.stat().st_size:,} bytes, over the limit of 2,000,000',
        'figures/ is in it: 1 file',
        'tests/ is in it: 2 files',
        'web/ is in it: 1 file',
        'node_modules/ is in it: 1 file',
        'PKG-INFO is missing',
        'README.md is missing',
        'LICENSE is missing',
        'chatsbom/__main__.py is missing',
    ]
