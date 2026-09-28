#!/usr/bin/env python3
"""Is the sdist the package, and not the repository? (#28)

Told nothing about what to take, hatchling put everything git does not
ignore in the sdist: 40 MB, most of it screen recordings in figures/,
with the dashboard, the deployment files and the test suite around the
package. It reads the root .gitignore alone, so a developer's
web/node_modules, which web/.gitignore ignores, went in as well, and
the sdist came to 99.6 MB, against PyPI's limit of 100.

pyproject.toml names what the sdist takes now. This checks what a build
made of it, so that losing that list, or widening it, fails the build
rather than the upload: CI runs it on the sdist `uv build` makes, and
the suite on one it builds itself (tests/sdist_test.py).

Usage:

    uv build && python scripts/check_sdist.py dist/*.tar.gz

Standard library only, so it runs wherever the sdist was built. It says
what is wrong with each sdist, and exits 1 if anything is.
"""
import argparse
import sys
import tarfile
from pathlib import Path

#: Several times what the package compresses to, and less than any one
#: of the recordings in figures/.
MAX_BYTES = 2_000_000

#: The repository's, not the package's: the recordings, the dashboard,
#: CI, the deployment and the database's configuration, and a suite
#: that reads compose, the Dockerfiles, web/ and deploy/, so cannot run
#: from an sdist.
UNWANTED = ('.github', 'database', 'deploy', 'figures', 'tests', 'web')

#: npm's, at any depth, since hatchling reads no other .gitignore.
NODE_MODULES = 'node_modules'

#: What building the wheel from the sdist reads: its configuration, the
#: files the metadata names, and the package with its entry point. And
#: PKG-INFO, the sdist's own metadata.
REQUIRED = (
    'PKG-INFO',
    'pyproject.toml',
    'README.md',
    'LICENSE',
    'chatsbom/__init__.py',
    'chatsbom/__main__.py',
)


def files(count: int) -> str:
    return f'{count} file' if count == 1 else f'{count} files'


def members(sdist: Path) -> list[str]:
    """Each file in `sdist`, as a path inside the `name-version/`
    directory it unpacks into."""
    with tarfile.open(sdist, 'r:gz') as archive:
        return [
            member.name.partition('/')[2]
            for member in archive.getmembers()
            if not member.isdir()
        ]


def problems(sdist: Path) -> list[str]:
    """What is wrong with `sdist`, a line each: none for a good one."""
    found = []
    size = sdist.stat().st_size
    if size > MAX_BYTES:
        found.append(f'{size:,} bytes, over the limit of {MAX_BYTES:,}')

    names = members(sdist)
    for top in UNWANTED:
        inside = [name for name in names if name.partition('/')[0] == top]
        if inside:
            found.append(f'{top}/ is in it: {files(len(inside))}')
    npm = [name for name in names if NODE_MODULES in name.split('/')]
    if npm:
        found.append(f'{NODE_MODULES}/ is in it: {files(len(npm))}')
    found.extend(
        f'{name} is missing' for name in REQUIRED if name not in names
    )
    return found


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='Check that an sdist holds the package, and no more.',
    )
    parser.add_argument(
        'sdists', nargs='+', type=Path, metavar='SDIST',
        help='a .tar.gz, as `uv build` writes it to dist/',
    )
    args = parser.parse_args(argv)

    failed = False
    for sdist in args.sdists:
        if not sdist.is_file():
            # An unmatched `dist/*.tar.gz` arrives as itself.
            print(f'{sdist}: no such file')
            failed = True
        elif found := problems(sdist):
            print('\n  '.join([f'{sdist}:', *found]))
            failed = True
        else:
            print(f'{sdist}: {sdist.stat().st_size:,} bytes, ok')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
