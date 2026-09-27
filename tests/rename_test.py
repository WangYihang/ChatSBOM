"""The repository was renamed; nothing may still point at the old name
(#20).

The systemd units ran in a checkout of that name and linked to its URL,
Dockerfile.lock built on an image compose had named after it, and the
citation gave its address. None of them works on a fresh clone of
WangYihang/ChatSBOM.
"""
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

#: Put together from its parts, so that this file does not match itself.
OLD_NAME = re.compile('sbom' + '[-_]?' + 'insight', re.IGNORECASE)

#: The one place the old name may stay: a note explaining the rename.
RENAME_NOTE = re.compile(r'CHANGELOG\b', re.IGNORECASE)


def files() -> list[Path]:
    """What is in the tree: tracked, or new and not ignored."""
    listing = subprocess.run(
        ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'],
        cwd=ROOT,
        capture_output=True,
        check=True,
    )
    return [
        ROOT / name for name in listing.stdout.decode().split('\0') if name
    ]


def test_nothing_points_at_the_old_name():
    found = []
    for path in files():
        if RENAME_NOTE.match(path.name) or not path.is_file():
            continue
        try:
            text = path.read_text(encoding='utf-8')
        except UnicodeDecodeError:
            continue
        for number, line in enumerate(text.splitlines(), start=1):
            if OLD_NAME.search(line):
                where = f'{path.relative_to(ROOT)}:{number}'
                found.append(f'{where}: {line.strip()}')
    assert found == []
