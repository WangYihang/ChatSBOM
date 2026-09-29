#!/usr/bin/env python3
"""Does any package uv.lock installs have a published advisory?

Dependabot moves what pyproject.toml names, a group a week, and nothing
moves what those packages pull in until a relock happens to. So nothing
said when one of them had an advisory: on 3dd89fa eight packages in the
lock had them, anyio's critical, and pyproject.toml names none of the
eight. This asks OSV (https://osv.dev), which gathers GitHub's, PyPA's
and the other advisory databases, about every package the lock pins,
whatever pulls it in.

Usage:

    python scripts/audit_lock.py              # this checkout's uv.lock
    python scripts/audit_lock.py path/to/uv.lock

`uv lock --upgrade` moves every package to the newest release the
constraints admit; `uv lock --upgrade-package <name>` moves one.

Standard library only. One request asks about every package, and one
more per vulnerable package fetches what its advisories say. It lists
each advisory under the package and version it affects, with its
severity where the advisory gives one and the versions that fix it, and
exits 1 if there is any, 2 if OSV could not be asked.
"""
import argparse
import json
import re
import sys
import tomllib
import urllib.request
from collections.abc import Iterator
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent

#: Every package in one request, answered with each advisory's ID alone.
QUERY_BATCH = 'https://api.osv.dev/v1/querybatch'
#: One package, answered with its advisories in full.
QUERY = 'https://api.osv.dev/v1/query'
#: What the batch endpoint takes in one request.
BATCH_SIZE = 1000
TIMEOUT = 60

Package = tuple[str, str]


def pinned(lock: Path) -> list[Package]:
    """Each package and version the lock takes from an index. Not the
    project itself, nor one from a path or a git repository: OSV knows
    them by no PyPI version."""
    data = tomllib.loads(lock.read_text(encoding='utf-8'))
    return sorted({
        (package['name'], package['version'])
        for package in data.get('package', [])
        if 'registry' in package.get('source', {})
    })


def post(url: str, body: dict[str, Any]) -> Any:
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        return json.load(response)


def query(package: Package) -> dict[str, Any]:
    name, version = package
    return {
        'package': {'name': name, 'ecosystem': 'PyPI'},
        'version': version,
    }


def vulnerable(packages: list[Package]) -> list[Package]:
    """The packages OSV has an advisory for."""
    found: list[Package] = []
    for start in range(0, len(packages), BATCH_SIZE):
        batch = packages[start:start + BATCH_SIZE]
        answer = post(QUERY_BATCH, {'queries': [query(p) for p in batch]})
        found.extend(
            package
            for package, result in zip(batch, answer['results'], strict=True)
            if result.get('vulns')
        )
    return found


def advisories(package: Package) -> list[dict[str, Any]]:
    """What OSV has on `package`, each advisory once.

    It holds one record per database, so an advisory GitHub and PyPA
    both published is two records that name each other as aliases.
    GitHub's comes first, as the one that grades severity.
    """
    records: list[dict[str, Any]] = []
    body = query(package)
    while True:
        answer = post(QUERY, body)
        records.extend(answer.get('vulns', []))
        if not answer.get('next_page_token'):
            break
        body = {**query(package), 'page_token': answer['next_page_token']}
    records.sort(key=lambda r: (not r['id'].startswith('GHSA-'), r['id']))
    seen: set[str] = set()
    kept = []
    for record in records:
        if record['id'] in seen:
            continue
        seen.update([record['id'], *record.get('aliases', [])])
        kept.append(record)
    return kept


def normalized(name: str) -> str:
    """A distribution's name as PEP 503 compares it."""
    return re.sub(r'[-_.]+', '-', name).lower()


def severity(record: dict[str, Any]) -> str:
    """GitHub's grade (LOW, MODERATE, HIGH, CRITICAL), where the record
    has one; PyPA's give a CVSS vector at most."""
    grade = record.get('database_specific', {}).get('severity')
    return str(grade) if grade else 'ungraded'


def fixes(record: dict[str, Any], name: str) -> list[str]:
    """The versions the advisory says fix `name`, in its order."""
    found: list[str] = []
    for affected in record.get('affected', []):
        if normalized(affected['package']['name']) != normalized(name):
            continue
        for span in affected.get('ranges', []):
            found.extend(
                event['fixed'] for event in span['events'] if 'fixed' in event
            )
    return list(dict.fromkeys(found))


def report(package: Package, records: list[dict[str, Any]]) -> Iterator[str]:
    name, version = package
    yield f'{name} {version}'
    for record in records:
        aliases = ', '.join(record.get('aliases', []))
        yield f"  {record['id']}" + (f' ({aliases})' if aliases else '')
        fixed = fixes(record, name)
        fix = f"fixed in {', '.join(fixed)}" if fixed else 'no fix released'
        yield f'    {severity(record)}; {fix}'
        if summary := record.get('summary'):
            yield f'    {summary}'


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='List the advisories OSV has for what a uv.lock pins.',
    )
    parser.add_argument(
        'lock', nargs='?', type=Path, default=ROOT / 'uv.lock',
        help='the lockfile (default: %(default)s)',
    )
    lock = parser.parse_args(argv).lock
    packages = pinned(lock)
    try:
        found = {
            package: advisories(package)
            for package in vulnerable(packages)
        }
    except OSError as e:
        # URLError, a timeout, a dropped connection: nothing was checked.
        print(f'OSV could not be asked: {e}', file=sys.stderr)
        return 2
    for package, records in found.items():
        print('\n'.join(report(package, records)))
    if not found:
        print(
            f'None of the {len(packages)} packages in {lock.name} has an '
            'advisory.',
        )
        return 0
    count = sum(len(records) for records in found.values())
    print(
        f'{len(found)} of the {len(packages)} packages in {lock.name} '
        f'have advisories, {count} in all.',
    )
    return 1


if __name__ == '__main__':
    sys.exit(main())
