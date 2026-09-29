"""scripts/audit_lock.py: what OSV has on each package uv.lock pins.

Dependabot moves what pyproject.toml names, and what those packages
pull in was never asked about: on 3dd89fa eight of them had advisories,
anyio's critical. OSV is a stand-in here, since no test may use the
network.
"""
import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.error import URLError

import pytest

ROOT = Path(__file__).resolve().parent.parent

#: A registry package twice, as a fork of the lock can hold it; the
#: project itself; and a package from git, which OSV has no version of.
LOCK = '''\
version = 1

[[package]]
name = "anyio"
version = "4.12.1"
source = { registry = "https://pypi.org/simple" }

[[package]]
name = "anyio"
version = "4.12.1"
source = { registry = "https://pypi.org/simple" }

[[package]]
name = "chatsbom"
version = "0.5.4"
source = { editable = "." }

[[package]]
name = "idna"
version = "3.20"
source = { registry = "https://pypi.org/simple" }

[[package]]
name = "somewhere"
version = "1.0.0"
source = { git = "https://example.com/somewhere.git#0123abc" }
'''

#: One advisory, as GitHub and PyPA each publish it: two records, each
#: naming the other.
GHSA = {
    'id': 'GHSA-82r6-8w77-94w6',
    'aliases': ['CVE-2026-63374', 'PYSEC-2026-1'],
    'summary': 'TLS host names encoded as IDNA 2003',
    'database_specific': {'severity': 'CRITICAL'},
    'affected': [{
        'package': {'name': 'AnyIO', 'ecosystem': 'PyPI'},
        'ranges': [{
            'type': 'ECOSYSTEM',
            'events': [{'introduced': '0'}, {'fixed': '4.14.2'}],
        }],
    }],
}
PYSEC = {
    'id': 'PYSEC-2026-1',
    'aliases': ['CVE-2026-63374', 'GHSA-82r6-8w77-94w6'],
    'affected': GHSA['affected'],
}


def audit_lock() -> ModuleType:
    """The script, loaded as a module: scripts/ is not a package."""
    path = ROOT / 'scripts' / 'audit_lock.py'
    spec = importlib.util.spec_from_file_location('audit_lock', path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def lock(tmp_path: Path) -> Path:
    path = tmp_path / 'uv.lock'
    path.write_text(LOCK)
    return path


def osv(
    monkeypatch: pytest.MonkeyPatch,
    script: ModuleType,
    records: dict[str, list[dict[str, Any]]],
) -> list[str]:
    """OSV's two endpoints, answering from `records` by package name.
    What each was asked about is returned, and grows as it is."""
    asked: list[str] = []

    def post(url: str, body: dict[str, Any]) -> Any:
        if url == script.QUERY_BATCH:
            names = [query['package']['name'] for query in body['queries']]
            asked.extend(names)
            return {
                'results': [
                    {'vulns': [{'id': r['id']} for r in records[name]]}
                    if records.get(name) else {}
                    for name in names
                ],
            }
        assert url == script.QUERY
        return {'vulns': records.get(body['package']['name'], [])}

    monkeypatch.setattr(script, 'post', post)
    return asked


def test_every_package_from_an_index_is_asked_about_once(lock, monkeypatch):
    script = audit_lock()
    asked = osv(monkeypatch, script, {})

    assert script.main([str(lock)]) == 0
    assert asked == ['anyio', 'idna']


def test_the_repositorys_own_lock_is_read_whole():
    """Every package it takes from PyPI, and the project not at all."""
    names = [name for name, _ in audit_lock().pinned(ROOT / 'uv.lock')]

    assert 'anyio' in names
    assert 'chatsbom' not in names


def test_an_advisory_is_listed_once_with_its_fix_and_fails_the_run(
    lock, monkeypatch, capsys,
):
    script = audit_lock()
    osv(monkeypatch, script, {'anyio': [PYSEC, GHSA]})

    assert script.main([str(lock)]) == 1
    out = capsys.readouterr().out
    assert out.splitlines() == [
        'anyio 4.12.1',
        '  GHSA-82r6-8w77-94w6 (CVE-2026-63374, PYSEC-2026-1)',
        '    CRITICAL; fixed in 4.14.2',
        '    TLS host names encoded as IDNA 2003',
        '1 of the 2 packages in uv.lock have advisories, 1 in all.',
    ]


def test_osv_out_of_reach_is_not_a_clean_lock(lock, monkeypatch, capsys):
    script = audit_lock()

    def unreachable(url: str, body: dict[str, Any]) -> Any:
        raise URLError('no route to host')

    monkeypatch.setattr(script, 'post', unreachable)

    assert script.main([str(lock)]) == 2
    assert 'OSV could not be asked' in capsys.readouterr().err
