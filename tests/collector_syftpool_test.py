"""Syft in a CPU pool (#161; #128 section 2.1).

Syft is the collector's only CPU-bound work: about 1.6 CPU-seconds a
content root. It runs in a pool of `cores - 1` subprocess slots, so that
the event loop and the network keep a core, each scan with a timeout
and a memory limit, and the scans waiting for a slot take it highest
priority first: a changed repository's before a rescan for a new Syft.

The stand-in Syft (`tests/fake_upstream_test.py`) is a real subprocess:
the slots, the timeout and the limit are held to what the kernel does.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
from pathlib import Path
from typing import Any

import pytest

from chatsbom.collector.settings import SettingsError
from chatsbom.collector.syftpool import DEFAULT_MEMORY
from chatsbom.collector.syftpool import default_slots
from chatsbom.collector.syftpool import DEFAULT_TIMEOUT
from chatsbom.collector.syftpool import syft_settings
from chatsbom.collector.syftpool import SyftFailed
from chatsbom.collector.syftpool import SyftPool
from chatsbom.collector.syftpool import SyftSettings
from chatsbom.services.sbom_service import SYFT_DOCUMENT_KEYS
from tests.fake_upstream_test import FakeSyft

MIB = 2**20


@pytest.fixture
def syft(tmp_path: Path) -> FakeSyft:
    return FakeSyft(tmp_path / 'bin')


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """A content root of two manifests."""
    root = tmp_path / 'content' / '1' / ('a' * 40)
    (root / 'app').mkdir(parents=True)
    (root / 'package.json').write_text('{}')
    (root / 'app' / 'go.mod').write_text('module x\n')
    return root


def pool_of(syft: FakeSyft, **settings: Any) -> SyftPool:
    return SyftPool(
        SyftSettings(
            slots=settings.pop('slots', 2),
            timeout=settings.pop('timeout', 30.0),
            memory=settings.pop('memory', 256 * MIB),
            command=str(syft.path),
        ),
    )


class TestTheSettings:
    def test_default_to_a_slot_per_core_but_one(self, monkeypatch):
        monkeypatch.setattr(
            os, 'sched_getaffinity', lambda pid: {0, 1, 2, 3}, raising=False,
        )
        assert default_slots() == 3
        monkeypatch.setattr(os, 'sched_getaffinity', lambda pid: {0})
        assert default_slots() == 1

    def test_default_to_ten_minutes_and_two_gib(self):
        settings = syft_settings({})
        assert settings.slots == default_slots()
        assert settings.timeout == DEFAULT_TIMEOUT == 600
        assert settings.memory == DEFAULT_MEMORY == 2 * 2**30
        assert settings.command == 'syft'

    @pytest.mark.parametrize(
        'memory,bytes_', [
            ('512MiB', 512 * MIB), ('2GiB', 2 * 2**30), ('1500MB', 1_500_000_000),
            ('1073741824', 2**30), ('64 MiB', 64 * MIB), ('0', 0),
        ],
    )
    def test_are_read_from_the_environment(self, memory, bytes_):
        settings = syft_settings({
            'CHATSBOM_SYFT_SLOTS': '5', 'CHATSBOM_SYFT_TIMEOUT': '90.5',
            'CHATSBOM_SYFT_MEMORY': memory,
        })
        assert (settings.slots, settings.timeout, settings.memory) == (
            5, 90.5, bytes_,
        )

    @pytest.mark.parametrize(
        'name,value', [
            ('CHATSBOM_SYFT_SLOTS', '0'),
            ('CHATSBOM_SYFT_SLOTS', 'many'),
            ('CHATSBOM_SYFT_TIMEOUT', '0'),
            ('CHATSBOM_SYFT_TIMEOUT', '-1'),
            ('CHATSBOM_SYFT_TIMEOUT', 'soon'),
            ('CHATSBOM_SYFT_MEMORY', '2 bananas'),
            ('CHATSBOM_SYFT_MEMORY', '-5'),
        ],
    )
    def test_a_value_it_cannot_use_is_refused_by_name(self, name, value):
        with pytest.raises(SettingsError) as refused:
            syft_settings({name: value})
        assert refused.value.setting == name
        assert value in str(refused.value)


class TestAScan:
    def test_gives_syfts_document(self, syft, root):
        document = asyncio.run(pool_of(syft).scan(root))
        loaded = json.loads(document)
        assert SYFT_DOCUMENT_KEYS <= loaded.keys()
        assert sorted(a['name'] for a in loaded['artifacts']) == [
            'app/go.mod', 'package.json',
        ]
        assert syft.scans[0]['scan'] == str(root.absolute())

    def test_tells_syft_not_to_look_for_its_own_updates(self, syft, root):
        asyncio.run(pool_of(syft).scan(root))
        assert syft.scans[0]['env']['SYFT_CHECK_FOR_APP_UPDATE'] == 'false'

    def test_tells_go_how_much_it_has(self, syft, root):
        """GOMEMLIMIT, a soft limit Go's collector works to stay under,
        below the hard one, which kills the scan."""
        asyncio.run(pool_of(syft, memory=400 * MIB).scan(root))
        assert syft.scans[0]['env']['GOMEMLIMIT'] == str(300 * MIB)

    def test_that_fails_says_what_syft_said(self, syft, root):
        syft.configure(
            exit=1, stderr='could not determine source: no such directory',
        )
        with pytest.raises(SyftFailed) as failed:
            asyncio.run(pool_of(syft).scan(root))
        assert failed.value.kind == 'exit'
        assert 'exited 1' in str(failed.value)
        assert 'could not determine source' in str(failed.value)

    @pytest.mark.parametrize('output', ['', 'not json', '{"artifacts": []}\n'])
    def test_that_writes_no_syft_document_fails(self, syft, root, output):
        syft.configure(output=output)
        with pytest.raises(SyftFailed) as failed:
            asyncio.run(pool_of(syft).scan(root))
        assert failed.value.kind == 'output'

    def test_without_syft_fails_as_missing(self, tmp_path, root):
        pool = SyftPool(
            SyftSettings(
                slots=1, timeout=5, memory=0,
                command=str(tmp_path / 'no-such-syft'),
            ),
        )
        with pytest.raises(SyftFailed) as failed:
            asyncio.run(pool.scan(root))
        assert failed.value.kind == 'missing'


class TestTheVersion:
    def test_is_what_syft_says_it_is(self, syft):
        syft.configure(version='1.53.0')
        assert asyncio.run(pool_of(syft).version()) == '1.53.0'

    def test_is_none_when_it_cannot_be_told(self, syft, tmp_path):
        syft.configure(no_version=True)
        assert asyncio.run(pool_of(syft).version()) is None
        missing = SyftPool(
            SyftSettings(
                slots=1, timeout=5, memory=0,
                command=str(tmp_path / 'no-such-syft'),
            ),
        )
        assert asyncio.run(missing.version()) is None

    def test_is_asked_again_after_an_upgrade(self, syft):
        """Each pass asks: the Syft on disk is what the next scan runs."""
        pool = pool_of(syft)
        assert asyncio.run(pool.version()) == '1.52.0'
        syft.configure(version='1.53.0')
        assert asyncio.run(pool.version()) == '1.53.0'


class TestTheSlots:
    def test_hold_as_many_scans_as_there_are_slots(self, syft, root):
        syft.configure(delay=0.3)
        pool = pool_of(syft, slots=2)

        async def scans() -> list[bytes]:
            return await asyncio.gather(*(pool.scan(root) for _ in range(5)))

        documents = asyncio.run(scans())
        assert len(documents) == 5
        assert pool.peak == 2
        assert syft.peak == 2
        assert pool.running == 0

    def test_go_to_the_highest_priority_first(self, syft, tmp_path):
        """One slot, held; a rescan asks, then a changed repository's
        scan: the changed one is scanned first."""
        roots = {}
        for name in ('first', 'rescan', 'changed'):
            roots[name] = tmp_path / name
            roots[name].mkdir()
            (roots[name] / 'go.mod').write_text('module x\n')
        syft.configure(delay=0.3)
        pool = pool_of(syft, slots=1)

        async def scans() -> None:
            first = asyncio.ensure_future(
                pool.scan(roots['first'], priority=1),
            )
            while pool.running == 0:
                await asyncio.sleep(0.01)
            rescan = asyncio.ensure_future(
                pool.scan(roots['rescan'], priority=3),
            )
            await asyncio.sleep(0.05)
            changed = asyncio.ensure_future(
                pool.scan(roots['changed'], priority=1),
            )
            await asyncio.sleep(0.05)
            assert pool.waiting == 2
            await asyncio.gather(first, rescan, changed)

        asyncio.run(scans())
        assert [Path(scan['scan']).name for scan in syft.scans] == [
            'first', 'changed', 'rescan',
        ]

    def test_a_scan_given_up_on_while_waiting_takes_no_slot(
        self, syft, root,
    ):
        syft.configure(delay=0.3)
        pool = pool_of(syft, slots=1)

        async def scans() -> None:
            first = asyncio.ensure_future(pool.scan(root))
            while pool.running == 0:
                await asyncio.sleep(0.01)
            second = asyncio.ensure_future(pool.scan(root))
            await asyncio.sleep(0.05)
            second.cancel()
            await asyncio.gather(second, return_exceptions=True)
            assert pool.waiting == 0
            await first
            assert pool.running == 0
            await pool.scan(root)

        asyncio.run(scans())
        assert len(syft.scans) == 2


class TestTheTimeout:
    def test_kills_a_scan_that_runs_past_it(self, syft, root):
        syft.configure(delay=60)
        pool = pool_of(syft, timeout=0.5)
        started = time.monotonic()
        with pytest.raises(SyftFailed) as failed:
            asyncio.run(pool.scan(root))
        assert time.monotonic() - started < 10
        assert failed.value.kind == 'timeout'
        assert 'killed after 0.5 s' in str(failed.value)
        [scan] = syft.scans
        with pytest.raises(ProcessLookupError):
            os.kill(scan['pid'], 0)

    def test_frees_its_slot(self, syft, root):
        syft.configure(delay=60)
        pool = pool_of(syft, slots=1, timeout=0.5)

        async def scans() -> bytes:
            with pytest.raises(SyftFailed):
                await pool.scan(root)
            syft.configure(delay=0)
            return await pool.scan(root)

        assert json.loads(asyncio.run(scans()))['artifacts']
        assert pool.running == 0


class TestTheMemoryLimit:
    def test_stops_a_scan_that_holds_more(self, syft, root):
        syft.configure(allocate=512 * MIB)
        with pytest.raises(SyftFailed) as failed:
            asyncio.run(pool_of(syft, memory=64 * MIB).scan(root))
        assert failed.value.kind == 'memory'
        assert '64 MiB' in str(failed.value)

    def test_leaves_a_scan_within_it_alone(self, syft, root):
        syft.configure(allocate=16 * MIB)
        document = asyncio.run(pool_of(syft, memory=128 * MIB).scan(root))
        assert json.loads(document)['artifacts']

    def test_of_nothing_is_no_limit(self, syft, root):
        syft.configure(allocate=128 * MIB)
        pool = pool_of(syft, memory=0)
        assert json.loads(asyncio.run(pool.scan(root)))['artifacts']
        assert syft.scans[0]['env']['GOMEMLIMIT'] is None


@pytest.fixture
def real_syft() -> str:
    """The Syft on PATH: CI installs the collector's, 1.52.0, and there a
    skip fails the run (tests/conftest.py)."""
    found = shutil.which('syft')
    if found is None:
        pytest.skip('syft is not installed')
    return found


def test_the_real_syft_scans_within_the_default_limit(real_syft, root):
    """Syft is Go: it reserves more address space than it uses, which a
    limit on the address space would refuse at start-up. The limit is
    on the data it writes (RLIMIT_DATA), which the default leaves room
    for."""
    pool = SyftPool(
        SyftSettings(
            slots=1, timeout=120, memory=DEFAULT_MEMORY, command=real_syft,
        ),
    )
    version = asyncio.run(pool.version())
    document = json.loads(asyncio.run(pool.scan(root)))
    assert document['descriptor']['name'] == 'syft'
    assert document['descriptor']['version'] == version
