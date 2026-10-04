"""A pass reads ahead of itself, several repositories at once (#187).

A disk that turns answers one request at a time at its slowest: given
many at once, it takes them in the order its head passes them. Measured
on the HDD this collector runs on, beside it, reading 300 repositories'
files took 116 s one repository after another, and 24 s sixteen at a
time. What is read ahead only warms the page cache: what the pass
reads, it reads itself, and what it builds is the same.
"""
from __future__ import annotations

import threading
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from chatsbom.warehouse import prefetch
from chatsbom.warehouse.prefetch import Prefetcher


@pytest.fixture
def read(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every file the prefetcher reads."""
    made: list[str] = []
    lock = threading.Lock()

    def record(path: str) -> None:
        with lock:
            made.append(path)

    monkeypatch.setattr(prefetch, '_read', record)
    return made


def unit(root: Path, name: str, files: int = 3) -> list[Path]:
    """A repository's directories in two stage roots, with files."""
    tops = [root / 'stage-a' / name, root / 'stage-b' / name]
    for top in tops:
        (top / 'sha' / 'deep').mkdir(parents=True)
        for k in range(files):
            (top / 'sha' / f'f{k}').write_text(name)
        (top / 'sha' / 'deep' / 'g').write_text(name)
    return tops


def wait_for(condition: Callable[[], bool], seconds: float = 5.0) -> None:
    deadline = time.monotonic() + seconds
    while not condition():
        assert time.monotonic() < deadline
        time.sleep(0.01)


def test_it_reads_every_file_of_what_the_pass_will_read(
    tmp_path: Path, read: list[str],
) -> None:
    plan = [unit(tmp_path, 'one'), unit(tmp_path, 'two')]
    with Prefetcher(plan, ahead=2):
        wait_for(lambda: len(read) == 16)
    assert sorted(read) == sorted(
        str(path) for tops in plan for top in tops for path in top.rglob('*')
        if path.is_file()
    )


def test_it_reads_no_more_than_ahead_repositories_in_front(
    tmp_path: Path, read: list[str],
) -> None:
    plan = [unit(tmp_path, str(k), files=1) for k in range(4)]
    with Prefetcher(plan, ahead=2) as ahead:
        wait_for(lambda: len(read) == 2 * 4)
        time.sleep(0.1)
        assert len(read) == 2 * 4
        ahead.advance()
        wait_for(lambda: len(read) == 3 * 4)
        ahead.advance()
        wait_for(lambda: len(read) == 4 * 4)
        ahead.advance()
        ahead.advance()


def test_under_a_stat_only_root_it_reads_nothing(
    tmp_path: Path, read: list[str],
) -> None:
    """A tree's files are stated, to date a commit by, and not read."""
    plan = [unit(tmp_path, 'one'), unit(tmp_path, 'two')]
    with Prefetcher(plan, ahead=2, stat_only=[tmp_path / 'stage-b']):
        wait_for(lambda: len(read) == 8)
        time.sleep(0.1)
    assert all('/stage-a/' in path for path in read)


def test_what_is_not_there_is_passed_over(
    tmp_path: Path, read: list[str],
) -> None:
    plan = [[tmp_path / 'gone'], unit(tmp_path, 'one')]
    with Prefetcher(plan, ahead=2):
        wait_for(lambda: len(read) == 8)
