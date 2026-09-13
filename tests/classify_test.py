"""Batch classification: concurrent, resumable, and batched at the DB."""
import json
import threading
import time

import pytest

from chatsbom.commands.github.classify import already_processed_ids
from chatsbom.commands.github.classify import ClassificationResult
from chatsbom.commands.github.classify import classify_repositories
from chatsbom.commands.github.classify import OutputFormat
from chatsbom.commands.github.classify import ResultWriter
from chatsbom.models.repository import Repository


def repos(count: int) -> list[Repository]:
    return [
        Repository.model_validate({
            'id': 100 + i, 'owner': 'o', 'name': f'r{i}', 'language': 'go',
        })
        for i in range(count)
    ]


def flat_of(repo: Repository) -> dict:
    return {'id': repo.id, 'repo': repo.repo, 'primary_framework': ''}


# --- resume ----------------------------------------------------------------

def test_already_processed_reads_jsonl_ids(tmp_path):
    p = tmp_path / 'out.jsonl'
    p.write_text('{"id": 1}\n\n{"id": 2}\n')
    assert already_processed_ids(p, OutputFormat.JSONL) == {1, 2}


def test_already_processed_reads_csv_ids(tmp_path):
    p = tmp_path / 'out.csv'
    p.write_text('id,repo\n1,a\n2,b\n')
    assert already_processed_ids(p, OutputFormat.CSV) == {1, 2}


def test_already_processed_on_missing_file_is_empty(tmp_path):
    assert already_processed_ids(
        tmp_path / 'nope.jsonl', OutputFormat.JSONL,
    ) == set()


def test_already_processed_ignores_unparsable_rows(tmp_path):
    p = tmp_path / 'out.jsonl'
    p.write_text('{"id": 1}\n{broken\n{"id": 3}\n')
    assert already_processed_ids(p, OutputFormat.JSONL) == {1, 3}


# --- writing ---------------------------------------------------------------

def test_writer_appends_jsonl(tmp_path):
    p = tmp_path / 'out.jsonl'
    with ResultWriter(p, OutputFormat.JSONL) as w:
        w.write({'id': 1, 'repo': 'a'})
        w.write({'id': 2, 'repo': 'b'})
    lines = p.read_text().strip().splitlines()
    assert [json.loads(x)['id'] for x in lines] == [1, 2]


def test_writer_writes_csv_header_once(tmp_path):
    p = tmp_path / 'out.csv'
    with ResultWriter(p, OutputFormat.CSV) as w:
        w.write({'id': 1, 'repo': 'a'})
    with ResultWriter(p, OutputFormat.CSV) as w:
        w.write({'id': 2, 'repo': 'b'})
    lines = p.read_text().strip().splitlines()
    assert lines[0] == 'id,repo'
    assert lines.count('id,repo') == 1, 'header must not repeat on resume'


def test_writer_is_safe_under_concurrent_writes(tmp_path):
    p = tmp_path / 'out.jsonl'
    with ResultWriter(p, OutputFormat.JSONL) as w:
        threads = [
            threading.Thread(target=w.write, args=({'id': i, 'repo': 'x'},))
            for i in range(50)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    lines = p.read_text().strip().splitlines()
    assert len(lines) == 50
    assert {json.loads(x)['id'] for x in lines} == set(range(50))


# --- orchestration ---------------------------------------------------------

def test_every_repository_is_classified(tmp_path):
    out = tmp_path / 'out.jsonl'
    with ResultWriter(out, OutputFormat.JSONL) as w:
        result = classify_repositories(repos(5), flat_of, w)

    assert result == ClassificationResult(processed=5, cached=0, failed=0)
    assert len(out.read_text().strip().splitlines()) == 5


def test_cached_repositories_are_skipped(tmp_path):
    out = tmp_path / 'out.jsonl'
    out.write_text('{"id": 100}\n{"id": 101}\n')
    with ResultWriter(out, OutputFormat.JSONL) as w:
        result = classify_repositories(
            repos(5), flat_of, w,
            processed_ids=already_processed_ids(out, OutputFormat.JSONL),
        )
    assert result.cached == 2
    assert result.processed == 3


def test_analysis_failures_are_counted_not_raised(tmp_path):
    def analyze(repo):
        if repo.id == 101:
            raise RuntimeError('llm exploded')
        if repo.id == 102:
            return None
        return flat_of(repo)

    with ResultWriter(tmp_path / 'out.jsonl', OutputFormat.JSONL) as w:
        result = classify_repositories(repos(4), analyze, w)

    assert result.processed == 2
    assert result.failed == 2


def test_progress_callback_fires_once_per_repository(tmp_path):
    seen = []
    with ResultWriter(tmp_path / 'out.jsonl', OutputFormat.JSONL) as w:
        classify_repositories(
            repos(7), flat_of, w, on_progress=lambda: seen.append(1),
        )
    assert len(seen) == 7


def test_work_runs_concurrently(tmp_path):
    """Eight 50ms calls at concurrency 8 must not take eight times 50ms."""
    def slow(repo):
        time.sleep(0.05)
        return flat_of(repo)

    start = time.monotonic()
    with ResultWriter(tmp_path / 'out.jsonl', OutputFormat.JSONL) as w:
        classify_repositories(repos(8), slow, w, concurrency=8)
    elapsed = time.monotonic() - start

    assert elapsed < 0.2, f'took {elapsed:.3f}s; looks sequential'


def test_concurrency_of_one_is_still_correct(tmp_path):
    out = tmp_path / 'out.jsonl'
    with ResultWriter(out, OutputFormat.JSONL) as w:
        result = classify_repositories(repos(3), flat_of, w, concurrency=1)
    assert result.processed == 3


def test_empty_input_does_nothing(tmp_path):
    with ResultWriter(tmp_path / 'out.jsonl', OutputFormat.JSONL) as w:
        assert classify_repositories([], flat_of, w) == ClassificationResult()


@pytest.mark.parametrize('bad', [0, -1])
def test_concurrency_must_be_positive(tmp_path, bad):
    with ResultWriter(tmp_path / 'out.jsonl', OutputFormat.JSONL) as w:
        with pytest.raises(ValueError, match='concurrency'):
            classify_repositories(repos(1), flat_of, w, concurrency=bad)
