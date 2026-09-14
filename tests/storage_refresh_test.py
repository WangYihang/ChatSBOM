"""Ledger files must be able to carry a re-collected repository.

`Storage.save` deduplicated by repository id and returned False for
anything already present. That is right for a discovery sweep — the same
repository turning up twice in paginated search results should not be
written twice — but wrong for continuous collection, where the point of
re-collecting is that the record changed. A repository's new releases
were silently dropped.
"""
from chatsbom.core.storage import load_jsonl
from chatsbom.core.storage import Storage
from chatsbom.models.repository import Repository


def repo(repo_id=1, **over):
    data = {'id': repo_id, 'owner': 'o', 'name': 'r', 'stargazers_count': 1}
    data.update(over)
    return Repository.model_validate(data)


# --- the original guarantee, preserved ------------------------------------

def test_a_duplicate_is_not_written_twice(tmp_path):
    store = Storage(tmp_path / 'l.jsonl')
    assert store.save(repo(1))
    assert not store.save(repo(1))
    assert len(load_jsonl(tmp_path / 'l.jsonl')) == 1


def test_distinct_repositories_are_both_written(tmp_path):
    store = Storage(tmp_path / 'l.jsonl')
    store.save(repo(1))
    store.save(repo(2))
    assert len(load_jsonl(tmp_path / 'l.jsonl')) == 2


def test_existing_records_are_loaded_on_open(tmp_path):
    path = tmp_path / 'l.jsonl'
    Storage(path).save(repo(1))
    assert not Storage(path).save(repo(1)), 'dedup survives reopening'


# --- the new capability ---------------------------------------------------

def test_an_updated_record_can_replace_the_stored_one(tmp_path):
    path = tmp_path / 'l.jsonl'
    store = Storage(path)
    store.save(repo(1, stargazers_count=10))

    assert store.save(repo(1, stargazers_count=99), replace=True)

    records = load_jsonl(path)
    assert len(records) == 1, 'replaced, not appended'
    assert records[0].stars == 99


def test_replacing_preserves_other_repositories(tmp_path):
    path = tmp_path / 'l.jsonl'
    store = Storage(path)
    store.save(repo(1, stargazers_count=10))
    store.save(repo(2, stargazers_count=20))

    store.save(repo(1, stargazers_count=99), replace=True)

    by_id = {r.id: r.stars for r in load_jsonl(path)}
    assert by_id == {1: 99, 2: 20}


def test_replacing_an_unknown_repository_just_appends(tmp_path):
    path = tmp_path / 'l.jsonl'
    store = Storage(path)
    assert store.save(repo(7), replace=True)
    assert [r.id for r in load_jsonl(path)] == [7]


def test_replace_is_durable_across_reopening(tmp_path):
    path = tmp_path / 'l.jsonl'
    Storage(path).save(repo(1, stargazers_count=10))
    Storage(path).save(repo(1, stargazers_count=99), replace=True)
    assert load_jsonl(path)[0].stars == 99


def test_rewrite_is_atomic(tmp_path):
    """A crash mid-rewrite must not truncate the ledger."""
    path = tmp_path / 'l.jsonl'
    store = Storage(path)
    for i in range(1, 20):
        store.save(repo(i))

    store.save(repo(10, stargazers_count=500), replace=True)

    records = load_jsonl(path)
    assert len(records) == 19, 'every record survived the rewrite'
    assert not list(path.parent.glob('*.tmp*')), 'no temp file left behind'
