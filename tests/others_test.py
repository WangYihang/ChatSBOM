"""The `other` lane: the sweep, minus what a language list already took.

mathesar is "Svelte" to GitHub and a Django application to anyone
reading its manifests; WebGoat is "JavaScript" and a Spring Boot one.
The first was in no language list, the second was dropped at `github
content`. These tests pin how the lane is chosen from the sweep.
"""
import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core.others import full_name
from chatsbom.core.others import read_ids
from chatsbom.core.others import select_others
from chatsbom.core.others import write_jsonl

runner = CliRunner()


def _repo(
    id: int, name: str, language: str | None, stars: int,
) -> dict:
    owner, repo = name.split('/')
    return {
        'id': id, 'owner': owner, 'repo': repo, 'full_name': name,
        'language': language, 'stars': stars,
    }


MATHESAR = _repo(1, 'mathesar-foundation/mathesar', 'Svelte', 3000)
WEBGOAT = _repo(2, 'WebGoat/WebGoat', 'JavaScript', 9000)
FLASK = _repo(3, 'pallets/flask', 'Python', 70000)
LLAMA = _repo(4, 'ggml-org/llama.cpp', 'C++', 80000)
CURL = _repo(5, 'curl/curl', 'C', 40000)
REDIS = _repo(6, 'redis/redis', 'C', 70000)
AWESOME = _repo(7, 'sindresorhus/awesome', None, 400000)


def _write(path: Path, records: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(''.join(json.dumps(r) + '\n' for r in records))
    return path


@pytest.fixture
def lanes(tmp_path):
    """A sweep of seven, where Python holds flask and JavaScript WebGoat.

    Flask reached the SBOM ledger; WebGoat did not.
    """
    search = tmp_path / '01-github-search'
    _write(
        search / 'all.jsonl',
        [MATHESAR, WEBGOAT, FLASK, LLAMA, CURL, REDIS, AWESOME],
    )
    _write(search / 'python.jsonl', [FLASK])
    _write(search / 'javascript.jsonl', [WEBGOAT])
    _write(tmp_path / '07-sbom' / 'python.jsonl', [FLASK])
    return {
        'sweep': search / 'all.jsonl',
        'claimed': [search / 'python.jsonl', search / 'javascript.jsonl'],
        'reached': [
            tmp_path / '07-sbom' / 'python.jsonl',
            tmp_path / '07-sbom' / 'javascript.jsonl',  # absent
        ],
    }


def _names(selection) -> list[str]:
    return [full_name(r) for r in selection.records]


def test_the_lane_is_the_sweep_minus_every_language_list(lanes):
    selection = select_others(**lanes)

    assert set(_names(selection)) == {
        'mathesar-foundation/mathesar', 'ggml-org/llama.cpp', 'curl/curl',
        'redis/redis', 'sindresorhus/awesome',
    }
    assert selection.unclaimed == 5


def test_records_keep_githubs_language(lanes):
    """`repositories.language` must still say Svelte, not `other`."""
    selection = select_others(**lanes)

    by_name = {full_name(r): r for r in selection.records}
    assert by_name['mathesar-foundation/mathesar']['language'] == 'Svelte'


def test_orders_by_stars_by_default(lanes):
    assert _names(select_others(**lanes))[:2] == [
        'sindresorhus/awesome', 'ggml-org/llama.cpp',
    ]


def test_a_claimed_repository_that_never_reached_the_sbom_ledger_is_an_orphan(lanes):
    """WebGoat: in javascript.jsonl, never in 07-sbom/javascript.jsonl."""
    left_out = select_others(**lanes)
    taken = select_others(**lanes, include_orphans=True)

    assert left_out.orphans == 1
    assert 'WebGoat/WebGoat' not in _names(left_out)
    assert 'WebGoat/WebGoat' in _names(taken)
    # Flask was claimed *and* indexed, so it is never an orphan.
    assert 'pallets/flask' not in _names(taken)


def test_named_repositories_come_first_and_count_toward_the_limit(lanes):
    selection = select_others(
        **lanes, include=['mathesar-foundation/MATHESAR', 'WebGoat/WebGoat'],
        limit=3,
    )

    assert _names(selection) == [
        'mathesar-foundation/mathesar', 'WebGoat/WebGoat',
        'sindresorhus/awesome',
    ]
    assert selection.included == [
        'mathesar-foundation/mathesar', 'WebGoat/WebGoat',
    ]


def test_naming_an_indexed_repository_is_refused(lanes):
    """It would be ingested a second time, and `artifacts` only appends."""
    with pytest.raises(ValueError, match='duplicate'):
        select_others(**lanes, include=['pallets/flask'])


def test_naming_a_repository_outside_the_sweep_is_refused(lanes):
    with pytest.raises(ValueError, match='not in'):
        select_others(**lanes, include=['nobody/nothing'])


def test_spread_takes_turns_across_languages(lanes):
    """The first few of a spread cover every label, not the biggest one."""
    selection = select_others(**lanes, spread=True, limit=4)

    languages = [r['language'] for r in selection.records]
    assert languages[0] == 'C'  # the most common label goes first
    assert set(languages) == {'C', 'C++', 'Svelte', None}
    # ...and within a label, the most-starred first.
    assert _names(selection)[0] == 'redis/redis'


def test_spread_keeps_every_record(lanes):
    assert sorted(_names(select_others(**lanes, spread=True))) == sorted(
        _names(select_others(**lanes)),
    )


def test_missing_ledgers_read_as_empty(tmp_path):
    assert read_ids([tmp_path / 'absent.jsonl']) == set()


def test_unparsable_lines_are_skipped(tmp_path):
    path = tmp_path / 'list.jsonl'
    path.write_text('{"id": 1}\nnot json\n\n{"no": "id"}\n{"id": 2}\n')
    assert read_ids([path]) == {1, 2}


def test_full_name_from_either_record_shape():
    assert full_name({'full_name': 'a/b'}) == 'a/b'
    assert full_name({'owner': {'login': 'a'}, 'name': 'b'}) == 'a/b'
    assert full_name({'owner': 'a', 'repo': 'b'}) == 'a/b'


def test_write_jsonl_leaves_no_temporary_file(tmp_path):
    path = tmp_path / 'deep' / 'other.jsonl'
    assert write_jsonl(path, [MATHESAR, CURL]) == 2
    assert [
        json.loads(line)['id']
        for line in path.read_text().splitlines()
    ] == [1, 5]
    assert list(path.parent.iterdir()) == [path]


# --- the command -----------------------------------------------------------

@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    """A data/ directory in the layout the stages use, as the cwd."""
    data = tmp_path / 'data'
    search = data / '01-github-search'
    _write(search / 'all.jsonl', [MATHESAR, WEBGOAT, FLASK, LLAMA, CURL])
    _write(search / 'python.jsonl', [FLASK])
    _write(search / 'javascript.jsonl', [WEBGOAT])
    _write(data / '02-github-repo' / 'python.jsonl', [FLASK])
    _write(data / '07-sbom' / 'python.jsonl', [FLASK])
    monkeypatch.chdir(tmp_path)
    return data


def _lane(data: Path) -> list[str]:
    path = data / '01-github-search' / 'other.jsonl'
    return [full_name(json.loads(line)) for line in path.read_text().splitlines()]


def test_data_other_writes_the_lane(data_dir):
    result = runner.invoke(app, ['data', 'other'])

    assert result.exit_code == 0, result.output
    assert _lane(data_dir) == [
        'ggml-org/llama.cpp', 'curl/curl', 'mathesar-foundation/mathesar',
    ]


def test_data_other_takes_orphans_and_names(data_dir):
    result = runner.invoke(
        app, [
            'data', 'other', '--include', 'WebGoat/WebGoat',
            '--limit', '2',
        ],
    )

    assert result.exit_code == 0, result.output
    assert _lane(data_dir) == ['WebGoat/WebGoat', 'ggml-org/llama.cpp']


def test_data_other_will_not_replace_a_lane_unasked(data_dir):
    assert runner.invoke(app, ['data', 'other', '--limit', '1']).exit_code == 0

    refused = runner.invoke(app, ['data', 'other'])
    assert refused.exit_code == 1
    assert _lane(data_dir) == ['ggml-org/llama.cpp']

    assert runner.invoke(app, ['data', 'other', '--force']).exit_code == 0
    assert len(_lane(data_dir)) == 3


def test_data_other_refuses_an_indexed_repository(data_dir):
    result = runner.invoke(
        app, ['data', 'other', '--include', 'pallets/flask'],
    )

    assert result.exit_code == 1
    assert not (data_dir / '01-github-search' / 'other.jsonl').exists()


def test_data_other_needs_the_sweep(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ['data', 'other'])

    assert result.exit_code == 1
    assert 'github search' in result.output


def test_github_search_refuses_the_other_lane():
    """`language:other` is not a GitHub qualifier; it would search nothing."""
    result = runner.invoke(
        app, ['github', 'search', '--language', 'other', '--token', 'x'],
    )

    assert result.exit_code == 1
    assert 'data other' in result.output
