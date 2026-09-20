"""`data slim` drops what nothing reads — and must stay loadable.

A record in `07-sbom/ruby.jsonl` is 63.1 KiB of which 98% is
`all_releases`, and four stages each append their own copy, so roughly
21 of the 22 GB of ledgers is that repetition.

The failure this guards against is the one it actually hit. The first
version kept `name`, which is not the key the model dumps — it dumps
`repo` — so every slimmed line failed validation. Nothing said so:
`load_jsonl` catches the error per line and returns what it could
parse, which was none of them, and the stage reported an empty language
and carried on. 5 GB of data became unusable without an error message.
"""
from __future__ import annotations

import json

from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.commands.data.slim import IDENTITY
from chatsbom.commands.data.slim import PROTECTED
from chatsbom.commands.data.slim import TARGETS
from chatsbom.core.storage import load_jsonl
from chatsbom.models.repository import Repository

runner = CliRunner()

FAT = {
    'id': 4321, 'owner': 'mikel', 'repo': 'mail', 'language': 'Ruby',
    'stars': 4197, 'url': 'https://github.com/mikel/mail',
    'local_content_path': 'data/06-github-content/ruby/mikel/mail/v3/abc',
    'depgraph_path': 'data/09-github-depgraph/ruby/mikel/mail/sbom.json',
    'download_target': {
        'ref': 'v3.2.0', 'ref_type': 'release',
        'commit_sha': 'abc123', 'commit_sha_short': 'abc123',
    },
    # The 98%.
    'all_releases': [
        {'id': i, 'tag_name': f'v{i}', 'source': 'release'}
        for i in range(200)
    ],
}


def _ledger(tmp_path, directory, count=3):
    path = tmp_path / directory / 'ruby.jsonl'
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w') as handle:
        for index in range(count):
            handle.write(json.dumps({**FAT, 'id': 4321 + index}) + '\n')
    return path


def _slim(tmp_path, *args):
    return runner.invoke(
        app, ['data', 'slim', '--language', 'ruby', *args],
        env={'CHATSBOM_DATA_DIR': str(tmp_path)},
    )


def test_every_kept_field_survives_validation():
    """The contract is the model, not a guess about which fields look
    required. `repo` is the key it dumps; `name` is not."""
    kept = {k: v for k, v in FAT.items() if k in IDENTITY}
    assert 'repo' in kept and 'name' not in kept
    assert Repository.model_validate(kept).repo == 'mail'


def test_a_slimmed_record_still_loads(tmp_path):
    """What the readers do: `load_jsonl` through `Repository`."""
    for target in TARGETS:
        kept = {
            k: v for k, v in FAT.items()
            if k in set(IDENTITY) | set(target.keeps)
        }
        path = tmp_path / f'{target.directory}.jsonl'
        path.write_text(json.dumps(kept) + '\n')
        loaded = load_jsonl(path)
        assert len(loaded) == 1, f'{target.directory} became unloadable'
        assert loaded[0].id == 4321


def test_the_readers_still_find_what_they_name():
    """Each target keeps exactly the fields its readers touch."""
    by_directory = {t.directory: t for t in TARGETS}
    assert 'local_content_path' in by_directory['06-github-content'].keeps, (
        '`sbom generate` reads it'
    )
    assert 'depgraph_path' in by_directory['09-github-depgraph'].keeps, (
        '`db index` reads it via `_depgraph_paths`'
    )
    for target in TARGETS:
        kept = {
            k: v for k, v in FAT.items()
            if k in set(IDENTITY) | set(target.keeps)
        }
        repo = Repository.model_validate(kept)
        dumped = repo.model_dump(mode='json')
        for field in target.keeps:
            assert dumped.get(field), f'{target.directory} lost {field}'


def test_the_releases_are_what_goes():
    """98% of a record, and already in ClickHouse twice over."""
    for target in TARGETS:
        assert 'all_releases' not in set(IDENTITY) | set(target.keeps)


def test_the_sbom_ledger_is_protected():
    """`db raw` derives the repository record from it, and the record
    lives nowhere else yet."""
    assert '07-sbom' in PROTECTED
    assert '07-sbom' not in {t.directory for t in TARGETS}

    result = runner.invoke(app, ['data', 'slim', '--directory', '07-sbom'])
    assert result.exit_code != 0
    assert 'Refusing' in result.output


def test_an_unknown_directory_is_refused():
    result = runner.invoke(app, ['data', 'slim', '--directory', 'nope'])
    assert result.exit_code != 0
