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
from pathlib import Path

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.commands.data.slim import IDENTITY
from chatsbom.commands.data.slim import PROTECTED
from chatsbom.commands.data.slim import TARGETS
from chatsbom.core.container import Container
from chatsbom.core.storage import load_jsonl
from chatsbom.models.repository import Repository

runner = CliRunner()

FAT = {
    'id': 4321, 'owner': 'mikel', 'repo': 'mail', 'language': 'Ruby',
    'stars': 4197, 'url': 'https://github.com/mikel/mail',
    'local_content_path': 'data/06-github-content/ruby/mikel/mail/v3/abc',
    'depgraph_path': 'data/09-github-depgraph/ruby/mikel/mail/sbom.json',
    'sbom_path': 'data/07-sbom/ruby/mikel/mail/v3/abc/sbom.json',
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


def test_the_metadata_ledger_is_protected():
    """`02-github-repo` is the overlay's only source and `db raw`
    lands it verbatim — there is nothing in it that is not read."""
    assert '02-github-repo' in PROTECTED
    assert '02-github-repo' not in {t.directory for t in TARGETS}

    result = runner.invoke(
        app, ['data', 'slim', '--directory', '02-github-repo'],
    )
    assert result.exit_code != 0
    assert 'Refusing' in result.output


def test_the_sbom_ledger_became_slimmable():
    """It was protected while `db raw` derived the repository record
    from it. The collector writes that record now, so the 5.2 GB of
    release lists in this ledger are redundant rather than load-bearing."""
    assert '07-sbom' not in PROTECTED
    assert '07-sbom' in {t.directory for t in TARGETS}


def test_an_unknown_directory_is_refused():
    result = runner.invoke(app, ['data', 'slim', '--directory', 'nope'])
    assert result.exit_code != 0


class TestTheRecordSurvivesSlimming:
    """`07-sbom` can only be slimmed because the record moved out of it.

    `db raw` used to derive `kind='repo'` from that ledger. Slim the
    ledger with that still in place and the derivation produces a
    record with no `all_releases` — and because it would be the newest
    row, `RawRecords` serves it in preference to the complete one. A
    5 GB reclaim that silently empties the releases table.
    """

    def test_db_raw_no_longer_derives_the_record_from_a_ledger(self):
        from chatsbom.commands.db.raw import RECORD_SOURCES

        directories = {directory for directory, _ in RECORD_SOURCES}
        assert '07-sbom' not in directories, (
            'slimming it would then degrade every repository record'
        )
        assert directories == {'02-github-repo'}

    def test_the_record_is_written_by_the_collector(self):
        """`RecordStore`, at the point the record is complete."""
        from chatsbom.core.documents import RecordStore

        assert hasattr(RecordStore, 'remember')

    def test_db_raw_no_longer_needs_the_paths_a_ledger_records(
        self, tmp_path, monkeypatch,
    ):
        """`db raw` found the syft documents by `sbom_path` and the
        manifest directories by `local_content_path`, read from `07-sbom`.
        It walks the repository-keyed stage directories now (#55), so
        slimming a ledger cannot make it stop finding what is on disk.

        Here the ledger keeps nothing but the identity, less than any
        slimming leaves, and every document is landed all the same.
        """
        from tests.raw_documents_test import db_raw
        from tests.raw_documents_test import SHA
        from tests.raw_documents_test import write_tree

        documents = {
            f'07-sbom/4321/{SHA}/sbom.json': '{"artifacts": []}',
            '09-github-depgraph/4321/legacy/sbom.spdx.json': '{}',
            f'05-github-tree/4321/{SHA}/manifests.json': '{"format": 1}',
            f'06-github-content/4321/{SHA}/Gemfile': "gem 'mail'\n",
        }
        identity = {key: FAT[key] for key in IDENTITY}
        data = write_tree(
            tmp_path, {
                **documents, '07-sbom/ruby.jsonl': json.dumps(identity) + '\n',
            },
        )

        result, zone = db_raw(data, monkeypatch, '--apply')

        assert result.exit_code == 0, result.output
        assert zone.landed() == sorted(documents)


# --- where a refusal is said (#124) ------------------------------------------

@pytest.fixture
def workdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A working directory of its own, where `data/` is: whatever is
    slimmed is the test's."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    monkeypatch.setattr(Container, '_instance', None)
    return tmp_path


def _unloadable(root: Path) -> Path:
    """`05-github-tree/ruby.jsonl`, holding a record that loads only
    through a field slimming drops: its name under GitHub's key, `name`,
    where the model dumps `repo`."""
    record = {**FAT, 'name': FAT['repo']}
    del record['repo']
    assert Repository.model_validate(record).repo == 'mail'
    path = root / 'data' / '05-github-tree' / 'ruby.jsonl'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(record) + '\n', encoding='utf-8')
    return path


#: Each refusal: the options, the words, and the event it is with JSON
#: logs, with the fields that say what was refused.
REFUSALS = {
    'protected': (
        ['--directory', '02-github-repo'],
        'Refusing to slim 02-github-repo.',
        'Refusing to slim a protected ledger',
        {'directory': '02-github-repo'},
    ),
    'no such target': (
        ['--directory', 'nope'],
        'No such target: nope',
        'No such ledger to slim',
        {'directory': 'nope'},
    ),
    'unloadable': (
        ['--directory', '05-github-tree', '--apply'],
        'repository 4321 would no longer load.',
        'Refusing to slim a ledger whose records would no longer load',
        {'repository_id': 4321},
    ),
}


@pytest.mark.parametrize(
    'options, said, event, fields', REFUSALS.values(), ids=list(REFUSALS),
)
def test_a_refusal_is_said_on_stderr(workdir, options, said, event, fields):
    """Each was printed on stdout, where the table of ledgers goes, and
    exited 1."""
    listing = _unloadable(workdir)
    before = listing.read_bytes()

    result = runner.invoke(app, ['data', 'slim', *options])

    assert result.exit_code == 1, result.output
    assert result.stdout == ''
    assert said in ' '.join(result.stderr.split())
    assert listing.read_bytes() == before


@pytest.mark.parametrize(
    'options, said, event, fields', REFUSALS.values(), ids=list(REFUSALS),
)
def test_a_refusal_is_one_json_event(
    workdir, json_logs, options, said, event, fields,
):
    """A machine reads stderr then: what it reads is one event."""
    _unloadable(workdir)

    result = runner.invoke(app, ['data', 'slim', *options])

    assert result.exit_code == 1, result.output
    assert result.stdout == ''
    [line] = [json.loads(line) for line in result.stderr.splitlines()]
    assert (line['event'], line['level'], line['logger']) == (
        event, 'error', 'data_slim',
    )
    assert {name: line[name] for name in fields} == fields
