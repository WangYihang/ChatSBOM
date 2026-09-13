"""`db index` option combinations that would destroy data.

`--rebuild` drops the whole artifacts table; `--language` narrows what is
re-ingested. Together they drop eight languages and re-ingest one — a
combination that reads as narrow and acts as total.
"""
from typer.testing import CliRunner

from chatsbom.__main__ import app

runner = CliRunner()


def test_rebuild_with_a_language_is_refused():
    result = runner.invoke(
        app, ['db', 'index', '--rebuild', '--language', 'java'],
    )
    assert result.exit_code != 0
    assert 'rebuild' in result.output.lower()
    assert 'language' in result.output.lower()


def test_the_refusal_explains_the_consequence():
    result = runner.invoke(
        app, ['db', 'index', '--rebuild', '--language', 'ruby'],
    )
    # Someone reaching for this combination wants one language refreshed,
    # so the message has to name what would actually happen.
    assert 'every language' in result.output or 'all languages' in result.output


def test_rebuild_alone_is_accepted():
    """Only the parse is checked here; the ingest needs a database."""
    result = runner.invoke(app, ['db', 'index', '--rebuild', '--help'])
    assert result.exit_code == 0


def test_language_alone_is_accepted():
    result = runner.invoke(
        app, ['db', 'index', '--language', 'java', '--help'],
    )
    assert result.exit_code == 0
