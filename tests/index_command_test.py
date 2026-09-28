"""`db index` option combinations that would destroy data.

`--rebuild` drops the whole artifacts table. Anything that narrows what
is then re-ingested turns a total operation into a partial one while
reading as the narrow thing.

Both narrowing options are covered, because covering only one is how
this went wrong: `--language` was guarded and tested, and then
`--rebuild --limit 3` -- meant as a smoke test -- discarded 19,384,196
rows and refilled 24 repositories. `--repos-file` took `--language`'s
place when `db index` stopped reading by language (#55).
"""
from typer.testing import CliRunner

from chatsbom.__main__ import app

runner = CliRunner()


def test_rebuild_with_a_repos_file_is_refused(tmp_path):
    repos = tmp_path / 'repos.txt'
    repos.write_text('mikel/mail\n')
    result = runner.invoke(
        app, ['db', 'index', '--rebuild', '--repos-file', str(repos)],
    )
    assert result.exit_code != 0
    assert 'rebuild' in result.output.lower()
    assert '--repos-file' in result.output


def test_the_refusal_explains_the_consequence(tmp_path):
    repos = tmp_path / 'repos.txt'
    repos.write_text('mikel/mail\n')
    result = runner.invoke(
        app, ['db', 'index', '--rebuild', '--repos-file', str(repos)],
    )
    # Someone reaching for this combination wants a few repositories
    # refreshed, so the message has to name what would actually happen.
    assert 'every repository' in result.output


def test_there_is_no_language_option():
    """Which list a repository was collected from selects nothing (#55)."""
    result = runner.invoke(app, ['db', 'index', '--language', 'java'])
    assert result.exit_code != 0
    assert 'No such option' in result.output


def test_rebuild_with_a_limit_is_refused():
    """The case the earlier guard missed.

    `--limit` narrows exactly as `--language` does, and read as
    harmless because it is what you reach for to try something small.
    """
    result = runner.invoke(app, ['db', 'index', '--rebuild', '--limit', '3'])
    assert result.exit_code != 0
    assert 'rebuild' in result.output.lower()
    assert 'limit' in result.output.lower()


def test_rebuild_with_a_limit_of_one_is_refused():
    """The smallest trial run is refused too: the rebuild is of the
    whole table, whatever is re-ingested into it."""
    result = runner.invoke(app, ['db', 'index', '--rebuild', '--limit', '1'])
    assert result.exit_code != 0
    assert '--limit' in result.output


def test_the_refusal_offers_the_thing_that_was_wanted():
    """Someone passing `--rebuild --limit 3` wants a small trial run, so
    refusing without naming the command that does that is half an
    answer."""
    result = runner.invoke(app, ['db', 'index', '--rebuild', '--limit', '3'])
    assert '--limit 3' in result.output


def test_both_narrowing_options_are_named_at_once(tmp_path):
    """Refusing one at a time would make the second failure a surprise."""
    repos = tmp_path / 'repos.txt'
    repos.write_text('mikel/mail\n')
    result = runner.invoke(
        app, [
            'db', 'index', '--rebuild', '--limit', '3',
            '--repos-file', str(repos),
        ],
    )
    assert result.exit_code != 0
    assert '--limit' in result.output and '--repos-file' in result.output


def test_rebuild_alone_is_accepted():
    """Only the parse is checked here; the ingest needs a database."""
    result = runner.invoke(app, ['db', 'index', '--rebuild', '--help'])
    assert result.exit_code == 0


def test_repos_file_alone_is_accepted():
    result = runner.invoke(
        app, ['db', 'index', '--repos-file', 'repos.txt', '--help'],
    )
    assert result.exit_code == 0


def test_limit_alone_is_accepted():
    result = runner.invoke(app, ['db', 'index', '--limit', '3', '--help'])
    assert result.exit_code == 0
