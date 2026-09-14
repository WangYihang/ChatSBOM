"""The repository dictionary, and the replacement it has to respect.

`dict_repositories` backs every point lookup the dashboard makes —
owner, repo and stars come from it rather than from a join. Its source
table is a `ReplacingMergeTree`, and a dictionary that loads such a
table *as a table* does not apply the replacement: it keeps whichever
duplicate it reads last.

Measured on a scratch table holding one id in two unmerged parts:

    stale inserted first, fresh second   dictGet -> fresh
    fresh inserted first, stale second   dictGet -> stale

`db index` creates that window on every run — the metadata overlay
writes a fresher row for around 24,000 repositories — so the dashboard
could have served seven-month-old star counts while
`repositories FINAL` held the new ones.
"""
from __future__ import annotations

from chatsbom.core.dictionaries import DICTIONARIES


def _without_comments(ddl: str) -> str:
    """The DDL as ClickHouse reads it, with `--` lines removed.

    Learned on `rollups_test.py`: a substring assertion cannot tell SQL
    from a note about SQL, so commenting out the guarded line passed
    the test that guarded it.
    """
    return '\n'.join(
        line for line in ddl.split('\n')
        if not line.strip().startswith('--')
    )


class TestRepositoryDictionary:

    def test_it_loads_through_final_rather_than_the_bare_table(self) -> None:
        _, ddl = next(d for d in DICTIONARIES if d[0] == 'dict_repositories')
        sql = _without_comments(ddl)
        assert 'FINAL' in sql
        # The bare form is what served the stale row.
        assert "TABLE 'repositories'" not in sql

    def test_it_selects_only_the_columns_the_lookup_reads(self) -> None:
        """Every column is another copy held in memory for all 28,075
        repositories. `SELECT *` would also break the declared layout,
        which lists exactly six."""
        _, ddl = next(d for d in DICTIONARIES if d[0] == 'dict_repositories')
        sql = _without_comments(ddl)
        assert '*' not in sql
        for column in ('id', 'owner', 'repo', 'url', 'stars', 'language'):
            assert column in sql

    def test_it_reloads_on_a_lifetime_rather_than_on_ingest(self) -> None:
        """Repository metadata changes on its own schedule — a stars
        refresh, a rename — and a dictionary that only reloaded during
        an ingest would serve the old numbers until the next one."""
        _, ddl = next(d for d in DICTIONARIES if d[0] == 'dict_repositories')
        assert 'LIFETIME' in _without_comments(ddl)

    def test_creating_one_does_not_replace_it(self) -> None:
        """`IF NOT EXISTS`, so starting the app cannot drop a loaded
        dictionary and leave the dashboard answering defaults."""
        for _, ddl in DICTIONARIES:
            assert 'IF NOT EXISTS' in _without_comments(ddl)

    def test_the_credentials_are_placeholders(self) -> None:
        """A dictionary DDL carries a password. It has to come from the
        config at creation time, never be committed."""
        for _, ddl in DICTIONARIES:
            assert '{password}' in ddl
            assert '{user}' in ddl
            assert '{database}' in ddl


class TestChangedDefinitionsReachTheDatabase:
    """`CREATE ... IF NOT EXISTS` cannot notice that a DDL changed.

    The rollups already take `recreate` for exactly this reason. The
    dictionaries did not, so the `QUERY ... FINAL` correction — a
    dictionary serving a superseded row — would have applied on a fresh
    machine and silently not on any database that already had the old
    one. Dropping a dictionary costs a reload of 28,075 rows and no
    stored data, unlike a rollup.
    """

    def test_ensure_dictionaries_can_recreate(self) -> None:
        import inspect
        from chatsbom.core.repository import IngestionRepository
        signature = inspect.signature(
            IngestionRepository._ensure_dictionaries,
        )
        assert 'recreate' in signature.parameters

    def test_it_drops_before_declaring_when_recreating(self) -> None:
        import inspect
        from chatsbom.core.repository import IngestionRepository
        source = inspect.getsource(
            IngestionRepository._ensure_dictionaries,
        )
        assert 'DROP DICTIONARY IF EXISTS' in source

    def test_a_rebuild_asks_for_it(self) -> None:
        """Otherwise the flag exists and nothing sets it, which is the
        same as not having it."""
        import inspect
        from chatsbom.core.repository import IngestionRepository
        source = inspect.getsource(IngestionRepository)
        assert '_ensure_dictionaries(recreate=bool(rebuild))' in source
