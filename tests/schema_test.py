"""Schema invariants: insert contracts must match the DDL they insert into."""
import pytest

from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import ARTIFACTS_DDL
from chatsbom.core.schema import ddl_columns
from chatsbom.core.schema import RELEASES
from chatsbom.core.schema import RELEASES_DDL
from chatsbom.core.schema import REPOSITORIES
from chatsbom.core.schema import REPOSITORIES_DDL

PAIRS = [
    (REPOSITORIES, REPOSITORIES_DDL),
    (ARTIFACTS, ARTIFACTS_DDL),
    (RELEASES, RELEASES_DDL),
]


@pytest.mark.parametrize('table,ddl', PAIRS, ids=lambda x: getattr(x, 'name', ''))
def test_every_insert_column_exists_in_ddl(table, ddl):
    declared = set(ddl_columns(ddl))
    assert declared, 'DDL column parse produced nothing'
    unknown = [c for c in table.columns if c not in declared]
    assert not unknown, f'{table.name} inserts columns absent from DDL: {unknown}'


@pytest.mark.parametrize('table,ddl', PAIRS, ids=lambda x: getattr(x, 'name', ''))
def test_insert_columns_follow_ddl_order(table, ddl):
    """Keeping the orders aligned makes the DDL readable as the contract."""
    declared = [c for c in ddl_columns(ddl) if c in set(table.columns)]
    assert list(table.columns) == declared


@pytest.mark.parametrize('table,ddl', PAIRS, ids=lambda x: getattr(x, 'name', ''))
def test_columns_omitted_from_insert_have_ddl_defaults(table, ddl):
    """Anything not inserted must be defaulted by ClickHouse, not left null."""
    omitted = [c for c in ddl_columns(ddl) if c not in set(table.columns)]
    for column in omitted:
        line = next(
            ln for ln in ddl.splitlines()
            if ln.strip().startswith(f'{column} ')
        )
        assert 'DEFAULT' in line or 'Nullable' in line, (
            f'{table.name}.{column} is neither inserted nor defaulted'
        )


def test_no_duplicate_insert_columns():
    for table, _ in PAIRS:
        assert len(set(table.columns)) == len(table.columns), table.name
