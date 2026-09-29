"""Schema invariants: insert contracts must match the DDL they insert into."""
import clickhouse_connect
import pytest

from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import ARTIFACTS_DDL
from chatsbom.core.schema import ddl_columns
from chatsbom.core.schema import identifier
from chatsbom.core.schema import RELEASES
from chatsbom.core.schema import RELEASES_DDL
from chatsbom.core.schema import REPOSITORIES
from chatsbom.core.schema import REPOSITORIES_DDL
from tests.conftest import CLICKHOUSE_HOST
from tests.conftest import CLICKHOUSE_PASSWORD
from tests.conftest import CLICKHOUSE_PORT
from tests.conftest import CLICKHOUSE_USER
from tests.conftest import requires_clickhouse

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


#: Names a database can be given that are not bare identifiers, as
#: CLICKHOUSE_DB takes them (#120).
AWKWARD_NAMES = [
    'chatsbom-test', 'with space', 'a.b', "it's", 'back`quote', 'back\\slash',
]


@pytest.fixture
def server():
    """The admin account on `default`, which every server has."""
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
        database='default',
    )
    try:
        yield client
    finally:
        client.close()


@requires_clickhouse
@pytest.mark.parametrize('name', AWKWARD_NAMES)
def test_a_quoted_identifier_is_the_name_it_quotes(server, name):
    """Read back by the server itself, as the name of a column."""
    result = server.query(f'SELECT 1 AS {identifier(name)}')
    assert result.column_names == (name,)
