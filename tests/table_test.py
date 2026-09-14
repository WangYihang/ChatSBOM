"""Tests for the column-contract layer that replaces positional row access."""
import pytest

from chatsbom.core.table import Table


TOY = Table(
    name='toy',
    columns=('id', 'owner', 'stars'),
)


def test_row_projects_mapping_into_column_order():
    assert TOY.row({'stars': 7, 'id': 1, 'owner': 'a'}) == [1, 'a', 7]


def test_rows_projects_many_mappings():
    assert TOY.rows([
        {'id': 1, 'owner': 'a', 'stars': 7},
        {'id': 2, 'owner': 'b', 'stars': 8},
    ]) == [[1, 'a', 7], [2, 'b', 8]]


def test_row_rejects_missing_column():
    with pytest.raises(KeyError, match='stars'):
        TOY.row({'id': 1, 'owner': 'a'})


def test_row_rejects_unknown_column():
    with pytest.raises(KeyError, match='nope'):
        TOY.row({'id': 1, 'owner': 'a', 'stars': 7, 'nope': True})


def test_columns_are_immutable():
    with pytest.raises(TypeError):
        TOY.columns[0] = 'other'


def test_column_names_exposed_for_clickhouse_insert():
    assert TOY.column_names == ['id', 'owner', 'stars']
