"""The dependency relationship is a closed set, not an open string."""
import pytest

from chatsbom.models.relationship import as_relationship
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import is_relationship
from chatsbom.models.relationship import RELATIONSHIPS
from chatsbom.models.relationship import TRANSITIVE
from chatsbom.models.relationship import UNKNOWN


def test_the_literal_and_the_constants_agree():
    """A new member of the Literal must show up here, not drift silently."""
    assert RELATIONSHIPS == (DIRECT, TRANSITIVE, UNKNOWN)


def test_values_are_the_strings_clickhouse_stores():
    assert (DIRECT, TRANSITIVE, UNKNOWN) == ('direct', 'transitive', 'unknown')


@pytest.mark.parametrize('value', RELATIONSHIPS)
def test_known_values_round_trip(value):
    assert as_relationship(value) == value
    assert is_relationship(value)


@pytest.mark.parametrize('value', ['Direct', 'dev', '', None, 0])
def test_unknown_values_are_rejected(value):
    assert not is_relationship(value)
    with pytest.raises(ValueError, match='relationship'):
        as_relationship(value)
