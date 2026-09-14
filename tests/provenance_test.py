"""Where a dependency record came from, and how firm its version is."""
import pytest

from chatsbom.models.provenance import ARTIFACT_SOURCES
from chatsbom.models.provenance import as_artifact_source
from chatsbom.models.provenance import as_version_kind
from chatsbom.models.provenance import CONSTRAINT
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.provenance import RESOLVED
from chatsbom.models.provenance import SYFT
from chatsbom.models.provenance import UNVERSIONED
from chatsbom.models.provenance import VERSION_KINDS


def test_sources_and_literal_agree():
    assert ARTIFACT_SOURCES == (SYFT, DEPGRAPH)


def test_version_kinds_and_literal_agree():
    assert VERSION_KINDS == (RESOLVED, CONSTRAINT, UNVERSIONED)


def test_values_are_the_strings_clickhouse_stores():
    assert (SYFT, DEPGRAPH) == ('syft', 'github-depgraph')
    assert (RESOLVED, CONSTRAINT, UNVERSIONED) == (
        'resolved', 'constraint', 'unversioned',
    )


@pytest.mark.parametrize('value', ARTIFACT_SOURCES)
def test_known_sources_round_trip(value):
    assert as_artifact_source(value) == value


@pytest.mark.parametrize('value', VERSION_KINDS)
def test_known_version_kinds_round_trip(value):
    assert as_version_kind(value) == value


@pytest.mark.parametrize('bad', ['Syft', 'github', '', None])
def test_unknown_source_rejected(bad):
    with pytest.raises(ValueError, match='source'):
        as_artifact_source(bad)


@pytest.mark.parametrize('bad', ['Resolved', 'range', '', None])
def test_unknown_version_kind_rejected(bad):
    with pytest.raises(ValueError, match='version kind'):
        as_version_kind(bad)


# --- version classification -----------------------------------------------

@pytest.mark.parametrize(
    'raw,expected', [
        ('2.9.0', ('2.9.0', RESOLVED)),
        ('v1.2.3', ('v1.2.3', RESOLVED)),
        ('1.0.0-beta.1', ('1.0.0-beta.1', RESOLVED)),
        ('>= 0', ('>= 0', CONSTRAINT)),
        ('^4.18.0', ('^4.18.0', CONSTRAINT)),
        ('~> 2.8', ('~> 2.8', CONSTRAINT)),
        ('*', ('*', CONSTRAINT)),
        ('latest', ('latest', CONSTRAINT)),
        ('1.0 - 2.0', ('1.0 - 2.0', CONSTRAINT)),
        ('', ('', UNVERSIONED)),
        (None, ('', UNVERSIONED)),
        ('   ', ('', UNVERSIONED)),
    ],
)
def test_classify_version(raw, expected):
    from chatsbom.models.provenance import classify_version
    assert classify_version(raw) == expected
