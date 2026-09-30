"""The canonical ecosystem mapping (`core/ecosystems.py`).

A spelling left out is not cosmetic. Counting `cargo` apart from
`rust-crate` made one registry look like two and inflated the edge
panels' warning fivefold: 39,658 ambiguous names and 51.5% of edges
against a true 2,730 and 10.3%.
"""
from __future__ import annotations

from chatsbom.core.ecosystems import artifact_ecosystem
from chatsbom.core.ecosystems import canonical_sql
from chatsbom.core.ecosystems import LANGUAGE_ECOSYSTEM
from chatsbom.core.ecosystems import MEMBERS
from chatsbom.core.ecosystems import RENAMES


class TestTheLanguageMap:
    """A framework's language names the ecosystem its packages are of."""

    def test_every_target_is_a_canonical_ecosystem(self) -> None:
        assert set(LANGUAGE_ECOSYSTEM.values()) <= set(MEMBERS)


class TestTheSqlExpression:

    def test_it_renames_the_collectors_spellings(self) -> None:
        sql = canonical_sql('type')
        for raw, canonical in RENAMES.items():
            assert f"'{raw}'" in sql
            assert f"'{canonical}'" in sql

    def test_an_unknown_type_passes_through(self) -> None:
        """`transform` without a default returns an empty string for an
        unmatched value, which would file a new ecosystem under no name
        at all. A new registry should read as itself — that is also the
        signal the table needs a line adding."""
        sql = canonical_sql('type')
        assert sql.rstrip().endswith('type)')

    def test_it_takes_the_column_it_is_given(self) -> None:
        assert canonical_sql('a.type').startswith('transform(a.type')

    def test_a_name_equal_to_its_canonical_is_not_renamed(self) -> None:
        """`npm` maps to `npm`; listing it would make the expression
        longer and say nothing."""
        assert 'npm' not in RENAMES
        assert RENAMES['rust-crate'] == 'cargo'


class TestSyftsOwnSpellings:
    """What Syft calls a type, beside what the graph and discovery call
    its ecosystem."""

    def test_a_dart_package_is_pub(self) -> None:
        """Syft 1.41.2 and 1.52.0 both type a Dart package `dart-pub`,
        with a purl of `pkg:pub/...`. The graph and discovery call it
        `pub`, so unmapped it was an ecosystem of its own (#120)."""
        assert RENAMES.get('dart-pub') == 'pub'
        assert artifact_ecosystem('dart-pub', 'pkg:pub/http@1.2.2') == 'pub'
