"""The canonical ecosystem mapping, and the two copies of it.

`core/ecosystems.py` serves the rollups and `web/src/ecosystems.ts`
serves the browser. Two copies of five lines of data is worse than one,
and a generator for five lines would be worse than either — so this is
what keeps them honest.

Drift here is not cosmetic. Counting `cargo` apart from `rust-crate`
made one registry look like two and inflated the edge panels' warning
fivefold: 39,658 ambiguous names and 51.5% of edges against a true
2,730 and 10.3%.
"""
from __future__ import annotations

import re
from pathlib import Path

from chatsbom.core.ecosystems import canonical_sql
from chatsbom.core.ecosystems import MEMBERS
from chatsbom.core.ecosystems import RENAMES

TYPESCRIPT = Path(__file__).resolve().parents[1] / 'web/src/ecosystems.ts'


def _typescript_members() -> dict[str, tuple[str, ...]]:
    """The mapping as the browser copy declares it.

    Parsed rather than imported: there is no Node in the Python test
    run, and the shape is a literal object precisely so it can be read
    this way.
    """
    source = TYPESCRIPT.read_text(encoding='utf-8')
    body = source[source.index('const MEMBERS'):]
    body = body[body.index('{'):body.index('};') + 1]
    members: dict[str, tuple[str, ...]] = {}
    for match in re.finditer(r"'?([\w.-]+)'?:\s*\[([^\]]*)\]", body):
        raw = re.findall(r"'([^']+)'", match.group(2))
        members[match.group(1)] = tuple(raw)
    return members


class TestTheTwoCopiesAgree:

    def test_the_typescript_copy_parses(self) -> None:
        """If this breaks, the comparison below is vacuous rather than
        failing — so it is asserted separately."""
        assert len(_typescript_members()) >= 10

    def test_every_canonical_name_matches(self) -> None:
        assert set(_typescript_members()) == set(MEMBERS)

    def test_every_member_list_matches(self) -> None:
        typescript = _typescript_members()
        for name, members in MEMBERS.items():
            assert tuple(typescript[name]) == members, name


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
