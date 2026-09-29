"""A snapshot's tables: `export d1`'s, and a `meta` that says what it is.

A snapshot starts from `D1_SCHEMA` (#132): every table and column the
D1 backend and `Dataset` read, under the same names, with D1's indexes,
so that the same statements answer the same questions over either. What
it adds is in `meta`, after the four columns D1's has, which `Dataset`
reads as it reads D1's: which snapshot this is, the code that wrote it,
the corpus it describes, and how many rows each table has.
"""
from __future__ import annotations

from chatsbom.export.d1 import D1_SCHEMA
from chatsbom.export.d1 import D1Column
from chatsbom.export.d1 import D1Schema
from chatsbom.export.d1 import D1Table
from chatsbom.export.d1 import META as D1_META

META = D1Table(
    name='meta',
    description=(
        'Provenance: which snapshot this is, the build that wrote it and '
        'the corpus it describes, how fresh its rows are and how many '
        'there are. One row.'
    ),
    columns=(
        *D1_META.columns,
        D1Column(
            'snapshot', 'TEXT NOT NULL',
            'Its id: the file is `<id>.sqlite`, and the same content has '
            'the same id.',
        ),
        D1Column(
            'version', 'TEXT NOT NULL',
            "The version of chatsbom that wrote it: the generator's.",
        ),
        D1Column(
            'corpus', 'TEXT NOT NULL',
            'The search snapshot the corpus is, or empty when the store '
            'has none and the corpus is every repository.',
        ),
        D1Column(
            'rows', 'TEXT NOT NULL',
            'Rows of each table, this one included, as a JSON object.',
        ),
    ),
)

SCHEMA = D1Schema(
    tables=tuple(
        META if table.name == META.name else table
        for table in D1_SCHEMA.tables
    ),
    indexes=D1_SCHEMA.indexes,
)
