"""Repository metadata as a dictionary, so the dependants query stops
joining for it.

`dependentsOf` is the dashboard's slowest query and the one thing the
rollups cannot help with: the package name is arbitrary, so there is
nothing to precompute. What it does spend its time on is a join to
`repositories` for five columns — owner, repo, url, stars, language —
over a dimension table of 28,075 rows.

A dictionary is the answer ClickHouse has for exactly that shape: the
whole table lives hashed in memory (9 MiB, load factor 0.43) and the
join becomes a lookup. Measured against the join it replaces:

    laravel/framework    7.6 ms -> 2.9 ms
    ms                  13.6 ms -> 4.3 ms
    typescript           9.4 ms -> 4.4 ms

**`dictGet` is not a join, and the difference matters.** A key the
dictionary does not hold yields the type's default — an empty string, a
zero — where `INNER JOIN` would have dropped the row. So a dependency
row pointing at a repository that is not in `repositories` would render
as a blank owner with zero stars rather than being left out. There are
none today (measured: zero rows fail `dictHas`), which is exactly why
the guard has to be written now rather than when someone notices blank
rows on the page.

`LIFETIME` rather than a manual reload: repository metadata changes on
its own schedule — a stars refresh, a rename — and a dictionary that
only reloads when an ingest runs would serve the old numbers until the
next one. Five to ten minutes is short against metadata that is
currently seven months old.
"""
from __future__ import annotations

#: Columns the dependants query reads. Nothing else, because every
#: column is another copy held in memory for every repository.
REPOSITORIES = """
CREATE DICTIONARY IF NOT EXISTS dict_repositories (
    id UInt64,
    owner String,
    repo String,
    url String,
    stars UInt64,
    language String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'repositories' DB '{database}' USER '{user}' PASSWORD '{password}'))
LIFETIME(MIN 300 MAX 600)
LAYOUT(HASHED())
""".strip()

DICTIONARIES: tuple[tuple[str, str], ...] = (
    ('dict_repositories', REPOSITORIES),
)
