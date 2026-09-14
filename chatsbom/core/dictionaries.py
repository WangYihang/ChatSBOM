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
-- `QUERY ... FINAL`, not `TABLE 'repositories'`.
--
-- `repositories` is a ReplacingMergeTree, and a dictionary loading it
-- as a plain table does not apply the replacement — it takes whichever
-- duplicate it reads last. Measured on a scratch table with two
-- unmerged parts holding one id:
--
--     stale inserted first, fresh second   dictGet -> fresh
--     fresh inserted first, stale second   dictGet -> stale
--
-- So the result is insert-order dependent, and the order is not ours
-- to choose. That is not hypothetical here: `db index` applies a
-- metadata overlay that writes a fresher row for around 24,000
-- repositories, so every ingest creates exactly the window this needs
-- to go wrong in — and the dashboard reads owner, repo and stars for
-- every point lookup from this dictionary. It would have shown seven
-- month old star counts while `repositories FINAL` held the new ones.
SOURCE(CLICKHOUSE(
    QUERY 'SELECT id, owner, repo, url, stars, language
           FROM {database}.repositories FINAL'
    USER '{user}' PASSWORD '{password}'))
LIFETIME(MIN 300 MAX 600)
LAYOUT(HASHED())
""".strip()

DICTIONARIES: tuple[tuple[str, str], ...] = (
    ('dict_repositories', REPOSITORIES),
)
