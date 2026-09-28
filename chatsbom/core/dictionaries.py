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

from chatsbom.core.schema import language_bucket_sql

#: Columns the dependants query reads. Nothing else, because every
#: column is another copy held in memory for every repository.
#:
#: `sbom_commit_sha` is read to filter, not to show: an artifact row
#: counts only if it belongs to the scan its repository records now,
#: which is how the CLI and the exports have always counted. Without it
#: the dashboard listed a repository at every version it was ever seen
#: at. The `current_artifacts` view answers the same question for the
#: rollups by joining `repositories FINAL`; on a per-request query that
#: join would be rebuilt every time, and this is a lookup.
#:
#: Measured on synthetic data of the corpus's shape — 28,075
#: repositories, 2,000,000 artifact rows — the check adds 0.4 ms to a
#: point lookup (4.3 ms to 4.7 ms), where reading it through the view
#: took 13.6 ms against 4.2 ms, and the dictionary grows from 9.0 MiB
#: to 12.5 MiB.
#:
#: `depgraph_observed_at` for the same reason, for dependency-graph
#: rows, which are current by the graph document their repository
#: records rather than by its commit (#22; `CURRENT_OBSERVATION` in
#: `core/schema.py`, which the dashboard's check restates). A `DateTime`,
#: as the rows' `observed_at` is, so the comparison is between two
#: stored seconds. On the same synthetic data it grows the dictionary
#: from 12.5 MiB to 13.5 MiB — each attribute of a HASHED layout is a
#: hash table of its own over every key, which four bytes a value do not
#: fill — where the document's sha256 as a `String` would have taken it
#: to 18.0 MiB.
#:
#: **Of the corpus only** (owner decision D2 on #55): loaded from the
#: `corpus` view, so `dictHas` is false for a repository the current
#: search snapshot does not list, and the dependants query leaves it
#: out as the rollups do.
#:
#: `language` is GitHub's language, verbatim, for the table to show;
#: `language_bucket` is the top-twelve fold (D7) the language filter
#: matches, computed by the same expression as the coverage rollup.
_REPOSITORIES_TEMPLATE = """
CREATE DICTIONARY IF NOT EXISTS dict_repositories (
    id UInt64,
    owner String,
    repo String,
    url String,
    stars UInt64,
    language String,
    sbom_commit_sha String,
    depgraph_observed_at DateTime,
    -- Last, as in the QUERY below: the source's columns are matched to
    -- these by position.
    language_bucket String
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
-- month old star counts while `repositories FINAL` held the new ones,
-- and now that it decides which scan is current, it would have counted
-- the previous scan instead of this one.
--
-- From `corpus`, which reads `repositories FINAL`: the same holds.
SOURCE(CLICKHOUSE(
    QUERY 'SELECT id, owner, repo, url, stars,
                  github_language AS language,
                  sbom_commit_sha, depgraph_observed_at,
                  [bucket] AS language_bucket
           FROM {database}.corpus'
    USER '{user}' PASSWORD '{password}'))
LIFETIME(MIN 300 MAX 600)
LAYOUT(HASHED())
""".strip()

#: The bucket expression filled in once, reading the database's own
#: `language_buckets`. Its quotes doubled: it sits inside the QUERY
#: string literal.
REPOSITORIES = _REPOSITORIES_TEMPLATE.replace(
    '[bucket]',
    language_bucket_sql(
        'github_language', '{database}.language_buckets',
    ).replace("'", "''"),
)

DICTIONARIES: tuple[tuple[str, str], ...] = (
    ('dict_repositories', REPOSITORIES),
)
