"""Write the dataset as Parquet, plus a manifest describing it.

The whole dependency graph compresses to tens of megabytes, which is small
enough to hand to a browser and query there. Exporting it decouples the
dashboard from ClickHouse entirely: the front end reads static files, so
there is no query backend to run, secure or pay for.

Columns are declared in `chatsbom.export.schema` and asserted against on
the way out, so the Parquet layout and the generated TypeScript types
cannot disagree.
"""
import hashlib
import json
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any
from typing import TYPE_CHECKING

import structlog

from chatsbom.__version__ import __version__
from chatsbom.core.repository import QueryRepository
from chatsbom.export.schema import ColumnType
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.export.schema import ExportSchema
from chatsbom.export.schema import ExportTable
from chatsbom.models.relationship import DIRECT

if TYPE_CHECKING:  # pragma: no cover - import cost only matters at runtime
    import pyarrow as pa

logger = structlog.get_logger('export_parquet')

MANIFEST_NAME = 'manifest.json'
ROW_GROUP_SIZE = 200_000

REPOSITORIES_QUERY = f"""
SELECT
    r.id AS id,
    r.owner AS owner,
    r.repo AS repo,
    r.stars AS stars,
    lower(r.language) AS language,
    r.url AS url,
    r.description AS description,
    r.license_spdx_id AS license_spdx_id,
    formatDateTime(r.pushed_at, '%Y-%m-%d') AS pushed_at,
    -- When *we* last looked, as distinct from when upstream last
    -- pushed. A repository can have been pushed to yesterday and last
    -- scanned six months ago, and only the second explains a stale row.
    -- max(observed_at) over its artifacts is the recorded observation;
    -- the 11,840 repositories with no dependencies have no artifact to
    -- carry one, so those fall back to the row's own write time.
    formatDateTime(
        greatest(max(a.observed_at), r.updated_at), '%Y-%m-%d'
    ) AS observed_at,
    r.sbom_ref AS sbom_ref,
    r.sbom_commit_sha AS sbom_commit_sha,
    countDistinctIf(
        a.name, a.name != '' AND a.relationship = '{DIRECT}'
    ) AS direct_dependencies,
    countDistinctIf(a.name, a.name != '') AS total_dependencies,
    r.manifest_sources AS manifest_sources
FROM repositories AS r FINAL
LEFT JOIN artifacts AS a
    ON a.repository_id = r.id AND a.sbom_commit_sha = r.sbom_commit_sha
GROUP BY
    r.id, r.owner, r.repo, r.stars, r.language, r.url, r.description,
    r.license_spdx_id, r.pushed_at, r.updated_at, r.sbom_ref,
    r.sbom_commit_sha,
    r.manifest_sources
ORDER BY r.stars DESC, r.id ASC
"""

# Sorted by name so a "who depends on X" lookup touches few row groups.
ARTIFACTS_QUERY = """
SELECT
    a.repository_id AS repository_id,
    a.name AS name,
    a.version AS version,
    a.type AS type,
    a.found_by AS found_by,
    a.relationship AS relationship,
    a.source AS source,
    a.version_kind AS version_kind
FROM artifacts AS a
INNER JOIN (
    SELECT id, sbom_commit_sha FROM repositories FINAL
) AS r ON a.repository_id = r.id AND a.sbom_commit_sha = r.sbom_commit_sha
GROUP BY
    a.repository_id, a.name, a.version, a.type, a.found_by, a.relationship,
    a.source, a.version_kind
ORDER BY a.name ASC, a.repository_id ASC, a.version ASC
"""

# Monthly adoption per package, straight off the append-only table. Kept
# in its own file so the dashboard's current-state payload stays small —
# only a page asking a temporal question needs to fetch this.
HISTORY_QUERY = f"""
-- Per source, not merged.
--
-- Syft resolves a lockfile's closure; GitHub's graph parses manifests.
-- They ran seven months apart, so a single series over both drew a line
-- from February's 124 to September's 149 and read as adoption growing
-- when the only thing that changed was the instrument.
SELECT
    a.name AS name,
    formatDateTime(a.observed_at, '%Y-%m') AS month,
    a.source AS source,
    count(DISTINCT a.repository_id) AS repository_count,
    count(DISTINCT if(a.relationship = '{DIRECT}', a.repository_id, NULL))
        AS direct_count
FROM artifacts AS a
WHERE a.name != ''
GROUP BY a.name, month, a.source
ORDER BY a.name ASC, a.source ASC, month ASC
"""

# Licence distribution. Unknown is kept as an explicit empty string rather
# than dropped: "we do not know" is a finding about SBOM quality, and
# hiding it would overstate how well licences are covered.
LICENSES_QUERY = """
SELECT
    coalesce(arrayElement(a.licenses, 1), '') AS license,
    a.type AS type,
    countDistinct(a.name) AS package_count,
    countDistinct(a.repository_id) AS repository_count
FROM artifacts AS a
INNER JOIN (
    SELECT id, sbom_commit_sha FROM repositories FINAL
) AS r ON a.repository_id = r.id AND a.sbom_commit_sha = r.sbom_commit_sha
WHERE a.name != ''
GROUP BY license, type
ORDER BY repository_count DESC, license ASC
LIMIT 500
"""

QUERIES: dict[str, str] = {
    'repositories': REPOSITORIES_QUERY,
    'artifacts': ARTIFACTS_QUERY,
    'licenses': LICENSES_QUERY,
    'history': HISTORY_QUERY,
}


@dataclass(frozen=True, slots=True)
class ExportResult:
    """What an export produced, for logging and for the manifest."""

    directory: Path
    row_counts: dict[str, int] = field(default_factory=dict)
    checksums: dict[str, str] = field(default_factory=dict)
    sizes: dict[str, int] = field(default_factory=dict)
    #: Span of observation dates found in the data, for the manifest.
    #: Empty when nothing carried a date — a default would read as a
    #: real observation.
    freshness: dict[str, str] = field(default_factory=dict)

    @property
    def total_bytes(self) -> int:
        return sum(self.sizes.values())


def _require_pyarrow() -> Any:
    try:
        import pyarrow  # noqa: F401
        import pyarrow.parquet as pq
    except ModuleNotFoundError as e:  # pragma: no cover
        raise RuntimeError(
            'Parquet export needs pyarrow. Install it with '
            '`uv add pyarrow` or `pip install pyarrow`.',
        ) from e
    return pq


def _arrow_schema(table: ExportTable) -> 'pa.Schema':
    import pyarrow as pa

    mapping = {
        ColumnType.STRING: pa.string(),
        ColumnType.DATE: pa.string(),
        ColumnType.INTEGER: pa.int64(),
        ColumnType.STRING_LIST: pa.list_(pa.string()),
    }
    return pa.schema(
        [(c.name, mapping[c.type]) for c in table.columns],
    )


def _columnar(
    rows: Iterator[Mapping[str, Any]],
    table: ExportTable,
) -> dict[str, list[Any]]:
    """Collect named rows into per-column lists, in declared order."""
    columns: dict[str, list[Any]] = {c.name: [] for c in table.columns}
    for row in rows:
        missing = [name for name in columns if name not in row]
        if missing:
            raise KeyError(
                f"{table.name} query is missing column(s) "
                f"{', '.join(missing)}",
            )
        for name, values in columns.items():
            values.append(row[name])
    return columns


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as f:
        while chunk := f.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def export_dataset(
    query_repo: QueryRepository,
    directory: Path,
    schema: ExportSchema = EXPORT_SCHEMA,
) -> ExportResult:
    """Write one Parquet file per exported table, plus a manifest."""
    pq = _require_pyarrow()
    import pyarrow as pa

    directory.mkdir(parents=True, exist_ok=True)

    row_counts: dict[str, int] = {}
    checksums: dict[str, str] = {}
    sizes: dict[str, int] = {}
    freshness: dict[str, str] = {}

    for table in schema.tables:
        sql = QUERIES[table.name]

        # Probed before streaming, because a truncated stream is
        # indistinguishable from a complete one: the guest profile caps
        # `max_result_rows` with `result_overflow_mode=break`, which stops
        # returning rows without raising. A real export lost 6.0M of 6.1M
        # artifact rows and still printed "Export Complete".
        expected = query_repo.count_rows(sql)

        arrow_table = pa.table(
            _columnar(query_repo.stream_rows(sql), table),
            schema=_arrow_schema(table),
        )

        if arrow_table.num_rows != expected:
            raise RuntimeError(
                f"Export of {table.name!r} was truncated: wrote "
                f"{arrow_table.num_rows:,} of {expected:,} rows.\n\n"
                f"The usual cause is a result-row cap on the connecting "
                f"account — ClickHouse's result_overflow_mode=break stops "
                f"returning rows without an error. Export connects as "
                f"admin for this reason; check "
                f"database/config/users.d/ if you changed the profile.",
            )
        path = directory / f'{table.name}.parquet'

        # zstd with dictionary encoding is what makes the payload small
        # enough to ship to a browser; the row group size bounds how much
        # a client must fetch to answer a point lookup.
        pq.write_table(
            arrow_table,
            path,
            compression='zstd',
            compression_level=9,
            use_dictionary=True,
            row_group_size=ROW_GROUP_SIZE,
            write_statistics=True,
            store_schema=True,
        )

        # Named after its own content, which can only be known once it
        # is written — so write, hash, then rename. `immutable` is a lie
        # on a fixed filename: the URL would be reused by the next
        # export while clients kept the old bytes for a year.
        digest = _sha256(path)
        addressed = content_addressed_name(f'{table.name}.parquet', digest)
        path.replace(directory / addressed)
        path = directory / addressed

        row_counts[table.name] = arrow_table.num_rows
        checksums[addressed] = digest
        sizes[addressed] = path.stat().st_size

        # Freshness comes from whichever table carries observation
        # dates, read off the column that was just written rather than
        # queried again — the two could disagree if collection landed
        # between them.
        if 'observed_at' in table.column_names:
            freshness = observed_range(
                arrow_table.column('observed_at').to_pylist(),
            )

        logger.info(
            'Exported table',
            table=table.name,
            file=addressed,
            rows=arrow_table.num_rows,
            bytes=sizes[addressed],
        )

    result = ExportResult(
        directory=directory,
        row_counts=row_counts,
        checksums=checksums,
        sizes=sizes,
        freshness=freshness,
    )
    _write_manifest(directory, schema, result)
    return result


def content_addressed_name(filename: str, checksum: str) -> str:
    """Name a file after its own content, so `immutable` is honest.

    Parquet is served `immutable, max-age=31536000` because a client
    should never re-fetch a file it already holds — that is what makes a
    query cost one ranged GET rather than a download. With fixed
    filenames every export reused the same URLs, so a browser kept last
    week's table for a year while revalidating a manifest that described
    a different file. The symptom was a schema error, not a cache error:
    the manifest advertised sha 659592a2 while the browser still held
    e8e84bf5, and each query failed with `Binder Error: Table "r" does
    not have a column named "observed_at"`.

    The manifest itself is exempt. It is the entry point, so its URL has
    to be stable to be found at all — which is why it alone is served
    with `must-revalidate`.
    """
    if filename == MANIFEST_NAME:
        return filename
    stem, _, extension = filename.rpartition('.')
    return f'{stem}-{checksum[:8]}.{extension}'


def observed_range(dates: Iterable[str]) -> dict[str, str]:
    """The span of observation dates actually present in a table.

    Derived from the rows rather than read off a clock, for two reasons.
    The manifest is content-addressed by its checksums, so a wall time
    would make byte-identical exports differ. And an export can run long
    after collection, so a wall time describes when the export ran —
    which is the wrong thing to hold up against a row that looks stale.

    Blank dates are observations that never happened and are excluded;
    including them would report an `observedFrom` of '' for any dataset
    with one unscanned row.
    """
    seen = sorted(d for d in dates if d)
    if not seen:
        return {}
    return {'observedFrom': seen[0], 'observedTo': seen[-1]}


def _write_manifest(
    directory: Path,
    schema: ExportSchema,
    result: ExportResult,
) -> None:
    """Describe the export so a client can verify and version it.

    No clock reading: the manifest is content-addressed by its
    checksums, so a wall time would make byte-identical exports differ.
    Freshness is carried instead as the span of observation dates found
    in the data, which is reproducible *and* the more useful answer —
    an export can run long after collection, so its wall time describes
    the export rather than the rows.
    """
    manifest = {
        'schemaVersion': schema.version,
        'generator': f'chatsbom/{__version__}',
        'rowCounts': result.row_counts,
        # Derived from the rows, so the manifest stays reproducible and
        # describes the data's age rather than the export's.
        'freshness': result.freshness,
        'files': [
            {
                'name': name,
                'bytes': result.sizes[name],
                'sha256': checksum,
            }
            for name, checksum in sorted(result.checksums.items())
        ],
        'schema': schema.to_dict(),
    }
    path = directory / MANIFEST_NAME
    path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + '\n',
        encoding='utf-8',
    )
