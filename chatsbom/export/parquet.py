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
    r.sbom_ref AS sbom_ref,
    r.sbom_commit_sha AS sbom_commit_sha,
    countDistinctIf(
        a.name, a.name != '' AND a.relationship = '{DIRECT}'
    ) AS direct_dependencies,
    countDistinctIf(a.name, a.name != '') AS total_dependencies,
    [] AS manifest_sources
FROM repositories AS r FINAL
LEFT JOIN artifacts AS a
    ON a.repository_id = r.id AND a.sbom_commit_sha = r.sbom_commit_sha
GROUP BY
    r.id, r.owner, r.repo, r.stars, r.language, r.url, r.description,
    r.license_spdx_id, r.pushed_at, r.sbom_ref, r.sbom_commit_sha
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
    a.relationship AS relationship
FROM artifacts AS a
INNER JOIN (
    SELECT id, sbom_commit_sha FROM repositories FINAL
) AS r ON a.repository_id = r.id AND a.sbom_commit_sha = r.sbom_commit_sha
GROUP BY
    a.repository_id, a.name, a.version, a.type, a.found_by, a.relationship
ORDER BY a.name ASC, a.repository_id ASC, a.version ASC
"""

QUERIES: dict[str, str] = {
    'repositories': REPOSITORIES_QUERY,
    'artifacts': ARTIFACTS_QUERY,
}


@dataclass(frozen=True, slots=True)
class ExportResult:
    """What an export produced, for logging and for the manifest."""

    directory: Path
    row_counts: dict[str, int] = field(default_factory=dict)
    checksums: dict[str, str] = field(default_factory=dict)
    sizes: dict[str, int] = field(default_factory=dict)

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

    for table in schema.tables:
        arrow_table = pa.table(
            _columnar(
                query_repo.stream_rows(QUERIES[table.name]),
                table,
            ),
            schema=_arrow_schema(table),
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

        row_counts[table.name] = arrow_table.num_rows
        checksums[f'{table.name}.parquet'] = _sha256(path)
        sizes[f'{table.name}.parquet'] = path.stat().st_size

        logger.info(
            'Exported table',
            table=table.name,
            rows=arrow_table.num_rows,
            bytes=sizes[f'{table.name}.parquet'],
        )

    result = ExportResult(
        directory=directory,
        row_counts=row_counts,
        checksums=checksums,
        sizes=sizes,
    )
    _write_manifest(directory, schema, result)
    return result


def _write_manifest(
    directory: Path,
    schema: ExportSchema,
    result: ExportResult,
) -> None:
    """Describe the export so a client can verify and version it.

    No timestamp: the manifest is content-addressed by the checksums, and
    a clock reading would make byte-identical exports differ.
    """
    manifest = {
        'schemaVersion': schema.version,
        'generator': f'chatsbom/{__version__}',
        'rowCounts': result.row_counts,
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
