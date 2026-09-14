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
from chatsbom.export.queries import observed_range
from chatsbom.export.queries import QUERIES
from chatsbom.export.schema import ColumnType
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.export.schema import ExportSchema
from chatsbom.export.schema import ExportTable

if TYPE_CHECKING:  # pragma: no cover - import cost only matters at runtime
    import pyarrow as pa

logger = structlog.get_logger('export_parquet')

MANIFEST_NAME = 'manifest.json'
ROW_GROUP_SIZE = 200_000


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
    _remove_superseded(directory, set(sizes))
    return result


def _remove_superseded(directory: Path, current: set[str]) -> None:
    """Delete Parquet files this export did not write.

    Names are content-addressed, so a changed table lands under a new
    name and the old file is simply left behind. Measured after a
    re-export: `dist/data` held both `artifacts-5d2cc120.parquet` and
    `artifacts-283b3ee0.parquet`, 49 MB of superseded data that a
    deploy would upload and keep reachable at a live, `immutable` URL.

    `test_no_unaddressed_parquet_is_left_behind` names exactly this
    risk — "an upload ships both and the stale URL stays reachable" —
    and could not catch it, because it exports once into an empty
    directory.

    Only `*.parquet` in this directory, and only names absent from the
    manifest just written. The manifest is the record of what belongs;
    anything else is a previous run's.
    """
    for path in sorted(directory.glob('*.parquet')):
        if path.name in current:
            continue
        try:
            size = path.stat().st_size
            path.unlink()
        except OSError as error:
            logger.warning(
                'Could not remove superseded export',
                file=path.name, error=str(error),
            )
            continue
        logger.info('Removed superseded export', file=path.name, bytes=size)


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
