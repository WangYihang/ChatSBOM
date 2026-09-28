"""Write the dataset as Parquet, plus a manifest describing it.

The whole dependency graph compresses to tens of megabytes: a copy of
the dataset that DuckDB or pandas reads directly, worth attaching to a
release. The dashboard read these files in the browser once; it asks its
Worker now, which answers from ClickHouse or D1, and nothing serves
them.

Columns are declared in `chatsbom.export.schema` and asserted against on
the way out, so the Parquet layout and the generated TypeScript types
cannot disagree.

Each table is streamed from ClickHouse as Arrow record batches and
written a row group at a time, so what the export holds is a row group,
not a table. It held the table: every row as Python objects in per-column
lists, about 241 bytes a row before Arrow copied it — 3.8 GiB for 16.8
million artifact rows.
"""
import contextlib
import hashlib
import json
import re
from collections.abc import Iterable
from collections.abc import Iterator
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any
from typing import TYPE_CHECKING

import structlog

from chatsbom.__version__ import __version__
from chatsbom.core.extras import install_command
from chatsbom.core.fs import temporary_beside
from chatsbom.core.repository import QueryRepository
from chatsbom.export.queries import EXPORT_SETTINGS
from chatsbom.export.queries import QUERIES
from chatsbom.export.queries import repository_freshness
from chatsbom.export.queries import whole
from chatsbom.export.schema import ColumnType
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.export.schema import ExportSchema
from chatsbom.export.schema import ExportTable

if TYPE_CHECKING:  # pragma: no cover - import cost only matters at runtime
    import pyarrow as pa

logger = structlog.get_logger('export_parquet')

MANIFEST_NAME = 'manifest.json'
ROW_GROUP_SIZE = 200_000

#: Hex digits of a file's SHA-256 in its name (`content_addressed_name`).
DIGEST_PREFIX = 8


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
    #: The file each table was written to, by table name.
    files: dict[str, str] = field(default_factory=dict)

    @property
    def total_bytes(self) -> int:
        return sum(self.sizes.values())


def _require_pyarrow() -> Any:
    try:
        import pyarrow  # noqa: F401
        import pyarrow.parquet as pq
    except ModuleNotFoundError as e:
        # For a caller other than `export parquet`, which checks first.
        raise RuntimeError(
            'Parquet export needs pyarrow, which comes with the `export` '
            f"extra: {install_command('export')}.",
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


def _conform(
    batches: Iterable['pa.RecordBatch'],
    table: ExportTable,
) -> Iterator['pa.RecordBatch']:
    """Each batch with the declared columns, in declared order, and of
    the declared types.

    A batch must carry the declared columns and nothing else. One the
    schema does not declare was dropped here without a word:
    `history`'s query returns `source` and the schema left it out, so
    the file mixed Syft's series with the dependency graph's. Either
    mismatch means the query and the contract disagree, and the reader
    of the file would be the first to find out.

    Cast, because ClickHouse's Arrow is its own types: `UInt64` arrives
    unsigned and every column not null, where the contract declares
    signed and nullable. The cast is checked, so a value that does not
    fit fails the export rather than wrapping.
    """
    schema = _arrow_schema(table)
    declared = table.column_names
    for batch in batches:
        names = batch.schema.names
        # One comparison per batch; the names only on a mismatch.
        if sorted(names) != sorted(declared):
            missing = [name for name in declared if name not in names]
            undeclared = [name for name in names if name not in declared]
            problems = []
            if missing:
                problems.append(f"is missing column(s) {', '.join(missing)}")
            if undeclared:
                problems.append(
                    f"returns column(s) {', '.join(undeclared)} the "
                    f"schema does not declare",
                )
            raise KeyError(
                f"{table.name} query {' and '.join(problems)}",
            )
        yield batch.select(declared).cast(schema)


def _write_rows(
    batches: Iterable['pa.RecordBatch'],
    path: Path,
    schema: 'pa.Schema',
) -> int:
    """Write `batches` to `path` as they arrive, and return the rows.

    Held until a row group is full, then written and let go, so at most
    a row group and a batch are in memory. The groups are
    `ROW_GROUP_SIZE` rows, as when the whole table was written at once,
    and each is made contiguous before it is written: the pages come
    out as they did then, so a table that has not changed keeps its
    bytes and its content-addressed name.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    def contiguous(pending: list['pa.RecordBatch']) -> 'pa.Table':
        return pa.Table.from_batches(pending, schema=schema).combine_chunks()

    rows = 0
    pending: list[pa.RecordBatch] = []
    held = 0
    # zstd with dictionary encoding is what makes the payload small
    # enough to ship to a browser; the row group size bounds how much
    # a client must fetch to answer a point lookup.
    with pq.ParquetWriter(
        path,
        schema,
        compression='zstd',
        compression_level=9,
        use_dictionary=True,
        write_statistics=True,
        store_schema=True,
    ) as writer:
        for batch in batches:
            pending.append(batch)
            held += batch.num_rows
            while held >= ROW_GROUP_SIZE:
                group = contiguous(pending)
                writer.write_table(
                    group.slice(0, ROW_GROUP_SIZE),
                    row_group_size=ROW_GROUP_SIZE,
                )
                rest = group.slice(ROW_GROUP_SIZE)
                pending, held = rest.to_batches(), rest.num_rows
                rows += ROW_GROUP_SIZE
        # The last, short group; and for a table with no rows, the one
        # empty group the whole-table write gave it.
        if held or not rows:
            writer.write_table(
                contiguous(pending), row_group_size=ROW_GROUP_SIZE,
            )
            rows += held
    return rows


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

    directory.mkdir(parents=True, exist_ok=True)

    row_counts: dict[str, int] = {}
    checksums: dict[str, str] = {}
    sizes: dict[str, int] = {}
    freshness: dict[str, str] = {}
    files: dict[str, str] = {}

    for table in schema.tables:
        # Each query runs once. The export ran it twice, the first time
        # as a `count()` to catch a cap that truncates without an error;
        # `EXPORT_SETTINGS` make such a cap fail the query instead, and
        # `whole` says which table it stopped.
        batches = _conform(
            whole(
                table.name,
                query_repo.stream_arrow(
                    QUERIES[table.name], settings=EXPORT_SETTINGS,
                ),
            ),
            table,
        )

        # Named after its own content, which can only be known once it
        # is written — so write, hash, then rename. `immutable` is a lie
        # on a fixed filename: the URL would be reused by the next
        # export while clients kept the old bytes for a year.
        #
        # Written aside under a dotted temporary name. It was written as
        # `<table>.parquet`, in a directory a person chose, and anything
        # they kept under that name was overwritten and renamed away.
        temporary = temporary_beside(directory / f'{table.name}.parquet')
        try:
            rows = _write_rows(batches, temporary, _arrow_schema(table))
            digest = _sha256(temporary)
            addressed = content_addressed_name(
                f'{table.name}.parquet', digest,
            )
            temporary.replace(directory / addressed)
        except BaseException:
            with contextlib.suppress(OSError):
                temporary.unlink()
            raise
        path = directory / addressed

        row_counts[table.name] = rows
        checksums[addressed] = digest
        sizes[addressed] = path.stat().st_size
        files[table.name] = addressed

        # Freshness comes from the repositories' observation dates, read
        # off the file that was just written rather than queried again —
        # the two could disagree if collection landed between them. Two
        # columns of one row per repository, so reading them back is
        # small whatever the size of the dataset.
        if table.name == 'repositories':
            freshness = repository_freshness(
                pq.read_table(
                    path, columns=['observed_at', 'total_dependencies'],
                ).to_pylist(),
            )

        logger.info(
            'Exported table',
            table=table.name,
            file=addressed,
            rows=rows,
            bytes=sizes[addressed],
        )

    result = ExportResult(
        directory=directory,
        row_counts=row_counts,
        checksums=checksums,
        sizes=sizes,
        freshness=freshness,
        files=files,
    )
    _write_manifest(directory, schema, result)
    _remove_superseded(
        directory, set(sizes), [table.name for table in schema.tables],
    )
    return result


def _remove_superseded(
    directory: Path,
    current: set[str],
    tables: Iterable[str],
) -> None:
    """Delete the Parquet files of earlier exports.

    Names are content-addressed, so a changed table lands under a new
    name and the old file is simply left behind. Measured after a
    re-export: `dist/data` held both `artifacts-5d2cc120.parquet` and
    `artifacts-283b3ee0.parquet`, 49 MB of superseded data that a
    deploy would upload and keep reachable at a live, `immutable` URL.

    `test_no_unaddressed_parquet_is_left_behind` names exactly this
    risk — "an upload ships both and the stale URL stays reachable" —
    and could not catch it, because it exports once into an empty
    directory.

    Only names an export writes, `<table>-<digest>.parquet` for one of
    `tables`, and of those only the ones absent from the manifest just
    written. It was every `*.parquet` in the directory, and the
    directory is one a person chose: a user's `my-own-analysis.parquet`
    was deleted as a previous run's.
    """
    ours = addressed_names(tables)
    for path in sorted(directory.glob('*.parquet')):
        if path.name in current or not ours.fullmatch(path.name):
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
    return f'{stem}-{checksum[:DIGEST_PREFIX]}.{extension}'


def addressed_names(tables: Iterable[str]) -> re.Pattern[str]:
    """Every name `content_addressed_name` gives these tables' files.

    What an export may delete as its own (`_remove_superseded`), so it
    is the writer's naming and nothing looser.
    """
    names = '|'.join(re.escape(table) for table in tables)
    return re.compile(rf'(?:{names})-[0-9a-f]{{{DIGEST_PREFIX}}}\.parquet')


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
        # With the file each table landed in, which only this export
        # can name.
        'schema': schema.to_dict(files=result.files),
    }
    path = directory / MANIFEST_NAME
    path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + '\n',
        encoding='utf-8',
    )
