"""The export contract: one declaration, two consumers.

The Parquet files are read by a TypeScript dashboard, so the column names
and types cross a language boundary. Declaring them here — and generating
both the JSON manifest and the TypeScript types from this declaration —
means a renamed column breaks the dashboard's build instead of returning
`undefined` at runtime.

This is the same lesson as `core.table`: the bug lives at the seam, so the
seam is where the contract is enforced.
"""
import json
from dataclasses import dataclass
from dataclasses import field
from enum import Enum

from chatsbom.models.provenance import ARTIFACT_SOURCES
from chatsbom.models.provenance import VERSION_KINDS
from chatsbom.models.relationship import RELATIONSHIPS

SCHEMA_VERSION = '4'


class ColumnType(str, Enum):
    """Portable column types, mapped per target by the generators."""

    STRING = 'string'
    INTEGER = 'integer'
    DATE = 'date'
    STRING_LIST = 'string[]'

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class ExportColumn:
    name: str
    type: ColumnType
    description: str
    #: Closed set of values, when the column is an enum.
    enum: list[str] | None = None
    #: TypeScript type name to use instead of the structural mapping.
    ts_type: str | None = None

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            'name': self.name,
            'type': str(self.type),
            'description': self.description,
        }
        if self.enum is not None:
            payload['enum'] = list(self.enum)
        return payload


@dataclass(frozen=True, slots=True)
class ExportTable:
    name: str
    description: str
    primary_key: str
    columns: tuple[ExportColumn, ...]
    #: Column the rows are sorted by in the Parquet file, for row-group
    #: pruning on the client.
    sorted_by: tuple[str, ...] = ()

    def column(self, name: str) -> ExportColumn:
        for column in self.columns:
            if column.name == name:
                return column
        raise KeyError(f"{self.name} has no column {name!r}")

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def to_dict(self) -> dict[str, object]:
        return {
            'name': self.name,
            'description': self.description,
            'primaryKey': self.primary_key,
            'sortedBy': list(self.sorted_by),
            'file': f'{self.name}.parquet',
            'columns': [c.to_dict() for c in self.columns],
        }


@dataclass(frozen=True, slots=True)
class ExportSchema:
    version: str
    tables: tuple[ExportTable, ...] = field(default=())

    def table(self, name: str) -> ExportTable:
        for table in self.tables:
            if table.name == name:
                return table
        raise KeyError(f"no exported table named {name!r}")

    def to_dict(self) -> dict[str, object]:
        return {
            'version': self.version,
            'tables': [t.to_dict() for t in self.tables],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False) + '\n'


REPOSITORIES_TABLE = ExportTable(
    name='repositories',
    description='One row per analysed repository.',
    primary_key='id',
    sorted_by=('stars',),
    columns=(
        ExportColumn('id', ColumnType.INTEGER, 'GitHub repository id.'),
        ExportColumn('owner', ColumnType.STRING, 'Repository owner login.'),
        ExportColumn('repo', ColumnType.STRING, 'Repository name.'),
        ExportColumn(
            'stars', ColumnType.INTEGER,
            'Star count at collection time.',
        ),
        ExportColumn(
            'language', ColumnType.STRING,
            'Primary language, lowercased.',
        ),
        ExportColumn('url', ColumnType.STRING, 'Repository URL.'),
        ExportColumn(
            'description', ColumnType.STRING,
            'Repository description.',
        ),
        ExportColumn(
            'license_spdx_id', ColumnType.STRING,
            'SPDX licence id, or empty.',
        ),
        ExportColumn(
            'pushed_at', ColumnType.DATE,
            'Last push, as YYYY-MM-DD.',
        ),
        ExportColumn(
            'sbom_ref', ColumnType.STRING,
            'Tag or branch the SBOM was taken from.',
        ),
        ExportColumn(
            'sbom_commit_sha', ColumnType.STRING,
            'Full commit SHA the SBOM describes.',
        ),
        ExportColumn(
            'direct_dependencies', ColumnType.INTEGER,
            'Distinct packages the manifest declares.',
        ),
        ExportColumn(
            'total_dependencies', ColumnType.INTEGER,
            'Distinct packages in the resolved closure.',
        ),
        ExportColumn(
            'manifest_sources', ColumnType.STRING_LIST,
            'Manifest files that were read, relative to the repo root.',
        ),
    ),
)

ARTIFACTS_TABLE = ExportTable(
    name='artifacts',
    description='One row per (repository, package, version) in the SBOM.',
    primary_key='',
    sorted_by=('name', 'repository_id'),
    columns=(
        ExportColumn(
            'repository_id', ColumnType.INTEGER,
            'Joins to repositories.id.',
        ),
        ExportColumn(
            'name', ColumnType.STRING,
            'Package name as the ecosystem spells it.',
        ),
        ExportColumn(
            'version', ColumnType.STRING,
            'Resolved version, or empty.',
        ),
        ExportColumn(
            'type', ColumnType.STRING,
            'Package ecosystem, e.g. gem, npm, go-module.',
        ),
        ExportColumn(
            'found_by', ColumnType.STRING,
            'Detector that reported the package.',
        ),
        ExportColumn(
            'relationship', ColumnType.STRING,
            'Whether the repository declares this package itself, '
            'inherited it, or could not be determined.',
            enum=list(RELATIONSHIPS),
            ts_type='Relationship',
        ),
        ExportColumn(
            'source', ColumnType.STRING,
            'Which collector produced the row: syft (lockfile, resolved '
            'closure) or github-depgraph (manifest, declared only).',
            enum=list(ARTIFACT_SOURCES),
            ts_type='ArtifactSource',
        ),
        ExportColumn(
            'version_kind', ColumnType.STRING,
            'Whether the version is exact, a manifest constraint, or absent.',
            enum=list(VERSION_KINDS),
            ts_type='VersionKind',
        ),
    ),
)

LICENSES_TABLE = ExportTable(
    name='licenses',
    description='Package counts per SPDX licence, by ecosystem.',
    primary_key='',
    sorted_by=('repository_count',),
    columns=(
        ExportColumn(
            'license', ColumnType.STRING,
            'SPDX expression, or empty when unknown.',
        ),
        ExportColumn('type', ColumnType.STRING, 'Package ecosystem.'),
        ExportColumn(
            'package_count', ColumnType.INTEGER,
            'Distinct packages under this licence.',
        ),
        ExportColumn(
            'repository_count', ColumnType.INTEGER,
            'Repositories carrying one.',
        ),
    ),
)

HISTORY_TABLE = ExportTable(
    name='history',
    description=(
        'Monthly adoption per package: how many repositories used it, and '
        'how many declared it. The temporal series a snapshot cannot give.'
    ),
    primary_key='',
    sorted_by=('name', 'month'),
    columns=(
        ExportColumn('name', ColumnType.STRING, 'Package name.'),
        ExportColumn(
            'month', ColumnType.STRING,
            'Observation month, YYYY-MM.',
        ),
        ExportColumn(
            'repository_count', ColumnType.INTEGER,
            'Repositories using it that month.',
        ),
        ExportColumn(
            'direct_count', ColumnType.INTEGER,
            'Of those, how many declared it.',
        ),
    ),
)

EXPORT_SCHEMA = ExportSchema(
    version=SCHEMA_VERSION,
    tables=(
        REPOSITORIES_TABLE, ARTIFACTS_TABLE, LICENSES_TABLE, HISTORY_TABLE,
    ),
)
