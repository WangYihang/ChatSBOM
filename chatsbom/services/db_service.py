import json
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import structlog

from chatsbom.core.config import get_config
from chatsbom.core.manifest import DirectDependencies
from chatsbom.core.manifest import resolve_relationships
from chatsbom.core.manifest import UNKNOWN
from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import RELEASES
from chatsbom.core.schema import REPOSITORIES
from chatsbom.core.stats import BaseStats
from chatsbom.core.table import Table
from chatsbom.models.framework import Framework
from chatsbom.models.framework import FrameworkFactory
from chatsbom.models.language import Language
from chatsbom.models.language import LanguageFactory
from chatsbom.models.provenance import classify_version
from chatsbom.models.provenance import SYFT
from chatsbom.models.query import DatabaseStats
from chatsbom.models.query import Dependent
from chatsbom.models.query import LanguageCount
from chatsbom.models.query import LibraryCandidate
from chatsbom.models.query import PackagePopularity
from chatsbom.models.repository import Repository
from chatsbom.services.dependency_graph_service import load_artifacts

logger = structlog.get_logger('db_service')

BATCH_SIZE = 1000
DEFAULT_DATE = datetime(1970, 1, 2, tzinfo=timezone.utc)


#: Asset fields worth keeping, of the sixteen GitHub returns.
#:
#: `release_assets` is written and never read — no query, no rollup, no
#: panel touches it — and it was the largest column in the database:
#: 5.00 GiB uncompressed against about 90 MiB for every other column in
#: `releases` combined, and 9.7 GiB of the ledgers on disk. A single
#: asset averaged 1,555 bytes, of which `uploader` was a complete
#: GitHub user object.
#:
#: Kept rather than dropped entirely, because the column being unread
#: today is not evidence nobody will ask: "which releases ship a
#: binary, how large, and does it carry a checksum" is a reasonable
#: question of a supply-chain dataset, and this project has twice paid
#: for discarding what it had not yet needed.
#:
#: `digest` is on 11.2% of assets and is the checksum, so it stays even
#: though most rows lack it. Measured: 1,555 -> 301 bytes, 81% smaller,
#: which takes the column from 5.00 GiB to about 0.97 GiB.
ASSET_FIELDS: frozenset[str] = frozenset({
    'name',
    'content_type',
    'size',
    'download_count',
    'browser_download_url',
    'created_at',
    'digest',
})


def _trimmed_assets(assets: object) -> list[dict[str, object]]:
    """Release assets, carrying only the fields worth storing.

    Anything that is not a list of mappings is returned as an empty
    list rather than raised on: this runs inside an ingest over 28,000
    repositories, and one oddly-shaped release is not a reason to lose
    the rest.
    """
    if not isinstance(assets, list):
        return []
    trimmed = []
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        trimmed.append({
            key: value for key, value in asset.items()
            if key in ASSET_FIELDS
        })
    return trimmed


@dataclass(frozen=True, slots=True)
class FrameworkUsage:
    """How widely one framework is used within a language."""

    framework: Framework
    repository_count: int
    direct_count: int
    samples: list[Dependent]


@dataclass(frozen=True, slots=True)
class FrameworkStats:
    """Framework usage for one language."""

    language: Language
    frameworks: list[FrameworkUsage]


@dataclass
class DbStats(BaseStats):
    repos: int = 0
    artifacts: int = 0
    releases: int = 0


class Batch:
    """Accumulates column-keyed records and flushes them in fixed chunks."""

    def __init__(self, table: Table, repo_db: IngestionRepository, size: int = BATCH_SIZE):
        self.table = table
        self.repo_db = repo_db
        self.size = size
        self._pending: list[Mapping[str, Any]] = []

    def add(self, record: Mapping[str, Any]) -> None:
        self._pending.append(record)
        if len(self._pending) >= self.size:
            self.flush()

    def extend(self, records: Sequence[Mapping[str, Any]]) -> None:
        for record in records:
            self.add(record)

    def flush(self) -> None:
        if not self._pending:
            return
        started = datetime.now()
        rows = self.table.rows(self._pending)
        self.repo_db.insert_batch(
            self.table.name, rows, self.table.column_names,
        )
        logger.info(
            'Batch Inserted',
            table=self.table.name,
            count=len(rows),
            elapsed=f"{(datetime.now() - started).total_seconds():.3f}s",
        )
        self._pending = []


def _naive(value: datetime | None) -> datetime:
    """ClickHouse DateTime columns take naive datetimes."""
    return (value or DEFAULT_DATE).replace(tzinfo=None)


def _stated_creation(path: Path) -> str | None:
    """`creationInfo.created` from a stored SPDX document, if present.

    GitHub writes it — `2026-09-14T03:56:20Z`, alongside
    `Tool: GitHub.com-Dependency-Graph` — and it is the graph's own view
    of when it was produced, which beats any timestamp this side of the
    wire. Read cheaply and forgivingly: a document that cannot be parsed
    here is still ingested by `load_artifacts`, so a failure must not
    raise, only fall through to the mtime.
    """
    try:
        with path.open(encoding='utf-8') as handle:
            document = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    sbom = document.get('sbom', document)
    if not isinstance(sbom, dict):
        return None
    info = sbom.get('creationInfo')
    if not isinstance(info, dict):
        return None
    created = info.get('created')
    return created if isinstance(created, str) else None


def _observed_from_document(path: Path, stated: str | None = None) -> datetime:
    """When the document was collected, not when it was indexed.

    `observed_at` defaulted to `now()`, which records the *ingest*. That
    reads as the collection date everywhere downstream — the export
    comments it as "when *we* last looked", the dashboard column is
    headed "SCANNED" — and a `db index --rebuild` reset all 19,361,638
    rows to the moment it ran. Measured: the syft documents were
    collected 2026-02-11 and the dependency graphs 2026-09-14, and the
    table claimed 2026-09-14 for every row. Six million of those were
    seven months old.

    Two sources of truth, in order of authority:

    - what the document says. GitHub's SPDX carries
      `creationInfo.created`, which is the graph's own timestamp;
    - the file's mtime. Syft's output carries no timestamp at all — its
      `descriptor` names the tool and version and nothing else — so for
      those this is all there is.

    Never `now()`: a rebuild must not change when something was
    observed.
    """
    if stated:
        try:
            # SPDX writes RFC 3339 with a literal Z, which
            # fromisoformat accepts only from 3.11.
            return _naive(datetime.fromisoformat(stated.replace('Z', '+00:00')))
        except ValueError:
            logger.warning(
                'Unparsable creation timestamp, falling back to mtime',
                path=str(path), stated=stated,
            )
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).replace(
            tzinfo=None,
        )
    except OSError:
        return _naive(None)


class DbService:
    """Service for ingesting repository data and SBOMs into ClickHouse."""

    def __init__(self):
        self.config = get_config()

    # -- ingestion ----------------------------------------------------------

    def ingest_from_list(
        self,
        input_file: Path,
        repo_db: IngestionRepository,
        progress_callback: Callable[[], None] | None = None,
        limit: int | None = None,
        depgraph_index: Path | None = None,
        metadata_index: Path | None = None,
    ) -> DbStats:
        """Ingest repositories, releases and SBOMs from a JSONL ledger.

        `input_file` decides *which* repositories are ingested — it is the
        SBOM ledger, and the complete list. `depgraph_index` only supplies
        extra documents for the repositories it happens to cover.

        Keeping those separate matters: the depgraph ledger was once used
        as the input list on the assumption it was a superset, and
        `github depgraph --limit 120` turned it into a subset that
        silently cut Java from 1,215 indexed repositories to 87.
        """
        stats = DbStats()

        if not input_file.exists():
            logger.warning(f"Input file not found: {input_file}")
            return stats

        depgraphs = self._depgraph_paths(depgraph_index)
        fresher = self._fresh_metadata(metadata_index)

        repos = Batch(REPOSITORIES, repo_db)
        artifacts = Batch(ARTIFACTS, repo_db)
        releases = Batch(RELEASES, repo_db)

        for data in self._read_records(input_file, limit):
            try:
                # The SBOM ledger carries the repository metadata as it
                # was when the SBOM was generated. `github repo` can
                # refresh that in place, and without this the refresh
                # would be invisible: `db index` reads only this file,
                # so stars and `pushed_at` would stay at the value they
                # had months ago.
                #
                # Overlaid rather than replaced, because the ledger also
                # carries the paths this stage needs — `sbom_path`,
                # `local_content_path` — which the metadata file does
                # not have.
                repository_id = data.get('id')
                update = (
                    fresher.get(repository_id)
                    if isinstance(repository_id, int)
                    else None
                )
                if update:
                    data = {**data, **update}
                repo = Repository.model_validate(data)
                direct_deps = self._direct_dependencies(repo)
                repo_row = self.parse_repository(repo, direct_deps)
                release_rows = self.parse_releases(repo)

                artifact_rows: list[dict[str, Any]] = []

                sbom_path = data.get('sbom_path')
                if sbom_path:
                    artifact_rows += self.parse_artifacts(
                        Path(sbom_path), repo.id, repo_row,
                        direct_deps=direct_deps,
                    )
                else:
                    stats.inc_skipped()

                # A second, independent source: GitHub's dependency graph
                # covers the Maven and Composer projects Syft cannot read.
                depgraph_path = data.get(
                    'depgraph_path',
                ) or depgraphs.get(repo.id)
                if depgraph_path:
                    artifact_rows += self.parse_dependency_graph(
                        Path(depgraph_path), repo.id, repo_row,
                    )

                repos.add(repo_row)
                releases.extend(release_rows)
                artifacts.extend(artifact_rows)

                stats.repos += 1
                stats.releases += len(release_rows)
                stats.artifacts += len(artifact_rows)
            except Exception as e:
                logger.error('Failed to process record', error=str(e))
                stats.inc_failed()

            if progress_callback:
                progress_callback()

        for batch in (repos, artifacts, releases):
            batch.flush()

        return stats

    @staticmethod
    def _fresh_metadata(index: Path | None) -> dict[int, dict[str, Any]]:
        """repository id -> newer metadata, for fields that go stale.

        Only the fields that change on their own. A blanket merge would
        also overwrite `sbom_path` and `sbom_commit_sha`, which describe
        *this* SBOM and must keep pointing at the commit that was
        actually scanned — a fresher `pushed_at` beside a stale
        `sbom_commit_sha` is the truth, and the panel says so.
        """
        if index is None or not index.exists():
            return {}

        wanted = (
            'stars', 'pushed_at', 'description', 'license_spdx_id',
            'license_name', 'topics', 'is_archived', 'is_fork',
            'fork_count', 'watchers_count', 'disk_usage',
            'default_branch', 'has_releases', 'total_releases',
            'latest_release_tag', 'latest_release_published_at',
            'vulnerability_alerts_count',
        )
        fresh: dict[int, dict[str, Any]] = {}
        with index.open(encoding='utf-8') as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                repository_id = record.get('id')
                if not isinstance(repository_id, int):
                    continue
                fresh[repository_id] = {
                    key: record[key] for key in wanted if key in record
                }
        logger.info('Fresh metadata loaded', repositories=len(fresh))
        return fresh

    @staticmethod
    def _depgraph_paths(index: Path | None) -> dict[int, str]:
        """repository id -> stored dependency-graph document.

        Absent or partial is normal: the graph is collected separately and
        covers whatever it has reached.
        """
        if index is None or not index.exists():
            return {}

        paths: dict[int, str] = {}
        with open(index, encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                    path = record.get('depgraph_path')
                    if path:
                        paths[int(record['id'])] = str(path)
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    continue

        if paths:
            logger.info('Dependency graphs available', count=len(paths))
        return paths

    @staticmethod
    def _direct_dependencies(repo: Repository) -> DirectDependencies | None:
        """Declared dependencies of a repo, or None when undeterminable.

        Needs both the downloaded content and a language we have a
        manifest parser for; without either, artifacts stay `unknown`.
        """
        if not repo.local_content_path or not repo.language:
            return None
        try:
            language = Language(repo.language.lower())
        except ValueError:
            return None
        try:
            return resolve_relationships(Path(repo.local_content_path), language)
        except ValueError:
            return None

    @staticmethod
    def _read_records(input_file: Path, limit: int | None) -> Iterator[dict]:
        with open(input_file, encoding='utf-8') as f:
            seen = 0
            for line in f:
                if not line.strip():
                    continue
                if limit is not None and seen >= limit:
                    return
                seen += 1
                yield json.loads(line)

    # -- parsing ------------------------------------------------------------

    def parse_repository(
        self,
        repo: Repository,
        direct_deps: DirectDependencies | None = None,
    ) -> dict[str, Any]:
        """Project a Repository into a `repositories` row mapping.

        `direct_deps` carries which manifests were read, which is the
        audit trail behind every direct/transitive verdict: without it a
        `transitive` label is indistinguishable from `unknown`.
        """
        target = repo.download_target
        release = repo.latest_stable_release

        return {
            'id': repo.id,
            'owner': repo.owner,
            'repo': repo.repo,
            'url': repo.url or '',
            'stars': repo.stars,
            'description': repo.description or '',
            'created_at': _naive(repo.created_at),
            'language': repo.language or '',
            'topics': repo.topics,
            'default_branch': repo.default_branch,
            'sbom_ref': target.ref if target else '',
            'sbom_ref_type': target.ref_type if target else '',
            'sbom_commit_sha': target.commit_sha if target else '',
            'sbom_commit_sha_short': target.commit_sha_short if target else '',
            'has_releases': bool(repo.has_releases),
            'latest_release_tag': release.tag_name if release else '',
            'latest_release_published_at': _naive(
                release.published_at if release else None,
            ),
            'total_releases': repo.total_releases,
            'pushed_at': _naive(repo.pushed_at),
            'is_archived': repo.is_archived,
            'is_fork': repo.is_fork,
            'is_template': repo.is_template,
            'is_mirror': repo.is_mirror,
            'disk_usage': repo.disk_usage,
            'fork_count': repo.fork_count,
            'watchers_count': repo.watchers_count,
            'license_spdx_id': repo.license_spdx_id or '',
            'license_name': repo.license_name or '',
            'manifest_sources': list(direct_deps.sources) if direct_deps else [],
        }

    def parse_releases(self, repo: Repository) -> list[dict[str, Any]]:
        """Project a Repository's releases into `releases` row mappings."""
        return [
            {
                'repository_id': repo.id,
                'release_id': r.id,
                'tag_name': r.tag_name,
                'name': r.name or '',
                'is_prerelease': r.is_prerelease,
                'is_draft': r.is_draft,
                'published_at': _naive(r.published_at),
                'target_commitish': r.target_commitish or '',
                'created_at': _naive(r.created_at),
                'release_assets': json.dumps(_trimmed_assets(r.assets)),
                'source': r.source,
            }
            for r in (repo.all_releases or [])
        ]

    def parse_artifacts(
        self,
        sbom_path: Path,
        repo_id: int,
        repo_row: Mapping[str, Any],
        direct_deps: DirectDependencies | None = None,
        observed_at: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Project a Syft SBOM into `artifacts` row mappings.

        SBOM provenance is carried over from the repository row by column
        name, so the artifact and its repository always agree on which
        commit was scanned.
        """
        if not sbom_path.exists():
            return []

        try:
            with open(sbom_path, encoding='utf-8') as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            raise ValueError(f"unreadable sbom {sbom_path}: {e}") from e

        sbom_ref = repo_row['sbom_ref']
        sbom_commit_sha = repo_row['sbom_commit_sha']
        seen_at = (
            _naive(observed_at) if observed_at
            else _observed_from_document(sbom_path)
        )

        return [
            {
                'repository_id': repo_id,
                'artifact_id': art.get('id', ''),
                'name': art.get('name', ''),
                'version': art.get('version', ''),
                'type': art.get('type', ''),
                'purl': art.get('purl', ''),
                'found_by': art.get('foundBy', ''),
                'licenses': _licenses(art.get('licenses', [])),
                'relationship': (
                    direct_deps.relationship_of(art.get('name') or '')
                    if direct_deps else UNKNOWN
                ),
                'source': SYFT,
                'version_kind': classify_version(art.get('version'))[1],
                'sbom_ref': sbom_ref,
                'sbom_commit_sha': sbom_commit_sha,
                'observed_at': seen_at,
            }
            for art in data.get('artifacts', [])
        ]

    def parse_dependency_graph(
        self,
        path: Path,
        repo_id: int,
        repo_row: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        """Project a stored GitHub dependency-graph document into rows.

        These land in the same table as Syft's output, distinguished by
        `source`. Their `relationship` is always `direct` — GitHub's graph
        is flat — and their versions are classified, since the graph
        reports manifest constraints rather than resolutions.
        """
        if not path.exists():
            return []

        seen_at = _observed_from_document(path, _stated_creation(path))

        return [
            {
                'repository_id': repo_id,
                'sbom_ref': repo_row['sbom_ref'],
                'sbom_commit_sha': repo_row['sbom_commit_sha'],
                'observed_at': seen_at,
                **row,
            }
            for row in load_artifacts(path)
        ]

    # -- queries ------------------------------------------------------------

    def get_db_stats(self, query_repo: QueryRepository) -> DatabaseStats:
        return query_repo.get_stats()

    def get_language_stats(
        self,
        query_repo: QueryRepository,
    ) -> list[LanguageCount]:
        return query_repo.get_language_stats()

    def get_top_packages(
        self,
        query_repo: QueryRepository,
        limit: int = 20,
        language: str | None = None,
    ) -> list[PackagePopularity]:
        return query_repo.get_top_packages(limit=limit, language=language)

    def get_framework_stats(
        self,
        query_repo: QueryRepository,
    ) -> list[FrameworkStats]:
        results: list[FrameworkStats] = []
        for lang in Language:
            try:
                handler = LanguageFactory.get_handler(lang)
            except ValueError:
                continue

            frameworks = handler.get_frameworks()
            if not frameworks:
                continue

            usage = [
                self._framework_usage(query_repo, lang, fw)
                for fw in frameworks
            ]
            results.append(FrameworkStats(language=lang, frameworks=usage))
        return results

    @staticmethod
    def _framework_usage(
        query_repo: QueryRepository,
        language: Language,
        framework: Framework,
    ) -> FrameworkUsage:
        packages = FrameworkFactory.create(framework).get_package_names()
        return FrameworkUsage(
            framework=framework,
            repository_count=query_repo.get_framework_usage(
                str(language), packages,
            ),
            direct_count=query_repo.get_framework_usage(
                str(language), packages, direct_only=True,
            ),
            samples=query_repo.get_top_projects_by_framework(
                str(language), packages, limit=3,
            ),
        )

    def search_library(
        self,
        query_repo: QueryRepository,
        component: str,
        language: str | None = None,
        limit: int = 10,
    ) -> list[LibraryCandidate]:
        return query_repo.search_library_candidates(
            component, language=language, limit=max(limit, 20),
        )

    def get_library_dependents(
        self,
        query_repo: QueryRepository,
        library_name: str,
        language: str | None = None,
        limit: int = 50,
        direct_only: bool = False,
    ) -> list[Dependent]:
        return query_repo.get_dependents(
            library_name, language=language, limit=limit,
            direct_only=direct_only,
        )


def _licenses(raw: list[Any]) -> list[str]:
    """Flatten Syft's several license shapes into SPDX-ish strings."""
    out = []
    for lic in raw:
        if isinstance(lic, dict):
            value = lic.get('value') or lic.get(
                'spdxExpression',
            ) or lic.get('name')
            if value:
                out.append(value)
        elif isinstance(lic, str):
            out.append(lic)
    return out
