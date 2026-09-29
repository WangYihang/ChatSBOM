"""The questions answered by reading one stored table.

The port of `web/src/dataset/reads.ts`: eight of the dashboard's
questions are one precomputed table each, read and never recomputed.
The overview's panels measured 3,122 ms and 1,082 ms aggregated live on
D1, reading every one of six million artifact rows, and the export
precomputes them into `agg_*` tables so a visitor does not pay that.

Each statement is the one the TypeScript writes for D1, placeholders and
all, and names each column as the answer names its field, as the page
spells it: `shape_read` makes the answer from those names. Values are
bound, never spliced in; the only text a statement is built from is this
file's.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Generic

from chatsbom.dataset.shape import AnswerT
from chatsbom.dataset.types import AdoptionPoint
from chatsbom.dataset.types import DependencyBucket
from chatsbom.dataset.types import EcosystemCoverage
from chatsbom.dataset.types import LanguageCoverage
from chatsbom.dataset.types import LicenseShare
from chatsbom.dataset.types import PackagePopularity
from chatsbom.dataset.types import SourceComparison
from chatsbom.dataset.types import Totals


@dataclass(frozen=True)
class Read(Generic[AnswerT]):
    """One stored table, read: the statement, and what a row becomes."""

    sql: str
    answer: type[AnswerT]


TOTALS = Read(
    'SELECT repositories, dependencies, packages, classified, tracked '
    'FROM agg_totals',
    Totals,
)

LANGUAGE_COVERAGE = Read(
    'SELECT language, repositories, with_sbom AS withSbom, '
    'with_syft AS withSyft, with_depgraph AS withDepgraph, '
    'with_manifest AS withManifest '
    'FROM agg_language_coverage '
    'ORDER BY repositories DESC, language',
    LanguageCoverage,
)

ECOSYSTEM_COVERAGE = Read(
    'SELECT ecosystem, repositories, with_any AS withAny, '
    'with_syft AS withSyft, with_depgraph AS withDepgraph, '
    'with_manifest AS withManifest '
    'FROM agg_ecosystem_coverage '
    'ORDER BY repositories DESC, ecosystem',
    EcosystemCoverage,
)

#: A precomputed rank window, bound with the flag, the ecosystem and the
#: depth: the panel has exactly two controls, so the answers are finite
#: and were enumerated when the table was written. The empty ecosystem
#: is the whole corpus, counting each repository once however many
#: ecosystems it has.
TOP_PACKAGES = Read(
    'SELECT name, repository_count AS repositoryCount, '
    'direct_count AS directCount '
    'FROM agg_top_packages '
    'WHERE direct_only = ? AND ecosystem = ? AND rank <= ? '
    'ORDER BY rank',
    PackagePopularity,
)

#: Ordered by the stored position: the labels are not ordinal, so
#: sorting by them would put '1000+' between '10-24' and '100-249'.
DEPENDENCY_DISTRIBUTION = Read(
    'SELECT bucket AS label, repositories '
    'FROM agg_dependency_buckets '
    'ORDER BY position',
    DependencyBucket,
)

#: The empty ecosystem is a record with no type, not an ecosystem; the
#: export never writes one.
SOURCE_COMPARISON = Read(
    'SELECT ecosystem, syft, depgraph, manifest '
    'FROM agg_source_comparison '
    "WHERE ecosystem <> '' "
    'ORDER BY syft + depgraph + manifest DESC, ecosystem',
    SourceComparison,
)

#: Unknown is a row like any other. "We do not know" is a finding about
#: SBOM quality, and filtering it out would overstate coverage. Tied
#: licences by name, so which of two makes the cut is settled. Bound
#: with the depth.
LICENSE_SHARES = Read(
    'SELECT license, repository_count AS repositoryCount, '
    'package_count AS packageCount '
    'FROM licenses '
    'ORDER BY repositoryCount DESC, license '
    'LIMIT ?',
    LicenseShare,
)

#: The monthly series for one package, per source: every observation,
#: history included, which is what a series over time is for. Bound with
#: the name.
ADOPTION_OVER_TIME = Read(
    'SELECT source, month, repository_count AS repositoryCount, '
    'direct_count AS directCount '
    'FROM history '
    'WHERE name = ? '
    'ORDER BY source, month',
    AdoptionPoint,
)
