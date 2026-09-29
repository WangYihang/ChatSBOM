"""What the dashboard's questions answer, as the page reads it.

The port of `web/src/dataset/types.ts`, which says what each field means.
The page reads these as JSON, under the TypeScript names: a field is
named in Python here, `repository_count`, and goes over the wire as
`repositoryCount`, its alias. So the page cannot tell which service
answered it (#128 §2.5), and `tests/dataset_contract_test.py` holds each
type's fields to the TypeScript interface of the same name.

Answers, not arguments: they are made here, from rows the queries read,
and never parsed from a caller.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import TypeAdapter
from pydantic.alias_generators import to_camel

from chatsbom.models.relationship import Relationship


class Answer(BaseModel):
    """Named in snake_case here, and in camelCase where the page reads
    it; frozen, since an answer is not edited on the way out."""

    model_config = ConfigDict(
        alias_generator=to_camel, populate_by_name=True, frozen=True,
    )


#: Serialises whatever it is given by what it is: an answer, a list of
#: them, None or a count.
_ANY: TypeAdapter[Any] = TypeAdapter(Any)


def jsonable(answer: object) -> Any:
    """An answer as the page reads it: JSON's values, under the page's
    names. What a web route sends, or a chat tool hands the model."""
    return _ANY.dump_python(answer, mode='json', by_alias=True)


# The fields of each are in the order the TypeScript builds its answer,
# which is the order they go over the wire today.


class Dependent(Answer):
    owner: str
    repo: str
    stars: int
    version: str
    url: str
    #: The repository's own language, not the package's.
    language: str
    #: Which registry, under the name the interface shows.
    ecosystem: str
    relationship: Relationship
    #: When this row's own source last observed the repository, as a
    #: UTC date: not the newest of its sources (#24).
    observed_at: str
    #: The facts one row of the table collapses: D1's export keeps one
    #: row per dependency fact, so one unless two cataloguers reported
    #: the same version.
    manifests: int


class RelationshipSplit(Answer):
    direct: int
    transitive: int
    unknown: int


class Totals(Answer):
    #: Repositories with dependency data, from any source.
    repositories: int
    dependencies: int
    packages: int
    classified: int
    #: Every repository of the current search snapshot, collected or
    #: not: the denominator of every coverage ratio (#55 D2).
    tracked: int


class EcosystemRelationship(Answer):
    ecosystem: str
    direct: int
    transitive: int
    unknown: int
    records: int


class LanguageCoverage(Answer):
    language: str
    repositories: int
    with_sbom: int
    with_syft: int
    with_depgraph: int
    with_manifest: int


class EcosystemCoverage(Answer):
    ecosystem: str
    repositories: int
    with_any: int
    with_syft: int
    with_depgraph: int
    with_manifest: int


class PackagePopularity(Answer):
    name: str
    repository_count: int
    direct_count: int


class DependencyBucket(Answer):
    label: str
    repositories: int


class PackageMatch(Answer):
    name: str
    #: None when the store counts the name alone, as a snapshot of the
    #: D1 schema does: the row then stands for every ecosystem of it.
    ecosystem: str | None
    repository_count: int
    name_total: int


class EdgeAmbiguity(Answer):
    names: int
    ambiguous_names: int
    edges: int
    ambiguous_edges: int
    largest_repository: int


class LicenseShare(Answer):
    license: str
    repository_count: int
    package_count: int


class AdoptionPoint(Answer):
    #: Which collector's series: two instruments are not one line.
    source: str
    month: str
    repository_count: int
    direct_count: int


class VersionShare(Answer):
    #: A resolved version, a manifest constraint, or none.
    kind: str
    version: str
    repository_count: int


class VersionSpread(Answer):
    versions: list[VersionShare]
    #: Every repository whose row carried a constraint, each once.
    constrained: int
    #: Every repository whose row carried no version at all.
    unversioned: int


class EcosystemShare(Answer):
    type: str
    repository_count: int
    direct_count: int


class PackageEdge(Answer):
    #: The package at the other end from the one asked about.
    name: str
    repositories: int


class TreeEdge(Answer):
    parent: str
    child: str
    repositories: int


class DependencyTree(Answer):
    root: str
    #: What the root pulls in, widest first.
    children: list[PackageEdge]
    #: The hop after, each naming the child it hangs from.
    grandchildren: list[TreeEdge]


class DatasetMeta(Answer):
    generator: str
    schema_version: str
    #: The oldest and the newest of the repositories' newest current
    #: observations, as UTC dates.
    observed_from: str
    observed_to: str


class SourceComparison(Answer):
    ecosystem: str
    syft: int
    depgraph: int
    #: Declared in Gradle build files (#55 D1).
    manifest: int
