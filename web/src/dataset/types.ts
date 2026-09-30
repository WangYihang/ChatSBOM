/**
 * What the dashboard's questions return.
 *
 * These describe the dataset, not a store. The service answers each
 * question from its dataset API (`chatsbom/dataset/`), whose answer
 * types are these, field for field: `tests/dataset_contract_test.py`
 * reads the interfaces here and holds the Python's to them.
 *
 * Types only. The browser's compile reads this file, so nothing here
 * may reach for a runtime.
 */
import type { Relationship } from '../schema';

export interface DependentQuery {
  name: string;
  /**
   * Ecosystem to scope to.
   *
   * A package name is not unique across ecosystems: `mail` is a Ruby
   * gem with 118 dependants and a Maven artifactId with 6. Counting
   * them together reports 124 dependants of something that does not
   * exist.
   *
   * Either collector's spelling names the same one: `php-composer` is
   * Composer, as `ecosystemsFor` would have called it.
   */
  type?: string;
  /**
   * The repository's GitHub language, folded as the coverage panel
   * folds it (#55 D7): one of the twelve with the most repositories,
   * lowercased, `other` or `none`. An attribute of the repository: the
   * package's ecosystem is `type`.
   */
  language?: string;
  directOnly?: boolean;
  limit?: number;
  /** Rows to skip, for paging. */
  offset?: number;
}

export interface Dependent {
  owner: string;
  repo: string;
  stars: number;
  version: string;
  url: string;
  relationship: Relationship;
  /**
   * When this row's source last observed the repository, as a UTC
   * date. Its own source's: a repository Syft scanned in February and
   * the dependency graph read in September has rows of each date.
   */
  observedAt: string;
  /**
   * Which registry, under the name the interface shows.
   *
   * A row without it cannot be told from a row about a different
   * package that happens to share the name — `mail` is a gem, a Maven
   * artifact and a PyPI package.
   */
  ecosystem: string;
  /** The repository's own language, not the package's. */
  language: string;
  /**
   * How many manifests in this repository declare it.
   *
   * GitHub's dependency graph reports per manifest, so one repository
   * can produce dozens of otherwise identical rows: searching
   * `requests` showed `affaan-m/everything-claude-code` four times,
   * distinguishable only by an opaque `SPDXRef-pypi-requests-4205b9`
   * the table does not display, and one repository declares it in 80.
   * Eighty of the hundred rows would have been one repository.
   *
   * So the rows are collapsed and the count is shown instead — the
   * fact is disclosed rather than repeated, and the table stops
   * disagreeing with the "3,156 dependants" above it, which counts
   * repositories.
   *
   * A snapshot keeps one row per dependency fact rather than per
   * manifest, so it counts facts: one, unless two cataloguers reported
   * the same version.
   */
  manifests: number;
}

export interface RelationshipSplit {
  direct: number;
  transitive: number;
  unknown: number;
}

export interface Totals {
  /** Repositories with dependency data, from any source. */
  repositories: number;
  dependencies: number;
  packages: number;
  classified: number;
  /**
   * Repositories in the current search snapshot, collected or not: the
   * denominator of every coverage ratio (#55 D2).
   */
  tracked: number;
}

/**
 * How an ecosystem's dependencies arrived.
 *
 * The headline says 83.6% of all records are inherited. This is the
 * same question asked per ecosystem, and the answer is not uniform:
 * npm declares a small share of what it holds and Cargo a large one,
 * which is the difference between a lockfile that resolves a deep
 * tree and one that does not. A single global figure hides that.
 *
 * Keyed by the package's ecosystem, not the repository's language: a
 * repository with a Maven backend under a TypeScript label contributes
 * to both npm and Maven. Records partition by ecosystem, so these add
 * up to the corpus's.
 */
export interface EcosystemRelationship {
  ecosystem: string;
  direct: number;
  transitive: number;
  unknown: number;
  records: number;
}

/**
 * Repositories per GitHub language, folded to the top twelve, `other`
 * and `none` (#55 D7), and how much of each the collectors cover.
 *
 * The denominator is every repository of the current snapshot,
 * collected or not.
 */
export interface LanguageCoverage {
  language: string;
  repositories: number;
  /** With dependency data from any source. */
  withSbom: number;
  withSyft: number;
  withDepgraph: number;
  withManifest: number;
}

/**
 * Per ecosystem: repositories whose artifacts or manifests are of it,
 * and how many of those each source covers. A repository counts under
 * every ecosystem it has, so the rows overlap and must not be summed.
 */
export interface EcosystemCoverage {
  ecosystem: string;
  repositories: number;
  withAny: number;
  withSyft: number;
  withDepgraph: number;
  withManifest: number;
}

export interface PackagePopularity {
  name: string;
  repositoryCount: number;
  directCount: number;
}

export interface DependencyBucket {
  label: string;
  repositories: number;
}

/**
 * One suggestion: a package name in one ecosystem.
 *
 * Keyed on the pair, not the name. 39,658 names live in more than one
 * ecosystem and `mail` is three — the Ruby gem with 167 dependants, a
 * Maven artifact with 6, a PyPI package with 1. Offering them as a
 * single row meant picking "mail" and then reaching for a separate
 * filter to say which; offering them separately makes the choice the
 * click.
 */
export interface PackageMatch {
  name: string;
  /**
   * Under the name the interface shows, not the collector's spelling.
   * Null when the store counts the name alone: a snapshot stores one
   * count per name, and splitting it would count the artifacts on
   * every keystroke. The row then stands for the name across all of
   * its ecosystems.
   */
  ecosystem: string | null;
  /** Repositories depending on it *in this ecosystem*. */
  repositoryCount: number;
  /** Repositories depending on the name in any ecosystem. */
  nameTotal: number;
}

/**
 * How far name-keyed edges are polluted by cross-ecosystem collisions.
 *
 * `edges` is keyed on package name and has no ecosystem column, so a
 * name used in two ecosystems merges their edges. The dashboard states
 * the scale of that as a caveat, and stating it from a measurement
 * pasted into the copy went stale: the sentence claimed 23.7% of edges
 * were affected long after the real figure had become 51.5%.
 */
export interface EdgeAmbiguity {
  names: number;
  ambiguousNames: number;
  edges: number;
  ambiguousEdges: number;
  /** Most distinct packages in any one repository. */
  largestRepository: number;
}

export interface LicenseShare {
  license: string;
  repositoryCount: number;
  packageCount: number;
}

/**
 * One month of one source's observations of one package.
 *
 * `source` is not decoration. Syft resolves a lockfile's closure and
 * GitHub's graph parses manifests, and the two ran seven months apart —
 * so a series that merged them drew a line from February's 124 to
 * September's 149 for `mail` and read as adoption growing, when the
 * only thing that changed was the instrument.
 */
export interface AdoptionPoint {
  source: string;
  month: string;
  repositoryCount: number;
  directCount: number;
}

export interface VersionShare {
  version: string;
  repositoryCount: number;
  /**
   * Whether this is a resolved version or a manifest constraint.
   *
   * GitHub's dependency graph reports both — 525,899 rows are
   * constraints like `>= 13.0,< 14.0` and 140,731 carry no version at
   * all, 3.4% of the corpus together. Counted alongside resolutions,
   * the constraint `>= 13.0,< 14.0` topped `laravel/framework`'s
   * "versions in use" with 11 repositories against the real leading
   * version's 7.
   */
  kind: string;
}

/**
 * What a package's version spread looks like, and what was set aside.
 *
 * The count of unresolved rows travels with the resolved ones so the
 * panel can say how much it is not showing. A query that filtered them
 * out silently would make the panel's denominator unknowable.
 */
export interface VersionSpread {
  versions: VersionShare[];
  /**
   * Repositories whose row carried a constraint, not a resolution:
   * every one of them, not only those a list would show, and each once
   * however many constraint strings it declares.
   */
  constrained: number;
  /** Repositories whose row carried no version at all. */
  unversioned: number;
}

export interface EcosystemShare {
  type: string;
  repositoryCount: number;
  directCount: number;
}

/**
 * One aggregated package-to-package edge, from the named end's
 * perspective: `name` is the *other* package, and `repositories` is how
 * many repositories show the pair together.
 *
 * Deliberately one shape for both directions. The two questions —
 * "what does X pull in" and "what pulls in X" — differ in which end is
 * fixed, not in what an answer looks like, and a second interface would
 * only mean two ways to render the same row.
 */
export interface PackageEdge {
  name: string;
  repositories: number;
}

/** One second-hop edge, naming the first-hop package it hangs from. */
export interface TreeEdge {
  parent: string;
  child: string;
  repositories: number;
}

/**
 * Two hops of the graph around one package, bounded on both.
 *
 * Bounded because the alternative is not a view. The largest repository
 * in this dataset had 5,388 distinct dependencies when this was
 * measured, and a full transitive
 * expansion of a popular package draws an image that is unreadable at
 * every zoom level. Two hops, the widest few edges per node, is a
 * diagram; the unbounded version is a hairball.
 *
 * `grandchildren` names its own parent rather than nesting, because the
 * same package legitimately appears under two parents — `depd` is
 * pulled in by both `http-errors` and `body-parser` — and a nested
 * shape would have to either duplicate it or pick one.
 */
export interface DependencyTree {
  root: string;
  /** First hop: what the root pulls in, widest first. */
  children: PackageEdge[];
  /** Second hop, each row naming the first-hop package it hangs from. */
  grandchildren: TreeEdge[];
}

export interface DatasetMeta {
  generator: string;
  schemaVersion: string;
  /**
   * The span of the data's age: the oldest and the newest of the
   * repositories' newest current observations, as UTC dates. History a
   * later scan replaced is not in it, in either store.
   */
  observedFrom: string;
  observedTo: string;
}

/** Dependency records per ecosystem, by the collector that made them. */
export interface SourceComparison {
  ecosystem: string;
  syft: number;
  depgraph: number;
  /** Declared in Gradle build files (#55 D1). */
  manifest: number;
}
