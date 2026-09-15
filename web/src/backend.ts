/**
 * What the dashboard needs answered, independent of what answers it.
 *
 * This is the seam for changing the store. It sits at the **method**
 * level deliberately, because the obvious alternative — an interface
 * with `query(sql, params)` — is not a seam at all. Two stores do not
 * want the same SQL in different dialects; they want different
 * strategies, and the measurements say so:
 *
 *     sourceComparison    D1 / SQLite    3,122 ms computed live
 *                                            3 ms from a precomputed table
 *                         ClickHouse        37 ms computed live, natively
 *
 *     dependentsOf        D1 / SQLite        4 ms, index lookup
 *                         ClickHouse       2.8 ms, sparse index,
 *                                          22,333 rows read of 6,062,896
 *
 * SQLite needs the overview precomputed into `agg_*` tables or it reads
 * six million rows per visitor. ClickHouse does not: its aggregates are
 * fast enough that those tables would be pure overhead, and keeping them
 * in sync would be work done for nothing. A SQL-level interface would
 * impose SQLite's compromise on a store that does not share the problem.
 *
 * So each implementation answers these questions however suits it. To
 * add a store, implement this and change one line in `d1/api.ts` where
 * the instance is constructed; nothing else, and in particular nothing
 * in the browser — the page has only ever sent method names.
 */
import type {
  AdoptionPoint,
  DatasetMeta,
  DependencyBucket,
  Dependent,
  DependentQuery,
  DependencyTree,
  EdgeAmbiguity,
  PackageEdge,
  EcosystemShare,
  LanguageCoverage,
  LanguageRelationship,
  LicenseShare,
  PackageMatch,
  PackagePopularity,
  RelationshipSplit,
  SourceComparison,
  Totals,
  VersionKindShare,
  VersionShare,
  VersionSpread,
} from './d1/queries';

/**
 * The questions.
 *
 * Result types live in `d1/queries.ts` for now because that is where
 * they were written; they describe the dataset rather than the store,
 * and a second implementation would import them from there unchanged.
 *
 * Note what is absent: no method takes SQL, and none returns rows to be
 * interpreted by the caller. If either appears, the seam has moved back
 * to the SQL level and a caller has started depending on one store's
 * dialect.
 */
export interface DatasetQueries {
  /* ---- point lookups: an arbitrary package name, so no precompute --- */

  dependentsOf(query: DependentQuery): Promise<Dependent[]>;
  countDependents(query: DependentQuery): Promise<number>;
  ecosystemsFor(name: string): Promise<EcosystemShare[]>;
  /**
   * Which resolved versions are in use, and how much was set aside.
   *
   * Returns the unresolved counts alongside, because a panel headed
   * "repositories on each resolved version" has to be able to say what
   * it excluded — 3.4% of the corpus carries a manifest constraint or
   * no version, and for some packages that is the top row.
   */
  versionSpread(name: string, limit?: number): Promise<VersionSpread>;
  adoptionOverTime(name: string): Promise<AdoptionPoint[]>;
  searchPackages(term: string, limit?: number): Promise<PackageMatch[]>;

  /* ---- the edge table: both directions, and a bounded tree --------- */

  /**
   * What a package pulls in, and what pulls it in.
   *
   * Both directions, because they are different questions and the
   * second is the more useful one: it is how a reader finds out why a
   * package they never chose is in their lockfile.
   */
  dependenciesOf(name: string, limit?: number): Promise<PackageEdge[]>;
  pulledInBy(name: string, limit?: number): Promise<PackageEdge[]>;

  /**
   * Two hops around one package, for the tree diagram.
   *
   * Bounds are the store's to enforce, not the caller's to remember: a
   * backend that returned every hop would hand the page a result it
   * cannot draw, and the page has no way to know that before it
   * arrives.
   */
  dependencyTree(
    name: string,
    options?: { children?: number; branch?: number },
  ): Promise<DependencyTree>;

  /* ---- the overview: fixed questions, finite answers ---------------- */

  totals(): Promise<Totals>;
  relationshipSplit(language?: string): Promise<RelationshipSplit>;
  languageCoverage(): Promise<LanguageCoverage[]>;
  /** The declared/inherited split per language, for all of them at once. */
  relationshipByLanguage(): Promise<LanguageRelationship[]>;
  /** Resolutions against ranges, across the corpus. */
  versionKindShares(): Promise<VersionKindShare[]>;
  /**
   * Null when the store cannot answer it. D1's `artifacts` is four
   * integers with no ecosystem column, so it genuinely cannot, and the
   * caveat drops its figures rather than inventing them.
   */
  edgeAmbiguity(): Promise<EdgeAmbiguity | null>;
  topPackages(options: {
    directOnly?: boolean;
    language?: string;
    limit?: number;
  }): Promise<PackagePopularity[]>;
  dependencyDistribution(): Promise<DependencyBucket[]>;
  sourceComparison(): Promise<SourceComparison[]>;
  licenseShares(limit?: number): Promise<LicenseShare[]>;

  /* ---- provenance --------------------------------------------------- */

  /**
   * Which build produced the data and how fresh it is.
   *
   * Every store must answer this, however it keeps it: D1 reads a `meta`
   * row written by the export, and a live store would report its own
   * `max(observed_at)` instead. The panel that shows it exists to
   * explain a surprising number, so a store that cannot say is a store
   * whose numbers cannot be checked.
   */
  meta(): Promise<DatasetMeta>;
}
