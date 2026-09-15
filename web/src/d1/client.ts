/**
 * The browser's side of the query boundary.
 *
 * One `POST /api/q` per question, naming a method the Worker exposes.
 * No SQL leaves the page, and there is nothing here that could compose
 * any — the shapes below are the whole vocabulary.
 *
 * The method names are typed against the Worker's own return types, so
 * a rename on that side is a compile error here rather than a 400 at
 * runtime. `queries.ts` on the Worker is the single declaration of what
 * a result looks like; this file only says how to ask.
 */
import type {
  AdoptionPoint,
  DatasetMeta,
  DependencyBucket,
  DependencyTree,
  Dependent,
  DependentQuery,
  PackageEdge,
  LanguageCoverage,
  LanguageRelationship,
  PackageMatch,
  PackagePopularity,
  EcosystemShare,
  EdgeAmbiguity,
  LicenseShare,
  RelationshipSplit,
  SourceComparison,
  Totals,
  VersionKindShare,
  VersionSpread,
} from './queries';

export class QueryError extends Error {}

/**
 * Calls the query endpoint.
 *
 * Failures arrive as rejections rather than as an error-shaped result,
 * because a UI that styles failures differently from answers needs them
 * separable — and every message the Worker returns is already written
 * for a reader.
 */
export class DatasetClient {
  constructor(private readonly endpoint = '/api/q') {}

  private async call<T>(
    method: string,
    params?: Record<string, unknown>,
  ): Promise<T> {
    const response = await fetch(this.endpoint, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(params ? { method, params } : { method }),
    });

    const payload: unknown = await response.json().catch(() => null);
    if (!response.ok) {
      const message =
        payload &&
        typeof payload === 'object' &&
        'error' in payload &&
        typeof payload.error === 'string'
          ? payload.error
          : `The query failed (${response.status}).`;
      throw new QueryError(message);
    }
    return payload as T;
  }

  dependentsOf(query: DependentQuery): Promise<Dependent[]> {
    return this.call('dependentsOf', { ...query });
  }

  countDependentRows(query: DependentQuery): Promise<number> {
    return this.call('countDependentRows', { ...query });
  }

  countDependents(query: DependentQuery): Promise<number> {
    return this.call('countDependents', { ...query });
  }

  relationshipSplit(language?: string): Promise<RelationshipSplit> {
    return this.call('relationshipSplit', language ? { language } : {});
  }

  totals(): Promise<Totals> {
    return this.call('totals');
  }

  languageCoverage(): Promise<LanguageCoverage[]> {
    return this.call('languageCoverage');
  }

  topPackages(options: {
    directOnly?: boolean;
    language?: string;
    limit?: number;
  }): Promise<PackagePopularity[]> {
    return this.call('topPackages', { ...options });
  }

  dependencyDistribution(): Promise<DependencyBucket[]> {
    return this.call('dependencyDistribution');
  }

  sourceComparison(): Promise<SourceComparison[]> {
    return this.call('sourceComparison');
  }

  searchPackages(term: string, limit?: number): Promise<PackageMatch[]> {
    return this.call('searchPackages', limit === undefined ? { term } : { term, limit });
  }

  licenseShares(limit?: number): Promise<LicenseShare[]> {
    return this.call('licenseShares', limit === undefined ? {} : { limit });
  }

  adoptionOverTime(name: string): Promise<AdoptionPoint[]> {
    return this.call('adoptionOverTime', { name });
  }

  versionSpread(name: string, limit?: number): Promise<VersionSpread> {
    return this.call('versionSpread', limit === undefined ? { name } : { name, limit });
  }

  edgeAmbiguity(): Promise<EdgeAmbiguity | null> {
    return this.call('edgeAmbiguity');
  }

  relationshipByLanguage(): Promise<LanguageRelationship[]> {
    return this.call('relationshipByLanguage');
  }

  versionKindShares(): Promise<VersionKindShare[]> {
    return this.call('versionKindShares');
  }

  ecosystemsFor(name: string): Promise<EcosystemShare[]> {
    return this.call('ecosystemsFor', { name });
  }

  dependenciesOf(name: string, limit?: number): Promise<PackageEdge[]> {
    return this.call(
      'dependenciesOf',
      limit === undefined ? { name } : { name, limit },
    );
  }

  pulledInBy(name: string, limit?: number): Promise<PackageEdge[]> {
    return this.call(
      'pulledInBy',
      limit === undefined ? { name } : { name, limit },
    );
  }

  dependencyTree(
    name: string,
    options: { children?: number; branch?: number } = {},
  ): Promise<DependencyTree> {
    return this.call('dependencyTree', { name, ...options });
  }

  meta(): Promise<DatasetMeta> {
    return this.call('meta');
  }
}
