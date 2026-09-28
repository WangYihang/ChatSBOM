/**
 * The browser's side of the query boundary.
 *
 * One `POST /api/q` per question, naming a method the Worker exposes.
 * No SQL leaves the page, and there is nothing here that could compose
 * any — the shapes below are the whole vocabulary.
 *
 * The method names are typed against the Worker's own return types, so
 * a rename on that side is a compile error here rather than a 400 at
 * runtime. `dataset/types.ts` is the single declaration of what a result
 * looks like, whichever store the Worker asks; this file only says how
 * to ask.
 */
import type {
  AdoptionPoint,
  DatasetMeta,
  DependencyBucket,
  DependencyTree,
  Dependent,
  DependentQuery,
  PackageEdge,
  EcosystemCoverage,
  EcosystemRelationship,
  LanguageCoverage,
  PackageMatch,
  PackagePopularity,
  EcosystemShare,
  EdgeAmbiguity,
  LicenseShare,
  RelationshipSplit,
  SourceComparison,
  Totals,
  VersionSpread,
} from '../dataset/types';

export class QueryError extends Error {}

/** One request, the callers waiting on it, and how to abandon it. */
interface Shared {
  answer: Promise<unknown>;
  abandon: AbortController;
  waiting: number;
}

/**
 * Calls the query endpoint.
 *
 * Failures arrive as rejections rather than as an error-shaped result,
 * because a UI that styles failures differently from answers needs them
 * separable — and every message the Worker returns is already written
 * for a reader.
 *
 * Every method takes an optional `signal` last: the caller's, for a
 * question it may stop wanting before it is answered (#42).
 */
export class DatasetClient {
  /**
   * Requests on their way, by the body they were sent with.
   *
   * The page asks some questions from two places at once — the
   * languages and the ecosystems for the root and the overview, the
   * totals for the header and the metadata panel — and each asked the
   * Worker separately, every visit (#42). A question already on its way
   * is joined rather than sent again. Only while it is on its way: no
   * answer is kept here, so a question asked later is asked again, and
   * whether that reaches the store is the Worker's cache's to decide.
   */
  private readonly inFlight = new Map<string, Shared>();

  constructor(private readonly endpoint = '/api/q') {}

  /**
   * One question: joined if it is already on its way, sent if not.
   *
   * A caller's signal ends that caller's wait. It ends the request only
   * once nobody is waiting on it: the root and the overview ask for the
   * ecosystems together, and one of them moving on must not cost the
   * other its answer.
   */
  private call<T>(
    method: string,
    params?: Record<string, unknown>,
    signal?: AbortSignal,
  ): Promise<T> {
    if (signal?.aborted) return Promise.reject(signal.reason);
    const body = JSON.stringify(params ? { method, params } : { method });
    const request = this.inFlight.get(body) ?? this.send(body);

    request.waiting += 1;
    if (!signal) return request.answer as Promise<T>;

    return new Promise<T>((resolve, reject) => {
      const giveUp = () => {
        request.waiting -= 1;
        if (request.waiting === 0) {
          request.abandon.abort();
          if (this.inFlight.get(body) === request) this.inFlight.delete(body);
        }
        reject(signal.reason);
      };
      signal.addEventListener('abort', giveUp, { once: true });
      request.answer.then(
        (value) => {
          signal.removeEventListener('abort', giveUp);
          resolve(value as T);
        },
        (error: unknown) => {
          signal.removeEventListener('abort', giveUp);
          reject(error);
        },
      );
    });
  }

  /** Send a question, and hold it where the next caller can join it. */
  private send(body: string): Shared {
    const abandon = new AbortController();
    const request: Shared = {
      answer: this.post(body, abandon.signal),
      abandon,
      waiting: 0,
    };
    // Settled, either way: the next asking is a question of its own.
    const done = () => {
      if (this.inFlight.get(body) === request) this.inFlight.delete(body);
    };
    request.answer.then(done, done);
    this.inFlight.set(body, request);
    return request;
  }

  private async post(body: string, signal: AbortSignal): Promise<unknown> {
    const response = await fetch(this.endpoint, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body,
      signal,
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
    return payload;
  }

  dependentsOf(query: DependentQuery, signal?: AbortSignal): Promise<Dependent[]> {
    return this.call('dependentsOf', { ...query }, signal);
  }

  countDependentRows(query: DependentQuery, signal?: AbortSignal): Promise<number> {
    return this.call('countDependentRows', { ...query }, signal);
  }

  countDependents(query: DependentQuery, signal?: AbortSignal): Promise<number> {
    return this.call('countDependents', { ...query }, signal);
  }

  relationshipSplit(
    ecosystem?: string,
    signal?: AbortSignal,
  ): Promise<RelationshipSplit> {
    return this.call('relationshipSplit', ecosystem ? { ecosystem } : {}, signal);
  }

  totals(signal?: AbortSignal): Promise<Totals> {
    return this.call('totals', undefined, signal);
  }

  languageCoverage(signal?: AbortSignal): Promise<LanguageCoverage[]> {
    return this.call('languageCoverage', undefined, signal);
  }

  ecosystemCoverage(signal?: AbortSignal): Promise<EcosystemCoverage[]> {
    return this.call('ecosystemCoverage', undefined, signal);
  }

  topPackages(
    options: {
      directOnly?: boolean;
      ecosystem?: string;
      limit?: number;
    },
    signal?: AbortSignal,
  ): Promise<PackagePopularity[]> {
    return this.call('topPackages', { ...options }, signal);
  }

  dependencyDistribution(signal?: AbortSignal): Promise<DependencyBucket[]> {
    return this.call('dependencyDistribution', undefined, signal);
  }

  sourceComparison(signal?: AbortSignal): Promise<SourceComparison[]> {
    return this.call('sourceComparison', undefined, signal);
  }

  searchPackages(
    term: string,
    limit?: number,
    signal?: AbortSignal,
  ): Promise<PackageMatch[]> {
    return this.call(
      'searchPackages',
      limit === undefined ? { term } : { term, limit },
      signal,
    );
  }

  licenseShares(limit?: number, signal?: AbortSignal): Promise<LicenseShare[]> {
    return this.call('licenseShares', limit === undefined ? {} : { limit }, signal);
  }

  adoptionOverTime(name: string, signal?: AbortSignal): Promise<AdoptionPoint[]> {
    return this.call('adoptionOverTime', { name }, signal);
  }

  versionSpread(
    name: string,
    limit?: number,
    signal?: AbortSignal,
  ): Promise<VersionSpread> {
    return this.call(
      'versionSpread',
      limit === undefined ? { name } : { name, limit },
      signal,
    );
  }

  edgeAmbiguity(signal?: AbortSignal): Promise<EdgeAmbiguity | null> {
    return this.call('edgeAmbiguity', undefined, signal);
  }

  relationshipByEcosystem(signal?: AbortSignal): Promise<EcosystemRelationship[]> {
    return this.call('relationshipByEcosystem', undefined, signal);
  }

  ecosystemsFor(name: string, signal?: AbortSignal): Promise<EcosystemShare[]> {
    return this.call('ecosystemsFor', { name }, signal);
  }

  dependenciesOf(
    name: string,
    limit?: number,
    signal?: AbortSignal,
  ): Promise<PackageEdge[]> {
    return this.call(
      'dependenciesOf',
      limit === undefined ? { name } : { name, limit },
      signal,
    );
  }

  pulledInBy(
    name: string,
    limit?: number,
    signal?: AbortSignal,
  ): Promise<PackageEdge[]> {
    return this.call(
      'pulledInBy',
      limit === undefined ? { name } : { name, limit },
      signal,
    );
  }

  dependencyTree(
    name: string,
    options: { children?: number; branch?: number } = {},
    signal?: AbortSignal,
  ): Promise<DependencyTree> {
    return this.call('dependencyTree', { name, ...options }, signal);
  }

  meta(signal?: AbortSignal): Promise<DatasetMeta> {
    return this.call('meta', undefined, signal);
  }
}
