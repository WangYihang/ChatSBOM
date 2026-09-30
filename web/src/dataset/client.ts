/**
 * The browser's side of the query boundary.
 *
 * The page asks the Python service, `chatsbom web serve` (#144), which
 * answers from an immutable snapshot of the dataset. It asks `GET
 * /api/meta` which snapshot is current, once, and then each question as
 * a GET under it:
 *
 *     GET /api/v/<snapshot>/<method>?<parameters>
 *
 * the method by its name here, and each parameter by its name, written
 * as text, in the order of the names. So one question is one URL, and
 * its answer is that snapshot's for good: the browser and anything
 * between may keep it. A snapshot the service no longer serves answers
 * 410, and the page asks `meta` again and the question once more under
 * the snapshot it names. No SQL leaves the page, and there is nothing
 * here that could compose any — the shapes below are the whole
 * vocabulary.
 *
 * The service's dataset API (`chatsbom/dataset/`) answers each method
 * under the same name in snake_case, and nothing else:
 * `tests/dataset_contract_test.py` reads the methods from this class
 * and holds the two to each other. `types.ts` is the single declaration
 * of what an answer looks like; this file only says how to ask.
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

/**
 * A question the service refused, and the status it refused it with.
 *
 * The message is the service's own sentence, in English, or the page's
 * where it wrote none. The status is what the page says it by in the
 * reader's language (`i18n/failure.ts`, #43): the service is not told
 * which language that is.
 */
export class QueryError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

/** One request, the callers waiting on it, and how to abandon it. */
interface Shared {
  answer: Promise<unknown>;
  abandon: AbortController;
  waiting: number;
}

/**
 * A question as its URL writes it, after the snapshot: the method, and
 * each parameter given, by name, in the order of the names, as the text
 * of its value. A parameter left out is not written: the store's
 * default, not an empty value.
 */
function pathOf(method: string, params: Record<string, unknown> = {}): string {
  const query = new URLSearchParams();
  for (const name of Object.keys(params).sort()) {
    const value = params[name];
    if (value !== undefined && value !== null) query.append(name, String(value));
  }
  const written = query.toString();
  return written ? `${method}?${written}` : method;
}

/** A refusal, with the sentence it was said with, or the page's where there is none. */
async function refusal(response: Response, what: string): Promise<QueryError> {
  const payload: unknown = await response.json().catch(() => null);
  const message =
    payload &&
    typeof payload === 'object' &&
    'error' in payload &&
    typeof payload.error === 'string'
      ? payload.error
      : `${what} failed (${response.status}).`;
  return new QueryError(message, response.status);
}

/**
 * Calls the service.
 *
 * Failures arrive as rejections rather than as an error-shaped result,
 * because a UI that styles failures differently from answers needs them
 * separable — and every message the service returns is already written
 * for a reader, in English; the status says it in the reader's language.
 *
 * Every method takes an optional `signal` last: the caller's, for a
 * question it may stop wanting before it is answered (#42).
 */
export class DatasetClient {
  /**
   * Requests on their way, by the question they ask.
   *
   * The page asks some questions from two places at once — the
   * languages and the ecosystems for the root and the overview, the
   * totals for the header and the metadata panel — and each asked
   * separately, every visit (#42). A question already on its way is
   * joined rather than sent again. Only while it is on its way: no
   * answer is kept here, so a question asked later is asked again, and
   * whether that reaches the service is for the browser's cache to
   * decide, which keeps an answer under its URL.
   */
  private readonly inFlight = new Map<string, Shared>();

  /**
   * What `/api/meta` said, or is saying: asked once for every question,
   * and again when a question is told its snapshot is gone. A failure is
   * not kept: the next question asks again.
   */
  private told: Promise<DatasetMeta> | null = null;

  constructor(private readonly base = '/api') {}

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
    const path = pathOf(method, params);
    const request = this.inFlight.get(path) ?? this.send(path);

    request.waiting += 1;
    return this.wait(request.answer as Promise<T>, signal, () => {
      request.waiting -= 1;
      if (request.waiting === 0) {
        request.abandon.abort();
        if (this.inFlight.get(path) === request) this.inFlight.delete(path);
      }
    });
  }

  /** `answer`, or the caller giving up on it first, which `giveUp` is told of. */
  private wait<T>(
    answer: Promise<T>,
    signal: AbortSignal | undefined,
    giveUp: () => void = () => {},
  ): Promise<T> {
    if (!signal) return answer;
    if (signal.aborted) return Promise.reject(signal.reason);
    return new Promise<T>((resolve, reject) => {
      const abandon = () => {
        giveUp();
        reject(signal.reason);
      };
      signal.addEventListener('abort', abandon, { once: true });
      answer.then(
        (value) => {
          signal.removeEventListener('abort', abandon);
          resolve(value);
        },
        (error: unknown) => {
          signal.removeEventListener('abort', abandon);
          reject(error);
        },
      );
    });
  }

  /** Send a question, and hold it where the next caller can join it. */
  private send(path: string): Shared {
    const abandon = new AbortController();
    const request: Shared = {
      answer: this.ask(path, abandon.signal),
      abandon,
      waiting: 0,
    };
    // Settled, either way: the next asking is a question of its own.
    const done = () => {
      if (this.inFlight.get(path) === request) this.inFlight.delete(path);
    };
    request.answer.then(done, done);
    this.inFlight.set(path, request);
    return request;
  }

  /**
   * A question, under the snapshot `meta` named; and once more under
   * the one it names now, if the service no longer serves that one: a
   * pass has published since the page asked.
   */
  private async ask(path: string, signal: AbortSignal): Promise<unknown> {
    const told = this.tell();
    const { snapshot } = await told;
    try {
      return await this.get(this.under(snapshot, path), signal);
    } catch (error) {
      if (!(error instanceof QueryError && error.status === 410)) throw error;
      const { snapshot: current } = await this.tell(told);
      return this.get(this.under(current, path), signal);
    }
  }

  private under(snapshot: string, path: string): string {
    return `${this.base}/v/${encodeURIComponent(snapshot)}/${path}`;
  }

  /**
   * What `/api/meta` says: what it said already, unless that was
   * `stale`, a snapshot a question has been told is gone. Then it is
   * asked again, once for all the questions told so at once, and past
   * the copy the browser may keep for a minute, which would name the
   * snapshot that is gone.
   */
  private tell(stale?: Promise<DatasetMeta>): Promise<DatasetMeta> {
    if (this.told === null || (stale !== undefined && this.told === stale)) {
      const told = this.askMeta(stale !== undefined);
      this.told = told;
      told.catch(() => {
        if (this.told === told) this.told = null;
      });
    }
    return this.told;
  }

  private async askMeta(fresh: boolean): Promise<DatasetMeta> {
    const response = await fetch(`${this.base}/meta`, {
      headers: { accept: 'application/json' },
      ...(fresh ? { cache: 'no-cache' as const } : {}),
    });
    if (!response.ok) throw await refusal(response, 'Asking which snapshot to read');
    return (await response.json()) as DatasetMeta;
  }

  private async get(url: string, signal: AbortSignal): Promise<unknown> {
    // Given up on while `meta` was being asked: not sent at all.
    signal.throwIfAborted();
    const response = await fetch(url, {
      headers: { accept: 'application/json' },
      signal,
    });
    if (!response.ok) throw await refusal(response, 'The query');
    return response.json();
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

  /**
   * What `/api/meta` said: the snapshot the questions are asked of, and
   * its provenance. No question of its own. Once a question has found
   * its snapshot gone, it is what `meta` said after.
   */
  meta(signal?: AbortSignal): Promise<DatasetMeta> {
    return this.wait(
      this.tell().then(({ snapshot, generator, schemaVersion, observedFrom, observedTo }) => ({
        snapshot,
        generator,
        schemaVersion,
        observedFrom,
        observedTo,
      })),
      signal,
    );
  }
}
