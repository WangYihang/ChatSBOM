/**
 * The browser client.
 *
 * Its job is narrow — name a method, pass parameters, surface failures —
 * and the tests here are about exactly that narrowness. In particular
 * that it sends no SQL, because the whole point of the boundary is that
 * the page cannot express any.
 *
 * And, since the page asks the Python service (#144), which snapshot
 * it asks: `GET /api/meta` once, then each question as a GET under the
 * snapshot it named, and `meta` again when that snapshot is gone.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';

import { DatasetClient, QueryError } from '../src/d1/client';

afterEach(() => vi.unstubAllGlobals());

const SNAPSHOT = '0123456789abcdef';
const NEXT = 'fedcba9876543210';

const PROVENANCE = {
  generator: 'chatsbom/0.5.4',
  schemaVersion: 'd1 v8',
  observedFrom: '2026-02-11',
  observedTo: '2026-09-14',
};

/** One request the client made: where, and how. */
interface Sent {
  url: string;
  init: RequestInit | undefined;
}

const json = (payload: unknown, status = 200) =>
  new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });

/**
 * The service: `/api/meta` naming each of `snapshots` in turn, the last
 * of them from then on, and every question answered by `reply`.
 */
function stubService(
  reply: (url: string) => Response | Promise<Response> = () => json([]),
  snapshots: string[] = [SNAPSHOT],
) {
  const sent: Sent[] = [];
  vi.stubGlobal('fetch', (url: string, init?: RequestInit) => {
    sent.push({ url, init });
    if (url === '/api/meta') {
      const snapshot = snapshots.length > 1 ? snapshots.shift()! : snapshots[0]!;
      return Promise.resolve(json({ snapshot, ...PROVENANCE }));
    }
    return Promise.resolve(reply(url));
  });
  return sent;
}

/** The questions sent, after the snapshot: `totals`, `dependentsOf?name=mail`. */
const questions = (sent: Sent[], snapshot = SNAPSHOT) =>
  sent
    .map(({ url }) => url)
    .filter((url) => url.startsWith(`/api/v/${snapshot}/`))
    .map((url) => url.slice(`/api/v/${snapshot}/`.length));

describe('DatasetClient', () => {
  it('asks which snapshot is current, then the question under it', async () => {
    const sent = stubService();
    await new DatasetClient().dependentsOf({ name: 'mail', directOnly: true });
    expect(sent.map(({ url }) => url)).toEqual([
      '/api/meta',
      `/api/v/${SNAPSHOT}/dependentsOf?directOnly=true&name=mail`,
    ]);
    // GETs, which anything between may keep.
    expect(sent.every(({ init }) => (init?.method ?? 'GET') === 'GET')).toBe(true);
    expect(sent.every(({ init }) => init?.body === undefined)).toBe(true);
  });

  it('asks which is current once, for every question after', async () => {
    const sent = stubService();
    const client = new DatasetClient();
    await Promise.all([client.totals(), client.languageCoverage()]);
    await client.sourceComparison();
    expect(sent.filter(({ url }) => url === '/api/meta')).toHaveLength(1);
    expect(questions(sent)).toEqual(['totals', 'languageCoverage', 'sourceComparison']);
  });

  it('writes each parameter as text, by name, in the order of the names', async () => {
    // One URL for one question, however its parameters were put
    // together: an answer is kept under its URL.
    const sent = stubService();
    const client = new DatasetClient();
    await client.dependentsOf({ offset: 100, name: 'laravel/framework', limit: 100, type: 'composer' });
    await client.dependentsOf({ type: 'composer', limit: 100, name: 'laravel/framework', offset: 100 });
    await client.topPackages({ directOnly: false, limit: 20 });
    await client.searchPackages('a b%', 8);
    expect(questions(sent)).toEqual([
      'dependentsOf?limit=100&name=laravel%2Fframework&offset=100&type=composer',
      'dependentsOf?limit=100&name=laravel%2Fframework&offset=100&type=composer',
      'topPackages?directOnly=false&limit=20',
      'searchPackages?limit=8&term=a+b%25',
    ]);
  });

  it('omits a parameter left out rather than sending an empty one', async () => {
    const sent = stubService();
    const client = new DatasetClient();
    await client.relationshipSplit();
    await client.licenseShares();
    await client.dependencyTree('ms');
    await client.versionSpread('mail', undefined);
    expect(questions(sent)).toEqual([
      'relationshipSplit',
      'licenseShares',
      'dependencyTree?name=ms',
      'versionSpread?name=mail',
    ]);
  });

  it('sends no SQL, for any method', async () => {
    const sent = stubService();
    const client = new DatasetClient();
    await client.dependentsOf({ name: 'mail' });
    await client.topPackages({ directOnly: true });
    await client.relationshipSplit('Ruby');
    await client.totals();
    await client.meta();

    const wire = JSON.stringify(sent);
    for (const word of ['SELECT', 'FROM', 'JOIN', 'WHERE', 'artifacts']) {
      expect(wire).not.toContain(word);
    }
  });

  it('returns the parsed result', async () => {
    stubService(() =>
      json({ repositories: 28075, dependencies: 6062896, packages: 141938, classified: 6053469 }),
    );
    const totals = await new DatasetClient().totals();
    expect(totals.repositories).toBe(28075);
  });

  it('says the provenance from what `meta` said, without asking again', async () => {
    const sent = stubService();
    const client = new DatasetClient();
    expect(await client.meta()).toEqual(PROVENANCE);
    await client.totals();
    expect(sent.map(({ url }) => url)).toEqual(['/api/meta', `/api/v/${SNAPSHOT}/totals`]);
  });

  it('rejects with the message the service wrote', async () => {
    stubService(() => json({ error: 'Unknown method: nope' }, 404));
    const failed = new DatasetClient().totals();
    await expect(failed).rejects.toThrow('Unknown method: nope');
    await expect(failed).rejects.toMatchObject({ status: 404 });
  });

  it('rejects with a readable message when the body is not JSON', async () => {
    stubService(() => new Response('<html>bad gateway</html>', { status: 502 }));
    await expect(new DatasetClient().totals()).rejects.toThrow(/502/);
  });

  it('rejects rather than returning an error-shaped result', async () => {
    stubService(() => json({ error: 'nope' }, 500));
    // A UI that styles failures differently needs them separable from
    // answers; returning `{error}` as a value makes every caller check.
    await expect(new DatasetClient().totals()).rejects.toBeInstanceOf(QueryError);
  });

  it('rejects a question when `meta` is refused, with what it said', async () => {
    const said = 'No dataset is configured on this deployment.';
    vi.stubGlobal('fetch', () => Promise.resolve(json({ error: said }, 503)));
    const client = new DatasetClient();
    await expect(client.totals()).rejects.toMatchObject({ status: 503, message: said });
    await expect(client.meta()).rejects.toMatchObject({ status: 503, message: said });
  });

  it('asks `meta` again after it failed, rather than keeping the failure', async () => {
    let refuse = true;
    const sent: string[] = [];
    vi.stubGlobal('fetch', (url: string) => {
      sent.push(url);
      if (url === '/api/meta') {
        return Promise.resolve(
          refuse
            ? json({ error: 'The dataset cannot be read for a moment.' }, 503)
            : json({ snapshot: SNAPSHOT, ...PROVENANCE }),
        );
      }
      return Promise.resolve(json({ tracked: 12 }));
    });
    const client = new DatasetClient();
    await expect(client.totals()).rejects.toMatchObject({ status: 503 });
    refuse = false;
    expect(await client.totals()).toEqual({ tracked: 12 });
    expect(sent).toEqual(['/api/meta', '/api/meta', `/api/v/${SNAPSHOT}/totals`]);
  });
});

/**
 * A snapshot the service no longer serves (#144): a pass published two
 * more since the page asked `meta`. It answers 410, and the page asks
 * `meta` again and the question once more under the snapshot it names.
 */
describe('DatasetClient, told its snapshot is gone', () => {
  const GONE = { error: 'This snapshot of the dataset is no longer served. Reload the page.' };

  /** Questions under SNAPSHOT refused 410, and under NEXT answered. */
  const retired = (url: string) =>
    url.startsWith(`/api/v/${SNAPSHOT}/`) ? json(GONE, 410) : json({ tracked: 3 });

  it('asks `meta` again, past any copy kept, and the question once more', async () => {
    const sent = stubService(retired, [SNAPSHOT, NEXT]);
    const client = new DatasetClient();
    expect(await client.totals()).toEqual({ tracked: 3 });
    expect(sent.map(({ url }) => url)).toEqual([
      '/api/meta',
      `/api/v/${SNAPSHOT}/totals`,
      '/api/meta',
      `/api/v/${NEXT}/totals`,
    ]);
    // The browser may keep `meta` a minute: the second asking goes to
    // the service, or it would name the snapshot that is gone.
    expect(sent[0]!.init?.cache).toBeUndefined();
    expect(sent[2]!.init?.cache).toBe('no-cache');
  });

  it('asks every question after under the snapshot it was told of', async () => {
    const sent = stubService(retired, [SNAPSHOT, NEXT]);
    const client = new DatasetClient();
    await client.totals();
    await client.languageCoverage();
    expect(questions(sent, NEXT)).toEqual(['totals', 'languageCoverage']);
    expect(sent.filter(({ url }) => url === '/api/meta')).toHaveLength(2);
  });

  it('asks `meta` again once for the questions told at once', async () => {
    const sent = stubService(retired, [SNAPSHOT, NEXT]);
    const client = new DatasetClient();
    await Promise.all([client.totals(), client.languageCoverage(), client.ecosystemCoverage()]);
    expect(sent.filter(({ url }) => url === '/api/meta')).toHaveLength(2);
    expect(questions(sent, NEXT).sort()).toEqual(['ecosystemCoverage', 'languageCoverage', 'totals']);
  });

  it('tries once more, not again: a second 410 is the caller’s', async () => {
    // `meta` naming the same snapshot again, as a service mid-publish
    // might: the page is not to go round and round.
    const sent = stubService(() => json(GONE, 410), [SNAPSHOT]);
    const failed = new DatasetClient().totals();
    await expect(failed).rejects.toBeInstanceOf(QueryError);
    await expect(failed).rejects.toMatchObject({ status: 410, message: GONE.error });
    expect(sent.map(({ url }) => url)).toEqual([
      '/api/meta',
      `/api/v/${SNAPSHOT}/totals`,
      '/api/meta',
      `/api/v/${SNAPSHOT}/totals`,
    ]);
  });

  it('does not ask again for any other refusal', async () => {
    const sent = stubService(() => json({ error: 'Too many queries. Wait a moment.' }, 429));
    await expect(new DatasetClient().totals()).rejects.toMatchObject({ status: 429 });
    expect(sent.map(({ url }) => url)).toEqual(['/api/meta', `/api/v/${SNAPSHOT}/totals`]);
  });
});

/**
 * The same question asked by two parts of the page at once (#42): the
 * root and the overview both asked for the ecosystems and the
 * languages, and the header and the metadata panel both for the totals,
 * on every visit.
 */
describe('DatasetClient, asked twice at once', () => {
  /** The service, answering each question when told to, with its signal kept. */
  function held() {
    const requests: { url: string; signal: AbortSignal | undefined; answer(): void }[] = [];
    vi.stubGlobal('fetch', (url: string, options?: RequestInit) => {
      if (url === '/api/meta') {
        return Promise.resolve(json({ snapshot: SNAPSHOT, ...PROVENANCE }));
      }
      return new Promise((resolve, reject) => {
        const signal = options?.signal ?? undefined;
        signal?.addEventListener('abort', () =>
          reject(new DOMException('The operation was aborted.', 'AbortError')),
        );
        requests.push({
          url: url.slice(`/api/v/${SNAPSHOT}/`.length),
          signal,
          answer: () => resolve(json({ repositories: 3 })),
        });
      });
    });
    return requests;
  }

  /** Let the client's asking of `meta` settle, and its questions go out. */
  const sent = () => new Promise((resolve) => setTimeout(resolve, 0));

  it('sends one request for the same question asked at once', async () => {
    const requests = held();
    const client = new DatasetClient();
    const first = client.totals();
    const second = client.totals();
    await sent();
    expect(requests).toHaveLength(1);
    requests[0]!.answer();
    expect(await first).toEqual({ repositories: 3 });
    expect(await second).toEqual({ repositories: 3 });
  });

  it('asks again once the first has been answered: it shares a request, it keeps no answers', async () => {
    const requests = held();
    const client = new DatasetClient();
    const first = client.totals();
    await sent();
    requests[0]!.answer();
    await first;
    const again = client.totals();
    await sent();
    expect(requests).toHaveLength(2);
    requests[1]!.answer();
    await again;
  });

  it('keeps different questions apart, parameters included', async () => {
    const requests = held();
    const client = new DatasetClient();
    void client.totals();
    void client.languageCoverage();
    void client.topPackages({ directOnly: true, limit: 20 });
    void client.topPackages({ directOnly: false, limit: 20 });
    await sent();
    expect(requests.map(({ url }) => url)).toEqual([
      'totals',
      'languageCoverage',
      'topPackages?directOnly=true&limit=20',
      'topPackages?directOnly=false&limit=20',
    ]);
  });

  it('abandons the request when the one caller waiting on it gives up', async () => {
    const requests = held();
    const client = new DatasetClient();
    const giveUp = new AbortController();
    const asked = client.totals(giveUp.signal);
    await sent();
    giveUp.abort();
    await expect(asked).rejects.toMatchObject({ name: 'AbortError' });
    expect(requests[0]!.signal?.aborted).toBe(true);
  });

  it('sends nothing for a question given up before `meta` answered', async () => {
    const requests = held();
    const client = new DatasetClient();
    const giveUp = new AbortController();
    const asked = client.totals(giveUp.signal);
    giveUp.abort();
    await expect(asked).rejects.toMatchObject({ name: 'AbortError' });
    await sent();
    expect(requests).toEqual([]);
  });

  it('keeps the request while another caller still waits on it', async () => {
    const requests = held();
    const client = new DatasetClient();
    const giveUp = new AbortController();
    const abandoned = client.totals(giveUp.signal);
    const kept = client.totals();
    await sent();
    giveUp.abort();
    await expect(abandoned).rejects.toMatchObject({ name: 'AbortError' });
    expect(requests[0]!.signal?.aborted).toBe(false);
    requests[0]!.answer();
    expect(await kept).toEqual({ repositories: 3 });
  });
});

describe('DatasetClient edge questions', () => {
  it('names the direction it is asking about', async () => {
    const sent = stubService();
    const client = new DatasetClient();
    await client.pulledInBy('ms', 15);
    await client.dependenciesOf('body-parser');
    // No limit given means the store's default, not a limit of
    // undefined written into the request.
    expect(questions(sent)).toEqual(['pulledInBy?limit=15&name=ms', 'dependenciesOf?name=body-parser']);
  });

  it('passes the tree bounds through, and omits the ones not set', async () => {
    const sent = stubService(() => json({ root: 'ms', children: [], grandchildren: [] }));
    const client = new DatasetClient();
    await client.dependencyTree('ms', { children: 12, branch: 3 });
    await client.dependencyTree('ms');
    expect(questions(sent)).toEqual(['dependencyTree?branch=3&children=12&name=ms', 'dependencyTree?name=ms']);
  });

  it('sends no SQL for the edge questions either', async () => {
    const sent = stubService();
    const client = new DatasetClient();
    await client.pulledInBy('ms');
    await client.dependencyTree('ms', { branch: 3 });
    const wire = JSON.stringify(sent);
    for (const word of ['SELECT', 'agg_edges', 'parent_id', 'ROW_NUMBER']) {
      expect(wire).not.toContain(word);
    }
  });
});
