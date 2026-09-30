/**
 * What the page asks the Python service for each call the contract
 * suite makes, kept for the service to be held to (#144).
 *
 * `fixtures/contract/calls.json` holds what D1 answered each call, and
 * the Python dataset API is held to those answers (#142). The page asks
 * that API through the service now, by URL (`src/d1/client.ts`): the
 * method by its name, each parameter by the page's name for it, which
 * the service reads as the method's own. So the URL the page's client
 * asks for each of those calls is written down, in
 * `fixtures/contract/urls.json`, and `tests/server_queries_test.py`
 * asks the service each one and expects D1's answer. A parameter the
 * page names otherwise than the service reads it fails there.
 *
 * Recorded, from `web/`, after a change to the client or to the calls:
 *
 *     CONTRACT_RECORD_URLS=1 npx vitest run test/contracturls.test.ts
 *
 * Without the variable nothing is written, and URLs other than the
 * file's fail here, naming that command.
 */
import { readFileSync, writeFileSync } from 'node:fs';

import { afterEach, expect, it, vi } from 'vitest';

import { DatasetClient } from '../src/d1/client';
import { type Call, layout, readCalls } from './contractcalls';

const ENV = (
  globalThis as unknown as { process: { env: Record<string, string | undefined> } }
).process.env;

/** Beside the calls they ask. */
const URLS = decodeURIComponent(
  new URL('./fixtures/contract/urls.json', import.meta.url).pathname,
);

/** What records the file again, as a failure names it. */
const RECORD = 'CONTRACT_RECORD_URLS=1 npx vitest run test/contracturls.test.ts';

/** The snapshot's id, as the file writes it: the service's test names its own. */
const SNAPSHOT = 'SNAPSHOT';

/** One recorded call, and the URL the page asks for it last. */
interface Asked {
  method: string;
  params: unknown[];
  url: string;
}

afterEach(() => vi.unstubAllGlobals());

/** The URL the page's client asks for `call`, answered as D1 answered it. */
async function ask(call: Call): Promise<string> {
  let asked = '';
  vi.stubGlobal('fetch', (url: string) => {
    asked = url;
    const answer =
      url === '/api/meta'
        ? { snapshot: SNAPSHOT, ...(call.method === 'meta' ? (call.returns as object) : {}) }
        : call.returns;
    return Promise.resolve(
      new Response(JSON.stringify(answer), {
        headers: { 'content-type': 'application/json' },
      }),
    );
  });
  const client = new DatasetClient();
  const method: unknown = Reflect.get(client, call.method);
  expect(typeof method).toBe('function');
  const answer: unknown = await Reflect.apply(
    method as (...params: unknown[]) => Promise<unknown>,
    client,
    call.params,
  );
  // What the page takes from the answer is the answer.
  expect(answer).toEqual(call.returns);
  return asked;
}

it('asks the service for each recorded call as the file says', async () => {
  const calls = JSON.parse(readCalls()) as Call[];
  const asked: Asked[] = [];
  for (const call of calls) {
    asked.push({ method: call.method, params: call.params, url: await ask(call) });
  }
  const text = layout(asked);
  if (ENV['CONTRACT_RECORD_URLS'] === '1') {
    writeFileSync(URLS, text);
    return;
  }
  let kept = '';
  try {
    kept = readFileSync(URLS, 'utf8');
  } catch {
    // Not recorded yet: said below, with how to record it.
  }
  expect(
    kept,
    `The page asks otherwise than fixtures/contract/urls.json says, which `
      + `the service is held to. Record it again with\n  ${RECORD}`,
  ).toBe(text);
});
