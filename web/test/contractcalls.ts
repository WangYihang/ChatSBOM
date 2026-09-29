/**
 * What D1 answers each call the contract suite makes, kept for the
 * Python dataset API to be held to (#138).
 *
 * `contract.test.ts` asks D1 and ClickHouse the same questions and
 * expects one answer. The Python port of those questions,
 * `chatsbom/dataset/`, reads the schema D1 reads, and the page is not to
 * notice which of them answers it; but pytest runs no TypeScript. So
 * the suite's calls of D1 and what D1 answered each are written down,
 * in `fixtures/contract/calls.json`, and `tests/dataset_contract_test.py`
 * asks the Python every one of them.
 *
 * Recorded rather than written by hand, so that what the Python is held
 * to is what D1 said. After a change to a D1 statement, to `d1.sql` or
 * to the calls the suite makes, record again, from `web/`:
 *
 *     CONTRACT_RECORD_CALLS=1 npx vitest run test/contract.test.ts
 *
 * which writes the file afresh from the suite's calls: run the whole
 * file, since a call it did not make is not written. Without the
 * variable nothing is written, and a call of the suite that is not in
 * the file, answered as D1 answers it now, fails and names that
 * command; `contractcalls.test.ts` asks D1 every call in the file
 * again. Neither side can drift without one of them failing.
 */
import { readFileSync, writeFileSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';

import type { DatasetQueries } from '../src/backend';
import { D1Dataset } from '../src/d1/queries';
import d1Script from './fixtures/contract/d1.sql?raw';

/** Beside the export the calls were asked of. */
const CALLS = decodeURIComponent(
  new URL('./fixtures/contract/calls.json', import.meta.url).pathname,
);

/** What records the file again, as a failure names it. */
const RECORD = 'CONTRACT_RECORD_CALLS=1 npx vitest run test/contract.test.ts';

/** One call of a `DatasetQueries` method, and what D1 answered. */
export interface Call {
  method: string;
  /** Its parameters, in order, as the suite passed them. */
  params: unknown[];
  returns: unknown;
}

/** SQLite, holding the export exactly as `wrangler d1 execute` applies it. */
export function d1(): D1Dataset {
  const database = new DatabaseSync(':memory:');
  database.exec(d1Script);
  return new D1Dataset({
    all: async <T>(sql: string, params: unknown[] = []) =>
      database.prepare(sql).all(...params) as T[],
  });
}

/** The file as it stands. */
export function readCalls(): string {
  return readFileSync(CALLS, 'utf8');
}

/** A call as a person would write it. */
export function label(call: Pick<Call, 'method' | 'params'>): string {
  const params = call.params.map((param) => JSON.stringify(param));
  return `${call.method}(${params.join(', ')})`;
}

/** Object keys in order, at every depth. */
function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value !== null && typeof value === 'object') {
    const object = value as Record<string, unknown>;
    return Object.fromEntries(
      Object.keys(object)
        .sort()
        .map((key) => [key, sortKeys(object[key])]),
    );
  }
  return value;
}

/**
 * A value as JSON carries it, its keys in order: what went over the
 * wire, which is what the Python has to match, and not the object that
 * held it.
 */
function plain(value: unknown): unknown {
  return sortKeys(JSON.parse(JSON.stringify(value ?? null)));
}

/**
 * One key per call, whatever order its parameters' keys were written
 * in: `{ name, limit }` and `{ limit, name }` are the one call.
 */
function keyOf(call: Pick<Call, 'method' | 'params'>): string {
  return JSON.stringify([call.method, plain(call.params)]);
}

/**
 * The file's text: each call once, ordered by its method and
 * parameters, and laid out as the repository's JSON hook
 * (`pretty-format-json --indent=4`) lays JSON out, keys sorted and
 * anything past ASCII escaped. So recording again changes the file only
 * where an answer changed, and the hook has nothing to change in it.
 */
export function formatCalls(calls: Iterable<Call>): string {
  const byKey = new Map<string, Call>();
  for (const call of calls) byKey.set(keyOf(call), call);
  const ordered = [...byKey.entries()]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([, call]) => call);
  const text = JSON.stringify(plain(ordered), null, 4).replace(
    /[\u007f-￿]/g,
    (unit) => `\\u${unit.charCodeAt(0).toString(16).padStart(4, '0')}`,
  );
  return `${text}\n`;
}

/**
 * A store whose every call is recorded, or checked against the file.
 *
 * Recording, each call and its answer is kept, and `finish` writes the
 * file. Otherwise each call has to be in the file already, answered as
 * the store answers it now, or the call fails and says how to record it.
 */
export class CallRecorder {
  private readonly heard = new Map<string, Call>();
  private kept: Map<string, Call> | undefined;

  constructor(private readonly recording: boolean) {}

  wrap(dataset: DatasetQueries): DatasetQueries {
    return new Proxy(dataset, {
      get: (target, property) => {
        const value: unknown = Reflect.get(target, property);
        if (typeof property !== 'string' || typeof value !== 'function') {
          return value;
        }
        return async (...params: unknown[]) => {
          // Applied to the store itself, not to this wrapper, so that
          // what it asks of its own methods — the tree's first hop is
          // `dependenciesOf` — is not taken for a call of the suite's.
          const returns: unknown = await Reflect.apply(
            value as (...params: unknown[]) => Promise<unknown>,
            target,
            params,
          );
          this.hear({ method: property, params, returns });
          return returns;
        };
      },
    });
  }

  /** Writes the file, if this was recording. */
  finish(): void {
    if (this.recording) writeFileSync(CALLS, formatCalls(this.heard.values()));
  }

  private hear(call: Call): void {
    const key = keyOf(call);
    if (this.recording) {
      this.heard.set(key, call);
      return;
    }
    const kept = this.recorded().get(key);
    if (!kept) {
      throw new Error(
        `${label(call)} is not in fixtures/contract/calls.json, which the`
        + ` Python dataset API is held to. Record it with\n  ${RECORD}`,
      );
    }
    if (JSON.stringify(plain(kept.returns)) !== JSON.stringify(plain(call.returns))) {
      throw new Error(
        `D1 no longer answers ${label(call)} as fixtures/contract/calls.json`
        + ` says, and the Python dataset API is held to that. Record it`
        + ` again with\n  ${RECORD}`,
      );
    }
  }

  /** The file, read once, by call. */
  private recorded(): Map<string, Call> {
    if (!this.kept) {
      let calls: Call[];
      try {
        calls = JSON.parse(readCalls()) as Call[];
      } catch (error) {
        throw new Error(
          `fixtures/contract/calls.json cannot be read (${String(error)}).`
          + ` Record it with\n  ${RECORD}`,
          { cause: error },
        );
      }
      this.kept = new Map(calls.map((call) => [keyOf(call), call]));
    }
    return this.kept;
  }
}
