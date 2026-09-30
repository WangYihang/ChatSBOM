/**
 * The calls the contract suite made of D1, and what D1 answered each,
 * as they were recorded before the Worker, and D1 with it, were
 * deleted (#151): `fixtures/contract/calls.json`.
 *
 * The Python dataset API, which answers the page now, is held to them:
 * `tests/dataset_contract_test.py` asks it every call over the same
 * corpus, `fixtures/contract/d1.sql`, and expects the same answer. And
 * the page's URL for each is recorded from them
 * (`contracturls.test.ts`), which `tests/server_queries_test.py` asks
 * the service. Nothing records them again: a method the page comes to
 * ask needs a call written into the file, with its answer, by hand.
 */
import { readFileSync } from 'node:fs';

/** Beside the corpus the calls were asked of. */
const CALLS = decodeURIComponent(
  new URL('./fixtures/contract/calls.json', import.meta.url).pathname,
);

/** One call of a dataset method, and what D1 answered. */
export interface Call {
  method: string;
  /** Its parameters, in order, as the suite passed them. */
  params: unknown[];
  returns: unknown;
}

/** The file as it stands. */
export function readCalls(): string {
  return readFileSync(CALLS, 'utf8');
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
 * JSON as the repository's hook (`pretty-format-json --indent=4`) lays
 * it out: keys sorted, anything past ASCII escaped, and a newline last.
 * As JSON carries a value, too: what went over the wire, which is what
 * the service has to match, and not the object that held it.
 */
export function layout(value: unknown): string {
  const plain = sortKeys(JSON.parse(JSON.stringify(value ?? null)));
  const text = JSON.stringify(plain, null, 4).replace(
    /[\u007f-￿]/g,
    (unit) => `\\u${unit.charCodeAt(0).toString(16).padStart(4, '0')}`,
  );
  return `${text}\n`;
}
