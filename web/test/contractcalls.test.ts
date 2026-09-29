/**
 * The calls the contract suite made of D1, and what D1 answered, as kept
 * for the Python dataset API (#138).
 *
 * `contract.test.ts` records them into `fixtures/contract/calls.json`
 * (`contractcalls.ts` says how) and `tests/dataset_contract_test.py`
 * holds the Python to them. This holds the file to D1: every call in it
 * is asked again, and the answer has to be the one written down. A
 * changed statement, a changed `d1.sql` or a hand-edited answer fails
 * here, rather than leaving the Python held to answers D1 no longer
 * gives.
 */
import { describe, expect, it } from 'vitest';

import { type Call, d1, formatCalls, label, readCalls } from './contractcalls';

const text = readCalls();
const calls = JSON.parse(text) as Call[];
const dataset = d1();

describe('the calls kept for the Python dataset API', () => {
  it('are written as the recorder writes them: each once, in order', () => {
    // So that recording again changes the file only where an answer
    // changed, and the JSON hook has nothing to rewrite.
    expect(text).toBe(formatCalls(calls));
  });

  it('are some calls, not none', () => {
    // Every method the page calls is among them: the Python side checks
    // that against `DatasetQueries`. Here, only that the check below is
    // not vacuous.
    expect(calls.length).toBeGreaterThan(20);
  });

  it.each(calls.map((call) => [label(call), call] as const))(
    'D1 still answers %s as recorded',
    async (_, call) => {
      const method: unknown = Reflect.get(dataset, call.method);
      expect(typeof method).toBe('function');
      const answer: unknown = await Reflect.apply(
        method as (...params: unknown[]) => Promise<unknown>,
        dataset,
        call.params,
      );
      expect(JSON.parse(JSON.stringify(answer ?? null))).toEqual(call.returns);
    },
  );
});
