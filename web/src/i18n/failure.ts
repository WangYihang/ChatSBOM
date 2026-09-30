/**
 * What a failure says, in the reader's language (#43).
 *
 * The service writes its failures in English, for the log and the
 * English page alike, and the Chinese page showed them as they came. It
 * is not told the reader's language, and need not be: a failure says
 * what kind it is — the status a query was refused with, the code a
 * question failed with (#144) — and the dictionary says that kind in
 * the page's language, when the failure is shown rather than when it
 * happens, so a switch of language reaches one already on the page.
 *
 * A question to the model says its failures in `ask/failure.ts`, beside
 * the Ask panel: it knows the chat's errors, and the page loads those
 * with the panel, not before it (#44).
 *
 * `.ts`, since there is no markup in it. It was `.tsx` only because the
 * dictionary it is typed against is, and the tests' compiler read no
 * JSX and refused a `.ts` file that imported one; it reads JSX now,
 * `.tsx` tests included (#44).
 */
import { QueryError } from '../d1/client';
import type { Dictionary } from './strings';

/** The English a failure was raised with, if it was raised with any. */
export const said = (error: unknown) => (error instanceof Error ? error.message : '');

/** A question to the dataset that failed. */
export function queryFailure(error: unknown, words: Dictionary): string {
  return error instanceof QueryError
    ? words.queryRefused(error.status, error.message)
    : words.queryFailed(said(error));
}
