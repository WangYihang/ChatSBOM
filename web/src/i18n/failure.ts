/**
 * What a failure says, in the reader's language (#43).
 *
 * The Worker, the agent loop and the Turnstile widget write their
 * failures in English, for the log and the English page alike, and the
 * Chinese page showed them as they came. None of them is told the
 * reader's language, and none needs to be: a failure says what kind it
 * is — the status the Worker refused with, why the model stopped, which
 * step of the challenge failed — and the dictionary says that kind in
 * the page's language, when the failure is shown rather than when it
 * happens, so a switch of language reaches one already on the page.
 *
 * A question to the model says its failures in `ask/failure.ts`, beside
 * the Ask panel: it knows the agent's and the widget's errors, and the
 * page loads those with the panel, not before it (#44).
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
