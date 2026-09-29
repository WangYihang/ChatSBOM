/**
 * What a question to the model says when it fails, in the reader's
 * language (#43), as `i18n/failure.ts` says a query's.
 *
 * Here rather than there because it knows the chat's errors, and
 * knowing them is importing them: the page loads those with the Ask
 * panel, not before it (#44).
 */
import { said } from '../i18n/failure';
import type { Dictionary } from '../i18n/strings';
import { AskError } from './stream';

/** A question to the model that failed. */
export function askFailure(error: unknown, words: Dictionary): string {
  if (error instanceof AskError) return words.askUnanswered(error.failure);
  return words.askFailed(said(error));
}
