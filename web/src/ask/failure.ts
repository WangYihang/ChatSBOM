/**
 * What a question to the model says when it fails, in the reader's
 * language (#43), as `i18n/failure.ts` says a query's.
 *
 * Here rather than there because it knows the agent's and the widget's
 * errors, and knowing them is importing them: the page loads those with
 * the Ask panel, not before it, and this with them (#44).
 */
import { AgentError } from '../agent';
import { said } from '../i18n/failure';
import type { Dictionary } from '../i18n/strings';
import { VerificationError } from './turnstile';

/** A question to the model that failed. */
export function askFailure(error: unknown, words: Dictionary): string {
  if (error instanceof AgentError) {
    const { failure } = error;
    return failure.kind === 'refused'
      ? words.askRefused(failure.status, error.message)
      : words.askStopped(failure, error.message);
  }
  if (error instanceof VerificationError) {
    return words.askUnverified(error.step, error.code, error.message);
  }
  return words.askFailed(said(error));
}
