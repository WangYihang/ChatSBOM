/**
 * The seam for a natural-language query UI.
 *
 * A generic one is coming from elsewhere, so this file defines what it
 * gets and what the page expects back, and nothing about how it looks.
 * The component currently in the slot is a placeholder: it is the
 * smallest thing that exercises the contract end to end, so the seam is
 * known to work before anything is dropped into it.
 *
 * What the page provides:
 *
 *   - `ask`, a question in and prose out, with progress reported as it
 *     goes. Tool calls run against the dataset in this browser; the
 *     Worker only relays to the Messages API and never sees a result.
 *   - `onPackage`, so an answer that names a package can send the reader
 *     to that package's view rather than leaving them to retype it.
 *
 * What it must not need:
 *
 *   - the dataset itself. Anything that queries directly can also
 *     compose SQL, and the whole point of the tool layer is that a
 *     question cannot reach data the UI could not.
 *   - an API key. It is a Worker secret and stays there.
 *   - a DOM host. The slot renders inside a panel that owns its layout.
 */

/** Progress from a run, for a UI that wants to show its work. */
export interface AskProgress {
  /** A summary of the model's reasoning, when it shares one. */
  onThinking?(text: string): void;
  /** A tool is about to run, named with the arguments it was given. */
  onToolCall?(name: string, input: unknown): void;
}

/**
 * One question, one answer.
 *
 * Rejects rather than returning an error string: a UI that wants to
 * style failures differently from answers needs them separable, and
 * every failure here is already a message written for a reader (rate
 * limited, budget exhausted, model unreachable, gave up after N turns).
 */
export type AskFn = (
  question: string,
  progress?: AskProgress,
) => Promise<string>;

export interface AskUiProps {
  /** Run a question. */
  ask: AskFn;
  /** Send the reader to a package's view. */
  onPackage?(name: string): void;
  /**
   * Questions worth offering when the box is empty.
   *
   * Supplied by the page because they depend on what is in the dataset,
   * which the UI has no way to know.
   */
  suggestions?: readonly string[];
}
