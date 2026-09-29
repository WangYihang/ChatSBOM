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
 *     goes. The service runs the model's loop and its tool calls, each
 *     a question of the dataset it answers itself, and streams the
 *     answer back (#144): it sees the questions and the rows alike.
 *   - `onPackage`, so an answer that names a package can send the reader
 *     to that package's view rather than leaving them to retype it.
 *   - `reset`, which starts a new conversation: the earlier questions
 *     and answers a question brings are forgotten (#42).
 *
 * What it must not need:
 *
 *   - the dataset itself. Anything that queries directly can also
 *     compose SQL, and the whole point of the tool layer is that a
 *     question cannot reach data the UI could not.
 *   - an API key. It is the service's, and stays there.
 *   - a DOM host. The slot renders inside a panel that owns its layout.
 */

/** Progress from a run, for a UI that wants to show its work. */
export interface AskProgress {
  /**
   * What the model said before it called its tools: its thinking aloud,
   * not the answer.
   */
  onThinking?(text: string): void;
  /** A tool is about to run, named with the arguments it was given. */
  onToolCall?(name: string, input: unknown): void;
  /**
   * The answer so far, as the model writes it: each time the whole of
   * it, and nothing once the model turns to its tools again.
   */
  onText?(text: string): void;
}

/**
 * One question, one answer.
 *
 * Rejects rather than returning an error string: a UI that wants to
 * style failures differently from answers needs them separable, and
 * every failure here says what kind it is — an `AskError`, with the
 * service's code for it (rate limited, budget exhausted, model
 * unreachable, gave up after N turns) — which `askFailure` says in the
 * page's language (#43).
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
  /** Start a new conversation: forget every question asked so far. */
  reset?(): void;
  /**
   * Questions worth offering when the box is empty.
   *
   * Supplied by the page because they depend on what is in the dataset,
   * which the UI has no way to know.
   */
  suggestions?: readonly string[];
}
