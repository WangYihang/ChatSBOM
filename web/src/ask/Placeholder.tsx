/**
 * Placeholder natural-language query UI.
 *
 * Deliberately plain: a generic one is coming from elsewhere and will
 * replace this file. What it is here for is to exercise the seam in
 * ./contract.ts end to end, so the boundary is known to work before
 * anything is dropped into it — a slot nothing has ever run in is a
 * guess, not an interface.
 *
 * It therefore takes `AskUiProps` and nothing else. In particular it
 * does not take the dataset: anything holding that could compose SQL,
 * and the point of the tool layer is that a question cannot reach data
 * the UI could not.
 */
import { useCallback, useRef, useState } from 'react';

import type { AskUiProps } from './contract';
import { askFailure } from './failure';
import type { Dictionary } from '../i18n/strings';

interface TraceLine {
  id: number;
  text: string;
  kind: 'thinking' | 'tool';
}

/**
 * The package a tool call looks up, if it looks one up: every tool that
 * does names it `name` (`tools.ts`). A search's fragment is not one.
 */
function lookedUp(input: unknown): string | null {
  const name = (input as { name?: unknown } | null)?.name;
  return typeof name === 'string' && name !== '' ? name : null;
}

export function AskPlaceholder({
  ask,
  onPackage,
  reset,
  suggestions = [],
  words,
}: AskUiProps & { words: Dictionary }) {
  const [question, setQuestion] = useState('');
  const [asking, setAsking] = useState(false);
  const [trace, setTrace] = useState<TraceLine[]>([]);
  // A failure is kept as it came and said when it is drawn, in the
  // language the page speaks then (#43).
  const [answer, setAnswer] = useState<
    { failed: false; text: string } | { failed: true; error: unknown } | null
  >(null);
  // Whether there is a conversation to start over from. Only an answer
  // makes one: a question that fails leaves the conversation as it was.
  const [answered, setAnswered] = useState(false);
  // The packages the question looked up, in the order it did (#123):
  // what its answer is about, and so where the answer can send the
  // reader. The page gave `onPackage` for that and it went unused, so
  // this part of the seam had never run.
  const [packages, setPackages] = useState<string[]>([]);
  const nextId = useRef(0);

  const push = useCallback((kind: TraceLine['kind'], text: string) => {
    setTrace((lines) => [...lines, { id: (nextId.current += 1), kind, text }]);
  }, []);

  const submit = (event: React.FormEvent) => {
    event.preventDefault();
    const asked = question.trim();
    if (!asked || asking) return;

    setTrace([]);
    setAnswer(null);
    setPackages([]);
    setAsking(true);

    ask(asked, {
      onThinking: (text) => push('thinking', text.split('\n')[0] ?? ''),
      onToolCall: (name, input) => {
        push('tool', `${name}(${JSON.stringify(input)})`);
        const looked = lookedUp(input);
        if (looked) {
          setPackages((names) => (names.includes(looked) ? names : [...names, looked]));
        }
      },
      onPause: () => push('thinking', words.askPaused),
    })
      .then((text) => {
        setAnswer({ failed: false, text });
        setAnswered(true);
      })
      .catch((error: unknown) => setAnswer({ failed: true, error }))
      .finally(() => setAsking(false));
  };

  const startOver = () => {
    if (asking || !reset) return;
    reset();
    setAnswered(false);
    setTrace([]);
    setAnswer(null);
    setPackages([]);
  };

  return (
    <>
      <form className="controls" onSubmit={submit}>
        <input
          id="question"
          type="text"
          autoComplete="off"
          placeholder={suggestions[0] ?? words.askQuestionLabel}
          aria-label={words.askQuestionLabel}
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
        />
        <button type="submit" className="primary" disabled={asking}>
          {asking ? words.askAsking : words.askButton}
        </button>
        {/* Not while a question is running: its answer would land in a
            conversation that no longer holds it. */}
        {reset ? (
          <button
            type="button"
            className="quiet"
            disabled={asking || !answered}
            onClick={startOver}
          >
            {words.askNewConversation}
          </button>
        ) : null}
      </form>

      {/* Suggestions come from the page: they depend on what is in the
          dataset, which this component has no way to know. */}
      {!question && suggestions.length > 0 ? (
        <p className="note" style={{ marginTop: '.35rem' }}>
          {suggestions.map((text, index) => (
            <span key={text}>
              {index > 0 ? ' · ' : ''}
              <button
                type="button"
                className="drill"
                onClick={() => setQuestion(text)}
              >
                {text}
              </button>
            </span>
          ))}
        </p>
      ) : null}

      <div className="trace" aria-live="polite">
        {trace.map((line) => (
          <div key={line.id} className={line.kind === 'tool' ? 'tool' : ''}>
            {line.text}
          </div>
        ))}
      </div>

      {answer ? (
        <div className={answer.failed ? 'answer error' : 'answer'}>
          {answer.failed ? askFailure(answer.error, words) : answer.text}
        </div>
      ) : null}

      {/* Beside an answer, not a failure: a question that failed has
          nothing to send the reader to. */}
      {onPackage && answer && !answer.failed && packages.length > 0 ? (
        <p className="note" style={{ marginTop: '.35rem' }}>
          {words.askPackages}{' '}
          {packages.map((name, index) => (
            <span key={name}>
              {index > 0 ? ' · ' : ''}
              <button type="button" className="drill" onClick={() => onPackage(name)}>
                {name}
              </button>
            </span>
          ))}
        </p>
      ) : null}
    </>
  );
}
