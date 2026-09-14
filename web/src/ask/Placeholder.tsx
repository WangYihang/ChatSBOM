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

interface TraceLine {
  id: number;
  text: string;
  kind: 'thinking' | 'tool';
}

export function AskPlaceholder({ ask, suggestions = [] }: AskUiProps) {
  const [question, setQuestion] = useState('');
  const [asking, setAsking] = useState(false);
  const [trace, setTrace] = useState<TraceLine[]>([]);
  const [answer, setAnswer] = useState<{ text: string; failed: boolean } | null>(
    null,
  );
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
    setAsking(true);

    ask(asked, {
      onThinking: (text) => push('thinking', text.split('\n')[0] ?? ''),
      onToolCall: (name, input) => push('tool', `${name}(${JSON.stringify(input)})`),
    })
      .then((text) => setAnswer({ text, failed: false }))
      .catch((error: unknown) =>
        setAnswer({
          text:
            error instanceof Error
              ? error.message
              : 'The question could not be answered.',
          failed: true,
        }),
      )
      .finally(() => setAsking(false));
  };

  return (
    <>
      <form className="controls" onSubmit={submit}>
        <input
          id="question"
          type="text"
          autoComplete="off"
          placeholder="Which projects declare mail rather than inheriting it?"
          aria-label="Question"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
        />
        <button type="submit" className="primary" disabled={asking}>
          {asking ? 'Asking…' : 'Ask'}
        </button>
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
          {answer.text}
        </div>
      ) : null}
    </>
  );
}
