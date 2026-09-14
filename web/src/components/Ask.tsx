/**
 * The natural-language question box.
 *
 * The model's tools are the same typed queries this page uses, so a
 * question cannot reach anything the UI could not. The agent loop runs
 * in the page; the Worker only relays to the Messages API and never sees
 * a query result.
 */
import { useCallback, useMemo, useRef, useState } from 'react';

import { Agent, AgentError } from '../agent';
import type { Dataset } from '../queries';

interface TraceLine {
  id: number;
  text: string;
  kind: 'thinking' | 'tool';
}

export function Ask({ dataset }: { dataset: Dataset }) {
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

  // One agent per dataset: it keeps the conversation, so remaking it on
  // every render would lose the history mid-answer.
  const agent = useMemo(
    () =>
      new Agent(dataset, {
        onThinking: (text) => push('thinking', text.split('\n')[0] ?? ''),
        onToolCall: (name, input) =>
          push('tool', `${name}(${JSON.stringify(input)})`),
      }),
    [dataset, push],
  );

  const submit = (event: React.FormEvent) => {
    event.preventDefault();
    const asked = question.trim();
    if (!asked || asking) return;

    setTrace([]);
    setAnswer(null);
    setAsking(true);

    agent
      .ask(asked)
      .then((text) => setAnswer({ text, failed: false }))
      .catch((error: unknown) =>
        setAnswer({
          text:
            error instanceof AgentError || error instanceof Error
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
