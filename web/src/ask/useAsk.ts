/**
 * The page's side of the natural-language seam.
 *
 * Adapts the browser-side agent loop to the `AskFn` in ./contract.ts, so
 * whatever UI sits in the slot needs to know only "question in, prose
 * out, progress on the way" — not that there is an Anthropic client
 * behind a Worker, nor that the tools query D1 through it.
 */
import { useCallback, useRef } from 'react';

import { Agent } from '../agent';
import type { DatasetClient } from '../d1/client';
import type { AskFn, AskProgress } from './contract';

export function useAsk(dataset: DatasetClient): AskFn {
  // One agent for the life of the dataset: it holds the conversation, so
  // rebuilding it per question would drop the history that makes a
  // follow-up question work.
  const agent = useRef<Agent | null>(null);
  const events = useRef<AskProgress>({});

  if (!agent.current) {
    agent.current = new Agent(dataset, {
      // Routed through a ref so a UI can supply its own handlers per
      // question without the agent being rebuilt — rebuilding it is
      // what would lose the conversation.
      onThinking: (text) => events.current.onThinking?.(text),
      onToolCall: (name, input) => events.current.onToolCall?.(name, input),
    });
  }

  return useCallback(
    (question: string, progress: AskProgress = {}) => {
      events.current = progress;
      return agent.current!.ask(question);
    },
    [],
  );
}
