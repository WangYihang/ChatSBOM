/**
 * The agent loop, running in the page.
 *
 * The dataset is here, not on the Worker, so this side owns the loop: post
 * a turn, execute whatever tools come back, post the results, repeat
 * until the model stops asking.
 *
 * The loop is here rather than in the Worker so that one request is one
 * model turn: the Worker stays stateless, and a conversation that goes
 * long cannot hold a request open. The tool calls themselves reach the
 * Worker's query endpoint, which answers from D1.
 */
import type Anthropic from '@anthropic-ai/sdk';

import type { DatasetClient } from './d1/client';
import { executeTool } from './tools';

/** One model turn, as the Worker returns it. */
interface TurnResponse {
  id: string;
  stop_reason: Anthropic.Message['stop_reason'];
  content: Anthropic.ContentBlock[];
  usage: Anthropic.Usage;
}

export interface AgentEvents {
  /** A summary of the model's reasoning, when it chose to share one. */
  onThinking?(text: string): void;
  /** Prose from the model. */
  onText?(text: string): void;
  /** A tool is about to run, so the UI can say what is happening. */
  onToolCall?(name: string, input: unknown): void;
  /** Cumulative token usage, for an honest cost display. */
  onUsage?(usage: Anthropic.Usage): void;
}

/**
 * A loop that never runs forever: each turn is a paid API call, and a
 * model that keeps calling tools would otherwise spend without bound.
 */
const MAX_TURNS = 8;

export class AgentError extends Error {}

export class Agent {
  private readonly messages: Anthropic.MessageParam[] = [];

  constructor(
    private readonly dataset: DatasetClient,
    private readonly events: AgentEvents = {},
    private readonly endpoint = '/api/chat',
    /** Supplied by the Turnstile widget when the deployment requires it. */
    private turnstileToken?: string,
  ) {}

  setTurnstileToken(token: string): void {
    this.turnstileToken = token;
  }

  /** Ask a question, returning the model's final prose. */
  async ask(question: string): Promise<string> {
    this.messages.push({ role: 'user', content: question });

    for (let turn = 0; turn < MAX_TURNS; turn += 1) {
      const response = await this.postTurn();
      this.events.onUsage?.(response.usage);

      for (const block of response.content) {
        if (block.type === 'thinking' && block.thinking) {
          this.events.onThinking?.(block.thinking);
        } else if (block.type === 'text') {
          this.events.onText?.(block.text);
        }
      }

      // Keep the assistant turn verbatim: tool_use ids must match the
      // tool_result blocks that answer them.
      this.messages.push({ role: 'assistant', content: response.content });

      if (response.stop_reason !== 'tool_use') {
        return response.content
          .filter((b): b is Anthropic.TextBlock => b.type === 'text')
          .map((b) => b.text)
          .join('\n')
          .trim();
      }

      const calls = response.content.filter(
        (b): b is Anthropic.ToolUseBlock => b.type === 'tool_use',
      );

      // Parallel tool calls must come back in a *single* user message;
      // splitting them teaches the model to stop making them.
      const results = await Promise.all(calls.map((call) => this.run(call)));
      this.messages.push({ role: 'user', content: results });
    }

    throw new AgentError(
      `Gave up after ${MAX_TURNS} turns without a final answer.`,
    );
  }

  private async run(
    call: Anthropic.ToolUseBlock,
  ): Promise<Anthropic.ToolResultBlockParam> {
    this.events.onToolCall?.(call.name, call.input);
    try {
      const result = await executeTool(this.dataset, call.name, call.input);
      return {
        type: 'tool_result',
        tool_use_id: call.id,
        content: JSON.stringify(result),
      };
    } catch (error) {
      // A failed tool is reported back, never dropped: the model can
      // recover, and a missing tool_result is a malformed conversation.
      return {
        type: 'tool_result',
        tool_use_id: call.id,
        is_error: true,
        content:
          error instanceof Error ? error.message : 'tool execution failed',
      };
    }
  }

  private async postTurn(): Promise<TurnResponse> {
    const response = await fetch(this.endpoint, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        messages: this.messages,
        ...(this.turnstileToken ? { turnstileToken: this.turnstileToken } : {}),
      }),
    });

    const payload = (await response.json().catch(() => null)) as
      | { error?: string }
      | TurnResponse
      | null;

    if (!response.ok) {
      const message =
        payload && 'error' in payload && payload.error
          ? payload.error
          : `Chat request failed (${response.status}).`;
      throw new AgentError(message);
    }
    if (!payload || !('content' in payload)) {
      throw new AgentError('Chat response was not understood.');
    }
    return payload;
  }
}
