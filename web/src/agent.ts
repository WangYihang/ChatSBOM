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
  /**
   * On the answer to a turn that passed Turnstile: what the question's
   * later turns present in place of a token (#32).
   */
  session?: string;
}

/** What the Worker needs before a question: a Turnstile token, or nothing. */
interface Settings {
  turnstile: { siteKey: string } | null;
}

/** A refused turn, and how to pass, when a Turnstile token would. */
interface Refusal {
  error: string;
  turnstile?: { siteKey: string };
}

/**
 * Solves a Turnstile challenge for a site key, resolving with its token.
 *
 * The page supplies it, since the widget has to be drawn somewhere, and
 * a token is good for one turn: this is asked once a question, and
 * again only when a question's session is refused.
 */
export type SolveChallenge = (siteKey: string) => Promise<string>;

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

  /** What the current question's first turn was answered with (#32). */
  private session: string | undefined;

  constructor(
    private readonly dataset: DatasetClient,
    private readonly events: AgentEvents = {},
    private readonly endpoint = '/api/chat',
    /**
     * How the page passes Turnstile, for a deployment that requires it
     * (#32). An agent without one asks without, as it always did, and
     * such a deployment refuses it.
     */
    private readonly solve?: SolveChallenge,
  ) {}

  /** Ask a question, returning the model's final prose. */
  async ask(question: string): Promise<string> {
    // Before the question joins the conversation, so that a challenge
    // that cannot be solved leaves nothing half-asked behind.
    let token = await this.verify();
    this.messages.push({ role: 'user', content: question });

    for (let turn = 0; turn < MAX_TURNS; turn += 1) {
      const response = await this.postTurn(token);
      // Cloudflare accepts a token once. The turns after this one
      // present the session its answer carried.
      token = undefined;
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

  /**
   * A Turnstile token for the first turn of a question, when the
   * deployment asks for one.
   *
   * The Worker is asked before every question rather than once a page:
   * a deployment can turn Turnstile on while a page is open, and a
   * page that remembered it off would have every question refused.
   */
  private async verify(): Promise<string | undefined> {
    this.session = undefined;
    if (!this.solve) return undefined;

    const response = await fetch(this.endpoint, {
      method: 'GET',
      headers: { accept: 'application/json' },
    });
    const payload = (await response.json().catch(() => null)) as
      | (Partial<Settings> & { error?: string })
      | null;
    if (!response.ok) {
      throw new AgentError(
        payload?.error || `Chat request failed (${response.status}).`,
      );
    }
    const turnstile = payload?.turnstile;
    return turnstile ? this.solve(turnstile.siteKey) : undefined;
  }

  /**
   * One turn: posted with a fresh token, or with the question's session.
   *
   * A session can be refused mid-question — it lasts ten minutes, and is
   * tied to the address the visitor asks from — and the refusal says
   * what passing again needs. That turn is posted once more with a
   * fresh token; a fresh token refused is the end of the question.
   */
  private async postTurn(token?: string): Promise<TurnResponse> {
    let reply = await this.send(token);
    if ('error' in reply && reply.turnstile && !token && this.solve) {
      reply = await this.send(await this.solve(reply.turnstile.siteKey));
    }
    if ('error' in reply) throw new AgentError(reply.error);
    if (reply.session) this.session = reply.session;
    return reply;
  }

  private async send(token?: string): Promise<TurnResponse | Refusal> {
    const response = await fetch(this.endpoint, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        messages: this.messages,
        ...(token
          ? { turnstileToken: token }
          : this.session
            ? { session: this.session }
            : {}),
      }),
    });

    const payload = (await response.json().catch(() => null)) as
      | Partial<Refusal>
      | TurnResponse
      | null;

    if (!response.ok) {
      const refusal = (payload ?? {}) as Partial<Refusal>;
      return {
        error: refusal.error || `Chat request failed (${response.status}).`,
        ...(refusal.turnstile ? { turnstile: refusal.turnstile } : {}),
      };
    }
    if (!payload || !('content' in payload)) {
      throw new AgentError('Chat response was not understood.');
    }
    return payload;
  }
}
