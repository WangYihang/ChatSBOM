/**
 * The agent loop, running in the page.
 *
 * This side owns the loop: post a turn, execute whatever tools come
 * back, post the results, repeat until the model stops asking.
 *
 * The loop is here rather than in the Worker so that one request is one
 * model turn: the Worker stays stateless, and a conversation that goes
 * long cannot hold a request open. The tool calls themselves reach the
 * Worker's query endpoint, which answers from ClickHouse or D1,
 * whichever the deployment configures.
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

/**
 * A Turnstile challenge, as the Worker describes it: the widget's site
 * key, and the action to render it with, which the Worker checks the
 * token was solved for (#115).
 */
export interface Challenge {
  siteKey: string;
  action: string;
}

/** What the Worker needs before a question: a Turnstile token, or nothing. */
interface Settings {
  turnstile: Challenge | null;
}

/** A refused turn, and how to pass, when a Turnstile token would. */
interface Refusal {
  error: string;
  /** The HTTP status it was refused with. */
  status: number;
  turnstile?: Challenge;
}

/**
 * Solves a Turnstile challenge, resolving with its token.
 *
 * The page supplies it, since the widget has to be drawn somewhere, and
 * a token is good for one turn: this is asked once a question, and
 * again only when a question's session is refused.
 */
export type SolveChallenge = (challenge: Challenge) => Promise<string>;

export interface AgentEvents {
  /** A summary of the model's reasoning, when it chose to share one. */
  onThinking?(text: string): void;
  /** Prose from the model. */
  onText?(text: string): void;
  /** A tool is about to run, so the UI can say what is happening. */
  onToolCall?(name: string, input: unknown): void;
  /** Cumulative token usage, for an honest cost display. */
  onUsage?(usage: Anthropic.Usage): void;
  /** The API paused a long turn, and the agent is carrying it on. */
  onPause?(): void;
}

/**
 * A loop that never runs forever: each turn is a paid API call, and a
 * model that keeps calling tools would otherwise spend without bound.
 */
const MAX_TURNS = 8;

/**
 * Why a question got no answer, for a page to say in its own words.
 *
 * The error's message stays the English sentence — the log's, and the
 * English page's — and this says which sentence it is, so a page in
 * another language can say it in that one (#43). A refusal keeps the
 * Worker's status: one status stands for several of its sentences, and
 * only the sentence says which.
 */
export type AgentFailure =
  | { kind: 'refused'; status: number }
  | { kind: 'cut-off' }
  | { kind: 'declined' }
  | { kind: 'too-long' }
  | { kind: 'stopped'; reason: string }
  | { kind: 'turns'; turns: number }
  | { kind: 'garbled' };

export class AgentError extends Error {
  constructor(
    message: string,
    readonly failure: AgentFailure,
  ) {
    super(message);
  }
}

/** The prose of a turn. */
function prose(content: Anthropic.ContentBlock[]): string {
  return content
    .filter((b): b is Anthropic.TextBlock => b.type === 'text')
    .map((b) => b.text)
    .join('\n')
    .trim();
}

export class Agent {
  /** Every question since the last `reset`, and what answered it. */
  private messages: Anthropic.MessageParam[] = [];

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

  /**
   * Start a new conversation, forgetting every question asked so far.
   *
   * The Worker refuses a conversation past 40 messages or its bound on
   * characters with "Start a new one", and nothing on the page could
   * (#42). A new list rather than an emptied one: a question still on
   * its way finishes against the conversation it was asked in, and
   * nothing it adds reaches this one.
   */
  reset(): void {
    this.messages = [];
    this.session = undefined;
  }

  /**
   * Ask a question, returning the model's final prose.
   *
   * A question that fails takes itself back out of the conversation,
   * with every turn it added (#42). It stayed, and rode along with every
   * later question: one refused as too long was refused again on each
   * question after it, and only a reload would clear it.
   */
  async ask(question: string): Promise<string> {
    // Before the question joins the conversation, so that a challenge
    // that cannot be solved leaves nothing half-asked behind.
    const token = await this.verify();
    const conversation = this.messages;
    const before = conversation.length;
    conversation.push({ role: 'user', content: question });
    try {
      return await this.answer(conversation, token);
    } catch (error) {
      conversation.splice(before);
      throw error;
    }
  }

  /**
   * The turns of one question, until one of them is its answer.
   *
   * Only `tool_use` runs tools, and only an ended turn is an answer. The
   * other stops were returned as answers too (#42): a turn cut off at
   * its length limit, a refusal. Each is said for what it is now, and
   * its content is not kept — a turn cut off inside a tool call carries
   * that call's input half-written.
   */
  private async answer(
    conversation: Anthropic.MessageParam[],
    token: string | undefined,
  ): Promise<string> {
    for (let turn = 0; turn < MAX_TURNS; turn += 1) {
      const response = await this.postTurn(conversation, token);
      // Cloudflare accepts a token once. The turns after this one
      // present the session its answer carried.
      token = undefined;
      this.events.onUsage?.(response.usage);

      switch (response.stop_reason) {
        case 'end_turn':
        case 'stop_sequence':
          this.keep(conversation, response);
          return prose(response.content);

        case 'tool_use': {
          this.keep(conversation, response);
          const calls = response.content.filter(
            (b): b is Anthropic.ToolUseBlock => b.type === 'tool_use',
          );
          // Parallel tool calls must come back in a *single* user
          // message; splitting them teaches the model to stop making
          // them.
          const results = await Promise.all(calls.map((call) => this.run(call)));
          conversation.push({ role: 'user', content: results });
          break;
        }

        case 'pause_turn':
          // The API paused a long turn, and carries it on when the turn
          // is sent back as it came — last, with nothing after it. A
          // user message asking it to go on would be a question the
          // reader never asked.
          this.keep(conversation, response);
          this.events.onPause?.();
          break;

        case 'max_tokens':
          throw new AgentError(
            'The answer was cut off at its length limit before it finished. '
            + 'Try a narrower question.',
            { kind: 'cut-off' },
          );

        case 'refusal':
          throw new AgentError('The model declined to answer this question.', {
            kind: 'declined',
          });

        case 'model_context_window_exceeded':
          throw new AgentError(
            'The conversation is too long for the model. Start a new conversation.',
            { kind: 'too-long' },
          );

        default:
          throw new AgentError(
            `The model stopped without an answer (${String(response.stop_reason)}).`,
            { kind: 'stopped', reason: String(response.stop_reason) },
          );
      }
    }

    throw new AgentError(
      `Gave up after ${MAX_TURNS} turns without a final answer.`,
      { kind: 'turns', turns: MAX_TURNS },
    );
  }

  /** Report a turn's reasoning and prose, and add it to the conversation. */
  private keep(conversation: Anthropic.MessageParam[], response: TurnResponse): void {
    for (const block of response.content) {
      if (block.type === 'thinking' && block.thinking) {
        this.events.onThinking?.(block.thinking);
      } else if (block.type === 'text') {
        this.events.onText?.(block.text);
      }
    }
    // Verbatim: tool_use ids must match the tool_result blocks that
    // answer them, and a paused turn is carried on as it came.
    conversation.push({ role: 'assistant', content: response.content });
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
        { kind: 'refused', status: response.status },
      );
    }
    const turnstile = payload?.turnstile;
    return turnstile ? this.solve(turnstile) : undefined;
  }

  /**
   * One turn: posted with a fresh token, or with the question's session.
   *
   * A session can be refused mid-question — it lasts ten minutes, and is
   * tied to the address the visitor asks from — and the refusal says
   * what passing again needs. That turn is posted once more with a
   * fresh token; a fresh token refused is the end of the question.
   */
  private async postTurn(
    conversation: Anthropic.MessageParam[],
    token?: string,
  ): Promise<TurnResponse> {
    let reply = await this.send(conversation, token);
    if ('error' in reply && reply.turnstile && !token && this.solve) {
      reply = await this.send(conversation, await this.solve(reply.turnstile));
    }
    if ('error' in reply) {
      throw new AgentError(reply.error, { kind: 'refused', status: reply.status });
    }
    if (reply.session) this.session = reply.session;
    return reply;
  }

  private async send(
    conversation: Anthropic.MessageParam[],
    token?: string,
  ): Promise<TurnResponse | Refusal> {
    const response = await fetch(this.endpoint, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        messages: conversation,
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
        status: response.status,
        ...(refusal.turnstile ? { turnstile: refusal.turnstile } : {}),
      };
    }
    if (!payload || !('content' in payload)) {
      throw new AgentError('Chat response was not understood.', { kind: 'garbled' });
    }
    return payload;
  }
}
