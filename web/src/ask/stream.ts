/**
 * A question to the service, answered as it streams (#144).
 *
 * `POST /api/ask`, with `{question, prior, altcha}`: the question, the
 * reader's earlier questions and the answers they were given, as text,
 * and the proof of work solved for it (`altcha.ts`). The service runs
 * the model's loop, and its tools, itself (#140), and answers with
 * server-sent events:
 *
 *   tool   {name, arguments}        a tool the model called, as it runs
 *   text   {delta}                  the answer's next words
 *   done   {turns, usage, cost_usd} the answer is complete
 *   error  {code, message, ...}     it is not, and why
 *
 * or refuses the question before any of it: `{error, code}`, with a
 * status. An EventSource cannot post, so the page reads the body itself
 * (`events`), as the HTML standard says a stream is read.
 *
 * Every failure is an `AskError` with the code it was said with, which
 * the page says in its reader's words (`i18n/strings.tsx`); the service's
 * English is kept beside it, for the log and for what only it says.
 */
import type { AskProgress } from './contract';

/** An earlier question, and the answer the reader was given. */
export interface Exchange {
  q: string;
  a: string;
}

/** A question as the service takes it. */
export interface Asked {
  question: string;
  prior: readonly Exchange[];
  /** The solved challenge, as the widget writes it. */
  altcha: string;
}

/**
 * The longest question, and the most earlier exchanges a question may
 * bring and their characters in all, as JavaScript counts them: what
 * the service takes (`chatsbom/server/ask.py`), past which it refuses
 * the question.
 */
export const MAX_QUESTION = 4_000;
export const MAX_PRIOR = 3;
export const MAX_PRIOR_CHARS = 12_000;

/**
 * What a question brings of the conversation before it: its last
 * exchanges, as many as fit, in the order they were asked. The newest
 * is cut to fit rather than left out, since it is what a follow-up is
 * about; the ones before it fit whole, or are left out with any before
 * them.
 */
export function earlier(conversation: readonly Exchange[]): Exchange[] {
  const kept: Exchange[] = [];
  let room = MAX_PRIOR_CHARS;
  for (const exchange of conversation.slice(-MAX_PRIOR).reverse()) {
    const size = exchange.q.length + exchange.a.length;
    if (size <= room) {
      kept.push(exchange);
      room -= size;
      continue;
    }
    if (kept.length === 0 && exchange.q.length < room) {
      kept.push({ q: exchange.q, a: cut(exchange.a, room - exchange.q.length) });
    }
    break;
  }
  return kept.reverse();
}

/** `text` in at most `room` characters, an ellipsis last, and no half of a pair. */
function cut(text: string, room: number): string {
  let end = Math.max(0, room - 1);
  const last = text.charCodeAt(end - 1);
  if (last >= 0xd800 && last <= 0xdbff) end -= 1;
  return `${text.slice(0, end)}…`;
}

/** Why a question got no answer. */
export interface AskFailure {
  /**
   * The service's code for it (`chatsbom/server/ask.py`), or the page's
   * own: `unverified`, a proof of work that could not be done;
   * `interrupted`, an answer that stopped arriving; `garbled`, one not
   * understood; `refused`, a refusal that said no code.
   */
  code: string;
  /** What the service said, in English, or the page where it said nothing. */
  said: string;
  /** The status a refusal came with; null for what failed after the answer began. */
  status: number | null;
  /** Why a challenge was not taken, for `verification-failed`. */
  verdict?: string;
  /** Why the model stopped, for `stopped`. */
  reason?: string;
  /** How many turns the model had, for an error in the stream. */
  turns?: number;
}

export class AskError extends Error {
  constructor(readonly failure: AskFailure) {
    super(failure.said);
  }
}

/** One event of a stream: its name, and its data. */
export interface StreamEvent {
  event: string;
  data: string;
}

/** Where a line ends: LF, CRLF or CR. */
const END_OF_LINE = /\r\n|\r|\n/;

/**
 * The events of `body`, as a browser's EventSource reads them: a line
 * at a time, however the bytes were split, a comment skipped, and an
 * event dispatched at the blank line that ends it. One the stream ended
 * before its blank line is not dispatched: it may be cut short.
 *
 * Stopping early, as a caller does at an error, cancels the body, and
 * with it the request.
 */
export async function* events(body: ReadableStream<Uint8Array>): AsyncGenerator<StreamEvent> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let finished = false;
  let buffer = '';
  let event = '';
  let data: string[] = [];
  try {
    while (!finished) {
      const { value, done } = await reader.read();
      finished = done;
      buffer += done ? decoder.decode() : decoder.decode(value, { stream: true });
      for (;;) {
        const end = END_OF_LINE.exec(buffer);
        // A CR last may be the first half of a CRLF: the next chunk says.
        if (!end || (end[0] === '\r' && end.index === buffer.length - 1 && !finished)) break;
        const line = buffer.slice(0, end.index);
        buffer = buffer.slice(end.index + end[0].length);
        if (line === '') {
          if (data.length > 0) yield { event: event || 'message', data: data.join('\n') };
          event = '';
          data = [];
          continue;
        }
        if (line.startsWith(':')) continue;
        const colon = line.indexOf(':');
        const field = colon < 0 ? line : line.slice(0, colon);
        const text = colon < 0 ? '' : line.slice(colon + 1).replace(/^ /, '');
        if (field === 'event') event = text;
        else if (field === 'data') data.push(text);
      }
    }
  } finally {
    // Released only when cancelled: an ended stream needs no other
    // reader, and letting go of it as it ended had Chromium record the
    // request as cancelled every time, though every byte had come.
    if (!finished) {
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
  }
}

/** A refusal made before the answer began, by its code, or its status where it gave none. */
export async function refusal(response: Response): Promise<AskError> {
  const payload = (await response.json().catch(() => null)) as Record<string, unknown> | null;
  const code = typeof payload?.['code'] === 'string' ? payload['code'] : null;
  const said =
    typeof payload?.['error'] === 'string'
      ? payload['error']
      : `The question was refused (${response.status}).`;
  return new AskError({
    code: code ?? 'refused',
    said,
    status: response.status,
    ...(typeof payload?.['verdict'] === 'string' ? { verdict: payload['verdict'] } : {}),
  });
}

const GARBLED = "The service's answer was not understood.";

/** An event's data, which the service writes as one JSON object. */
function parsed(data: string): Record<string, unknown> {
  let value: unknown;
  try {
    value = JSON.parse(data);
  } catch {
    value = null;
  }
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new AskError({ code: 'garbled', said: GARBLED, status: null });
  }
  return value as Record<string, unknown>;
}

/** An error event, as the failure it says. */
function failed(said: Record<string, unknown>): AskError {
  const text = (key: string) => (typeof said[key] === 'string' ? (said[key] as string) : undefined);
  const reason = text('reason');
  const turns = typeof said['turns'] === 'number' ? said['turns'] : undefined;
  return new AskError({
    code: text('code') ?? 'failed',
    said: text('message') ?? '',
    status: null,
    ...(reason === undefined ? {} : { reason }),
    ...(turns === undefined ? {} : { turns }),
  });
}

/**
 * Ask `asked`, and resolve with the answer once the service says it is
 * done, telling `progress` of each tool as it runs and of the answer as
 * it is written.
 *
 * What a turn wrote before it called tools is not the answer: it is the
 * model thinking aloud, and goes to `onThinking`, as the Worker's turns
 * did; the answer is what the last turn writes.
 */
export async function ask(
  asked: Asked,
  progress: AskProgress = {},
  endpoint = '/api/ask',
): Promise<string> {
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
    body: JSON.stringify(asked),
  });
  if (!response.ok) throw await refusal(response);
  const kind = response.headers.get('content-type') ?? '';
  if (!response.body || !kind.startsWith('text/event-stream')) {
    await response.body?.cancel().catch(() => {});
    throw new AskError({ code: 'garbled', said: GARBLED, status: null });
  }

  let answer = '';
  // The last event, `done` or `error`, and what it came to. The service
  // ends the stream after it, and the stream is read to that end: cut
  // off before it, the request is one the browser says was aborted.
  let outcome: { answer: string } | AskError | null = null;
  for await (const { event, data } of events(response.body)) {
    if (outcome) continue;
    if (event === 'text') {
      const { delta } = parsed(data);
      answer += typeof delta === 'string' ? delta : '';
      progress.onText?.(answer);
    } else if (event === 'tool') {
      const { name, arguments: input } = parsed(data);
      if (answer.trim()) progress.onThinking?.(answer.trim());
      if (answer) progress.onText?.('');
      answer = '';
      progress.onToolCall?.(typeof name === 'string' ? name : '', input ?? null);
    } else if (event === 'done') {
      outcome = { answer: answer.trim() };
    } else if (event === 'error') {
      outcome = failed(parsed(data));
    }
    // Any other event is one this page does not know: passed over.
  }
  if (outcome instanceof AskError) throw outcome;
  if (outcome) return outcome.answer;
  throw new AskError({
    code: 'interrupted',
    said: 'The answer stopped arriving before it was finished.',
    status: null,
  });
}
